#include <gtest/gtest.h>

#include <thread>

#include "client/client.h"
#include "client/txn_client.h"
#include "cluster_harness.h"

namespace flotilla::server {
namespace {

using testing::HarnessCluster;

TEST(TxnCluster, CommitAndSnapshotReads) {
  HarnessCluster cluster("txn_basic");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  client::Txn t1(&c);
  ASSERT_TRUE(t1.Begin().ok());
  t1.Put("acct-a", "100");
  t1.Put("acct-b", "200");
  ASSERT_TRUE(t1.Commit().ok());

  client::Txn t2(&c);
  ASSERT_TRUE(t2.Begin().ok());
  std::string v;
  ASSERT_TRUE(t2.Get("acct-a", &v).ok());
  EXPECT_EQ(v, "100");
  ASSERT_TRUE(t2.Get("acct-b", &v).ok());
  EXPECT_EQ(v, "200");
  EXPECT_TRUE(t2.Get("missing", &v).IsNotFound());
  ASSERT_TRUE(t2.Commit().ok());  // read-only commit is a no-op
}

TEST(TxnCluster, SnapshotIsolationAgainstLaterCommit) {
  HarnessCluster cluster("txn_snapshot");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  client::Txn setup(&c);
  ASSERT_TRUE(setup.Begin().ok());
  setup.Put("x", "before");
  ASSERT_TRUE(setup.Commit().ok());

  client::Txn reader(&c);
  ASSERT_TRUE(reader.Begin().ok());

  client::Client c2(cluster.ClientAddrs());
  client::Txn writer(&c2);
  ASSERT_TRUE(writer.Begin().ok());
  writer.Put("x", "after");
  ASSERT_TRUE(writer.Commit().ok());

  // The reader's snapshot predates the writer's commit.
  std::string v;
  ASSERT_TRUE(reader.Get("x", &v).ok());
  EXPECT_EQ(v, "before");

  client::Txn fresh(&c);
  ASSERT_TRUE(fresh.Begin().ok());
  ASSERT_TRUE(fresh.Get("x", &v).ok());
  EXPECT_EQ(v, "after");
}

TEST(TxnCluster, WriteWriteConflictAbortsOne) {
  HarnessCluster cluster("txn_conflict");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c1(cluster.ClientAddrs());
  client::Client c2(cluster.ClientAddrs());

  client::Txn t1(&c1);
  client::Txn t2(&c2);
  ASSERT_TRUE(t1.Begin().ok());
  ASSERT_TRUE(t2.Begin().ok());
  t1.Put("contested", "from-t1");
  t2.Put("contested", "from-t2");
  ASSERT_TRUE(t1.Commit().ok());
  // t2 began before t1's commit: its prewrite must hit a write conflict.
  Status s = t2.Commit();
  EXPECT_FALSE(s.ok()) << "second writer must abort";

  client::Txn check(&c1);
  ASSERT_TRUE(check.Begin().ok());
  std::string v;
  ASSERT_TRUE(check.Get("contested", &v).ok());
  EXPECT_EQ(v, "from-t1");
}

TEST(TxnCluster, CrossShardAtomicCommit) {
  HarnessCluster cluster("txn_cross_shard");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  // Split so "a..." and "z..." live in different raft groups.
  ASSERT_TRUE(c.Put("a-seed", "1").ok());
  ASSERT_TRUE(c.Split("m").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  client::Txn t(&c);
  ASSERT_TRUE(t.Begin().ok());
  t.Put("a-left", "L");
  t.Put("z-right", "R");
  ASSERT_TRUE(t.Commit().ok());

  client::Txn check(&c);
  ASSERT_TRUE(check.Begin().ok());
  std::string v;
  ASSERT_TRUE(check.Get("a-left", &v).ok());
  EXPECT_EQ(v, "L");
  ASSERT_TRUE(check.Get("z-right", &v).ok());
  EXPECT_EQ(v, "R");

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(check.Scan("a", "zz", 100, &rows).ok());
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0].first, "a-left");
  EXPECT_EQ(rows[1].first, "z-right");
}

TEST(TxnCluster, AbandonedTxnResolvedByReader) {
  HarnessCluster cluster("txn_abandoned");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  client::Txn setup(&c);
  ASSERT_TRUE(setup.Begin().ok());
  setup.Put("k", "committed-value");
  ASSERT_TRUE(setup.Commit().ok());

  // Simulate a coordinator that dies mid-2PC: prewrite directly with an old
  // wall clock so its TTL is already expired, then never commit.
  net::Request pre;
  pre.type = net::MsgType::kTxnPrewrite;
  pre.key = "k";
  pre.value = "orphaned";
  pre.wop = 1;
  pre.primary = "k";
  {
    client::Txn ts_source(&c);
    ASSERT_TRUE(ts_source.Begin().ok());
    pre.ts = ts_source.start_ts();
  }
  pre.ts2 = 1;  // wall_ms far in the past: instantly expired
  net::Response resp;
  ASSERT_TRUE(c.Call(pre, &resp).ok());
  ASSERT_TRUE(resp.ok()) << resp.message;

  // A new reader must resolve the orphaned lock (rollback) and read through.
  client::Txn reader(&c);
  ASSERT_TRUE(reader.Begin().ok());
  std::string v;
  ASSERT_TRUE(reader.Get("k", &v).ok());
  EXPECT_EQ(v, "committed-value");
}

TEST(TxnCluster, HalfCommittedTxnRolledForwardByReader) {
  HarnessCluster cluster("txn_roll_forward");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());
  ASSERT_TRUE(c.Split("m").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  // Manually run 2PC but "crash" after committing only the primary.
  uint64_t start_ts, commit_ts;
  {
    client::Txn ts1(&c);
    ASSERT_TRUE(ts1.Begin().ok());
    start_ts = ts1.start_ts();
  }
  auto prewrite = [&](const std::string& key, const std::string& value) {
    net::Request req;
    req.type = net::MsgType::kTxnPrewrite;
    req.key = key;
    req.value = value;
    req.wop = 1;
    req.ts = start_ts;
    req.ts2 = 1;  // expired TTL so resolution does not have to wait
    req.primary = "a-key";
    net::Response resp;
    ASSERT_TRUE(c.Call(req, &resp).ok());
    ASSERT_TRUE(resp.ok()) << resp.message;
  };
  prewrite("a-key", "A");
  prewrite("z-key", "Z");
  {
    client::Txn ts2(&c);
    ASSERT_TRUE(ts2.Begin().ok());
    commit_ts = ts2.start_ts();
  }
  net::Request commit;
  commit.type = net::MsgType::kTxnCommit;
  commit.key = "a-key";
  commit.ts = start_ts;
  commit.ts2 = commit_ts;
  net::Response resp;
  ASSERT_TRUE(c.Call(commit, &resp).ok());
  ASSERT_TRUE(resp.ok()) << resp.message;
  // z-key still holds its lock: the txn is committed but half-applied.

  // A reader of z-key must discover the primary committed and roll forward.
  client::Txn reader(&c);
  ASSERT_TRUE(reader.Begin().ok());
  std::string v;
  ASSERT_TRUE(reader.Get("z-key", &v).ok());
  EXPECT_EQ(v, "Z");
  ASSERT_TRUE(reader.Get("a-key", &v).ok());
  EXPECT_EQ(v, "A");
}

TEST(TxnCluster, ConcurrentTransfersConserveTotal) {
  HarnessCluster cluster("txn_transfers");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  constexpr int kAccounts = 4;
  constexpr int kInitial = 100;
  {
    client::Txn init(&c);
    ASSERT_TRUE(init.Begin().ok());
    for (int i = 0; i < kAccounts; i++) {
      init.Put("acct" + std::to_string(i), std::to_string(kInitial));
    }
    ASSERT_TRUE(init.Commit().ok());
  }

  std::atomic<int> committed{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 3; t++) {
    threads.emplace_back([&, t] {
      client::Client tc(cluster.ClientAddrs());
      std::mt19937 rng(static_cast<unsigned>(t) + 1);
      for (int i = 0; i < 10; i++) {
        int from = static_cast<int>(rng() % kAccounts);
        int to = static_cast<int>(rng() % kAccounts);
        if (from == to) continue;
        client::Txn txn(&tc);
        if (!txn.Begin().ok()) continue;
        std::string fv, tv;
        if (!txn.Get("acct" + std::to_string(from), &fv).ok()) continue;
        if (!txn.Get("acct" + std::to_string(to), &tv).ok()) continue;
        int amount = 1 + static_cast<int>(rng() % 10);
        txn.Put("acct" + std::to_string(from), std::to_string(atoi(fv.c_str()) - amount));
        txn.Put("acct" + std::to_string(to), std::to_string(atoi(tv.c_str()) + amount));
        if (txn.Commit().ok()) committed.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_GT(committed.load(), 0);

  client::Txn audit(&c);
  ASSERT_TRUE(audit.Begin().ok());
  int total = 0;
  for (int i = 0; i < kAccounts; i++) {
    std::string v;
    ASSERT_TRUE(audit.Get("acct" + std::to_string(i), &v).ok());
    total += atoi(v.c_str());
  }
  EXPECT_EQ(total, kAccounts * kInitial)
      << "snapshot isolation must conserve the transfer total";
}

}  // namespace
}  // namespace flotilla::server
