#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "client/client.h"
#include "cluster_harness.h"

namespace flotilla::server {
namespace {

using ShardCluster = testing::HarnessCluster;

TEST(Shard, SplitLiveRangeKeepsAllData) {
  ShardCluster cluster("shard_split");
  ASSERT_NE(cluster.WaitForLeader(), 0);

  client::Client c(cluster.ClientAddrs());
  for (int i = 0; i < 60; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%03d", i);
    ASSERT_TRUE(c.Put(buf, "v" + std::to_string(i)).ok());
  }

  ASSERT_TRUE(c.Split("key030").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  std::vector<std::pair<std::string, std::string>> ranges;
  ASSERT_TRUE(c.Ranges(&ranges).ok());
  ASSERT_EQ(ranges.size(), 2u);

  // Every key is still readable and writable, on both sides of the split.
  std::string v;
  ASSERT_TRUE(c.Get("key000", &v).ok());
  EXPECT_EQ(v, "v0");
  ASSERT_TRUE(c.Get("key030", &v).ok());
  EXPECT_EQ(v, "v30");
  ASSERT_TRUE(c.Get("key059", &v).ok());
  EXPECT_EQ(v, "v59");
  ASSERT_TRUE(c.Put("key010", "updated-left").ok());
  ASSERT_TRUE(c.Put("key045", "updated-right").ok());
  ASSERT_TRUE(c.Get("key010", &v).ok());
  EXPECT_EQ(v, "updated-left");
  ASSERT_TRUE(c.Get("key045", &v).ok());
  EXPECT_EQ(v, "updated-right");

  // Scans span both ranges in order.
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 60u);
  EXPECT_EQ(rows.front().first, "key000");
  EXPECT_EQ(rows.back().first, "key059");
  for (size_t i = 1; i < rows.size(); i++) EXPECT_GT(rows[i].first, rows[i - 1].first);

  rows.clear();
  ASSERT_TRUE(c.Scan("key025", "key035", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 10u);
  EXPECT_EQ(rows.front().first, "key025");
  EXPECT_EQ(rows.back().first, "key034");
}

TEST(Shard, MultipleSplitsAndDeepScan) {
  ShardCluster cluster("shard_multi");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  for (int i = 0; i < 90; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "m%03d", i);
    ASSERT_TRUE(c.Put(buf, std::to_string(i)).ok());
  }
  ASSERT_TRUE(c.Split("m030").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Split("m060").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(3));
  // Split the middle range again.
  ASSERT_TRUE(c.Split("m045").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(4));

  std::vector<std::pair<std::string, std::string>> ranges;
  ASSERT_TRUE(c.Ranges(&ranges).ok());
  EXPECT_EQ(ranges.size(), 4u);

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 90u);
  for (size_t i = 1; i < rows.size(); i++) EXPECT_GT(rows[i].first, rows[i - 1].first);

  std::string v;
  ASSERT_TRUE(c.Get("m044", &v).ok());
  EXPECT_EQ(v, "44");
  ASSERT_TRUE(c.Get("m046", &v).ok());
  EXPECT_EQ(v, "46");

  // Invalid splits are rejected.
  EXPECT_FALSE(c.Split("m030").ok());  // range start
}

TEST(Shard, SplitSurvivesNodeFailureAndRestart) {
  ShardCluster cluster("shard_failover");
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  client::Client c(cluster.ClientAddrs());

  for (int i = 0; i < 40; i++) {
    ASSERT_TRUE(c.Put("s" + std::to_string(100 + i), "v" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(c.Split("s120").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Put("s110", "left").ok());
  ASSERT_TRUE(c.Put("s130", "right").ok());

  // Kill a node entirely; both groups must keep serving.
  cluster.StopNode(leader);
  ASSERT_NE(cluster.WaitForLeader(10000), 0);
  std::string v;
  ASSERT_TRUE(c.Get("s110", &v).ok());
  EXPECT_EQ(v, "left");
  ASSERT_TRUE(c.Get("s130", &v).ok());
  EXPECT_EQ(v, "right");
  ASSERT_TRUE(c.Put("s135", "while-down").ok());

  // Restarted node must rebuild both groups from disk.
  cluster.StartNode(leader);
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Get("s135", &v).ok());
  EXPECT_EQ(v, "while-down");

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("s1", "s2", 0, &rows).ok());
  EXPECT_EQ(rows.size(), 40u);  // s100..s139; later writes hit existing keys
}

TEST(Shard, GroupsCanHaveDifferentLeaders) {
  ShardCluster cluster("shard_leaders");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());
  ASSERT_TRUE(c.Put("a1", "x").ok());
  ASSERT_TRUE(c.Put("z1", "y").ok());
  ASSERT_TRUE(c.Split("n").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  // Regardless of which node leads which group, ops on both sides work when
  // pointed at any single node (server redirects per group).
  for (int node = 1; node <= 3; node++) {
    client::Client single(
        {cluster.config_.nodes[static_cast<size_t>(node - 1)].client_addr});
    std::string v;
    ASSERT_TRUE(single.Put("a-n" + std::to_string(node), "1").ok());
    ASSERT_TRUE(single.Put("z-n" + std::to_string(node), "2").ok());
    ASSERT_TRUE(single.Get("a1", &v).ok());
    ASSERT_TRUE(single.Get("z1", &v).ok());
  }
}

}  // namespace
}  // namespace flotilla::server
