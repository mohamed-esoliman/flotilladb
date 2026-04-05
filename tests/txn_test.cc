#include <gtest/gtest.h>

#include "storage/db.h"
#include "testutil.h"
#include "txn/codec.h"
#include "txn/txn_ops.h"

namespace flotilla::txn {
namespace {

TEST(TxnCodec, EscapePreservesOrder) {
  std::vector<std::string> keys = {"", std::string("a\0b", 3), "a", "ab",
                                   std::string("a\0", 2), "b", std::string("\0", 1)};
  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& k : keys) pairs.emplace_back(EscapeKey(k), k);
  std::sort(pairs.begin(), pairs.end());
  for (size_t i = 1; i < pairs.size(); i++) {
    EXPECT_LT(pairs[i - 1].second, pairs[i].second)
        << "escape encoding must preserve key order";
  }
  for (const auto& [escaped, original] : pairs) {
    std::string decoded;
    size_t consumed = 0;
    ASSERT_TRUE(UnescapeKey(escaped, &decoded, &consumed));
    EXPECT_EQ(decoded, original);
    EXPECT_EQ(consumed, escaped.size());
  }
}

TEST(TxnCodec, WriteKeysSortNewestFirst) {
  std::string k10 = WriteKey("k", 10);
  std::string k20 = WriteKey("k", 20);
  std::string other = WriteKey("k0", 5);
  EXPECT_LT(k20, k10);   // newer commit sorts first
  EXPECT_LT(k10, other);  // and stays within its user key

  std::string user_key;
  uint64_t ts;
  ASSERT_TRUE(ParseWriteKey(k20, &user_key, &ts));
  EXPECT_EQ(user_key, "k");
  EXPECT_EQ(ts, 20u);
}

TEST(TxnCodec, RecordRoundtrips) {
  LockRecord lock;
  lock.start_ts = 42;
  lock.op = 2;
  lock.wall_ms = 123456;
  lock.primary = "pk";
  LockRecord lock2;
  ASSERT_TRUE(DecodeLock(EncodeLock(lock), &lock2));
  EXPECT_EQ(lock2.start_ts, 42u);
  EXPECT_EQ(lock2.op, 2);
  EXPECT_EQ(lock2.primary, "pk");

  WriteRecord write;
  write.kind = kWriteDelete;
  write.start_ts = 7;
  WriteRecord write2;
  ASSERT_TRUE(DecodeWrite(EncodeWrite(write), &write2));
  EXPECT_EQ(write2.kind, kWriteDelete);
  EXPECT_EQ(write2.start_ts, 7u);
}

class TxnOpsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::make_unique<test::TempDir>("txn_ops");
    storage::Options opts;
    opts.fsync_writes = false;
    ASSERT_TRUE(storage::DB::Open(opts, dir_->path(), &db_).ok());
  }

  std::string MustGet(const std::string& key, uint64_t ts) {
    TxnReadResult result;
    EXPECT_TRUE(TxnGet(db_.get(), key, ts, &result).ok());
    EXPECT_FALSE(result.locked);
    EXPECT_TRUE(result.found);
    return result.value;
  }

  bool NotFoundAt(const std::string& key, uint64_t ts) {
    TxnReadResult result;
    EXPECT_TRUE(TxnGet(db_.get(), key, ts, &result).ok());
    return !result.locked && !result.found;
  }

  std::unique_ptr<test::TempDir> dir_;
  std::unique_ptr<storage::DB> db_;
};

TEST_F(TxnOpsTest, PrewriteCommitReadCycle) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v1", 1, 10, "k", 0).ok());
  // Reads at or after start_ts are blocked by the lock.
  TxnReadResult result;
  ASSERT_TRUE(TxnGet(db_.get(), "k", 15, &result).ok());
  EXPECT_TRUE(result.locked);
  // Reads below the lock's start_ts pass through.
  EXPECT_TRUE(NotFoundAt("k", 5));

  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 20).ok());
  EXPECT_EQ(MustGet("k", 25), "v1");
  EXPECT_TRUE(NotFoundAt("k", 15));  // committed at 20, invisible at 15
}

TEST_F(TxnOpsTest, SnapshotVersions) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "old", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 11).ok());
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "new", 1, 20, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 20, 21).ok());

  EXPECT_EQ(MustGet("k", 15), "old");
  EXPECT_EQ(MustGet("k", 21), "new");
  EXPECT_EQ(MustGet("k", 100), "new");
  EXPECT_TRUE(NotFoundAt("k", 10));
}

TEST_F(TxnOpsTest, DeleteVersions) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 11).ok());
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "", 2, 20, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 20, 21).ok());

  EXPECT_EQ(MustGet("k", 15), "v");
  EXPECT_TRUE(NotFoundAt("k", 25));
}

TEST_F(TxnOpsTest, WriteWriteConflict) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "a", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 15).ok());
  // A txn that began before the commit must fail its prewrite.
  Status s = ApplyPrewrite(db_.get(), "k", "b", 1, 12, "k", 0);
  EXPECT_TRUE(s.IsConflict()) << s.ToString();
}

TEST_F(TxnOpsTest, LockConflictAndIdempotentReplay) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "a", 1, 10, "k", 0).ok());
  EXPECT_TRUE(ApplyPrewrite(db_.get(), "k", "b", 1, 12, "other", 0).IsConflict());
  // Replaying our own prewrite is fine.
  EXPECT_TRUE(ApplyPrewrite(db_.get(), "k", "a", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 20).ok());
  // Replaying the commit is fine too.
  EXPECT_TRUE(ApplyCommit(db_.get(), "k", 10, 20).ok());
}

TEST_F(TxnOpsTest, RollbackBlocksLatePrewriteAndCommit) {
  ASSERT_TRUE(ApplyRollback(db_.get(), "k", 10).ok());
  EXPECT_TRUE(ApplyPrewrite(db_.get(), "k", "v", 1, 10, "k", 0).IsAborted());
  EXPECT_TRUE(ApplyCommit(db_.get(), "k", 10, 20).IsAborted());
  // Other transactions are unaffected.
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v", 1, 30, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 30, 31).ok());
  EXPECT_EQ(MustGet("k", 40), "v");
}

TEST_F(TxnOpsTest, RollbackRemovesLockAndData) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyRollback(db_.get(), "k", 10).ok());
  EXPECT_TRUE(NotFoundAt("k", 100));
  // Rolling back a committed txn is refused.
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v2", 1, 20, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 20, 21).ok());
  EXPECT_TRUE(ApplyRollback(db_.get(), "k", 20).IsConflict());
}

TEST_F(TxnOpsTest, FindTxnOutcome) {
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k", "v", 1, 10, "k", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k", 10, 20).ok());
  ASSERT_TRUE(ApplyRollback(db_.get(), "k", 30).ok());

  uint64_t commit_ts = 0;
  bool rolled_back = false;
  ASSERT_TRUE(FindTxnOutcome(db_.get(), "k", 10, &commit_ts, &rolled_back).ok());
  EXPECT_EQ(commit_ts, 20u);
  EXPECT_FALSE(rolled_back);

  ASSERT_TRUE(FindTxnOutcome(db_.get(), "k", 30, &commit_ts, &rolled_back).ok());
  EXPECT_EQ(commit_ts, 0u);
  EXPECT_TRUE(rolled_back);

  ASSERT_TRUE(FindTxnOutcome(db_.get(), "k", 99, &commit_ts, &rolled_back).ok());
  EXPECT_EQ(commit_ts, 0u);
  EXPECT_FALSE(rolled_back);
}

TEST_F(TxnOpsTest, ScanAtSnapshot) {
  for (int i = 0; i < 5; i++) {
    std::string key = "k" + std::to_string(i);
    ASSERT_TRUE(ApplyPrewrite(db_.get(), key, "v" + std::to_string(i), 1, 10, "k0", 0).ok());
    ASSERT_TRUE(ApplyCommit(db_.get(), key, 10, 11).ok());
  }
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k2", "", 2, 20, "k2", 0).ok());
  ASSERT_TRUE(ApplyCommit(db_.get(), "k2", 20, 21).ok());

  std::vector<std::pair<std::string, std::string>> rows;
  bool blocked = false;
  LockRecord lock;
  std::string blocking_key;
  ASSERT_TRUE(
      TxnScan(db_.get(), "", "", 15, 100, &rows, &blocked, &lock, &blocking_key).ok());
  EXPECT_FALSE(blocked);
  EXPECT_EQ(rows.size(), 5u);

  rows.clear();
  ASSERT_TRUE(
      TxnScan(db_.get(), "", "", 25, 100, &rows, &blocked, &lock, &blocking_key).ok());
  EXPECT_EQ(rows.size(), 4u);  // k2 deleted at 21

  // A pending lock blocks the scan.
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "k3", "x", 1, 30, "k3", 0).ok());
  rows.clear();
  ASSERT_TRUE(
      TxnScan(db_.get(), "", "", 35, 100, &rows, &blocked, &lock, &blocking_key).ok());
  EXPECT_TRUE(blocked);
  EXPECT_EQ(blocking_key, "k3");
  EXPECT_EQ(lock.start_ts, 30u);
}

TEST_F(TxnOpsTest, ScanSeesLockOnNeverCommittedKey) {
  // A key with a lock but no write history must still block the scan.
  ASSERT_TRUE(ApplyPrewrite(db_.get(), "fresh", "x", 1, 10, "fresh", 0).ok());
  std::vector<std::pair<std::string, std::string>> rows;
  bool blocked = false;
  LockRecord lock;
  std::string blocking_key;
  ASSERT_TRUE(
      TxnScan(db_.get(), "", "", 15, 100, &rows, &blocked, &lock, &blocking_key).ok());
  EXPECT_TRUE(blocked);
  EXPECT_EQ(blocking_key, "fresh");
}

}  // namespace
}  // namespace flotilla::txn
