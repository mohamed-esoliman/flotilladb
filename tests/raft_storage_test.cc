#include "raft/raft_storage.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "raft/wire.h"
#include "testutil.h"

namespace flotilla::raft {
namespace {

LogEntry MakeEntry(uint64_t term, uint64_t index, const std::string& cmd) {
  LogEntry e;
  e.term = term;
  e.index = index;
  e.command = cmd;
  return e;
}

TEST(Wire, MessageRoundtrip) {
  Message m;
  m.kind = MsgKind::kAppendEntries;
  m.from = 2;
  m.to = 3;
  m.term = 7;
  m.prev_index = 41;
  m.prev_term = 6;
  m.commit = 40;
  m.hb_seq = 9;
  m.entries.push_back(MakeEntry(7, 42, "put k v"));
  m.entries.push_back(MakeEntry(7, 43, ""));

  Message got;
  ASSERT_TRUE(DecodeMessage(EncodeMessage(m), &got));
  EXPECT_EQ(got.kind, MsgKind::kAppendEntries);
  EXPECT_EQ(got.from, 2u);
  EXPECT_EQ(got.term, 7u);
  EXPECT_EQ(got.entries.size(), 2u);
  EXPECT_EQ(got.entries[0].command, "put k v");
  EXPECT_EQ(got.entries[1].index, 43u);

  Message bad;
  EXPECT_FALSE(DecodeMessage("garbage", &bad));
  EXPECT_FALSE(DecodeMessage("", &bad));
}

TEST(RaftStorage, PersistsAcrossReopen) {
  test::TempDir dir("raft_storage");
  {
    std::unique_ptr<RaftStorage> rs;
    ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
    EXPECT_EQ(rs->hard_state().term, 0u);
    EXPECT_TRUE(rs->entries().empty());

    ASSERT_TRUE(rs->SaveHardState({5, 2}).ok());
    ASSERT_TRUE(rs->Append({MakeEntry(5, 1, "a"), MakeEntry(5, 2, "b")}).ok());
    ASSERT_TRUE(rs->Append({MakeEntry(5, 3, "c")}).ok());
  }
  {
    std::unique_ptr<RaftStorage> rs;
    ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
    EXPECT_EQ(rs->hard_state().term, 5u);
    EXPECT_EQ(rs->hard_state().voted_for, 2u);
    ASSERT_EQ(rs->entries().size(), 3u);
    EXPECT_EQ(rs->entries()[2].command, "c");
  }
}

TEST(RaftStorage, TruncateFromRewrites) {
  test::TempDir dir("raft_truncate");
  std::unique_ptr<RaftStorage> rs;
  ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
  ASSERT_TRUE(
      rs->Append({MakeEntry(1, 1, "a"), MakeEntry(1, 2, "b"), MakeEntry(1, 3, "c")}).ok());
  ASSERT_TRUE(rs->TruncateFrom(2).ok());
  ASSERT_TRUE(rs->Append({MakeEntry(2, 2, "b2")}).ok());

  std::unique_ptr<RaftStorage> rs2;
  rs.reset();
  ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs2).ok());
  ASSERT_EQ(rs2->entries().size(), 2u);
  EXPECT_EQ(rs2->entries()[1].command, "b2");
  EXPECT_EQ(rs2->entries()[1].term, 2u);
}

TEST(RaftStorage, TornLogTailDropped) {
  test::TempDir dir("raft_torn");
  {
    std::unique_ptr<RaftStorage> rs;
    ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
    ASSERT_TRUE(rs->Append({MakeEntry(1, 1, "a"), MakeEntry(1, 2, "b")}).ok());
  }
  auto log = std::filesystem::path(dir.path()) / "log";
  auto size = std::filesystem::file_size(log);
  std::filesystem::resize_file(log, size - 2);

  std::unique_ptr<RaftStorage> rs;
  ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
  ASSERT_EQ(rs->entries().size(), 1u);
  EXPECT_EQ(rs->entries()[0].command, "a");
  ASSERT_TRUE(rs->Append({MakeEntry(1, 2, "b2")}).ok());
}

TEST(RaftStorage, SnapshotCompactsLog) {
  test::TempDir dir("raft_snap");
  {
    std::unique_ptr<RaftStorage> rs;
    ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
    std::vector<LogEntry> entries;
    for (uint64_t i = 1; i <= 10; i++) entries.push_back(MakeEntry(1, i, "c"));
    ASSERT_TRUE(rs->Append(entries).ok());

    Snapshot snap;
    snap.last_index = 7;
    snap.last_term = 1;
    snap.data = "machine-state";
    ASSERT_TRUE(rs->SaveSnapshot(snap, false).ok());
    ASSERT_EQ(rs->entries().size(), 3u);
    EXPECT_EQ(rs->entries()[0].index, 8u);
  }
  {
    std::unique_ptr<RaftStorage> rs;
    ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
    EXPECT_EQ(rs->snapshot().last_index, 7u);
    EXPECT_EQ(rs->snapshot().data, "machine-state");
    ASSERT_EQ(rs->entries().size(), 3u);
    EXPECT_EQ(rs->entries()[0].index, 8u);
  }
}

TEST(RaftStorage, PersistAppliesReadyInOrder) {
  test::TempDir dir("raft_ready");
  std::unique_ptr<RaftStorage> rs;
  ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs).ok());
  ASSERT_TRUE(rs->Append({MakeEntry(1, 1, "a"), MakeEntry(1, 2, "b")}).ok());

  Ready ready;
  ready.truncate_from = 2;
  ready.entries_to_append = {MakeEntry(2, 2, "b2"), MakeEntry(2, 3, "c")};
  ready.hard_state_changed = true;
  ready.hard_state = {2, 3};
  ASSERT_TRUE(rs->Persist(ready).ok());

  std::unique_ptr<RaftStorage> rs2;
  rs.reset();
  ASSERT_TRUE(RaftStorage::Open(dir.path(), &rs2).ok());
  EXPECT_EQ(rs2->hard_state().term, 2u);
  ASSERT_EQ(rs2->entries().size(), 3u);
  EXPECT_EQ(rs2->entries()[1].command, "b2");
}

}  // namespace
}  // namespace flotilla::raft
