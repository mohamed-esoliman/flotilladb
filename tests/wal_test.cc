#include "storage/wal.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "testutil.h"

namespace flotilla::storage {
namespace {

Entry MakeEntry(uint64_t seq, Op op, const std::string& k, const std::string& v) {
  Entry e;
  e.seqno = seq;
  e.op = op;
  e.key = k;
  e.value = v;
  return e;
}

std::string ReadRaw(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), {});
}

void WriteRaw(const std::string& path, const std::string& data) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << data;
}

TEST(Wal, RoundtripAndAppendAcrossReopens) {
  test::TempDir dir("wal_roundtrip");
  std::string path = dir.file("000001.wal");

  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    ASSERT_TRUE(w->Append(MakeEntry(1, kPut, "a", "1")).ok());
    ASSERT_TRUE(w->Append(MakeEntry(2, kDelete, "a", "")).ok());
    ASSERT_TRUE(w->Sync().ok());
  }
  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    ASSERT_TRUE(w->Append(MakeEntry(3, kPut, "b", "big value here")).ok());
    ASSERT_TRUE(w->Sync().ok());
  }

  std::vector<Entry> entries;
  ASSERT_TRUE(RecoverWal(path, &entries).ok());
  ASSERT_EQ(entries.size(), 3u);
  EXPECT_EQ(entries[0].seqno, 1u);
  EXPECT_EQ(entries[0].key, "a");
  EXPECT_EQ(entries[0].value, "1");
  EXPECT_EQ(entries[1].op, kDelete);
  EXPECT_EQ(entries[2].key, "b");
  EXPECT_EQ(entries[2].value, "big value here");
}

TEST(Wal, MissingFileIsEmpty) {
  test::TempDir dir("wal_missing");
  std::vector<Entry> entries;
  ASSERT_TRUE(RecoverWal(dir.file("nope.wal"), &entries).ok());
  EXPECT_TRUE(entries.empty());
}

TEST(Wal, TornTailTruncatedAtEveryByte) {
  test::TempDir dir("wal_torn");
  std::string path = dir.file("000001.wal");
  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    ASSERT_TRUE(w->Append(MakeEntry(1, kPut, "key1", "value1")).ok());
    ASSERT_TRUE(w->Append(MakeEntry(2, kPut, "key2", "value2")).ok());
    ASSERT_TRUE(w->Sync().ok());
  }
  std::string full = ReadRaw(path);
  std::vector<Entry> all;
  ASSERT_TRUE(RecoverWal(path, &all).ok());
  ASSERT_EQ(all.size(), 2u);

  // Find the boundary of record 1 by recovering prefixes: any cut strictly
  // inside a record must recover exactly the records before it.
  for (size_t cut = 0; cut < full.size(); cut++) {
    WriteRaw(path, full.substr(0, cut));
    std::vector<Entry> got;
    ASSERT_TRUE(RecoverWal(path, &got).ok()) << "cut=" << cut;
    ASSERT_LE(got.size(), 2u);
    for (size_t i = 0; i < got.size(); i++) EXPECT_EQ(got[i].seqno, all[i].seqno);
    // The file must have been truncated to a clean boundary: recovering again
    // yields the same result and appending works.
    std::vector<Entry> again;
    ASSERT_TRUE(RecoverWal(path, &again).ok());
    EXPECT_EQ(again.size(), got.size());
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    ASSERT_TRUE(w->Append(MakeEntry(9, kPut, "x", "y")).ok());
    ASSERT_TRUE(w->Sync().ok());
    std::vector<Entry> after;
    ASSERT_TRUE(RecoverWal(path, &after).ok());
    ASSERT_EQ(after.size(), got.size() + 1);
    EXPECT_EQ(after.back().seqno, 9u);
  }
}

TEST(Wal, CorruptMiddleStopsRecovery) {
  test::TempDir dir("wal_corrupt");
  std::string path = dir.file("000001.wal");
  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    ASSERT_TRUE(w->Append(MakeEntry(1, kPut, "aaaa", "1111")).ok());
    ASSERT_TRUE(w->Append(MakeEntry(2, kPut, "bbbb", "2222")).ok());
    ASSERT_TRUE(w->Sync().ok());
  }
  std::string full = ReadRaw(path);
  // Flip a byte inside the second record's payload.
  std::string corrupted = full;
  corrupted[corrupted.size() - 2] ^= 0xFF;
  WriteRaw(path, corrupted);

  std::vector<Entry> got;
  ASSERT_TRUE(RecoverWal(path, &got).ok());
  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0].key, "aaaa");
}

}  // namespace
}  // namespace flotilla::storage
