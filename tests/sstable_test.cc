#include "storage/sstable.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/fs.h"
#include "storage/bloom.h"
#include "testutil.h"

namespace flotilla::storage {
namespace {

Entry MakeEntry(const std::string& k, uint64_t seq, Op op, const std::string& v) {
  Entry e;
  e.key = k;
  e.seqno = seq;
  e.op = op;
  e.value = v;
  return e;
}

TEST(Bloom, NoFalseNegatives) {
  std::vector<std::string> keys;
  for (int i = 0; i < 1000; i++) keys.push_back("key" + std::to_string(i));
  std::vector<std::string_view> views(keys.begin(), keys.end());
  std::string filter = BuildBloom(views, 10);
  for (const auto& k : keys) EXPECT_TRUE(BloomMayContain(filter, k));
}

TEST(Bloom, FalsePositiveRateReasonable) {
  std::vector<std::string> keys;
  for (int i = 0; i < 1000; i++) keys.push_back("key" + std::to_string(i));
  std::vector<std::string_view> views(keys.begin(), keys.end());
  std::string filter = BuildBloom(views, 10);
  int fp = 0;
  for (int i = 0; i < 10000; i++) {
    if (BloomMayContain(filter, "absent" + std::to_string(i))) fp++;
  }
  EXPECT_LT(fp, 500);  // ~1% expected at 10 bits/key; 5% is generous
}

TEST(Bloom, MalformedFilterFailsOpen) {
  EXPECT_TRUE(BloomMayContain("", "anything"));
  EXPECT_TRUE(BloomMayContain("abc", "anything"));
}

class SSTableTest : public ::testing::Test {
 protected:
  std::shared_ptr<Table> Build(const std::vector<Entry>& entries, size_t block_size = 64) {
    test_dir_ = std::make_unique<test::TempDir>("sstable");
    std::string path = test_dir_->file("000001.sst");
    TableOptions opts;
    opts.block_size = block_size;  // tiny blocks exercise boundaries
    TableBuilder builder(path, opts);
    for (const auto& e : entries) EXPECT_TRUE(builder.Add(e).ok());
    TableMeta meta;
    EXPECT_TRUE(builder.Finish(&meta).ok());
    EXPECT_EQ(meta.entries, entries.size());
    if (!entries.empty()) {
      EXPECT_EQ(meta.smallest, entries.front().key);
      EXPECT_EQ(meta.largest, entries.back().key);
    }
    std::shared_ptr<Table> table;
    EXPECT_TRUE(Table::Open(path, &table).ok());
    return table;
  }

  std::unique_ptr<test::TempDir> test_dir_;
};

TEST_F(SSTableTest, RoundtripAndPointGets) {
  std::vector<Entry> entries;
  for (int i = 0; i < 200; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%05d", i);
    entries.push_back(MakeEntry(buf, 1000 + static_cast<uint64_t>(i), kPut,
                                "value" + std::to_string(i)));
  }
  auto table = Build(entries);
  ASSERT_EQ(table->entries(), 200u);

  Entry e;
  ASSERT_TRUE(table->Get("key00000", &e).ok());
  EXPECT_EQ(e.value, "value0");
  ASSERT_TRUE(table->Get("key00199", &e).ok());
  EXPECT_EQ(e.value, "value199");
  ASSERT_TRUE(table->Get("key00117", &e).ok());
  EXPECT_EQ(e.value, "value117");
  EXPECT_TRUE(table->Get("key99999", &e).IsNotFound());
  EXPECT_TRUE(table->Get("aaa", &e).IsNotFound());
}

TEST_F(SSTableTest, NewestVersionWinsAcrossBlockBoundaries) {
  // Many versions of one key so the run crosses several tiny blocks.
  std::vector<Entry> entries;
  entries.push_back(MakeEntry("aaa", 1, kPut, "a"));
  for (uint64_t s = 50; s >= 1; s--) {
    entries.push_back(MakeEntry("hot", s, kPut, "v" + std::to_string(s)));
  }
  entries.push_back(MakeEntry("zzz", 2, kPut, "z"));
  auto table = Build(entries);

  Entry e;
  ASSERT_TRUE(table->Get("hot", &e).ok());
  EXPECT_EQ(e.seqno, 50u);
  EXPECT_EQ(e.value, "v50");
  ASSERT_TRUE(table->Get("aaa", &e).ok());
  ASSERT_TRUE(table->Get("zzz", &e).ok());
}

TEST_F(SSTableTest, TombstonesReturned) {
  auto table = Build({MakeEntry("k", 7, kDelete, "")});
  Entry e;
  ASSERT_TRUE(table->Get("k", &e).ok());
  EXPECT_EQ(e.op, kDelete);
}

TEST_F(SSTableTest, IteratorFullScanAndSeek) {
  std::vector<Entry> entries;
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%04d", i);
    entries.push_back(MakeEntry(buf, static_cast<uint64_t>(i + 1), kPut, "v"));
  }
  auto table = Build(entries);
  std::unique_ptr<InternalIterator> it(table->NewIterator());

  it->SeekToFirst();
  size_t n = 0;
  for (; it->Valid(); it->Next()) {
    EXPECT_EQ(it->entry().key, entries[n].key);
    n++;
  }
  EXPECT_EQ(n, entries.size());

  it->Seek("k0050");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->entry().key, "k0050");
  it->Seek("k0050x");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->entry().key, "k0051");
  it->Seek("zzz");
  EXPECT_FALSE(it->Valid());
}

TEST_F(SSTableTest, OpenRejectsGarbage) {
  test::TempDir dir("sstable_garbage");
  std::string path = dir.file("bad.sst");
  ASSERT_TRUE(WriteFileAtomic(path, "this is not an sstable at all").ok());
  std::shared_ptr<Table> table;
  EXPECT_FALSE(Table::Open(path, &table).ok());
}

}  // namespace
}  // namespace flotilla::storage
