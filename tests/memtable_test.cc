#include "storage/memtable.h"

#include <gtest/gtest.h>

#include <memory>

namespace flotilla::storage {
namespace {

TEST(MemTable, GetNewestVersion) {
  MemTable mem;
  mem.Add(1, kPut, "k", "v1");
  mem.Add(2, kPut, "k", "v2");
  mem.Add(3, kPut, "other", "x");

  Entry e;
  ASSERT_TRUE(mem.Get("k", &e));
  EXPECT_EQ(e.seqno, 2u);
  EXPECT_EQ(e.value, "v2");
  EXPECT_FALSE(mem.Get("missing", &e));
}

TEST(MemTable, TombstoneVisible) {
  MemTable mem;
  mem.Add(1, kPut, "k", "v");
  mem.Add(2, kDelete, "k", "");
  Entry e;
  ASSERT_TRUE(mem.Get("k", &e));
  EXPECT_EQ(e.op, kDelete);
  EXPECT_EQ(e.seqno, 2u);
}

TEST(MemTable, IteratorOrder) {
  MemTable mem;
  mem.Add(5, kPut, "b", "b5");
  mem.Add(3, kPut, "a", "a3");
  mem.Add(7, kPut, "a", "a7");
  mem.Add(1, kPut, "c", "c1");

  std::unique_ptr<InternalIterator> it(mem.NewIterator());
  it->SeekToFirst();
  std::vector<std::pair<std::string, uint64_t>> got;
  for (; it->Valid(); it->Next()) got.emplace_back(it->entry().key, it->entry().seqno);
  std::vector<std::pair<std::string, uint64_t>> want = {
      {"a", 7}, {"a", 3}, {"b", 5}, {"c", 1}};
  EXPECT_EQ(got, want);
}

TEST(MemTable, SeekLandsOnNewest) {
  MemTable mem;
  for (uint64_t s = 1; s <= 10; s++) mem.Add(s, kPut, "k" + std::to_string(s % 3), "v");
  std::unique_ptr<InternalIterator> it(mem.NewIterator());
  it->Seek("k1");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->entry().key, "k1");
  EXPECT_EQ(it->entry().seqno, 10u);

  it->Seek("k11");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->entry().key, "k2");

  it->Seek("zzz");
  EXPECT_FALSE(it->Valid());
}

TEST(MemTable, ManyKeysStaySorted) {
  MemTable mem;
  for (uint64_t s = 1; s <= 2000; s++) {
    mem.Add(s, kPut, "key" + std::to_string((s * 7919) % 500), "v" + std::to_string(s));
  }
  EXPECT_EQ(mem.Count(), 2000u);
  std::unique_ptr<InternalIterator> it(mem.NewIterator());
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  std::string prev_key = it->entry().key;
  uint64_t prev_seq = it->entry().seqno;
  size_t n = 1;
  for (it->Next(); it->Valid(); it->Next(), n++) {
    const Entry& e = it->entry();
    ASSERT_LT(InternalCompare(prev_key, prev_seq, e.key, e.seqno), 0);
    prev_key = e.key;
    prev_seq = e.seqno;
  }
  EXPECT_EQ(n, 2000u);
}

}  // namespace
}  // namespace flotilla::storage
