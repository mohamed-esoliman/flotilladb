#pragma once

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <string_view>

#include "storage/entry.h"

namespace flotilla::storage {

// Skiplist keyed by (user key asc, seqno desc). Not internally synchronized:
// the DB serializes writers and guarantees no concurrent write during reads
// of the same node fields it mutates (writers hold the DB mutex; readers
// access an immutable snapshot or hold the mutex briefly to pick sources).
class MemTable {
 public:
  MemTable();
  ~MemTable();

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Add(uint64_t seqno, Op op, std::string_view key, std::string_view value);

  // Returns true if the newest entry for key exists in this table, filling
  // *out (which may be a tombstone).
  bool Get(std::string_view key, Entry* out) const;

  size_t ApproximateBytes() const { return bytes_; }
  size_t Count() const { return count_; }

  InternalIterator* NewIterator() const;

 private:
  static constexpr int kMaxHeight = 12;

  struct Node;
  friend class MemTableIterator;

  Node* NewNode(const Entry& e, int height);
  int RandomHeight();
  // Returns the first node >= (key, seqno) in internal order; fills prev[].
  Node* FindGreaterOrEqual(std::string_view key, uint64_t seqno, Node** prev) const;

  Node* head_;
  int max_height_ = 1;
  size_t bytes_ = 0;
  size_t count_ = 0;
  std::mt19937 rng_{0x5eed};
};

}  // namespace flotilla::storage
