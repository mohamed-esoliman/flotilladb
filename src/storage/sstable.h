#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/entry.h"

namespace flotilla::storage {

struct TableMeta {
  uint64_t number = 0;
  int level = 0;
  std::string smallest;  // user keys
  std::string largest;
  uint64_t entries = 0;
  uint64_t file_bytes = 0;
};

struct TableOptions {
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;
};

// Writes entries (which must arrive in internal-key order) into an SSTable
// file: data blocks, sparse index, bloom filter, footer.
class TableBuilder {
 public:
  TableBuilder(std::string path, TableOptions opts);
  ~TableBuilder();

  Status Add(const Entry& e);
  Status Finish(TableMeta* meta);
  void Abandon();

  uint64_t BytesWritten() const { return offset_ + block_.size(); }
  uint64_t Entries() const { return entries_; }

 private:
  Status FlushBlock();
  Status WriteRaw(std::string_view data);

  std::string path_;
  TableOptions opts_;
  int fd_ = -1;
  Status status_;

  std::string block_;
  std::string block_first_key_;
  std::string index_;
  std::vector<std::string> keys_;  // user keys for bloom
  uint64_t offset_ = 0;
  uint64_t entries_ = 0;
  std::string smallest_, largest_;
  bool finished_ = false;
};

class Table : public std::enable_shared_from_this<Table> {
 public:
  static Status Open(const std::string& path, std::shared_ptr<Table>* out);
  ~Table();

  // Fills *out with the newest entry for key (possibly a tombstone).
  // NotFound if the table has no entry for key.
  Status Get(std::string_view key, Entry* out) const;

  InternalIterator* NewIterator() const;

  uint64_t entries() const { return num_entries_; }

 private:
  struct IndexEntry {
    std::string first_key;
    uint64_t offset;
    uint32_t size;
  };
  friend class TableIterator;

  Table() = default;
  Status ReadBlock(uint64_t off, uint32_t size, std::string* out) const;
  // Index slot of the last block whose first key is strictly < key, or -1.
  // The first entry with user key >= key is in this block or a later one
  // (a run of equal keys can cross block boundaries).
  int FindBlock(std::string_view key) const;

  int fd_ = -1;
  std::string path_;
  std::vector<IndexEntry> index_;
  std::string bloom_;
  uint64_t num_entries_ = 0;
};

}  // namespace flotilla::storage
