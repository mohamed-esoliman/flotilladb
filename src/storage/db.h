#pragma once

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/entry.h"
#include "storage/manifest.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"

namespace flotilla::storage {

struct Options {
  size_t write_buffer_size = 4 << 20;
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;
  int l0_compaction_trigger = 4;
  uint64_t level_base_bytes = 8 << 20;   // max total bytes for L1; x10 per level
  uint64_t max_output_file_bytes = 2 << 20;
  bool fsync_writes = true;
};

// User-facing ordered scan over live keys (newest versions, tombstones hidden).
class Iterator {
 public:
  virtual ~Iterator() = default;
  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void Seek(std::string_view key) = 0;
  virtual void Next() = 0;
  virtual std::string_view key() const = 0;
  virtual std::string_view value() const = 0;
};

class DB {
 public:
  static Status Open(const Options& options, const std::string& dir,
                     std::unique_ptr<DB>* out);
  ~DB();

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  Status Get(std::string_view key, std::string* value);
  std::unique_ptr<Iterator> NewIterator();

  // Flushes the memtable and waits until all immutable memtables hit L0.
  Status Flush();

  // Writes a self-contained copy of the database (hard-linked SSTables +
  // manifest) into dir, which must not exist. The copy opens like any DB.
  Status Checkpoint(const std::string& dir);

  uint64_t LastSequence();

  struct Stats {
    uint64_t last_seq;
    size_t memtable_bytes;
    size_t immutable_count;
    std::vector<size_t> files_per_level;
  };
  Stats GetStats();

 private:
  struct TableHandle {
    TableMeta meta;
    std::shared_ptr<Table> table;
  };
  // Immutable snapshot of the file layout; replaced wholesale on change.
  struct Version {
    std::vector<std::vector<TableHandle>> levels;  // levels[0] sorted by number desc
  };
  struct Compaction {
    int output_level;
    std::vector<TableHandle> inputs;  // all input tables, any level
  };

  DB(Options options, std::string dir);

  Status Recover();
  Status WriteImpl(Op op, std::string_view key, std::string_view value);
  Status SwitchMemtable();  // requires mutex_
  void BackgroundLoop();
  void FlushOne(std::unique_lock<std::mutex>& lock);
  bool PickCompaction(Compaction* c);  // requires mutex_
  void DoCompaction(const Compaction& c, std::unique_lock<std::mutex>& lock);
  Status BuildTableFromIterator(InternalIterator* iter, int output_level,
                                const Version& base, uint64_t max_bytes,
                                std::vector<TableHandle>* outputs);
  ManifestData CurrentManifestData();  // requires mutex_
  uint64_t MaxLevelBytes(int level) const;
  static bool KeyMayExistBelow(const Version& v, int below_level, std::string_view key);
  static void SortLevel(std::vector<TableHandle>* files, int level);

  const Options options_;
  const std::string dir_;

  std::mutex mutex_;
  std::condition_variable cv_;
  Status bg_error_;
  bool shutdown_ = false;

  uint64_t next_file_ = 1;
  uint64_t last_seq_ = 0;

  std::shared_ptr<MemTable> mem_;
  std::unique_ptr<WalWriter> wal_;
  uint64_t wal_number_ = 0;
  struct ImmEntry {
    std::shared_ptr<MemTable> mem;
    uint64_t wal_number;
  };
  std::deque<ImmEntry> imm_;

  std::shared_ptr<const Version> version_;
  size_t compact_ptr_[8] = {};

  std::thread bg_thread_;
};

}  // namespace flotilla::storage
