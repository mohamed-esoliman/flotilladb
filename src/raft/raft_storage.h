#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "raft/raft.h"

namespace flotilla::raft {

// Durable raft state in one directory:
//   state    term + voted_for, atomic rewrite, fsynced before use
//   snap     snapshot metadata + data, atomic rewrite
//   log      append-only entry records with CRC, fsynced per batch;
//            rewritten (tmp + rename) on truncation or compaction
// Entries are also kept in memory (the log is bounded by snapshot cadence).
class RaftStorage {
 public:
  static Status Open(const std::string& dir, std::unique_ptr<RaftStorage>* out);
  ~RaftStorage();

  const HardState& hard_state() const { return hard_; }
  const Snapshot& snapshot() const { return snap_; }
  const std::vector<LogEntry>& entries() const { return entries_; }

  Status SaveHardState(const HardState& hs);
  Status Append(const std::vector<LogEntry>& entries);
  // Drops entries with index >= index.
  Status TruncateFrom(uint64_t index);
  // Persists the snapshot and drops entries with index <= snap.last_index.
  // With drop_all, the whole log is cleared (follower InstallSnapshot).
  Status SaveSnapshot(const Snapshot& snap, bool drop_all);

  // Executes the persistence part of a Ready in the required order:
  // snapshot, truncate, append, hard state. Call before sending messages.
  Status Persist(const Ready& ready);

 private:
  explicit RaftStorage(std::string dir) : dir_(std::move(dir)) {}

  Status Load();
  Status RewriteLog();
  Status OpenLogForAppend();
  std::string Path(const char* name) const;

  std::string dir_;
  HardState hard_;
  Snapshot snap_;
  std::vector<LogEntry> entries_;
  int log_fd_ = -1;
};

}  // namespace flotilla::raft
