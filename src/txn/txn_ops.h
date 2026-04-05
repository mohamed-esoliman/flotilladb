#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/db.h"
#include "txn/codec.h"

namespace flotilla::txn {

// State-machine side of the Percolator protocol. Every function is a
// deterministic function of the storage state and its arguments; they run at
// apply time on every replica and must produce identical results. Returned
// statuses are logical outcomes (Conflict/Aborted/...), not IO failures: IO
// failures surface through the returned status too and the caller treats
// Corruption/IOError as fatal.

// Writes lock + data. Conflicts: newer committed write, rollback marker at
// start_ts, or a different transaction's lock.
Status ApplyPrewrite(storage::DB* db, std::string_view key, std::string_view value,
                     uint8_t op, uint64_t start_ts, std::string_view primary,
                     uint64_t wall_ms);

// Replaces the lock with a write record at commit_ts. Idempotent for
// replayed commits; Aborted if the lock vanished without a commit.
Status ApplyCommit(storage::DB* db, std::string_view key, uint64_t start_ts,
                   uint64_t commit_ts);

// Removes lock + data and leaves a rollback marker. Conflict if the txn
// already committed.
Status ApplyRollback(storage::DB* db, std::string_view key, uint64_t start_ts);

struct TxnReadResult {
  bool found = false;
  std::string value;
  // Set when blocked by a lock:
  bool locked = false;
  LockRecord lock;
};

// Snapshot read at ts. Sets locked (with the lock) instead of failing so the
// caller can drive resolution.
Status TxnGet(storage::DB* db, std::string_view key, uint64_t ts, TxnReadResult* out);

// Snapshot range scan at ts over [start, end) user keys, up to limit rows.
// A blocking lock aborts the scan and reports it via *blocked.
Status TxnScan(storage::DB* db, std::string_view start, std::string_view end,
               uint64_t ts, uint32_t limit,
               std::vector<std::pair<std::string, std::string>>* rows, bool* blocked,
               LockRecord* blocking_lock, std::string* blocking_key);

// Reads the lock on key, if any.
Status GetLock(storage::DB* db, std::string_view key, bool* has_lock, LockRecord* lock);

// Finds the commit record whose start_ts matches (for resolve): returns the
// commit_ts, or 0 if none, and whether a rollback marker exists at start_ts.
Status FindTxnOutcome(storage::DB* db, std::string_view key, uint64_t start_ts,
                      uint64_t* commit_ts, bool* rolled_back);

}  // namespace flotilla::txn
