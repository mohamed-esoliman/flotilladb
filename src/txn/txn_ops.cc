#include "txn/txn_ops.h"

#include <functional>

namespace flotilla::txn {

namespace {

// Iterates write records for one user key, newest first, starting at ts.
// Callback returns true to continue.
Status ForEachWriteAtOrBelow(storage::DB* db, std::string_view key, uint64_t ts,
                             const std::function<bool(uint64_t, const WriteRecord&)>& fn) {
  std::string seek = WriteKey(key, ts);
  auto it = db->NewIterator();
  for (it->Seek(seek); it->Valid(); it->Next()) {
    std::string user_key;
    uint64_t commit_ts;
    if (!ParseWriteKey(it->key(), &user_key, &commit_ts)) break;
    if (user_key != key) break;
    WriteRecord rec;
    if (!DecodeWrite(it->value(), &rec)) {
      return Status::Corruption("bad write record");
    }
    if (!fn(commit_ts, rec)) break;
  }
  return Status::OK();
}

}  // namespace

Status GetLock(storage::DB* db, std::string_view key, bool* has_lock, LockRecord* lock) {
  std::string raw;
  Status s = db->Get(LockKey(key), &raw);
  if (s.IsNotFound()) {
    *has_lock = false;
    return Status::OK();
  }
  if (!s.ok()) return s;
  if (!DecodeLock(raw, lock)) return Status::Corruption("bad lock record");
  *has_lock = true;
  return Status::OK();
}

Status ApplyPrewrite(storage::DB* db, std::string_view key, std::string_view value,
                     uint8_t op, uint64_t start_ts, std::string_view primary,
                     uint64_t wall_ms) {
  // Write-write conflict: any commit at or after start_ts.
  uint64_t newest_commit = 0;
  bool rollback_here = false;
  Status s = ForEachWriteAtOrBelow(db, key, UINT64_MAX,
                                   [&](uint64_t commit_ts, const WriteRecord& rec) {
                                     if (rec.kind == kWriteRollback) {
                                       if (rec.start_ts == start_ts) rollback_here = true;
                                       return commit_ts > start_ts;  // keep looking
                                     }
                                     newest_commit = std::max(newest_commit, commit_ts);
                                     return false;
                                   });
  if (!s.ok()) return s;
  if (rollback_here) return Status::Aborted("transaction rolled back");
  if (newest_commit >= start_ts) {
    return Status::Conflict("write conflict: committed at " +
                            std::to_string(newest_commit));
  }

  bool has_lock = false;
  LockRecord existing;
  s = GetLock(db, key, &has_lock, &existing);
  if (!s.ok()) return s;
  if (has_lock) {
    if (existing.start_ts == start_ts) return Status::OK();  // replayed prewrite
    return Status::Conflict("locked by txn " + std::to_string(existing.start_ts) +
                            " primary " + existing.primary);
  }

  LockRecord lock;
  lock.start_ts = start_ts;
  lock.op = op;
  lock.wall_ms = wall_ms;
  lock.primary = std::string(primary);
  s = db->Put(LockKey(key), EncodeLock(lock));
  if (s.ok() && op == 1) s = db->Put(DataKey(key, start_ts), value);
  return s;
}

Status ApplyCommit(storage::DB* db, std::string_view key, uint64_t start_ts,
                   uint64_t commit_ts) {
  bool has_lock = false;
  LockRecord lock;
  Status s = GetLock(db, key, &has_lock, &lock);
  if (!s.ok()) return s;

  if (!has_lock || lock.start_ts != start_ts) {
    // Replayed commit or lost lock: consult history.
    uint64_t existing_commit = 0;
    bool rolled_back = false;
    s = FindTxnOutcome(db, key, start_ts, &existing_commit, &rolled_back);
    if (!s.ok()) return s;
    if (existing_commit != 0) return Status::OK();  // idempotent
    if (rolled_back) return Status::Aborted("transaction rolled back");
    return Status::Aborted("lock lost before commit");
  }

  WriteRecord rec;
  rec.kind = lock.op == 2 ? kWriteDelete : kWritePut;
  rec.start_ts = start_ts;
  s = db->Put(WriteKey(key, commit_ts), EncodeWrite(rec));
  if (s.ok()) s = db->Delete(LockKey(key));
  return s;
}

Status ApplyRollback(storage::DB* db, std::string_view key, uint64_t start_ts) {
  bool has_lock = false;
  LockRecord lock;
  Status s = GetLock(db, key, &has_lock, &lock);
  if (!s.ok()) return s;

  uint64_t existing_commit = 0;
  bool rolled_back = false;
  s = FindTxnOutcome(db, key, start_ts, &existing_commit, &rolled_back);
  if (!s.ok()) return s;
  if (existing_commit != 0) {
    return Status::Conflict("transaction already committed");
  }

  if (has_lock && lock.start_ts == start_ts) {
    s = db->Delete(LockKey(key));
    if (s.ok()) s = db->Delete(DataKey(key, start_ts));
    if (!s.ok()) return s;
  }
  if (rolled_back) return Status::OK();
  WriteRecord rec;
  rec.kind = kWriteRollback;
  rec.start_ts = start_ts;
  // Rollback markers use start_ts as their commit_ts slot.
  return db->Put(WriteKey(key, start_ts), EncodeWrite(rec));
}

Status TxnGet(storage::DB* db, std::string_view key, uint64_t ts, TxnReadResult* out) {
  *out = TxnReadResult{};
  bool has_lock = false;
  LockRecord lock;
  Status s = GetLock(db, key, &has_lock, &lock);
  if (!s.ok()) return s;
  if (has_lock && lock.start_ts <= ts) {
    out->locked = true;
    out->lock = lock;
    return Status::OK();
  }

  uint64_t data_ts = 0;
  bool deleted = false;
  s = ForEachWriteAtOrBelow(db, key, ts, [&](uint64_t, const WriteRecord& rec) {
    if (rec.kind == kWriteRollback) return true;
    if (rec.kind == kWriteDelete) {
      deleted = true;
    } else {
      data_ts = rec.start_ts;
    }
    return false;
  });
  if (!s.ok()) return s;
  if (deleted || data_ts == 0) return Status::OK();  // not found at ts

  s = db->Get(DataKey(key, data_ts), &out->value);
  if (s.IsNotFound()) return Status::Corruption("missing data record");
  if (!s.ok()) return s;
  out->found = true;
  return Status::OK();
}

Status TxnScan(storage::DB* db, std::string_view start, std::string_view end,
               uint64_t ts, uint32_t limit,
               std::vector<std::pair<std::string, std::string>>* rows, bool* blocked,
               LockRecord* blocking_lock, std::string* blocking_key) {
  *blocked = false;
  // Lock pre-pass: a key that has only ever been prewritten has no write
  // records, so the write-column walk below would never see its lock.
  {
    std::string seek = std::string(kLockPrefix) + EscapeKey(start);
    seek.resize(seek.size() - 2);
    auto it = db->NewIterator();
    for (it->Seek(seek); it->Valid(); it->Next()) {
      std::string_view key = it->key();
      if (key.substr(0, 2) != kLockPrefix) break;
      std::string user_key;
      size_t consumed = 0;
      if (!UnescapeKey(key.substr(2), &user_key, &consumed)) break;
      if (!end.empty() && user_key >= end) break;
      LockRecord lock;
      if (!DecodeLock(it->value(), &lock)) return Status::Corruption("bad lock record");
      if (lock.start_ts <= ts) {
        *blocked = true;
        *blocking_lock = lock;
        *blocking_key = user_key;
        return Status::OK();
      }
    }
  }
  // Walk distinct user keys via the write column, checking locks per key.
  std::string seek = std::string(kWritePrefix) + EscapeKey(start);
  seek.resize(seek.size() - 2);  // drop terminator: seek to first key >= start
  auto it = db->NewIterator();
  it->Seek(seek);
  std::string last_key;
  bool have_last = false;
  while (it->Valid() && rows->size() < limit) {
    std::string user_key;
    uint64_t commit_ts;
    if (!ParseWriteKey(it->key(), &user_key, &commit_ts)) break;
    if (!end.empty() && user_key >= end) break;
    if (have_last && user_key == last_key) {
      it->Next();
      continue;
    }
    last_key = user_key;
    have_last = true;

    TxnReadResult result;
    Status s = TxnGet(db, user_key, ts, &result);
    if (!s.ok()) return s;
    if (result.locked) {
      *blocked = true;
      *blocking_lock = result.lock;
      *blocking_key = user_key;
      return Status::OK();
    }
    if (result.found) rows->emplace_back(user_key, std::move(result.value));
    it->Next();
  }
  return Status::OK();
}

Status FindTxnOutcome(storage::DB* db, std::string_view key, uint64_t start_ts,
                      uint64_t* commit_ts, bool* rolled_back) {
  *commit_ts = 0;
  *rolled_back = false;
  return ForEachWriteAtOrBelow(db, key, UINT64_MAX,
                               [&](uint64_t cts, const WriteRecord& rec) {
                                 if (rec.start_ts == start_ts) {
                                   if (rec.kind == kWriteRollback) {
                                     *rolled_back = true;
                                   } else {
                                     *commit_ts = cts;
                                   }
                                   return false;
                                 }
                                 // Records are newest-first; stop once we
                                 // pass below start_ts.
                                 return cts > start_ts;
                               });
}

}  // namespace flotilla::txn
