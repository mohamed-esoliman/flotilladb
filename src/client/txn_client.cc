#include "client/txn_client.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>

namespace flotilla::client {

using net::MsgType;
using net::Request;
using net::Response;

namespace {
uint64_t NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count());
}
}  // namespace

Status Txn::AllocateTs(uint64_t* ts) {
  Request req;
  req.type = MsgType::kTxnTs;
  Response resp;
  Status s = client_->Call(req, &resp);
  if (!s.ok()) return s;
  if (!resp.ok()) return resp.ToStatus();
  *ts = resp.ts;
  return Status::OK();
}

Status Txn::Begin() {
  mutations_.clear();
  prewritten_.clear();
  Status s = AllocateTs(&start_ts_);
  active_ = s.ok();
  return s;
}

Status Txn::ResolveLock(const Response& resp) {
  if (resp.lock_ts == 0 || resp.lock_primary.empty()) {
    return Status::Conflict(resp.message);
  }
  Request resolve;
  resolve.type = MsgType::kTxnResolve;
  resolve.key = resp.lock_primary;
  resolve.ts = resp.lock_ts;
  Response outcome;
  Status s = client_->Call(resolve, &outcome);
  if (!s.ok()) return s;
  if (!outcome.ok()) return outcome.ToStatus();  // txn alive: Conflict

  // Roll the blocked key forward or back to match the primary's fate.
  Request fix;
  fix.key = resp.lock_key;
  fix.ts = resp.lock_ts;
  if (outcome.ts != 0) {
    fix.type = MsgType::kTxnCommit;
    fix.ts2 = outcome.ts;
  } else {
    fix.type = MsgType::kTxnRollback;
  }
  Response fixed;
  s = client_->Call(fix, &fixed);
  if (!s.ok()) return s;
  return fixed.ToStatus();
}

Status Txn::Get(const std::string& key, std::string* value) {
  if (!active_) return Status::InvalidArgument("transaction not begun");
  auto it = mutations_.find(key);
  if (it != mutations_.end()) {
    if (it->second.op == 2) return Status::NotFound(key);
    *value = it->second.value;
    return Status::OK();
  }

  Request req;
  req.type = MsgType::kTxnGet;
  req.key = key;
  req.ts = start_ts_;
  for (int attempt = 0; attempt < 20; attempt++) {
    Response resp;
    Status s = client_->Call(req, &resp);
    if (!s.ok()) return s;
    if (resp.code == static_cast<uint8_t>(Status::Code::kConflict)) {
      s = ResolveLock(resp);
      if (s.IsConflict()) {
        ::usleep(200 * 1000);  // holder may still be live; wait out its TTL
        continue;
      }
      if (!s.ok()) return s;
      continue;
    }
    if (!resp.ok()) return resp.ToStatus();
    if (!resp.found) return Status::NotFound(key);
    *value = resp.value;
    return Status::OK();
  }
  return Status::Timeout("lock never resolved");
}

Status Txn::Scan(const std::string& start, const std::string& end, uint32_t limit,
                 std::vector<std::pair<std::string, std::string>>* rows) {
  if (!active_) return Status::InvalidArgument("transaction not begun");
  Request req;
  req.type = MsgType::kTxnScan;
  req.key = start;
  req.end_key = end;
  req.limit = limit;
  req.ts = start_ts_;
  for (int attempt = 0; attempt < 20; attempt++) {
    Response resp;
    Status s = client_->Call(req, &resp);
    if (!s.ok()) return s;
    if (resp.code == static_cast<uint8_t>(Status::Code::kConflict)) {
      s = ResolveLock(resp);
      if (s.IsConflict()) {
        ::usleep(200 * 1000);
        continue;
      }
      if (!s.ok()) return s;
      continue;
    }
    if (!resp.ok()) return resp.ToStatus();
    *rows = resp.kvs;
    // Overlay buffered writes.
    for (const auto& [key, mutation] : mutations_) {
      if (key < start || (!end.empty() && key >= end)) continue;
      auto pos = std::lower_bound(
          rows->begin(), rows->end(), key,
          [](const auto& row, const std::string& k) { return row.first < k; });
      if (pos != rows->end() && pos->first == key) {
        if (mutation.op == 2) {
          rows->erase(pos);
        } else {
          pos->second = mutation.value;
        }
      } else if (mutation.op == 1) {
        rows->insert(pos, {key, mutation.value});
      }
    }
    return Status::OK();
  }
  return Status::Timeout("lock never resolved");
}

void Txn::Put(const std::string& key, const std::string& value) {
  mutations_[key] = {1, value};
}

void Txn::Delete(const std::string& key) { mutations_[key] = {2, ""}; }

Status Txn::Commit() {
  if (!active_) return Status::InvalidArgument("transaction not begun");
  active_ = false;
  if (mutations_.empty()) return Status::OK();

  const std::string& primary = mutations_.begin()->first;
  uint64_t wall = NowMs();

  auto abort_all = [&](const Status& cause) {
    for (const auto& key : prewritten_) {
      Request rb;
      rb.type = MsgType::kTxnRollback;
      rb.key = key;
      rb.ts = start_ts_;
      Response resp;
      client_->Call(rb, &resp);
    }
    return cause;
  };

  for (const auto& [key, mutation] : mutations_) {
    Request req;
    req.type = MsgType::kTxnPrewrite;
    req.key = key;
    req.value = mutation.value;
    req.wop = mutation.op;
    req.ts = start_ts_;
    req.ts2 = wall;
    req.primary = primary;
    bool done = false;
    for (int attempt = 0; attempt < 10 && !done; attempt++) {
      Response resp;
      Status s = client_->Call(req, &resp);
      if (!s.ok()) return abort_all(s);
      if (resp.code == static_cast<uint8_t>(Status::Code::kConflict) &&
          resp.lock_ts != 0) {
        // Try to clear an abandoned lock, then retry once more.
        s = ResolveLock(resp);
        if (s.IsConflict()) {
          ::usleep(200 * 1000);
          continue;
        }
        if (!s.ok()) return abort_all(s);
        continue;
      }
      if (!resp.ok()) return abort_all(resp.ToStatus());
      done = true;
    }
    if (!done) return abort_all(Status::Conflict("prewrite kept conflicting"));
    prewritten_.push_back(key);
  }

  uint64_t commit_ts = 0;
  Status s = AllocateTs(&commit_ts);
  if (!s.ok()) return abort_all(s);

  // Commit point: the primary key.
  Request commit;
  commit.type = MsgType::kTxnCommit;
  commit.key = primary;
  commit.ts = start_ts_;
  commit.ts2 = commit_ts;
  Response resp;
  s = client_->Call(commit, &resp);
  if (!s.ok()) return abort_all(s);
  if (!resp.ok()) return abort_all(resp.ToStatus());

  // Roll secondaries forward; failures are recovered by future readers.
  for (const auto& [key, mutation] : mutations_) {
    if (key == primary) continue;
    Request req;
    req.type = MsgType::kTxnCommit;
    req.key = key;
    req.ts = start_ts_;
    req.ts2 = commit_ts;
    Response ignored;
    client_->Call(req, &ignored);
  }
  return Status::OK();
}

Status Txn::Rollback() {
  active_ = false;
  for (const auto& key : prewritten_) {
    Request req;
    req.type = MsgType::kTxnRollback;
    req.key = key;
    req.ts = start_ts_;
    Response resp;
    client_->Call(req, &resp);
  }
  mutations_.clear();
  prewritten_.clear();
  return Status::OK();
}

}  // namespace flotilla::client
