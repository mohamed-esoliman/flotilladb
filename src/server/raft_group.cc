#include "server/raft_group.h"

#include <chrono>
#include <random>

#include "common/coding.h"
#include "common/logger.h"
#include "raft/wire.h"

namespace flotilla::server {

using raft::LogEntry;
using raft::Snapshot;

std::string AppliedKey(uint32_t group_id) {
  return std::string(1, '\0') + "applied/" + std::to_string(group_id);
}

std::string RangeKey(uint32_t range_id) {
  return std::string(1, '\0') + "range/" + std::to_string(range_id);
}

std::string SpawnsKey(uint32_t group_id) {
  return std::string(1, '\0') + "spawns/" + std::to_string(group_id);
}

std::string EncodeRangeDesc(const RangeDesc& d) {
  std::string out;
  PutLengthPrefixed(&out, d.start);
  PutLengthPrefixed(&out, d.end);
  return out;
}

bool DecodeRangeDesc(uint32_t id, std::string_view data, RangeDesc* out) {
  Decoder dec(data);
  out->id = id;
  out->start = dec.Str();
  out->end = dec.Str();
  return dec.ok() && dec.remaining() == 0;
}

namespace {

std::string EncodeSpawns(const std::vector<RangeDesc>& spawns) {
  std::string out;
  PutFixed32(&out, static_cast<uint32_t>(spawns.size()));
  for (const auto& d : spawns) {
    PutFixed32(&out, d.id);
    PutLengthPrefixed(&out, d.start);
    PutLengthPrefixed(&out, d.end);
  }
  return out;
}

bool DecodeSpawns(std::string_view data, std::vector<RangeDesc>* out) {
  Decoder dec(data);
  uint32_t count = dec.U32();
  out->clear();
  for (uint32_t i = 0; i < count && dec.ok(); i++) {
    RangeDesc d;
    d.id = dec.U32();
    d.start = dec.Str();
    d.end = dec.Str();
    out->push_back(std::move(d));
  }
  return dec.ok() && dec.remaining() == 0;
}

bool DecodeCommand(std::string_view cmd, uint8_t* op, std::string* key,
                   std::string* value) {
  Decoder dec(cmd);
  *op = dec.U8();
  *key = dec.Str();
  *value = dec.Str();
  return dec.ok() && dec.remaining() == 0 && *op >= 1 && *op <= 3;
}

}  // namespace

std::string RaftGroup::EncodeCommand(uint8_t op, std::string_view key,
                                     std::string_view value) {
  std::string out;
  PutFixed8(&out, op);
  PutLengthPrefixed(&out, key);
  PutLengthPrefixed(&out, value);
  return out;
}

Status RaftGroup::Start(const GroupOptions& options, storage::DB* db, GroupHost* host,
                        std::unique_ptr<RaftGroup>* out) {
  std::unique_ptr<RaftGroup> group(new RaftGroup(options, db, host));
  Status s = group->Init();
  if (!s.ok()) return s;
  *out = std::move(group);
  return Status::OK();
}

RaftGroup::~RaftGroup() { Stop(); }

Status RaftGroup::Init() {
  range_ = options_.range;

  // Durable range/spawn state overrides the creation-time range (restart).
  std::string raw;
  Status s = db_->Get(RangeKey(options_.group_id), &raw);
  if (s.ok()) {
    RangeDesc stored;
    if (!DecodeRangeDesc(options_.group_id, raw, &stored)) {
      return Status::Corruption("bad range descriptor for group " +
                                std::to_string(options_.group_id));
    }
    range_ = stored;
  } else if (!s.IsNotFound()) {
    return s;
  }
  s = db_->Get(SpawnsKey(options_.group_id), &raw);
  if (s.ok()) {
    if (!DecodeSpawns(raw, &spawned_)) {
      return Status::Corruption("bad spawn list for group " +
                                std::to_string(options_.group_id));
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  s = db_->Get(AppliedKey(options_.group_id), &raw);
  if (s.ok() && raw.size() == 8) {
    applied_index_ = DecodeFixed64(raw.data());
  } else if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  s = raft::RaftStorage::Open(options_.raft_dir, &raft_storage_);
  if (!s.ok()) return s;

  const Snapshot& snap = raft_storage_->snapshot();
  if (snap.last_index > applied_index_) {
    ApplySnapshot(snap);
  }
  applied_index_ = std::max(applied_index_, snap.last_index);

  raft::Config cfg;
  cfg.id = options_.node_id;
  cfg.peers = options_.peers;
  cfg.election_timeout_min = options_.election_timeout_min_ticks;
  cfg.election_timeout_max = options_.election_timeout_max_ticks;
  cfg.heartbeat_interval = options_.heartbeat_interval_ticks;
  cfg.rng_seed = std::random_device{}() ^
                 (static_cast<uint64_t>(options_.group_id) << 40) ^
                 (static_cast<uint64_t>(options_.node_id) << 32);
  raft_ = std::make_unique<raft::Raft>(cfg, raft_storage_->hard_state(),
                                       raft_storage_->entries(), snap);
  apply_thread_ = std::thread(&RaftGroup::ApplyLoop, this);
  return Status::OK();
}

void RaftGroup::Stop() {
  if (stopping_.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lock(raft_mutex_);
    raft_cv_.notify_all();
  }
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    apply_cv_.notify_all();
  }
  if (apply_thread_.joinable()) apply_thread_.join();
}

void RaftGroup::Tick() {
  std::lock_guard<std::mutex> lock(raft_mutex_);
  if (stopping_.load()) return;
  raft_->Tick();
  DrainReady();
}

void RaftGroup::StepMessage(const raft::Message& m) {
  std::lock_guard<std::mutex> lock(raft_mutex_);
  if (stopping_.load()) return;
  raft_->Step(m);
  DrainReady();
}

void RaftGroup::DrainReady() {
  raft::Ready ready = raft_->TakeReady();
  if (ready.Empty()) return;

  Status s = raft_storage_->Persist(ready);
  if (!s.ok()) {
    FLOG_ERROR("group %u raft persistence failed, aborting: %s", options_.group_id,
               s.ToString().c_str());
    abort();
  }

  for (auto& m : ready.messages) {
    host_->SendRaft(m.to, options_.group_id, raft::EncodeMessage(m));
  }

  bool wake_apply = false;
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    if (ready.snapshot_to_apply.has_value()) {
      snapshot_queue_.push_back(*ready.snapshot_to_apply);
      wake_apply = true;
    }
    for (auto& e : ready.committed) {
      apply_queue_.push_back(std::move(e));
      wake_apply = true;
    }
  }
  if (wake_apply) apply_cv_.notify_all();

  for (const auto& [ctx, read_index] : ready.confirmed_reads) {
    auto it = reads_.find(ctx);
    if (it != reads_.end()) {
      it->second->confirmed = true;
      it->second->read_index = read_index;
    }
  }
  if (!ready.confirmed_reads.empty()) raft_cv_.notify_all();
}

Status RaftGroup::PersistAppliedMarker(uint64_t index) {
  std::string marker;
  PutFixed64(&marker, index);
  return db_->Put(AppliedKey(options_.group_id), marker);
}

Status RaftGroup::ApplySplit(uint32_t child_id, const std::string& split_key) {
  RangeDesc parent_now, child;
  {
    // The host callback below takes the node's group map lock, which is also
    // held while calling into range(); never invoke it under range_mutex_.
    std::lock_guard<std::mutex> lock(range_mutex_);
    if (!range_.Contains(split_key) || split_key == range_.start) {
      // Stale or invalid split (e.g. replayed after an earlier split landed).
      return Status::OK();
    }
    std::string probe;
    Status s = db_->Get(RangeKey(child_id), &probe);
    if (s.ok()) return Status::OK();  // child already exists: replay
    if (!s.IsNotFound()) return s;

    child.id = child_id;
    child.start = split_key;
    child.end = range_.end;
    parent_now = range_;
    parent_now.end = split_key;

    s = db_->Put(RangeKey(child_id), EncodeRangeDesc(child));
    if (s.ok()) s = db_->Put(RangeKey(range_.id), EncodeRangeDesc(parent_now));
    if (s.ok()) {
      spawned_.push_back(child);
      s = db_->Put(SpawnsKey(options_.group_id), EncodeSpawns(spawned_));
    }
    if (!s.ok()) return s;
    range_ = parent_now;
  }
  host_->OnSplitApplied(options_.group_id, parent_now, child);
  FLOG_INFO("group %u split at key '%s': child group %u", options_.group_id,
            split_key.c_str(), child_id);
  return Status::OK();
}

Status RaftGroup::ApplyCommand(const LogEntry& e) {
  if (e.command.empty()) return Status::OK();
  uint8_t op;
  std::string key, value;
  if (!DecodeCommand(e.command, &op, &key, &value)) {
    return Status::Corruption("bad command at index " + std::to_string(e.index));
  }
  switch (op) {
    case 1: return db_->Put(key, value);
    case 2: return db_->Delete(key);
    case 3: {
      Decoder dec(value);
      uint32_t child_id = dec.U32();
      if (!dec.ok()) return Status::Corruption("bad split payload");
      return ApplySplit(child_id, key);
    }
  }
  return Status::Corruption("unknown op");
}

void RaftGroup::ApplyLoop() {
  while (true) {
    std::deque<Snapshot> snaps;
    std::deque<LogEntry> entries;
    {
      std::unique_lock<std::mutex> lock(apply_mutex_);
      apply_cv_.wait(lock, [&] {
        return stopping_.load() || !apply_queue_.empty() || !snapshot_queue_.empty();
      });
      if (stopping_.load()) return;
      snaps.swap(snapshot_queue_);
      entries.swap(apply_queue_);
    }

    for (const auto& snap : snaps) {
      ApplySnapshot(snap);
      {
        std::lock_guard<std::mutex> lock(apply_mutex_);
        applied_index_ = std::max(applied_index_, snap.last_index);
        applied_since_snapshot_ = 0;
      }
      apply_cv_.notify_all();
    }

    bool applied_any = false;
    for (const auto& e : entries) {
      {
        std::lock_guard<std::mutex> lock(apply_mutex_);
        if (e.index <= applied_index_) continue;
      }
      Status s = ApplyCommand(e);
      if (s.ok()) s = PersistAppliedMarker(e.index);
      if (!s.ok()) {
        FLOG_ERROR("group %u apply failed at %llu: %s", options_.group_id,
                   static_cast<unsigned long long>(e.index), s.ToString().c_str());
        std::lock_guard<std::mutex> lock(apply_mutex_);
        apply_error_ = s;
        apply_cv_.notify_all();
        return;
      }
      {
        std::lock_guard<std::mutex> lock(apply_mutex_);
        applied_index_ = e.index;
        applied_since_snapshot_++;
      }
      applied_any = true;
      apply_cv_.notify_all();

      std::lock_guard<std::mutex> lock(raft_mutex_);
      auto it = proposals_.find(e.index);
      if (it != proposals_.end()) {
        it->second->done = true;
        it->second->result =
            it->second->term == e.term
                ? Status::OK()
                : Status::NotLeader("superseded by another leader");
        proposals_.erase(it);
        raft_cv_.notify_all();
      }
    }

    if (applied_any) MaybeSnapshot();
  }
}

std::string RaftGroup::SerializeStateMachine() {
  RangeDesc range;
  std::vector<RangeDesc> spawns;
  {
    std::lock_guard<std::mutex> lock(range_mutex_);
    range = range_;
    spawns = spawned_;
  }
  std::string out;
  PutLengthPrefixed(&out, range.start);
  PutLengthPrefixed(&out, range.end);
  out += EncodeSpawns(spawns);

  std::string body;
  uint32_t count = 0;
  auto it = db_->NewIterator();
  for (it->Seek(range.start); it->Valid(); it->Next()) {
    std::string_view key = it->key();
    if (IsSystemKey(key)) continue;
    if (!range.end.empty() && key >= range.end) break;
    PutLengthPrefixed(&body, key);
    PutLengthPrefixed(&body, it->value());
    count++;
  }
  PutFixed32(&out, count);
  out += body;
  return out;
}

void RaftGroup::ApplySnapshot(const Snapshot& snap) {
  Decoder dec(snap.data);
  RangeDesc range;
  range.id = options_.group_id;
  range.start = dec.Str();
  range.end = dec.Str();
  std::vector<RangeDesc> spawns;
  uint32_t nspawns = dec.U32();
  for (uint32_t i = 0; i < nspawns && dec.ok(); i++) {
    RangeDesc d;
    d.id = dec.U32();
    d.start = dec.Str();
    d.end = dec.Str();
    spawns.push_back(std::move(d));
  }
  uint32_t count = dec.U32();

  // Wipe exactly the snapshot's range, then insert its contents.
  std::vector<std::string> to_delete;
  {
    auto it = db_->NewIterator();
    for (it->Seek(range.start); it->Valid(); it->Next()) {
      std::string_view key = it->key();
      if (IsSystemKey(key)) continue;
      if (!range.end.empty() && key >= range.end) break;
      to_delete.emplace_back(key);
    }
  }
  Status s = Status::OK();
  for (const auto& key : to_delete) {
    s = db_->Delete(key);
    if (!s.ok()) break;
  }
  for (uint32_t i = 0; i < count && dec.ok() && s.ok(); i++) {
    std::string key = dec.Str();
    std::string value = dec.Str();
    if (dec.ok()) s = db_->Put(key, value);
  }
  if (s.ok()) s = db_->Put(RangeKey(range.id), EncodeRangeDesc(range));
  if (s.ok()) s = db_->Put(SpawnsKey(options_.group_id), EncodeSpawns(spawns));
  if (s.ok()) s = PersistAppliedMarker(snap.last_index);
  if (!dec.ok() || !s.ok()) {
    FLOG_ERROR("group %u snapshot restore failed, aborting: %s", options_.group_id,
               s.ToString().c_str());
    abort();
  }

  {
    std::lock_guard<std::mutex> lock(range_mutex_);
    range_ = range;
    spawned_ = spawns;
  }
  host_->OnSnapshotRestored(options_.group_id, range, spawns);
  FLOG_INFO("group %u restored snapshot through index %llu", options_.group_id,
            static_cast<unsigned long long>(snap.last_index));
}

void RaftGroup::MaybeSnapshot() {
  if (options_.snapshot_interval_entries == 0) return;
  uint64_t applied;
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    if (applied_since_snapshot_ < options_.snapshot_interval_entries) return;
    applied = applied_index_;
  }
  std::string data = SerializeStateMachine();
  std::lock_guard<std::mutex> lock(raft_mutex_);
  if (stopping_.load() || applied > raft_->commit_index()) return;
  raft_->CompactTo(applied, std::move(data));
  Status s = raft_storage_->SaveSnapshot(raft_->base_snapshot(), false);
  if (!s.ok()) {
    FLOG_ERROR("group %u snapshot persistence failed: %s", options_.group_id,
               s.ToString().c_str());
    return;
  }
  {
    std::lock_guard<std::mutex> alock(apply_mutex_);
    applied_since_snapshot_ = 0;
  }
  FLOG_INFO("group %u compacted raft log through index %llu", options_.group_id,
            static_cast<unsigned long long>(applied));
}

Status RaftGroup::ProposeAndWait(const std::string& command) {
  std::shared_ptr<PendingProposal> pending;
  {
    std::unique_lock<std::mutex> lock(raft_mutex_);
    if (stopping_.load()) return Status::Aborted("group stopping");
    uint64_t index = 0, term = 0;
    if (!raft_->Propose(command, &index, &term)) {
      return Status::NotLeader("");
    }
    pending = std::make_shared<PendingProposal>();
    pending->term = term;
    proposals_[index] = pending;
    DrainReady();

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.request_timeout_ms);
    while (!pending->done && !stopping_.load()) {
      if (raft_cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
    }
    if (!pending->done) {
      proposals_.erase(index);
      return stopping_.load() ? Status::Aborted("group stopping")
                              : Status::Timeout("proposal not committed in time");
    }
  }
  return pending->result;
}

Status RaftGroup::Put(std::string_view key, std::string_view value) {
  return ProposeAndWait(EncodeCommand(1, key, value));
}

Status RaftGroup::Delete(std::string_view key) {
  return ProposeAndWait(EncodeCommand(2, key, ""));
}

Status RaftGroup::Split(std::string_view key, uint32_t child_id) {
  {
    std::lock_guard<std::mutex> lock(range_mutex_);
    if (!range_.Contains(key) || key == range_.start) {
      return Status::InvalidArgument("split key not strictly inside range");
    }
  }
  std::string payload;
  PutFixed32(&payload, child_id);
  return ProposeAndWait(EncodeCommand(3, key, payload));
}

Status RaftGroup::LinearizableReadBarrier() {
  uint64_t read_index = 0;
  {
    std::unique_lock<std::mutex> lock(raft_mutex_);
    if (stopping_.load()) return Status::Aborted("group stopping");
    if (raft_->role() != raft::Role::kLeader) return Status::NotLeader("");
    uint64_t ctx = next_read_ctx_++;
    auto pending = std::make_shared<PendingRead>();
    reads_[ctx] = pending;
    if (!raft_->StartReadIndex(ctx)) {
      reads_.erase(ctx);
      return Status::NotLeader("");
    }
    DrainReady();

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.request_timeout_ms);
    while (!pending->confirmed && !stopping_.load()) {
      if (raft_cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
    }
    reads_.erase(ctx);
    if (!pending->confirmed) {
      return stopping_.load() ? Status::Aborted("group stopping")
                              : Status::Timeout("read index not confirmed");
    }
    read_index = pending->read_index;
  }

  std::unique_lock<std::mutex> lock(apply_mutex_);
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(options_.request_timeout_ms);
  while (applied_index_ < read_index && apply_error_.ok()) {
    if (stopping_.load()) return Status::Aborted("group stopping");
    if (apply_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return Status::Timeout("apply lagging behind read index");
    }
  }
  return apply_error_;
}

RangeDesc RaftGroup::range() {
  std::lock_guard<std::mutex> lock(range_mutex_);
  return range_;
}

bool RaftGroup::IsLeader() {
  std::lock_guard<std::mutex> lock(raft_mutex_);
  return raft_->role() == raft::Role::kLeader;
}

raft::NodeId RaftGroup::LeaderId() {
  std::lock_guard<std::mutex> lock(raft_mutex_);
  return raft_->leader();
}

RaftGroup::Info RaftGroup::GetInfo() {
  Info info;
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    info.applied = applied_index_;
  }
  std::lock_guard<std::mutex> lock(raft_mutex_);
  info.role = raft_->role() == raft::Role::kLeader     ? "leader"
              : raft_->role() == raft::Role::kCandidate ? "candidate"
                                                         : "follower";
  info.term = raft_->term();
  info.commit = raft_->commit_index();
  info.last_log = raft_->last_index();
  info.snapshot_index = raft_->base_snapshot().last_index;
  info.leader = raft_->leader();
  return info;
}

}  // namespace flotilla::server
