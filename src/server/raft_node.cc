#include "server/raft_node.h"

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <random>

#include "common/coding.h"
#include "common/logger.h"
#include "net/frame.h"
#include "net/socket.h"
#include "raft/wire.h"

namespace flotilla::server {

using raft::LogEntry;
using raft::Snapshot;

namespace {

const std::string kAppliedKey = std::string(1, '\0') + "applied_index";
constexpr size_t kMaxOutboxFrames = 1024;

bool IsSystemKey(std::string_view key) { return !key.empty() && key[0] == '\0'; }

bool DecodeCommand(std::string_view cmd, uint8_t* op, std::string* key,
                   std::string* value) {
  Decoder dec(cmd);
  *op = dec.U8();
  *key = dec.Str();
  *value = dec.Str();
  return dec.ok() && dec.remaining() == 0 && (*op == 1 || *op == 2);
}

}  // namespace

std::string RaftNode::EncodeCommand(uint8_t op, std::string_view key,
                                    std::string_view value) {
  std::string out;
  PutFixed8(&out, op);
  PutLengthPrefixed(&out, key);
  PutLengthPrefixed(&out, value);
  return out;
}

Status RaftNode::Start(const NodeOptions& options, std::unique_ptr<RaftNode>* out) {
  std::unique_ptr<RaftNode> node(new RaftNode(options));
  Status s = node->Init();
  if (!s.ok()) return s;
  *out = std::move(node);
  return Status::OK();
}

RaftNode::~RaftNode() { Stop(); }

Status RaftNode::Init() {
  self_ = options_.cluster.Find(options_.id);
  if (self_ == nullptr) {
    return Status::InvalidArgument("node id " + std::to_string(options_.id) +
                                   " not in cluster config");
  }
  Logger::Prefix() = "[n" + std::to_string(options_.id) + "] ";

  auto kv_dir = (std::filesystem::path(options_.data_dir) / "kv").string();
  Status s = storage::DB::Open(options_.db_options, kv_dir, &db_);
  if (!s.ok()) return s;

  std::string applied_raw;
  s = db_->Get(kAppliedKey, &applied_raw);
  if (s.ok() && applied_raw.size() == 8) {
    applied_index_ = DecodeFixed64(applied_raw.data());
  } else if (!s.IsNotFound() && !s.ok()) {
    return s;
  }

  auto raft_dir = (std::filesystem::path(options_.data_dir) / "raft").string();
  s = raft::RaftStorage::Open(raft_dir, &raft_storage_);
  if (!s.ok()) return s;

  const Snapshot& snap = raft_storage_->snapshot();
  if (snap.last_index > applied_index_) {
    // Crash between persisting a received snapshot and restoring the state
    // machine: finish the restore now.
    ApplySnapshot(snap);
  }
  applied_index_ = std::max(applied_index_, snap.last_index);

  raft::Config cfg;
  cfg.id = options_.id;
  for (const auto& n : options_.cluster.nodes) cfg.peers.push_back(n.id);
  cfg.election_timeout_min = options_.election_timeout_min_ticks;
  cfg.election_timeout_max = options_.election_timeout_max_ticks;
  cfg.heartbeat_interval = options_.heartbeat_interval_ticks;
  cfg.rng_seed = std::random_device{}() ^ (static_cast<uint64_t>(options_.id) << 32);
  raft_ = std::make_unique<raft::Raft>(cfg, raft_storage_->hard_state(),
                                       raft_storage_->entries(), snap);

  for (const auto& n : options_.cluster.nodes) {
    if (n.id == options_.id) continue;
    auto box = std::make_unique<PeerOutbox>();
    box->id = n.id;
    box->addr = n.raft_addr;
    outboxes_.push_back(std::move(box));
  }

  std::string host;
  uint16_t port;
  s = net::ParseAddr(self_->raft_addr, &host, &port);
  if (!s.ok()) return s;
  s = raft_server_.Start(host, port, [this](std::string_view req, std::string* resp) {
    (void)resp;  // raft frames are one-way
    OnRaftFrame(req);
    return true;
  });
  if (!s.ok()) return s;

  for (size_t i = 0; i < outboxes_.size(); i++) {
    sender_threads_.emplace_back(&RaftNode::SenderLoop, this, i);
  }
  apply_thread_ = std::thread(&RaftNode::ApplyLoop, this);
  tick_thread_ = std::thread(&RaftNode::TickLoop, this);
  FLOG_INFO("raft node started, cluster size %zu, applied %llu",
            options_.cluster.nodes.size(),
            static_cast<unsigned long long>(applied_index_));
  return Status::OK();
}

void RaftNode::Stop() {
  if (stopping_.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lock(raft_mutex_);
    raft_cv_.notify_all();
  }
  raft_server_.Stop();
  for (auto& box : outboxes_) {
    std::lock_guard<std::mutex> lock(box->mutex);
    box->cv.notify_all();
  }
  for (auto& t : sender_threads_) {
    if (t.joinable()) t.join();
  }
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    apply_cv_.notify_all();
  }
  if (apply_thread_.joinable()) apply_thread_.join();
  if (tick_thread_.joinable()) tick_thread_.join();
}

void RaftNode::TickLoop() {
  while (true) {
    ::usleep(static_cast<useconds_t>(options_.tick_ms) * 1000);
    std::lock_guard<std::mutex> lock(raft_mutex_);
    if (stopping_) return;
    raft_->Tick();
    DrainReady();
  }
}

void RaftNode::OnRaftFrame(std::string_view payload) {
  std::string_view body;
  if (!net::DecodeIsRaftFrame(payload, &body)) return;
  raft::Message m;
  if (!raft::DecodeMessage(body, &m)) return;
  std::lock_guard<std::mutex> lock(raft_mutex_);
  if (stopping_) return;
  raft_->Step(m);
  DrainReady();
}

void RaftNode::DrainReady() {
  raft::Ready ready = raft_->TakeReady();
  if (ready.Empty()) return;

  Status s = raft_storage_->Persist(ready);
  if (!s.ok()) {
    // A node that cannot persist raft state must not keep participating.
    FLOG_ERROR("raft persistence failed, aborting: %s", s.ToString().c_str());
    abort();
  }

  for (auto& m : ready.messages) {
    for (auto& box : outboxes_) {
      if (box->id != m.to) continue;
      std::string frame = net::EncodeRaftFrame(raft::EncodeMessage(m));
      std::lock_guard<std::mutex> lock(box->mutex);
      if (box->frames.size() >= kMaxOutboxFrames) box->frames.pop_front();
      box->frames.push_back(std::move(frame));
      box->cv.notify_one();
      break;
    }
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

void RaftNode::SenderLoop(size_t peer_slot) {
  PeerOutbox& box = *outboxes_[peer_slot];
  int fd = -1;
  while (true) {
    std::string frame;
    {
      std::unique_lock<std::mutex> lock(box.mutex);
      box.cv.wait(lock, [&] { return stopping_.load() || !box.frames.empty(); });
      if (stopping_.load()) break;
      frame = std::move(box.frames.front());
      box.frames.pop_front();
    }
    if (fd < 0 && !net::Connect(box.addr, &fd).ok()) {
      fd = -1;
      ::usleep(20 * 1000);  // peer down; raft retransmits, don't spin
      continue;
    }
    if (!net::WriteFrame(fd, frame).ok()) {
      net::CloseSocket(fd);
      fd = -1;
    }
  }
  if (fd >= 0) net::CloseSocket(fd);
}

void RaftNode::ApplyLoop() {
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

    uint64_t applied_now = 0;
    for (const auto& e : entries) {
      {
        std::lock_guard<std::mutex> lock(apply_mutex_);
        if (e.index <= applied_index_) continue;
      }
      Status s = Status::OK();
      if (!e.command.empty()) {
        uint8_t op;
        std::string key, value;
        if (!DecodeCommand(e.command, &op, &key, &value)) {
          s = Status::Corruption("bad command at index " + std::to_string(e.index));
        } else {
          std::shared_lock<std::shared_mutex> db_lock(db_lock_);
          s = op == 1 ? db_->Put(key, value) : db_->Delete(key);
        }
      }
      if (s.ok()) {
        std::string marker;
        PutFixed64(&marker, e.index);
        std::shared_lock<std::shared_mutex> db_lock(db_lock_);
        s = db_->Put(kAppliedKey, marker);
      }
      if (!s.ok()) {
        FLOG_ERROR("apply failed at index %llu: %s",
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
      applied_now = e.index;
      apply_cv_.notify_all();

      // Resolve the waiting proposal, if this index has one.
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

    if (applied_now != 0) MaybeSnapshot();
  }
}

void RaftNode::MaybeSnapshot() {
  if (options_.snapshot_interval_entries == 0) return;
  uint64_t applied;
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    if (applied_since_snapshot_ < options_.snapshot_interval_entries) return;
    applied = applied_index_;
  }
  std::string data = SerializeStateMachine();
  std::lock_guard<std::mutex> lock(raft_mutex_);
  if (stopping_ || applied > raft_->commit_index()) return;
  raft_->CompactTo(applied, std::move(data));
  Status s = raft_storage_->SaveSnapshot(raft_->base_snapshot(), false);
  if (!s.ok()) {
    FLOG_ERROR("snapshot persistence failed: %s", s.ToString().c_str());
    return;
  }
  {
    std::lock_guard<std::mutex> alock(apply_mutex_);
    applied_since_snapshot_ = 0;
  }
  FLOG_INFO("compacted raft log through index %llu",
            static_cast<unsigned long long>(applied));
}

std::string RaftNode::SerializeStateMachine() {
  std::shared_lock<std::shared_mutex> db_lock(db_lock_);
  std::string out;
  uint32_t count = 0;
  std::string body;
  auto it = db_->NewIterator();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (IsSystemKey(it->key())) continue;
    PutLengthPrefixed(&body, it->key());
    PutLengthPrefixed(&body, it->value());
    count++;
  }
  PutFixed32(&out, count);
  out += body;
  return out;
}

void RaftNode::ApplySnapshot(const Snapshot& snap) {
  std::unique_lock<std::shared_mutex> db_lock(db_lock_);
  auto kv_dir = (std::filesystem::path(options_.data_dir) / "kv").string();
  db_.reset();
  std::filesystem::remove_all(kv_dir);
  Status s = storage::DB::Open(options_.db_options, kv_dir, &db_);
  if (!s.ok()) {
    FLOG_ERROR("reopen kv for snapshot failed, aborting: %s", s.ToString().c_str());
    abort();
  }

  Decoder dec(snap.data);
  uint32_t count = dec.U32();
  for (uint32_t i = 0; i < count && dec.ok(); i++) {
    std::string key = dec.Str();
    std::string value = dec.Str();
    if (dec.ok()) s = db_->Put(key, value);
    if (!s.ok()) break;
  }
  std::string marker;
  PutFixed64(&marker, snap.last_index);
  if (s.ok()) s = db_->Put(kAppliedKey, marker);
  if (!dec.ok() || !s.ok()) {
    FLOG_ERROR("snapshot restore failed, aborting: %s", s.ToString().c_str());
    abort();
  }
  FLOG_INFO("restored snapshot through index %llu",
            static_cast<unsigned long long>(snap.last_index));
}

std::string RaftNode::LeaderClientAddr() {
  raft::NodeId leader = raft_->leader();
  const NodeInfo* info = options_.cluster.Find(leader);
  return info != nullptr ? info->client_addr : "";
}

net::Response RaftNode::NotLeaderResponse() {
  net::Response resp = net::Response::FromStatus(Status::NotLeader("not the leader"));
  std::lock_guard<std::mutex> lock(raft_mutex_);
  resp.leader_addr = LeaderClientAddr();
  return resp;
}

Status RaftNode::ProposeAndWait(const std::string& command) {
  std::shared_ptr<PendingProposal> pending;
  uint64_t index = 0;
  {
    std::unique_lock<std::mutex> lock(raft_mutex_);
    if (stopping_) return Status::Aborted("node stopping");
    uint64_t term = 0;
    if (!raft_->Propose(command, &index, &term)) {
      return Status::NotLeader(LeaderClientAddr());
    }
    pending = std::make_shared<PendingProposal>();
    pending->term = term;
    proposals_[index] = pending;
    DrainReady();

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.request_timeout_ms);
    while (!pending->done && !stopping_) {
      if (raft_cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
    }
    if (!pending->done) {
      proposals_.erase(index);
      return stopping_ ? Status::Aborted("node stopping")
                       : Status::Timeout("proposal not committed in time");
    }
  }
  return pending->result;
}

Status RaftNode::LinearizableReadBarrier() {
  uint64_t read_index = 0;
  {
    std::unique_lock<std::mutex> lock(raft_mutex_);
    if (stopping_) return Status::Aborted("node stopping");
    if (raft_->role() != raft::Role::kLeader) {
      return Status::NotLeader(LeaderClientAddr());
    }
    uint64_t ctx = next_read_ctx_++;
    auto pending = std::make_shared<PendingRead>();
    reads_[ctx] = pending;
    if (!raft_->StartReadIndex(ctx)) {
      reads_.erase(ctx);
      // Leader without a committed entry in its term yet: not ready to serve.
      return Status::NotLeader(LeaderClientAddr());
    }
    DrainReady();

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.request_timeout_ms);
    while (!pending->confirmed && !stopping_) {
      if (raft_cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
    }
    reads_.erase(ctx);
    if (!pending->confirmed) {
      return stopping_ ? Status::Aborted("node stopping")
                       : Status::Timeout("read index not confirmed");
    }
    read_index = pending->read_index;
  }

  std::unique_lock<std::mutex> lock(apply_mutex_);
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(options_.request_timeout_ms);
  while (applied_index_ < read_index && apply_error_.ok()) {
    if (stopping_.load()) return Status::Aborted("node stopping");
    if (apply_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return Status::Timeout("apply lagging behind read index");
    }
  }
  return apply_error_;
}

bool RaftNode::IsLeader() {
  std::lock_guard<std::mutex> lock(raft_mutex_);
  return raft_->role() == raft::Role::kLeader;
}

bool RaftNode::HandleClientFrame(std::string_view payload, std::string* out) {
  net::Request req;
  if (!DecodeRequest(payload, &req)) return false;
  *out = EncodeResponse(Handle(req));
  return true;
}

net::Response RaftNode::Handle(const net::Request& req) {
  using net::MsgType;
  net::Response resp;
  switch (req.type) {
    case MsgType::kGet: {
      Status s = LinearizableReadBarrier();
      if (s.IsNotLeader()) return NotLeaderResponse();
      if (!s.ok()) return net::Response::FromStatus(s);
      std::shared_lock<std::shared_mutex> db_lock(db_lock_);
      s = db_->Get(req.key, &resp.value);
      if (s.ok()) {
        resp.found = true;
      } else if (s.IsNotFound()) {
        resp.found = false;
      } else {
        return net::Response::FromStatus(s);
      }
      break;
    }
    case MsgType::kPut: {
      Status s = ProposeAndWait(EncodeCommand(1, req.key, req.value));
      if (s.IsNotLeader()) return NotLeaderResponse();
      if (!s.ok()) return net::Response::FromStatus(s);
      break;
    }
    case MsgType::kDelete: {
      Status s = ProposeAndWait(EncodeCommand(2, req.key, ""));
      if (s.IsNotLeader()) return NotLeaderResponse();
      if (!s.ok()) return net::Response::FromStatus(s);
      break;
    }
    case MsgType::kScan: {
      Status s = LinearizableReadBarrier();
      if (s.IsNotLeader()) return NotLeaderResponse();
      if (!s.ok()) return net::Response::FromStatus(s);
      uint32_t limit = req.limit == 0 ? 1000 : req.limit;
      std::shared_lock<std::shared_mutex> db_lock(db_lock_);
      auto it = db_->NewIterator();
      if (req.key.empty()) {
        it->SeekToFirst();
      } else {
        it->Seek(req.key);
      }
      while (it->Valid() && resp.kvs.size() < limit) {
        if (!req.end_key.empty() && std::string_view(it->key()) >= req.end_key) break;
        if (!IsSystemKey(it->key())) {
          resp.kvs.emplace_back(std::string(it->key()), std::string(it->value()));
        }
        it->Next();
      }
      break;
    }
    case MsgType::kStatus: {
      uint64_t applied;
      {
        std::lock_guard<std::mutex> lock(apply_mutex_);
        applied = applied_index_;
      }
      std::lock_guard<std::mutex> lock(raft_mutex_);
      const char* role = raft_->role() == raft::Role::kLeader     ? "leader"
                         : raft_->role() == raft::Role::kCandidate ? "candidate"
                                                                    : "follower";
      resp.kvs.emplace_back("node_id", std::to_string(options_.id));
      resp.kvs.emplace_back("role", role);
      resp.kvs.emplace_back("term", std::to_string(raft_->term()));
      resp.kvs.emplace_back("leader_id", std::to_string(raft_->leader()));
      resp.kvs.emplace_back("leader_addr", LeaderClientAddr());
      resp.kvs.emplace_back("commit_index", std::to_string(raft_->commit_index()));
      resp.kvs.emplace_back("applied_index", std::to_string(applied));
      resp.kvs.emplace_back("last_log_index", std::to_string(raft_->last_index()));
      resp.kvs.emplace_back("snapshot_index",
                            std::to_string(raft_->base_snapshot().last_index));
      break;
    }
    default:
      return net::Response::FromStatus(
          Status::InvalidArgument("unsupported request type"));
  }
  return resp;
}

}  // namespace flotilla::server
