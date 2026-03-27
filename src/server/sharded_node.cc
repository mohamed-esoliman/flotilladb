#include "server/sharded_node.h"

#include <unistd.h>

#include <filesystem>

#include "common/coding.h"
#include "common/logger.h"
#include "net/frame.h"
#include "net/socket.h"
#include "raft/wire.h"

namespace flotilla::server {

namespace {
constexpr size_t kMaxOutboxFrames = 4096;
constexpr uint32_t kDefaultScanLimit = 1000;
}  // namespace

Status ShardedNode::Start(const NodeOptions& options, std::unique_ptr<ShardedNode>* out) {
  std::unique_ptr<ShardedNode> node(new ShardedNode(options));
  Status s = node->Init();
  if (!s.ok()) return s;
  *out = std::move(node);
  return Status::OK();
}

ShardedNode::~ShardedNode() { Stop(); }

Status ShardedNode::Init() {
  self_ = options_.cluster.Find(options_.id);
  if (self_ == nullptr) {
    return Status::InvalidArgument("node id " + std::to_string(options_.id) +
                                   " not in cluster config");
  }
  Logger::Prefix() = "[n" + std::to_string(options_.id) + "] ";

  auto kv_dir = (std::filesystem::path(options_.data_dir) / "kv").string();
  Status s = storage::DB::Open(options_.db_options, kv_dir, &db_);
  if (!s.ok()) return s;

  // Discover locally known ranges from durable descriptors; bootstrap the
  // initial full-keyspace range if this is a fresh node. Bootstrap writes are
  // identical and deterministic on every node.
  std::vector<RangeDesc> ranges;
  {
    std::string prefix = RangeKey(0);
    prefix.resize(prefix.size() - 1);  // "\0range/"
    auto it = db_->NewIterator();
    for (it->Seek(prefix); it->Valid(); it->Next()) {
      std::string_view key = it->key();
      if (key.substr(0, prefix.size()) != prefix) break;
      uint32_t id =
          static_cast<uint32_t>(strtoul(std::string(key.substr(prefix.size())).c_str(),
                                        nullptr, 10));
      RangeDesc d;
      if (!DecodeRangeDesc(id, it->value(), &d)) {
        return Status::Corruption("bad range descriptor on disk");
      }
      ranges.push_back(std::move(d));
    }
  }
  if (ranges.empty()) {
    RangeDesc initial;
    initial.id = 1;
    s = db_->Put(RangeKey(1), EncodeRangeDesc(initial));
    if (!s.ok()) return s;
    ranges.push_back(initial);
  }

  for (const auto& n : options_.cluster.nodes) {
    if (n.id == options_.id) continue;
    auto box = std::make_unique<PeerOutbox>();
    box->id = n.id;
    box->addr = n.raft_addr;
    outboxes_.push_back(std::move(box));
  }

  for (const auto& range : ranges) {
    s = EnsureGroup(range);
    if (!s.ok()) return s;
  }

  std::string host;
  uint16_t port;
  s = net::ParseAddr(self_->raft_addr, &host, &port);
  if (!s.ok()) return s;
  s = raft_server_.Start(host, port, [this](std::string_view req, std::string* resp) {
    (void)resp;
    OnRaftFrame(req);
    return true;
  });
  if (!s.ok()) return s;

  for (size_t i = 0; i < outboxes_.size(); i++) {
    sender_threads_.emplace_back(&ShardedNode::SenderLoop, this, i);
  }
  tick_thread_ = std::thread(&ShardedNode::TickLoop, this);
  FLOG_INFO("node started: %zu range(s), cluster size %zu", ranges.size(),
            options_.cluster.nodes.size());
  return Status::OK();
}

Status ShardedNode::EnsureGroup(const RangeDesc& range) {
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    if (groups_.count(range.id) > 0) return Status::OK();
  }
  RaftGroup::GroupOptions gopts;
  gopts.group_id = range.id;
  gopts.range = range;
  gopts.raft_dir = (std::filesystem::path(options_.data_dir) /
                    ("raft-g" + std::to_string(range.id)))
                       .string();
  gopts.node_id = options_.id;
  for (const auto& n : options_.cluster.nodes) gopts.peers.push_back(n.id);
  gopts.election_timeout_min_ticks = options_.election_timeout_min_ticks;
  gopts.election_timeout_max_ticks = options_.election_timeout_max_ticks;
  gopts.heartbeat_interval_ticks = options_.heartbeat_interval_ticks;
  gopts.request_timeout_ms = options_.request_timeout_ms;
  gopts.snapshot_interval_entries = options_.snapshot_interval_entries;

  std::unique_ptr<RaftGroup> group;
  Status s = RaftGroup::Start(gopts, db_.get(), this, &group);
  if (!s.ok()) return s;

  std::lock_guard<std::mutex> lock(groups_mutex_);
  if (groups_.count(range.id) > 0) return Status::OK();  // lost a race
  RangeDesc current = group->range();  // durable state may be narrower
  groups_[range.id] = std::move(group);
  routing_[current.start] = range.id;
  return Status::OK();
}

void ShardedNode::Stop() {
  if (stopping_.exchange(true)) return;
  raft_server_.Stop();
  std::vector<std::shared_ptr<RaftGroup>> groups;
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    for (auto& [id, g] : groups_) groups.push_back(g);
  }
  for (auto& g : groups) g->Stop();
  for (auto& box : outboxes_) {
    std::lock_guard<std::mutex> lock(box->mutex);
    box->cv.notify_all();
  }
  for (auto& t : sender_threads_) {
    if (t.joinable()) t.join();
  }
  if (tick_thread_.joinable()) tick_thread_.join();
}

void ShardedNode::TickLoop() {
  while (!stopping_.load()) {
    ::usleep(static_cast<useconds_t>(options_.tick_ms) * 1000);
    std::vector<std::shared_ptr<RaftGroup>> groups;
    {
      std::lock_guard<std::mutex> lock(groups_mutex_);
      for (auto& [id, g] : groups_) groups.push_back(g);
    }
    for (auto& g : groups) {
      if (stopping_.load()) return;
      g->Tick();
    }
  }
}

void ShardedNode::SendRaft(raft::NodeId to, uint32_t group_id, const std::string& body) {
  std::string payload;
  payload.push_back(static_cast<char>(net::MsgType::kRaft));
  PutFixed32(&payload, group_id);
  payload += body;
  for (auto& box : outboxes_) {
    if (box->id != to) continue;
    std::lock_guard<std::mutex> lock(box->mutex);
    if (box->frames.size() >= kMaxOutboxFrames) box->frames.pop_front();
    box->frames.push_back(std::move(payload));
    box->cv.notify_one();
    return;
  }
}

void ShardedNode::SenderLoop(size_t peer_slot) {
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
      ::usleep(20 * 1000);
      continue;
    }
    if (!net::WriteFrame(fd, frame).ok()) {
      net::CloseSocket(fd);
      fd = -1;
    }
  }
  if (fd >= 0) net::CloseSocket(fd);
}

void ShardedNode::OnRaftFrame(std::string_view payload) {
  std::string_view body;
  if (!net::DecodeIsRaftFrame(payload, &body)) return;
  if (body.size() < 4) return;
  uint32_t group_id = DecodeFixed32(body.data());
  raft::Message m;
  if (!raft::DecodeMessage(body.substr(4), &m)) return;
  // Frames for groups this replica has not yet created (its parent split is
  // still ahead of us) are dropped; raft retransmits.
  auto group = GroupById(group_id);
  if (group == nullptr) return;
  group->StepMessage(m);
}

std::shared_ptr<RaftGroup> ShardedNode::GroupById(uint32_t id) {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  auto it = groups_.find(id);
  return it == groups_.end() ? nullptr : it->second;
}

std::shared_ptr<RaftGroup> ShardedNode::RouteToGroup(std::string_view key) {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  auto it = routing_.upper_bound(std::string(key));
  if (it == routing_.begin()) return nullptr;
  --it;
  auto git = groups_.find(it->second);
  return git == groups_.end() ? nullptr : git->second;
}

std::vector<RangeDesc> ShardedNode::RangesIntersecting(const std::string& start,
                                                       const std::string& end) {
  std::vector<RangeDesc> out;
  std::lock_guard<std::mutex> lock(groups_mutex_);
  for (const auto& [range_start, id] : routing_) {
    auto git = groups_.find(id);
    if (git == groups_.end()) continue;
    RangeDesc d = git->second->range();
    bool starts_before_end = end.empty() || d.start < end;
    bool ends_after_start = d.end.empty() || d.end > start;
    if (starts_before_end && ends_after_start) out.push_back(std::move(d));
  }
  return out;
}

uint32_t ShardedNode::AllocateRangeId() {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  uint32_t max_id = 0;
  for (const auto& [id, g] : groups_) max_id = std::max(max_id, id);
  return max_id + 1;
}

void ShardedNode::OnSplitApplied(uint32_t parent_id, const RangeDesc& parent_now,
                                 const RangeDesc& child) {
  (void)parent_id;
  (void)parent_now;  // parent start is unchanged; routing keyed by start
  Status s = EnsureGroup(child);
  if (!s.ok()) {
    FLOG_ERROR("creating split child group %u failed, aborting: %s", child.id,
               s.ToString().c_str());
    abort();
  }
}

void ShardedNode::OnSnapshotRestored(uint32_t group_id, const RangeDesc& range,
                                     const std::vector<RangeDesc>& spawned) {
  (void)group_id;
  (void)range;
  for (const auto& child : spawned) {
    Status s = EnsureGroup(child);
    if (!s.ok()) {
      FLOG_ERROR("creating spawned group %u failed, aborting: %s", child.id,
                 s.ToString().c_str());
      abort();
    }
  }
}

net::Response ShardedNode::NotLeaderResponse(RaftGroup* group) {
  net::Response resp =
      net::Response::FromStatus(Status::NotLeader("not the leader for this range"));
  const NodeInfo* info = options_.cluster.Find(group->LeaderId());
  if (info != nullptr) resp.leader_addr = info->client_addr;
  return resp;
}

bool ShardedNode::IsLeader() {
  auto group = RouteToGroup("");
  return group != nullptr && group->IsLeader();
}

size_t ShardedNode::GroupCount() {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  return groups_.size();
}

bool ShardedNode::HandleClientFrame(std::string_view payload, std::string* out) {
  net::Request req;
  if (!DecodeRequest(payload, &req)) return false;
  *out = EncodeResponse(Handle(req));
  return true;
}

net::Response ShardedNode::ForwardScan(const std::string& addr, const net::Request& req) {
  int fd = -1;
  Status s = net::Connect(addr, &fd);
  if (!s.ok()) return net::Response::FromStatus(s);
  net::Response resp;
  s = net::WriteFrame(fd, EncodeRequest(req));
  std::string payload;
  if (s.ok()) s = net::ReadFrame(fd, &payload);
  net::CloseSocket(fd);
  if (!s.ok()) return net::Response::FromStatus(s);
  if (!DecodeResponse(payload, &resp)) {
    return net::Response::FromStatus(Status::Corruption("bad forwarded response"));
  }
  return resp;
}

net::Response ShardedNode::ScanAcrossRanges(const net::Request& req) {
  uint32_t limit = req.limit == 0 ? kDefaultScanLimit : req.limit;
  net::Response resp;
  for (const RangeDesc& range : RangesIntersecting(req.key, req.end_key)) {
    if (resp.kvs.size() >= limit) break;
    std::string sub_start = std::max(req.key, range.start);
    std::string sub_end = range.end;
    if (!req.end_key.empty() && (sub_end.empty() || req.end_key < sub_end)) {
      sub_end = req.end_key;
    }
    auto group = GroupById(range.id);
    if (group == nullptr) continue;

    if (group->IsLeader()) {
      Status s = group->LinearizableReadBarrier();
      if (s.IsNotLeader()) return NotLeaderResponse(group.get());
      if (!s.ok()) return net::Response::FromStatus(s);
      auto it = db_->NewIterator();
      if (sub_start.empty()) {
        it->SeekToFirst();
      } else {
        it->Seek(sub_start);
      }
      while (it->Valid() && resp.kvs.size() < limit) {
        std::string_view key = it->key();
        if (!sub_end.empty() && key >= sub_end) break;
        if (!IsSystemKey(key)) {
          resp.kvs.emplace_back(std::string(key), std::string(it->value()));
        }
        it->Next();
      }
      continue;
    }

    if (req.flags & net::kRequestNoForward) return NotLeaderResponse(group.get());
    const NodeInfo* info = options_.cluster.Find(group->LeaderId());
    if (info == nullptr) return NotLeaderResponse(group.get());
    net::Request sub = req;
    sub.key = sub_start;
    sub.end_key = sub_end;
    sub.limit = limit - static_cast<uint32_t>(resp.kvs.size());
    sub.flags |= net::kRequestNoForward;
    net::Response sub_resp = ForwardScan(info->client_addr, sub);
    if (!sub_resp.ok()) return sub_resp;
    for (auto& kv : sub_resp.kvs) resp.kvs.push_back(std::move(kv));
  }
  return resp;
}

net::Response ShardedNode::Handle(const net::Request& req) {
  using net::MsgType;
  net::Response resp;
  switch (req.type) {
    case MsgType::kGet: {
      auto group = RouteToGroup(req.key);
      if (group == nullptr) {
        return net::Response::FromStatus(Status::IOError("no range for key"));
      }
      Status s = group->LinearizableReadBarrier();
      if (s.IsNotLeader()) return NotLeaderResponse(group.get());
      if (!s.ok()) return net::Response::FromStatus(s);
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
    case MsgType::kPut:
    case MsgType::kDelete: {
      auto group = RouteToGroup(req.key);
      if (group == nullptr) {
        return net::Response::FromStatus(Status::IOError("no range for key"));
      }
      Status s = req.type == MsgType::kPut ? group->Put(req.key, req.value)
                                           : group->Delete(req.key);
      if (s.IsNotLeader()) return NotLeaderResponse(group.get());
      if (!s.ok()) return net::Response::FromStatus(s);
      break;
    }
    case MsgType::kScan:
      return ScanAcrossRanges(req);
    case MsgType::kSplit: {
      auto group = RouteToGroup(req.key);
      if (group == nullptr) {
        return net::Response::FromStatus(Status::IOError("no range for key"));
      }
      Status s = group->Split(req.key, AllocateRangeId());
      if (s.IsNotLeader()) return NotLeaderResponse(group.get());
      if (!s.ok()) return net::Response::FromStatus(s);
      break;
    }
    case MsgType::kRanges: {
      std::vector<std::shared_ptr<RaftGroup>> groups;
      {
        std::lock_guard<std::mutex> lock(groups_mutex_);
        for (auto& [id, g] : groups_) groups.push_back(g);
      }
      for (auto& g : groups) {
        RangeDesc d = g->range();
        std::string bounds = "[" + (d.start.empty() ? "-inf" : d.start) + ", " +
                             (d.end.empty() ? "+inf" : d.end) + ")";
        resp.kvs.emplace_back(
            "range " + std::to_string(d.id),
            bounds + " leader=node" + std::to_string(g->LeaderId()));
      }
      break;
    }
    case MsgType::kStatus: {
      auto first = RouteToGroup("");
      if (first != nullptr) {
        RaftGroup::Info info = first->GetInfo();
        const NodeInfo* leader_info = options_.cluster.Find(info.leader);
        resp.kvs.emplace_back("node_id", std::to_string(options_.id));
        resp.kvs.emplace_back("role", info.role);
        resp.kvs.emplace_back("term", std::to_string(info.term));
        resp.kvs.emplace_back("leader_id", std::to_string(info.leader));
        resp.kvs.emplace_back("leader_addr",
                              leader_info ? leader_info->client_addr : "");
        resp.kvs.emplace_back("commit_index", std::to_string(info.commit));
        resp.kvs.emplace_back("applied_index", std::to_string(info.applied));
        resp.kvs.emplace_back("last_log_index", std::to_string(info.last_log));
        resp.kvs.emplace_back("snapshot_index", std::to_string(info.snapshot_index));
      }
      resp.kvs.emplace_back("ranges", std::to_string(GroupCount()));
      std::vector<std::shared_ptr<RaftGroup>> groups;
      {
        std::lock_guard<std::mutex> lock(groups_mutex_);
        for (auto& [id, g] : groups_) groups.push_back(g);
      }
      for (auto& g : groups) {
        RaftGroup::Info info = g->GetInfo();
        resp.kvs.emplace_back(
            "g" + std::to_string(g->id()),
            std::string(info.role) + " term=" + std::to_string(info.term) +
                " applied=" + std::to_string(info.applied) +
                " snap=" + std::to_string(info.snapshot_index));
      }
      break;
    }
    default:
      return net::Response::FromStatus(
          Status::InvalidArgument("unsupported request type"));
  }
  return resp;
}

}  // namespace flotilla::server
