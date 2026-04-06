#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/messages.h"
#include "net/tcp_server.h"
#include "server/cluster_config.h"
#include "server/raft_group.h"
#include "storage/db.h"

namespace flotilla::server {

// One database node hosting many Raft groups (multi-raft) over one shared
// storage engine. Owns the raft-port transport (frames carry a group id),
// the routing table, and client request dispatch. With a single range this
// behaves exactly like a plain raft KV node.
class ShardedNode : public GroupHost {
 public:
  struct NodeOptions {
    std::string data_dir;
    raft::NodeId id = 0;
    ClusterConfig cluster;
    storage::Options db_options;
    int tick_ms = 10;
    int election_timeout_min_ticks = 15;
    int election_timeout_max_ticks = 30;
    int heartbeat_interval_ticks = 2;
    int request_timeout_ms = 5000;
    uint64_t snapshot_interval_entries = 8192;
  };

  static Status Start(const NodeOptions& options, std::unique_ptr<ShardedNode>* out);
  ~ShardedNode() override;

  void Stop();

  net::Response Handle(const net::Request& req);
  bool HandleClientFrame(std::string_view payload, std::string* out);

  // Leader of the range containing the empty key (tests and demos).
  bool IsLeader();
  size_t GroupCount();

  // GroupHost:
  void SendRaft(raft::NodeId to, uint32_t group_id, const std::string& body) override;
  void OnSplitApplied(uint32_t parent_id, const RangeDesc& parent_now,
                      const RangeDesc& child) override;
  void OnSnapshotRestored(uint32_t group_id, const RangeDesc& range,
                          const std::vector<RangeDesc>& spawned) override;

 private:
  explicit ShardedNode(NodeOptions options) : options_(std::move(options)) {}

  Status Init();
  Status EnsureGroup(const RangeDesc& range);
  std::shared_ptr<RaftGroup> RouteToGroup(std::string_view key);
  std::shared_ptr<RaftGroup> GroupById(uint32_t id);
  std::vector<RangeDesc> RangesIntersecting(const std::string& start,
                                            const std::string& end);
  uint32_t AllocateRangeId();
  net::Response NotLeaderResponse(RaftGroup* group);
  net::Response ScanAcrossRanges(const net::Request& req);
  net::Response TxnScanAcrossRanges(const net::Request& req);
  net::Response ForwardScan(const std::string& addr, const net::Request& req);

  void TickLoop();
  void OnRaftFrame(std::string_view payload);
  void SenderLoop(size_t peer_slot);

  NodeOptions options_;
  const NodeInfo* self_ = nullptr;

  std::unique_ptr<storage::DB> db_;
  std::atomic<bool> stopping_{false};

  std::mutex groups_mutex_;
  std::map<uint32_t, std::shared_ptr<RaftGroup>> groups_;
  std::map<std::string, uint32_t> routing_;  // range start -> range id

  struct PeerOutbox {
    raft::NodeId id;
    std::string addr;
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<std::string> frames;
  };
  std::vector<std::unique_ptr<PeerOutbox>> outboxes_;
  std::vector<std::thread> sender_threads_;

  net::TcpServer raft_server_;
  std::thread tick_thread_;
};

}  // namespace flotilla::server
