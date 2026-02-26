#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/messages.h"
#include "net/tcp_server.h"
#include "raft/raft.h"
#include "raft/raft_storage.h"
#include "server/cluster_config.h"
#include "storage/db.h"

namespace flotilla::server {

// One consensus-backed KV node: raft core + raft storage behind a mutex, a
// tick thread, the raft-port TCP server, per-peer sender threads, and an
// apply thread feeding committed commands into the storage engine.
//
// Keys beginning with byte 0x00 are system keys (applied-index marker) and
// are hidden from client scans.
class RaftNode {
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
    // Take a snapshot and compact the raft log after this many applied
    // entries (0 disables; milestone 5 wiring).
    uint64_t snapshot_interval_entries = 8192;
  };

  static Status Start(const NodeOptions& options, std::unique_ptr<RaftNode>* out);
  ~RaftNode();

  void Stop();

  // Client-facing dispatch: consensus writes, ReadIndex reads, status.
  net::Response Handle(const net::Request& req);
  bool HandleClientFrame(std::string_view payload, std::string* out);

  bool IsLeader();
  uint16_t raft_port() const { return raft_server_.port(); }

 private:
  explicit RaftNode(NodeOptions options) : options_(std::move(options)) {}

  Status Init();
  Status ProposeAndWait(const std::string& command);
  Status LinearizableReadBarrier();
  std::string LeaderClientAddr();  // requires raft mutex
  net::Response NotLeaderResponse();

  void TickLoop();
  void ApplyLoop();
  void SenderLoop(size_t peer_slot);
  void OnRaftFrame(std::string_view payload);
  void DrainReady();  // requires raft mutex
  void ApplySnapshot(const raft::Snapshot& snap);
  void MaybeSnapshot();  // requires raft mutex

  static std::string EncodeCommand(uint8_t op, std::string_view key,
                                   std::string_view value);
  std::string SerializeStateMachine();

  NodeOptions options_;
  const NodeInfo* self_ = nullptr;

  std::unique_ptr<storage::DB> db_;
  std::unique_ptr<raft::RaftStorage> raft_storage_;

  std::mutex raft_mutex_;
  std::condition_variable raft_cv_;
  std::unique_ptr<raft::Raft> raft_;
  std::atomic<bool> stopping_{false};

  // Guards db_ replacement during snapshot restore; normal reads/writes take
  // it shared (the DB is internally thread-safe).
  std::shared_mutex db_lock_;

  // Apply pipeline.
  std::mutex apply_mutex_;
  std::condition_variable apply_cv_;
  std::deque<raft::LogEntry> apply_queue_;
  std::deque<raft::Snapshot> snapshot_queue_;
  uint64_t applied_index_ = 0;
  uint64_t applied_since_snapshot_ = 0;
  Status apply_error_;

  // Pending proposals keyed by log index.
  struct PendingProposal {
    uint64_t term;
    bool done = false;
    Status result;
  };
  std::map<uint64_t, std::shared_ptr<PendingProposal>> proposals_;

  // Pending reads keyed by context id.
  struct PendingRead {
    bool confirmed = false;
    uint64_t read_index = 0;
  };
  uint64_t next_read_ctx_ = 1;
  std::map<uint64_t, std::shared_ptr<PendingRead>> reads_;

  // Outboxes, one per peer, drained by sender threads.
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
  std::thread apply_thread_;
};

}  // namespace flotilla::server
