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
#include "raft/raft.h"
#include "raft/raft_storage.h"
#include "server/cluster_config.h"
#include "storage/db.h"

namespace flotilla::server {

struct RangeDesc {
  uint32_t id = 0;
  std::string start;
  std::string end;  // empty = +infinity

  bool Contains(std::string_view key) const {
    return key >= start && (end.empty() || key < end);
  }
};

// Callbacks a group makes into its hosting node. Implementations must be
// thread-safe; they are invoked from group tick/apply/step paths.
class GroupHost {
 public:
  virtual ~GroupHost() = default;
  virtual void SendRaft(raft::NodeId to, uint32_t group_id, const std::string& body) = 0;
  // Called on apply of a split: create the child group (idempotent) and
  // update routing for both parent and child.
  virtual void OnSplitApplied(uint32_t parent_id, const RangeDesc& parent_now,
                              const RangeDesc& child) = 0;
  // Called when a snapshot restore changes this group's range or reveals
  // children spawned while this replica was behind.
  virtual void OnSnapshotRestored(uint32_t group_id, const RangeDesc& range,
                                  const std::vector<RangeDesc>& spawned) = 0;
};

// One Raft consensus group over one key range. All groups on a node share the
// node's storage engine; a group only ever touches user keys inside its range
// plus its own system keys. Commands: 1=put 2=delete 3=split.
class RaftGroup {
 public:
  struct GroupOptions {
    uint32_t group_id = 0;
    RangeDesc range;
    std::string raft_dir;
    raft::NodeId node_id = 0;
    std::vector<raft::NodeId> peers;
    int election_timeout_min_ticks = 15;
    int election_timeout_max_ticks = 30;
    int heartbeat_interval_ticks = 2;
    int request_timeout_ms = 5000;
    uint64_t snapshot_interval_entries = 8192;
  };

  static Status Start(const GroupOptions& options, storage::DB* db, GroupHost* host,
                      std::unique_ptr<RaftGroup>* out);
  ~RaftGroup();

  void Stop();
  void Tick();                              // called by the host's ticker
  void StepMessage(const raft::Message& m); // called by the host's transport

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  // ReadIndex barrier; afterwards the caller reads the shared DB directly.
  Status LinearizableReadBarrier();
  // Proposes splitting this group's range at key, creating child_id.
  Status Split(std::string_view key, uint32_t child_id);

  // Transactional commands (Percolator state machine, ops 4-7). Logical
  // outcomes (Conflict/Aborted) come back as the returned status.
  Status TsTick(uint64_t* ts);
  Status TxnPrewrite(std::string_view key, std::string_view value, uint8_t wop,
                     uint64_t start_ts, std::string_view primary, uint64_t wall_ms);
  Status TxnCommit(std::string_view key, uint64_t start_ts, uint64_t commit_ts);
  Status TxnRollback(std::string_view key, uint64_t start_ts);

  uint32_t id() const { return options_.group_id; }
  RangeDesc range();  // current applied range
  bool IsLeader();
  raft::NodeId LeaderId();
  struct Info {
    const char* role;
    uint64_t term, commit, applied, last_log, snapshot_index;
    raft::NodeId leader;
  };
  Info GetInfo();

  static std::string EncodeCommand(uint8_t op, std::string_view key,
                                   std::string_view value);

 private:
  RaftGroup(GroupOptions options, storage::DB* db, GroupHost* host)
      : options_(std::move(options)), db_(db), host_(host) {}

  Status Init();
  Status ProposeAndWait(const std::string& command, uint64_t* applied_index = nullptr);
  void ApplyLoop();
  void DrainReady();  // requires raft_mutex_
  // Returns the command's logical outcome; Corruption/IOError are fatal.
  Status ApplyCommand(const raft::LogEntry& e);
  Status ApplySplit(uint32_t child_id, const std::string& split_key);
  void ApplySnapshot(const raft::Snapshot& snap);
  void MaybeSnapshot();
  std::string SerializeStateMachine();
  Status PersistAppliedMarker(uint64_t index);

  GroupOptions options_;
  storage::DB* db_;
  GroupHost* host_;

  std::mutex raft_mutex_;
  std::condition_variable raft_cv_;
  std::unique_ptr<raft::Raft> raft_;
  std::unique_ptr<raft::RaftStorage> raft_storage_;
  std::atomic<bool> stopping_{false};

  std::mutex range_mutex_;
  RangeDesc range_;
  std::vector<RangeDesc> spawned_;

  std::mutex apply_mutex_;
  std::condition_variable apply_cv_;
  std::deque<raft::LogEntry> apply_queue_;
  std::deque<raft::Snapshot> snapshot_queue_;
  uint64_t applied_index_ = 0;
  uint64_t applied_since_snapshot_ = 0;
  Status apply_error_;

  struct PendingProposal {
    uint64_t term;
    bool done = false;
    Status result;
  };
  std::map<uint64_t, std::shared_ptr<PendingProposal>> proposals_;

  struct PendingRead {
    bool confirmed = false;
    uint64_t read_index = 0;
  };
  uint64_t next_read_ctx_ = 1;
  std::map<uint64_t, std::shared_ptr<PendingRead>> reads_;

  std::thread apply_thread_;
};

// System key helpers shared with the sharded node.
std::string AppliedKey(uint32_t group_id);
std::string RangeKey(uint32_t range_id);
std::string SpawnsKey(uint32_t group_id);
std::string EncodeRangeDesc(const RangeDesc& d);
bool DecodeRangeDesc(uint32_t id, std::string_view data, RangeDesc* out);
inline bool IsSystemKey(std::string_view key) {
  return !key.empty() && key[0] == '\0';
}
// Raw client keys may not collide with system keys or the transactional
// keyspace ('!' prefixed).
inline bool IsReservedKey(std::string_view key) {
  return !key.empty() && (key[0] == '\0' || key[0] == '!');
}

}  // namespace flotilla::server
