#pragma once

#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace flotilla::raft {

using NodeId = uint32_t;  // 1-based; 0 means "none"

struct LogEntry {
  uint64_t term = 0;
  uint64_t index = 0;
  std::string command;  // empty = leader no-op
};

enum class MsgKind : uint8_t {
  kRequestVote = 1,
  kRequestVoteResp = 2,
  kAppendEntries = 3,
  kAppendEntriesResp = 4,
  kInstallSnapshot = 5,
  kInstallSnapshotResp = 6,
};

struct Message {
  MsgKind kind = MsgKind::kRequestVote;
  NodeId from = 0;
  NodeId to = 0;
  uint64_t term = 0;

  uint64_t last_log_index = 0;  // RequestVote
  uint64_t last_log_term = 0;   // RequestVote
  bool granted = false;         // RequestVoteResp

  uint64_t prev_index = 0;      // AppendEntries
  uint64_t prev_term = 0;       // AppendEntries
  uint64_t commit = 0;          // AppendEntries
  uint64_t hb_seq = 0;          // AppendEntries + resp echo (ReadIndex quorum)
  std::vector<LogEntry> entries;
  bool success = false;         // AppendEntriesResp / InstallSnapshotResp
  uint64_t match_index = 0;     // AppendEntriesResp / InstallSnapshotResp
  uint64_t hint_index = 0;      // AppendEntriesResp conflict hint

  uint64_t snap_index = 0;      // InstallSnapshot
  uint64_t snap_term = 0;       // InstallSnapshot
  std::string snap_data;        // InstallSnapshot
};

struct HardState {
  uint64_t term = 0;
  NodeId voted_for = 0;
};

struct Snapshot {
  uint64_t last_index = 0;
  uint64_t last_term = 0;
  std::string data;
};

enum class Role { kFollower, kCandidate, kLeader };

// Side effects for the host to execute, in this order: truncate log, append
// entries, persist hard state, send messages, apply committed, complete reads.
struct Ready {
  bool hard_state_changed = false;
  HardState hard_state;
  uint64_t truncate_from = 0;  // if nonzero, drop persisted entries with index >= this
  std::vector<LogEntry> entries_to_append;
  std::vector<Message> messages;
  std::vector<LogEntry> committed;
  std::vector<std::pair<uint64_t, uint64_t>> confirmed_reads;  // (ctx, read_index)
  std::optional<Snapshot> snapshot_to_apply;  // restore state machine before applying

  bool Empty() const {
    return !hard_state_changed && truncate_from == 0 && entries_to_append.empty() &&
           messages.empty() && committed.empty() && confirmed_reads.empty() &&
           !snapshot_to_apply.has_value();
  }
};

struct Config {
  NodeId id = 0;
  std::vector<NodeId> peers;  // all cluster members, including self
  int election_timeout_min = 10;  // ticks
  int election_timeout_max = 20;
  int heartbeat_interval = 2;
  size_t max_entries_per_msg = 128;
  size_t max_bytes_per_msg = 1 << 20;
  uint64_t rng_seed = 1;
};

// Deterministic Raft core: no threads, no IO, no wall clock. Everything
// enters via Tick/Step/Propose/StartReadIndex and leaves via TakeReady.
class Raft {
 public:
  Raft(Config config, HardState hs, std::vector<LogEntry> log, Snapshot base);

  void Tick();
  void Step(const Message& m);
  // Leader-only; appends to the local log and begins replication.
  bool Propose(std::string command, uint64_t* index, uint64_t* term);
  // Leader-only; ctx is echoed back in Ready::confirmed_reads.
  bool StartReadIndex(uint64_t ctx);
  Ready TakeReady();

  // Snapshot support (milestone 5): the host reports that the state machine
  // is durably snapshotted through applied_index; the core drops covered log
  // entries. snapshot_data is what will be sent to lagging followers.
  void CompactTo(uint64_t applied_index, std::string snapshot_data);

  Role role() const { return role_; }
  NodeId id() const { return config_.id; }
  NodeId leader() const { return leader_; }
  uint64_t term() const { return hard_.term; }
  uint64_t commit_index() const { return commit_; }
  uint64_t last_index() const { return base_.last_index + log_.size(); }
  const Snapshot& base_snapshot() const { return base_; }

 private:
  uint64_t TermAt(uint64_t index) const;  // 0 if unknown/compacted-below-base
  const LogEntry& At(uint64_t index) const;
  uint64_t LastTerm() const { return TermAt(last_index()); }

  void BecomeFollower(uint64_t term, NodeId leader);
  void StartElection();
  void BecomeLeader();
  void BroadcastAppend(bool heartbeat_only);
  void SendAppend(NodeId peer);
  void MaybeAdvanceCommit();
  void EmitCommitted();
  void ResetElectionTimer();
  void PersistHardState();
  void HandleRequestVote(const Message& m);
  void HandleRequestVoteResp(const Message& m);
  void HandleAppendEntries(const Message& m);
  void HandleAppendEntriesResp(const Message& m);
  void HandleInstallSnapshot(const Message& m);
  void HandleInstallSnapshotResp(const Message& m);
  void Send(Message m);

  Config config_;
  HardState hard_;
  std::vector<LogEntry> log_;  // log_[i] has index base_.last_index + 1 + i
  Snapshot base_;

  Role role_ = Role::kFollower;
  NodeId leader_ = 0;
  uint64_t commit_ = 0;
  uint64_t last_emitted_ = 0;  // committed entries handed out via Ready

  int election_elapsed_ = 0;
  int election_deadline_ = 0;
  int heartbeat_elapsed_ = 0;
  std::unordered_map<NodeId, bool> votes_;
  std::unordered_map<NodeId, uint64_t> next_;
  std::unordered_map<NodeId, uint64_t> match_;
  std::unordered_map<NodeId, uint64_t> hb_acked_;  // highest hb_seq echoed per peer
  uint64_t hb_seq_ = 0;

  struct PendingRead {
    uint64_t ctx;
    uint64_t read_index;
    uint64_t hb_seq;
  };
  std::vector<PendingRead> pending_reads_;

  std::mt19937_64 rng_;
  Ready ready_;
};

}  // namespace flotilla::raft
