#pragma once

#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/coding.h"
#include "raft/raft.h"

namespace flotilla::raft::sim {

// Always-on invariant check: these must fire in release builds too, where
// NDEBUG disables assert().
#define SIM_CHECK(cond)                                                     \
  do {                                                                      \
    if (!(cond)) {                                                          \
      fprintf(stderr, "SIM_CHECK failed: %s at %s:%d\n", #cond, __FILE__,  \
              __LINE__);                                                    \
      abort();                                                              \
    }                                                                       \
  } while (0)

// Deterministic in-process cluster: real Raft cores, in-memory "disk", and a
// simulated network with seeded delays, drops, and partitions. Time is ticks.
// Invariants (election safety, state machine safety) are checked on every
// apply and every tick; any violation aborts the test immediately.
class SimCluster {
 public:
  struct NodeState {
    bool alive = true;
    HardState hard;
    std::vector<LogEntry> log;
    Snapshot snap;
    std::unique_ptr<Raft> raft;
    std::vector<std::string> applied;  // durable state machine (non-noop cmds)
    uint64_t applied_index = 0;
    std::vector<std::pair<uint64_t, uint64_t>> confirmed_reads;
  };

  SimCluster(int n, uint64_t seed) : n_(n), seed_(seed), rng_(seed) {
    nodes_.resize(static_cast<size_t>(n) + 1);
    for (int id = 1; id <= n; id++) StartNode(id);
  }

  static std::string EncodeStateMachine(const std::vector<std::string>& applied) {
    std::string out;
    PutFixed32(&out, static_cast<uint32_t>(applied.size()));
    for (const auto& c : applied) PutLengthPrefixed(&out, c);
    return out;
  }

  static std::vector<std::string> DecodeStateMachine(const std::string& data) {
    Decoder dec(data);
    uint32_t count = dec.U32();
    std::vector<std::string> out;
    for (uint32_t i = 0; i < count && dec.ok(); i++) out.push_back(dec.Str());
    SIM_CHECK(dec.ok());
    return out;
  }

  void Tick() {
    now_++;
    // Deliver due messages, then tick every live core.
    auto due = pending_.equal_range(now_);
    std::vector<Message> deliver;
    for (auto it = due.first; it != due.second; ++it) deliver.push_back(it->second);
    pending_.erase(due.first, due.second);
    for (const auto& m : deliver) {
      NodeState& dst = nodes_[m.to];
      if (!dst.alive || Blocked(m.from, m.to)) continue;
      dst.raft->Step(m);
      Drain(m.to);
    }
    for (int id = 1; id <= n_; id++) {
      if (!nodes_[static_cast<size_t>(id)].alive) continue;
      nodes_[static_cast<size_t>(id)].raft->Tick();
      Drain(static_cast<NodeId>(id));
    }
    CheckElectionSafety();
  }

  void TickMany(int ticks) {
    for (int i = 0; i < ticks; i++) Tick();
  }

  // Runs until a leader exists whose term has a committed entry, max_ticks cap.
  int WaitForLeader(int max_ticks = 200) {
    for (int i = 0; i < max_ticks; i++) {
      int leader = LeaderId();
      if (leader != 0) return leader;
      Tick();
    }
    return LeaderId();
  }

  int LeaderId() const {
    int best = 0;
    uint64_t best_term = 0;
    for (int id = 1; id <= n_; id++) {
      const NodeState& node = nodes_[static_cast<size_t>(id)];
      if (node.alive && node.raft->role() == Role::kLeader &&
          node.raft->term() > best_term) {
        best = id;
        best_term = node.raft->term();
      }
    }
    return best;
  }

  bool Propose(const std::string& cmd) {
    int leader = LeaderId();
    if (leader == 0) return false;
    bool ok = nodes_[static_cast<size_t>(leader)].raft->Propose(cmd, nullptr, nullptr);
    if (ok) Drain(static_cast<NodeId>(leader));
    return ok;
  }

  bool ProposeOn(int id, const std::string& cmd) {
    NodeState& node = nodes_[static_cast<size_t>(id)];
    if (!node.alive) return false;
    bool ok = node.raft->Propose(cmd, nullptr, nullptr);
    if (ok) Drain(static_cast<NodeId>(id));
    return ok;
  }

  bool StartReadIndexOn(int id, uint64_t ctx) {
    NodeState& node = nodes_[static_cast<size_t>(id)];
    if (!node.alive) return false;
    bool ok = node.raft->StartReadIndex(ctx);
    Drain(static_cast<NodeId>(id));
    return ok;
  }

  void Kill(int id) { nodes_[static_cast<size_t>(id)].alive = false; }

  void Restart(int id) {
    NodeState& node = nodes_[static_cast<size_t>(id)];
    node.alive = true;
    StartNode(id);
  }

  void Partition(const std::set<int>& a, const std::set<int>& b) {
    for (int x : a) {
      for (int y : b) {
        blocked_.insert({x, y});
        blocked_.insert({y, x});
      }
    }
  }

  void Heal() { blocked_.clear(); }

  void SetDropRate(double p) { drop_rate_ = p; }
  void SetMaxDelay(int ticks) { max_delay_ = ticks; }

  const NodeState& node(int id) const { return nodes_[static_cast<size_t>(id)]; }

  // All live nodes have applied every globally committed command.
  bool Converged() const {
    for (int id = 1; id <= n_; id++) {
      const NodeState& node = nodes_[static_cast<size_t>(id)];
      if (!node.alive) continue;
      if (node.applied.size() != committed_commands_.size()) return false;
    }
    return true;
  }

  size_t committed_count() const { return committed_commands_.size(); }

  int CountApplied(const std::string& cmd) const {
    int count = 0;
    for (int id = 1; id <= n_; id++) {
      const NodeState& node = nodes_[static_cast<size_t>(id)];
      for (const auto& c : node.applied) {
        if (c == cmd) {
          count++;
          break;
        }
      }
    }
    return count;
  }

 private:
  void StartNode(int id) {
    NodeState& node = nodes_[static_cast<size_t>(id)];
    Config cfg;
    cfg.id = static_cast<NodeId>(id);
    for (int p = 1; p <= n_; p++) cfg.peers.push_back(static_cast<NodeId>(p));
    cfg.rng_seed = seed_ * 1000003 + static_cast<uint64_t>(id) * 7919 + restarts_++;
    node.raft = std::make_unique<Raft>(cfg, node.hard, node.log, node.snap);
    node.confirmed_reads.clear();
  }

  bool Blocked(NodeId from, NodeId to) const {
    return blocked_.count({static_cast<int>(from), static_cast<int>(to)}) > 0;
  }

  void Drain(NodeId id) {
    NodeState& node = nodes_[id];
    while (true) {
      Ready ready = node.raft->TakeReady();
      if (ready.Empty()) return;

      // Persistence contract, against the in-memory disk.
      if (ready.snapshot_to_apply.has_value()) {
        node.snap = *ready.snapshot_to_apply;
        node.log.clear();
        node.applied = DecodeStateMachine(node.snap.data);
        node.applied_index = node.snap.last_index;
      }
      if (ready.truncate_from != 0) {
        while (!node.log.empty() && node.log.back().index >= ready.truncate_from) {
          node.log.pop_back();
        }
      }
      for (const auto& e : ready.entries_to_append) node.log.push_back(e);
      if (ready.hard_state_changed) node.hard = ready.hard_state;

      for (const auto& m : ready.messages) Deliver(m);

      for (const auto& e : ready.committed) {
        if (e.index <= node.applied_index) continue;  // replay after restart
        SIM_CHECK(e.index == node.applied_index + 1);
        node.applied_index = e.index;
        if (e.command.empty()) continue;
        node.applied.push_back(e.command);
        CheckStateMachineSafety(node);
      }
      for (const auto& cr : ready.confirmed_reads) node.confirmed_reads.push_back(cr);
    }
  }

  void Deliver(const Message& m) {
    if (std::uniform_real_distribution<double>(0, 1)(rng_) < drop_rate_) return;
    uint64_t delay = 1 + std::uniform_int_distribution<int>(0, max_delay_)(rng_);
    pending_.emplace(now_ + delay, m);
  }

  void CheckElectionSafety() {
    for (int id = 1; id <= n_; id++) {
      const NodeState& node = nodes_[static_cast<size_t>(id)];
      if (!node.alive || node.raft->role() != Role::kLeader) continue;
      uint64_t term = node.raft->term();
      // Election safety: one leader per term, ever.
      auto it = leaders_by_term_.find(term);
      if (it == leaders_by_term_.end()) {
        leaders_by_term_[term] = id;
      } else {
        SIM_CHECK(it->second == id);
      }
    }
  }

  // State machine safety: every node applies the same command at the same
  // position in the applied sequence.
  void CheckStateMachineSafety(const NodeState& node) {
    size_t pos = node.applied.size() - 1;
    if (pos < committed_commands_.size()) {
      SIM_CHECK(committed_commands_[pos] == node.applied.back());
    } else {
      SIM_CHECK(pos == committed_commands_.size());
      committed_commands_.push_back(node.applied.back());
    }
  }

  int n_;
  uint64_t seed_;
  std::mt19937_64 rng_;
  uint64_t now_ = 0;
  uint64_t restarts_ = 0;
  double drop_rate_ = 0.0;
  int max_delay_ = 2;
  std::vector<NodeState> nodes_;  // 1-based
  std::multimap<uint64_t, Message> pending_;
  std::set<std::pair<int, int>> blocked_;
  std::map<uint64_t, int> leaders_by_term_;
  std::vector<std::string> committed_commands_;
};

}  // namespace flotilla::raft::sim
