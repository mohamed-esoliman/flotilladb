#include "raft/raft.h"

#include <algorithm>
#include <cassert>

namespace flotilla::raft {

Raft::Raft(Config config, HardState hs, std::vector<LogEntry> log, Snapshot base)
    : config_(std::move(config)),
      hard_(hs),
      log_(std::move(log)),
      base_(std::move(base)),
      rng_(config_.rng_seed) {
  commit_ = base_.last_index;
  last_emitted_ = base_.last_index;
  ResetElectionTimer();
  for (size_t i = 0; i < log_.size(); i++) {
    assert(log_[i].index == base_.last_index + 1 + i);
  }
}

uint64_t Raft::TermAt(uint64_t index) const {
  if (index == base_.last_index) return base_.last_term;
  if (index > base_.last_index && index <= last_index()) {
    return log_[index - base_.last_index - 1].term;
  }
  return 0;
}

const LogEntry& Raft::At(uint64_t index) const {
  return log_[index - base_.last_index - 1];
}

void Raft::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> dist(config_.election_timeout_min,
                                          config_.election_timeout_max);
  election_deadline_ = dist(rng_);
}

void Raft::PersistHardState() {
  ready_.hard_state_changed = true;
  ready_.hard_state = hard_;
}

void Raft::Send(Message m) {
  m.from = config_.id;
  m.term = hard_.term;
  ready_.messages.push_back(std::move(m));
}

void Raft::Tick() {
  if (role_ == Role::kLeader) {
    heartbeat_elapsed_++;
    if (heartbeat_elapsed_ >= config_.heartbeat_interval) {
      heartbeat_elapsed_ = 0;
      hb_seq_++;
      BroadcastAppend(false);
    }
    return;
  }
  election_elapsed_++;
  if (election_elapsed_ >= election_deadline_) StartElection();
}

void Raft::BecomeFollower(uint64_t term, NodeId leader) {
  if (term > hard_.term) {
    hard_.term = term;
    hard_.voted_for = 0;
    PersistHardState();
  }
  role_ = Role::kFollower;
  leader_ = leader;
  votes_.clear();
  pending_reads_.clear();
  ResetElectionTimer();
}

void Raft::StartElection() {
  role_ = Role::kCandidate;
  hard_.term++;
  hard_.voted_for = config_.id;
  PersistHardState();
  leader_ = 0;
  votes_.clear();
  votes_[config_.id] = true;
  ResetElectionTimer();

  size_t quorum = config_.peers.size() / 2 + 1;
  if (votes_.size() >= quorum) {
    BecomeLeader();
    return;
  }
  for (NodeId peer : config_.peers) {
    if (peer == config_.id) continue;
    Message m;
    m.kind = MsgKind::kRequestVote;
    m.to = peer;
    m.last_log_index = last_index();
    m.last_log_term = LastTerm();
    Send(std::move(m));
  }
}

void Raft::BecomeLeader() {
  role_ = Role::kLeader;
  leader_ = config_.id;
  heartbeat_elapsed_ = 0;
  next_.clear();
  match_.clear();
  hb_acked_.clear();
  for (NodeId peer : config_.peers) {
    next_[peer] = last_index() + 1;
    match_[peer] = 0;
  }

  // No-op entry: commits the new term without client traffic, unblocking
  // ReadIndex and entries from previous terms (figure 8 rule).
  LogEntry noop;
  noop.term = hard_.term;
  noop.index = last_index() + 1;
  log_.push_back(noop);
  ready_.entries_to_append.push_back(noop);
  match_[config_.id] = last_index();

  hb_seq_++;
  BroadcastAppend(false);
  MaybeAdvanceCommit();
}

bool Raft::Propose(std::string command, uint64_t* index, uint64_t* term) {
  if (role_ != Role::kLeader) return false;
  LogEntry e;
  e.term = hard_.term;
  e.index = last_index() + 1;
  e.command = std::move(command);
  log_.push_back(e);
  ready_.entries_to_append.push_back(e);
  match_[config_.id] = last_index();
  if (index != nullptr) *index = e.index;
  if (term != nullptr) *term = e.term;
  BroadcastAppend(false);
  MaybeAdvanceCommit();
  return true;
}

bool Raft::StartReadIndex(uint64_t ctx) {
  if (role_ != Role::kLeader) return false;
  // Until the term's no-op commits, our commit index may lag entries a prior
  // leader already acked; confirming a read here could serve stale data.
  if (TermAt(commit_) != hard_.term) return false;

  if (config_.peers.size() == 1) {
    ready_.confirmed_reads.emplace_back(ctx, commit_);
    return true;
  }
  hb_seq_++;
  pending_reads_.push_back({ctx, commit_, hb_seq_});
  BroadcastAppend(true);
  return true;
}

void Raft::BroadcastAppend(bool heartbeat_only) {
  (void)heartbeat_only;  // followers get pending entries either way
  for (NodeId peer : config_.peers) {
    if (peer == config_.id) continue;
    SendAppend(peer);
  }
}

void Raft::SendAppend(NodeId peer) {
  uint64_t next = next_[peer];
  if (next <= base_.last_index) {
    Message m;
    m.kind = MsgKind::kInstallSnapshot;
    m.to = peer;
    m.snap_index = base_.last_index;
    m.snap_term = base_.last_term;
    m.snap_data = base_.data;
    Send(std::move(m));
    return;
  }
  Message m;
  m.kind = MsgKind::kAppendEntries;
  m.to = peer;
  m.prev_index = next - 1;
  m.prev_term = TermAt(next - 1);
  m.commit = commit_;
  m.hb_seq = hb_seq_;
  size_t bytes = 0;
  for (uint64_t i = next; i <= last_index(); i++) {
    if (m.entries.size() >= config_.max_entries_per_msg) break;
    if (bytes > config_.max_bytes_per_msg) break;
    m.entries.push_back(At(i));
    bytes += At(i).command.size() + 24;
  }
  Send(std::move(m));
}

void Raft::MaybeAdvanceCommit() {
  if (role_ != Role::kLeader) return;
  size_t quorum = config_.peers.size() / 2 + 1;
  for (uint64_t n = last_index(); n > commit_; n--) {
    if (TermAt(n) != hard_.term) break;  // only own-term entries commit by counting
    size_t count = 0;
    for (NodeId peer : config_.peers) {
      if (match_[peer] >= n) count++;
    }
    if (count >= quorum) {
      commit_ = n;
      EmitCommitted();
      break;
    }
  }
}

void Raft::EmitCommitted() {
  while (last_emitted_ < commit_) {
    last_emitted_++;
    ready_.committed.push_back(At(last_emitted_));
  }
}

void Raft::Step(const Message& m) {
  if (m.term > hard_.term) {
    bool from_leader =
        m.kind == MsgKind::kAppendEntries || m.kind == MsgKind::kInstallSnapshot;
    BecomeFollower(m.term, from_leader ? m.from : 0);
  } else if (m.term < hard_.term) {
    // Stale sender: tell it about the newer term.
    if (m.kind == MsgKind::kRequestVote) {
      Message resp;
      resp.kind = MsgKind::kRequestVoteResp;
      resp.to = m.from;
      resp.granted = false;
      Send(std::move(resp));
    } else if (m.kind == MsgKind::kAppendEntries ||
               m.kind == MsgKind::kInstallSnapshot) {
      Message resp;
      resp.kind = m.kind == MsgKind::kAppendEntries ? MsgKind::kAppendEntriesResp
                                                    : MsgKind::kInstallSnapshotResp;
      resp.to = m.from;
      resp.success = false;
      Send(std::move(resp));
    }
    return;
  }

  switch (m.kind) {
    case MsgKind::kRequestVote: HandleRequestVote(m); break;
    case MsgKind::kRequestVoteResp: HandleRequestVoteResp(m); break;
    case MsgKind::kAppendEntries: HandleAppendEntries(m); break;
    case MsgKind::kAppendEntriesResp: HandleAppendEntriesResp(m); break;
    case MsgKind::kInstallSnapshot: HandleInstallSnapshot(m); break;
    case MsgKind::kInstallSnapshotResp: HandleInstallSnapshotResp(m); break;
  }
}

void Raft::HandleRequestVote(const Message& m) {
  bool up_to_date = m.last_log_term > LastTerm() ||
                    (m.last_log_term == LastTerm() && m.last_log_index >= last_index());
  bool can_vote = hard_.voted_for == 0 || hard_.voted_for == m.from;
  Message resp;
  resp.kind = MsgKind::kRequestVoteResp;
  resp.to = m.from;
  resp.granted = false;
  if (can_vote && up_to_date) {
    resp.granted = true;
    if (hard_.voted_for != m.from) {
      hard_.voted_for = m.from;
      PersistHardState();
    }
    ResetElectionTimer();
  }
  Send(std::move(resp));
}

void Raft::HandleRequestVoteResp(const Message& m) {
  if (role_ != Role::kCandidate) return;
  votes_[m.from] = m.granted;
  size_t granted = 0;
  for (const auto& [id, g] : votes_) {
    if (g) granted++;
  }
  size_t quorum = config_.peers.size() / 2 + 1;
  if (granted >= quorum) BecomeLeader();
}

void Raft::HandleAppendEntries(const Message& m) {
  if (role_ == Role::kCandidate) BecomeFollower(hard_.term, m.from);
  leader_ = m.from;
  ResetElectionTimer();

  Message resp;
  resp.kind = MsgKind::kAppendEntriesResp;
  resp.to = m.from;
  resp.hb_seq = m.hb_seq;

  uint64_t prev_index = m.prev_index;
  uint64_t prev_term = m.prev_term;
  std::vector<LogEntry> entries = m.entries;

  // Entries at or below our snapshot base are already committed and applied.
  if (prev_index < base_.last_index) {
    uint64_t covered = base_.last_index - prev_index;
    if (entries.size() <= covered) {
      resp.success = true;
      resp.match_index = std::max(base_.last_index, prev_index + entries.size());
      Send(std::move(resp));
      return;
    }
    entries.erase(entries.begin(), entries.begin() + static_cast<long>(covered));
    prev_index = base_.last_index;
    prev_term = base_.last_term;
  }

  if (prev_index > last_index() || TermAt(prev_index) != prev_term) {
    resp.success = false;
    if (prev_index > last_index()) {
      resp.hint_index = last_index() + 1;
    } else {
      // Back off to the first index of the conflicting term.
      uint64_t ct = TermAt(prev_index);
      uint64_t first = prev_index;
      while (first > base_.last_index + 1 && TermAt(first - 1) == ct) first--;
      resp.hint_index = first;
    }
    Send(std::move(resp));
    return;
  }

  for (size_t i = 0; i < entries.size(); i++) {
    const LogEntry& e = entries[i];
    if (e.index <= last_index()) {
      if (TermAt(e.index) == e.term) continue;  // already have it
      assert(e.index > commit_);
      log_.resize(e.index - base_.last_index - 1);
      if (ready_.truncate_from == 0 || e.index < ready_.truncate_from) {
        ready_.truncate_from = e.index;
      }
      std::erase_if(ready_.entries_to_append,
                    [&](const LogEntry& p) { return p.index >= e.index; });
    }
    log_.push_back(e);
    ready_.entries_to_append.push_back(e);
  }

  resp.success = true;
  resp.match_index = prev_index + entries.size();
  if (m.commit > commit_) {
    commit_ = std::min(m.commit, last_index());
    EmitCommitted();
  }
  Send(std::move(resp));
}

void Raft::HandleAppendEntriesResp(const Message& m) {
  if (role_ != Role::kLeader) return;
  uint64_t& acked = hb_acked_[m.from];
  acked = std::max(acked, m.hb_seq);

  if (m.success) {
    match_[m.from] = std::max(match_[m.from], m.match_index);
    next_[m.from] = std::max(next_[m.from], m.match_index + 1);
    MaybeAdvanceCommit();
  } else {
    uint64_t next = next_[m.from];
    uint64_t backoff = m.hint_index != 0 ? m.hint_index : next - 1;
    next_[m.from] = std::max<uint64_t>(1, std::min(backoff, next - 1));
    SendAppend(m.from);
  }

  // ReadIndex quorum: self plus peers that echoed a heartbeat sent at or
  // after the read was registered.
  if (!pending_reads_.empty()) {
    size_t quorum = config_.peers.size() / 2 + 1;
    std::vector<PendingRead> keep;
    for (const auto& pr : pending_reads_) {
      size_t count = 1;
      for (const auto& [peer, seq] : hb_acked_) {
        if (peer != config_.id && seq >= pr.hb_seq) count++;
      }
      if (count >= quorum) {
        ready_.confirmed_reads.emplace_back(pr.ctx, pr.read_index);
      } else {
        keep.push_back(pr);
      }
    }
    pending_reads_.swap(keep);
  }
}

void Raft::HandleInstallSnapshot(const Message& m) {
  if (role_ == Role::kCandidate) BecomeFollower(hard_.term, m.from);
  leader_ = m.from;
  ResetElectionTimer();

  Message resp;
  resp.kind = MsgKind::kInstallSnapshotResp;
  resp.to = m.from;
  resp.success = true;

  if (m.snap_index <= commit_) {
    // Stale snapshot; we already have everything it covers.
    resp.match_index = commit_;
    Send(std::move(resp));
    return;
  }

  base_.last_index = m.snap_index;
  base_.last_term = m.snap_term;
  base_.data = m.snap_data;
  log_.clear();
  commit_ = m.snap_index;
  last_emitted_ = m.snap_index;
  ready_.entries_to_append.clear();
  ready_.committed.clear();
  ready_.truncate_from = 0;
  ready_.snapshot_to_apply = base_;
  resp.match_index = m.snap_index;
  Send(std::move(resp));
}

void Raft::HandleInstallSnapshotResp(const Message& m) {
  if (role_ != Role::kLeader) return;
  if (m.success) {
    match_[m.from] = std::max(match_[m.from], m.match_index);
    next_[m.from] = std::max(next_[m.from], m.match_index + 1);
    MaybeAdvanceCommit();
  }
}

void Raft::CompactTo(uint64_t applied_index, std::string snapshot_data) {
  if (applied_index <= base_.last_index) return;
  assert(applied_index <= commit_);
  uint64_t term = TermAt(applied_index);
  uint64_t drop = applied_index - base_.last_index;
  log_.erase(log_.begin(), log_.begin() + static_cast<long>(drop));
  base_.last_index = applied_index;
  base_.last_term = term;
  base_.data = std::move(snapshot_data);
}

Ready Raft::TakeReady() {
  Ready r = std::move(ready_);
  ready_ = Ready{};
  return r;
}

}  // namespace flotilla::raft
