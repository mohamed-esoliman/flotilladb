# Raft consensus

Hand-built Raft (leader election, log replication, persistence; snapshots in
milestone 5). The core is a deterministic, single-threaded, IO-free state
machine driven by `Tick()` and `Step(message)` — the etcd-style Ready pattern —
so the same code runs under the deterministic in-process simulation and the
real TCP server.

## Core interface

    Raft(config, restored_hard_state, restored_log, base_snapshot)
    Tick()                      logical clock; election/heartbeat timers count ticks
    Step(Message)               feed one incoming message
    Propose(cmd, *index)        leader appends a command; false if not leader
    StartReadIndex(ctx)         begin a linearizable read (leader only)
    TakeReady()                 drain side effects for the host to execute

`Ready` carries: hard-state change (term/vote), a log truncation point,
entries to append, outbound messages, newly committed entries, and confirmed
read contexts. The host contract, in order:

1. Truncate the persistent log if `truncate_from != 0`.
2. Append and fsync `entries_to_append`.
3. Persist and fsync hard state if changed.
4. Only then send `messages`.
5. Apply `committed` to the state machine (order matters, exactly once).

Persist-before-send is what makes vote grants and AppendEntries acks safe to
restart through.

## State and rules (standard Raft, RAFT paper section 5)

- Roles follower/candidate/leader; randomized election timeout in ticks
  (seeded RNG, so the simulation is fully deterministic).
- Any message with a higher term converts the node to follower and adopts the
  term (vote cleared, persisted).
- Vote granted iff not yet voted this term (or same candidate) and the
  candidate's log is at least as up-to-date (last term, then last index).
- On winning, a leader appends an empty no-op entry for its new term: this
  lets it commit (and thus serve ReadIndex) without waiting for client
  traffic, and is skipped by the state machine.
- AppendEntries consistency check on (prev_index, prev_term); on mismatch the
  follower replies with a conflict hint (first index of the conflicting term,
  or last index + 1) so the leader backs off in one round instead of one
  index at a time.
- Leader advances commit only for entries of its own term with a majority
  match (figure 8 rule); follower commit = min(leader_commit, last index).

## Log entries and messages

    LogEntry { term u64, index u64, command bytes }

Command bytes are opaque to Raft; the KV layer encodes `[op u8][key][value]`.
An empty command is the leader no-op.

Messages (one struct, kind-tagged): RequestVote(+Resp),
AppendEntries(+Resp), later InstallSnapshot(+Resp). AppendEntries carries a
heartbeat sequence number echoed in responses; ReadIndex counts echoes.
Encoding is the shared fixed-width coding, exchanged as one-way `RAFT` frames
over the node-to-node TCP connections (each direction is its own connection;
responses are independent messages, not connection replies).

## ReadIndex (linearizable reads)

1. Leader records read_index = commit index, tags the context with the current
   heartbeat sequence.
2. Next heartbeat broadcast; when a quorum (counting self) has echoed a
   sequence >= the tagged one, the read context is confirmed via Ready.
3. The host waits until applied index >= read_index, then serves the read from
   the storage engine.

A leader that cannot confirm (lost quorum) simply never confirms; the host
times the request out and the client retries elsewhere.

## Persistence

- `raft_state`: term + voted_for, written via atomic rename + fsync on change.
- `raft_log`: append-only records `[len][crc][term, index, command]`, fsynced
  per Ready batch; tail-truncated on CRC mismatch at recovery like the storage
  WAL. Suffix truncation (conflict) and prefix truncation (after snapshot,
  milestone 5) rewrite the file via tmp + rename — logs stay small because
  snapshots bound them.

## Invariants

- current_term never decreases; at most one vote per term survives restarts.
- commit index never decreases; applied index never exceeds commit index.
- Committed entries are never truncated (truncation points are always above
  the commit index — conflicts can only exist in unreplicated suffixes).
- Election safety: at most one leader per term (checked every tick in the
  simulation).
- Log matching: two logs agreeing on (index, term) agree on the whole prefix
  (checked in the simulation against all committed prefixes).

## Deterministic simulation

`tests/raft_sim.h` runs N cores against in-memory storage and a simulated
network: per-message seeded delivery delay, drop probability, partitions as a
blocked-pair set, node kill/restart (rebuilds the core from the in-memory
"disk"). Every tick asserts election safety and applied-prefix consistency.
Failures reproduce exactly from the seed. TCP integration tests then cover the
same scenarios coarsely against real processes' behavior (kill/restart the
leader over loopback).

## Server integration

`RaftNode` (in `src/server/`) owns the core behind a mutex plus: a tick thread
(10 ms per tick), the raft-port TCP server feeding `Step`, per-peer sender
threads with reconnecting one-way connections, and an apply thread executing
committed commands against the storage engine. Client writes register a
pending proposal (index, term) fulfilled at apply time — an applied entry with
a different term fails the proposal with NOT_LEADER (superseded). Reads run
the ReadIndex flow. Non-leaders answer client ops with NOT_LEADER plus the
last known leader's client address as the redirect hint.
