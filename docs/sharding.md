# Multi-Raft sharding

The keyspace is split into ranges; each range is its own Raft group running the
same consensus code, all hosted on every node (static membership: every group's
replicas are the full node set, so sharding distributes leadership and log/
snapshot load, not data placement — the first step of TiKV/Cockroach-style
multi-Raft before rebalancing exists).

*Design revision (during milestone 6):* the original sketch routed splits
through a dedicated meta group. That makes the copy/freeze protocol between
the meta log and the data group's log racy to get deterministic, so the built
design follows TiKV instead: **a split is a command in the parent range's own
log**, which pins it to a definite position in the parent's history, and **all
groups on a node share one storage engine**, so a split moves no data at all.

## Node architecture

`ShardedNode` hosts one `RaftGroup` per range. Each group has its own raft
log/state/snapshot directory (`data-dir/raft-g<id>/`); all groups share the
node's one LSM storage engine (`data-dir/kv/`). Raft frames carry a group id
on the shared node-to-node connections and are demultiplexed on receipt;
frames for a group this replica has not created yet (its parent's split is
still ahead of the replica) are dropped and raft retransmits. One tick thread
drives all groups; each group has its own apply thread.

Per-group system keys in the shared engine (byte 0x00 prefix, hidden from
clients): `applied/<gid>` (apply progress), `range/<rid>` (durable
descriptor), `spawns/<gid>` (children this group has split off).

## Routing

Each node derives its routing table (range start -> group) from the durable
descriptors; it is updated when a split applies. Clients need no shard
awareness: any node routes a request by key to the right local group and
answers NOT_LEADER with *that group's* leader as the redirect hint, so the
existing client redirect loop works per-range. `RANGES` exposes the table for
inspection.

SCAN fans out across the ranges intersecting the requested interval, in key
order: locally-led ranges are served after that group's ReadIndex barrier;
ranges led elsewhere are forwarded as single-range sub-scans (marked
no-forward so leadership churn surfaces as a retryable error instead of a
forwarding loop).

## Manual split

`split <key>` in the CLI:

1. The receiving node routes to the parent group and proposes
   `split(child_id, key)` in the parent's own log (child id = max known range
   id + 1; a stale allocation is rejected at apply and the operator retries).
2. On apply — at the same log position on every replica — the parent's range
   [a, c) shrinks to [a, key), descriptors and the spawn list are written to
   the shared engine, and the child group [key, c) is created locally, with
   an empty log.
3. No data moves: the shared engine already holds the child's keys; the child
   group simply starts owning writes/reads for them.

Split safety follows from the log position: every replica shrinks the parent
and creates the child at the same point in the parent's history.

## Snapshots with shared storage

A group's snapshot serializes only its own range (plus its range bounds and
spawn list). Restoring wipes exactly the snapshot's range in the shared
engine, reinserts the snapshot contents, and re-creates any spawned child
groups the lagging replica has not seen yet (they then catch up through their
own leaders). A replica that was down across a split therefore converges:
either it replays the split from the parent's log, or the parent's post-split
snapshot tells it the child exists.

## Invariants

- Ranges in every node's routing table are disjoint and cover the keyspace.
- A split changes ownership at one definite parent-log position; range bounds
  only ever shrink at the start-side group, so a group's snapshot range is a
  deterministic function of its applied log.
- The child group only exists on a node after that node applied the parent's
  split (or a post-split parent snapshot); raft traffic for unknown groups is
  dropped, never buffered.
- Cross-group atomicity does not exist at this milestone (that is milestone
  7's 2PC).

## Testing

- Cluster tests: split a live range and verify every key on both sides
  (reads, writes, ordered cross-range scans); repeated and nested splits;
  node kill + restart rebuilding both groups from disk; per-group leadership
  divergence with server-side redirects.
- Chaos: the standard workload with splits issued at startup, so the checker
  validates linearizability across group boundaries.
