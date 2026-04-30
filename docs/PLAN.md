# Build plan

Working plan for building FlotillaDB per `flotilladb-spec.md`. Each milestone starts
with its design doc in `docs/` and ends with the doc reconciled against what was built.
Checkboxes track progress; commits are finer-grained than milestones.

## Milestone 1 — Storage engine

Standalone embedded ordered KV library: WAL, skiplist memtable, SSTables with sparse
index and bloom filters, manifest, leveled compaction, crash recovery.

- [x] docs/storage.md design doc
- [x] common utilities: Status, coding (fixed-width LE), CRC32, logging
- [x] WAL: append records with CRC, fsync, tail-truncating recovery scan
- [x] skiplist memtable keyed by (key, seqno desc) with tombstones
- [x] SSTable writer: data blocks, sparse index, bloom filter, footer
- [x] SSTable reader: footer/index/bloom load, point get, iterator
- [x] manifest: atomic rewrite tracking live SSTables per level
- [x] DB open/recovery: replay WAL into memtable, load manifest
- [x] read path: merging iterator memtable -> immutable -> L0 -> Ln
- [x] flush: memtable -> L0 SSTable, WAL rotation
- [x] leveled compaction on background thread
- [x] checkpoint (hard-link/copy live files) for later Raft snapshots
- [x] tests: WAL crash points, SSTable roundtrip, bloom stats, compaction, recovery

## Milestone 2 — Single-node server

- [x] docs/wire-protocol.md design doc
- [x] frame codec: length-prefixed binary messages, hand-rolled encode/decode
- [x] message types: GET/PUT/DELETE/SCAN/STATUS + error responses with leader hint
- [x] TCP server, thread-per-connection; clean shutdown
- [x] single-node service applying ops directly to the storage engine
- [x] flotilla-cli: REPL, pretty output, GET/PUT/DELETE/SCAN/STATUS
- [x] tests: codec roundtrip, server end-to-end over loopback

## Milestone 3 — Raft

Deterministic core state machine (tick/step/Ready pattern), simulated transport
first, then real TCP.

- [x] docs/raft.md design doc
- [x] raft core: elections, log replication, commit rules; no threads, no IO
- [x] persistent state: term/vote file (atomic rename), log file with CRC records
- [x] deterministic in-process simulation: seeded drops/reorders/delays/partitions
- [x] sim tests: election, replication, leader failover, log convergence, restarts
- [x] TCP raft transport reusing the frame codec
- [x] server integration: propose on leader, apply loop, NOT_LEADER redirects
- [x] ReadIndex linearizable reads
- [x] tests: 3-node cluster over loopback, kill/restart leader

## Milestone 4 — Chaos harness

- [x] docs/chaos.md design doc
- [x] chaos/proxy.py: per-link TCP proxies with drop/delay control
- [x] chaos/client.py: Python wire-protocol client
- [x] chaos/run.py: seeded fault schedule (kill/restart/partition/heal/delay),
      concurrent workloads, full invocation/response history log
- [x] chaos/checker.py: single-key linearizability checker (Wing-Gong style)
- [x] checker unit tests with known-good and known-bad histories
- [x] run repeatedly, fix every bug it finds

## Milestone 5 — Snapshots + compaction hardening (resume-ready)

- [x] snapshot: storage checkpoint + last included index/term, log truncation
- [x] InstallSnapshot RPC for lagging followers
- [x] compaction under load hardening, backpressure
- [x] chaos runs with snapshots enabled
- [x] reconcile docs

## Milestone 6 — Multi-Raft sharding

- [x] docs/sharding.md design doc
- [x] range descriptors in a meta group; multiple raft groups per node
- [x] routing: client fetches/caches range table, WRONG_RANGE retry
- [x] manual shard split command
- [x] tests + chaos across shards

## Milestone 7 — MVCC transactions

- [x] docs/mvcc.md design doc
- [x] versioned keys (user_key + inverted commit_ts), reads at snapshot ts
- [x] timestamp allocation via meta group leader
- [x] percolator-style 2PC: prewrite locks, primary commit, snapshot isolation
- [x] tests: conflict aborts, snapshot reads, cross-shard commit atomicity

## Milestone 8 — SQL subset

- [x] docs/sql.md design doc
- [x] lexer + recursive-descent parser: CREATE TABLE, INSERT, SELECT ... WHERE,
      UPDATE, DELETE
- [x] catalog in system keys; row/index encoding onto KV
- [x] planner/executor: PK point lookup or filtered scan, via MVCC transactions
- [x] flotilla-cli SQL mode
- [x] tests: parser, end-to-end SQL over a live cluster

## Final phases

- [ ] end-to-end verification: full suite, live 3-node demo, chaos run
- [ ] README: architecture diagrams, recorded chaos run, verified setup steps
- [ ] CI workflow (clang/macOS + gcc/Linux)
