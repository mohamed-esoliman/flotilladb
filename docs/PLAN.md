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

- [ ] docs/wire-protocol.md design doc
- [ ] frame codec: length-prefixed binary messages, hand-rolled encode/decode
- [ ] message types: GET/PUT/DELETE/SCAN/STATUS + error responses with leader hint
- [ ] TCP server, thread-per-connection; clean shutdown
- [ ] single-node service applying ops directly to the storage engine
- [ ] flotilla-cli: REPL, pretty output, GET/PUT/DELETE/SCAN/STATUS
- [ ] tests: codec roundtrip, server end-to-end over loopback

## Milestone 3 — Raft

Deterministic core state machine (tick/step/Ready pattern), simulated transport
first, then real TCP.

- [ ] docs/raft.md design doc
- [ ] raft core: elections, log replication, commit rules; no threads, no IO
- [ ] persistent state: term/vote file (atomic rename), log file with CRC records
- [ ] deterministic in-process simulation: seeded drops/reorders/delays/partitions
- [ ] sim tests: election, replication, leader failover, log convergence, restarts
- [ ] TCP raft transport reusing the frame codec
- [ ] server integration: propose on leader, apply loop, NOT_LEADER redirects
- [ ] ReadIndex linearizable reads
- [ ] tests: 3-node cluster over loopback, kill/restart leader

## Milestone 4 — Chaos harness

- [ ] docs/chaos.md design doc
- [ ] chaos/proxy.py: per-link TCP proxies with drop/delay control
- [ ] chaos/client.py: Python wire-protocol client
- [ ] chaos/run.py: seeded fault schedule (kill/restart/partition/heal/delay),
      concurrent workloads, full invocation/response history log
- [ ] chaos/checker.py: single-key linearizability checker (Wing-Gong style)
- [ ] checker unit tests with known-good and known-bad histories
- [ ] run repeatedly, fix every bug it finds

## Milestone 5 — Snapshots + compaction hardening (resume-ready)

- [ ] snapshot: storage checkpoint + last included index/term, log truncation
- [ ] InstallSnapshot RPC for lagging followers
- [ ] compaction under load hardening, backpressure
- [ ] chaos runs with snapshots enabled
- [ ] reconcile docs

## Milestone 6 — Multi-Raft sharding

- [ ] docs/sharding.md design doc
- [ ] range descriptors in a meta group; multiple raft groups per node
- [ ] routing: client fetches/caches range table, WRONG_RANGE retry
- [ ] manual shard split command
- [ ] tests + chaos across shards

## Milestone 7 — MVCC transactions

- [ ] docs/mvcc.md design doc
- [ ] versioned keys (user_key + inverted commit_ts), reads at snapshot ts
- [ ] timestamp allocation via meta group leader
- [ ] percolator-style 2PC: prewrite locks, primary commit, snapshot isolation
- [ ] tests: conflict aborts, snapshot reads, cross-shard commit atomicity

## Milestone 8 — SQL subset

- [ ] docs/sql.md design doc
- [ ] lexer + recursive-descent parser: CREATE TABLE, INSERT, SELECT ... WHERE,
      UPDATE, DELETE
- [ ] catalog in system keys; row/index encoding onto KV
- [ ] planner/executor: PK point lookup or filtered scan, via MVCC transactions
- [ ] flotilla-cli SQL mode
- [ ] tests: parser, end-to-end SQL over a live cluster

## Final phases

- [ ] end-to-end verification: full suite, live 3-node demo, chaos run
- [ ] README: architecture diagrams, recorded chaos run, verified setup steps
- [ ] CI workflow (clang/macOS + gcc/Linux)
