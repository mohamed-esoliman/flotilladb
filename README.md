# FlotillaDB

A distributed SQL database built from scratch in C++20 - Raft consensus, a
hand-written LSM-tree storage engine, a custom binary wire protocol,
multi-Raft sharding, MVCC transactions with two-phase commit, and a SQL
layer, with a chaos test harness that actively tries to break the consistency
claims and a linearizability checker that proves it failed. No RocksDB, no
gRPC, no Raft library: every layer is hand-built on the C++ standard library
and POSIX sockets, mirroring the architecture of real systems (etcd, TiKV,
CockroachDB) so every design decision maps onto a known production trade-off.

## Sixty-second tour

Start a 3-node cluster and talk SQL to it:

```
$ ./build/src/server/flotilladb --config cluster.conf --node-id 1 --data-dir d1 &
$ ./build/src/server/flotilladb --config cluster.conf --node-id 2 --data-dir d2 &
$ ./build/src/server/flotilladb --config cluster.conf --node-id 3 --data-dir d3 &

$ ./build/src/client/flotilla-cli 127.0.0.1:4001 4002 4003
flotilla> sql CREATE TABLE fleet (id INT PRIMARY KEY, ship TEXT, crew INT)
table fleet created
flotilla> sql INSERT INTO fleet VALUES (1, 'endeavour', 85), (2, 'resolution', 110)
OK (2 rows affected)
flotilla> sql SELECT ship FROM fleet WHERE crew >= 100
ship
----------
resolution
(1 rows)
```

Kill the leader mid-workload with `kill -9`; the cluster elects a new leader
in a few hundred milliseconds, the client redirects transparently, and no
acknowledged write is ever lost - the chaos harness's linearizability checker
verifies exactly that on every run:

```
$ python3 chaos/run.py --seed 42 --nodes 3 --duration 60 --splits k3,k6
[chaos 17:47:14] seed=42 nodes=3 duration=60.0s
[chaos 17:47:16] split at k3
[chaos 17:47:17] split at k6
[chaos 17:47:21] node 3 killed
[chaos 17:47:24] delaying 3 links by 242ms
[chaos 17:47:27] partitioned [2] | [1, 3]
[chaos 17:47:30] node 3 started pid 33648
[chaos 17:47:33] healed all partitions
[chaos 17:47:36] partitioned [3] | [1, 2]
[chaos 17:47:40] node 1 killed
[chaos 17:47:43] healed all partitions
...
[chaos 17:48:20] workloads finished: 3717 successful operations
[chaos 17:48:20]   OK  k0: linearizable (356 states)
[chaos 17:48:20]   OK  k1: linearizable (320 states)
[chaos 17:48:20]   OK  k2: linearizable (345 states)
[chaos 17:48:20]   OK  k3: linearizable (382 states)
[chaos 17:48:20]   OK  k4: linearizable (354 states)
[chaos 17:48:20]   OK  k5: linearizable (313 states)
[chaos 17:48:20]   OK  k6: linearizable (312 states)
[chaos 17:48:20]   OK  k7: linearizable (290 states)
[chaos 17:48:20] VERDICT: PASS (4031 ops linearizable)
```

That transcript is a real recorded run: two live range splits, three leader
kills, four network partitions, and injected latency over sixty seconds, with
every one of the 4031 acknowledged operations proven linearizable afterwards.
```

## Features

- **LSM-tree storage engine** (`src/storage/`): write-ahead log with CRC
  records and torn-tail recovery, lock-free-reader skiplist memtable, SSTables
  with sparse indexes and bloom filters, leveled background compaction,
  crash-safe manifest, checkpoints. Usable standalone as an embedded ordered
  KV library.
- **Raft consensus** (`src/raft/`): leader election, log replication,
  persistent term/vote/log state, conflict-hint log repair, snapshots with
  log truncation and InstallSnapshot for lagging followers, linearizable
  reads via ReadIndex. The core is a deterministic, IO-free state machine
  (tick/step/Ready, etcd-style) validated by a seeded in-process simulation
  that checks election safety and state-machine safety on every tick.
- **Custom wire protocol** (`src/net/`): length-prefixed binary framing over
  TCP, hand-rolled encoding, thread-per-connection server, one-way raft
  frames multiplexed per group.
- **Multi-Raft sharding** (`src/server/`): the keyspace splits into ranges,
  each its own Raft group over one shared storage engine, so a split moves no
  data. Splits are commands in the parent range's own log (TiKV-style),
  making them deterministic at a log position. Per-range leaders, server-side
  routing, cross-range scans.
- **MVCC transactions** (`src/txn/`, `src/client/txn_client.*`):
  Percolator-style snapshot isolation with client-coordinated two-phase
  commit across shards, timestamp allocation through Raft, and lock
  resolution that rolls abandoned transactions forward or back via their
  primary key.
- **SQL subset** (`src/sql/`): hand-written lexer, recursive-descent parser,
  and planner/executor (point reads on primary key, filtered scans) over the
  transactional API. CREATE/DROP TABLE, INSERT, SELECT ... WHERE, UPDATE,
  DELETE; INT and TEXT; TiDB-style row-to-KV mapping.
- **Chaos harness** (`chaos/`): Python orchestrator that runs the real
  binaries behind per-link TCP proxies, then kills nodes, partitions the
  network, and injects latency mid-workload from a seeded fault schedule.
  Every operation is recorded and checked by a Wing & Gong style single-key
  linearizability checker; failures reproduce exactly from the seed.

## Architecture

```
                       flotilla-cli / client library
                    sql | txn (2PC coordinator) | raw kv
                                  |
                     length-prefixed binary protocol (TCP)
                                  |
   +------------------------- node (x3) ---------------------------+
   |                                                               |
   |   client server --- routing table --- RANGE SPLIT admin       |
   |        |                                                      |
   |   +----------------+  +----------------+       raft port      |
   |   | raft group 1   |  | raft group N   | <--- (one-way raft   |
   |   | range [a, m)   |  | range [m, inf) |       frames, tagged |
   |   |  raft core     |  |  raft core     |       by group)      |
   |   |  raft log/snap |  |  raft log/snap |                      |
   |   |  apply thread  |  |  apply thread  |                      |
   |   +-------+--------+  +-------+--------+                      |
   |           |                   |                               |
   |           +---------+---------+                               |
   |                     |                                         |
   |          shared LSM storage engine                            |
   |     (WAL -> memtable -> SSTables, leveled compaction)         |
   |      raw keys | txn locks | txn writes | txn data             |
   +---------------------------------------------------------------+
```

A write goes: client -> range leader -> Raft log (fsync, replicate, majority
commit) -> apply thread -> LSM engine (WAL -> memtable -> SSTable). A
linearizable read goes: leader records its commit index, confirms leadership
with a heartbeat round (ReadIndex), waits for apply to catch up, then reads
from the engine. Transactions layer Percolator lock/write/data records over
the same keyspace; SQL rows encode onto transactional keys.

Each subsystem has a design doc written before its implementation and
reconciled after: see [docs/](docs/README.md) for the storage engine, wire
protocol, raft, chaos harness, sharding, MVCC, and SQL designs, including
the on-disk formats and the invariants each layer maintains.

## Building

Prerequisites: a C++20 compiler (clang or gcc), CMake 3.20+, Python 3.8+
(chaos harness only, stdlib only). GoogleTest is fetched by CMake and is
test-only; the database itself has zero third-party dependencies.

```
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Running a cluster

Write a config listing every node's client and raft address:

```
node 1 127.0.0.1:4001 127.0.0.1:4101
node 2 127.0.0.1:4002 127.0.0.1:4102
node 3 127.0.0.1:4003 127.0.0.1:4103
```

Start each node, then connect the CLI to any subset of them:

```
./build/src/server/flotilladb --config cluster.conf --node-id 1 --data-dir data1
./build/src/client/flotilla-cli 127.0.0.1:4001 127.0.0.1:4002 127.0.0.1:4003
```

CLI highlights (`help` shows everything):

```
put k v / get k / del k / scan [start] [end]     raw KV, linearizable
status / ranges                                  cluster and range state
split m                                          split the range containing "m"
txn set a 1 ; set z 2                            atomic cross-shard transaction
sql SELECT * FROM t WHERE ...                    SQL, one txn per statement
```

A single-node standalone mode (`--data-dir d --listen host:port`, no
consensus) exercises the storage engine directly.

## Chaos testing

```
python3 chaos/run.py --seed N --nodes 3 --duration 60 --splits k3,k6
```

runs concurrent workloads against a live cluster while a seeded fault
schedule kills nodes (SIGKILL), partitions the network at the TCP level,
and injects link latency; snapshots and range splits are active throughout.
Afterwards the checker validates every key's history for linearizability -
timed-out operations are correctly treated as maybe-applied. Any failure
reproduces from its seed, and `chaos/check.py history.jsonl` re-checks a
recorded run offline. The checker itself is unit-tested against known-bad
histories (stale reads, lost acknowledged writes, phantom values) before
being trusted about the database.

## Test suite

About 100 tests across every layer: storage engine crash tests (WAL torn at
every byte offset, recovery mid-compaction, checkpoint isolation), the
deterministic Raft simulation (elections, failover, partitions, message loss,
restarts, snapshot catch-up, and a randomized multi-seed soak with always-on
safety invariants), live 3-node cluster tests over real TCP (leader kill,
disk recovery, InstallSnapshot, splits, cross-shard transactions, SQL CRUD),
and the chaos checker's own unit tests.

## Deliberate limitations (v1)

- Static cluster membership (no joint consensus / reconfiguration).
- No authentication or TLS; not hardened for untrusted networks.
- SQL is a subset by design: no joins, aggregates, NULLs, or secondary
  indexes.
- Raw KV and transactional keyspaces are disjoint; do not mix them on the
  same data.
- Correctness first, throughput second: fsync on every write-path hop,
  thread-per-connection networking, one consensus round per timestamp.

## Layout

```
src/common/    status, coding, crc32, fs helpers
src/storage/   LSM engine: wal, memtable, sstable, compaction, db
src/raft/      raft core, wire codec, persistent state
src/net/       framing, sockets, tcp server, message codecs
src/server/    raft groups, sharded node, cluster config, main
src/txn/       percolator state machine and txn key codecs
src/sql/       lexer, parser, catalog, executor
src/client/    client library, 2PC coordinator, flotilla-cli
tests/         GoogleTest suites incl. the raft simulator
chaos/         orchestrator, proxies, linearizability checker
docs/          per-subsystem design docs
```
