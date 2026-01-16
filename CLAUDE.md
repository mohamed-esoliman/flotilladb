# FlotillaDB

Distributed SQL database built from scratch in C++20: hand-written LSM storage
engine, Raft consensus, custom binary wire protocol, multi-Raft sharding, MVCC
transactions, and a Python chaos test harness. No third-party runtime
dependencies; GoogleTest (fetched by CMake) is test-only.

## Commands

- Configure: `cmake -B build`
- Build: `cmake --build build -j`
- Test: `ctest --test-dir build --output-on-failure`
- Run a node: `./build/src/server/flotilladb --config <cluster.conf> --node-id <n> --data-dir <dir>`
- CLI: `./build/src/client/flotilla-cli --host 127.0.0.1 --port 4001`
- Chaos run: `python3 chaos/run.py --seed 1 --nodes 3 --duration 30`

## Layout

`src/common` shared utilities, `src/storage` LSM engine, `src/raft` consensus,
`src/server` node binary + wire protocol, `src/client` CLI, `src/sql` SQL layer,
`tests/` GoogleTest suites, `chaos/` Python harness, `docs/` design docs
(see `docs/PLAN.md` for build progress).

## Conventions

- Design doc in `docs/` before each subsystem; keep it reconciled with the code.
- Storage/raft core code is deterministic and thread-free; threads live at the
  edges (server, transports, background compaction).
- Every non-trivial change lands with tests; run the full suite before declaring
  a milestone done.
