# Design docs

Each subsystem gets a short design doc before implementation: scope and
constraints, on-disk/wire formats, invariants the code must maintain, and
failure-handling control flow. Docs are updated when the design changes.

- [PLAN.md](PLAN.md) — milestone build plan and progress
- [storage.md](storage.md) — LSM storage engine
- [wire-protocol.md](wire-protocol.md) — framing, message types, server model
- [raft.md](raft.md) — consensus core, persistence, transports
- [chaos.md](chaos.md) — fault injection harness and linearizability checker
- [sharding.md](sharding.md) — multi-Raft ranges, routing, split
- [mvcc.md](mvcc.md) — versioned storage, snapshot isolation, 2PC
- [sql.md](sql.md) — SQL subset, row-to-KV encoding
