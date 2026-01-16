# Design docs

Each subsystem gets a short design doc before implementation: scope and
constraints, on-disk/wire formats, invariants the code must maintain, and
failure-handling control flow. Docs are updated when the design changes.

- [PLAN.md](PLAN.md) — milestone build plan and progress
- storage.md — LSM storage engine (milestone 1)
- wire-protocol.md — framing, message types, server model (milestone 2)
- raft.md — consensus core, persistence, transports (milestone 3)
- chaos.md — fault injection harness and linearizability checker (milestone 4)
- sharding.md — multi-Raft ranges, routing, split (milestone 6)
- mvcc.md — versioned storage, snapshot isolation, 2PC (milestone 7)
- sql.md — SQL subset, row-to-KV encoding (milestone 8)
