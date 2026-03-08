# Chaos harness

Python 3 (stdlib only) orchestrator that runs a real FlotillaDB cluster as OS
processes, injects faults, records every client operation, and checks the
resulting history for single-key linearizability. Any failure reproduces
exactly from its seed.

## Topology

Node processes are real `flotilladb` binaries. Raft traffic is routed through
in-harness TCP proxies so links can be cut and delayed per direction: each
node gets its own generated cluster config in which every peer's raft address
points at a dedicated proxy (n*(n-1) proxies for n nodes), and each proxy
forwards to the peer's real raft port. Client traffic connects directly.

    node1 --(proxy 1->2)--> node2      proxy state: pass | drop, delay_ms
    node1 --(proxy 1->3)--> node3      partitions = drop on both directions

## Components

- `chaos/client.py` — wire-protocol client (framing + request/response codec
  mirroring `src/net/messages.h`), with timeouts; used by workloads.
- `chaos/proxy.py` — threaded TCP proxy; per-link drop and delay controls.
  Dropping kills existing connections and refuses new bytes, which models a
  partition at the TCP level (the raft transport reconnects and retries).
- `chaos/history.py` — history event log (JSONL) and its record types.
- `chaos/checker.py` — single-key linearizability checker.
- `chaos/run.py` — the orchestrator:
  `python3 chaos/run.py --seed N --nodes 3 --duration 60`.

## Workload and history

Concurrent workload threads issue PUT/GET/DELETE on a small key space
(default 8 keys) so per-key histories interleave heavily. Every PUT value is
unique (`w<worker>-<seq>`), which lets the checker treat each write as
distinguishable. Each operation logs an invocation record before the call and
a completion record after, with monotonic timestamps:

    {"id": 17, "worker": 2, "op": "put", "key": "k3", "value": "w2-9",
     "invoked": 3.417, "returned": 3.522, "outcome": "ok" | "notfound" | "timeout" | "error"}

Timeouts and connection errors are indeterminate: the operation may or may not
have taken effect, now or later. The checker models this by giving such
operations an unbounded interval and permitting them to be dropped entirely.

## Fault schedule

Derived deterministically from the seed: every 2-4 (seeded) seconds pick one:

- kill a random node with SIGKILL (at most a minority down at once),
- restart a downed node,
- partition the cluster into two seeded halves (drop both directions),
- heal all partitions,
- inject 50-300 ms latency on a seeded subset of links, or clear it.

The schedule, workload choices, and timings all come from one seeded RNG, so a
failing run replays with `--seed`. The run ends with heal + restart + drain,
then a final full verification pass.

## Linearizability checker

Single-key register model: PUT(v) sets the value, DELETE clears it, GET
returns the current value or not-found. Per key, a Wing & Gong style DFS with
memoization searches for a linearization:

- An operation may be linearized next only if it was invoked before every
  other pending operation's earliest completion.
- GETs must observe exactly the current register value.
- Operations without a response may be linearized at any later point or
  discarded (never took effect).
- Memoization on (set of linearized ops, register value) keeps the search
  tractable; real histories have low per-key concurrency.

The checker is validated by its own unit tests with hand-built good and bad
histories (stale read, lost acknowledged write, phantom value) before being
trusted against the database.

## Verdict

`run.py` prints per-key results and exits nonzero if any key's history is
non-linearizable, if any acknowledged write is missing at final verification,
or if the cluster fails to make progress. The history file is kept for
post-mortem (`chaos/check.py history.jsonl` re-checks offline).
