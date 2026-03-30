# MVCC transactions

Percolator-style snapshot-isolation transactions over the sharded KV, with
client-coordinated two-phase commit across ranges. The SQL layer (milestone 8)
runs entirely on this API.

## Keyspaces

The raw KV API (GET/PUT/DELETE/SCAN) and the transactional API use disjoint
keyspaces and must not be mixed on the same data (the TiKV rawkv/txnkv rule).
Transactional records live under prefixed, memcomparable-encoded keys in the
same storage engines:

    !l <ekey>            lock record        (one per key, present while a txn
                                            has the key prewritten)
    !w <ekey> <~cts>     write record       (the commit history of a key)
    !d <ekey> <~sts>     data record        (value written at prewrite time)

`<ekey>` is the user key escape-encoded (0x00 -> 0x00 0xFF, terminated by
0x00 0x01) so that concatenating the timestamp suffix preserves user-key
order. `<~ts>` is the bitwise NOT of the big-endian 64-bit timestamp, so newer
versions sort first within a key.

Record payloads:

    lock:  [u64 start_ts][u8 op][u64 wall_ms][lp primary_key]
    write: [u8 kind: 1 put | 2 delete | 3 rollback][u64 start_ts]
    data:  raw value bytes

`wall_ms` is stamped by the proposing leader (commands must apply
deterministically, so wall time never originates inside apply); it exists only
for lock-TTL resolution.

## Timestamps

Timestamps are Raft log indexes of the first range's group (the group whose
range starts at the empty key — it exists on every cluster from bootstrap).
`TXN_TS` proposes a tick command there and returns the committed entry's
index: globally unique, strictly monotonic, and totally ordered with every
other timestamp — one consensus round per timestamp, which favors correctness
and simplicity over TSO batching (a known production optimization).

## Transaction protocol (client-coordinated)

Begin: fetch start_ts. Reads go through `TXN_GET`/`TXN_SCAN` at start_ts;
writes buffer client-side.

Commit (2PC, primary = first written key):
1. `TXN_PREWRITE` every buffered write (start_ts, primary). Per key, applied
   in the owning range's group: fails with Conflict if a newer commit exists
   (write-write conflict), a rollback marker exists at start_ts, or another
   txn holds the lock. Success writes lock + data records.
2. Fetch commit_ts.
3. `TXN_COMMIT` the primary: atomically (one log entry) replaces its lock
   with a write record at commit_ts. **This is the commit point.**
4. `TXN_COMMIT` the secondaries (roll-forward). Failures here are benign:
   any future reader can finish the job via the primary (below).

Rollback: `TXN_ROLLBACK` each prewritten key — removes lock + data and leaves
a rollback marker at start_ts so a delayed prewrite of the same txn can never
sneak in later.

## Reads

`TXN_GET(key, ts)` after the owning group's ReadIndex barrier:
1. If a lock exists with lock.start_ts <= ts: the read cannot proceed
   (the locking txn may commit below ts). Return Conflict carrying the lock's
   primary and start_ts.
2. Otherwise scan write records newest-first from ts: skip rollbacks; a put
   points at the data record (read at its start_ts); a delete is not-found.

`TXN_SCAN(start, end, ts)` iterates write records across ranges the same way
(sub-scans forwarded to range leaders like raw scans), reporting the first
blocking lock as a Conflict.

## Lock resolution (crashed coordinators)

A reader hitting a lock retries briefly, then asks the primary's range leader
to `TXN_RESOLVE(primary, start_ts)`:
- primary lock still present and younger than the TTL: txn is live; keep
  waiting.
- primary lock present but older than the TTL (leader wall-clock decision,
  made at propose time, not apply time): propose its rollback.
- primary lock absent: consult the primary's write records — a commit record
  at start_ts means the txn committed (reader rolls the secondary forward by
  issuing `TXN_COMMIT` with that commit_ts); a rollback marker means it
  aborted (reader issues `TXN_ROLLBACK` on the secondary).

This gives the standard Percolator guarantee: the primary key's state is the
single source of truth for the transaction's outcome.

## Server integration

Prewrite/commit/rollback are state-machine commands (ops 4-6) in the owning
range's group; their logical outcome (ok/conflict/aborted) is computed
deterministically at apply on every replica and returned to the waiting
proposer on the leader (infrastructure failures still abort the node; logical
failures are results, and the log entry is a no-op on every replica that
computes the same failure). The ts tick is op 7. Reads reuse the ReadIndex
path. Snapshot isolation follows: all reads see the state as of start_ts, and
write-write conflicts abort one of the two transactions.

## Invariants

- A key's write records carry strictly decreasing commit_ts from newest to
  oldest; commit_ts > start_ts for every record.
- A lock and a write record for the same (key, start_ts) never coexist.
- After a rollback marker at (key, start_ts), no prewrite or commit for that
  (key, start_ts) can succeed.
- Cross-range atomicity: secondaries are readable exactly when the primary's
  commit record exists (readers roll forward, never observe half a txn).

## Testing

- Codec unit tests: escape-encoding order preservation, record roundtrips.
- Single-range: commit visibility at ts, write-write conflict aborts, reads
  at old ts see old snapshot, rollback markers block late prewrites.
- Cross-range: split, then transactions spanning both ranges commit
  atomically; a txn abandoned between prewrite and commit is rolled forward /
  back correctly by the next reader via the primary.
- CLI: interactive txn mode exercised in the end-to-end demo.
