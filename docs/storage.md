# Storage engine

Embedded ordered KV store (LSM tree), usable standalone and as the state machine
under Raft. Single-writer, multi-reader. All multi-byte integers little-endian.

## Scope and constraints

- API: `Put(key, value)`, `Delete(key)`, `Get(key)`, `NewIterator()` (ordered
  scan), `Flush()`, `Checkpoint(dir)`, `Open/Close`.
- Keys and values are arbitrary byte strings; keys ordered bytewise.
- Deletes are tombstones; space is reclaimed by compaction.
- Concurrency: one mutex guards mutable state (memtable switch, version list);
  reads take a consistent snapshot of sources under the mutex then read without
  it. One background thread runs flush + compaction.

## On-disk layout

A database directory contains:

- `<n>.wal` — write-ahead log for the active memtable (one per memtable
  generation; older ones exist only until their memtable is flushed).
- `<n>.sst` — immutable sorted tables.
- `MANIFEST` — current version: live SSTables per level, next file number,
  last sequence number. Rewritten atomically (tmp + fsync + rename).

File numbers come from a single counter persisted in the manifest.

### WAL record

    [u32 payload_len][u32 crc32(payload)][payload]
    payload = [u64 seqno][u8 op][u32 klen][key][u32 vlen][value]   op: 1=put 2=del

Recovery scans sequentially and stops at EOF, a short record, or a CRC
mismatch; the file is truncated to the last valid boundary (a torn tail is a
crash artifact, not corruption to propagate).

### SSTable

    [data blocks][index block][bloom block][footer]

- Data block: concatenated entries `[u32 klen][key][u64 seqno][u8 op][u32 vlen][value]`,
  sorted by (key asc, seqno desc); a new block starts after ~4 KB.
- Index block (sparse): per data block `[u32 klen][first_key][u64 off][u32 size]`.
- Bloom block: `[u32 k][u32 nbits][bits]`, ~10 bits/key, double hashing from a
  64-bit FNV-1a pair.
- Footer (fixed 48 bytes): index off/size, bloom off/size, entry count,
  format version, magic `0xF107111ADB` — readers validate both.

Index and bloom are loaded whole into memory per open table (tables are small
at this project's scale; a block cache is a non-goal).

### Manifest

Text lines, rewritten whole on every version change:

    next_file <n>
    last_seq <n>
    file <num> <level> <smallest_hex> <largest_hex> <entries>

## Invariants

- A write is acked only after its WAL record is fsynced (fsync per batch).
- Memtable insert happens after WAL append; recovery replay is idempotent
  because seqnos are stored with entries.
- The manifest is renamed into place only after the tables it references are
  fsynced. On open, `.sst`/`.wal` files not referenced by the manifest or the
  active generation are deleted (crash leftovers).
- L0 tables may overlap (each is one flushed memtable, newest file number =
  newest data); L1+ tables are disjoint within a level.
- Read precedence: memtable, immutable memtables (newest first), L0 by file
  number descending, then L1, L2, ... For equal user keys the higher seqno wins.
- A tombstone is dropped during compaction only when no deeper level can
  contain the key.

## Write path

1. Assign seqno, append WAL record, fsync.
2. Insert into skiplist memtable (sorted by key asc, seqno desc).
3. If memtable exceeds `write_buffer_size`: move it to the immutable list,
   start a fresh memtable + WAL generation, wake the background thread.
4. Background: write immutable memtable to an L0 `.sst`, fsync, commit a new
   manifest, delete the old WAL, then run compaction if triggered.

## Compaction

Leveled. Triggers: L0 file count >= `l0_compaction_trigger` (merge all L0 plus
overlapping L1 into L1), or level size over target (base size for L1, x10 per
level after; pick the first file, merge with overlapping files one level down).
Merged output keeps only the newest version of each user key. Obsolete inputs
are deleted after the manifest commit.

## Crash recovery on open

1. Read manifest (missing manifest = fresh database).
2. Delete unreferenced files.
3. Replay WAL generations in file-number order into a fresh memtable,
   truncating each at the first invalid record; restore last_seq to the max
   of manifest and WAL seqnos.

## Checkpoint

`Checkpoint(dir)`: block writes briefly, flush the memtable, hard-link all live
`.sst` files plus a copy of the manifest into `dir`. Used by Raft snapshots.

## Test strategy

- WAL: roundtrip, recovery with the tail truncated at every byte offset of the
  final record, CRC corruption.
- Skiplist: ordering with mixed seqnos, overwrite visibility.
- SSTable: write/read roundtrip, sparse index boundaries, bloom false-negative
  freedom, iterator order.
- DB: put/get/delete/scan across flushes, reopen recovery mid-generation,
  compaction correctness (overwrites and tombstones), crash between flush and
  manifest commit, checkpoint restore.
