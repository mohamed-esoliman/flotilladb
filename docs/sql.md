# SQL subset

A hand-written lexer, recursive-descent parser, and planner/executor for a
deliberate SQL subset, executed over the transactional KV API (TiDB-style row
mapping). The SQL layer lives client-side (`src/sql/`, linked into
`flotilla-cli` and tests): every statement runs inside its own
snapshot-isolation transaction via `client::Txn`, so the server needs no SQL
awareness at all.

## Supported statements

    CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)
    INSERT INTO t VALUES (1, 'ada', 36), (2, 'grace', 45)
    INSERT INTO t (id, name, age) VALUES (3, 'edsger', 72)
    SELECT * FROM t
    SELECT name, age FROM t WHERE age >= 40 AND name != 'x'
    UPDATE t SET age = 37 WHERE id = 1
    DELETE FROM t WHERE age < 40
    DROP TABLE t

Types: INT (64-bit signed) and TEXT. Exactly one PRIMARY KEY column, any
type. WHERE is a conjunction of `col op literal` (=, !=, <, <=, >, >=). No
joins, ORDER BY, aggregates, NULLs, or secondary indexes — a documented
non-goal (the spec's SQL layer is a deliberate subset; index encoding is
described but out of scope for v1).

## Catalog and row mapping

All SQL state lives in the transactional keyspace:

    __cat/<table_name>      -> schema { table_id u32, cols [name, type, is_pk] }
    __cat_next_id           -> next table id
    t<table_id BE>r<pk_enc> -> row [u32 ncols][per col: u8 type][lp bytes]

`pk_enc` is order-preserving: INT as big-endian with the sign bit flipped,
TEXT as raw bytes. A table scan is a TxnScan over the `t<table_id>r` prefix,
so rows come back in primary-key order.

DDL is transactional like everything else: CREATE TABLE reads the catalog,
allocates an id, and writes the schema in one transaction (a concurrent
CREATE of the same name loses the write-write conflict).

## Planner

Point plan when the WHERE clause pins the primary key with equality
(`pk = literal`): a single TxnGet. Everything else is a full table scan with
predicate filtering on the client. UPDATE/DELETE reuse the same access path,
buffer their writes in the transaction, and commit atomically — a multi-row
UPDATE either lands entirely or not at all, across shards.

## Errors

Parse errors carry position and expectation ("expected ')' at 'WHERE'");
execution errors are typed (unknown table/column, type mismatch, duplicate
primary key on INSERT, conflict aborts surfaced as retryable errors).

## CLI

`sql <statement>` runs one statement; `sql` alone enters an interactive SQL
shell. Results print as aligned tables with a row count; writes report rows
affected.

## Testing

- Lexer/parser unit tests including error positions.
- Row/PK encoding order-preservation tests.
- End-to-end over a live cluster: CREATE/INSERT/SELECT/UPDATE/DELETE happy
  paths, WHERE operators on both types, duplicate-PK rejection, multi-row
  atomic UPDATE across a split, concurrent INSERT conflicts.
