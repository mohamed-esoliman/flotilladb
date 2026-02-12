# Wire protocol and server

Custom length-prefixed binary protocol over TCP. No protobuf, no external
serialization: all encoding is the hand-rolled fixed-width little-endian coding
from `src/common/coding.h`.

## Framing

    [u32 payload_len][payload]

Payload length is capped at 32 MB; a frame that declares more is a protocol
error and the connection is closed. The first payload byte is the message type;
the rest is the type-specific body. One request maps to exactly one response on
the same connection, processed in order (no pipelining); the model is
thread-per-connection on the server, synchronous request/response in clients.

## Message types

    1  GET      { key }
    2  PUT      { key, value }
    3  DELETE   { key }
    4  SCAN     { start_key, end_key, u32 limit }   end_key "" = unbounded,
                                                    end is exclusive, limit 0 = server default
    5  STATUS   { }
    16 RESPONSE { u8 code, leader_addr, message, u8 found, value,
                  u32 count, count * (key, value) }
    32 RAFT     { raft message body }                (milestone 3, node-to-node)

Strings are `[u32 len][bytes]`. RESPONSE is shared by every request type:

- `code` 0 is success; other values mirror `Status::Code` (not found = 1,
  not leader = 5, ...).
- `leader_addr` (host:port, client-facing) accompanies a NOT_LEADER code so the
  client can redirect. Empty when unknown (e.g. mid-election).
- GET uses `found`/`value`; SCAN and STATUS use the `(key, value)` list
  (STATUS reports info fields as string pairs).

Unknown request types get a RESPONSE with an invalid-argument code, keeping the
connection usable; a malformed frame body closes the connection.

## Invariants

- A response is written for every fully-read request frame, in receive order.
- The server never blocks a connection on another connection's request
  (storage-level locking is bounded work: no request holds the DB mutex while
  waiting on the network).
- Frame reads and writes are complete-or-fail: short reads mean the peer died
  and the connection is dropped without a partial dispatch.

## Server model

`TcpServer` owns the listen socket and an accept loop thread; each accepted
connection gets a thread running: read frame, dispatch to the registered
handler, write response frame, repeat until EOF/error. `Stop()` closes the
listen socket, shuts down every open connection socket, and joins all threads.
Thread-per-connection is a deliberate v1 choice (the spec keeps an event-loop
server as a stretch goal); chaos-scale clusters use tens of connections, not
thousands.

In milestone 2 the handler applies ops directly to one storage engine
(single-node mode, no consensus). Milestone 3 replaces that handler with the
Raft-backed service; the protocol does not change.

## CLI

`flotilla-cli` connects to one node and provides GET/PUT/DELETE/SCAN/STATUS as
a REPL with aligned tabular output. On a NOT_LEADER response it reconnects to
`leader_addr` and retries the command transparently (printing the redirect).
The redirect/retry logic lives in a small client library (`src/client/client.h`)
that tests and the chaos harness's C++ pieces can reuse.
