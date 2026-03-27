"""Wire-protocol client mirroring src/net/messages.h.

Framing: [u32 len][payload]; payload = [u8 type][body]. All integers are
little-endian. Strings are [u32 len][bytes].
"""

import socket
import struct

MSG_GET = 1
MSG_PUT = 2
MSG_DELETE = 3
MSG_SCAN = 4
MSG_STATUS = 5
MSG_RESPONSE = 16

CODE_OK = 0
CODE_NOT_FOUND = 1
CODE_NOT_LEADER = 5


class WireError(Exception):
    pass


def _lp(data: bytes) -> bytes:
    return struct.pack("<I", len(data)) + data


def encode_request(msg_type: int, key: bytes = b"", value: bytes = b"",
                   end_key: bytes = b"", limit: int = 0, flags: int = 0) -> bytes:
    return (struct.pack("<B", msg_type) + _lp(key) + _lp(value) + _lp(end_key) +
            struct.pack("<IB", limit, flags))


class _Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        if len(self.data) - self.pos < n:
            raise WireError("short payload")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def string(self) -> bytes:
        return self.take(self.u32())


def decode_response(payload: bytes) -> dict:
    r = _Reader(payload)
    if r.u8() != MSG_RESPONSE:
        raise WireError("not a response frame")
    resp = {
        "code": r.u8(),
        "leader_addr": r.string().decode(),
        "message": r.string().decode(),
        "found": r.u8() != 0,
        "value": r.string(),
    }
    count = r.u32()
    resp["kvs"] = [(r.string(), r.string()) for _ in range(count)]
    return resp


class Connection:
    def __init__(self, addr: str, timeout: float = 2.0):
        host, port = addr.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=timeout)
        self.sock.settimeout(timeout)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def call(self, payload: bytes) -> dict:
        self.sock.sendall(struct.pack("<I", len(payload)) + payload)
        header = self._read_exact(4)
        (length,) = struct.unpack("<I", header)
        if length > 32 << 20:
            raise WireError("oversized frame")
        return decode_response(self._read_exact(length))

    def _read_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise WireError("connection closed")
            buf += chunk
        return buf


class Client:
    """Follows NOT_LEADER redirects and rotates through node addresses."""

    def __init__(self, addrs, timeout: float = 2.0, max_attempts: int = 8):
        self.addrs = list(addrs)
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.conn = None
        self.next_addr = 0

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _connect_next(self):
        last_error = None
        for _ in range(len(self.addrs)):
            addr = self.addrs[self.next_addr % len(self.addrs)]
            self.next_addr += 1
            try:
                self.conn = Connection(addr, self.timeout)
                return
            except OSError as e:
                last_error = e
        raise last_error or OSError("no addresses")

    def _call(self, payload: bytes) -> dict:
        last_error = None
        for _ in range(self.max_attempts):
            try:
                if self.conn is None:
                    self._connect_next()
                resp = self.conn.call(payload)
            except (OSError, WireError) as e:
                last_error = e
                self.close()
                continue
            if resp["code"] == CODE_NOT_LEADER:
                self.close()
                leader = resp["leader_addr"]
                if leader:
                    if leader not in self.addrs:
                        self.addrs.append(leader)
                    self.next_addr = self.addrs.index(leader)
                last_error = WireError("not leader")
                continue
            return resp
        raise last_error or WireError("retries exhausted")

    def put(self, key: bytes, value: bytes):
        resp = self._call(encode_request(MSG_PUT, key=key, value=value))
        if resp["code"] != CODE_OK:
            raise WireError(f"put failed: {resp['message']}")

    def delete(self, key: bytes):
        resp = self._call(encode_request(MSG_DELETE, key=key))
        if resp["code"] != CODE_OK:
            raise WireError(f"delete failed: {resp['message']}")

    def get(self, key: bytes):
        """Returns the value, or None if not found."""
        resp = self._call(encode_request(MSG_GET, key=key))
        if resp["code"] != CODE_OK:
            raise WireError(f"get failed: {resp['message']}")
        return resp["value"] if resp["found"] else None

    def scan(self, start: bytes = b"", end: bytes = b"", limit: int = 0):
        resp = self._call(encode_request(MSG_SCAN, key=start, end_key=end,
                                         limit=limit))
        if resp["code"] != CODE_OK:
            raise WireError(f"scan failed: {resp['message']}")
        return resp["kvs"]

    def status(self):
        resp = self._call(encode_request(MSG_STATUS))
        if resp["code"] != CODE_OK:
            raise WireError(f"status failed: {resp['message']}")
        return {k.decode(): v.decode() for k, v in resp["kvs"]}
