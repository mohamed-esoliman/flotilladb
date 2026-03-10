"""Threaded TCP proxy for one directed raft link, with fault controls.

drop: existing connections are torn down and new data is refused, modeling a
network partition; the raft transport reconnects and raft retransmits.
delay_ms: each chunk is held before forwarding, modeling link latency.
"""

import socket
import threading
import time


class LinkProxy:
    def __init__(self, listen_port: int, target_addr: str):
        self.listen_port = listen_port
        self.target_addr = target_addr
        self.dropping = False
        self.delay_ms = 0
        self._lock = threading.Lock()
        self._conns = set()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", listen_port))
        self._listener.listen(64)
        self._stopping = False
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def set_drop(self, dropping: bool):
        with self._lock:
            self.dropping = dropping
            if dropping:
                for sock in list(self._conns):
                    self._kill(sock)

    def set_delay(self, delay_ms: int):
        with self._lock:
            self.delay_ms = delay_ms

    def stop(self):
        self._stopping = True
        try:
            self._listener.close()
        except OSError:
            pass
        with self._lock:
            for sock in list(self._conns):
                self._kill(sock)

    def _kill(self, sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass
        self._conns.discard(sock)

    def _accept_loop(self):
        while not self._stopping:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                if self.dropping or self._stopping:
                    self._kill(client)
                    continue
                self._conns.add(client)
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        try:
            host, port = self.target_addr.rsplit(":", 1)
            upstream = socket.create_connection((host, int(port)), timeout=2.0)
            upstream.settimeout(None)
        except OSError:
            with self._lock:
                self._kill(client)
            return
        with self._lock:
            if self.dropping or self._stopping:
                self._kill(client)
                try:
                    upstream.close()
                except OSError:
                    pass
                return
            self._conns.add(upstream)
        threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
        threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    def _pump(self, src, dst):
        while True:
            try:
                data = src.recv(65536)
            except OSError:
                break
            if not data:
                break
            with self._lock:
                if self.dropping:
                    break
                delay = self.delay_ms
            if delay:
                time.sleep(delay / 1000.0)
            try:
                dst.sendall(data)
            except OSError:
                break
        with self._lock:
            self._kill(src)
            self._kill(dst)
