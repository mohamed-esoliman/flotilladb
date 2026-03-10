"""Operation history records and JSONL logging."""

import json
import threading


class HistoryLogger:
    def __init__(self, path: str):
        self._file = open(path, "w", encoding="utf-8")
        self._lock = threading.Lock()
        self._next_id = 0

    def next_id(self) -> int:
        with self._lock:
            op_id = self._next_id
            self._next_id += 1
            return op_id

    def record(self, event: dict):
        with self._lock:
            self._file.write(json.dumps(event) + "\n")
            self._file.flush()

    def close(self):
        with self._lock:
            self._file.close()


def load_history(path: str):
    """Merges invoke/complete records into one op dict per id."""
    ops = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            op_id = event["id"]
            if event["phase"] == "invoke":
                ops[op_id] = event
            else:
                ops[op_id].update(event)
    return list(ops.values())
