"""Checker unit tests: known-good and known-bad histories."""

import unittest

from checker import check_key


def op(op_id, kind, inv, ret, value=None, result=None, outcome="ok"):
    rec = {"id": op_id, "op": kind, "key": "k", "invoked": inv, "returned": ret,
           "outcome": outcome}
    if value is not None:
        rec["value"] = value
    if kind == "get":
        rec["result"] = result
        if outcome == "ok" and result is None:
            rec["outcome"] = "notfound"
    return rec


class CheckerTest(unittest.TestCase):
    def test_empty_history_ok(self):
        self.assertTrue(check_key([]).ok)

    def test_sequential_ops_ok(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "get", 3, 4, result="a"),
            op(2, "put", 5, 6, value="b"),
            op(3, "get", 7, 8, result="b"),
            op(4, "del", 9, 10),
            op(5, "get", 11, 12, result=None),
        ]
        self.assertTrue(check_key(history).ok)

    def test_read_fresh_value_during_overlap_ok(self):
        # get overlaps the put and may see either old or new value.
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, 7, value="b"),
            op(2, "get", 4, 6, result="b"),
        ]
        self.assertTrue(check_key(history).ok)

    def test_read_old_value_during_overlap_ok(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, 7, value="b"),
            op(2, "get", 4, 6, result="a"),
        ]
        self.assertTrue(check_key(history).ok)

    def test_stale_read_bad(self):
        # put(b) completed before the get began; reading "a" is stale.
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, 4, value="b"),
            op(2, "get", 5, 6, result="a"),
        ]
        result = check_key(history)
        self.assertFalse(result.ok)

    def test_lost_acknowledged_write_bad(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "get", 3, 4, result=None),
        ]
        self.assertFalse(check_key(history).ok)

    def test_phantom_value_bad(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "get", 3, 4, result="never-written"),
        ]
        self.assertFalse(check_key(history).ok)

    def test_timed_out_write_may_apply(self):
        # The put timed out but a later read observes it: allowed.
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, None, value="b", outcome="timeout"),
            op(2, "get", 10, 11, result="b"),
        ]
        self.assertTrue(check_key(history).ok)

    def test_timed_out_write_may_never_apply(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, None, value="b", outcome="timeout"),
            op(2, "get", 10, 11, result="a"),
        ]
        self.assertTrue(check_key(history).ok)

    def test_timed_out_write_cannot_unhappen_after_read(self):
        # Read sees "b", later read sees "a" again with no writer in between:
        # not linearizable even though put(b) timed out.
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, None, value="b", outcome="timeout"),
            op(2, "get", 10, 11, result="b"),
            op(3, "get", 12, 13, result="a"),
        ]
        self.assertFalse(check_key(history).ok)

    def test_read_own_window_ordering_bad(self):
        # Two sequential reads observe values in an impossible order.
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "put", 3, 4, value="b"),
            op(2, "get", 5, 6, result="b"),
            op(3, "get", 7, 8, result="a"),
        ]
        self.assertFalse(check_key(history).ok)

    def test_delete_visible(self):
        history = [
            op(0, "put", 1, 2, value="a"),
            op(1, "del", 3, 4),
            op(2, "get", 5, 6, result=None),
        ]
        self.assertTrue(check_key(history).ok)

    def test_concurrent_writers_both_orders_ok(self):
        history = [
            op(0, "put", 1, 5, value="a"),
            op(1, "put", 2, 6, value="b"),
            op(2, "get", 7, 8, result="a"),
        ]
        self.assertTrue(check_key(history).ok)

    def test_larger_interleaved_history(self):
        history = []
        op_id = 0
        t = 0.0
        value = None
        for round_num in range(40):
            value = f"v{round_num}"
            history.append(op(op_id, "put", t, t + 1, value=value))
            op_id += 1
            history.append(op(op_id, "get", t + 2, t + 3, result=value))
            op_id += 1
            t += 4
        result = check_key(history)
        self.assertTrue(result.ok)


if __name__ == "__main__":
    unittest.main()
