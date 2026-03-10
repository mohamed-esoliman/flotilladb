"""Single-key linearizability checker (Wing & Gong style DFS with memoization).

Model: a register per key. put(v) sets it, delete clears it (None), get
observes it exactly. An operation with no response ("timeout"/"error") is
indeterminate: it may take effect at any point after its invocation, or never.

An operation may be linearized next only if it was invoked no later than the
earliest completion among all pending operations (real-time order). GETs must
observe the current register value. Indeterminate operations may also be
discarded. The search memoizes (linearized-set, register value) states.
"""

INF = float("inf")


class Op:
    __slots__ = ("op_id", "kind", "value", "observed", "inv", "ret", "determinate")

    def __init__(self, op_id, kind, value, observed, inv, ret, determinate):
        self.op_id = op_id
        self.kind = kind  # "put" | "del" | "get"
        self.value = value  # put payload
        self.observed = observed  # get result (None = not found)
        self.inv = inv
        self.ret = ret
        self.determinate = determinate


def build_ops(raw_ops):
    """Converts merged history dicts (one key's ops) into Op objects.

    Indeterminate PUTs whose value no read ever observed are discarded up
    front: such a write can only ever constrain a linearization (its effect
    was either never applied or overwritten unread), never enable one, and
    each pending indeterminate op doubles the search space.
    """
    observed_values = {rec.get("result") for rec in raw_ops
                       if rec["op"] == "get" and rec.get("outcome") in ("ok", "notfound")}
    ops = []
    for rec in raw_ops:
        outcome = rec.get("outcome", "timeout")
        determinate = outcome in ("ok", "notfound")
        kind = rec["op"]
        if kind == "get" and not determinate:
            continue  # a read that observed nothing constrains nothing
        if kind == "put" and not determinate and rec.get("value") not in observed_values:
            continue
        observed = None
        if kind == "get":
            observed = rec.get("result")
        ops.append(Op(
            op_id=rec["id"],
            kind=kind,
            value=rec.get("value"),
            observed=observed,
            inv=rec["invoked"],
            ret=rec["returned"] if determinate else INF,
            determinate=determinate,
        ))
    return ops


class CheckResult:
    def __init__(self, ok, reason="", states=0):
        self.ok = ok
        self.reason = reason
        self.states = states


def check_key(raw_ops, initial=None, max_states=5_000_000):
    """Checks one key's history. Returns CheckResult."""
    ops = build_ops(raw_ops)
    if not ops:
        return CheckResult(True, "empty", 0)
    ops.sort(key=lambda o: o.inv)
    n = len(ops)
    index_of = {op.op_id: i for i, op in enumerate(ops)}

    visited = set()
    states = 0

    # Iterative DFS: stack of (done_mask, value).
    all_done = (1 << n) - 1
    stack = [(0, initial)]
    while stack:
        done, value = stack.pop()
        if done == all_done:
            return CheckResult(True, "linearizable", states)
        key = (done, value)
        if key in visited:
            continue
        visited.add(key)
        states += 1
        if states > max_states:
            return CheckResult(False, "state limit exceeded (undecided)", states)

        min_ret = INF
        for i, op in enumerate(ops):
            if done & (1 << i):
                continue
            if op.ret < min_ret:
                min_ret = op.ret

        for i, op in enumerate(ops):
            if done & (1 << i):
                continue
            if op.inv > min_ret:
                break  # ops are inv-sorted; nothing later can be next
            new_done = done | (1 << i)
            if op.kind == "get":
                if op.observed == value:
                    stack.append((new_done, value))
            else:
                effect = op.value if op.kind == "put" else None
                stack.append((new_done, effect))
                if not op.determinate:
                    # May never have taken effect.
                    stack.append((new_done, value))
    return CheckResult(False, "no valid linearization", states)


def check_history(merged_ops, max_states=5_000_000):
    """Groups merged ops by key and checks each. Returns dict key -> CheckResult."""
    by_key = {}
    for rec in merged_ops:
        by_key.setdefault(rec["key"], []).append(rec)
    return {key: check_key(ops, max_states=max_states)
            for key, ops in sorted(by_key.items())}
