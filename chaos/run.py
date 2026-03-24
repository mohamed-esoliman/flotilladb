"""Chaos test orchestrator.

    python3 chaos/run.py --seed 1 --nodes 3 --duration 30

Starts a real cluster with raft links routed through in-process proxies,
drives concurrent workloads, injects seeded faults (kill/restart/partition/
heal/latency), logs the full operation history, and checks single-key
linearizability at the end. Exits nonzero on any violation.
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import checker  # noqa: E402
import history as history_mod  # noqa: E402
from client import Client, WireError  # noqa: E402
from proxy import LinkProxy  # noqa: E402


class Cluster:
    def __init__(self, binary, workdir, nodes, port_base, log, snapshot_interval=0):
        self.snapshot_interval = snapshot_interval
        self.binary = binary
        self.workdir = workdir
        self.n = nodes
        self.port_base = port_base
        self.log = log
        self.procs = {}
        self.proxies = {}  # (src, dst) -> LinkProxy

        self.client_addr = {
            i: f"127.0.0.1:{port_base + i}" for i in range(1, nodes + 1)}
        self.raft_addr = {
            i: f"127.0.0.1:{port_base + 100 + i}" for i in range(1, nodes + 1)}
        proxy_port = port_base + 200
        for src in range(1, nodes + 1):
            for dst in range(1, nodes + 1):
                if src == dst:
                    continue
                self.proxies[(src, dst)] = LinkProxy(proxy_port, self.raft_addr[dst])
                proxy_port += 1

        for i in range(1, nodes + 1):
            self._write_config(i)

    def _write_config(self, node_id):
        # Node's own raft address is real; peers point at this node's
        # outbound proxies so each direction can be cut independently.
        lines = []
        for j in range(1, self.n + 1):
            if j == node_id:
                raft = self.raft_addr[j]
            else:
                raft = f"127.0.0.1:{self.proxies[(node_id, j)].listen_port}"
            lines.append(f"node {j} {self.client_addr[j]} {raft}")
        path = os.path.join(self.workdir, f"cluster-{node_id}.conf")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def start_node(self, node_id):
        conf = os.path.join(self.workdir, f"cluster-{node_id}.conf")
        data = os.path.join(self.workdir, f"node{node_id}")
        logfile = open(os.path.join(self.workdir, f"node{node_id}.log"), "a")
        cmd = [self.binary, "--config", conf, "--node-id", str(node_id),
               "--data-dir", data]
        if self.snapshot_interval:
            cmd += ["--snapshot-interval", str(self.snapshot_interval)]
        self.procs[node_id] = subprocess.Popen(cmd, stdout=logfile, stderr=logfile)
        self.log(f"node {node_id} started pid {self.procs[node_id].pid}")

    def kill_node(self, node_id):
        proc = self.procs.get(node_id)
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            self.log(f"node {node_id} killed")

    def alive(self, node_id):
        proc = self.procs.get(node_id)
        return proc is not None and proc.poll() is None

    def partition(self, group_a, group_b):
        for a in group_a:
            for b in group_b:
                self.proxies[(a, b)].set_drop(True)
                self.proxies[(b, a)].set_drop(True)
        self.log(f"partitioned {sorted(group_a)} | {sorted(group_b)}")

    def heal(self):
        for proxy in self.proxies.values():
            proxy.set_drop(False)
        self.log("healed all partitions")

    def set_delay(self, links, delay_ms):
        for link in links:
            self.proxies[link].set_delay(delay_ms)

    def clear_delays(self):
        for proxy in self.proxies.values():
            proxy.set_delay(0)

    def stop(self):
        for node_id in list(self.procs):
            self.kill_node(node_id)
        for proxy in self.proxies.values():
            proxy.stop()


class Workload(threading.Thread):
    def __init__(self, worker_id, cluster, logger, stop_event, seed, num_keys):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.cluster = cluster
        self.logger = logger
        self.stop_event = stop_event
        self.rng = random.Random(seed)
        self.num_keys = num_keys
        self.seq = 0
        self.ok_count = 0

    def run(self):
        client = Client(list(self.cluster.client_addr.values()), timeout=1.5,
                        max_attempts=4)
        while not self.stop_event.is_set():
            # Throttle: linearizability checking cost grows with history size;
            # tens of ops per second per worker is plenty of concurrency.
            time.sleep(self.rng.uniform(0.01, 0.04))
            roll = self.rng.random()
            key = f"k{self.rng.randrange(self.num_keys)}"
            if roll < 0.5:
                self._do(client, "put", key, f"w{self.worker_id}-{self.seq}")
                self.seq += 1
            elif roll < 0.85:
                self._do(client, "get", key, None)
            else:
                self._do(client, "del", key, None)
        client.close()

    def _do(self, client, op, key, value):
        # One shared clock for every worker: intervals must be comparable
        # across threads or the checker sees impossible orderings.
        op_id = self.logger.next_id()
        record = {"id": op_id, "phase": "invoke", "worker": self.worker_id,
                  "op": op, "key": key, "invoked": time.monotonic()}
        if value is not None:
            record["value"] = value
        self.logger.record(record)

        outcome, result = "ok", None
        try:
            if op == "put":
                client.put(key.encode(), value.encode())
            elif op == "del":
                client.delete(key.encode())
            else:
                got = client.get(key.encode())
                result = got.decode() if got is not None else None
                if result is None:
                    outcome = "notfound"
        except (OSError, WireError):
            outcome = "timeout"
        if outcome in ("ok", "notfound"):
            self.ok_count += 1

        done = {"id": op_id, "phase": "complete", "outcome": outcome,
                "returned": time.monotonic()}
        if op == "get" and outcome != "timeout":
            done["result"] = result
        self.logger.record(done)


class FaultSchedule(threading.Thread):
    def __init__(self, cluster, stop_event, seed, log):
        super().__init__(daemon=True)
        self.cluster = cluster
        self.stop_event = stop_event
        self.rng = random.Random(seed ^ 0xC4A05)
        self.log = log
        self.partitioned = False
        self.delayed = False

    def run(self):
        while not self.stop_event.wait(self.rng.uniform(2.0, 4.0)):
            self._one_fault()

    def _one_fault(self):
        c = self.cluster
        down = [i for i in range(1, c.n + 1) if not c.alive(i)]
        up = [i for i in range(1, c.n + 1) if c.alive(i)]
        choices = []
        if down:
            choices += ["restart"] * 3
        minority = (c.n - 1) // 2
        if len(down) < minority:
            choices += ["kill"] * 3
        choices += ["partition" if not self.partitioned else "heal"] * 3
        choices += ["delay" if not self.delayed else "clear_delay"] * 2
        action = self.rng.choice(choices)

        if action == "kill":
            victim = self.rng.choice(up)
            c.kill_node(victim)
        elif action == "restart":
            c.start_node(self.rng.choice(down))
        elif action == "partition":
            nodes = list(range(1, c.n + 1))
            self.rng.shuffle(nodes)
            cut = self.rng.randrange(1, c.n)
            c.partition(nodes[:cut], nodes[cut:])
            self.partitioned = True
        elif action == "heal":
            c.heal()
            self.partitioned = False
        elif action == "delay":
            links = [l for l in c.proxies if self.rng.random() < 0.5]
            delay = self.rng.randrange(50, 300)
            c.set_delay(links, delay)
            self.log(f"delaying {len(links)} links by {delay}ms")
            self.delayed = True
        else:
            c.clear_delays()
            self.log("cleared delays")
            self.delayed = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--keys", type=int, default=8)
    parser.add_argument("--port-base", type=int, default=16000)
    parser.add_argument("--binary", default=None)
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--snapshot-interval", type=int, default=300,
                        help="raft log entries between snapshots (0 = server default)")
    parser.add_argument("--no-faults", action="store_true")
    args = parser.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    binary = args.binary or os.path.join(repo, "build", "src", "server", "flotilladb")
    if not os.path.exists(binary):
        print(f"binary not found: {binary} (build first)", file=sys.stderr)
        return 2
    workdir = args.workdir or os.path.join(
        repo, "build", f"chaos-seed{args.seed}-{int(time.time())}")
    os.makedirs(workdir, exist_ok=True)

    def log(msg):
        print(f"[chaos {time.strftime('%H:%M:%S')}] {msg}", flush=True)

    log(f"seed={args.seed} nodes={args.nodes} duration={args.duration}s "
        f"workdir={workdir}")

    logger = history_mod.HistoryLogger(os.path.join(workdir, "history.jsonl"))
    cluster = Cluster(binary, workdir, args.nodes, args.port_base, log,
                      snapshot_interval=args.snapshot_interval)
    exit_code = 1
    try:
        for i in range(1, args.nodes + 1):
            cluster.start_node(i)
        time.sleep(2.0)

        stop_event = threading.Event()
        workers = [Workload(w, cluster, logger, stop_event,
                            seed=args.seed * 1000 + w, num_keys=args.keys)
                   for w in range(args.workers)]
        for w in workers:
            w.start()
        faults = None
        if not args.no_faults:
            faults = FaultSchedule(cluster, stop_event, args.seed, log)
            faults.start()

        time.sleep(args.duration)
        stop_event.set()
        for w in workers:
            w.join(timeout=10)
        if faults:
            faults.join(timeout=10)

        log("draining: heal + restart everything")
        cluster.heal()
        cluster.clear_delays()
        for i in range(1, args.nodes + 1):
            if not cluster.alive(i):
                cluster.start_node(i)
        time.sleep(3.0)

        # Final determinate reads anchor the linearizability check and catch
        # lost acknowledged writes.
        final_client = Client(list(cluster.client_addr.values()), timeout=3.0,
                              max_attempts=10)
        for key_index in range(args.keys):
            key = f"k{key_index}"
            op_id = logger.next_id()
            logger.record({"id": op_id, "phase": "invoke", "worker": -1,
                           "op": "get", "key": key, "invoked": time.monotonic()})
            got = final_client.get(key.encode())
            result = got.decode() if got is not None else None
            logger.record({"id": op_id, "phase": "complete",
                           "outcome": "ok" if result is not None else "notfound",
                           "result": result, "returned": time.monotonic()})
        final_client.close()

        ok_ops = sum(w.ok_count for w in workers)
        log(f"workloads finished: {ok_ops} successful operations")
        logger.close()

        merged = history_mod.load_history(os.path.join(workdir, "history.jsonl"))
        results = checker.check_history(merged)
        all_ok = True
        for key, result in results.items():
            marker = "OK " if result.ok else "FAIL"
            log(f"  {marker} {key}: {result.reason} ({result.states} states)")
            all_ok = all_ok and result.ok

        if ok_ops == 0:
            log("VERDICT: FAIL (no operation ever succeeded)")
        elif not all_ok:
            log(f"VERDICT: FAIL (non-linearizable history, reproduce with "
                f"--seed {args.seed})")
        else:
            log(f"VERDICT: PASS ({len(merged)} ops linearizable)")
            exit_code = 0
    finally:
        cluster.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
