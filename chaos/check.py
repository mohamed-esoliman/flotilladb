"""Offline re-check of a recorded history: python3 chaos/check.py history.jsonl"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import checker  # noqa: E402
import history as history_mod  # noqa: E402


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    merged = history_mod.load_history(sys.argv[1])
    results = checker.check_history(merged)
    all_ok = True
    for key, result in results.items():
        marker = "OK " if result.ok else "FAIL"
        print(f"{marker} {key}: {result.reason} ({result.states} states)")
        all_ok = all_ok and result.ok
    print("PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
