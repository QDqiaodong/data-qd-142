"""Release crash/race injection driver for the disclosure acceptance tests.

Runs a real subprocess release so the parent test can verify process-crash
recovery and multi-process coordination:

* ``--crash during-solve``: the process dies with ``os._exit(3)`` at the
  solve pause point (after the knowledge snapshot is read, before the exact
  solve), so the parent can prove a killed solver neither blocks later
  releases nor leaves partial state;
* ``--crash after-commit``: the release transaction is fully committed but
  the process dies with ``os._exit(4)`` before printing the receipt, so the
  parent can prove a restarted identical request replays the stored receipt
  bytes without registering anything again;
* ``--ready FILE`` / ``--gate FILE``: the pause hook touches READY once the
  knowledge snapshot is read and then waits for GATE to exist, so a parent
  can hold two processes on the same empty history and then let them race
  to commit.
"""
import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from distribution_drift.disclosure import Store  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--request', required=True)
    parser.add_argument('--crash', choices=('none', 'during-solve', 'after-commit'),
                        default='none')
    parser.add_argument('--ready')
    parser.add_argument('--gate')
    args = parser.parse_args()

    store = Store(args.db)
    raw = Path(args.request).read_bytes()

    def pause(generation):
        if args.ready:
            Path(args.ready).touch()
        if args.gate:
            deadline = time.monotonic() + 30
            while not Path(args.gate).exists():
                if time.monotonic() > deadline:
                    raise RuntimeError('gate timeout')
                time.sleep(0.01)
        if args.crash == 'during-solve':
            os._exit(3)

    receipt, text, idempotent = store.release_bytes(raw, pause=pause)
    store.close()
    if args.crash == 'after-commit':
        os._exit(4)
    sys.stdout.write(text)


if __name__ == '__main__':
    main()
