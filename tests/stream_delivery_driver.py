"""Export crash-injection driver for the delivery-recovery acceptance tests.

Runs a real subprocess export and crashes the process with ``os._exit`` at a
chosen point, so the parent test can verify that a restarted export completes
delivery byte-identically:

* ``--crash before-publish``: the report rows are committed in the database
  but the process dies before any file replace (the temp-file publish step
  is replaced by an immediate exit);
* ``--crash after-replace``: the first target file is atomically replaced
  but the process dies before the delivery confirmation is committed.
"""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from distribution_drift import stream  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--crash', choices=('none', 'before-publish', 'after-replace'),
                        default='none')
    args = parser.parse_args()
    store = stream.Store(args.db)
    if args.crash == 'before-publish':
        def crash_publish(path, data):
            os._exit(3)
        stream._publish_file = crash_publish
    elif args.crash == 'after-replace':
        def crash_confirm(end):
            os._exit(4)
        store._confirm = crash_confirm
    result = store.export(args.output_dir)
    store.close()
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
