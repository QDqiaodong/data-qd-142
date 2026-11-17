"""Command line interface for the restartable event-time drift pipeline.

Usage:
    python -m distribution_drift.stream_cli init --db DB --config CONFIG.json
    python -m distribution_drift.stream_cli apply --db DB --input EVENTS.jsonl
    python -m distribution_drift.stream_cli export --db DB --output-dir DIR
    python -m distribution_drift.stream_cli status --db DB
"""
DESCRIPTION = ('Restartable event-time drift monitoring pipeline '
               '(SQLite, standard library only)')
EPILOG = """\
config (init): strict JSON {"sources", "categories", "alpha", "width"} with
  1..64 unique source identifiers, 1..1000 unique category identifiers and
  alpha exactly as in the model.analyze contract, and a strict integer
  window width in 1..1000000.  The config is fixed in the database at init;
  re-running init with the identical config is an idempotent no-op and never
  loses progress, a different config is rejected.

input (apply): UTF-8 JSONL read once, in order, from a seekable file or any
  one-shot stream.  Each line is an event {"source", "seq", "kind": "event",
  "ts", "category"} or a watermark {"source", "seq", "kind": "watermark",
  "time"}.  Blank lines, duplicate JSON keys, NaN/Infinity constants,
  unknown fields, lines over 65536 bytes, non-strict integers, unknown
  sources and out-of-domain categories are rejected.  Per source, seq is
  accepted contiguously from 1; a gap fails the whole batch.  Replaying
  already-committed sequence numbers is allowed in any later batch and is
  idempotent exactly when the canonical payload is identical (JSON
  whitespace and key order are irrelevant); the same seq with a different
  payload rejects the whole apply.  Events with ts below their source's
  current watermark are quarantined as late (seq is consumed, never counted
  in windows); ts equal to the watermark is still accepted.  One apply
  accepts at most 10,000,000 lines and commits atomically: any invalid line
  — even the last one — rolls back every source progress, late-audit and
  report change of the batch.

windows: a window with end E = k*width (k >= 2) is closed once the minimum
  watermark G across all configured sources reaches E; the reference
  interval is [E-2*width, E-width) and the current interval [E-width, E).
  Windows with no events on both sides produce no file; every other window
  is generated exactly once.  When both sides are positive the report
  embeds the model.analyze distribution over the full declared domain;
  otherwise distribution is null with status insufficient_population.

export: publishes every committed report as OUTPUT_DIR/window-END.json in
  ascending end order and is safe to re-run.  There is no cross-media atomic
  transaction between the database commit and the file publish, so a crash
  may leave a committed report unwritten, or written but unconfirmed.
  Recovery is simply re-running export: stored report bytes are published
  verbatim (never recomputed), a target already holding identical bytes
  counts as success, and a target with different bytes or a symlink aborts
  the run with the foreign object untouched — remove it manually and re-run
  export to complete delivery.  apply never publishes files, so uncommitted
  content can never be mistaken for a valid report.

status: prints the fixed config, per-source committed seq / watermark /
  accepted / late counts, the global minimum watermark, the next window end
  and the pending/delivered report counts.  Late-event audit records are
  available through the streaming API Store.iter_late([source]) in
  (source, seq) order.

exit codes: 0 on success, 2 on any contract, conflict, delivery or I/O
  error (stderr carries the message; committed state is never half-written).
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .common import parse_strict, read_bytes
from .stream import Store, init_db


def _parser():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)

    init = sub.add_parser('init', help='create a pipeline database and fix its config',
                          description=DESCRIPTION, epilog=EPILOG,
                          formatter_class=argparse.RawDescriptionHelpFormatter)
    init.add_argument('--db', type=Path, required=True, help='pipeline SQLite database')
    init.add_argument('--config', type=Path, required=True,
                      help='strict JSON config {"sources", "categories", "alpha", "width"}')

    apply = sub.add_parser('apply', help='atomically accept one JSONL batch',
                           description=DESCRIPTION, epilog=EPILOG,
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    apply.add_argument('--db', type=Path, required=True, help='pipeline SQLite database')
    apply.add_argument('--input', type=Path, required=True,
                       help='UTF-8 JSONL event/watermark stream, read once in order')

    export = sub.add_parser('export', help='publish committed reports to files',
                            description=DESCRIPTION, epilog=EPILOG,
                            formatter_class=argparse.RawDescriptionHelpFormatter)
    export.add_argument('--db', type=Path, required=True, help='pipeline SQLite database')
    export.add_argument('--output-dir', type=Path, required=True,
                        help='directory receiving window-END.json reports')

    status = sub.add_parser('status', help='print source progress and report counts',
                            description=DESCRIPTION, epilog=EPILOG,
                            formatter_class=argparse.RawDescriptionHelpFormatter)
    status.add_argument('--db', type=Path, required=True, help='pipeline SQLite database')
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command == 'init':
            config = parse_strict(read_bytes(args.config))
            store, created = init_db(args.db, config)
            with store:
                result = {'status': 'initialized' if created else 'unchanged',
                          'db': str(args.db)}
        elif args.command == 'apply':
            with Store(args.db) as store:
                summary = store.apply_file(args.input)
            result = {'status': 'applied', **summary}
        elif args.command == 'export':
            with Store(args.db) as store:
                outcome = store.export(args.output_dir)
            result = {'status': 'exported', 'output_dir': str(args.output_dir), **outcome}
        else:
            with Store(args.db) as store:
                result = store.status()
    except (ValueError, TypeError, KeyError, OSError, OverflowError,
            RecursionError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
