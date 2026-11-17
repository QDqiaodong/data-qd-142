"""Command line interface for the cross-batch safe aggregate publication workbench.

Usage:
    python -m distribution_drift.disclosure_cli init --db DB --spec SPEC.json
    python -m distribution_drift.disclosure_cli preview --db DB --request REQUEST.json
    python -m distribution_drift.disclosure_cli release --db DB --request REQUEST.json
    python -m distribution_drift.disclosure_cli receipt --db DB --request-id ID
"""
DESCRIPTION = ('Cross-batch safe aggregate publication workbench '
               '(SQLite, standard library only)')
EPILOG = """\
spec (init): strict JSON {"categories", "counts", "protected"} with 2..32
  unique category identifiers, one {"category", "count"} row per declared
  category (strict integer 0..10^9) and a nonempty unique protected subset.
  The private vector is fixed in the database at init; re-running init with
  the canonically identical spec (array order is irrelevant) is an
  idempotent no-op, any difference is rejected.

request (preview/release): strict JSON {"request_id", "queries"} with
  1..22 queries {"id", "members", "utility"}; ids unique within the request,
  members a nonempty unique subset of the declared categories, utility a
  strict integer 1..10^6.  Member and query array order is irrelevant for
  the request identity; utility and any set content are part of it.

release prints the committed public receipt (request_id, history
  generation, total utility and the selected {id, members, value} rows
  sorted by id) and nothing else; replaying the canonically identical
  request prints the stored receipt bytes verbatim.  receipt prints the
  stored receipt for a request id.  preview prints the planned selection
  and the history generation it is based on, without values and without
  registering anything.

exit codes: 0 on success, 2 on any contract, conflict or I/O error
  (stderr carries the message; committed state is never half-written).
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .common import parse_strict, read_bytes
from .disclosure import Store, init_db, parse_spec_bytes


def _parser():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)

    init = sub.add_parser('init', help='create a workbench database and fix its private vector',
                          description=DESCRIPTION, epilog=EPILOG,
                          formatter_class=argparse.RawDescriptionHelpFormatter)
    init.add_argument('--db', type=Path, required=True, help='workbench SQLite database')
    init.add_argument('--spec', type=Path, required=True,
                      help='strict JSON spec {"categories", "counts", "protected"}')

    preview = sub.add_parser('preview', help='plan against the current history (registers nothing)',
                             description=DESCRIPTION, epilog=EPILOG,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
    preview.add_argument('--db', type=Path, required=True, help='workbench SQLite database')
    preview.add_argument('--request', type=Path, required=True,
                         help='strict JSON request {"request_id", "queries"}')

    release = sub.add_parser('release', help='commit the optimal safe selection as public equations',
                             description=DESCRIPTION, epilog=EPILOG,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
    release.add_argument('--db', type=Path, required=True, help='workbench SQLite database')
    release.add_argument('--request', type=Path, required=True,
                         help='strict JSON request {"request_id", "queries"}')

    receipt = sub.add_parser('receipt', help='print the stored public receipt for a request id',
                             description=DESCRIPTION, epilog=EPILOG,
                             formatter_class=argparse.RawDescriptionHelpFormatter)
    receipt.add_argument('--db', type=Path, required=True, help='workbench SQLite database')
    receipt.add_argument('--request-id', required=True, help='registered request identifier')
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command == 'init':
            spec = parse_spec_bytes(read_bytes(args.spec))
            store, created = init_db(args.db, spec)
            with store:
                result = {'status': 'initialized' if created else 'unchanged',
                      'db': str(args.db)}
            print(json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False))
            return 0
        if args.command == 'preview':
            with Store(args.db) as store:
                preview = store.preview_bytes(read_bytes(args.request))
            print(json.dumps(preview, sort_keys=True, ensure_ascii=False, allow_nan=False))
            return 0
        if args.command == 'release':
            with Store(args.db) as store:
                receipt, text, idempotent = store.release_bytes(read_bytes(args.request))
            sys.stdout.write(text)
            return 0
        with Store(args.db) as store:
            text = store.receipt(args.request_id)
        sys.stdout.write(text)
        return 0
    except (ValueError, TypeError, KeyError, OSError, OverflowError,
            RecursionError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
