"""Command line interface for the entity cohort migration pipeline.

Usage:
    python -m distribution_drift.cohort_cli audit \
        --reference REF.jsonl --current CUR.jsonl --config CONFIG.json \
        --output CONFLICTS.jsonl
    python -m distribution_drift.cohort_cli reconcile \
        --reference REF.jsonl --current CUR.jsonl --config CONFIG.json \
        --rulings RULINGS.jsonl --output REPORT.json
"""
DESCRIPTION = 'Entity cohort migration audit and reconciliation (stdlib + SQLite external storage)'
EPILOG = """\
input formats:
  reference/current: UTF-8 JSONL, one strict {"entity", "category"} object per
    line; blank lines, duplicate JSON keys, NaN/Infinity constants, unknown
    fields and lines longer than 65536 bytes are rejected; the file may be
    empty; at most 10,000,000 lines per side; entities match across files by
    exact string equality (no case folding).
  config: strict JSON {"categories", "alpha"} with 1..1000 unique category
    identifiers and a strict positive integer alpha (model.analyze contract).
  rulings (reconcile only): UTF-8 JSONL, one
    {"side", "entity", "expected_categories", "selected"} object per line;
    expected_categories is the complete sorted unique audited candidate list
    and selected is one of those categories or null; stale, duplicate,
    non-conflict, missing-entity or mismatched rulings fail the whole run.
resource strategy:
  inputs are streamed once in order (no seek or re-read); every entity- or
  row-proportional structure lives in a per-run temporary SQLite database that
  is deleted on exit and never reused as cross-run state; the Python process
  keeps only scalar counters, the bounded category domain and streaming
  cursors (peak RSS target <= 128 MiB including the SQLite page cache, which
  is capped at ~8 MiB); temporary disk grows with the input and only non-zero
  migration cells are materialised (no category cartesian product); results
  are written to a sibling temporary file and atomically replace the target,
  and any invalid line or ruling preserves any pre-existing target file.
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .cohort import (CohortError, load_config, run_audit, run_reconcile,
                     write_audit_jsonl, write_reconcile_report)


def _parser():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--reference', type=Path, required=True,
                        help='reference-side observation JSONL')
    common.add_argument('--current', type=Path, required=True,
                        help='current-side observation JSONL')
    common.add_argument('--config', type=Path, required=True,
                        help='strict JSON config {"categories", "alpha"}')
    common.add_argument('--output', type=Path, required=True,
                        help='target path; a symlink or an input path is rejected')

    audit = sub.add_parser('audit', parents=[common],
                           help='fold repeats and emit same-side candidate conflicts as JSONL',
                           description=DESCRIPTION, epilog=EPILOG,
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    reconcile = sub.add_parser(
        'reconcile', parents=[common],
        help='apply rulings, isolate unresolved/excluded entities and produce the migration report',
        description=DESCRIPTION, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    reconcile.add_argument('--rulings', type=Path, required=True,
                           help='adjudication JSONL for current-run conflict entities')
    return parser


def _ensure_distinct(paths):
    resolved = [path.resolve() for path in paths]
    seen = set()
    for path in resolved:
        if path in seen:
            raise CohortError('input and output paths must all differ')
        seen.add(path)


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command == 'audit':
            _ensure_distinct((args.reference, args.current, args.config, args.output))
            domain, alpha, _ = load_config(args.config)
            with args.reference.open('rb') as ref, args.current.open('rb') as cur:
                store = run_audit(ref, cur, {'categories': domain, 'alpha': alpha})
            try:
                conflicts = write_audit_jsonl(store, args.output)
                result = {'status': 'audited', 'conflicts': conflicts,
                          'audit': store.side_audit(), 'output': str(args.output)}
            finally:
                store.close()
        else:
            _ensure_distinct((args.reference, args.current, args.config,
                              args.rulings, args.output))
            domain, alpha, _ = load_config(args.config)
            with args.reference.open('rb') as ref, args.current.open('rb') as cur, \
                    args.rulings.open('rb') as rulings:
                store = run_reconcile(
                    ref, cur, {'categories': domain, 'alpha': alpha}, rulings)
            try:
                summary = write_reconcile_report(
                    store, args.output, domain, alpha)
                result = {'status': 'reconciled', 'output': str(args.output), **summary}
            finally:
                store.close()
    except (CohortError, ValueError, TypeError, KeyError, OSError,
            OverflowError, RecursionError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
