"""Command line interface for the immutable revision ledger and sealed snapshots.

Usage:
    python -m distribution_drift.snapshot_cli init --db DB
    python -m distribution_drift.snapshot_cli ingest --db DB --source SHARD.json
    python -m distribution_drift.snapshot_cli seal --db DB --manifest MANIFEST.json --output REPORT.json
"""
DESCRIPTION='Immutable revision chains and sealed drift snapshots (SQLite, standard library only)'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import save_bytes
from .snapshot import Store, SnapshotError


def _parser():
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    sub=parser.add_subparsers(dest='command',required=True)

    init=sub.add_parser('init',help='create or open a ledger database')
    init.add_argument('--db',type=Path,required=True)

    ingest=sub.add_parser('ingest',help='append one shard revision')
    ingest.add_argument('--db',type=Path,required=True)
    ingest.add_argument('--source',type=Path,required=True)

    seal=sub.add_parser('seal',help='seal a manifest into an immutable report')
    seal.add_argument('--db',type=Path,required=True)
    seal.add_argument('--manifest',type=Path,required=True)
    seal.add_argument('--output',type=Path,required=True)
    return parser


def main(argv=None):
    args=_parser().parse_args(argv)
    try:
        if args.command=='init':
            with Store(args.db) as store:
                result={'status':'initialized','db':str(args.db)}
            print(json.dumps(result,sort_keys=True))
            return 0
        if args.command=='ingest':
            with Store(args.db) as store:
                digest,idempotent=store.ingest_file(args.source)
            print(json.dumps({'status':'idempotent' if idempotent else 'ingested',
                              'content_sha256':digest},sort_keys=True))
            return 0
        if args.command=='seal':
            if args.manifest.resolve()==args.output.resolve():
                raise SnapshotError('manifest and output must differ')
            with Store(args.db) as store:
                report,text,idempotent=store.seal_manifest_file(args.manifest)
            save_bytes(args.output,text.encode('utf-8'))
            print(json.dumps({'status':'replayed' if idempotent else 'sealed',
                              'snapshot_id':report['snapshot_id'],
                              'manifest_sha256':report['manifest_sha256'],
                              'output':str(args.output)},sort_keys=True))
            return 0
    except (ValueError,TypeError,KeyError,OSError,OverflowError,
            RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr)
        return 2
    return 2


if __name__=='__main__':
    raise SystemExit(main())
