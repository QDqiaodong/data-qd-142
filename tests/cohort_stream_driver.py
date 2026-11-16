"""Streaming reconciliation driver for the RSS-budget acceptance test.

The observation sources are one-shot generators that yield fixed-size byte
chunks: they never seek, never re-read and never hold the whole input in
memory.  Run as a subprocess so peak RSS can be read from
``/proc/<pid>/status`` by the parent test.
"""
import argparse
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

from distribution_drift.cohort import CohortStore, write_reconcile_report  # noqa: E402


class ByteLineReader:
    """Reassemble arbitrary byte chunks into newline-delimited lines.

    Mirrors a socket/pipe transport: chunks may split a JSON record at any
    byte boundary.  Only one partial line is ever buffered, so neither the
    reader nor the ingest path holds a whole input.
    """

    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self._buffer = b''

    def read(self, size=-1):
        while b'\n' not in self._buffer:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                tail = self._buffer
                self._buffer = b''
                return tail
        newline = self._buffer.find(b'\n') + 1
        line = self._buffer[:newline]
        self._buffer = self._buffer[newline:]
        return line


def chunked_jsonl(rows, chunk_bytes=4096):
    buffer = b''
    for row in rows:
        buffer += (json.dumps(row, ensure_ascii=False) + '\n').encode('utf-8')
        while len(buffer) >= chunk_bytes:
            yield buffer[:chunk_bytes]
            buffer = buffer[chunk_bytes:]
    if buffer:
        yield buffer


def reference_rows(n_entities, categories):
    # Each entity appears under two categories -> every reference entity is a
    # conflict, so the audited candidate tables grow linearly on disk only.
    for i in range(n_entities):
        entity = 'entity-%09d' % i
        yield {'entity': entity, 'category': categories[i % len(categories)]}
        yield {'entity': entity, 'category': categories[(i + 1) % len(categories)]}


def current_rows(n_entities, categories):
    for i in range(n_entities):
        yield {'entity': 'entity-%09d' % i,
               'category': categories[(i + 2) % len(categories)]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--entities', type=int, required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--workdir', required=True)
    args = parser.parse_args()
    categories = ['a', 'b', 'c', 'd']
    config = {'categories': categories, 'alpha': 1}
    store = CohortStore(args.workdir)
    try:
        store.ingest_observations(
            'reference',
            ByteLineReader(chunked_jsonl(reference_rows(args.entities, categories))),
            frozenset(categories))
        store.ingest_observations(
            'current',
            ByteLineReader(chunked_jsonl(current_rows(args.entities, categories))),
            frozenset(categories))
        # empty ruling stream: all reference conflicts stay unresolved, which
        # exercises eligibility/isolation at full scale without any adjudications
        store.ingest_rulings(io.BytesIO(b''), frozenset(categories))
        store.build_migration()
        write_reconcile_report(store, args.output, categories, 1)
        audit = store.side_audit()
    finally:
        store.close()
    print(json.dumps({'audit': audit, 'pid': os.getpid()}, sort_keys=True))


if __name__ == '__main__':
    main()
