from bisect import bisect
from datetime import datetime
from gzip import compress, decompress
from itertools import zip_longest
from pathlib import Path
from typing import Any, Generator, Generic, Iterator, List, Optional, Sequence, Tuple

from anura.btree import AVLTree
from anura.constants import BLOCK_SIZE, SPARSE_IDX_EXT, SSTABLE_EXT
from anura.io import decode, encode, read_block, write_from
from anura.metadata.parser import parse
from anura.model import K, MemNode, V
from anura.types import IType, LongType
from anura.utils import chunk, k_way_merge_sort


class MemTable(Generic[K, V]):
    def __init__(self) -> None:
        self._btree = AVLTree[MemNode[K, V]]()

    def __getitem__(self, key: K) -> Optional[V]:
        data = self._btree.find(MemNode[K, V](key))
        if data and not data.is_deleted:
            return data.value
        return None

    def __setitem__(self, key: K, value: V) -> None:
        self._btree.insert(MemNode[K, V](key, value))

    def __delitem__(self, key: K) -> None:
        if node := self._btree.find(MemNode[K, V](key)):
            node.is_deleted = True

    def __repr__(self) -> str:
        return repr(self._btree)

    def __iter__(self) -> Generator[MemNode[K, V], None, None]:
        for node in self._btree:
            yield node.data


class Metadata:
    def __init__(self, path: Path):
        self._path = path / "meta.data"
        with open(self._path) as f:
            self._meta = parse(f.read())

    @property
    def key_type(self) -> Any:
        return self._meta["key"]

    @property
    def value_type(self) -> Any:
        return self._meta["value"]

    @property
    def tombstone_type(self) -> Any:
        return self._meta["tombstone"]

    def __iter__(self) -> Iterator[IType]:
        return iter((self.key_type, self.value_type, self.tombstone_type))


class SSTable(Generic[K, V]):
    _offset_meta = LongType()

    def __init__(self, path: Path, metadata: Metadata, serial: Optional[int] = None):
        self._index: List[Tuple[K, int]] = []
        self._metadata = list(metadata)
        self._index_meta = (metadata.key_type, self._offset_meta)
        # TODO microsecond precision
        self.serial = serial or int(datetime.utcnow().timestamp())
        self._table_path = path / f"{self.serial}.{SSTABLE_EXT}"
        self._index_path = path / f"{self.serial}.{SPARSE_IDX_EXT}"

    @staticmethod
    def _search(key: K, block: Sequence[Any]) -> Optional[MemNode[K, V]]:
        j = bisect(block, key, key=lambda x: x[0]) - 1  # type: ignore[call-overload]
        if j >= 0 and key == block[j][0]:
            return MemNode[K, V](*block[j])
        return None

    def flush(self, it: Iterator, block_size: int = BLOCK_SIZE) -> None:
        # TODO consider using mmap
        pipeline = self._write_pipeline(it, block_size)
        write_from(self._table_path, pipeline)

        it = encode(self._index, self._index_meta)
        write_from(self._index_path, it)

    def _write_pipeline(self, it: Iterator, block_size: int) -> Iterator[bytes]:
        offset = 0
        for block in chunk(it, block_size):
            self._index.append((block[0].key, offset))
            acc = b"".join(encode(block, self._metadata))
            raw = compress(acc)
            yield raw
            offset += len(raw)

    def find(self, key: K) -> Optional[MemNode[K, V]]:
        i = bisect(self._index, key, key=lambda x: x[0])  # type: ignore[call-overload]
        if i == 0:
            return None

        offset = self._index[i - 1][1]
        length = -1
        if i < len(self._index):
            length = self._index[i][1] - offset

        # read pipeline
        block = self._read_pipeline(offset, length)

        # search in block
        return self._search(key, list(block))

    def _read_pipeline(self, offset: int, length: int) -> Iterator[Sequence[Any]]:
        raw = read_block(self._table_path, offset=offset, n=length)
        uncompressed = decompress(raw)
        yield from decode(uncompressed, self._metadata)

    def seq_scan(self) -> Iterator[MemNode[K, V]]:
        for (_, offset), _next in zip_longest(self._index, self._index[1:]):
            length = -1
            if _next:
                length = _next[1] - offset

            for record in self._read_pipeline(offset, length):
                yield MemNode[K, V](*record)

    def __iter__(self) -> Iterator[MemNode[K, V]]:
        return self.seq_scan()


class LSMTree(Generic[K, V]):
    # TODO: background process compacting tables: merge tables

    def __init__(self, path: Path) -> None:
        self._path = path
        # TODO invert control
        self._meta = Metadata(self._path)
        self._mem_table = MemTable[K, V]()
        self._tables: List[SSTable[K, V]] = []

    def get(self, key: K) -> Optional[V]:
        value = self._mem_table[key]
        if not value:
            return self._find(key)
        return value

    def put(self, key: K, value: V) -> None:
        self._mem_table[key] = value

    def delete(self, key: K) -> None:
        del self._mem_table[key]

    def merge(self) -> Iterator[MemNode[K, V]]:
        # TODO invert control
        prev = None
        for curr in k_way_merge_sort(self._tables, key=lambda x: -x.serial):
            if prev != curr and not curr.is_deleted:
                yield curr
                prev = curr

    def flush(self) -> None:
        # TODO invert control
        # TODO: background process flushing data
        table = SSTable[K, V](self._path, self._meta)
        table.flush(iter(self._mem_table))
        self._tables.append(table)
        self._mem_table = MemTable[K, V]()

    def _find(self, key: K) -> Optional[V]:
        # TODO: test correctness
        for table in sorted(self._tables, key=lambda x: -x.serial):
            node = table.find(key)
            if node and not node.is_deleted:
                return node.value
        return None
