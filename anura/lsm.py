from pathlib import Path
from typing import Generator, Generic, Iterator, List, Optional

from anura.btree import AVLTree
from anura.metadata import TableMetadata
from anura.model import K, MemNode, V
from anura.sstable import SSTable
from anura.utils import k_way_merge_sort


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


class LSMTree(Generic[K, V]):
    # TODO: background process compacting tables: merge tables

    def __init__(self, path: Path) -> None:
        self._path = path
        # TODO invert control
        self._meta = TableMetadata(self._path)
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
        # TODO KWayMergeCompactor - invert control
        #  https://www.datastax.com/blog/leveled-compaction-apache-cassandra
        #  https://en.wikipedia.org/wiki/Log-structured_merge-tree
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
