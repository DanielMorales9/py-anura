from pathlib import Path
from typing import Any, Generator, Generic, List, Optional

from anura.btree import AVLTree
from anura.metadata import TableMetadata, parse_metadata
from anura.model import K, MemNode, V
from anura.sstable import SSTable


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
    def __init__(self, path: Path) -> None:
        self._path = path
        self._metadata = TableMetadata(**parse_metadata(path / "metadata.json"))
        self._mem_table = MemTable[K, V]()
        self._tables: List[SSTable[K, V]] = []

    @property
    def sstables(self) -> List[SSTable[K, V]]:
        return self._tables

    @property
    def metadata(self) -> TableMetadata:
        return self._metadata

    @property
    def mem_table(self) -> MemTable[K, V]:
        return self._mem_table

    def delete_sstables(self) -> None:
        while self._tables:
            table = self._tables.pop()
            table.delete()

    def append_sstable(self, table: SSTable) -> None:
        self._tables.append(table)

    def get(self, key: K) -> Optional[V]:
        value = self._mem_table[key]
        if not value:
            return self._find(key)
        return value

    def put(self, key: K, value: V) -> None:
        self._mem_table[key] = value

    def delete(self, key: K) -> None:
        del self._mem_table[key]

    def reset_cache(self) -> None:
        self._mem_table = MemTable[K, V]()

    def create_sstable(self, **kwargs: Any) -> SSTable[K, V]:
        return SSTable[K, V](self._path, self._metadata, **kwargs)

    def _find(self, key: K) -> Optional[V]:
        # TODO: test correctness
        for table in sorted(self._tables, key=lambda x: -x.serial):
            node = table.find(key)
            if node and not node.is_deleted:
                return node.value
        return None
