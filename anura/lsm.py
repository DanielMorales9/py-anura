import struct
from bisect import bisect
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Generic, Iterator, List, Optional, Sequence, Tuple, TypeVar

from anura.btree import BinarySearchTree, Comparable
from anura.constants import BLOCK_SIZE, SPARSE_IDX_EXT, SSTABLE_EXT, MetaConfig, MetaType
from anura.utils import chunk

K = TypeVar("K")
V = TypeVar("V")


class KeyValueEntry(Comparable, Generic[K, V]):
    def __init__(self, key: K, value: Optional[V] = None):
        self._key = key
        self._value = value

    @property
    def key(self) -> K:
        return self._key

    @property
    def value(self) -> Optional[V]:
        return self._value

    def __eq__(self, other: object) -> Any:
        if not isinstance(other, KeyValueEntry):
            return False
        return self.key == other.key

    def __lt__(self, other: object) -> Any:
        if not isinstance(other, KeyValueEntry):
            return False
        return self.key < other.key

    def __gt__(self, other: object) -> Any:
        if not isinstance(other, KeyValueEntry):
            return False
        return self.key > other.key

    def __le__(self, other: object) -> Any:
        if not isinstance(other, KeyValueEntry):
            return False
        return self.key <= other.key

    def __ge__(self, other: object) -> Any:
        if not isinstance(other, KeyValueEntry):
            return False
        return self.key >= other.key

    def __repr__(self) -> str:
        return f"{self._key}:{self._value}"


class MemNode(KeyValueEntry[K, V]):
    def __init__(self, key: K, value: Optional[V] = None, tombstone: bool = False):
        super().__init__(key, value)
        self._tombstone: bool = tombstone

    @property
    def is_deleted(self) -> bool:
        return self._tombstone

    @is_deleted.setter
    def is_deleted(self, value: bool) -> None:
        self._tombstone = value

    def __iter__(self) -> Iterator[Any]:
        return iter((self.key, self.value, self.is_deleted))


class MemTable(Generic[K, V]):
    def __init__(self) -> None:
        self._btree = BinarySearchTree[MemNode[K, V]]()

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


def decode(block: bytes, metadata: Sequence[MetaType]) -> Sequence[Any]:
    i = 0
    res = []
    while i < len(block):
        record = [None] * len(metadata)
        for j, metatype in enumerate(metadata):
            el, offset = unpack(block[i:], **MetaConfig[metatype])
            record[j] = el
            i += offset
        res.append(record)
    return res


def encode(record: Sequence[Any], metadata: Sequence[MetaType]) -> bytes:
    acc = b""
    for el, meta in zip(record, metadata):
        acc += pack(el, **MetaConfig[meta])
    return acc


def unpack(
    block: bytes,
    struct_symbol: Any,
    base_size: Any,
    is_container: Any = False,
    charset: Any = None,
    length_type: Any = None,
    **kwargs: Any,
) -> Tuple[Any, int]:
    start, offset, size = 0, base_size, 1
    if is_container:
        size, offset = unpack(block[start:], **MetaConfig[length_type])
        start, offset = start + offset, start + offset + base_size * size
    res = struct.unpack(f">{size}{struct_symbol}", block[start:offset])[0]
    if charset:
        res = res.decode(charset)
    return res, offset


def pack(
    field: Any,
    struct_symbol: Any,
    is_container: Any = False,
    charset: Any = None,
    length_type: Any = None,
    **kwargs: Any,
) -> bytes:
    size = 1
    args = []
    length_symbol = ""

    if length_type:
        length_symbol = MetaConfig[length_type]["struct_symbol"]  # type: ignore[assignment]

    if is_container:
        size = len(field)
        args.append(size)

    if charset:
        field = field.encode(charset)

    args.append(field)
    res = struct.pack(f">{length_symbol}{size}{struct_symbol}", *args)
    return res


WRITE_MODE = "wb"


class SSTable(Generic[K, V]):
    _offset_meta = MetaType.LONG

    def __init__(self, path: Path, metadata: Sequence[MetaType], serial: Optional[int] = None):
        self._index: List[Tuple[K, int]] = []
        self._metadata = metadata
        self._index_meta = (metadata[0], self._offset_meta)
        self._serial = serial or int(datetime.utcnow().timestamp())
        self._table_path = path / f"{self._serial}.{SSTABLE_EXT}"
        self._index_path = path / f"{self._serial}.{SPARSE_IDX_EXT}"

    def flush(self, table: MemTable[K, V]) -> None:
        offset = 0
        with open(self._table_path, "wb") as f:
            for block in chunk(table, BLOCK_SIZE):
                self._index.append((block[0].key, offset))
                # TODO compress records
                for record in block:
                    e_record = encode(record, self._metadata)
                    f.write(e_record)
                    offset += len(e_record)

        self._flush_index()

    def _flush_index(self) -> None:
        with open(self._index_path, "wb") as f:
            for el in self._index:
                f.write(encode(el, self._index_meta))

    def find(self, key: K) -> Optional[MemNode[K, V]]:
        # TODO consider using mmap
        i = bisect(self._index, key, key=lambda x: x[0])  # type: ignore[call-overload]
        if i == 0:
            return None

        start = self._index[i - 1][1]
        length = -1
        if i < len(self._index):
            end = self._index[i][1]
            length = end - start

        with open(self._table_path, "rb") as f:
            f.seek(start)
            raw = f.read(length)
            # TODO: decompress
            block = decode(raw, self._metadata)
            j = bisect(block, key, key=lambda x: x[0]) - 1  # type: ignore[call-overload]
            if block[j][0] == key:
                return MemNode[K, V](*block[j])

        return None


class MetaData:
    def __init__(self, path: Path):
        self._path = path / "meta.data"
        # TODO parsing of complex metadata like: struct, array, fixed-length char, varchar...
        with open(self._path) as f:
            self._meta = f.read().split(",")

    def __iter__(self) -> Iterator[Any]:
        return iter(self._meta)


class LSMTree(Generic[K, V]):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._meta = MetaData(self._path)
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

    def flush(self) -> None:
        table = SSTable[K, V](self._path, list(self._meta))
        table.flush(self._mem_table)
        self._tables.append(table)
        self._mem_table = MemTable[K, V]()

    def _find(self, key: K) -> Optional[V]:
        for table in self._tables:
            node = table.find(key)
            if node and not node.is_deleted:
                return node.value
        return None
