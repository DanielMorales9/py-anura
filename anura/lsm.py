import struct
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Generic, List, Optional, Sequence, Tuple, TypeVar

from anura.btree import BinarySearchTree, Comparable
from anura.constants import BLOCK_SIZE, MetaConfig, MetaType
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


def decode(block: bytes, metadata: Tuple[MetaType, MetaType]) -> Generator[MemNode[K, V], None, None]:
    key_type, value_type = metadata
    i = 0
    while i < len(block):
        key, offset = unpack(block[i:], **MetaConfig[key_type])
        i += offset
        value, offset = unpack(block[i:], **MetaConfig[value_type])
        i += offset
        tombstone, offset = unpack(block[i:], **MetaConfig[MetaType.BOOL])
        i += offset
        yield MemNode(key, value, tombstone)


def encode(block: Sequence[MemNode[K, V]], metadata: Tuple[MetaType, MetaType]) -> Generator[bytes, None, None]:
    key_type, value_type = metadata
    for record in block:
        e_key = pack(record.key, **MetaConfig[key_type])
        e_value = pack(record.value, **MetaConfig[value_type])
        e_tombstone = pack(record.is_deleted, **MetaConfig[MetaType.BOOL])
        yield e_key + e_value + e_tombstone


def unpack(
    block: bytes,
    struct_symbol: Any,
    base_size: Any,
    is_container: Any = False,
    charset: Any = None,
    **kwargs: Any,
) -> Tuple[Any, int]:
    start, offset, size = 0, base_size, 1
    if is_container:
        size, offset = unpack(block[start:], **MetaConfig[MetaType.UNSIGNED_SHORT])
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


class SSTable(Generic[K, V]):
    def __init__(self, serial: Optional[int] = None):
        if serial:
            self._serial = serial
        else:
            self._serial = int(datetime.utcnow().timestamp())

    def flush(self, path: Path, table: MemTable[K, V], metadata: Tuple[MetaType, MetaType]) -> None:
        with open(path / f"{self._serial}.sst", "wb") as f:
            for block in chunk(table, BLOCK_SIZE):
                # index.append((block[0].key, offset))
                for record in encode(block, metadata):
                    f.write(record)

        # TODO do something with index


class LSMTree(Generic[K, V]):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._mem_table = MemTable[K, V]()
        self._tables: List[SSTable[K, V]] = []

    def get(self, key: K) -> Optional[V]:
        return self._mem_table[key]

    def put(self, key: K, value: V) -> None:
        self._mem_table[key] = value

    def delete(self, key: K) -> None:
        del self._mem_table[key]

    def flush(self) -> None:
        table = SSTable[K, V]()
        # TODO handle metadata differently
        table.flush(self._path, self._mem_table, metadata=(MetaType.VARCHAR, MetaType.VARCHAR))
        self._tables.append(table)
        self._mem_table = MemTable[K, V]()
