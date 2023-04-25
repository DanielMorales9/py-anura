from typing import Any, Generic, Optional, TypeVar

from anura.btree import BinarySearchTree, Comparable

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


class LSMTree(Generic[K, V]):
    def __init__(self) -> None:
        self.__mem_table = MemTable[K, V]()

    def get(self, key: K) -> Optional[V]:
        return self.__mem_table[key]

    def put(self, key: K, value: V) -> None:
        self.__mem_table[key] = value

    def delete(self, key: K) -> None:
        del self.__mem_table[key]
