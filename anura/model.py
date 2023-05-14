from typing import Any, Generic, Iterator, Optional, TypeVar

from anura.btree import Comparable

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
