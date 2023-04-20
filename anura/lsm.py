from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class KeyValueEntry(ABC, Generic[K, V]):
    @property
    @abstractmethod
    def key(self) -> K:
        pass

    @property
    @abstractmethod
    def value(self) -> Optional[V]:
        pass

    @property
    @abstractmethod
    def meta(self) -> Any:
        pass

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, KeyValueEntry):
            return False
        return id(self.key) == id(other.key)

    def __lt__(self, other: "KeyValueEntry") -> bool:
        return id(self.key) < id(other.key)


T = TypeVar("T", bound=KeyValueEntry)


@dataclass
class Node(Generic[T]):
    data: T
    parent: Optional["Node"] = None
    left: Optional["Node"] = None
    right: Optional["Node"] = None


class BinaryTree(Generic[T]):
    def __init__(self) -> None:
        self._root: Optional[Node] = None
        self._size = 0

    def traverse(self, node: Optional[Node]) -> List[T]:
        res: List[Any] = []
        if not node:
            return res
        if node.left:
            res += self.traverse(node.left)
        res.append(node.data)
        if node.right:
            res += self.traverse(node.right)
        return res

    def __repr__(self) -> str:
        return repr(self.traverse(self._root))

    @staticmethod
    def bst_with_parent(root: Optional[Node], obj: T) -> Tuple[Optional[Node[T]], Optional[Node[T]]]:
        node = root
        parent: Optional[Node] = None

        while node:
            parent = node
            if obj == node.data:
                return node, parent
            elif obj < node.data:
                node = node.left
            else:
                node = node.right
        return None, parent

    def find(self, obj: T) -> Optional[T]:
        node, _ = self.bst_with_parent(self._root, obj)
        if node:
            return node.data
        return None

    def insert(self, obj: T) -> None:
        if not self._root:
            self._root = Node(obj)
            self._size += 1
            self._root.data = obj
            return

        node, parent = self.bst_with_parent(self._root, obj)
        if node:
            node.data = obj

        new_node = Node(obj)
        if parent:
            if obj < parent.data:
                parent.left = new_node
            else:
                parent.right = new_node

        # TODO: re-balance

        self._size += 1
        return None


class MemNode(KeyValueEntry[K, V]):
    def __init__(self, key: K, value: Optional[V] = None, thumb_stone: bool = False):
        self._key: K = key
        self._value: Optional[V] = value
        self._meta: Dict[str, Any] = {"thumb_stone": thumb_stone}

    @property
    def key(self) -> K:
        return self._key

    @property
    def value(self) -> Optional[V]:
        return self._value

    @property
    def meta(self) -> Dict[str, Any]:
        return self._meta

    def __repr__(self) -> str:
        return f"{self._key}:{self._value}"


class MemTable(Generic[K, V]):
    def __init__(self) -> None:
        self._btree = BinaryTree[MemNode[K, V]]()

    def __getitem__(self, key: K) -> Optional[V]:
        if (data := self._btree.find(MemNode[K, V](key))) and not data.meta["thumb_stone"]:
            return data.value
        return None

    def __setitem__(self, key: K, value: V) -> None:
        self._btree.insert(MemNode[K, V](key, value))

    def __delitem__(self, key: K) -> None:
        if node := self._btree.find(MemNode[K, V](key)):
            node.meta["thumb_stone"] = True

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
