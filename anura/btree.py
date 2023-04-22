from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, List, Optional, Tuple, TypeVar


class Comparable(ABC):
    @abstractmethod
    def __eq__(self, other: object) -> Any:
        pass

    @abstractmethod
    def __lt__(self, other: object) -> Any:
        pass

    @abstractmethod
    def __gt__(self, other: object) -> Any:
        pass

    @abstractmethod
    def __le__(self, other: object) -> Any:
        pass

    @abstractmethod
    def __ge__(self, other: object) -> Any:
        pass


T = TypeVar("T", bound=Comparable)


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
