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


class BinarySearchTree(Generic[T]):
    def __init__(self) -> None:
        self._root: Optional[Node] = None
        self.size = 0

    def __repr__(self) -> str:
        return repr(rec_traverse(self._root))

    @staticmethod
    def search(root: Optional[Node], obj: T) -> Tuple[Optional[Node[T]], Optional[Node[T]]]:
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
        node, _ = self.search(self._root, obj)
        if node:
            return node.data
        return None

    def insert(self, obj: T) -> None:
        if not self._root:
            self._root = Node(obj)
            self.size += 1
            self._root.data = obj
            return

        node, parent = self.search(self._root, obj)
        if node:
            node.data = obj

        new_node = Node(obj)
        if parent:
            if obj < parent.data:
                parent.left = new_node
            else:
                parent.right = new_node

        # TODO: re-balance

        self.size += 1
        return None

    def visual_repr(self) -> str:
        return "\n" + rec_visualise(self._root) + "\n"


def rec_visualise(node: Optional[Node]) -> str:
    if not node:
        return ""
    lines = []
    if node.right:
        found = False
        for line in rec_visualise(node.right).split("\n"):
            if line[0] != " ":
                found = True
                line = " ┌─" + line
            elif found:
                line = " | " + line
            else:
                line = "   " + line
            lines.append(line)
    lines.append(repr(node.data))
    if node.left:
        found = False
        for line in rec_visualise(node.left).split("\n"):
            if line[0] != " ":
                found = True
                line = " └─" + line
            elif found:
                line = "   " + line
            else:
                line = " | " + line
            lines.append(line)
    return "\n".join(lines)


def rec_traverse(node: Optional[Node]) -> List[T]:
    res: List[Any] = []
    if not node:
        return res
    if node.left:
        res += rec_traverse(node.left)
    res.append(node.data)
    if node.right:
        res += rec_traverse(node.right)
    return res
