from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generator, Generic, Optional, Tuple, TypeVar


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


def inorder_traversal(node: Optional[Node[T]]) -> Generator[Node[T], None, None]:
    if not node:
        return
    stack = [node]
    while stack:
        if node.left:
            stack.append(node.left)
            node = node.left
        else:
            node = stack.pop()
            yield node
            if node.right:
                stack.append(node.right)
                node = node.right


class BinarySearchTree(Generic[T]):
    def __init__(self) -> None:
        self._root: Optional[Node[T]] = None
        self.size = 0

    def __iter__(self) -> Generator[Node[T], None, None]:
        yield from inorder_traversal(self._root)

    def __repr__(self) -> str:
        return repr([node.data for node in inorder_traversal(self._root)])

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
