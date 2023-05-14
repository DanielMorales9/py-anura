import typing
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
    balance: int = 0


@typing.no_type_check
def inorder_traversal(node: Optional[Node[T]]) -> Generator[Node[T], None, None]:
    if not node:
        return
    stack = []
    while True:
        if node:
            stack.append(node)
            node = node.left
        elif stack:
            node = stack.pop()
            yield node
            node = node.right
        else:
            break


@typing.no_type_check
def rotate_left(node: Node) -> Node:
    x = node.right
    node.right = x.left
    if node.right:
        node.right.parent = node

    x.parent = node.parent
    if x.parent:
        if x.parent.left == node:
            x.parent.left = x
        else:
            x.parent.right = x

    node.parent = x
    x.left = node

    node.balance += 1
    if x.balance < 0:
        node.balance -= x.balance

    x.balance += 1
    if node.balance > 0:
        x.balance += node.balance
    return x


@typing.no_type_check
def rotate_right(node: Node) -> Node:
    x = node.left
    node.left = x.right
    if node.left:
        node.left.parent = node

    x.parent = node.parent
    if x.parent:
        if x.parent.left == node:
            x.parent.left = x
        else:
            x.parent.right = x

    node.parent = x
    x.right = node

    node.balance -= 1
    if x.balance > 0:
        node.balance -= x.balance

    x.balance -= 1
    if node.balance < 0:
        x.balance += node.balance
    return x


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


class AVLTree(Generic[T]):
    def __init__(self) -> None:
        self._root: Optional[Node[T]] = None
        self.size = 0

    def __iter__(self) -> Generator[Node[T], None, None]:
        yield from inorder_traversal(self._root)

    def __repr__(self) -> str:
        return repr([node.data for node in inorder_traversal(self._root)])

    def find(self, obj: T) -> Optional[T]:
        node, _ = search(self._root, obj)
        if node:
            return node.data
        return None

    @typing.no_type_check
    def insert(self, obj: T) -> None:
        if not self._root:
            self._root = Node(obj)
            self.size += 1
            return

        node, parent = search(self._root, obj)
        if node:
            # update node with new data
            node.data = obj
        else:
            # insert node with data
            new_node = Node(obj, parent)
            if obj < parent.data:
                parent.left = new_node
            else:
                parent.right = new_node

            self.balance(parent, obj)

        self.size += 1
        return None

    @typing.no_type_check
    def balance(self, parent: Optional[Node[T]], obj: T) -> None:
        while parent:
            if parent.data < obj:
                parent.balance -= 1
            else:
                parent.balance += 1

            if parent.balance in (-1, 1):
                parent = parent.parent
            elif parent.balance < -1:
                if parent.right.balance == 1:
                    rotate_right(parent.right)
                new_root = rotate_left(parent)
                if parent == self._root:
                    self._root = new_root
                parent = None
            elif parent.balance > 1:
                if parent.left.balance == -1:
                    rotate_left(parent.left)
                new_root = rotate_right(parent)
                if parent == self._root:
                    self._root = new_root
                parent = None
            else:
                parent = None

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
