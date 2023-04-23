import pytest as pytest

from anura.btree import BinarySearchTree
from anura.lsm import KeyValueEntry


@pytest.fixture
def my_btree():
    return BinarySearchTree[KeyValueEntry]()


def test_insert_find_btree(my_btree):
    my_btree.insert(KeyValueEntry(2, "two"))
    my_btree.insert(KeyValueEntry(1, "one"))
    my_btree.insert(KeyValueEntry(3, "three"))
    assert my_btree.find(KeyValueEntry(1)).value == "one"
    assert my_btree.find(KeyValueEntry(2)).value == "two"
    assert my_btree.find(KeyValueEntry(3)).value == "three"


VISUAL_TREE_REPR_213 = """
 ┌─3:three
2:two
 └─1:one
"""

VISUAL_TREE_REPR_321 = """
3:three
 └─2:two
    └─1:one
"""

VISUAL_TREE_REPR_123 = """
    ┌─3:three
 ┌─2:two
1:one
"""


@pytest.mark.parametrize(
    "entries, visual_representation",
    [
        ([(2, "two"), (1, "one"), (3, "three")], VISUAL_TREE_REPR_213),
        ([(3, "three"), (2, "two"), (1, "one")], VISUAL_TREE_REPR_321),
        ([(1, "one"), (2, "two"), (3, "three")], VISUAL_TREE_REPR_123),
    ],
)
def test_visual_representation(my_btree, entries, visual_representation):
    for k, v in entries:
        my_btree.insert(KeyValueEntry(k, v))
    assert my_btree.visual_repr() == visual_representation