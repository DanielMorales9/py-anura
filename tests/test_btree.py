import pytest as pytest

from anura.btree import BinaryTree
from anura.lsm import KeyValueEntry


def test_insert_find_btree(my_btree):
    my_btree.insert(KeyValueEntry(2, "two"))
    my_btree.insert(KeyValueEntry(1, "one"))
    my_btree.insert(KeyValueEntry(3, "three"))
    assert my_btree.find(KeyValueEntry(1)).value == "one"
    assert my_btree.find(KeyValueEntry(2)).value == "two"
    assert my_btree.find(KeyValueEntry(3)).value == "three"


@pytest.fixture
def my_btree():
    return BinaryTree[KeyValueEntry]()
