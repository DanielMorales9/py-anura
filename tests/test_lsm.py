import pytest as pytest

from anura.lsm import LSMTree, MemTable


@pytest.fixture
def my_lsm():
    return LSMTree()


@pytest.fixture
def my_mem_table():
    return MemTable()


def test_lsm(my_lsm):
    assert my_lsm.get("key") is None
    my_lsm.put("key", "value")
    assert my_lsm.get("key") == "value"
    my_lsm.delete("key")
    assert my_lsm.get("key") is None


def test_get_mem_table(my_mem_table):
    my_mem_table["key1"] = "value1"
    my_mem_table["key2"] = "value2"
    assert my_mem_table["key2"] == "value2"


def test_get_mem_table_is_bst(my_mem_table):
    for i in range(10):
        my_mem_table[i] = i
    assert repr(my_mem_table) == "[" + ", ".join(f"{i}:{i}" for i in range(10)) + "]"


def test_delete_mem_table(my_mem_table):
    my_mem_table["key1"] = "value1"
    my_mem_table["key2"] = "value2"
    del my_mem_table["key1"]
    assert my_mem_table["key1"] is None
