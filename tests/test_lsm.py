from unittest.mock import patch

import pytest as pytest

from anura.constants import MetaType
from anura.lsm import LSMTree, MemNode, MemTable, SSTable, decode


@pytest.fixture
def my_lsm(tmp_path):
    return LSMTree(tmp_path / "data" / "base")


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


def test_flush_lsm(my_lsm):
    my_lsm.put("key", "value")

    with patch.object(SSTable, "flush") as mock_method:
        my_lsm.flush()
        assert my_lsm._mem_table._btree.size == 0
        mock_method.assert_called()


def test_find_lsm(my_lsm, tmp_path):
    metadata = (MetaType.LONG, MetaType.LONG, MetaType.BOOL)
    my_lsm._tables = [SSTable(tmp_path, metadata, serial=101)]
    with patch.object(SSTable, "find") as mock_method:
        assert my_lsm._find("key") is None
        mock_method.assert_called_once_with("key")


@pytest.mark.parametrize(
    "data, index, metadata",
    [
        ([(i, i) for i in range(100)], [[0, 0], [50, (4 * 2 + 1) * 50]], (MetaType.LONG, MetaType.LONG, MetaType.BOOL)),
        (
            [(f"key{i:02}", f"val{i:02d}") for i in range(100)],
            [["key00", 0], ["key50", ((5 + 2) * 2 + 1) * 50]],
            (MetaType.VARCHAR, MetaType.VARCHAR, MetaType.BOOL),
        ),
        (
            [(i, f"val{i:02d}") for i in range(100)],
            [[0, 0], [50, (4 + (5 + 2) + 1) * 50]],
            (MetaType.LONG, MetaType.VARCHAR, MetaType.BOOL),
        ),
        (
            [(float(i), f"val{i:02d}") for i in range(100)],
            [[0.0, 0], [50.0, (8 + (5 + 2) + 1) * 50]],
            (MetaType.DOUBLE, MetaType.VARCHAR, MetaType.BOOL),
        ),
    ],
)
def test_flush_table(tmp_path, my_mem_table, data, index, metadata):
    serial = 101
    table = SSTable(tmp_path, metadata, serial=serial)
    for k, v in data:
        my_mem_table[k] = v

    table.flush(my_mem_table)
    with open(tmp_path / f"{serial}.sst", "rb") as f:
        decoded_block = [MemNode(*el) for el in decode(f.read(), metadata)]
        assert len(decoded_block) == 100
        assert decoded_block == sorted([MemNode(k, v) for k, v in data], key=lambda x: x.key)
        assert all(not record.is_deleted for record in decoded_block)

    with open(tmp_path / f"{serial}.spx", "rb") as f:
        decoded_index = list(decode(f.read(), table._index_meta))
        assert decoded_index == index
