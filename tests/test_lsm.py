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


@pytest.mark.parametrize(
    "data, metadata",
    [
        ([(i, i) for i in range(100)], (MetaType.LONG, MetaType.LONG)),
        ([(f"key{i}", f"value{i}") for i in range(100)], (MetaType.VARCHAR, MetaType.VARCHAR)),
        ([(i, f"value{i}") for i in range(100)], (MetaType.LONG, MetaType.VARCHAR)),
        ([(float(i), f"value{i}") for i in range(100)], (MetaType.DOUBLE, MetaType.VARCHAR)),
    ],
)
def test_flush_sstable(tmp_path, data, metadata):
    serial = 101
    table = SSTable(serial=serial)
    mem = MemTable()
    for k, v in data:
        mem[k] = v

    table.flush(tmp_path, mem, metadata=metadata)
    with open(tmp_path / f"{serial}.sst", "rb") as f:
        raw = f.read()
        decoded_block = list(decode(raw, metadata))
        assert len(decoded_block) == 100
        assert decoded_block == sorted([MemNode(k, v) for k, v in data], key=lambda x: x.key)
