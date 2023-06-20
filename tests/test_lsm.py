from gzip import decompress
from itertools import zip_longest
from unittest.mock import patch

import pytest as pytest
from constants import TEST_META
from utils import setup_metadata

from anura.io import decode
from anura.lsm import LSMTree, MemTable
from anura.model import MemNode
from anura.sstable import SSTable


@pytest.fixture
def my_lsm(tmp_path):
    return LSMTree(tmp_path, setup_metadata(tmp_path, metadata=TEST_META))


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
    # fixture setup
    my_lsm.put("key", "value")

    # test
    with patch.object(SSTable, "write") as mock_method:
        my_lsm.flush()
        assert my_lsm._mem_table._btree.size == 0
        mock_method.assert_called()


TEST_DATA = [
    (
        [(i, i) for i in range(100)],
        [0, 50],
        {"fields": {"key": {"type": "LONG"}, "value": {"type": "LONG"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(f"key{i:02}", f"val{i:02d}") for i in range(100)],
        ["key00", "key50"],
        {"fields": {"key": {"type": "VARCHAR"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(i, f"val{i:02d}") for i in range(100)],
        [0, 50],
        {"fields": {"key": {"type": "LONG"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(float(i), f"val{i:02d}") for i in range(100)],
        [0, 50],
        {"fields": {"key": {"type": "DOUBLE"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(float(i), i) for i in range(100)],
        [0, 50],
        {"fields": {"key": {"type": "FLOAT"}, "value": {"type": "INT"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(i, float(i)) for i in range(100)],
        [0, 50],
        {"fields": {"key": {"type": "SHORT"}, "value": {"type": "DOUBLE"}, "tombstone": {"type": "BOOL"}}},
    ),
    (
        [(i, [i, i]) for i in range(100)],
        [0, 50],
        {
            "fields": {
                "key": {"type": "LONG"},
                "value": {"type": "ARRAY", "options": {"inner_type": {"type": "INT"}}},
                "tombstone": {"type": "BOOL"},
            }
        },
    ),
    (
        [(i, {"a": i, "b": f"val{i:02d}"}) for i in range(100)],
        [0, 50],
        {
            "fields": {
                "key": {"type": "LONG"},
                "value": {"type": "STRUCT", "options": {"inner": {"a": {"type": "INT"}, "b": {"type": "VARCHAR"}}}},
                "tombstone": {"type": "BOOL"},
            }
        },
    ),
    (
        [(i, {"a": [0] * i, "b": f"val{i:02d}"}) for i in range(100)],
        [0, 50],
        {
            "fields": {
                "key": {"type": "INT"},
                "value": {
                    "type": "STRUCT",
                    "options": {
                        "inner": {
                            "a": {"type": "ARRAY", "options": {"inner_type": {"type": "INT"}}},
                            "b": {"type": "VARCHAR"},
                        }
                    },
                },
                "tombstone": {"type": "BOOL"},
            }
        },
    ),
    (
        [(i, f"val{i:02d}") for i in range(100)],
        [0, 50],
        {
            "fields": {
                "key": {"type": "INT"},
                "value": {"type": "VARCHAR", "options": {"charset": "ascii"}},
                "tombstone": {"type": "BOOL"},
            }
        },
    ),
    (
        [(i, f"val{i:02d}") for i in range(100)],
        [0, 50],
        {
            "fields": {
                "key": {"type": "INT"},
                "value": {"type": "VARCHAR", "options": {"charset": "ascii", "length_type": {"type": "UNSIGNED_INT"}}},
                "tombstone": {"type": "BOOL"},
            }
        },
    ),
]


@pytest.mark.parametrize(
    "data, index, meta",
    TEST_DATA,
)
def test_flush_table(tmp_path, data, index, meta):
    # fixture setup
    serial = 101
    metadata = setup_metadata(tmp_path, meta)
    table = SSTable(tmp_path, metadata, serial=serial)
    # test
    table.write(MemNode(k, v) for k, v in data)
    with open(tmp_path / f"{serial}.sst", "rb") as f:
        decoded_block = []
        for a, b in zip_longest(table._index, table._index[1:]):
            f.seek(a[1])
            size = b[1] - a[1] if b else -1
            decoded_block.extend(MemNode(*el) for el in decode(decompress(f.read(size)), list(metadata)))
    assert len(decoded_block) == 100
    assert decoded_block == sorted([MemNode(k, v) for k, v in data], key=lambda x: x.key)
    assert all(not record.is_deleted for record in decoded_block)

    with open(tmp_path / f"{serial}.spx", "rb") as f:
        decoded_index = list(decode(f.read(), table._index_meta))
        assert [x[0] for x in decoded_index] == index


@pytest.mark.parametrize(
    "data, index, meta",
    TEST_DATA,
)
def test_seq_scan(tmp_path, data, index, meta):
    metadata = setup_metadata(tmp_path, meta)
    table = SSTable(tmp_path, metadata)

    table.write(MemNode(k, v) for k, v in data)

    assert [(rec.key, rec.value) for rec in table.seq_scan()] == data


@pytest.mark.parametrize(
    "data, index, meta",
    TEST_DATA,
)
def test_iter(tmp_path, data, index, meta):
    serial = 101
    metadata = setup_metadata(tmp_path, meta)
    table = SSTable(tmp_path, metadata, serial=serial)

    table.write(MemNode(k, v) for k, v in data)

    assert [(rec.key, rec.value) for rec in iter(table)] == data


@pytest.mark.parametrize(
    "key, data",
    [
        (0, []),
        (0, [(1,)]),
        (7, [(1,), (4,), (5,)]),
    ],
)
def test_search_empty_block(tmp_path, key, data):
    metadata = setup_metadata(tmp_path)
    table = SSTable(tmp_path, metadata, serial=101)
    assert table._search(key, data) is None


def test_find_lsm(my_lsm, tmp_path):
    my_lsm._tables = [SSTable(tmp_path, setup_metadata(tmp_path), serial=101)]
    with patch.object(SSTable, "find") as mock_method:
        assert my_lsm._find("key") is None
        mock_method.assert_called_once_with("key")


@pytest.mark.parametrize(
    "key, value",
    [
        (5, 5),
        (0, 0),
        (49, 49),
        (51, 51),
        (66, 66),
        (99, 99),
        (-1, None),
        (100, None),
    ],
)
def test_find_table(tmp_path, key, value):
    # fixture setup
    table = SSTable(tmp_path, setup_metadata(tmp_path, TEST_META), serial=101)
    table.write(MemNode(i, i) for i in range(100))

    # test
    if value is not None:
        assert table.find(key) == MemNode(key, value)
    else:
        assert table.find(key) is None


def test_lsm_get_or_find_in_disk(tmp_path):
    # fixture setup
    lsm = LSMTree(tmp_path, setup_metadata(tmp_path, TEST_META))
    lsm._tables = [SSTable(tmp_path, setup_metadata(tmp_path, TEST_META), serial=101)]
    # test
    with patch.object(SSTable, "find") as mock_method:
        assert lsm.get("key") is None
        mock_method.assert_called_once_with("key")
