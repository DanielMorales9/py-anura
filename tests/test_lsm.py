from gzip import decompress
from itertools import product, zip_longest
from unittest.mock import patch

import pytest as pytest

from anura.constants import META_CONFIG, ComplexType, PrimitiveType
from anura.io import decode
from anura.lsm import LSMTree, MemTable, Metadata, SSTable
from anura.model import MemNode

TEST_META = (PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.BOOL)


@pytest.fixture
def my_lsm(tmp_path):
    meta_data_path = tmp_path / "meta.data"
    meta_data_path.write_text("key=LONG,value=VARCHAR,tombstone=BOOL")
    return LSMTree(tmp_path)


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
    with patch.object(SSTable, "flush") as mock_method:
        my_lsm.flush()
        assert my_lsm._mem_table._btree.size == 0
        mock_method.assert_called()


def setup_metadata(tmp_path, meta):
    meta_data_path = tmp_path / "meta.data"
    value_type = f"{meta[1][1]}[]" if isinstance(meta[1], tuple) else meta[1]

    meta_data_path.write_text(f"key={meta[0]},value={value_type},tombstone={meta[2]}")
    metadata = Metadata(tmp_path)
    return metadata


TEST_DATA = [
    (
        [(i, i) for i in range(100)],
        [0, 50],
        (PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.BOOL),
    ),
    (
        [(f"key{i:02}", f"val{i:02d}") for i in range(100)],
        ["key00", "key50"],
        (PrimitiveType.VARCHAR, PrimitiveType.VARCHAR, PrimitiveType.BOOL),
    ),
    (
        [(i, f"val{i:02d}") for i in range(100)],
        [0, 50],
        (PrimitiveType.LONG, PrimitiveType.VARCHAR, PrimitiveType.BOOL),
    ),
    (
        [(float(i), f"val{i:02d}") for i in range(100)],
        [0, 50],
        (PrimitiveType.DOUBLE, PrimitiveType.VARCHAR, PrimitiveType.BOOL),
    ),
    (
        [(float(i), i) for i in range(100)],
        [0, 50],
        (PrimitiveType.FLOAT, PrimitiveType.INT, PrimitiveType.BOOL),
    ),
    (
        [(i, float(i)) for i in range(100)],
        [0, 50],
        (PrimitiveType.SHORT, PrimitiveType.DOUBLE, PrimitiveType.BOOL),
    ),
    (
        # TODO simplify encoding for primitives
        [(i, [i, i]) for i in range(100)],
        [0, 50],
        (PrimitiveType.LONG, (ComplexType.ARRAY, PrimitiveType.INT), PrimitiveType.BOOL),
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
    table.flush(MemNode(k, v) for k, v in data)
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

    table.flush(MemNode(k, v) for k, v in data)

    assert [(rec.key, rec.value) for rec in table.seq_scan()] == data


@pytest.mark.parametrize(
    "data, index, meta",
    TEST_DATA,
)
def test_iter(tmp_path, data, index, meta):
    serial = 101
    metadata = setup_metadata(tmp_path, meta)
    table = SSTable(tmp_path, metadata, serial=serial)

    table.flush(MemNode(k, v) for k, v in data)

    assert [(rec.key, rec.value) for rec in iter(table)] == data


@pytest.mark.parametrize(
    "data, expected",
    [
        pytest.param(
            ((1, range(10), False), (2, range(10, 20), False)), ((i, f"v{i%10+1}") for i in range(20)), id="1"
        ),
        pytest.param(((2, range(10), False), (1, range(10), False)), ((i, "v2") for i in range(10)), id="2"),
        pytest.param(
            ((1, range(20, 30), False), (2, range(10), False), (3, range(10, 20), False)),
            ((i, f"v{i%10+1}") for i in range(30)),
            id="3",
        ),
        pytest.param(((1, range(20, 30), False), (2, range(10), True)), ((i, "v1") for i in range(20, 30)), id="4"),
        pytest.param(
            ((1, range(1_000), False), (2, range(1_000, 2_000), False)),
            ((i, f"v{i % 1_000 + 1}") for i in range(2_000)),
            id="4",
        ),
        pytest.param(
            ((1, range(1, 20, 3), False), (2, range(0, 20, 7), False)),
            [(0, "v2"), (1, "v1"), (4, "v1"), (7, "v2"), (10, "v1"), (13, "v1"), (14, "v2"), (16, "v1"), (19, "v1")],
            id="5",
        ),
    ],
)
def test_merge_tables(tmp_path, my_lsm, data, expected):
    meta = (PrimitiveType.LONG, PrimitiveType.VARCHAR, PrimitiveType.BOOL)
    metadata = setup_metadata(tmp_path, meta)

    tables = []
    for serial, it, deleted in data:
        table = SSTable(tmp_path, metadata, serial=serial)
        table.flush((MemNode(el, f"v{serial}", deleted) for el in it), block_size=5)
        tables.append(table)

    my_lsm._tables = tables
    assert list(my_lsm.merge()) == [MemNode(*v) for v in expected]


@pytest.mark.parametrize(
    "key, data",
    [
        (0, []),
        (0, [(1,)]),
        (7, [(1,), (4,), (5,)]),
    ],
)
def test_search_empty_block(tmp_path, key, data):
    meta = (PrimitiveType.LONG, PrimitiveType.VARCHAR, PrimitiveType.BOOL)
    metadata = setup_metadata(tmp_path, meta)
    table = SSTable(tmp_path, metadata, serial=101)
    assert table._search(key, data) is None


def test_find_lsm(my_lsm, tmp_path):
    # fixture setup
    meta = (PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.BOOL)
    my_lsm._tables = [SSTable(tmp_path, setup_metadata(tmp_path, meta), serial=101)]
    # test
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
    table.flush(MemNode(i, i) for i in range(100))

    # test
    if value is not None:
        assert table.find(key) == MemNode(key, value)
    else:
        assert table.find(key) is None


def test_lsm_get_or_find_in_disk(my_lsm, tmp_path):
    # fixture setup
    my_lsm._tables = [SSTable(tmp_path, setup_metadata(tmp_path, TEST_META), serial=101)]
    # test
    with patch.object(SSTable, "find") as mock_method:
        assert my_lsm.get("key") is None
        mock_method.assert_called_once_with("key")


@pytest.mark.parametrize("meta", product(PrimitiveType, repeat=2))
def test_meta(tmp_path, meta):
    metadata = setup_metadata(tmp_path, (*meta, PrimitiveType.BOOL))
    assert metadata.key_type["struct_symbol"] == META_CONFIG[meta[0]]["struct_symbol"]
    assert metadata.value_type["struct_symbol"] == META_CONFIG[meta[1]]["struct_symbol"]


def test_meta_array(tmp_path):
    meta_data_path = tmp_path / "meta.data"
    meta_data_path.write_text("key=VARCHAR[],value=VARCHAR[],tombstone=BOOL")
    metadata = Metadata(tmp_path)
    # TODO switch to dataclasses
    assert isinstance(metadata._meta["key"], dict)
    assert isinstance(metadata._meta["value"], dict)
    assert isinstance(metadata._meta["tombstone"], dict)
