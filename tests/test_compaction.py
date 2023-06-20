import pytest
from utils import setup_metadata

from anura.compaction import NaiveCompaction
from anura.lsm import LSMTree
from anura.model import MemNode
from anura.sstable import SSTable


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
def test_naive_compactor(tmp_path, data, expected):
    metadata = setup_metadata(
        tmp_path,
        metadata={"fields": {"key": {"type": "LONG"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}}},
    )
    my_lsm = LSMTree(tmp_path, metadata)
    tables = []
    for serial, it, deleted in data:
        table = SSTable(tmp_path, metadata, serial=serial)
        table.write((MemNode(el, f"v{serial}", deleted) for el in it), block_size=5)
        tables.append(table)
    my_lsm._tables = tables

    NaiveCompaction().compact(my_lsm)

    assert all(not table.exists() for table in tables)
    assert list(my_lsm._tables[0]) == [MemNode(*v) for v in expected]
