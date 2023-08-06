import pytest
from utils import setup_metadata

from anura.concurrent.manager import LockManager
from anura.flusher import TableFlusher
from anura.lsm import LSMTree, MemTable
from anura.model import MemNode


@pytest.mark.parametrize(
    "data",
    [
        pytest.param([(i, f"v{i%10+1}") for i in range(20)], id="1"),
        pytest.param([(i, "v2") for i in range(10)], id="2"),
        pytest.param(
            [(i, f"v{i%10+1}") for i in range(30)],
            id="3",
        ),
        pytest.param([(i, "v1") for i in range(20, 30)], id="4"),
        pytest.param(
            [(i, f"v{i % 1_000 + 1}") for i in range(2_000)],
            id="4",
        ),
        pytest.param(
            [(0, "v2"), (1, "v1"), (4, "v1"), (7, "v2"), (10, "v1"), (13, "v1"), (14, "v2"), (16, "v1"), (19, "v1")],
            id="5",
        ),
    ],
)
def test_flusher(tmp_path, data):
    setup_metadata(
        tmp_path,
        metadata={
            "table_name": "dummy",
            "fields": {"key": {"type": "LONG"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}},
        },
    )
    my_lsm = LSMTree(tmp_path)
    my_lsm._mem_table = cache = MemTable()

    for k, v in data:
        cache[k] = v

    TableFlusher(LockManager()).flush(my_lsm, 1)

    assert list(my_lsm._mem_table) == []
    assert list(my_lsm.sstables[0]) == [MemNode(*v) for v in data]
