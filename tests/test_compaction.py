import copy
from threading import Thread

import pytest
from utils import setup_metadata

from anura.compaction import NaiveCompaction
from anura.concurrency import LockManager
from anura.lsm import LSMTree
from anura.model import MemNode

TEST_META = {
    "table_name": "dummy",
    "fields": {"key": {"type": "LONG"}, "value": {"type": "VARCHAR"}, "tombstone": {"type": "BOOL"}},
}

TEST_CASE = [
    (
        ((1, range(10), False), (2, range(10, 20), False)),
        ((i, f"v{i % 10 + 1}") for i in range(20)),
    ),
    (
        ((2, range(10), False), (1, range(10), False)),
        ((i, "v2") for i in range(10)),
    ),
    (
        ((1, range(20, 30), False), (2, range(10), False), (3, range(10, 20), False)),
        ((i, f"v{i % 10 + 1}") for i in range(30)),
    ),
    (
        ((1, range(20, 30), False), (2, range(10), True)),
        ((i, "v1") for i in range(20, 30)),
    ),
    (
        ((1, range(1_000), False), (2, range(1_000, 2_000), False)),
        ((i, f"v{i % 1_000 + 1}") for i in range(2_000)),
    ),
    (
        ((1, range(1, 20, 3), False), (2, range(0, 20, 7), False)),
        [(0, "v2"), (1, "v1"), (4, "v1"), (7, "v2"), (10, "v1"), (13, "v1"), (14, "v2"), (16, "v1"), (19, "v1")],
    ),
]


@pytest.mark.parametrize("data, expected", TEST_CASE)
def test_naive_compactor(tmp_path, data, expected):
    setup_metadata(
        tmp_path,
        metadata=TEST_META,
    )
    my_lsm = LSMTree(tmp_path)
    for serial, it, deleted in data:
        table = my_lsm.create_sstable(serial=serial)
        table.write((MemNode(el, f"v{serial}", deleted) for el in it), block_size=5)
        my_lsm.append_sstable(table)
    tables = copy.copy(my_lsm.sstables)

    NaiveCompaction(LockManager()).compact(my_lsm)

    assert all(not table.exists() for table in tables)
    assert list(my_lsm._tables[0]) == [MemNode(*v) for v in expected]


def target_fun(lsm: LSMTree, op: NaiveCompaction):
    op.compact(lsm)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            ((1, range(10), False), (2, range(10, 20), False)),
            ((i, f"v{i % 10 + 1}") for i in range(20)),
        ),
        (
            ((2, range(10), False), (1, range(10), False)),
            ((i, "v2") for i in range(10)),
        ),
        (
            ((1, range(20, 30), False), (2, range(10), False), (3, range(10, 20), False)),
            ((i, f"v{i % 10 + 1}") for i in range(30)),
        ),
        (
            ((1, range(20, 30), False), (2, range(10), True)),
            ((i, "v1") for i in range(20, 30)),
        ),
        (
            ((1, range(1_000), False), (2, range(1_000, 2_000), False)),
            ((i, f"v{i % 1_000 + 1}") for i in range(2_000)),
        ),
        (
            ((1, range(1, 20, 3), False), (2, range(0, 20, 7), False)),
            [(0, "v2"), (1, "v1"), (4, "v1"), (7, "v2"), (10, "v1"), (13, "v1"), (14, "v2"), (16, "v1"), (19, "v1")],
        ),
    ],
)
def test_multiple_compactions(tmp_path, data, expected):
    setup_metadata(
        tmp_path,
        metadata=TEST_META,
    )
    my_lsm = LSMTree(tmp_path)
    for serial, it, deleted in data:
        table = my_lsm.create_sstable(serial=serial)
        table.write((MemNode(el, f"v{serial}", deleted) for el in it), block_size=5)
        my_lsm.append_sstable(table)
    tables = copy.copy(my_lsm.sstables)

    compactor = NaiveCompaction(LockManager())
    t1 = Thread(
        target=target_fun,
        args=(
            my_lsm,
            compactor,
        ),
    )
    t2 = Thread(
        target=target_fun,
        args=(
            my_lsm,
            compactor,
        ),
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert all(not table.exists() for table in tables)
    assert list(my_lsm._tables[0]) == [MemNode(*v) for v in expected]
