import abc
from typing import Iterator, List

from anura.algorithms import k_way_merge_sort
from anura.concurrency import LockManager, LockMode
from anura.lsm import LSMTree
from anura.model import MemNode
from anura.sstable import SSTable


class ICompaction(abc.ABC):
    @abc.abstractmethod
    def compact(self, lsm: LSMTree) -> None:
        pass


def gen_sort_uniq(tables: List[SSTable]) -> Iterator[MemNode]:
    prev = None
    for curr in k_way_merge_sort(tables, key=lambda x: -x.serial):
        if prev != curr and not curr.is_deleted:
            yield curr
            prev = curr


# TODO https://en.wikipedia.org/wiki/Log-structured_merge-tree
#  https://www.datastax.com/blog/leveled-compaction-apache-cassandra
class NaiveCompaction(ICompaction):
    def __init__(self, lock_manager: LockManager) -> None:
        self._mgr = lock_manager

    def compact(self, lsm: "LSMTree") -> None:
        lock_id = hash(lsm.metadata.table_name)
        with self._mgr.lock(lock_id, LockMode.EXCLUSIVE):
            if len(lsm.sstables) > 1:
                table = lsm.create_sstable()
                table.write(gen_sort_uniq(lsm.sstables))
                lsm.delete_sstables()
                lsm.append_sstable(table)
