import abc
from typing import Iterator, List

from anura.algorithms import k_way_merge_sort
from anura.concurrency import ReadWriteLock
from anura.lsm import LSMTree
from anura.model import K, MemNode, V
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
    def compact(self, lsm: "LSMTree") -> None:
        # enter process safe section
        table = SSTable[K, V](lsm.path, lsm.metadata, is_temp=True)
        table.write(gen_sort_uniq(lsm.tables))
        # exit process safe section

        # acquire write lock
        # TODO https://www.geeksforgeeks.org/implementation-of-locking-in-dbms/
        #  https://github.com/dstibrany/LockManager/tree/master/src/main/java/com/dstibrany/lockmanager
        with ReadWriteLock().w_lock():
            table.commit()
            lsm._tables = [table]
