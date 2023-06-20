from anura.concurrency import ReadWriteLock
from anura.lsm import LSMTree


class TableFlusher:
    @staticmethod
    def flush(lsm: LSMTree) -> None:
        # TODO acquire lock from manager
        with ReadWriteLock().w_lock():
            table = lsm.create_table()
            lsm.reset_cache()
            lsm.append_table(table)
