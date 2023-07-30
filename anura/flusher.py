from anura.concurrency import LockManager, LockMode
from anura.lsm import LSMTree


class TableFlusher:
    @staticmethod
    def flush(lsm: LSMTree) -> None:
        mgr = LockManager()
        lock_id = hash(lsm.metadata.table_name)
        with mgr.lock(lock_id, LockMode.EXCLUSIVE):
            table = lsm.create_sstable()
            table.write(iter(lsm.mem_table))
            lsm.reset_cache()
            lsm.append_sstable(table)
