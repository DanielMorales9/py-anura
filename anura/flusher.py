from anura.concurrent.manager import LockManager
from anura.constants import LockMode
from anura.lsm import LSMTree


class TableFlusher:
    def __init__(self, lock_manager: LockManager) -> None:
        self._mgr = lock_manager

    def flush(self, lsm: LSMTree, txn_id: int) -> None:
        lock_id = hash(lsm.metadata.table_name)
        with self._mgr.lock(lock_id, txn_id, LockMode.EXCLUSIVE):
            table = lsm.create_sstable()
            table.write(iter(lsm.mem_table))
            lsm.reset_cache()
            lsm.append_sstable(table)
