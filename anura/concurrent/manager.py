import dataclasses
import logging
from contextlib import contextmanager
from threading import Condition, RLock
from typing import Iterator, Optional, Set

from anura.algorithms import has_cycle
from anura.concurrent._locks import ReentrantReadWriteLock
from anura.concurrent.util import ConcurrentDict
from anura.constants import LockMode


class DeadlockException(Exception):
    pass


class InterruptedException(Exception):
    pass


class TransactionLock:
    """The Transaction Lock"""

    def __init__(self, lock_id: int, graph: "WaitForGraph") -> None:
        self.lock_id = lock_id
        self._graph = graph
        self._lock = Condition(RLock())
        self._slock_count = 0
        self._xlock_count = 0
        self._owners: Set[Transaction] = set()  # Set of Writer thread IDs

    def s_acquire(self, txn: "Transaction") -> None:
        """Acquire a read-lock. Blocks only if a thread has acquired the write-lock."""
        logging.debug("TXN %d ACQ SLOCK %d", txn.txn_id, self.lock_id)
        self._lock.acquire()
        try:
            while self.is_xlock():
                self._graph.add(txn, self._owners)
                self._graph.detect_deadlock(txn)
                logging.debug("TXN %d WAIT SLOCK %d", txn.txn_id, self.lock_id)
                self._lock.wait()
            self._slock_count += 1
            self._owners.add(txn)
        finally:
            self._lock.release()

    def x_acquire(self, txn: "Transaction") -> None:
        """Acquires a write lock. Blocks until there are no acquired read or write locks."""
        logging.debug("TXN %d ACQ XLOCK %d", txn.txn_id, self.lock_id)
        self._lock.acquire()
        try:
            while self.is_xlock() or self.is_slock():
                self._graph.add(txn, self._owners)
                self._graph.detect_deadlock(txn)
                logging.debug("TXN %d WAIT XLOCK %d", txn.txn_id, self.lock_id)
                self._lock.wait()
            self._xlock_count += 1
            self._owners.add(txn)
        finally:
            self._lock.release()

    def is_xlock(self) -> int:
        return self._xlock_count == 1

    def is_slock(self) -> int:
        return self._slock_count > 0

    def upgrade(self, txn: "Transaction") -> None:
        logging.debug("TXN %d UPG LCK %d", txn.txn_id, self.lock_id)
        self._lock.acquire()
        try:
            logging.debug("TXN %d XLOCK count %d SLOCK count %d", txn.txn_id, self._xlock_count, self._slock_count)
            if self.is_xlock() and txn.txn_id in self._owners:
                return
            while self.is_xlock() or self._slock_count > 1:
                logging.debug("TXN %d XLOCK count %d SLOCK count %d", txn.txn_id, self._xlock_count, self._slock_count)
                self._graph.add(txn, self._owners - {txn})
                self._graph.detect_deadlock(txn)
                self._lock.wait()

            self._owners.remove(txn)
            self._slock_count = 0
            self._xlock_count = 1
        finally:
            self._lock.release()

    def acquire(self, txn: "Transaction", mode: LockMode) -> None:
        if mode == LockMode.EXCLUSIVE:
            self.x_acquire(txn)
        elif mode == LockMode.SHARED:
            self.s_acquire(txn)
        else:
            raise ValueError(f"Lock mode does not exists {mode}")

    def release(self, txn: "Transaction") -> None:
        logging.debug("TXN %d RLS LCK %d", txn.txn_id, self.lock_id)
        self._lock.acquire()
        try:
            logging.debug("TXN %d XLOCK count %d SLOCK count %d", txn.txn_id, self._xlock_count, self._slock_count)
            if self._slock_count > 0:
                self._slock_count -= 1

            if self._xlock_count == 1:
                self._xlock_count = 0

            if txn in self._owners:
                self._owners.remove(txn)

            logging.debug("TXN %d RLS LCK %d", txn.txn_id, self.lock_id)
            self._graph.remove(txn)
            self._lock.notify_all()
        finally:
            self._lock.release()

    @property
    def mode(self) -> Optional[LockMode]:
        with self._lock:
            mode = None
            if self.is_xlock():
                mode = LockMode.EXCLUSIVE
            elif self.is_slock():
                mode = LockMode.SHARED
            return mode

    def __repr__(self) -> str:
        return f"LCK={self.lock_id}"


@dataclasses.dataclass
class Transaction:
    txn_id: int
    locks: Set[TransactionLock] = dataclasses.field(default_factory=lambda: set(), init=False)

    def __hash__(self) -> int:
        return self.txn_id

    def add(self, lock: TransactionLock) -> None:
        self.locks.add(lock)

    def remove(self, lock: TransactionLock) -> None:
        if lock in self.locks:
            self.locks.remove(lock)

    def abort(self) -> None:
        logging.error("TXN %d aborted", self.txn_id)
        raise InterruptedException(f"Aborting TXN {self.txn_id} with Locks {self.locks}")

    def __repr__(self) -> str:
        return f"TXN={self.txn_id}"


class WaitForGraph:
    def __init__(self) -> None:
        self._adjacent = ConcurrentDict[Transaction, Set[Transaction]]()
        self._lock = ReentrantReadWriteLock()

    def __repr__(self) -> str:
        return repr(self._adjacent)

    def add(self, predecessor: Transaction, successors: Set[Transaction]) -> None:
        self._lock.acquire_read()
        try:
            transactions = self._adjacent.setdefault(predecessor, set())
            transactions.update(successors)
            self._adjacent[predecessor] = successors
        finally:
            self._lock.release_read()

    def remove(self, txn: Transaction) -> None:
        logging.debug("TXN %d RM", txn.txn_id)
        self._lock.acquire_read()
        try:
            if txn in self._adjacent:
                del self._adjacent[txn]
            self._remove_successor(txn)
        finally:
            self._lock.release_read()

    def detect_deadlock(self, txn: Transaction) -> None:
        logging.debug("TXN %d DLOCK", txn.txn_id)
        self._lock.acquire_write()
        try:
            if has_cycle(self._adjacent, txn):  # type: ignore[arg-type]
                txn.abort()
        finally:
            self._lock.release_write()

    def _remove_successor(self, txn: Transaction) -> None:
        for predecessor in self._adjacent:
            successors = self._adjacent.get(predecessor)
            if successors and txn in successors:
                successors.remove(txn)


class LockManager:
    """
    The Lock Manager provides concurrency control at the entity level.
    Similar implementation https://github.com/dstibrany/LockManager
    """

    def __init__(self) -> None:
        logging.debug("Creating the LockManager")
        self._lock_table = ConcurrentDict[int, TransactionLock]()
        self._txn_table = ConcurrentDict[int, Transaction]()
        self._graph = WaitForGraph()

    @contextmanager
    def lock(self, lock_id: int, txn_id: int, request_mode: LockMode) -> Iterator[None]:
        lock = self._lock_table.setdefault(lock_id, TransactionLock(lock_id, self._graph))
        txn = self._txn_table.get(txn_id, Transaction(txn_id))

        try:
            if self.has_lock(txn, lock) and request_mode == lock.mode:
                logging.debug("TXN %d has already LCK %d", txn_id, lock_id)
                yield
                return
            elif self.has_lock(txn, lock) and request_mode == LockMode.SHARED and lock.mode == LockMode.EXCLUSIVE:
                logging.debug("TXN %d has already LCK %d with higher privilege", txn_id, lock_id)
            elif self.has_lock(txn, lock) and request_mode == LockMode.EXCLUSIVE and lock.mode == LockMode.SHARED:
                logging.debug("TXN %d attempts upgrade on LCK %d", txn_id, lock_id)
                lock.upgrade(txn)
            else:
                lock.acquire(txn, request_mode)
        except InterruptedException as e:
            logging.exception("Deadlock Detected")
            # remove transactions
            self.remove_transaction(txn)
            raise DeadlockException(txn_id) from e

        txn.add(lock)
        self._txn_table[txn_id] = txn
        yield

        # release lock
        lock.release(txn)
        txn.remove(lock)

    def remove_transaction(self, txn: Transaction) -> None:
        txn = self._txn_table.get(txn.txn_id)
        if txn and txn.locks:
            for lock in txn.locks:
                lock.release(txn)
            del self._txn_table[txn.txn_id]

    def has_lock(self, txn: Transaction, lock: TransactionLock) -> bool:
        txn = self._txn_table.get(txn.txn_id)
        if not txn:
            return False

        if not txn.locks:
            return False

        return self._lock_table.get(lock.lock_id) in txn.locks
