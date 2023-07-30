from contextlib import contextmanager
from enum import Enum
from threading import Condition, RLock, get_ident
from typing import Generic, Iterator, List, Optional, TypeVar


def get_id() -> int:
    return get_ident()


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self, with_promotion: bool = False) -> None:
        self._read_ready = Condition(RLock())
        self._r_num = 0
        self._w_num = 0
        self._with_promotion = with_promotion
        self._readers: List[int] = []  # List of Reader thread IDs
        self._writers: List[int] = []  # List of Writer thread IDs

    def acquire_read(self) -> None:
        """Acquire a read-lock. Blocks only if a thread has
        acquired the write-lock."""
        self._read_ready.acquire()
        try:
            while self._w_num > 0:
                self._read_ready.wait()
            self._r_num += 1
        finally:
            self._readers.append(get_id())
            self._read_ready.release()

    def release_read(self) -> None:
        """Release a read-lock."""
        self._read_ready.acquire()
        try:
            self._r_num -= 1
            if not self._r_num:
                self._read_ready.notify_all()
        finally:
            self._readers.remove(get_id())
            self._read_ready.release()

    @contextmanager
    def r_lock(self) -> Iterator[None]:
        """This method is designed to be used via the `with` statement."""
        try:
            self.acquire_read()
            yield
        finally:
            self.release_read()

    def acquire_write(self) -> None:
        """Acquires a write lock. Blocks until there are no
        acquired read or write locks."""

        self._read_ready.acquire()  # A re-entrant lock lets a thread re-acquire the lock
        self._w_num += 1
        self._writers.append(get_id())
        while self._r_num > 0:
            # promote to write lock, only if all the readers are trying to promote to writer
            # If there are other reader threads, then wait till they complete reading
            if self._with_promotion and get_id() in self._readers and set(self._readers).issubset(set(self._writers)):
                break
            else:
                self._read_ready.wait()

    def release_write(self) -> None:
        """Release a write lock."""

        self._w_num -= 1
        self._writers.remove(get_id())
        self._read_ready.notify_all()
        self._read_ready.release()

    @contextmanager
    def w_lock(self) -> Iterator[None]:
        """This method is designed to be used via the `with` statement."""
        try:
            self.acquire_write()
            yield
        finally:
            self.release_write()


VT = TypeVar("VT")
KT = TypeVar("KT")


class ConcurrentHashTable(Generic[KT, VT]):
    def __init__(self) -> None:
        super().__init__()
        self._dict: dict[KT, VT] = {}
        self._lock = RLock()

    def setdefault(self, key: KT, default: VT) -> VT:
        with self._lock:
            return self._dict.setdefault(key, default)

    def get(self, key: KT) -> Optional[VT]:
        with self._lock:
            return self._dict.get(key)


class LockMode(Enum):
    SHARED = 0
    EXCLUSIVE = 1


# TODO
#  https://github.com/dstibrany/LockManager/blob/master/src/main/java/com/dstibrany/lockmanager/LockManager.java
class LockManager:
    _instance: Optional["LockManager"] = None

    def __new__(cls) -> "LockManager":
        if cls._instance is None:
            print("Creating the LockManager")
            cls._instance = super().__new__(cls)
            cls._lock_table = ConcurrentHashTable[int, ReadWriteLock]()
        return cls._instance

    @contextmanager
    def lock(self, lock_id: int, mode: LockMode) -> Iterator[None]:
        lock = self._lock_table.setdefault(lock_id, ReadWriteLock())
        print("LCK", get_id(), id(lock))

        if mode == LockMode.EXCLUSIVE:
            acquire, release = lock.acquire_write, lock.release_write
        elif mode == LockMode.SHARED:
            acquire, release = lock.acquire_read, lock.release_read
        else:
            raise ValueError(f"Lock mode does not exists {mode.name}")

        try:
            acquire()
            yield
        finally:
            release()
