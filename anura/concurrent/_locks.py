from threading import Condition, RLock, get_ident
from typing import List


class ReentrantReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self) -> None:
        self._read_ready = Condition(RLock())
        self._r_count = 0
        self._w_count = 0
        self._promote = True
        self._readers: List[int] = []  # List of Reader thread IDs
        self._writers: List[int] = []  # List of Writer thread IDs

    def acquire_read(self) -> None:
        """Acquire a read lock. Blocks only if a thread has acquired the write-lock."""
        self._read_ready.acquire()
        try:
            while self._w_count > 0:
                self._read_ready.wait()
            self._r_count += 1
        finally:
            self._readers.append(get_ident())
            self._read_ready.release()

    def release_read(self) -> None:
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._r_count -= 1
            if not self._r_count:
                self._read_ready.notify_all()
        finally:
            self._readers.remove(get_ident())
            self._read_ready.release()

    def acquire_write(self) -> None:
        """Acquire a write lock. Blocks until there are no acquired read or write locks."""
        self._read_ready.acquire()  # A re-entrant lock lets a thread re-acquire the lock
        self._w_count += 1
        self._writers.append(get_ident())
        while self._r_count > 0:
            # promote to write lock, only if all the readers are trying to promote to writer
            # If there are other reader threads, then wait till they complete reading
            if get_ident() in self._readers and set(self._readers).issubset(set(self._writers)):
                break
            else:
                self._read_ready.wait()

    def release_write(self) -> None:
        """Release a write lock."""
        self._w_count -= 1
        self._writers.remove(get_ident())
        self._read_ready.notify_all()
        self._read_ready.release()
