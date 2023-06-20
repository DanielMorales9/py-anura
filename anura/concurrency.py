import os
from contextlib import contextmanager
from typing import Iterator, List

from anura.constants import PROCESS_BASED

if PROCESS_BASED == 1:
    from multiprocessing import Condition, RLock

    def get_id() -> int:
        return os.getpid()

else:
    from threading import Condition, RLock, get_ident  # type: ignore[misc]

    def get_id() -> int:
        return get_ident()


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self, with_promotion: bool = False) -> None:
        self._read_ready = Condition(RLock())
        self._readers = 0
        self._writers = 0
        self._promote = with_promotion
        self._readerList: List[int] = []  # List of Reader thread IDs
        self._writerList: List[int] = []  # List of Writer thread IDs

    def acquire_read(self) -> None:
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._readerList.append(get_id())
            self._read_ready.release()

    def release_read(self) -> None:
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notify_all()
        finally:
            self._readerList.remove(get_id())
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
        self._writers += 1
        self._writerList.append(get_id())
        while self._readers > 0:
            # promote to write lock, only if all the readers are trying to promote to writer
            # If there are other reader threads, then wait till they complete reading
            if self._promote and get_id() in self._readerList and set(self._readerList).issubset(set(self._writerList)):
                break
            else:
                self._read_ready.wait()

    def release_write(self) -> None:
        """Release a write lock."""

        self._writers -= 1
        self._writerList.remove(get_id())
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
