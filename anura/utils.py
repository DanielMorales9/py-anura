import contextlib
import heapq
from itertools import islice
from typing import Any, Callable, Iterator, List, Optional, Sequence


def chunk(it: Iterator, size: int) -> Iterator[Sequence[Any]]:
    while _chunk := list(islice(it, size)):
        yield _chunk


INIT = 0


def sequential(*_: Any) -> int:
    global INIT
    i = INIT
    INIT += 1
    return i


def k_way_merge_sort(tables: List[Any], key: Optional[Callable[[Any], Any]] = None) -> Iterator:
    if not key:
        key = sequential

    heap = []
    for table in tables:
        it = iter(table)
        heap.append((next(it), key(table), it))

    heapq.heapify(heap)
    while heap:
        curr, key, it = heapq.heappop(heap)  # type: ignore[assignment]
        yield curr
        with contextlib.suppress(StopIteration):
            heapq.heappush(heap, (next(it), key, it))
