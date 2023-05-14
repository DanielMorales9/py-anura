from itertools import islice
from typing import Any, Iterator, Sequence


def chunk(it: Iterator, size: int) -> Iterator[Sequence[Any]]:
    while _chunk := list(islice(it, size)):
        yield _chunk
