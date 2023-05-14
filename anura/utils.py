from itertools import islice
from typing import Any, Generator, Iterator, Sequence


def chunk(it: Iterator, size: int) -> Generator[Sequence[Any], None, None]:
    while _chunk := list(islice(it, size)):
        yield _chunk
