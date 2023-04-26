from itertools import islice
from typing import Any, Generator, Iterable, Sequence


def chunk(container: Iterable[Any], size: int) -> Generator[Sequence[Any], None, None]:
    iterator = iter(container)
    while _chunk := list(islice(iterator, size)):
        yield _chunk
