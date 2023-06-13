from pathlib import Path
from typing import Any, Iterator, Sequence

from anura.types import IType


def decode(block: bytes, metadata: Sequence[IType]) -> Iterator[Sequence[Any]]:
    i = 0
    while i < len(block):
        record = [None] * len(metadata)
        for j, metatype in enumerate(metadata):
            el, offset = metatype.decode(block[i:])
            record[j] = el
            i += offset
        yield record


def encode(block: Sequence[Any], metadata: Sequence[IType]) -> Iterator[bytes]:
    for record in block:
        for el, metatype in zip(record, metadata):
            yield metatype.encode(el)


def write_from(path: Path | str, it: Iterator, mode: str = "wb") -> None:
    with open(path, mode) as f:
        f.writelines(it)


def read_block(path: Path | str, mode: str = "rb", offset: int = 0, n: int = -1) -> Any:
    with open(path, mode) as f:
        f.seek(offset)
        return f.read(n)
