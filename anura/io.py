import struct
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

from anura.constants import META_CONFIG


def decode(block: bytes, metadata: Sequence[Dict]) -> Iterator[Sequence[Any]]:
    i = 0
    while i < len(block):
        record = [None] * len(metadata)
        for j, metatype in enumerate(metadata):
            el, offset = unpack(block[i:], **metatype)
            record[j] = el
            i += offset
        yield record


def encode(block: Sequence[Any], metadata: Sequence[Dict]) -> Iterator[bytes]:
    for record in block:
        for el, metatype in zip(record, metadata):
            yield pack(el, **metatype)


def unpack(
    block: bytes,
    struct_symbol: str,
    base_size: int,
    is_container: Optional[bool] = False,
    charset: Optional[str] = None,
    length_type: Optional[str] = None,
    inner_type: Optional[Dict] = None,
    **kwargs: Any,
) -> Tuple[Any, int]:
    start, offset, size = 0, base_size, 1

    # TODO refactor: simplify
    if is_container and length_type:
        size, offset = unpack(block[start:], **META_CONFIG[length_type])  # type: ignore[arg-type]
        start, offset = start + offset, start + offset + base_size * size

    if inner_type:
        i = 0
        res = []
        while i < size:
            inner, offset = unpack(block[start:], **inner_type)
            res.append(inner)
            i += 1
            start += offset
        offset = start
    else:
        res = struct.unpack(f">{size}{struct_symbol}", block[start:offset])[0]

        if charset:
            res = res.decode(charset)  # type: ignore[attr-defined]
    return res, offset


def pack(
    field: Any,
    struct_symbol: str,
    is_container: Optional[bool] = False,
    charset: Optional[str] = None,
    length_type: Optional[str] = None,
    inner_type: Optional[Dict] = None,
    **kwargs: Any,
) -> bytes:
    size = 1
    args = []
    length_symbol = ""

    if charset:
        field = field.encode(charset)

    # TODO refactor: simplify
    if length_type:
        length_symbol = str(META_CONFIG[length_type]["struct_symbol"])

    if is_container:
        size = len(field)
        args.append(size)

    if inner_type:
        res = struct.pack(f">{length_symbol}", size)
        for el in field:
            res += pack(el, **inner_type)
    else:
        args.append(field)
        res = struct.pack(f">{length_symbol}{size}{struct_symbol}", *args)
    return res


def write_from(path: Path | str, it: Iterator, mode: str = "wb") -> None:
    with open(path, mode) as f:
        f.writelines(it)


def read_block(path: Path | str, mode: str = "rb", offset: int = 0, n: int = -1) -> Any:
    with open(path, mode) as f:
        f.seek(offset)
        return f.read(n)
