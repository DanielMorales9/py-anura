import dataclasses
from pathlib import Path
from typing import Any, Dict, Iterator

from anura.types import IType, get_class_type
from anura.utils import load_json


def _parse_type(inp: Dict | str | int) -> Any:
    if not isinstance(inp, dict):
        return inp
    if "type" not in inp:
        return {k: _parse_type(v) for k, v in inp.items()}

    opts = inp.setdefault("options", {})
    kwargs = {k: _parse_type(v) for k, v in opts.items()}
    return get_class_type(inp["type"])(**kwargs)


def parse_metadata(path: Path) -> dict:
    content = load_json(path)
    meta = {"table_name": content["table_name"]}
    for name, value in content["fields"].items():
        meta[name] = _parse_type(value)
    return meta


@dataclasses.dataclass
class TableMetadata:
    table_name: str
    key: IType
    value: IType
    tombstone: IType

    def __iter__(self) -> Iterator[IType]:
        return iter((self.key, self.value, self.tombstone))
