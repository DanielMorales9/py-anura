import json
from pathlib import Path
from typing import Any, Dict, Iterator

from anura.types import IType, get_class_type


def _parse_type(inp: Dict | str | int) -> Any:
    if not isinstance(inp, dict):
        return inp
    if "type" not in inp:
        return {k: _parse_type(v) for k, v in inp.items()}

    opts = inp.setdefault("options", {})
    kwargs = {k: _parse_type(v) for k, v in opts.items()}
    return get_class_type(inp["type"])(**kwargs)


class TableMetadata:
    def __init__(self, path: Path):
        self._path = path / "metadata.json"
        with open(self._path) as f:
            json_meta = json.load(f)
        self._meta = {field: _parse_type(value) for field, value in json_meta["fields"].items()}

    @property
    def key_type(self) -> Any:
        return self._meta["key"]

    @property
    def value_type(self) -> Any:
        return self._meta["value"]

    @property
    def tombstone_type(self) -> Any:
        return self._meta["tombstone"]

    def __iter__(self) -> Iterator[IType]:
        return iter((self.key_type, self.value_type, self.tombstone_type))
