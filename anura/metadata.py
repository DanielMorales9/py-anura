from pathlib import Path
from typing import Any, Iterator

from anura.experimental.parser import parse
from anura.types import IType


class TableMetadata:
    def __init__(self, path: Path):
        self._path = path / "meta.data"
        with open(self._path) as f:
            # TODO use a json parser instead
            self._meta = parse(f.read())

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
