import json
import time
from pathlib import Path
from typing import Any, Type


def normalize_name(name: str) -> str:
    return "".join(el.capitalize() for el in name.split("_"))


def is_builtin_type(source_type: Type) -> bool:
    return source_type.__module__ == "builtins"


class ConversionError(ValueError):
    pass


def convert_to_builtin_type(field_type: Type, value: str) -> Any:
    try:
        value = field_type(value)
    except ValueError as e:
        raise ConversionError(f"'{value}' cannot be cast to {field_type.__name__}") from e
    return value


def load_json(path: Path) -> Any:
    with open(path) as f:
        return json.load(f)


def generate_id() -> int:
    return time.time_ns()
