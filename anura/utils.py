from typing import Any, Type


def normalize_name(name: str) -> str:
    return "".join(el.capitalize() for el in name.split("_"))


def is_builtin_type(source_type: Type) -> bool:
    return source_type.__module__ == "builtins"


def convert_to_builtin_type(field_type: Type, value: str) -> Any:
    try:
        value = field_type(value)
    except ValueError as e:
        raise ValueError(f"'{value}' cannot be cast to {field_type.__name__}") from e
    return value
