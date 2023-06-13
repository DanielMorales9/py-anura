from dataclasses import dataclass, field, make_dataclass
from typing import Dict, Iterable, Type

from anura.constants import DEFAULT_CHARSET, META_PRIMITIVE_MAP
from anura.utils import normalize_name


@dataclass
class IType:
    pass


@dataclass
class PrimitiveType(IType):
    struct_symbol: str = "x"
    base_size: int = 0


@dataclass
class VarcharType(IType):
    length_type: PrimitiveType
    struct_symbol: str = "s"
    base_size: int = 1
    charset: str = DEFAULT_CHARSET


@dataclass
class ArrayType(IType):
    length_type: PrimitiveType
    inner_type: IType


@dataclass
class StructType(IType):
    inner: Dict[str, IType]


def register_primitive_types() -> None:
    field_names = ["struct_symbol", "base_size"]
    field_types: Iterable[Type] = [str, int]
    for name, args in META_PRIMITIVE_MAP.items():
        type_name = f"{normalize_name(name)}Type"
        fields = [(name, _type, field(default=value)) for name, _type, value in zip(field_names, field_types, args)]
        globals()[type_name] = make_dataclass(type_name, fields=fields, bases=(PrimitiveType,))


register_primitive_types()
