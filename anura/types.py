import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, Type

from anura.constants import Charset
from anura.utils import normalize_name


@dataclass
class IType:
    def _meta_validation(self, field_: str, value: Any) -> None:
        validation_func = getattr(self, f"_validate_{field_}", None)
        if callable(validation_func):
            validation_func(value)

    def __post_init__(self) -> None:
        for field_ in dataclasses.fields(self):
            self._meta_validation(field_.name, getattr(self, field_.name))

    def __setattr__(self, key: str, value: Any) -> None:
        self._meta_validation(key, value)
        super().__setattr__(key, value)


@dataclass
class APrimitiveType(IType):
    # TODO improve design
    _struct_symbol: str
    _base_size: int

    @property
    def struct_symbol(self) -> str:
        return self._struct_symbol

    @property
    def base_size(self) -> int:
        return self._base_size


@dataclass
class ShortType(APrimitiveType):
    _struct_symbol: str = "h"
    _base_size: int = 2


@dataclass
class IntType(APrimitiveType):
    _struct_symbol: str = "i"
    _base_size: int = 4


@dataclass
class LongType(APrimitiveType):
    _struct_symbol: str = "l"
    _base_size: int = 4


@dataclass
class FloatType(APrimitiveType):
    _struct_symbol: str = "f"
    _base_size: int = 4


@dataclass
class DoubleType(APrimitiveType):
    _struct_symbol: str = "d"
    _base_size: int = 8


@dataclass
class BoolType(APrimitiveType):
    _struct_symbol: str = "?"
    _base_size: int = 1


@dataclass
class UnsignedShortType(APrimitiveType):
    _struct_symbol: str = "H"
    _base_size: int = 2


@dataclass
class UnsignedIntType(APrimitiveType):
    _struct_symbol: str = "I"
    _base_size: int = 4


@dataclass
class UnsignedLongType(APrimitiveType):
    _struct_symbol: str = "L"
    _base_size: int = 4


def _validate_length_type(value: APrimitiveType) -> None:
    if not isinstance(value, (UnsignedShortType, UnsignedIntType, UnsignedLongType)):
        raise TypeError(f"'{value.__class__.__name__}' is not a valid length type")


@dataclass
class VarcharType(APrimitiveType):
    _struct_symbol: str = "s"
    _base_size: int = 1
    charset: str = Charset.UTF_8.value
    length_type: APrimitiveType = UnsignedShortType()
    one_more_type: int = 10

    def _validate_charset(self, value: str) -> None:
        if value not in list(Charset):
            raise ValueError(f"'{value}' is not a valid charset for {self.__class__.__name__}")

    # TODO refactor using decorators
    @staticmethod
    def _validate_length_type(value: APrimitiveType) -> None:
        _validate_length_type(value)


@dataclass
class ArrayType(IType):
    inner_type: IType
    length_type: APrimitiveType = UnsignedShortType()

    @staticmethod
    def _validate_length_type(value: APrimitiveType) -> None:
        _validate_length_type(value)


@dataclass
class StructType(IType):
    inner: Dict[str, IType]


# TODO add type Timestamp
# TODO add type Date
# TODO add type Char


def get_class_type(terminal: str) -> Type:
    return globals()[f"{normalize_name(terminal)}Type"]  # type: ignore[no-any-return]
