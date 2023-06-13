import abc
import struct
from typing import Any, Tuple

from anura.constants import DEFAULT_CHARSET


class IType(abc.ABC):
    def __init__(self, struct_symbol: str, base_size: int):
        self.struct_symbol = struct_symbol
        self.base_size = base_size

    @abc.abstractmethod
    def encode(self, field: Any) -> bytes:
        pass

    @abc.abstractmethod
    def decode(self, block: bytes) -> Tuple[Any, int]:
        pass


class PrimitiveType(IType):
    def encode(self, field: Any) -> bytes:
        return struct.pack(f">{self.struct_symbol}", field)

    def decode(self, block: bytes) -> Tuple[Any, int]:
        res = struct.unpack(f">{self.struct_symbol}", block[: self.base_size])[0]
        return res, self.base_size


class ShortType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="h", base_size=2)


class IntType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="i", base_size=4)


class LongType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="l", base_size=4)


class FloatType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="f", base_size=4)


class DoubleType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="d", base_size=8)


class BoolType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="?", base_size=1)


class UnsignedShortType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(struct_symbol="H", base_size=2)


class VarcharType(IType):
    def __init__(self, length_type: IType, charset: str = DEFAULT_CHARSET):
        super().__init__(struct_symbol="s", base_size=1)
        self.charset = charset
        self.length_type = length_type

    def encode(self, field: Any) -> bytes:
        length_symbol = self.length_type.struct_symbol
        field = field.encode(self.charset)
        size = len(field)
        return struct.pack(f">{length_symbol}{size}{self.struct_symbol}", size, field)

    def decode(self, block: bytes) -> Tuple[Any, int]:
        size, offset = self.length_type.decode(block)
        start, offset = offset, offset + self.base_size * size
        res = struct.unpack(f">{size}{self.struct_symbol}", block[start:offset])[0]
        res = res.decode(self.charset)
        return res, offset


class ArrayType(IType):
    def __init__(self, length_type: IType, inner_type: IType):
        super().__init__(struct_symbol="x", base_size=0)
        self.length_type = length_type
        self.inner_type = inner_type

    def encode(self, field: Any) -> bytes:
        length_symbol = self.length_type.struct_symbol
        size = len(field)
        res = struct.pack(f">{length_symbol}", size)
        for el in field:
            res += self.inner_type.encode(el)
        return res

    def decode(self, block: bytes) -> Tuple[Any, int]:
        size, start = self.length_type.decode(block)

        i = 0
        res = []
        while i < size:
            inner, offset = self.inner_type.decode(block[start:])
            res.append(inner)
            i += 1
            start += offset
        return res, start
