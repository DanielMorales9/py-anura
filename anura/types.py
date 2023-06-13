from typing import Dict

from anura.constants import DEFAULT_CHARSET


class IType:
    def __init__(self, struct_symbol: str, base_size: int):
        self.struct_symbol = struct_symbol
        self.base_size = base_size


class PrimitiveType(IType):
    pass


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


class ArrayType(IType):
    def __init__(self, length_type: IType, inner_type: IType):
        super().__init__(struct_symbol="x", base_size=0)
        self.length_type = length_type
        self.inner_type = inner_type


class StructType(IType):
    def __init__(self, inner: Dict[str, IType]):
        super().__init__(struct_symbol="x", base_size=0)
        self.inner = inner
