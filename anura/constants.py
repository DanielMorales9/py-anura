from enum import Enum

BLOCK_SIZE = 50
DEFAULT_CHARSET = "utf-8"
SSTABLE_EXT = "sst"
SPARSE_IDX_EXT = "spx"


class PrimitiveType(str, Enum):
    # TODO is varchar primitive
    VARCHAR = "VARCHAR"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    UNSIGNED_SHORT = "UNSIGNED_SHORT"


class ComplexType(str, Enum):
    ARRAY = "ARRAY"


META_CONFIG = {
    PrimitiveType.VARCHAR: {
        "struct_symbol": "s",
        "base_size": 1,
        "is_container": True,
        "charset": DEFAULT_CHARSET,
        "length_type": PrimitiveType.UNSIGNED_SHORT,
    },
    PrimitiveType.SHORT: {
        "struct_symbol": "h",
        "base_size": 2,
    },
    PrimitiveType.INT: {
        "struct_symbol": "i",
        "base_size": 4,
    },
    PrimitiveType.LONG: {
        "struct_symbol": "l",
        "base_size": 4,
    },
    PrimitiveType.FLOAT: {
        "struct_symbol": "f",
        "base_size": 4,
    },
    PrimitiveType.DOUBLE: {
        "struct_symbol": "d",
        "base_size": 8,
    },
    PrimitiveType.BOOL: {
        "struct_symbol": "?",
        "base_size": 1,
    },
    PrimitiveType.UNSIGNED_SHORT: {
        "struct_symbol": "H",
        "base_size": 2,
    },
    ComplexType.ARRAY: {
        "struct_symbol": "x",
        "base_size": 0,
        "is_container": True,
        "length_type": PrimitiveType.UNSIGNED_SHORT,
    },
}
