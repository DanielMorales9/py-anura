from enum import Enum

BLOCK_SIZE = 50
DEFAULT_CHARSET = "utf-8"
SSTABLE_EXT = "sst"
SPARSE_IDX_EXT = "spx"


class TypeEnum(str, Enum):
    # TODO is varchar primitive
    VARCHAR = "VARCHAR"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    UNSIGNED_SHORT = "UNSIGNED_SHORT"


class ComplexTypeEnum(str, Enum):
    ARRAY = "ARRAY"
    # STRUCT = "STRUCT"


META_CONFIG = {
    ComplexTypeEnum.ARRAY: {
        "struct_symbol": "x",
        "base_size": 0,
        "is_container": True,
        "length_type": TypeEnum.UNSIGNED_SHORT,
    },
    TypeEnum.VARCHAR: {
        "struct_symbol": "s",
        "base_size": 1,
        "is_container": True,
        "charset": DEFAULT_CHARSET,
        "length_type": TypeEnum.UNSIGNED_SHORT,
    },
    TypeEnum.SHORT: {
        "struct_symbol": "h",
        "base_size": 2,
    },
    TypeEnum.INT: {
        "struct_symbol": "i",
        "base_size": 4,
    },
    TypeEnum.LONG: {
        "struct_symbol": "l",
        "base_size": 4,
    },
    TypeEnum.FLOAT: {
        "struct_symbol": "f",
        "base_size": 4,
    },
    TypeEnum.DOUBLE: {
        "struct_symbol": "d",
        "base_size": 8,
    },
    TypeEnum.BOOL: {
        "struct_symbol": "?",
        "base_size": 1,
    },
    TypeEnum.UNSIGNED_SHORT: {
        "struct_symbol": "H",
        "base_size": 2,
    },
}
