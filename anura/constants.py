from enum import Enum

BLOCK_SIZE = 50
DEFAULT_CHARSET = "utf-8"


class MetaType(str, Enum):
    VARCHAR = "VARCHAR"
    LONG = "LONG"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    UNSIGNED_SHORT = "UNSIGNED_SHORT"


MetaConfig = {
    MetaType.VARCHAR: {
        "struct_symbol": "s",
        "base_size": 1,
        "is_container": True,
        "charset": DEFAULT_CHARSET,
        "length_type": MetaType.UNSIGNED_SHORT,
    },
    MetaType.LONG: {
        "struct_symbol": "l",
        "base_size": 4,
    },
    MetaType.DOUBLE: {
        "struct_symbol": "d",
        "base_size": 8,
    },
    MetaType.BOOL: {
        "struct_symbol": "?",
        "base_size": 1,
    },
    MetaType.UNSIGNED_SHORT: {
        "struct_symbol": "H",
        "base_size": 2,
    },
}
SSTABLE_EXT = "sst"
SPARSE_IDX_EXT = "spx"
