import os
from enum import Enum

PROCESS_BASED = int(os.environ.get("PROCESS_BASED", 1))
BLOCK_SIZE = 50
SSTABLE_EXT = "sst"
SPARSE_IDX_EXT = "spx"


class PrimitiveType(str, Enum):
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    UNSIGNED_SHORT = "UNSIGNED_SHORT"
    UNSIGNED_INT = "UNSIGNED_INT"
    UNSIGNED_LONG = "UNSIGNED_LONG"


class ComplexType(str, Enum):
    VARCHAR = "VARCHAR"


class Serializer(str, Enum):
    ENCODER = "Encoder"
    DECODER = "Decoder"


class Charset(str, Enum):
    ASCII = "ascii"
    UTF_8 = "utf-8"


TMP_EXT = ".tmp"
