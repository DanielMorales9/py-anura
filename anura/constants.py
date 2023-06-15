from enum import Enum

BLOCK_SIZE = 50
SSTABLE_EXT = "sst"
SPARSE_IDX_EXT = "spx"


class PrimitiveTypeEnum(str, Enum):
    # TODO is varchar primitive
    VARCHAR = "VARCHAR"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    UNSIGNED_SHORT = "UNSIGNED_SHORT"


class Serializer(str, Enum):
    ENCODER = "Encoder"
    DECODER = "Decoder"


class Charset(str, Enum):
    ASCII = "ascii"
    UTF_8 = "utf-8"
