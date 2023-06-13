from enum import Enum

BLOCK_SIZE = 50
DEFAULT_CHARSET = "utf-8"
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


class SerializerEnum(str, Enum):
    ENCODER = "Encoder"
    DECODER = "Decoder"


class ComplexTypeEnum(str, Enum):
    ARRAY = "ARRAY"
    # STRUCT = "STRUCT"


DEFAULT_LENGTH_TYPE = PrimitiveTypeEnum.UNSIGNED_SHORT
META_PRIMITIVE_MAP = {
    "SHORT": ("h", 2),
    "INT": ("i", 4),
    "LONG": ("l", 4),
    "FLOAT": ("f", 4),
    "DOUBLE": ("d", 8),
    "BOOL": ("?", 1),
    "UNSIGNED_SHORT": ("H", 2),
}
