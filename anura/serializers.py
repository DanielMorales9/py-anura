import abc
import struct
from typing import Any, Tuple

from anura.constants import PrimitiveTypeEnum, SerializerEnum
from anura.types import ArrayType, IType, PrimitiveType, StructType, VarcharType
from anura.utils import normalize_name


class IEncoder(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def encode(field: Any, meta: Any) -> bytes:
        pass


class IDecoder(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def decode(block: bytes, meta: Any) -> Tuple[Any, int]:
        pass


class PrimitiveEncoder(IEncoder):
    @staticmethod
    def encode(field: Any, meta: PrimitiveType) -> bytes:
        return struct.pack(f">{meta.struct_symbol}", field)


class VarcharEncoder(IEncoder):
    @staticmethod
    def encode(field: Any, meta: VarcharType) -> bytes:
        length_symbol = meta.length_type.struct_symbol
        field = field.encode(meta.charset)
        size = len(field)
        return struct.pack(f">{length_symbol}{size}{meta.struct_symbol}", size, field)


class ArrayEncoder(IEncoder):
    @staticmethod
    def encode(field: Any, meta: ArrayType) -> bytes:
        length_symbol = meta.length_type.struct_symbol
        size = len(field)
        res = struct.pack(f">{length_symbol}", size)
        for el in field:
            res += get_encoder(meta.inner_type).encode(el, meta=meta.inner_type)
        return res


class StructEncoder(IEncoder):
    @staticmethod
    def encode(field: Any, meta: StructType) -> bytes:
        res = b""
        for key, value in field.items():
            res += get_encoder(meta.inner[key]).encode(value, meta=meta.inner[key])
        return res


class PrimitiveDecoder(IDecoder):
    @staticmethod
    def decode(block: bytes, meta: PrimitiveType) -> Tuple[Any, int]:
        res = struct.unpack(f">{meta.struct_symbol}", block[: meta.base_size])[0]
        return res, meta.base_size


class VarcharDecoder(IDecoder):
    @staticmethod
    def decode(block: bytes, meta: VarcharType) -> Tuple[Any, int]:
        size, offset = get_decoder(meta.length_type).decode(block, meta=meta.length_type)
        start, offset = offset, offset + meta.base_size * size
        res = struct.unpack(f">{size}{meta.struct_symbol}", block[start:offset])[0]
        res = res.decode(meta.charset)
        return res, offset


class ArrayDecoder(IDecoder):
    @staticmethod
    def decode(block: bytes, meta: ArrayType) -> Tuple[Any, int]:
        size, start = get_decoder(meta.length_type).decode(block, meta=meta.length_type)

        i = 0
        res = []
        while i < size:
            el, offset = get_decoder(meta.inner_type).decode(block[start:], meta=meta.inner_type)
            res.append(el)
            i += 1
            start += offset
        return res, start


class StructDecoder(IDecoder):
    @staticmethod
    def decode(block: bytes, meta: StructType) -> Tuple[Any, int]:
        start = 0
        res = {}
        for key, value in meta.inner.items():
            el, offset = get_decoder(value).decode(block[start:], meta=value)
            res[key] = el
            start += offset
        return res, start


def get_decoder(metatype: IType) -> IDecoder:
    return get_serializer_class(metatype, SerializerEnum.DECODER)  # type: ignore[no-any-return]


def get_encoder(metatype: IType) -> IEncoder:
    return get_serializer_class(metatype, SerializerEnum.ENCODER)  # type: ignore[no-any-return]


def get_serializer_class(metatype: IType, class_type: str) -> Any:
    encoder_class_name = metatype.__class__.__name__.replace("Type", class_type)
    return globals()[encoder_class_name]()


def _register_primitive_serializer_classes(class_type: str) -> None:
    base_class = globals()[f"Primitive{class_type}"]
    for name in PrimitiveTypeEnum:
        # TODO simplify PRIMITIVE + VARCHAR
        if name != PrimitiveTypeEnum.VARCHAR:
            class_name = f"{normalize_name(name)}{class_type}"
            globals()[class_name] = type(class_name, (base_class,), {})


for serializer_name in SerializerEnum:
    _register_primitive_serializer_classes(serializer_name)
