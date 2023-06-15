import pytest as pytest

from anura.constants import Charset
from anura.metadata.exceptions import ParsingError
from anura.metadata.parser import parse
from anura.types import VarcharType  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "meta, expected",
    [
        (
            "value=VARCHAR(charset='utf-8')",
            {"value": VarcharType(charset=Charset.UTF_8.value)},
        ),
        (
            "value=VARCHAR(charset='ascii')",
            {"value": VarcharType(charset=Charset.ASCII.value)},
        ),
    ],
)
def test_meta_with_options(meta, expected):
    assert parse(meta) == expected


@pytest.mark.parametrize(
    "meta, error, match",
    [
        ("value=VARCHAR(,)", ParsingError, "LexToken\\(COMMA,',',1,14\\)"),
        ("value=VARCHAR(struct_symbol='x')", AttributeError, "can't set attribute 'struct_symbol'"),
        ("value=VARCHAR(unexistent='x')", AttributeError, "Attribute 'unexistent' not found in VarcharType"),
        ("value=VARCHAR(charset='wrong')", ValueError, "'wrong' is not a valid charset for VarcharType"),
        ("value=VARCHAR(_base_size='1')", AttributeError, "can't set attribute 'base_size'"),
        ("value=VARCHAR(length_type='ARRAY')", ValueError, "'ARRAY' is not a PrimitiveType"),
        ("value=VARCHAR(length_type='VARCHAR')", TypeError, "'VarcharType' is not a valid length type"),
        (
            # TODO: fix duplicate options
            "value=VARCHAR(charset='ascii',length_type='LONG',length_type='VARCHAR')",
            TypeError,
            "'VarcharType' is not a valid length type",
        ),
    ],
)
def test_meta_with_options_raises_error(meta, error, match):
    with pytest.raises(error, match=match):
        parse(meta)
