from typing import Any, Dict, Sequence, Tuple, Type

from ply.lex import lex
from ply.yacc import yacc

# All tokens must be named in advance.
from anura import types
from anura.constants import PrimitiveTypeEnum

# --- Tokenizer
from anura.metadata.exceptions import ParsingError
from anura.types import ArrayType, IType, PrimitiveType, StructType
from anura.utils import normalize_name

tokens = ("ID", "ASSIGN", "TYPE", "COMMA", "LBRACE", "RBRACE", "LSQUARE", "RSQUARE", "LPAREN", "RPAREN", "VALUE")

# Token matching rules are written as regex
t_LBRACE = r"\{"
t_RBRACE = r"\}"
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_LSQUARE = r"\["
t_RSQUARE = r"\]"
t_ASSIGN = r"="
t_COMMA = r","
t_TYPE = r"|".join(PrimitiveTypeEnum)
t_ID = r"[a-zA-Z][a-zA-Z_0-9-]*"


def t_VALUE(t):
    r"'[a-z-A-Z_0-9-]+'"
    t.value = t.value[1:-1]
    return t


t_ignore = r" "


# Ignored token with an action associated with it
def t_ignore_newline(t):
    r"\n+"
    t.lexer.lineno += t.value.count("\n")


# Error handler for illegal characters
def t_error(t):
    print(f"Illegal character {t.value[0]!r}")
    t.lexer.skip(1)


precedence = (
    ("left", "COMMA"),
    ("left", "ASSIGN"),
)


def create_array(p) -> IType:
    return ArrayType(inner_type=create_type(p))


def is_builtin_type(source_type: Type) -> bool:
    return source_type.__module__ == "builtins"


def edit_options(anura_type, **options):
    for field, value in options.items():
        validate_has_attribute(anura_type, field)

        field_type = type(getattr(anura_type, field))
        if not isinstance(value, field_type):
            value = convert_value_to_field_type(field_type, value)

        setattr(anura_type, field, value)

    return anura_type


def convert_value_to_field_type(field_type: Type, value: str) -> Any:
    if is_builtin_type(field_type):
        value = convert_to_builtin_type(field_type, value)
    elif issubclass(field_type, PrimitiveType):
        value = convert_to_primitive_type(value)
    else:
        raise ValueError(f"Unknown {field_type}")
    return value


def convert_to_primitive_type(value: str) -> PrimitiveType:
    if value not in list(PrimitiveTypeEnum):
        raise ValueError(f"'{value}' is not a PrimitiveType")
    return getattr(types, f"{normalize_name(value)}Type")()


def convert_to_builtin_type(field_type: Type, value: str) -> Any:
    try:
        value = field_type(value)
    except ValueError as e:
        raise ValueError(f"'{value}' cannot be cast to {field_type.__name__}") from e
    return value


def validate_has_attribute(anura_type: str, field: str) -> None:
    type_name = anura_type.__class__.__name__
    if not hasattr(anura_type, field):
        raise AttributeError(f"Attribute '{field}' not found in {type_name}")


def create_type(p: str) -> IType:
    return getattr(types, f"{normalize_name(p)}Type")()


def run(p: Tuple | str, is_option: bool = False) -> Any:
    if isinstance(p, tuple):
        if p[0] == "group":
            return {**run(p[1], is_option), **run(p[2], is_option)}
        elif p[0] == "assign":
            return {p[1]: run(p[2], is_option)}
        elif p[0] == "array":
            return create_array(p[1])
        elif p[0] == "struct":
            return StructType(run(p[1]))
        elif p[0] == "option":
            _type = run(p[2])
            options = run(p[1], is_option=True)
            return edit_options(_type, **options)
    elif is_option:
        return p
    else:
        return create_type(p)


def p_calc(p):
    """
    calc : expression
         | empty
    """
    p[0] = p[1]


def p_empty(p):
    """
    empty :
    """
    p[0] = "Did not get it!"


def p_expression_comma(p):
    """
    expression : expression COMMA expression
    """
    p[0] = ("group", p[1], p[3])


def p_expression(p):
    """
    expression : ID ASSIGN type
    """
    p[0] = ("assign", p[1], p[3])


def p_type_with_option(p):
    """
    type : type LPAREN option RPAREN
    """
    p[0] = ("option", p[3], p[1])


def p_type(p):
    """
    type : TYPE
    """
    p[0] = p[1]


def p_type_struct(p):
    """
    type : LBRACE expression RBRACE
    """
    p[0] = ("struct", p[2])


def p_type_array(p):
    """
    type : TYPE LSQUARE RSQUARE
    """
    p[0] = ("array", p[1])


def p_option_comma(p):
    """
    option : option COMMA option
    """
    p[0] = ("group", p[1], p[3])


def p_option_assign(p):
    """
    option : ID ASSIGN VALUE
    """
    p[0] = ("assign", p[1], p[3])


def p_error(p):
    print("Syntax error in input!")
    raise ParsingError(p)


# Build the lexer object
lexer = lex()

# Build the parser
parser = yacc()


def parse(seq: Sequence[Any]) -> Dict[str, Any]:
    return run(parser.parse(seq))
