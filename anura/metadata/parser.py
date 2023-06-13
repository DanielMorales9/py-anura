from typing import Any, Dict, Sequence, Tuple

from ply.lex import lex
from ply.yacc import yacc

# All tokens must be named in advance.
from anura import types
from anura.constants import DEFAULT_LENGTH_TYPE, PrimitiveTypeEnum

# --- Tokenizer
from anura.types import ArrayType, IType
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
t_ID = r"[a-zA-Z_][a-zA-Z_0-9]*"
t_VALUE = r"[a-z-A-Z_0-9]+"

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
    return ArrayType(length_type=create_type(DEFAULT_LENGTH_TYPE), inner_type=create_type(p))


def edit_options(_type, **options):
    # TODO options validation
    # TODO edit options using class interface
    for key, value in options.items():
        if key in _type:
            _type[key] = value
        else:
            raise ValueError(f"{key} not allowed")
    return _type


def create_type(p) -> IType:
    extra = {}
    if p == PrimitiveTypeEnum.VARCHAR:
        extra["length_type"] = create_type(DEFAULT_LENGTH_TYPE)
    return getattr(types, f"{normalize_name(p)}Type")(**extra)


def run(p: Tuple | str) -> Dict[str, Any] | list | IType:
    if isinstance(p, tuple):
        if p[0] == "group":
            return {**run(p[1]), **run(p[2])}
        elif p[0] == "assign":
            return {p[1]: run(p[2])}
        elif p[0] == "array":
            return create_array(p[1])
        elif p[0] == "struct":
            # TODO: create_struct(p[1])
            return [run(p[1])]
        elif p[0] == "option":
            _type = run(p[2])
            return edit_options(_type)
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
    print(p)


# Build the lexer object
lexer = lex()

# Build the parser
parser = yacc()


def parse(seq: Sequence[Any]) -> Dict[str, Any]:
    return run(parser.parse(seq))
