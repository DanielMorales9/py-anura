import abc
from typing import Any, Dict, Sequence, Type

from ply.lex import lex
from ply.yacc import yacc

# All tokens must be named in advance.
from anura import types
from anura.constants import ComplexType, PrimitiveType

# --- Tokenizer
from anura.experimental.exceptions import ParsingError
from anura.types import APrimitiveType, ArrayType, IType, StructType, get_class_type
from anura.utils import convert_to_builtin_type, is_builtin_type, normalize_name

tokens = ("ID", "VALUE", "ASSIGN", "TYPE", "COMMA", "LBRACE", "RBRACE", "LSQUARE", "RSQUARE", "LPAREN", "RPAREN")

basic_types = list(PrimitiveType)
basic_types.append(ComplexType.VARCHAR)
# Token matching rules are written as regex
t_LBRACE = r"\{"
t_RBRACE = r"\}"
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_LSQUARE = r"\["
t_RSQUARE = r"\]"
t_ASSIGN = r"="
t_COMMA = r","
t_TYPE = r"|".join(basic_types)
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


class AOp(abc.ABC):
    @abc.abstractmethod
    def __call__(self) -> Any:
        pass

    def build(self):
        return self()


class TerminalOp(AOp):
    def __init__(self, terminal: Any):
        self._terminal = terminal

    def __call__(self) -> Any:
        return get_class_type(self._terminal)()

    def __repr__(self) -> str:
        return self._terminal


class LiteralOp(AOp):
    def __init__(self, literal: Any):
        self._literal = literal

    def __call__(self) -> Any:
        return self._literal

    def __repr__(self) -> str:
        return f"Literal({self._literal})"


class GroupOp(AOp):
    def __init__(self, left: Any, right: Any) -> None:
        self._left = left
        self._right = right

    def __call__(self) -> Dict:
        return {**self._left(), **self._right()}

    def __repr__(self) -> str:
        return f"Group({self._left}, {self._right})"


class AssignOp(AOp):
    def __init__(self, left: Any, right: Any) -> None:
        self._left = left
        self._right = right

    def __call__(self) -> Dict:
        return {self._left: self._right()}

    def __repr__(self) -> str:
        return f"Assign({self._left}, {self._right})"


class ArrayOp(AOp):
    def __init__(self, inner_type: Any) -> None:
        self._inner = inner_type

    def __call__(self) -> ArrayType:
        return ArrayType(self._inner())

    def __repr__(self) -> str:
        return f"Array({self._inner})"


class StructOp(AOp):
    def __init__(self, inner_type: Any) -> None:
        self._inner = inner_type

    def __call__(self) -> StructType:
        return StructType(self._inner())

    def __repr__(self) -> str:
        return f"Struct({self._inner})"


def validate_has_attribute(anura_type: IType, field: str) -> None:
    type_name = anura_type.__class__.__name__
    if not hasattr(anura_type, field):
        raise AttributeError(f"Attribute '{field}' not found in {type_name}")


def convert_to_primitive_type(value: str) -> APrimitiveType:
    if value not in list(PrimitiveType):
        raise ValueError(f"'{value}' is not a PrimitiveType")
    return getattr(types, f"{normalize_name(value)}Type")()


def convert_value_to_field_type(field_type: Type, value: str) -> Any:
    if is_builtin_type(field_type):
        return convert_to_builtin_type(field_type, value)

    if issubclass(field_type, APrimitiveType):
        return convert_to_primitive_type(value)

    raise ValueError(f"Unknown {field_type}")


class OptionOp(AOp):
    def __init__(self, left: Any, right: Any) -> None:
        self._left = left
        self._right = right

    @staticmethod
    def add_options_to_type(anura_type: IType, options: Dict[str, str]) -> IType:
        for field, value in options.items():
            validate_has_attribute(anura_type, field)

            field_type = type(getattr(anura_type, field))
            if not isinstance(value, field_type):
                value = convert_value_to_field_type(field_type, value)

            setattr(anura_type, field, value)

        return anura_type

    def __call__(self) -> IType:
        return self.add_options_to_type(self._left(), self._right())

    def __repr__(self):
        return f"Assign({self._left}, {self._right})"


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
    p[0] = GroupOp(p[1], p[3])


def p_expression(p):
    """
    expression : ID ASSIGN type
    """
    p[0] = AssignOp(p[1], p[3])


def p_type_with_option(p):
    """
    type : type LPAREN option RPAREN
    """
    p[0] = OptionOp(p[1], p[3])


def p_type(p):
    """
    type : TYPE
    """
    p[0] = TerminalOp(p[1])


def p_type_struct(p):
    """
    type : LBRACE expression RBRACE
    """
    p[0] = StructOp(p[2])


def p_type_array(p):
    """
    type : type LSQUARE RSQUARE
    """
    p[0] = ArrayOp(p[1])


def p_option_comma(p):
    """
    option : option COMMA option
    """
    p[0] = GroupOp(p[1], p[3])


def p_option_assign(p):
    """
    option : ID ASSIGN VALUE
    """
    p[0] = AssignOp(p[1], LiteralOp(p[3]))


def p_error(p):
    print("Syntax error in input!")
    raise ParsingError(p)


# Build the lexer object
lexer = lex()

# Build the parser
parser = yacc()


def parse(seq: Sequence[Any]) -> Dict[str, Any]:
    ast = parser.parse(seq)
    return ast.build()
