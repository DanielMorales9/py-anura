from typing import Any, Dict, Sequence

from ply.lex import lex
from ply.yacc import yacc

# All tokens must be named in advance.
from anura.constants import MetaType

# --- Tokenizer

tokens = ("ID", "ASSIGN", "TYPE", "COMMA", "LBRACE", "RBRACE")

# Token matching rules are written as regex
t_LBRACE = r"\{"
t_RBRACE = r"\}"
t_ASSIGN = r"="
t_COMMA = r","
t_TYPE = r"|".join(MetaType)
t_ID = r"[a-zA-Z_][a-zA-Z_0-9]*"


# Ignored token with an action associated with it
def t_ignore_newline(t):
    r"\n+"
    t.lexer.lineno += t.value.count("\n")


# Error handler for illegal characters
def t_error(t):
    print(f"Illegal character {t.value[0]!r}")
    t.lexer.skip(1)


# Build the lexer object
lexer = lex()


def p_expression(p):
    """
    expression : ID ASSIGN TYPE
    """
    p[0] = ("assign", p[2], p[1], p[3])


def p_expression_comma(p):
    "expression : expression COMMA expression"
    p[0] = ("group", p[1], p[3])


def p_expression_group(p):
    "expression : LBRACE expression RBRACE"
    p[0] = p[2]


def p_error(p):
    print("Syntax error in input!")


# Build the parser
parser = yacc()


def parse_ast(ast: Sequence[Any]) -> Dict[str, Any]:
    expr = ast[0]
    if expr == "group":
        left = parse_ast(ast[1])
        right = parse_ast(ast[2])
        return {**left, **right}
    elif expr == "assign":
        return {ast[2]: ast[3]}
    else:
        raise TypeError(f"Unsopported expr {expr}")


def parse(seq: Sequence[Any]) -> Dict[str, Any]:
    ast = parser.parse(seq)
    return parse_ast(ast)
