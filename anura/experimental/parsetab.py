# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = "3.10"

_lr_method = "LALR"

_lr_signature = (
    "leftCOMMAleftASSIGNASSIGN COMMA ID LBRACE LPAREN LSQUARE RBRACE RPAREN RSQUARE TYPE VALUE\n    calc : expression\n"
    "         | empty\n    \n    empty :\n    \n    expression : expression COMMA expression\n    \n    expression : ID"
    " ASSIGN type\n    \n    type : type LPAREN option RPAREN\n    \n    type : TYPE\n    \n    type : LBRACE"
    " expression RBRACE\n    \n    type : type LSQUARE RSQUARE\n    \n    option : option COMMA option\n    \n   "
    " option : ID ASSIGN VALUE\n    "
)

_lr_action_items = {
    "ID": (
        [
            0,
            5,
            10,
            11,
            19,
        ],
        [
            4,
            4,
            4,
            15,
            15,
        ],
    ),
    "$end": (
        [
            0,
            1,
            2,
            3,
            7,
            8,
            9,
            16,
            17,
            18,
        ],
        [
            -3,
            0,
            -1,
            -2,
            -4,
            -5,
            -7,
            -9,
            -8,
            -6,
        ],
    ),
    "COMMA": (
        [
            2,
            7,
            8,
            9,
            13,
            14,
            16,
            17,
            18,
            21,
            22,
        ],
        [
            5,
            -4,
            -5,
            -7,
            5,
            19,
            -9,
            -8,
            -6,
            -10,
            -11,
        ],
    ),
    "ASSIGN": (
        [
            4,
            15,
        ],
        [
            6,
            20,
        ],
    ),
    "TYPE": (
        [
            6,
        ],
        [
            9,
        ],
    ),
    "LBRACE": (
        [
            6,
        ],
        [
            10,
        ],
    ),
    "RBRACE": (
        [
            7,
            8,
            9,
            13,
            16,
            17,
            18,
        ],
        [
            -4,
            -5,
            -7,
            17,
            -9,
            -8,
            -6,
        ],
    ),
    "LPAREN": (
        [
            8,
            9,
            16,
            17,
            18,
        ],
        [
            11,
            -7,
            -9,
            -8,
            -6,
        ],
    ),
    "LSQUARE": (
        [
            8,
            9,
            16,
            17,
            18,
        ],
        [
            12,
            -7,
            -9,
            -8,
            -6,
        ],
    ),
    "RSQUARE": (
        [
            12,
        ],
        [
            16,
        ],
    ),
    "RPAREN": (
        [
            14,
            21,
            22,
        ],
        [
            18,
            -10,
            -11,
        ],
    ),
    "VALUE": (
        [
            20,
        ],
        [
            22,
        ],
    ),
}

_lr_action = {}
for _k, _v in _lr_action_items.items():
    for _x, _y in zip(_v[0], _v[1]):
        if _x not in _lr_action:
            _lr_action[_x] = {}
        _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {
    "calc": (
        [
            0,
        ],
        [
            1,
        ],
    ),
    "expression": (
        [
            0,
            5,
            10,
        ],
        [
            2,
            7,
            13,
        ],
    ),
    "empty": (
        [
            0,
        ],
        [
            3,
        ],
    ),
    "type": (
        [
            6,
        ],
        [
            8,
        ],
    ),
    "option": (
        [
            11,
            19,
        ],
        [
            14,
            21,
        ],
    ),
}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
    for _x, _y in zip(_v[0], _v[1]):
        if _x not in _lr_goto:
            _lr_goto[_x] = {}
        _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
    ("S' -> calc", "S'", 1, None, None, None),
    ("calc -> expression", "calc", 1, "p_calc", "parser.py", 134),
    ("calc -> empty", "calc", 1, "p_calc", "parser.py", 135),
    ("empty -> <empty>", "empty", 0, "p_empty", "parser.py", 142),
    ("expression -> expression COMMA expression", "expression", 3, "p_expression_comma", "parser.py", 149),
    ("expression -> ID ASSIGN type", "expression", 3, "p_expression", "parser.py", 156),
    ("type -> type LPAREN option RPAREN", "type", 4, "p_type_with_option", "parser.py", 163),
    ("type -> TYPE", "type", 1, "p_type", "parser.py", 170),
    ("type -> LBRACE expression RBRACE", "type", 3, "p_type_struct", "parser.py", 177),
    ("type -> type LSQUARE RSQUARE", "type", 3, "p_type_array", "parser.py", 184),
    ("option -> option COMMA option", "option", 3, "p_option_comma", "parser.py", 191),
    ("option -> ID ASSIGN VALUE", "option", 3, "p_option_assign", "parser.py", 198),
]