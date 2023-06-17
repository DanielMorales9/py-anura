from anura.experimental.parser import parser

while True:
    try:
        s = input(">>")
    except EOFError:
        break
    print(parser.parse(s))
