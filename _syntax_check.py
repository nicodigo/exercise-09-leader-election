import ast
import sys

files = [
    ("src/election.py", "election.py"),
    ("src/app.py", "app.py"),
]

all_ok = True
for path, name in files:
    try:
        with open(path) as f:
            ast.parse(f.read())
        print(f"{name}: syntax OK")
    except SyntaxError as e:
        print(f"{name}: SYNTAX ERROR - {e}")
        all_ok = False

if all_ok:
    print("All syntax checks passed")
else:
    sys.exit(1)
