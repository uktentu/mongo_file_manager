import ast
import sys
from pathlib import Path


class Python39Validator(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        self.errors = []

    def visit_BinOp(self, node):
        # Check for X | Y type hints (PEP 604)
        if isinstance(node.op, ast.BitOr):
            self.errors.append((node.lineno, "PEP 604 type unions (X | Y) are not supported in Python 3.9."))
        self.generic_visit(node)

    def visit_Match(self, node):
        # Check for pattern matching (PEP 634, Python 3.10+)
        self.errors.append((node.lineno, "Pattern matching (match/case) is not supported in Python 3.9."))
        self.generic_visit(node)

    def visit_Subscript(self, node):
        # Check for PEP 585 built-in generic types (e.g., list[dict], dict[str, Any])
        if isinstance(node.value, ast.Name):
            built_in_generics = {"list", "dict", "tuple", "set", "frozenset"}
            if node.value.id in built_in_generics:
                self.errors.append((node.lineno, f"PEP 585 built-in generic '{node.value.id}[]' is strongly discouraged in Python 3.9 annotations. Use typing.{node.value.id.capitalize()}[] instead."))
        self.generic_visit(node)


def scan_file(filepath: Path) -> list:
    try:
        content = filepath.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(filepath))
    except SyntaxError as e:
        return [(e.lineno, f"Syntax error: {e.msg}")]
    except Exception as e:
        return [(0, f"Could not parse file: {e}")]

    validator = Python39Validator(str(filepath))
    validator.visit(tree)
    return validator.errors


def main():
    repo_root = Path(__file__).resolve().parent.parent
    src_dir = repo_root / "src"

    if not src_dir.exists():
        print(f"Error: Could not find src directory at {src_dir}")
        sys.exit(1)

    py_files = list(src_dir.rglob("*.py"))
    
    # Also include the script itself and any others in repo_root directly (setup.py, root conftest etc)
    py_files.extend(list(repo_root.glob("*.py")))
    
    total_errors = 0
    files_with_errors = 0

    print(f"Scanning {len(py_files)} Python files for Python 3.10+ syntax features...")

    for py_file in py_files:
        errors = scan_file(py_file)
        if errors:
            print(f"\n❌ {py_file.relative_to(repo_root)}:")
            files_with_errors += 1
            for lineno, msg in errors:
                print(f"   Line {lineno}: {msg}")
                total_errors += 1

    if total_errors == 0:
        print("\n✅ All scanned files are fully compatible with Python 3.9 syntax requirements.")
        sys.exit(0)
    else:
        print(f"\n⚠️ Found {total_errors} compatibility issue(s) across {files_with_errors} file(s).")
        sys.exit(1)


if __name__ == "__main__":
    main()
