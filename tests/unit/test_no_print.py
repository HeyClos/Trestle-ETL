"""Smoke test: the `trestle_etl` package must not contain `print(` calls.

Requirement 12.1 mandates that the pipeline use the Python `logging` module for
all output and SHALL NOT use `print` statements for operational messages. This
test walks the package source tree and AST-parses every `.py` file to look for
`ast.Call` nodes whose `func` is an `ast.Name(id="print")`. Using the AST
instead of a raw grep avoids false positives from the word "print" appearing
inside comments or string literals (for example in docstrings or SQL).
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent / "trestle_etl"


def _find_print_calls(path: Path) -> list[tuple[Path, int]]:
    """Return (path, lineno) for every top-level `print(...)` call in the file."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls: list[tuple[Path, int]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
        ):
            calls.append((path, node.lineno))
    return calls


def test_no_print_calls_in_package() -> None:
    assert PACKAGE_ROOT.is_dir(), f"package root not found: {PACKAGE_ROOT}"

    violations: list[tuple[Path, int]] = []
    for py_file in PACKAGE_ROOT.rglob("*.py"):
        violations.extend(_find_print_calls(py_file))

    assert not violations, (
        "Found print() calls in trestle_etl package (Requirement 12.1 forbids "
        f"print for operational messages; use logging instead): {violations}"
    )
