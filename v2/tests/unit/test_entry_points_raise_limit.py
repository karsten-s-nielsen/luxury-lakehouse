"""Every entry point raises the open-file limit first (T12 spec D1; Reviewer B T12-SPEC-03).

A process that skips the raise runs at its parent's soft limit (256 under launchd), and one DuckDB scan of a table
with a few hundred files then fails with "Too many open files". The entry points are the `[project.scripts]` targets
and every module with a `__main__` guard; each must call `raise_open_file_limit` as its first statement, and a new
entry point fails this test until it does.
"""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path
from typing import Final

ROOT: Final = Path(__file__).resolve().parents[2]
SRC: Final = ROOT / "src" / "lakehouse"
EXPECTED: Final = {"cli.py", "mask.py", "schedule/windows.py", "web/__main__.py"}


def _is_main_guard(test: ast.expr) -> bool:
    return (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == "__name__"
        and any(isinstance(c, ast.Constant) and c.value == "__main__" for c in test.comparators)
    )


def _calls_raise(statement: ast.stmt) -> bool:
    for node in ast.walk(statement):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ""
            if name == "raise_open_file_limit":
                return True
    return False


def _first_real(body: list[ast.stmt]) -> ast.stmt:
    """The first statement after a docstring."""
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        return body[1]
    return body[0]


def _guarded_modules() -> dict[str, ast.Module]:
    found: dict[str, ast.Module] = {}
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if any(isinstance(node, ast.If) and _is_main_guard(node.test) for node in tree.body):
            found[path.relative_to(SRC).as_posix()] = tree
    return found


def _main_function(tree: ast.Module) -> ast.FunctionDef | None:
    return next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"), None)


def test_the_entry_points_are_the_known_ones() -> None:
    assert set(_guarded_modules()) == EXPECTED
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
    assert scripts == {"lakehouse": "lakehouse.cli:main"}


def test_every_entry_point_raises_the_limit_first() -> None:
    missing: list[str] = []
    for name, tree in _guarded_modules().items():
        main = _main_function(tree)
        if main is not None:
            if not _calls_raise(_first_real(main.body)):
                missing.append(f"{name}: main()")
        else:
            guard = next(n for n in tree.body if isinstance(n, ast.If) and _is_main_guard(n.test))
            if not _calls_raise(guard.body[0]):
                missing.append(f"{name}: the __main__ block")
    assert missing == [], f"entry points that do not raise the open-file limit first: {missing}"
