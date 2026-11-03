"""No catalog publish outside the checkpoint policy and the maintenance operations (T3 spec D1/D2; plan round 3 Task 9;
Reviewers A and B T3-PLAN-02, B T3-PLAN-12).

A catalog publish is an attribute call `.commit(...)` with at least one argument (`cat.commit("in_run")`); a DuckDB
`conn.commit()` takes none. Every publish of an ingestion run goes through `CheckpointPolicy`, so it runs the integrity
guard and the grace-aware pruning. The allowlist names functions, not files, so a publish added to `run_cleanup` (in
the same module as `sweep_now`) fails this gate."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Final

SRC: Final = Path(__file__).resolve().parents[2] / "src" / "lakehouse"

# (module path relative to src/lakehouse, enclosing function) -> why it may publish
ALLOWED: Final[dict[tuple[str, str], str]] = {
    ("runs/checkpoint.py", "_publish"): "the checkpoint policy: every ingestion-run publish",
    ("catalog/cleanup.py", "sweep_now"): "the maintenance sweep (`lakehouse sweep --allow-large`), not a run",
    ("catalog/restore.py", "_restore_held"): "a restore, not a run",
    ("catalog/relocate.py", "_relocate_held"): "a relocate, not a run",
}


def _publishes(path: Path) -> list[tuple[str, int]]:
    """Each catalog-publish call in the module, with its enclosing function name and line."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[str, int]] = []

    def visit(node: ast.AST, function: str) -> None:
        for child in ast.iter_child_nodes(node):
            name = child.name if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) else function
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "commit"
                and (child.args or child.keywords)
            ):
                found.append((function, child.lineno))
            visit(child, name)

    visit(tree, "<module>")
    return found


def test_no_catalog_publish_outside_the_allowlist() -> None:
    offenders = [
        f"{path.relative_to(SRC).as_posix()}:{line} (in {function})"
        for path in sorted(SRC.rglob("*.py"))
        for function, line in _publishes(path)
        if (path.relative_to(SRC).as_posix(), function) not in ALLOWED
    ]
    assert offenders == [], f"catalog publishes outside the checkpoint policy: {offenders}"


def test_the_allowlist_has_no_stale_entry() -> None:
    present = {
        (path.relative_to(SRC).as_posix(), function)
        for path in sorted(SRC.rglob("*.py"))
        for function, _ in _publishes(path)
    }
    assert set(ALLOWED) <= present, f"allowlist entries with no publish: {sorted(set(ALLOWED) - present)}"
