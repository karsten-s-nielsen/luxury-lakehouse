"""XG1-RETIRE UI migration regression test.

Asserts hf_taipy_app/src/state/shot_map.py references v2 columns
(xg_set_encoder, xg_ci_lower, xg_ci_upper) and NOT v1 columns
(xg_logistic, xg_gradient_boosted).

AST-walk-based to survive whitespace + formatting differences.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SHOT_MAP_PATH = REPO_ROOT / "hf_taipy_app" / "src" / "state" / "shot_map.py"

_V1_FORBIDDEN = ("xg_logistic", "xg_gradient_boosted")
_V2_REQUIRED = ("xg_set_encoder", "xg_ci_lower", "xg_ci_upper")


def _string_constants(tree: ast.AST) -> set[str]:
    """All string literals in an AST."""
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.add(node.value)
    return out


def test_shot_map_has_no_v1_columns() -> None:
    """No v1 column names should appear as string literals in shot_map.py."""
    tree = ast.parse(SHOT_MAP_PATH.read_text(encoding="utf-8"))
    strings = _string_constants(tree)
    leaks = [v1 for v1 in _V1_FORBIDDEN if any(v1 in s for s in strings)]
    assert not leaks, (
        f"v1 column reference(s) found in {SHOT_MAP_PATH.relative_to(REPO_ROOT)}: {leaks}. "
        "XG1-RETIRE migration incomplete — replace with v2 columns "
        "(xg_set_encoder + xg_ci_lower + xg_ci_upper)."
    )


def test_shot_map_has_v2_columns() -> None:
    """At least one of the v2 column names must appear in shot_map.py."""
    text = SHOT_MAP_PATH.read_text(encoding="utf-8")
    found = [v2 for v2 in _V2_REQUIRED if v2 in text]
    assert found, (
        f"None of the v2 columns {_V2_REQUIRED} found in "
        f"{SHOT_MAP_PATH.relative_to(REPO_ROOT)}. "
        "XG1-RETIRE migration incomplete — Shot Map needs to display v2 predictions."
    )
