# src/tests/test_action_context_schema_parity.py
"""Schema parity sentinel for action_context.py.

Ensures _RESULT_COLUMNS, _ACTION_CONTEXT_DDL, and the dbt contract
stay in sync. Same pattern as test_cost_hook_integration.py (ADR-002 §4).
"""

from __future__ import annotations

import re

from analytics.action_context.schema import (
    ACTION_CONTEXT_DDL as _ACTION_CONTEXT_DDL,
)
from analytics.action_context.schema import (
    RESULT_COLUMNS as _RESULT_COLUMNS,
)

_DDL_COL_RE = re.compile(r"(\w+)\s+\w+")


def _parse_ddl_columns(ddl: str) -> list[str]:
    """Extract column names from a Spark DDL string."""
    return _DDL_COL_RE.findall(ddl)


def test_result_columns_match_ddl() -> None:
    """_RESULT_COLUMNS and _ACTION_CONTEXT_DDL must list the same columns in order."""
    ddl_cols = _parse_ddl_columns(_ACTION_CONTEXT_DDL)
    assert ddl_cols == _RESULT_COLUMNS, (
        f"Column mismatch between _RESULT_COLUMNS ({len(_RESULT_COLUMNS)} cols) "
        f"and _ACTION_CONTEXT_DDL ({len(ddl_cols)} cols).\n"
        f"In RESULT but not DDL: {set(_RESULT_COLUMNS) - set(ddl_cols)}\n"
        f"In DDL but not RESULT: {set(ddl_cols) - set(_RESULT_COLUMNS)}"
    )


def test_result_columns_no_duplicates() -> None:
    """No duplicate column names in _RESULT_COLUMNS."""
    seen: set[str] = set()
    dupes: list[str] = []
    for col in _RESULT_COLUMNS:
        if col in seen:
            dupes.append(col)
        seen.add(col)
    assert not dupes, f"Duplicate columns in _RESULT_COLUMNS: {dupes}"
