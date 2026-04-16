"""Structural tests for dbt_project/models/marts/fct_workflow_costs.sql.

These tests parse the .sql file directly and assert the post-hook logic
has the fix shape. They do NOT require a live warehouse — pure text
pattern matching.

D65 fix 2026-04-15: the post-hook 1 watermark was the per-date
`MAX(usage_date) WHERE attributed_cost_usd IS NOT NULL + INTERVAL 1 DAY`
pattern, which over-pruned because a single row landing with billing
advanced the watermark. Two EXISTS-correlated alternatives surfaced edge
cases (NULL workflow_id false positives, sibling pruning under correlation).
The shipped fix is simple time-based retention with a 7-day window — the
same pattern as post-hook 2.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODEL = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_workflow_costs.sql"


@pytest.fixture(scope="module")
def model_sql() -> str:
    return _MODEL.read_text(encoding="utf-8")


def test_fct_workflow_costs_post_hook_does_not_use_max_usage_date_watermark(model_sql: str) -> None:
    """D65 regression guard: post-hook 1 must NOT use the per-date
    `MAX(usage_date) WHERE attributed_cost_usd IS NOT NULL + INTERVAL 1 DAY`
    watermark that advances monotonically per-row and over-prunes."""
    old_pattern = re.compile(
        r"MAX\s*\(\s*usage_date\s*\)\s*[,)]?\s*.*INTERVAL\s+1\s+DAY",
        re.IGNORECASE | re.DOTALL,
    )
    assert not old_pattern.search(model_sql), (
        "D65 regression: fct_workflow_costs.sql still uses "
        "`MAX(usage_date) + INTERVAL 1 DAY` watermark. Use simple "
        "time-based retention `ended_at < CURRENT_TIMESTAMP - INTERVAL N DAYS` "
        "instead — see post-hook 2 for the pattern."
    )


def test_fct_workflow_costs_post_hook_1_uses_time_based_retention(model_sql: str) -> None:
    """D65 fix: post-hook 1 must use simple time-based retention against
    `ended_at` so per-row billing arrivals cannot influence the prune
    decision. Pattern: `ended_at < CURRENT_TIMESTAMP - INTERVAL N DAYS`."""
    first_post_hook_match = re.search(
        r'"DELETE FROM\s+\{\{\s*this\.database\s*\}\}\.observability\.workflow_cost_live.*?"',
        model_sql,
        re.DOTALL,
    )
    assert first_post_hook_match is not None, "Could not locate post-hook 1"
    first_post_hook = first_post_hook_match.group(0)

    # Must filter on state != RUNNING (we only prune completed rows here)
    assert "state != 'RUNNING'" in first_post_hook, (
        "post-hook 1 must filter `state != 'RUNNING'` — RUNNING rows are "
        "handled by post-hook 2 with a different window."
    )

    # Must guard against NULL ended_at
    assert "ended_at IS NOT NULL" in first_post_hook, (
        "post-hook 1 must filter `ended_at IS NOT NULL` so RUNNING rows with no end time are not accidentally caught."
    )

    # Must use the time-based retention shape
    time_based_pattern = re.compile(
        r"ended_at\s*<\s*CURRENT_TIMESTAMP\s*-\s*INTERVAL\s+\d+\s+DAYS?",
        re.IGNORECASE,
    )
    assert time_based_pattern.search(first_post_hook), (
        "D65 fix: post-hook 1 must use time-based retention "
        "`ended_at < CURRENT_TIMESTAMP - INTERVAL N DAYS`. Two prior "
        "EXISTS-correlated attempts surfaced edge cases — see the doc "
        "comment block in the model for the rationale."
    )

    # Must NOT use EXISTS or NOT EXISTS (we explicitly chose time-based,
    # and presence of EXISTS would mean someone reverted the simpler approach)
    assert "EXISTS" not in first_post_hook, (
        "post-hook 1 must NOT use EXISTS-correlated logic. The shipped "
        "fix is time-based retention. EXISTS introduces NULL/correlation "
        "edge cases that the cycle explicitly rejected."
    )


def test_fct_workflow_costs_post_hook_2_unchanged(model_sql: str) -> None:
    """Post-hook 2 (orphaned RUNNING rows >24h) must stay as-is —
    D65 only touches post-hook 1 logic."""
    assert "INTERVAL 24 HOURS" in model_sql, (
        "Post-hook 2 (orphaned RUNNING >24h cleanup) was removed or changed. D65 only touches post-hook 1."
    )
    assert "state = 'RUNNING'" in model_sql, "Post-hook 2 state filter changed. D65 only touches post-hook 1."


def test_fct_workflow_costs_post_hook_1_retention_exceeds_billing_lag(model_sql: str) -> None:
    """D65 fix: post-hook 1 retention window must exceed the ~1-day billing
    lag by a safety margin. A minimum of 3 days (3x lag) prevents a future
    developer from silently shrinking the retention below the safe floor.

    Why: billing data arrives via system.billing.usage with ~1-day lag
    (per the model's doc comment block). A retention <3 days would prune
    warm-tier rows before their billing has reliably arrived, losing the
    cost-attribution context that the warm tier exists to provide. The
    shipped 7-day window is 7x the typical lag.
    """
    m = re.search(
        r"ended_at\s*<\s*CURRENT_TIMESTAMP\s*-\s*INTERVAL\s+(\d+)\s+DAYS?",
        model_sql,
        re.IGNORECASE,
    )
    assert m is not None, "post-hook 1 must use time-based retention `ended_at < CURRENT_TIMESTAMP - INTERVAL N DAYS`"
    days = int(m.group(1))
    assert days >= 3, (
        f"post-hook 1 retention is {days} days — billing lag is ~1 day, "
        "retention must be >= 3x lag (3 days minimum) to provide a safety "
        "margin. See fct_workflow_costs.sql doc comment block for rationale."
    )
