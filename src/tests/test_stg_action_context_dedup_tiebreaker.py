"""stg_action_context__values dedup must keep a stable, documented ORDER BY.

Source-text pin (PR-1 TC-1 retirement, Task 2). AC bronze is 0-dup by M13 work-unit
ownership; the real regression guard is the bronze-source singular test
assert_action_context_bronze_no_divergent_dups. This pin just prevents a future edit
from silently dropping the ORDER BY tiebreaker column from the dedup window.
"""

from pathlib import Path

_STG = Path("dbt_project/models/staging/action_context/stg_action_context__values.sql")


def test_dedup_orders_by_ingested_at_then_action_id() -> None:
    src = _STG.read_text(encoding="utf-8")
    assert "order by _ingested_at desc, action_id" in src, (
        "stg_action_context__values dedup ORDER BY tiebreaker was dropped"
    )


def test_dedup_points_at_the_real_bronze_guard() -> None:
    src = _STG.read_text(encoding="utf-8")
    assert "assert_action_context_bronze_no_divergent_dups" in src, (
        "comment pointing at the real bronze zero-dup guard was removed"
    )
