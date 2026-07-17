"""Unit tests for the pure re-derive planner (zero IO — no warehouse, no SDK)."""

from __future__ import annotations

from ingestion.rederive_planner import (
    D_REPROCESS_MODELS,
    plan_rederive,
)
from ingestion.refresh_synced_tables import SYNCED_TABLES


def _triggered_models() -> set[str]:
    return {c.source_table for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def test_d_mart_plans_merge_reprocess_no_full_refresh() -> None:
    steps = plan_rederive({"fct_action_values"}, [10, 20])
    assert len(steps) == 1
    step = steps[0]
    assert step.action == "D"
    assert step.full_refresh is False
    assert step.synced_table == "fct_action_values_synced"
    assert step.dbt_vars == {"reprocess_match_ids": [10, 20]}  # no enable vars injected


def test_table_mart_plans_plain_rebuild_t() -> None:
    # The `table` marts use the T (plain rebuild) action — no synced delete, no --full-refresh
    # (the daily plain-build path; since the 2026-06-10 platform change the rebuild strands the
    # synced table and the ADR-041 heal recreates it — ADR-043 amendment 2). No vars (no enable-var
    # injection; dbt_project.yml defaults match production).
    for mart in ("fct_pausa_values",):
        steps = plan_rederive({mart}, [])
        assert len(steps) == 1, mart
        step = steps[0]
        assert step.action == "T", mart
        assert step.full_refresh is False, mart
        assert step.dbt_vars == {}, mart


def test_merge_all_incremental_mart_plans_b() -> None:
    steps = plan_rederive({"fct_passes"}, [])
    assert len(steps) == 1
    step = steps[0]
    assert step.action == "B"
    assert step.full_refresh is True
    assert step.dbt_vars == {"allow_triggered_full_refresh": True}


def test_rebuild_routes_a_d_mart_through_b() -> None:
    # --rebuild: full-rebuild a D mart (schema/contract change) via the B path.
    steps = plan_rederive({"fct_action_values"}, [10], rebuild=True)
    assert len(steps) == 1 and steps[0].action == "B" and steps[0].full_refresh is True
    assert steps[0].dbt_vars == {"allow_triggered_full_refresh": True}


def test_rebuild_routes_a_table_mart_through_b() -> None:
    # --rebuild of a table mart forces the heavy delete->recreate (e.g. to refresh synced schema).
    steps = plan_rederive({"fct_pausa_values"}, [], rebuild=True)
    assert len(steps) == 1 and steps[0].action == "B"


def test_snapshot_mart_is_skipped() -> None:
    # fct_shots is SNAPSHOT (immune) — must produce no step.
    assert plan_rederive({"fct_shots"}, [1]) == []


def test_non_synced_model_is_skipped() -> None:
    assert plan_rederive({"int_running_score"}, [1]) == []


def test_d_then_t_then_b_ordering() -> None:
    steps = plan_rederive({"fct_passes", "fct_pausa_values", "fct_action_values"}, [5])
    assert [s.action for s in steps] == ["D", "T", "B"]


def test_every_d_model_is_triggered() -> None:
    assert D_REPROCESS_MODELS <= _triggered_models()
