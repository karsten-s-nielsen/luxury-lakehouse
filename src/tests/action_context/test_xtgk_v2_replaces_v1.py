"""xT-GK v1 -> v2 reconciliation guard (spec §7.4 / ADR xtgk-v2 replaces v1).

Two-tier contract (review-3 H-1):
  * The 16 v1 xt_gk METRIC columns are RETIRED from the DRAIN schema (RESULT_COLUMNS + ACTION_CONTEXT_DDL)
    — the AC drain no longer emits them.
  * ``gk_completion`` and the 4 resolved-coordinate columns (the v2 writer's geometry bridge) are KEPT.
  * The 6 v2 columns live ONLY in the ``fct_action_context`` MART contract (fed by the writer's bronze via
    a LEFT JOIN), NEVER in the drain schema/golden — the ADR-013 writer-join pattern.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from analytics.action_context.schema import ACTION_CONTEXT_DDL, RESULT_COLUMNS

# Parse the DDL into an exact column-name set (substring `in` would false-match e.g. "xt_gk" inside the
# KEPT "xt_gk_origin_x"). Same shape as test_action_context_schema_parity._parse_ddl_columns.
_DDL_COLUMNS = set(re.findall(r"(\w+)\s+\w+", ACTION_CONTEXT_DDL))

# The 16 retired v1 metric columns (spec §7.4 — includes the 5 philosophy presets with no v2 successor).
_RETIRED_V1_COLUMNS = [
    "xt_gk",
    "xt_gk_possession",
    "xt_gk_counter",
    "xt_gk_direct",
    "xt_gk_high_press",
    "xt_gk_low_block",
    "xt_gk_base",
    "xt_gk_pev",
    "xt_gk_rav",
    "xt_gk_dzv",
    "xt_gk_pressure",
    "xt_gk_origin_source",
    "xt_gk_dest_source",
    "xt_gk_origin_confidence",
    "xt_gk_completion_variant",
    "xt_gk_completion_source",
]

# KEPT: the resolved-coordinate geometry bridge the v2 writer reads + the distinct completion metric.
_KEPT_COLUMNS = ["xt_gk_origin_x", "xt_gk_origin_y", "xt_gk_dest_x", "xt_gk_dest_y", "gk_completion"]

# The 6 v2 mart-join columns (5 value + provenance) — writer-scored, NOT drain-native.
_V2_MART_COLUMNS = [
    "xt_gk_v2_position",
    "xt_gk_v2_pev",
    "xt_gk_v2_retention_loss",
    "xt_gk_v2_dzv",
    "xt_gk_v2",
    "gk_geometry_source",
]

_MARTS_YML = Path(__file__).resolve().parents[3] / "dbt_project" / "models" / "marts" / "_marts__models.yml"


def _fct_action_context_contract_columns() -> set[str]:
    doc = yaml.safe_load(_MARTS_YML.read_text(encoding="utf-8"))
    for model in doc["models"]:
        if model["name"] == "fct_action_context":
            return {c["name"] for c in model.get("columns", [])}
    raise AssertionError("fct_action_context model not found in _marts__models.yml")


class TestV1Retired:
    def test_v1_metric_columns_absent_from_drain_schema(self) -> None:
        for col in _RETIRED_V1_COLUMNS:
            assert col not in RESULT_COLUMNS, f"retired v1 column still in RESULT_COLUMNS: {col}"
            assert col not in _DDL_COLUMNS, f"retired v1 column still in ACTION_CONTEXT_DDL: {col}"

    def test_v1_metric_columns_absent_from_mart_contract(self) -> None:
        contract = _fct_action_context_contract_columns()
        for col in _RETIRED_V1_COLUMNS:
            assert col not in contract, f"retired v1 column still in fct_action_context contract: {col}"


class TestKept:
    def test_gk_completion_and_geometry_bridge_kept_in_drain(self) -> None:
        for col in _KEPT_COLUMNS:
            assert col in RESULT_COLUMNS, f"KEPT column missing from RESULT_COLUMNS: {col}"
            assert col in _DDL_COLUMNS, f"KEPT column missing from ACTION_CONTEXT_DDL: {col}"


class TestV2MartJoin:
    def test_v2_columns_present_in_mart_contract_only(self) -> None:
        contract = _fct_action_context_contract_columns()
        for col in _V2_MART_COLUMNS:
            assert col in contract, f"v2 mart-join column missing from fct_action_context contract: {col}"
            # v2 is writer-scored via a mart LEFT JOIN — it must NOT be a drain-native column.
            assert col not in RESULT_COLUMNS, f"v2 column wrongly registered as a drain column: {col}"
