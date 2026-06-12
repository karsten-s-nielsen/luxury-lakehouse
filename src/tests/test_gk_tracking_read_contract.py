"""App-expected GK mart columns must exist in the dbt contract (read-side parity, ADR-051).

The ADR-002 §4 writer/DDL parity pattern, applied READ-side: the Taipy app declares its expected
mart columns as constants in ``queries.gk_tracking``; this test asserts they are a subset of the
``_marts__models.yml`` contracts, so a mart column rename fails CI here instead of silently in
the deployed Space. Path setup follows the repo's established per-test-file sys.path-insert convention for
``hf_taipy_app/src`` imports (see test_conversion_funnel.py + the pyright extraPaths note in
pyproject.toml) — checked per the cross-session review LOW note: the conftest does NOT cover it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

_YML = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "_marts__models.yml"


def _contract_columns(model_name: str) -> set[str]:
    doc = yaml.safe_load(_YML.read_text(encoding="utf-8"))
    model = next(m for m in doc["models"] if m["name"] == model_name)
    return {c["name"] for c in model["columns"]}


def test_actions_query_columns_subset_of_contract():
    from queries.gk_tracking import GK_ACTIONS_COLUMNS

    missing = set(GK_ACTIONS_COLUMNS) - _contract_columns("fct_gk_tracking_actions")
    assert not missing, f"app expects columns absent from the dbt contract: {missing}"


def test_stats_query_columns_subset_of_contract():
    from queries.gk_tracking import GK_STATS_COLUMNS

    missing = set(GK_STATS_COLUMNS) - _contract_columns("fct_gk_tracking_stats")
    assert not missing, f"app expects columns absent from the stats contract: {missing}"


def test_preset_columns_subset_of_stats_contract():
    from queries.gk_tracking import PRESET_COLUMN

    missing = set(PRESET_COLUMN.values()) - _contract_columns("fct_gk_tracking_stats")
    assert not missing, f"PRESET_COLUMN names absent from the stats contract: {missing}"
