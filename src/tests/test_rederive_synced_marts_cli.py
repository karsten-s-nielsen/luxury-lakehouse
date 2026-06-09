"""Smoke tests for the rederive executor's pure helpers (no live dbt/SDK)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "rederive_synced_marts", Path(__file__).resolve().parents[2] / "scripts" / "rederive_synced_marts.py"
)
assert _SPEC is not None and _SPEC.loader is not None
mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mod)


def test_parse_model_names_strips_dbt_ls_noise() -> None:
    raw = "fct_action_values\nfct_pausa_values\n\nSome log line that is not a model\n"
    names = mod._parse_model_names(raw)
    assert "fct_action_values" in names
    assert "fct_pausa_values" in names


def test_downtime_estimate_flags_large_b_marts() -> None:
    assert "window" in mod._downtime_estimate("fct_action_context", "B").lower()
    assert mod._downtime_estimate("fct_action_values", "D") == "none (in-place MERGE)"
    assert "none" in mod._downtime_estimate("fct_pausa_values", "T").lower()


def test_requires_match_ids_when_d_step_present() -> None:
    from ingestion.rederive_planner import plan_rederive

    steps = plan_rederive({"fct_action_values"}, [])
    with pytest.raises(SystemExit):
        mod._validate_match_ids(steps, match_ids=[])
