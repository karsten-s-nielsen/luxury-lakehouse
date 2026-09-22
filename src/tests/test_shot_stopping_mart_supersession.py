"""GK shot-stopping 3-mart supersession byte-compat guard (sk4118 P1 Task B1).

The silly-kicks compute_shot_stopping writer (stg_shot_stopping) RE-SOURCES the GSAA columns in
fct_gk_shot_stopping / fct_gk_shot_stopping_pooled, and ADDs the 4 _excl_penalties companions. Hyrum's
Law: every column the pre-existing contract exposed (the app + downstream marts read them) MUST still be
present after the re-source — a re-source, not a rename. This static contract guard (no live DB) asserts:

  1. every PRE-EXISTING contract column on the two GK marts is STILL present (byte-compat), and
  2. the 4 _excl_penalties companions are PRESENT on both (the additive extension landed), and
  3. the writer's sk metric surface (incl. _excl_penalties) is a subset of both mart contracts.

A producer-side column drop or rename fails CI here instead of silently in the deployed Space / the
pooled evaluative surface.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from silly_kicks.shot_stopping import SHOT_STOPPING_METRIC_COLUMNS

_YML = Path(__file__).resolve().parents[2] / "dbt_project" / "models" / "marts" / "_marts__models.yml"

# The columns the pre-B1 contract exposed on each GK mart (Hyrum: consumers depend on ALL of these).
# fct_gk_shot_stopping (pre-B1, verified against the mart contract before the re-source).
_GK_SHOT_STOPPING_PREEXISTING: frozenset[str] = frozenset(
    {
        "gk_shot_stopping_id",
        "player_key",
        "match_key",
        "competition_key",
        "season_id",
        "data_source",
        "shots_faced",
        "shots_faced_total",
        "goals_conceded_on_shots",
        "psxg_faced",
        "goals_prevented",
        "psxg_variance_sum",
        "low_sample",
        "psxg_calibration",
        "model_version",
        "platt_version",
        "normalization_version",
        "_loaded_at",
    }
)

# fct_gk_shot_stopping_pooled (pre-B1) — the page's evaluative surface (the app read-contract test
# pins goals_prevented / goals_prevented_ci_low/high / shots_faced_total / low_sample; keep ALL).
_GK_POOLED_PREEXISTING: frozenset[str] = frozenset(
    {
        "gk_pooled_id",
        "player_key",
        "competition_key",
        "season_id",
        "data_source",
        "shots_faced",
        "shots_faced_total",
        "goals_conceded_on_shots",
        "psxg_faced",
        "goals_prevented",
        "goals_prevented_ci_low",
        "goals_prevented_ci_high",
        "low_sample",
        "ranking_enabled",
        "goals_prevented_pctile",
        "_loaded_at",
        # gkdv columns (silly-kicks 4.87.0 GKDV) — LEFT-joined, part of the pre-B1 shape.
        "gkdv_delta_das_mean",
        "gkdv_delta_das_median",
        "gkdv_delta_das_n",
        "gkdv_delta_das_n_nonzero",
        "gkdv_delta_das_n_games",
        "gkdv_delta_das_gate_eligible",
        "gkdv_delta_threat_mean",
        "gkdv_delta_threat_median",
        "gkdv_delta_threat_n",
        "gkdv_delta_threat_n_nonzero",
        "gkdv_delta_threat_n_games",
        "gkdv_delta_threat_gate_eligible",
    }
)

# The 4 _excl_penalties companions the sk writer adds to both marts.
_EXCL_PEN_COLUMNS: frozenset[str] = frozenset(
    {
        "shots_faced_excl_penalties",
        "goals_conceded_excl_penalties",
        "psxg_faced_excl_penalties",
        "goals_prevented_excl_penalties",
    }
)


def _contract_columns(model_name: str) -> set[str]:
    doc = yaml.safe_load(_YML.read_text(encoding="utf-8"))
    model = next(m for m in doc["models"] if m["name"] == model_name)
    return {c["name"] for c in model["columns"]}


def test_gk_shot_stopping_preexisting_columns_preserved() -> None:
    cols = _contract_columns("fct_gk_shot_stopping")
    missing = _GK_SHOT_STOPPING_PREEXISTING - cols
    assert not missing, f"B1 re-source DROPPED pre-existing fct_gk_shot_stopping columns (Hyrum): {sorted(missing)}"


def test_gk_pooled_preexisting_columns_preserved() -> None:
    cols = _contract_columns("fct_gk_shot_stopping_pooled")
    missing = _GK_POOLED_PREEXISTING - cols
    assert not missing, (
        f"B1 re-source DROPPED pre-existing fct_gk_shot_stopping_pooled columns (Hyrum): {sorted(missing)}"
    )


def test_gk_shot_stopping_excl_penalties_added() -> None:
    cols = _contract_columns("fct_gk_shot_stopping")
    missing = _EXCL_PEN_COLUMNS - cols
    assert not missing, f"B1 _excl_penalties companions missing from fct_gk_shot_stopping contract: {sorted(missing)}"


def test_gk_pooled_excl_penalties_added() -> None:
    cols = _contract_columns("fct_gk_shot_stopping_pooled")
    missing = _EXCL_PEN_COLUMNS - cols
    assert not missing, (
        f"B1 _excl_penalties companions missing from fct_gk_shot_stopping_pooled contract: {sorted(missing)}"
    )


def test_sk_metric_surface_subset_of_gk_marts() -> None:
    """The sk compute_shot_stopping metric columns (incl. _excl_penalties) map onto both marts.

    fct_gk_shot_stopping renames sk 'goals_conceded' -> 'goals_conceded_on_shots' (pre-existing name),
    so the mapping is by the sk name with that one rename applied.
    """
    sk_cols = set(SHOT_STOPPING_METRIC_COLUMNS)
    # sk goals_conceded is materialized as goals_conceded_on_shots (B3 pairing; pre-existing name).
    expected = {("goals_conceded_on_shots" if c == "goals_conceded" else c) for c in sk_cols}
    for mart in ("fct_gk_shot_stopping", "fct_gk_shot_stopping_pooled"):
        cols = _contract_columns(mart)
        missing = expected - cols
        assert not missing, f"{mart} missing sk shot_stopping metric surface: {sorted(missing)}"
