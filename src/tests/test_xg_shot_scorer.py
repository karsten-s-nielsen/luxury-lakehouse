"""Unit + synthetic-e2e tests for the pre-shot xG v3 scorer (Task 1.9).

Covers the PURE, Spark/torch-free scoring logic of ``ingestion.xg_shot_scorer``:

* the penalty constant is taken from the loaded envelope (never recomputed live);
* mode **selection** and **certification** are separate (M2) — a selected mode can
  still fail certification and force ``ood_flag=True``;
* the N4 missing-per-provider-calibrator fallback applies the pooled calibrator AND
  flags the row;
* full **serve parity** with the trainer — the scorer imports the SAME
  ``analytics.xg_model.build_features`` + ``analytics.xg_freeze_frame.normalize_freeze_frame``
  (import identity);
* a synthetic end-to-end run over a shot + its freeze frame joined on
  ``(match_key, action_id)`` produces exactly ONE prediction row per shot with the full
  ``bronze.xg_shot_predictions`` output schema (join-grain regression guard);
* the ADR-002 §4 DDL↔constant schema-drift guard for the bronze writer.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

import ingestion.xg_shot_scorer as scorer
from analytics.xg_calibration import AucCi, PlattParams, apply_platt

# ---------------------------------------------------------------------------
# Helpers — synthetic set-encoder weights + calibrator entries
# ---------------------------------------------------------------------------


def _synthetic_weights(tabular_dim: int, seed: int = 0) -> dict[str, np.ndarray]:
    """A shape-correct set-encoder weight dict (4->32->16 encoder; (tab+16)->64->32->1 MLP)."""
    rng = np.random.default_rng(seed)
    ctx = 16
    return {
        "encoder_fc1_weight": rng.normal(scale=0.1, size=(32, 4)),
        "encoder_fc1_bias": np.zeros(32),
        "encoder_fc2_weight": rng.normal(scale=0.1, size=(ctx, 32)),
        "encoder_fc2_bias": np.zeros(ctx),
        "pred_fc1_weight": rng.normal(scale=0.1, size=(64, tabular_dim + ctx)),
        "pred_fc1_bias": np.zeros(64),
        "pred_fc2_weight": rng.normal(scale=0.1, size=(32, 64)),
        "pred_fc2_bias": np.zeros(32),
        "pred_fc3_weight": rng.normal(scale=0.1, size=(1, 32)),
        "pred_fc3_bias": np.zeros(1),
    }


def _platt_entry(a: float, b: float) -> dict:
    return {"kind": "platt", "params": PlattParams(a=a, b=b).to_dict()}


_FEATURE_NAMES = list(scorer.UNIFORM_FEATURE_NAMES)


# ---------------------------------------------------------------------------
# 1. Penalty constant is read from the envelope, never recomputed
# ---------------------------------------------------------------------------


def test_scorer_reads_penalty_constant_from_envelope() -> None:
    penalty_xg = 0.7631
    shots = pd.DataFrame(
        {
            "match_id_native": ["m1"],
            "match_key": [100],
            "action_id": [3],
            "data_source": ["statsbomb"],
            "action_type": ["shot_penalty"],
            "action_result": ["fail"],
            # Deliberately absurd geometry: if the scorer recomputed, this would NOT be 0.7631.
            "start_x": [3.0],
            "start_y": [1.0],
        }
    )
    out = scorer.score_shot_rows(
        shots,
        None,
        weights={},  # penalty rows bypass the encoder — weights are never touched
        feature_names=_FEATURE_NAMES,
        calibrators={"per_provider": {}, "pooled": _platt_entry(1.0, 0.0)},
        penalty_xg=penalty_xg,
        provider_decisions={},
    )
    assert len(out) == 1
    row = out.iloc[0]
    assert row["xg"] == penalty_xg
    assert row["xg_ci_low"] == penalty_xg
    assert row["xg_ci_high"] == penalty_xg
    assert row["scoring_mode"] == scorer._PENALTY_SCORING_MODE
    assert bool(row["ood_flag"]) is False


# ---------------------------------------------------------------------------
# 2. Selection != certification
# ---------------------------------------------------------------------------


def test_mode_selection_vs_certification_separate() -> None:
    cfg = scorer.GateConfig(sb_auc=0.80, margin=0.05, floor=0.5)  # relative floor -> 0.75

    # (a) Neither mode clears the floor -> tabular_only is SELECTED (default), but its CI
    #     lower bound is below the floor -> NOT certified -> ood_flag True even though a mode was picked.
    weak = scorer.ProviderGateEvidence(
        context_ci=AucCi(auc=0.70, lo=0.60, hi=0.78),
        tabular_ci=AucCi(auc=0.68, lo=0.58, hi=0.74),
        sum_xg=5.0,
        sum_goals=5,
        n=100,
    )
    mode, ood = scorer.decide_scoring_mode(weak, cfg)
    assert mode == "tabular_only"
    assert ood is True

    # (b) Context clears the floor and beats tabular -> SELECTED and CERTIFIED -> ood False.
    strong = scorer.ProviderGateEvidence(
        context_ci=AucCi(auc=0.82, lo=0.80, hi=0.86),
        tabular_ci=AucCi(auc=0.70, lo=0.62, hi=0.76),
        sum_xg=5.0,
        sum_goals=5,
        n=100,
    )
    mode, ood = scorer.decide_scoring_mode(strong, cfg)
    assert mode == "context_aware"
    assert ood is False

    # (c) Fail-safe: no evidence at all -> tabular_only + flagged.
    mode, ood = scorer.decide_scoring_mode(None, cfg)
    assert mode == "tabular_only"
    assert ood is True


def test_certification_fails_on_bad_calibration_even_if_discriminating() -> None:
    cfg = scorer.GateConfig(sb_auc=0.80, margin=0.05, floor=0.5)
    # Discrimination clears the floor, but aggregate calibration is way off (sum_xg << goals).
    bad_cal = scorer.ProviderGateEvidence(
        context_ci=AucCi(auc=0.82, lo=0.80, hi=0.86),
        tabular_ci=AucCi(auc=0.70, lo=0.62, hi=0.76),
        sum_xg=1.0,
        sum_goals=40,
        n=100,
    )
    mode, ood = scorer.decide_scoring_mode(bad_cal, cfg)
    assert mode == "context_aware"
    assert ood is True


# ---------------------------------------------------------------------------
# 3. N4 missing-calibrator fallback -> pooled + flagged
# ---------------------------------------------------------------------------


def test_missing_calibrator_falls_back_pooled_and_flags() -> None:
    calibrators = {
        "per_provider": {"statsbomb": _platt_entry(2.0, -0.5)},
        "pooled": _platt_entry(1.3, -0.2),
    }
    raw = np.array([0.30])

    # Provider WITH a per-provider calibrator: no fallback.
    cal, fell_back = scorer.calibrate_xg(raw, "statsbomb", calibrators)
    assert fell_back is False
    np.testing.assert_allclose(cal, apply_platt(raw, PlattParams(a=2.0, b=-0.5)))

    # Provider WITHOUT one: pooled applied + flagged.
    cal, fell_back = scorer.calibrate_xg(raw, "skillcorner", calibrators)
    assert fell_back is True
    np.testing.assert_allclose(cal, apply_platt(raw, PlattParams(a=1.3, b=-0.2)))


# ---------------------------------------------------------------------------
# 4. Serve parity — import identity with the trainer's shared functions
# ---------------------------------------------------------------------------


def test_scorer_and_trainer_share_serve_functions() -> None:
    import analytics.xg_freeze_frame as xff
    import analytics.xg_model as xgm

    assert scorer.build_features is xgm.build_features
    assert scorer.normalize_freeze_frame is xff.normalize_freeze_frame


# ---------------------------------------------------------------------------
# 5. Synthetic end-to-end — one row per (match_key, action_id), full schema
# ---------------------------------------------------------------------------


def test_scorer_e2e_synthetic_keyed() -> None:
    weights = _synthetic_weights(tabular_dim=len(_FEATURE_NAMES), seed=7)
    calibrators = {
        "per_provider": {"skillcorner": _platt_entry(1.5, -0.3)},
        "pooled": _platt_entry(1.2, -0.2),
    }

    # Two shots that share action_id=7 under DIFFERENT match_keys — the per-match
    # (match_key, action_id) key must keep them (and their freeze frames) distinct.
    shots = pd.DataFrame(
        {
            "match_id_native": ["sc_a", "sc_b"],
            "match_key": [100, 200],
            "action_id": [7, 7],
            "data_source": ["skillcorner", "skillcorner"],
            "action_type": ["shot", "shot"],
            "action_result": ["fail", "success"],
            "start_x": [88.0, 95.0],
            "start_y": [40.0, 30.0],
        }
    )
    # Three freeze rows for shot (100,7); two for shot (200,7) — must NOT fan the output out.
    freeze = pd.DataFrame(
        {
            "match_key": [100, 100, 100, 200, 200],
            "action_id": [7, 7, 7, 7, 7],
            "x": [90.0, 100.0, 85.0, 96.0, 104.0],
            "y": [34.0, 30.0, 44.0, 30.0, 34.0],
            "is_keeper": [0, 0, 1, 0, 1],
            "is_teammate": [1, 0, 0, 1, 0],
            "shooter_attacks_high_x": [True, True, True, True, True],
        }
    )

    out = scorer.score_shot_rows(
        shots,
        freeze,
        weights=weights,
        feature_names=_FEATURE_NAMES,
        calibrators=calibrators,
        penalty_xg=0.76,
        provider_decisions={"skillcorner": ("context_aware", True)},
    )

    # Full output schema, in order.
    assert list(out.columns) == list(scorer._XG_SHOT_PRED_COLUMNS)
    # Exactly one prediction row per (match_key, action_id) — no fan-out on the 3+2 freeze rows.
    assert len(out) == 2
    assert set(map(tuple, out[["match_key", "action_id"]].to_numpy().tolist())) == {(100, 7), (200, 7)}
    assert not out.duplicated(subset=["match_key", "action_id"]).any()

    for _, row in out.iterrows():
        assert 0.0 <= row["xg"] <= 1.0
        assert row["xg_ci_low"] <= row["xg"] <= row["xg_ci_high"]
        assert row["scoring_mode"] == "context_aware"
        assert bool(row["ood_flag"]) is True  # provider decision carried the flag
        assert row["data_source"] == "skillcorner"


def test_scorer_e2e_tabular_only_zero_context() -> None:
    """A shot with NO freeze frame scores in tabular-only mode (zero context, set_cardinality 0)."""
    weights = _synthetic_weights(tabular_dim=len(_FEATURE_NAMES), seed=11)
    shots = pd.DataFrame(
        {
            "match_id_native": ["ws_1"],
            "match_key": [300],
            "action_id": [12],
            "data_source": ["wyscout"],
            "action_type": ["shot"],
            "action_result": ["fail"],
            "start_x": [80.0],
            "start_y": [40.0],
        }
    )
    out = scorer.score_shot_rows(
        shots,
        None,
        weights=weights,
        feature_names=_FEATURE_NAMES,
        calibrators={"per_provider": {}, "pooled": _platt_entry(1.0, 0.0)},
        penalty_xg=0.76,
        provider_decisions={},  # unknown provider -> fail-safe tabular_only + flag
    )
    assert len(out) == 1
    row = out.iloc[0]
    assert row["scoring_mode"] == "tabular_only"
    assert bool(row["ood_flag"]) is True
    assert 0.0 <= row["xg"] <= 1.0


# ---------------------------------------------------------------------------
# 6. ADR-002 §4 — DDL <-> writer-constant schema-drift guard
# ---------------------------------------------------------------------------

_DDL_PATH = Path("scripts/migrations/2026-07-08-xg-shot-predictions-ddl.sql")
_WRITER_ADDED_COLUMN = "_ingested_at"


def _parse_ddl_columns() -> list[str]:
    sql = _DDL_PATH.read_text(encoding="utf-8")
    match = re.search(r"CREATE TABLE[^(]*\(\s*(.*?)\s*\)\s*USING", sql, re.DOTALL | re.IGNORECASE)
    assert match, f"Could not find CREATE TABLE ... USING block in {_DDL_PATH}"
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        columns.append(line.split()[0].strip())
    return columns


class TestXgShotPredictionsSchemaDriftGuard:
    def test_ddl_file_exists(self) -> None:
        assert _DDL_PATH.is_file(), f"Migration DDL missing: {_DDL_PATH}"

    def test_columns_match_ddl_names_and_order(self) -> None:
        ddl_cols = _parse_ddl_columns()
        expected = [c for c in ddl_cols if c != _WRITER_ADDED_COLUMN]
        assert list(scorer._XG_SHOT_PRED_COLUMNS) == expected, (
            "Schema drift between xg_shot_predictions DDL and _XG_SHOT_PRED_COLUMNS.\n"
            f"  DDL (minus {_WRITER_ADDED_COLUMN}): {expected}\n"
            f"  _XG_SHOT_PRED_COLUMNS:            {list(scorer._XG_SHOT_PRED_COLUMNS)}"
        )

    def test_ingested_at_is_last_ddl_column(self) -> None:
        ddl_cols = _parse_ddl_columns()
        assert ddl_cols[-1] == _WRITER_ADDED_COLUMN
        assert _WRITER_ADDED_COLUMN not in scorer._XG_SHOT_PRED_COLUMNS

    def test_types_cover_all_columns(self) -> None:
        assert set(scorer._XG_SHOT_PRED_TYPES) == set(scorer._XG_SHOT_PRED_COLUMNS)

    def test_no_duplicate_columns(self) -> None:
        assert len(scorer._XG_SHOT_PRED_COLUMNS) == len(set(scorer._XG_SHOT_PRED_COLUMNS))
