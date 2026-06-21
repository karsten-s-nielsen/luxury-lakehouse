"""Tests for the PSxG trainer's domain helpers (plan task 0.4).

Covers the two pure functions the hardened ``scripts/train_psxg_hf.py`` relies on:

  - ``cross_validate_psxg_by_match`` — out-of-sample CV via GroupKFold grouped by
    match (the xT-3 cross-game leak class: plain k-fold puts same-match shots in
    both train and test and inflates the score). The trainer reports THIS, not the
    in-sample / random-split number.
  - ``serialize_psxg_model`` — the single-source JSON envelope (HF Hub + MLflow
    artifact + UC Volume), carrying ``feature_names`` (ADR-012 §2) + ``model_version``.
"""

from __future__ import annotations

import json
import math

import pandas as pd

from analytics.goalkeeper import (
    PSXG_FEATURE_NAMES,
    cross_validate_psxg_by_match,
    load_psxg_model,
    serialize_psxg_model,
    train_psxg_model,
)


def _multi_match_shots(n_matches: int = 4) -> pd.DataFrame:
    """Each match carries both classes (2 goals + 2 non-goals) with a learnable
    z→goal signal, so every GroupKFold train fold is two-class (no single-class skip)."""
    rows: list[dict[str, float | int]] = []
    for m in range(n_matches):
        match_key = (m + 1) * 10
        for z, goal in ((0.4, 0), (0.8, 0), (1.8, 1), (2.3, 1)):
            rows.append(
                {
                    "match_key": match_key,
                    "location_x": 105.0,
                    "location_y": 40.0,
                    "end_location_x": 120.0,
                    "end_location_y": 40.0,
                    "end_location_z": z,
                    "distance_to_goal": 15.0,
                    "shot_angle": 0.4,
                    "is_goal": goal,
                }
            )
    return pd.DataFrame(rows)


def test_cross_validate_groups_by_match_full_oos_coverage() -> None:
    df = _multi_match_shots(n_matches=4)
    cv = cross_validate_psxg_by_match(df, n_splits=5)

    # GroupKFold holds each match out exactly once -> every row gets one OOS prediction.
    # 16 rows fully covered is only possible because grouping is by match (4 groups).
    assert cv["n_groups"] == 4
    assert cv["n_oos_shots"] == 16
    brier = float(cv["brier_score"])
    auc = float(cv["roc_auc"])
    assert 0.0 <= brier <= 1.0
    assert 0.0 <= auc <= 1.0
    assert math.isfinite(float(cv["log_loss"]))


def test_cross_validate_single_match_is_degenerate() -> None:
    # One match cannot be held out -> NaN metrics, zero OOS shots (not a crash, not 0.0).
    df = _multi_match_shots(n_matches=1)
    cv = cross_validate_psxg_by_match(df, n_splits=5)
    assert cv["n_groups"] == 1
    assert cv["n_oos_shots"] == 0
    assert math.isnan(float(cv["brier_score"]))
    assert math.isnan(float(cv["roc_auc"]))


def test_cross_validate_falls_back_to_match_id() -> None:
    # No match_key column -> group by match_id; coverage identical.
    df = _multi_match_shots(n_matches=4).rename(columns={"match_key": "match_id"})
    cv = cross_validate_psxg_by_match(df, n_splits=5)
    assert cv["n_groups"] == 4
    assert cv["n_oos_shots"] == 16


def test_serialize_psxg_model_envelope_has_feature_names_and_roundtrips(tmp_path) -> None:
    model = train_psxg_model(_multi_match_shots(n_matches=4))
    payload = serialize_psxg_model(
        model,
        metrics={"brier_score": 0.2, "n_oos_shots": 16},
        model_version="v2-ontarget",
    )
    envelope = json.loads(payload)

    # ADR-012 §2: feature_names present + matches the canonical 4-feature order.
    assert envelope["feature_names"] == list(PSXG_FEATURE_NAMES)
    assert envelope["feature_names"][0] == "goalmouth_dist_from_centre"
    assert len(envelope["feature_names"]) == 4
    assert envelope["model_version"] == "v2-ontarget"
    assert len(envelope["coefficients"]) == 4
    assert "scaler_mean" in envelope and "scaler_scale" in envelope

    # load_psxg_model reads the same envelope (the inference consumer's contract).
    path = tmp_path / "psxg_model.json"
    path.write_bytes(payload)
    loaded = load_psxg_model(str(path))
    assert loaded.coefficients.tolist() == model.coefficients.tolist()
    assert loaded.intercept == model.intercept
