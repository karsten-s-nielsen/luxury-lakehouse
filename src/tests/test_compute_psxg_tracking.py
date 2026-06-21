"""Tests for the tracking PSxG writer's pure pieces (task 1.3).

The Spark read/write is deploy-gated; the model loader + prediction-row builder
(score + calibrate + yellow_card drop + provenance) are pure and tested here.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from analytics.goalkeeper import PSxGModel, load_psxg_model
from ingestion.compute_psxg_tracking import build_predictions


def test_load_psxg_model_roundtrip(tmp_path) -> None:
    envelope = {
        "coefficients": [1.5, -0.5],
        "intercept": [-1.0],  # shape (1,) per the published envelope
        "scaler_mean": [0.4, 0.2],
        "scaler_scale": [0.1, 0.05],
    }
    path = tmp_path / "psxg_model.json"
    path.write_text(json.dumps(envelope), encoding="utf-8")
    model = load_psxg_model(str(path))
    assert model.coefficients.tolist() == [1.5, -0.5]
    assert model.intercept == pytest.approx(-1.0)
    assert model.scaler_mean.tolist() == [0.4, 0.2]
    assert model.scaler_scale.tolist() == [0.1, 0.05]


def _model() -> PSxGModel:
    return PSxGModel(
        coefficients=np.array([1.0, 2.0, -0.05, 0.5]),
        intercept=-1.0,
        scaler_mean=np.zeros(4),
        scaler_scale=np.ones(4),
    )


def _shots() -> pd.DataFrame:
    # 2 matches x {goal, no-goal} gate-passing, 1 gated, 1 yellow_card.
    return pd.DataFrame(
        {
            "match_key": [1, 1, 2, 2, 1, 2],
            "action_id": [10, 11, 12, 13, 14, 15],
            "data_source": ["idsse", "idsse", "skillcorner", "skillcorner", "idsse", "skillcorner"],
            "start_x": [88.0, 90.0, 92.0, 89.0, 91.0, 90.0],
            "start_y": [34.0, 33.0, 35.0, 34.0, 36.0, 34.0],
            "shot_crossing_y": [33.0, 35.0, 32.0, 36.0, 34.0, 34.0],
            "shot_crossing_z": [1.0, 2.0, 0.5, 1.5, 1.0, 1.0],
            "shot_crossing_confidence": [0.9, 0.9, 0.8, 0.8, 0.2, 0.9],  # row idx4 gated
            "shot_fit_rmse": [0.1, 0.1, 0.2, 0.2, 0.1, 0.1],
            "action_result": ["success", "fail", "fail", "success", "fail", "yellow_card"],
        }
    )


def test_build_predictions_drops_yellow_card_and_shapes_output() -> None:
    out = build_predictions(_shots(), _model(), model_version="psxg-vTEST")
    # 6 input rows minus 1 yellow_card = 5 prediction rows.
    assert len(out) == 5
    assert set(out["data_source"]) <= {"idsse", "skillcorner"}
    assert (out["psxg_calibration"] == "platt").all()
    assert (out["model_version"] == "psxg-vTEST").all()
    assert out["normalization_version"].iloc[0] == "spadl-goalwidth-7.32-v1"
    expected_cols = {
        "match_key",
        "action_id",
        "data_source",
        "psxg",
        "psxg_recalibrated",
        "psxg_gated",
        "psxg_calibration",
        "model_version",
        "platt_version",
        "normalization_version",
    }
    assert expected_cols == set(out.columns)


def test_build_predictions_gated_row_has_null_psxg() -> None:
    out = build_predictions(_shots(), _model(), model_version="psxg-vTEST")
    gated = out[out["action_id"] == 14]  # the low-confidence row
    assert bool(gated["psxg_gated"].iloc[0]) is True
    assert np.isnan(gated["psxg"].iloc[0])
    assert np.isnan(gated["psxg_recalibrated"].iloc[0])  # gated stays NaN after Platt


def test_build_predictions_gatepassed_recalibrated_in_unit_interval() -> None:
    out = build_predictions(_shots(), _model(), model_version="psxg-vTEST")
    passed = out[~out["psxg_gated"].astype(bool)]
    recal = passed["psxg_recalibrated"].to_numpy(dtype=float)
    assert np.all((recal >= 0.0) & (recal <= 1.0))
