"""ExT-v2 (ADR-085) producer contract — ``ingestion.expected_threat`` fits a sk ``ExpectedThreat``
and persists its ``to_dict()`` JSON to bronze ``expected_threat_grids``.

Covers the pure, Spark-free halves of the retooled producer: the gold-slice -> sk-input mapping, the
16x12 fit, and the serialization contract the writer persists (``xt_model_json`` reconstructs a fitted
model via ``ExpectedThreat.from_dict``). The Spark-bound ``run_pipeline`` / ``_write_grid_if_material``
write path is exercised by the live drain, not here.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ingestion.expected_threat import _XT_L, _XT_W, _fit_sk_grid, _grid_drift, _to_sk_actions


def _actions(n: int = 300, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    types = rng.choice(["pass", "cross", "dribble", "shot", "clearance"], size=n, p=[0.55, 0.1, 0.2, 0.1, 0.05])
    results = rng.choice(["success", "fail"], size=n, p=[0.75, 0.25])
    return pd.DataFrame(
        {
            "type_name": types,
            "result_name": results,
            "start_x": rng.uniform(0, 105, n),
            "start_y": rng.uniform(0, 68, n),
            "end_x": rng.uniform(0, 105, n),
            "end_y": rng.uniform(0, 68, n),
        }
    )


def test_to_sk_actions_maps_names_to_numeric_ids() -> None:
    from silly_kicks.spadl import config as spadlconfig

    df = pd.DataFrame(
        {
            "type_name": ["pass", "shot"],
            "result_name": ["success", "fail"],
            "start_x": [10.0, 90.0],
            "start_y": [10.0, 30.0],
            "end_x": [20.0, 95.0],
            "end_y": [12.0, 34.0],
        }
    )
    out = _to_sk_actions(df)
    assert list(out.columns) == ["type_id", "result_id", "start_x", "start_y", "end_x", "end_y"]
    assert out["type_id"].tolist() == [spadlconfig.actiontype_id["pass"], spadlconfig.actiontype_id["shot"]]
    assert out["result_id"].tolist() == [spadlconfig.result_id["success"], spadlconfig.result_id["fail"]]
    assert out["type_id"].dtype == np.int64 and out["result_id"].dtype == np.int64


def test_to_sk_actions_drops_unknown_vocab() -> None:
    """A row whose type/result name is not in the SPADL vocab is dropped (sk needs concrete ids)."""
    df = _actions(n=10)
    df.loc[0, "type_name"] = "not_a_spadl_type"
    out = _to_sk_actions(df)
    assert len(out) == 9  # the unknown-type row dropped


def test_fit_sk_grid_is_16x12_and_fitted() -> None:
    model = _fit_sk_grid(_actions(n=400, seed=3))
    assert (model.l, model.w) == (_XT_L, _XT_W) == (16, 12)
    xt = np.asarray(model.xT)
    assert xt.shape == (12, 16)  # sk stores (w, l)
    assert np.any(xt), "unfitted (all-zero) xT — fit did not run"
    assert model.transition_matrix is not None


def test_persisted_payload_roundtrips_via_from_dict() -> None:
    """The exact payload the writer persists (json.dumps(model.to_dict())) reconstructs a fitted model."""
    from silly_kicks.xthreat import ExpectedThreat

    model = _fit_sk_grid(_actions(n=400, seed=5))
    xt_model_json = json.dumps(model.to_dict())  # what _write_grid_if_material stores
    reconstructed = ExpectedThreat.from_dict(json.loads(xt_model_json))
    assert (reconstructed.l, reconstructed.w) == (16, 12)
    assert reconstructed.transition_matrix is not None
    assert np.array_equal(np.asarray(reconstructed.xT), np.asarray(model.xT))
    assert json.loads(xt_model_json)["format_version"] == 1


def test_project_physical_zones_shape_and_orientation() -> None:
    """ADR-085 G-fix: the derived long-form projection is 16x12, physical-oriented (rises with zone_x).

    Non-circular: a known x-ramp is written into ``.xT`` (a random fit produces a flat grid, so
    directionality cannot be asserted from a fit) and the projection must rise monotonically with
    ``zone_x`` toward the attacking goal, independent of the ``.xT`` y-inversion storage.
    """
    from silly_kicks.xthreat import ExpectedThreat

    from ingestion.expected_threat import _XT_L, _XT_W, _project_physical_zones

    model = ExpectedThreat(l=_XT_L, w=_XT_W)
    # sk .xT is stored (w, l) = (y, x); the x-column index rises with physical x. A pure x-ramp
    # (constant in y) must project to values that rise monotonically with zone_x.
    model.xT = np.tile(np.linspace(0.0, 1.0, _XT_L), (_XT_W, 1))  # (w=12, l=16)

    zones = _project_physical_zones(model)
    assert set(zones.columns) == {"zone_x", "zone_y", "xt_value"}
    assert len(zones) == _XT_L * _XT_W
    assert (int(zones["zone_x"].min()), int(zones["zone_x"].max())) == (0, _XT_L - 1)
    assert (int(zones["zone_y"].min()), int(zones["zone_y"].max())) == (0, _XT_W - 1)
    per_x = zones.groupby("zone_x")["xt_value"].mean().to_numpy()
    assert np.all(np.diff(per_x) >= -1e-9), "projection not monotone in zone_x (orientation regression)"
    assert per_x[-1] > per_x[0]


def test_grid_drift_alias_delegates() -> None:
    prev = np.full((12, 16), 0.10)
    new = prev.copy()
    new[0, 0] = 0.13  # +30% on an above-floor cell
    assert _grid_drift(new, prev) == 0.30 * (0.10 / 0.10)  # 0.30
    assert _grid_drift(new, None) is None
