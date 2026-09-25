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


# ── sk4123 P2: distributed fit_from_counts producer (ADR-085 amendment) ───────────────────────
# The daily producer fits per-competition + global sk ExpectedThreat grids from a single distributed
# Spark count aggregation (fit_from_counts) instead of pulling the 9.5M-row corpus to the driver.
# These pure/Spark-free tests validate the pandas reference aggregator (mirrors the Spark SQL), the
# zone-binning parity vs sk, and the byte-identity of fit_from_counts(<agg>) vs .fit(actions).

from ingestion.expected_threat import (  # noqa: E402
    _PITCH_LENGTH,
    _PITCH_WIDTH,
    _aggregate_counts,
    _fit_grid_from_counts,
    _sum_counts,
    _zone_flat_index,
)


def _gold_row(action_type, action_result, sx, sy, ex, ey):
    return {
        "action_type": action_type,
        "action_result": action_result,
        "start_x": sx,
        "start_y": sy,
        "end_x": ex,
        "end_y": ey,
    }


def _ext_fixture() -> pd.DataFrame:
    """Gold-form (action_type/action_result name) SPADL carrying the §Tests boundary rows."""
    rows = [
        # shots: zone (20,34) 1 shot + 1 goal; zone (95,34) shots but ZERO goals (b: _safe_divide 0-branch)
        _gold_row("shot", "success", 20.0, 34.0, 105.0, 34.0),
        _gold_row("shot", "fail", 95.0, 34.0, 105.0, 34.0),
        _gold_row("shot", "fail", 96.0, 30.0, 105.0, 38.0),
        # moves spread across zones
        _gold_row("pass", "success", 10.0, 10.0, 40.0, 20.0),
        _gold_row("pass", "success", 40.0, 20.0, 70.0, 40.0),
        _gold_row("dribble", "success", 70.0, 40.0, 78.0, 44.0),
        _gold_row("cross", "success", 88.0, 8.0, 96.0, 34.0),
        _gold_row("pass", "fail", 50.0, 30.0, 65.0, 25.0),  # failed move, valid end → Singh denom not numerator
        # (a) valid start, NaN end — counts in move_counts, DROPPED from transition_start_counts (D1-SPEC-01)
        _gold_row("pass", "success", 45.0, 45.0, np.nan, np.nan),
        # (c) off-pitch end (x>105) → clamps to l-1
        _gold_row("pass", "success", 60.0, 30.0, 110.0, 34.0),
        # excluded types (EXT-PLAN-01): shot_penalty/shot_freekick NOT a shot; take_on/throw_in NOT a move.
        # placed in an otherwise-empty zone (~2,6 → high y, low x) so a filter bug makes that zone non-zero.
        _gold_row("shot_penalty", "success", 12.0, 62.0, 105.0, 34.0),
        _gold_row("shot_freekick", "fail", 13.0, 63.0, 40.0, 30.0),
        _gold_row("take_on", "success", 14.0, 60.0, 15.0, 61.0),
        _gold_row("throw_in", "success", 11.0, 64.0, 30.0, 55.0),
    ]
    return pd.DataFrame(rows)


def _ext_fixture_b() -> pd.DataFrame:
    rows = [
        _gold_row("shot", "success", 85.0, 40.0, 105.0, 34.0),
        _gold_row("pass", "success", 15.0, 55.0, 45.0, 50.0),
        _gold_row("dribble", "success", 45.0, 50.0, 52.0, 48.0),
        _gold_row("cross", "fail", 90.0, 12.0, 98.0, 40.0),
        _gold_row("pass", "success", 25.0, 20.0, 55.0, 30.0),
    ]
    return pd.DataFrame(rows)


_MATRICES = ("scoring_prob_matrix", "shot_prob_matrix", "move_prob_matrix", "transition_matrix")


def _sk_from_gold(actions: pd.DataFrame) -> pd.DataFrame:
    """Map gold-form (action_type/action_result) → the type_name/result_name _to_sk_actions expects."""
    renamed = actions.rename(columns={"action_type": "type_name", "action_result": "result_name"})
    return _to_sk_actions(renamed)


def _fit_actions_gold(actions: pd.DataFrame):
    """Reference .fit(actions) on the gold fixture via the retained _to_sk_actions map."""
    from silly_kicks.xthreat import ExpectedThreat

    return ExpectedThreat(l=_XT_L, w=_XT_W).fit(_sk_from_gold(actions))


def test_sql_binning_matches_sk_get_flat_indexes() -> None:
    """_zone_flat_index == sk _get_flat_indexes on a probe grid incl. edges + the 15/11 clamp (D1)."""
    from silly_kicks.xthreat._grid import _get_flat_indexes

    xs = np.array([0.0, 52.5, 105.0, 120.0, 60.0])
    ys = np.array([0.0, 34.0, 68.0, -5.0, 10.0])
    ref = _get_flat_indexes(pd.Series(xs), pd.Series(ys), _XT_L, _XT_W).to_numpy()
    got = _zone_flat_index(xs, ys, _XT_L, _XT_W)
    assert np.array_equal(got, ref)


def test_count_aggregation_matches_sk_internals() -> None:
    """_aggregate_counts (pandas ref, mirrors the Spark SQL) == sk _count / singh intermediates."""
    import silly_kicks.spadl.config as cfg
    from silly_kicks.xthreat._grid import _count, _get_flat_indexes, _get_move_actions

    a = _ext_fixture()
    sk = _sk_from_gold(a)
    counts = _aggregate_counts(a)
    shots = sk[sk.type_id == cfg.actiontype_id["shot"]]
    goals = shots[shots.result_id == cfg.result_id["success"]]
    moves = _get_move_actions(sk)
    mse = moves.dropna(subset=["start_x", "start_y", "end_x", "end_y"])
    succ = mse[mse.result_id == cfg.result_id["success"]]
    assert np.array_equal(counts["shot_counts"], _count(shots.start_x, shots.start_y, _XT_L, _XT_W))
    assert np.array_equal(counts["goal_counts"], _count(goals.start_x, goals.start_y, _XT_L, _XT_W))
    assert np.array_equal(counts["move_counts"], _count(moves.start_x, moves.start_y, _XT_L, _XT_W))
    assert np.array_equal(counts["transition_start_counts"], _count(mse.start_x, mse.start_y, _XT_L, _XT_W))
    n = _XT_L * _XT_W
    fs = _get_flat_indexes(succ.start_x, succ.start_y, _XT_L, _XT_W).to_numpy()
    fe = _get_flat_indexes(succ.end_x, succ.end_y, _XT_L, _XT_W).to_numpy()
    tc = np.zeros((n, n))
    np.add.at(tc, (fs, fe), 1.0)
    assert np.array_equal(counts["transition_counts"], tc)


def test_fit_from_counts_byte_identical_to_fit() -> None:
    """D6: fit_from_counts(<agg>) byte-identical to .fit(actions); fixture (a) NaN-end move exercises
    the move_counts (valid-start) vs transition_start_counts (valid-start+end) split non-vacuously."""
    a = _ext_fixture()
    counts = _aggregate_counts(a)
    # non-vacuity: the NaN-end move makes move_counts strictly exceed transition_start_counts
    assert counts["move_counts"].sum() > counts["transition_start_counts"].sum()
    xc = _fit_grid_from_counts(counts)
    xa = _fit_actions_gold(a)
    for m in _MATRICES:
        assert np.array_equal(getattr(xc, m), getattr(xa, m)), f"{m} not byte-identical"
    assert np.allclose(xc.xT, xa.xT, equal_nan=True)


def test_grid_dims_match_spadlconfig() -> None:
    import silly_kicks.spadl.config as cfg
    from silly_kicks.xthreat import ExpectedThreat

    assert (_XT_L, _XT_W) == (16, 12)
    assert (_PITCH_LENGTH, _PITCH_WIDTH) == (cfg.field_length, cfg.field_width)
    default = ExpectedThreat()
    assert (default.l, default.w) == (_XT_L, _XT_W)


def test_global_equals_sum_of_per_comp_counts() -> None:
    a, b = _ext_fixture(), _ext_fixture_b()
    summed = _sum_counts([_aggregate_counts(a), _aggregate_counts(b)])
    xg = _fit_grid_from_counts(summed)
    xcat = _fit_actions_gold(pd.concat([a, b], ignore_index=True))
    for m in _MATRICES:
        assert np.array_equal(getattr(xg, m), getattr(xcat, m)), f"{m} not additive"
    assert np.allclose(xg.xT, xcat.xT, equal_nan=True)


def test_excluded_action_types_are_not_counted() -> None:
    """EXT-PLAN-01: shot_penalty/shot_freekick are NOT shots; take_on/throw_in are NOT moves. Their
    otherwise-empty zones must be 0; the SAME zones ARE counted when re-typed to shot/pass (control)."""
    a = _ext_fixture()
    counts = _aggregate_counts(a)
    # the excluded rows sit at low-x/high-y zones; assert their start zones carry no shot/move count
    for _, r in a[a["action_type"].isin(["shot_penalty", "shot_freekick", "take_on", "throw_in"])].iterrows():
        zx, zy = int(r["start_x"] / (_PITCH_LENGTH / _XT_L)), int(r["start_y"] / (_PITCH_WIDTH / _XT_W))
        row = _XT_W - 1 - zy
        assert counts["shot_counts"][row, zx] == 0, "excluded shot-like type counted as shot"
        assert counts["move_counts"][row, zx] == 0, "excluded move-like type counted as move"
    # control: re-type the penalty→shot and take_on→pass; now those zones ARE counted (non-vacuity)
    ctrl = a.copy()
    ctrl.loc[ctrl["action_type"] == "shot_penalty", "action_type"] = "shot"
    ctrl.loc[ctrl["action_type"] == "take_on", "action_type"] = "pass"
    cc = _aggregate_counts(ctrl)
    assert cc["shot_counts"].sum() > counts["shot_counts"].sum()
    assert cc["move_counts"].sum() > counts["move_counts"].sum()
