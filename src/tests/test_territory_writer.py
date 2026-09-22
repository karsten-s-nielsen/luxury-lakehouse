"""Unit tests for ``ingestion.territory_writer`` (P1 Task C1) on synthetic SPADL actions.

Exercises the PURE core ``_compute_territory_group`` (the per-match ``applyInPandas`` closure body) on a
hand-built match with a defender who accumulates a defensive-action hull. territory is event-only
(silly-kicks TF-54) but requires an injected FITTED ``ExpectedThreat`` (both methods) and, for the
counterfactual method, an injected ``PassCompletionModel``; the writer fits the xT once at the driver and
broadcasts the grid + transition matrix (picklable primitives) into the closure — reconstructed per group.
The Spark ``run_pipeline`` (``applyInPandas`` dispatch) is validated live in Part B, same posture as the
sibling ADR-013 writers.

**Two-contract guard [SPEC-R5-01].** The v1 metric surface is pinned to
``silly_kicks.metric_contracts.METRIC_CONTRACTS['territory']`` (12 metric + 2 keys). The counterfactual
columns are METHOD-CONDITIONAL and NOT in the registry — the 5 cf-only cols are asserted separately as
``columns_for_method('counterfactual') minus columns_for_method('completed_failed')``. The registry parity
NEVER expects the cf cols folded in.

territory is per-PLAYER (a per-defender territorial-dominance evaluation) — ranking-LICENSED per the sk
defender-ranking census (ICC lo=0.121>0). It IS a member of ``PER_PLAYER_EVALUATIVE_CARDS`` (asserted in
``test_ai_governance_md.py``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import silly_kicks.spadl.config as spadlconfig
from silly_kicks.metric_contracts import METRIC_CONTRACTS
from silly_kicks.territory import (
    TERRITORY_KEYS,
    TERRITORY_METRIC_COLUMNS,
    columns_for_method,  # pyright: ignore[reportAttributeAccessIssue] -- sk re-export, resolves at runtime
)

from ingestion.territory_writer import (
    _CF_ONLY_COLUMNS,
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    _compute_territory_group,
    _reconstruct_fitted_xt,
)

_PASS = spadlconfig.actiontype_id["pass"]
_SHOT = spadlconfig.actiontype_id["shot"]
_TACKLE = spadlconfig.actiontype_id["tackle"]
_INTERCEPTION = spadlconfig.actiontype_id["interception"]
_CLEARANCE = spadlconfig.actiontype_id["clearance"]
_SUCCESS = spadlconfig.result_id["success"]
_FAIL = spadlconfig.result_id["fail"]
_FOOT = spadlconfig.bodypart_id["foot"]


def _mk(**kw: object) -> dict[str, object]:
    base: dict[str, object] = {
        "game_id": 1,
        "action_id": 0,
        "period_id": 1,
        "time_seconds": 0.0,
        "team_id_native": "TA",
        "player_id_native": "P100",
        "start_x": 50.0,
        "start_y": 34.0,
        "end_x": 60.0,
        "end_y": 34.0,
        "type_id": _PASS,
        "result_id": _SUCCESS,
        "bodypart_id": _FOOT,
    }
    base.update(kw)
    return base


def _corpus_rows() -> list[dict[str, object]]:
    """A pass/shot corpus rich enough for ExpectedThreat.fit to yield a non-zero xT + transition matrix."""
    rng = np.random.default_rng(1)
    rows: list[dict[str, object]] = []
    aid = 0
    for _ in range(240):
        typ = _SHOT if aid % 15 == 0 else _PASS
        res = _SUCCESS if rng.random() > 0.4 else _FAIL
        rows.append(
            _mk(
                action_id=aid,
                time_seconds=float(aid),
                team_id_native=str(rng.choice(["TA", "TB"])),
                player_id_native=f"P{int(rng.integers(1, 22))}",
                type_id=typ,
                result_id=res,
                start_x=float(rng.uniform(0, 100)),
                start_y=float(rng.uniform(0, 68)),
                end_x=float(min(104.9, 40 + aid * 0.2)),
                end_y=float(rng.uniform(0, 68)),
            )
        )
        aid += 1
    return rows


def _defender_hull_rows(player: str, n: int) -> list[dict[str, object]]:
    """``n`` defensive actions for one defender, spread enough to form a convex hull."""
    xs = [30.0, 32.0, 28.0, 31.0, 29.0, 33.0]
    ys = [20.0, 40.0, 25.0, 35.0, 22.0, 38.0]
    types = [_TACKLE, _INTERCEPTION, _CLEARANCE, _TACKLE, _INTERCEPTION, _CLEARANCE]
    rows: list[dict[str, object]] = []
    for i in range(n):
        rows.append(
            _mk(
                action_id=9000 + i,
                time_seconds=float(500 + i),
                team_id_native="TB",
                player_id_native=player,
                type_id=types[i % len(types)],
                result_id=_SUCCESS,
                start_x=xs[i % len(xs)],
                start_y=ys[i % len(ys)],
                end_x=xs[i % len(xs)],
                end_y=ys[i % len(ys)],
            )
        )
    return rows


def _match_actions(defender_actions: int = 6) -> pd.DataFrame:
    rows = _corpus_rows() + _defender_hull_rows("D1", defender_actions)
    df = pd.DataFrame(rows)
    df["data_source"] = "skillcorner"
    df["match_id_native"] = "M1"
    df["access_tier"] = "restricted"
    return df


def _fitted_xt_model_json(actions: pd.DataFrame) -> str:
    """Fit an ExpectedThreat(16x12) on the fixture -> to_dict JSON (the writer's broadcast payload, ADR-085)."""
    import json

    from silly_kicks.xthreat import ExpectedThreat

    return json.dumps(ExpectedThreat(l=16, w=12).fit(actions).to_dict())


def test_cf_only_columns_match_sk_set_diff() -> None:
    """[SPEC-R5-01] the writer's cf-only set == columns_for_method set-diff (5 cols), never in registry."""
    expected = tuple(c for c in columns_for_method("counterfactual") if c not in columns_for_method("completed_failed"))
    assert set(_CF_ONLY_COLUMNS) == set(expected)
    assert len(_CF_ONLY_COLUMNS) == 5
    # the cf-only cols are NOT in the registry (which is v1 only)
    registry_cols = set(METRIC_CONTRACTS["territory"]["metric_columns"])
    assert not (set(_CF_ONLY_COLUMNS) & registry_cols)


def test_territory_group_produces_20_col_union() -> None:
    """The bronze row carries the 20-col counterfactual union (12 v1 + hull_source + 4 cf metric + target_source)."""
    actions = _match_actions()
    xtj = _fitted_xt_model_json(actions)
    out = _compute_territory_group(actions, xtj)
    # every v1 metric column present
    assert set(TERRITORY_METRIC_COLUMNS).issubset(out.columns)
    # every cf-only column present
    assert set(_CF_ONLY_COLUMNS).issubset(out.columns)
    # exact writer output order
    assert list(out.columns) == list(OUTPUT_COLUMNS)


def test_territory_defender_finite_v1_and_cf() -> None:
    """A defender with a proper hull yields finite v1 (xt_conceded/prevented) AND cf (expected_threat_faced)."""
    actions = _match_actions()
    xtj = _fitted_xt_model_json(actions)
    out = _compute_territory_group(actions, xtj)
    d1 = out[out["player_id"] == "D1"]
    assert len(d1) == 1
    assert np.isfinite(d1["territory_xt_conceded"].iloc[0])
    assert np.isfinite(d1["territory_xt_prevented"].iloc[0])
    assert np.isfinite(d1["territory_expected_threat_faced"].iloc[0])
    assert d1["territory_hull_source"].iloc[0] != "degenerate"


def test_territory_degenerate_defender_counted_nan() -> None:
    """A <3-action defender -> hull_source='degenerate' with a NaN xt_conceded row, still emitted."""
    actions = _match_actions(defender_actions=2)
    xtj = _fitted_xt_model_json(actions)
    out = _compute_territory_group(actions, xtj)
    d1 = out[out["player_id"] == "D1"]
    assert len(d1) == 1
    assert d1["territory_hull_source"].iloc[0] == "degenerate"
    assert pd.isna(d1["territory_xt_conceded"].iloc[0])


def test_territory_identity_and_access_tier() -> None:
    """Native identity + access_tier stamped; exact OUTPUT_COLUMNS order."""
    actions = _match_actions()
    xtj = _fitted_xt_model_json(actions)
    out = _compute_territory_group(actions, xtj)
    assert (out["data_source"] == "skillcorner").all()
    assert (out["match_id"] == "M1").all()
    assert (out["access_tier"] == "restricted").all()


def test_territory_empty_returns_full_schema() -> None:
    """An empty match yields an empty frame carrying the full OUTPUT_COLUMNS schema."""
    actions = _match_actions()
    xtj = _fitted_xt_model_json(actions)
    out = _compute_territory_group(actions.iloc[0:0], xtj)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty


def test_territory_v1_registry_parity() -> None:
    """Schema-drift guard (SK-EXPORT). v1 metric+keys == set(metric_columns) | set(keys) — cf NOT folded in."""
    contract = METRIC_CONTRACTS["territory"]
    v1_expected = set(contract["metric_columns"]) | set(contract["keys"])
    assert len(contract["metric_columns"]) == 12
    assert set(TERRITORY_KEYS) == {"game_id", "player_id"}
    # every v1 metric column is present in the writer output types
    assert set(contract["metric_columns"]).issubset(set(_OUTPUT_TYPES))
    # the v1 registry set NEVER contains the cf-only cols
    assert not (set(_CF_ONLY_COLUMNS) & v1_expected)


def test_reconstruct_fitted_xt_round_trips() -> None:
    """The broadcast to_dict JSON reconstructs a fitted ExpectedThreat usable by the counterfactual method."""
    actions = _match_actions()
    xtj = _fitted_xt_model_json(actions)
    xt = _reconstruct_fitted_xt(xtj)
    assert np.any(np.asarray(xt.xT))
    assert np.asarray(xt.transition_matrix).ndim == 2
