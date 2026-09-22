"""Unit tests for ``ingestion.gk_decision_writer`` (P1 Task B2) -- RECONSTRUCTION TIER ONLY.

Exercises the PURE per-unit core ``score_gk_decision_unit`` (the tracking-marts drain body) on a synthetic
SB360 goal-kick fixture (mirrors the silly-kicks TF-62 sb360 e2e worked example): anonymous freeze-frame
rows + an ``is_actor`` keeper row, bridged to the real keeper id, scored with the bundled
``PassCompletionModel``. gk_decision is TRACKING-consuming, so the Spark drain dispatch is validated live
in Part B (P2) — this covers the pure core.

Key contracts under test:
  * ``decision_pct`` in [0, 1]; ``sel_efficiency`` in [0, 1]; ``chosen_ev`` / ``decision_value`` finite.
  * the ``GkDecisionReport`` conserves drops (scored + 5 drop reasons == decisions_in).
  * the emitted keeper is the NATIVE keeper id; option_set_source == 'reconstructed'.
  * the column set is pinned to the ``METRIC_CONTRACTS['gk_decision']`` registry (SK-EXPORT drift guard).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from silly_kicks.gk_decision import GK_DECISION_KEYS, GkDecisionParams
from silly_kicks.metric_contracts import METRIC_CONTRACTS

from ingestion.gk_decision_writer import (
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    _bundled_completion_model,
    score_gk_decision_reconstructed,
    score_gk_decision_unit,
)

_KEEPER_ID = 777  # real acting keeper id carried on the SPADL action (frames are anonymous)
_GOALKICK = 22  # SPADL type_id


def _actions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            dict(
                game_id="m1",
                period_id=1,
                action_id=1,
                time_seconds=10.0,
                type_id=_GOALKICK,
                player_id=_KEEPER_ID,
                team_id=1,
                start_x=8.0,
                start_y=34.0,
                end_x=30.0,
                end_y=34.0,
            )
        ]
    )


def _snapshot() -> pd.DataFrame:
    # per-action-LTR: keeper's team (1) attacks x=105. Anonymous player ids (0..) as the real port emits.
    # keeper (actor) deep; 3 reachable home teammates; 1 opponent -> exactly one opponent team.
    return pd.DataFrame(
        [
            dict(action_id=1, team_id=1, player_id=0, is_goalkeeper=True, is_actor=True, x=8.0, y=34.0),
            dict(action_id=1, team_id=1, player_id=1, is_goalkeeper=False, is_actor=False, x=30.0, y=34.0),
            dict(action_id=1, team_id=1, player_id=2, is_goalkeeper=False, is_actor=False, x=45.0, y=20.0),
            dict(action_id=1, team_id=1, player_id=3, is_goalkeeper=False, is_actor=False, x=25.0, y=50.0),
            dict(action_id=1, team_id=2, player_id=4, is_goalkeeper=False, is_actor=False, x=20.0, y=34.0),
        ]
    )


def _visible_area() -> pd.DataFrame:
    return pd.DataFrame(
        {"action_id": [1], "polygon": [np.array([[0.0, 0.0], [105.0, 0.0], [105.0, 68.0], [0.0, 68.0]])]}
    )


def _frames() -> pd.DataFrame:
    from silly_kicks.tracking import snapshot_to_tracking_frames

    frames, _links = snapshot_to_tracking_frames(_snapshot(), _actions())
    return frames


# keep every reachable option in the small scene (the bundled xPass is generous below 0.85 on SB360).
_PARAMS = GkDecisionParams(reachability_min_xpass=0.0)


def test_gk_decision_reconstructed_scores_and_bounds() -> None:
    samples, report = score_gk_decision_reconstructed(
        _actions(),
        _frames(),
        keeper_ids=[_KEEPER_ID],
        completion_model=_bundled_completion_model(),
        params=_PARAMS,
        visible_area=_visible_area(),
    )
    assert report.n_decisions_in == 1
    assert report.n_scored == 1  # 3 reachable teammates + chosen >= min_options
    row = samples.iloc[0]
    assert row["option_set_source"] == "reconstructed"
    assert row["keeper"] == str(_KEEPER_ID)  # canonical real id (the actor-identity bridge worked)
    assert 0.0 <= row["decision_pct"] <= 1.0
    assert 0.0 <= row["sel_efficiency"] <= 1.0
    assert np.isfinite(row["chosen_ev"]) and np.isfinite(row["decision_value"])


def test_gk_decision_report_census_conserves() -> None:
    """scored + the 5 drop reasons == decisions_in (GkDecisionReport conservation)."""
    _samples, report = score_gk_decision_reconstructed(
        _actions(),
        _frames(),
        keeper_ids=[_KEEPER_ID],
        completion_model=_bundled_completion_model(),
        params=_PARAMS,
        visible_area=_visible_area(),
    )
    total = (
        report.n_scored
        + report.n_too_few_options
        + report.n_no_unique_chosen
        + report.n_chosen_unvalued
        + report.n_no_frame
        + report.n_fov_cropped
    )
    assert total == report.n_decisions_in


def test_gk_decision_unit_identity_and_columns() -> None:
    """The per-unit drain body stamps native identity + access_tier + option_set_source; exact schema."""
    out = score_gk_decision_unit(
        _actions(),
        _frames(),
        _bundled_completion_model(),
        data_source="idsse",
        match_id="M1",
        access_tier="restricted",
        keeper_ids=[_KEEPER_ID],
        params=_PARAMS,
        visible_area=_visible_area(),
    )
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert not out.empty
    assert (out["data_source"] == "idsse").all()
    assert (out["match_id"] == "M1").all()
    assert (out["access_tier"] == "restricted").all()
    assert (out["option_set_source"] == "reconstructed").all()
    assert out["keeper"].iloc[0] == str(_KEEPER_ID)
    # sk4118 Phase E per-DECISION grain: decision_id == the GK-distribution action_id; period carried.
    assert int(out["decision_id"].iloc[0]) == 1
    assert int(out["period_id"].iloc[0]) == 1
    # One row per decision (the fixture has a single GK-distribution decision).
    assert out.groupby(["match_id", "keeper", "decision_id"]).size().max() == 1


def test_gk_decision_unit_empty_returns_full_schema() -> None:
    """Empty actions/frames -> full-schema empty frame (no crash)."""
    out = score_gk_decision_unit(
        _actions().iloc[0:0],
        _frames().iloc[0:0],
        _bundled_completion_model(),
        data_source="idsse",
        match_id="M1",
        access_tier="restricted",
    )
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty


def test_gk_decision_mart_matches_sk_columns() -> None:
    """Schema-drift guard (SK-EXPORT / METRIC_CONTRACTS registry, sk:ADR-098).

    The writer's ``_OUTPUT_TYPES`` carries every sk metric column; the family's keys
    ('game_id','keeper') map to native identity (match_id + native keeper). column_types is None for
    gk_decision (the lakehouse infers the DDL, OI-3).
    """
    contract = METRIC_CONTRACTS["gk_decision"]
    sk_metric_cols = set(contract["metric_columns"])
    assert sk_metric_cols.issubset(set(_OUTPUT_TYPES)), (
        f"writer missing sk metric columns: {sorted(sk_metric_cols - set(_OUTPUT_TYPES))}"
    )
    assert set(GK_DECISION_KEYS) == {"game_id", "keeper"}
    assert contract["column_types"] is None  # OI-3: lakehouse infers the DDL
    assert {"match_id", "keeper", "team_id", "access_tier", "option_set_source", "n_options"}.issubset(
        set(_OUTPUT_TYPES)
    )
