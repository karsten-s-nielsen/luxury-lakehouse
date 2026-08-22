"""Unit tests for the SB360 ``visible_area`` polygon parser (silly-kicks 4.87.0 visibility coverage).

Covers ``analytics.action_context.visible_area.build_visible_area`` — the ``action_id -> polygon``
frame the visibility aggregators consume (spec §7.1/§7.5) — plus the schema registration of the 8
visibility columns. No live SB360 AC fixture carries a raw ``visible_area`` (the committed SB360 fixture
is the POST-snapshot form ``action_id/team_id/is_goalkeeper/x/y``, no polygon), so the parser is tested
in isolation on a synthetic fixture and the join is exercised end-to-end against silly-kicks'
``add_visible_area_coverage`` (the real ADR-019 canonical-join risk), and the 8 columns are asserted
present in ``RESULT_COLUMNS`` + the DDL.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from silly_kicks.tracking import add_visible_area_coverage

from analytics.action_context.schema import ACTION_CONTEXT_DDL, RESULT_COLUMNS
from analytics.action_context.visible_area import build_visible_area

# A left-ish observed region (>=3 vertices) and a wider one, both in RAW StatsBomb coords (0-120 x 0-80,
# flat [x1,y1,...]). polygon_to_spadl scales/inverts to SPADL (0-105 x 0-68) — never 0-length.
_HALF_POLY = "[0, 0, 60, 0, 60, 80, 0, 80]"
_WIDE_POLY = "[0, 0, 100, 0, 100, 80, 0, 80]"

# The 8 visibility columns (spec §7.1/§7.5): 2 base (add_visible_area_coverage) + 6 companions
# (add_action_context(visible_area=)).
_VISIBILITY_COLUMNS = [
    "visible_area_fraction",
    "visible_area_source",
    "nearest_defender_distance_observed_fraction",
    "nearest_defender_distance_observed_source",
    "receiver_zone_density_observed_fraction",
    "receiver_zone_density_observed_source",
    "defenders_in_triangle_to_goal_observed_fraction",
    "defenders_in_triangle_to_goal_observed_source",
]


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Synthetic (actions, raw-sb360) mirroring the bronze shape: sb360 is per-PLAYER (visible_area
    replicated across an event's rows), so the parser must de-dup to one polygon per event.

    uuidA maps to TWO actions (10 then 12): keep-LAST -> 12. uuidC carries an odd/short polygon
    (published-but-unusable -> degenerate). uuidD carries an empty polygon (nothing published). uuidZ
    is unmapped (no action). Each mapped event appears on 2 player rows.
    """
    actions = pd.DataFrame(
        {
            "action_id": [10, 12, 11, 13, 14],
            "original_event_id": ["uuidA", "uuidA", "uuidB", "uuidC", "uuidD"],
            "team_id": [941, 941, 911, 941, 911],
        }
    )
    sb360 = pd.DataFrame(
        {
            "id": ["uuidA", "uuidA", "uuidB", "uuidB", "uuidC", "uuidC", "uuidD", "uuidZ"],
            "teammate": [True, False, True, False, True, False, True, True],
            "keeper": [False, True, False, False, False, False, False, False],
            "visible_area": [
                _HALF_POLY,
                _HALF_POLY,  # uuidA (replicated per player)
                _WIDE_POLY,
                _WIDE_POLY,  # uuidB
                "[1, 2, 3]",
                "[1, 2, 3]",  # uuidC — odd/short -> degenerate (published, unusable)
                None,
                "[0, 0, 5, 5]",  # uuidD (nothing published) / uuidZ (unmapped)
            ],
        }
    )
    return actions, sb360


def test_maps_events_to_actions_and_dedups_per_event() -> None:
    actions, sb360 = _fixture()
    got = build_visible_area(actions, sb360)
    assert list(got.columns) == ["action_id", "polygon"]
    # uuidA->12 (keep-last), uuidB->11, uuidC->13 (degenerate, still emitted). uuidD (empty) is NOT
    # emitted (nothing published); uuidZ is unmapped. One row per event despite replicated player rows.
    assert set(got["action_id"]) == {12, 11, 13}
    assert len(got) == 3


def test_polygon_is_spadl_scale_ndarray() -> None:
    actions, sb360 = _fixture()
    got = build_visible_area(actions, sb360)
    poly_11 = got.loc[got["action_id"] == 11, "polygon"].iloc[0]
    assert isinstance(poly_11, np.ndarray)
    assert poly_11.ndim == 2 and poly_11.shape[1] == 2 and poly_11.shape[0] >= 3
    assert np.isfinite(poly_11).all()
    # SPADL-scaled (0-105 x 0-68), NOT raw StatsBomb 0-120: the _WIDE_POLY x-max (100 in SB) maps well
    # below 120 and within the SPADL pitch + a small unclipped margin.
    assert poly_11[:, 0].max() <= 106.0
    assert poly_11[:, 1].max() <= 69.0


def test_degenerate_polygon_emitted_empty() -> None:
    actions, sb360 = _fixture()
    got = build_visible_area(actions, sb360)
    poly_13 = got.loc[got["action_id"] == 13, "polygon"].iloc[0]  # odd/short -> empty (0,2)
    assert isinstance(poly_13, np.ndarray)
    assert len(poly_13) == 0  # < MIN_VERTICES downstream -> degenerate_polygon (distinct from absent)


def test_empty_and_missing_inputs_return_schema() -> None:
    cols = ["action_id", "polygon"]
    assert list(build_visible_area(pd.DataFrame(), pd.DataFrame()).columns) == cols
    # Raw df lacking visible_area / id -> empty frame, never a KeyError.
    actions = pd.DataFrame({"action_id": [1], "original_event_id": ["u"]})
    assert build_visible_area(actions, pd.DataFrame({"id": ["u"]})).empty
    assert build_visible_area(actions, pd.DataFrame({"visible_area": ["[0,0,1,1,2,2]"]})).empty


def test_join_resolves_observed_end_to_end_cross_dtype() -> None:
    """The parsed frame drives add_visible_area_coverage to a real 'observed' fraction — proving the
    canonical_id join resolves (ADR-019). Non-vacuous: the coverage actions carry action_id as a STRING
    while the polygon frame's key is int, exactly the int64-vs-object case that a raw dict would report
    all-no_polygon for. If canonical bridging works we get 'observed' + a fraction in (0, 1]; if it were
    a raw dict we'd get all no_polygon.
    """
    actions, sb360 = _fixture()
    va = build_visible_area(actions, sb360)
    cov_actions = pd.DataFrame({"action_id": ["12", "11", "13", "14"]})  # STRING keys (cross-dtype)
    out = add_visible_area_coverage(cov_actions, visible_area=va)
    by_action = dict(zip(out["action_id"], out["visible_area_source"], strict=True))
    assert by_action["12"] == "observed"
    assert by_action["11"] == "observed"
    assert by_action["13"] == "degenerate_polygon"  # published-but-unusable
    assert by_action["14"] == "no_polygon"  # nothing published for this action
    frac_12 = out.loc[out["action_id"] == "12", "visible_area_fraction"].iloc[0]
    assert 0.0 < float(frac_12) <= 1.0


def test_eight_visibility_columns_registered_in_schema() -> None:
    for col in _VISIBILITY_COLUMNS:
        assert col in RESULT_COLUMNS, f"{col} missing from RESULT_COLUMNS"
        assert col in ACTION_CONTEXT_DDL, f"{col} missing from ACTION_CONTEXT_DDL"
    # All 8 land contiguously (the drain-native visibility block).
    idxs = [RESULT_COLUMNS.index(c) for c in _VISIBILITY_COLUMNS]
    assert idxs == list(range(idxs[0], idxs[0] + len(_VISIBILITY_COLUMNS)))
