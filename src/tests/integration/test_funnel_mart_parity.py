"""D58 parity integration test — env-gated, runs against live Lakebase.

Skipped by default unless LAKEBASE_HOST is set. Uses the 6 V10 locked oracle
fixtures captured 2026-04-17 18:56 UTC from the current live Lakebase (see
docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md § V10).

Four fixtures must hit exactly — those are cases the old query did NOT
truncate, so the mart should reproduce them zero-delta.

Two fixtures (comp=11 team=217 season) are the correctness-fix cases:
the old query silently dropped 57 % of actions via LIMIT 500000, so the
mart MUST return values >= the oracle on every stage.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "hf_taipy_app" / "src"))

pytestmark = pytest.mark.skipif(
    not os.environ.get("LAKEBASE_HOST"),
    reason="Live Lakebase integration test — set LAKEBASE_HOST to run",
)


def _rollup(df: pd.DataFrame, team_id: int, *, gs_filtered: bool) -> dict[str, dict[str, int]]:
    """Call rollup_stages after splitting the mart df by side."""
    from queries.funnel import rollup_stages

    team_rows = df[df["team_id"] == team_id]
    opp_rows = df[df["team_id"] != team_id]
    return {
        "primary": rollup_stages(team_rows, gs_filtered=gs_filtered),
        "opponent": rollup_stages(opp_rows, gs_filtered=gs_filtered),
    }


# Oracle values: (possessions, a3_entries, shots, goals)
# Captured 2026-04-17 18:56 UTC from the live Lakebase query path (V10).
_EXACT_FIXTURES: list[tuple[str, dict, dict, dict]] = [
    (
        "comp=11 team=213 match=None gs=None",
        {"comp_id": 11, "team_id": 213, "match_id": None, "game_state": None},
        {"primary": (6295, 2721, 742, 75), "opponent": (6510, 3341, 974, 115)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=213 match=None gs=winning",
        {"comp_id": 11, "team_id": 213, "match_id": None, "game_state": "winning"},
        {"primary": (1198, 589, 187, 37), "opponent": (2118, 1081, 320, 73)},
        {"gs_filtered": True},
    ),
    (
        "comp=11 team=217 match=3888713 gs=None",
        {"comp_id": 11, "team_id": 217, "match_id": 3888713, "game_state": None},
        {"primary": (101, 7, 21, 5), "opponent": (109, 69, 7, 0)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=217 match=3888713 gs=drawing",
        {"comp_id": 11, "team_id": 217, "match_id": 3888713, "game_state": "drawing"},
        {"primary": (32, 4, 4, 0), "opponent": (33, 23, 3, 0)},
        {"gs_filtered": True},
    ),
]

# Oracle is the TRUNCATED value; mart must be >= on every stage.
# Delta = correctness fix quantified.
_GREATER_OR_EQUAL_FIXTURES: list[tuple[str, dict, dict, dict]] = [
    (
        "comp=11 team=217 match=None gs=None",
        {"comp_id": 11, "team_id": 217, "match_id": None, "game_state": None},
        {"primary": (47201, 13812, 3523, 570), "opponent": (38526, 8186, 1933, 178)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=217 match=None gs=drawing",
        {"comp_id": 11, "team_id": 217, "match_id": None, "game_state": "drawing"},
        {"primary": (23244, 13777, 3240, 251), "opponent": (19766, 8302, 1762, 102)},
        {"gs_filtered": True},
    ),
]


def _tuple_from_dict(stages: dict[str, int]) -> tuple[int, int, int, int]:
    return (
        stages["possessions"],
        stages["a3_entries"],
        stages["shots"],
        stages["goals"],
    )


@pytest.mark.parametrize(
    ("label", "params", "oracle", "meta"),
    _EXACT_FIXTURES,
    ids=[f[0] for f in _EXACT_FIXTURES],
)
def test_mart_parity_exact(label: str, params: dict, oracle: dict, meta: dict) -> None:
    """Mart must reproduce the live-oracle tuple with zero delta."""
    from queries.funnel import fetch_funnel_agg

    df = fetch_funnel_agg(**params)
    assert not df.empty, f"mart returned empty rows for {label}"

    rolled = _rollup(df, team_id=params["team_id"], gs_filtered=meta["gs_filtered"])
    assert _tuple_from_dict(rolled["primary"]) == oracle["primary"], f"primary {label}"
    assert _tuple_from_dict(rolled["opponent"]) == oracle["opponent"], f"opponent {label}"


@pytest.mark.parametrize(
    ("label", "params", "oracle", "meta"),
    _GREATER_OR_EQUAL_FIXTURES,
    ids=[f[0] for f in _GREATER_OR_EQUAL_FIXTURES],
)
def test_mart_parity_greater_or_equal(label: str, params: dict, oracle: dict, meta: dict) -> None:
    """Mart closes the LIMIT-500000 correctness gap — must be >= oracle per stage."""
    from queries.funnel import fetch_funnel_agg

    df = fetch_funnel_agg(**params)
    assert not df.empty, f"mart returned empty rows for {label}"

    rolled = _rollup(df, team_id=params["team_id"], gs_filtered=meta["gs_filtered"])

    primary = _tuple_from_dict(rolled["primary"])
    opponent = _tuple_from_dict(rolled["opponent"])

    for i, stage in enumerate(("possessions", "a3_entries", "shots", "goals")):
        assert primary[i] >= oracle["primary"][i], (
            f"primary {stage} regressed: {primary[i]} < oracle {oracle['primary'][i]} ({label})"
        )
        assert opponent[i] >= oracle["opponent"][i], (
            f"opponent {stage} regressed: {opponent[i]} < oracle {oracle['opponent'][i]} ({label})"
        )
