"""SK3-MIG invariant — post-conversion, both teams' shots cluster at high-x.

Regression test that catches any future re-introduction of a stray
direction-of-play mirror anywhere in the conversion call chain. The
canonical SPADL LTR invariant per silly-kicks 3.0.0 docstring:
"every team's actions are oriented as if the team plays from left to
right — shots cluster at high-x for both teams, GK actions cluster
at low-x for both teams."

Pre-3.0.0 broken state for StatsBomb / Wyscout: converter mis-applied
the away-team mirror, splitting per-team shot x across the midline (the
OPT-1 diagnostic). Pre-3.0.1 broken state for IDSSE / Sportec / Metrica:
converter declared ABSOLUTE_FRAME_HOME_RIGHT but data is
PER_PERIOD_ABSOLUTE — single mirror cannot correct it.

silly-kicks 3.0.1 (PR-S23) added required ``home_team_start_left`` kwarg
on Sportec + Metrica converters. Lakehouse derives the bool authoritatively
from DFL XML KickOff TeamLeft for IDSSE, empirically from period-1 SHOT
positions for Metrica (see ``ingestion.spadl_adapter`` helpers).

Companion to silly-kicks's own ``tests/invariants/test_period_orientation.py``
(PR-S23). This is the lakehouse-side boundary test that catches drift
introduced by anything OUR adapters do between bronze events and the
silly-kicks converter.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import silly_kicks.spadl.metrica
import silly_kicks.spadl.sportec
import silly_kicks.spadl.statsbomb
import silly_kicks.spadl.wyscout

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "silly_kicks_boundary"

# SPADL canonical LTR pitch: length 105m, midline 52.5m.
_PITCH_LENGTH: float = 105.0
_PITCH_MID: float = _PITCH_LENGTH / 2.0

# SPADL shot action types per dbt_project/models/marts/fct_funnel_stages_agg.sql:121
_SHOT_TYPES: frozenset[str] = frozenset({"shot", "shot_penalty", "shot_freekick"})

# Minimum per-team shot count for a meaningful per-team x-mean.
_MIN_SHOTS_PER_TEAM: int = 2


def _adapt_and_convert(source: str, df: pd.DataFrame) -> pd.DataFrame:
    """Run our adapter + silly-kicks converter mirroring production usage.

    Returns the SPADL action DataFrame with ``type_name`` joined via
    ``silly_kicks.spadl.add_names``. Each provider's branch invokes the
    same kwargs the production SPADL UDFs in ``spadl_conversion.py`` use.
    """
    import silly_kicks.spadl as spadl

    if source == "statsbomb":
        from ingestion.spadl_adapter import adapt_statsbomb_events

        home_team_id = int(df["team_id"].dropna().iloc[0])
        adapted = adapt_statsbomb_events(df, home_team_id)
        actions, _ = silly_kicks.spadl.statsbomb.convert_to_actions(adapted, home_team_id=home_team_id)
    elif source == "wyscout":
        from ingestion.spadl_adapter import adapt_wyscout_events

        home_team_id = int(df["teamId"].dropna().iloc[0])
        adapted = adapt_wyscout_events(df)
        actions, _ = silly_kicks.spadl.wyscout.convert_to_actions(adapted, home_team_id=home_team_id)
    elif source == "idsse":
        # IDSSE shaper + deriver moved to the silly-kicks DFL parse port
        # under delete-and-depend (ADR-031 T3 / Gate B).
        from silly_kicks.providers.sportec import (
            derive_idsse_home_team_start_left,
            shape_events_to_native,
        )

        df = df[~((df["period"] == 2) & (df["timestamp_seconds"] < 0))].reset_index(drop=True)
        adapted = shape_events_to_native(df)
        home_team_id_native = str(df["home_team_id_native"].dropna().iloc[0])
        home_start_left = derive_idsse_home_team_start_left(adapted, home_team_id_native)
        actions, _ = silly_kicks.spadl.sportec.convert_to_actions(
            adapted, home_team_id="home", home_team_start_left=home_start_left
        )
    elif source == "metrica":
        from ingestion.spadl_adapter import adapt_metrica_events_for_silly_kicks, derive_metrica_home_team_start_left

        adapted = adapt_metrica_events_for_silly_kicks(df)
        home_start_left = derive_metrica_home_team_start_left(adapted, home_team_value="Home")
        actions, _ = silly_kicks.spadl.metrica.convert_to_actions(
            adapted, home_team_id="Home", home_team_start_left=home_start_left
        )
    else:
        raise ValueError(f"unknown source {source!r}")

    return spadl.add_names(actions)


_PARAMETRIZE = pytest.mark.parametrize(
    "source,fixture",
    [
        ("statsbomb", "sb_match_7298.parquet"),
        ("wyscout", "ws_match_2576335.parquet"),
        ("idsse", "idsse_J03WMX.parquet"),
        ("metrica", "metrica_sample_game_1.parquet"),
    ],
)


@_PARAMETRIZE
def test_both_teams_shot_start_x_cluster_at_high_x(source, fixture) -> None:  # type: ignore[no-untyped-def]
    """Canonical SPADL LTR: both teams' avg shot ``start_x`` lands at HIGH-x.

    silly-kicks 3.0.0 canonical SPADL LTR docstring contract: "every team's
    actions are oriented as if the team plays from left to right — shots
    cluster at high-x for both teams." If any team's avg shot x lands BELOW
    the midline, the direction-of-play mirror is broken somewhere in the
    chain — for SB/Wyscout that means the silly-kicks 3.0.0 fix regressed,
    for IDSSE/Metrica that means our home_team_start_left derivation is
    feeding silly-kicks 3.0.1 the wrong value.

    Test runs against real production-shape bronze fixtures used by
    ``test_silly_kicks_boundary.py``.
    """
    df = pd.read_parquet(_FIXTURE_DIR / fixture)
    actions = _adapt_and_convert(source, df)

    shots = actions[actions["type_name"].isin(_SHOT_TYPES)]
    if len(shots) == 0:
        pytest.skip(f"{source} fixture has no shot actions — invariant cannot be tested")

    per_team = shots.groupby("team_id").agg(n=("start_x", "size"), avg_x=("start_x", "mean"))
    qualifying = per_team[per_team["n"] >= _MIN_SHOTS_PER_TEAM]
    if len(qualifying) < 1:
        pytest.skip(
            f"{source} fixture: no team has ≥{_MIN_SHOTS_PER_TEAM} shots. Per-team counts: {per_team['n'].to_dict()}"
        )

    low_x_teams = qualifying[qualifying["avg_x"] <= _PITCH_MID]

    assert low_x_teams.empty, (
        f"{source}: {len(low_x_teams)} team(s) have avg shot start_x <= {_PITCH_MID}m "
        f"(canonical SPADL LTR requires ALL teams cluster at high-x). "
        f"Per-team avg_x: {qualifying['avg_x'].to_dict()}. "
        f"Pre-silly-kicks-3.0.1 this was the broken state for IDSSE+Metrica; "
        f"pre-silly-kicks-3.0.0 it was the broken state for StatsBomb+Wyscout."
    )
