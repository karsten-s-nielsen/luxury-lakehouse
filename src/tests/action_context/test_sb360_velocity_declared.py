"""SB360 velocity-availability declaration (ADR-063; silly-kicks 4.87.0 velocity fail-fast).

From silly-kicks 4.79/4.80 the pitch-control aggregators refuse frames that carry neither ``vx``/``vy``
nor an explicit ``speed_source`` marker (they ``raise ValueError`` rather than silently zero-fill). The
SB360 chain (``_enrich_sb360_match``) feeds the aggregators the output of ``snapshot_to_tracking_frames``,
which stamps every synthetic frame with ``speed_source = SPEED_SOURCE_UNAVAILABLE`` ("unavailable"). This
test pins that invariant on the real converter over the committed ``statsbomb/3835328`` snapshot fixture,
so a future switch to a hand-rolled frame path that forgot the marker fails loudly here (spec §6.3).
"""

from __future__ import annotations

import pandas as pd
from silly_kicks.spadl import add_game_state
from silly_kicks.tracking import SPEED_SOURCE_UNAVAILABLE, snapshot_to_tracking_frames

_ROOT = "src/tests/fixtures/action_context/statsbomb/3835328"


def test_sb360_frames_declare_velocity_unavailable() -> None:
    actions = pd.read_parquet(f"{_ROOT}/actions.parquet")
    snapshots = pd.read_parquet(f"{_ROOT}/sb360.parquet")

    out = add_game_state(actions)
    frames, _links = snapshot_to_tracking_frames(snapshots, out)

    assert not frames.empty, "SB360 fixture produced zero synthetic frames — cannot verify the marker"
    assert "speed_source" in frames.columns, "SB360 frames lack speed_source — 4.87.0 pitch-control aggregators raise"
    assert (frames["speed_source"] == SPEED_SOURCE_UNAVAILABLE).all(), (
        "every SB360 synthetic frame must declare velocity-unavailable (ADR-063)"
    )
