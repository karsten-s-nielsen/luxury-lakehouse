"""pitch_control compute — the pure frame -> pitch_control_values core (spec D3/D8; plan Task 4).

Builds a canonical frame the real way (v2-view rows -> shape_long_input -> resolve GK -> convert_to_frames) and scores
it with `pitch_control_values`, asserting the bronze shape + that each frame's two team fractions are in [0, 1] and
complementary. The match-iterating ComputeStep (catalog read + bronze write) is covered by the seeded e2e (Task 11).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from collections.abc import Callable

import pytest
from trackingkit import AWAY, HOME, canonical_frame

from lakehouse.compute.control import ComputeAbandoned
from lakehouse.compute.pitch_control import PITCH_CONTROL_SCHEMA, pitch_control_values


def test_pitch_control_values_shape_and_range() -> None:
    table = pitch_control_values(canonical_frame(), home_team_id=HOME, away_team_id=AWAY)
    assert table.schema.equals(PITCH_CONTROL_SCHEMA)
    assert table.num_rows == 10 * 2  # two teams per frame
    df = table.to_pandas()
    assert set(df["canonical_team_key"].unique()) == {HOME, AWAY}
    assert df["control_fraction"].between(0.0, 1.0).all()
    assert (df["canonical_match_key"] == 7).all()


def test_team_fractions_are_complementary() -> None:
    df = pitch_control_values(canonical_frame(), home_team_id=HOME, away_team_id=AWAY).to_pandas()
    per_frame = df.groupby(["period", "frame"])["control_fraction"].sum()
    assert per_frame.between(0.999, 1.001).all()  # home + away control sums to 1 each frame


def _stop_on_second_call() -> Callable[[], bool]:
    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2

    return should_stop


def test_pitch_control_abandons_when_should_stop_turns_true() -> None:
    with pytest.raises(ComputeAbandoned):
        pitch_control_values(
            canonical_frame(), home_team_id=HOME, away_team_id=AWAY, should_stop=_stop_on_second_call(), check_every=1
        )


def test_pitch_control_output_unchanged_with_should_stop_none() -> None:
    frame = canonical_frame()
    plain = pitch_control_values(frame, home_team_id=HOME, away_team_id=AWAY)
    checked = pitch_control_values(
        frame, home_team_id=HOME, away_team_id=AWAY, should_stop=lambda: False, check_every=1
    )
    assert plain.equals(checked)
    assert plain.equals(pitch_control_values(frame, home_team_id=HOME, away_team_id=AWAY, should_stop=None))
