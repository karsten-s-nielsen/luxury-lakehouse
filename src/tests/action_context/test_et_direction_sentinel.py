"""silly-kicks 4.0.0 ET-direction sentinel for the lakehouse hexagon.

silly-kicks 4.0's symmetric ET guard (``require_et_direction``) raises if
any per-period-absolute converter is called with frames containing
``period_id in {3, 4}`` but no ``home_team_start_left_extratime``. The
lakehouse extended ``MatchMeta`` with the ET field and plumbed it through
``pipeline.py:_convert_tracking_batch`` to ``convert_to_frames``.

This sentinel asserts that the lakehouse-side wiring **actually reaches the
silly-kicks guard** for ET-bearing batches — converting "we passed it through"
into "CI verifies the pass-through hits the converter under load". Without
this, a future refactor that drops the kwarg silently would pass all the
existing tests (none of them exercise ET periods) until prod hit an ET match.

See: silly-kicks PR-S70 plan §4 (Phase B sentinel test); spec v3 §4
(cross-repo coordination + sentinel pattern); lakehouse memory
``project_et_direction_section_8_audit.md`` (no ET in bronze today — this
sentinel is the only ET coverage the lakehouse owns).
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context.pipeline import _convert_tracking_batch
from analytics.action_context.work_unit import MatchMeta


def _synthetic_idsse_et_bronze_tracking() -> pd.DataFrame:
    """Build a tiny IDSSE-bronze-shaped tracking DataFrame with ET-period rows.

    Two frames in period 3 (ET first half) + 2 teams x 11 players + ball
    denormalised per row (bronze convention — ``ball_x``/``ball_y`` on every
    player row).
    """
    rows: list[dict[str, object]] = []
    home_tid, away_tid = "DFL-CLU-000008", "DFL-CLU-00000G"
    for frame_id in (100, 101):
        for team_id, side in ((home_tid, "left"), (away_tid, "right")):
            for p in range(11):
                x_offset = 10.0 + p * 5.0 if side == "left" else 55.0 + p * 5.0
                rows.append(
                    {
                        "match_id": "J99ET01",
                        "period": 3,
                        "frame": frame_id,
                        "timestamp": float(frame_id - 100) * 0.04,
                        "x": x_offset,
                        "y": 30.0,
                        "s": 0.0,
                        "ball_status": "1",
                        "frame_rate": 25,
                        "player_id": f"{team_id}_p{p}",
                        "team_id": team_id,
                        "is_goalkeeper": p == 0,
                        "ball_x": 50.0,
                        "ball_y": 34.0,
                        "ball_z": 0.0,
                        "ball_s": 1.0,
                    }
                )
    return pd.DataFrame(rows)


def _synthetic_actions() -> pd.DataFrame:
    """Minimal actions DataFrame with a period-3 action — pipeline.py's frame
    converters reference ``actions["game_id"]`` for ``int(.iloc[0])`` casting
    on some providers, so include game_id."""
    return pd.DataFrame(
        [
            {
                "action_id": 1,
                "type_id": 0,  # pass
                "period_id": 3,
                "time_seconds": 0.0,
                "team_id_native": "DFL-CLU-000008",
                "player_id_native": "DFL-CLU-000008_p1",
                "game_id": 99,
            }
        ]
    )


# ── Sentinel: lakehouse pipeline.py call-site reaches the silly-kicks 4.0 guard ──


def test_et_bearing_batch_without_flag_raises_via_silly_kicks_guard() -> None:
    """Lakehouse calls convert_to_frames(home_team_start_left_extratime=None)
    on an ET-period batch -> silly-kicks 4.0's require_et_direction raises.

    This test exists to catch a future refactor that drops the kwarg from
    pipeline.py:_convert_tracking_batch. If pipeline.py stops forwarding
    meta.home_team_start_left_extratime, ET batches would still appear to
    "work" against today's zero-ET-in-bronze state, but the moment an ET
    match arrived the guard would fire mid-batch in prod. This sentinel
    catches the drop at PR time.
    """
    meta = MatchMeta(home_team_id="DFL-CLU-000008", home_start_left=True, home_team_start_left_extratime=None)
    frames_bronze = _synthetic_idsse_et_bronze_tracking()
    actions = _synthetic_actions()

    with pytest.raises(ValueError, match=r"ET periods"):
        _convert_tracking_batch("idsse", frames_bronze, actions, meta)


def test_et_bearing_batch_with_flag_converts_without_raise() -> None:
    """Same fixture + correct ET flag -> no raise; converter accepts the pass-through."""
    meta = MatchMeta(home_team_id="DFL-CLU-000008", home_start_left=True, home_team_start_left_extratime=True)
    frames_bronze = _synthetic_idsse_et_bronze_tracking()
    actions = _synthetic_actions()

    # Should not raise — the ET flag is supplied.
    frames = _convert_tracking_batch("idsse", frames_bronze, actions, meta)
    assert len(frames) > 0
    # Confirm ET period rows survived the conversion.
    assert frames["period_id"].isin([3]).any()


def test_non_et_batch_with_none_flag_does_not_raise() -> None:
    """No ET periods in input -> silly-kicks 4.0's guard is a no-op regardless of flag.

    Regression guard: 4.0 must be bit-identical to 3.30 for non-ET data (which
    is 100% of lakehouse bronze today). If this fails, the guard fires too
    eagerly and would break the AC-1 production path on every match.
    """
    meta = MatchMeta(home_team_id="DFL-CLU-000008", home_start_left=True, home_team_start_left_extratime=None)
    # Same builder but flip period 3 → 1
    frames_bronze = _synthetic_idsse_et_bronze_tracking()
    frames_bronze = frames_bronze.assign(period=1)
    actions = _synthetic_actions().assign(period_id=1)

    # Must NOT raise — non-ET data with None ET flag is the steady state.
    frames = _convert_tracking_batch("idsse", frames_bronze, actions, meta)
    assert len(frames) > 0
    assert frames["period_id"].isin([1]).all()
