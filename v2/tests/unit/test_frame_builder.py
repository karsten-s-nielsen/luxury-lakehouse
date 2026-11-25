"""The canonical silly-kicks frame builder (spec D8; plan Task 3c).

`frames.build_match_frame` turns v2's per-provider tracking bronze into silly-kicks' `TRACKING_FRAMES_COLUMNS` by
shaping the rows into each provider's `convert_to_frames` input (silly-kicks owns the coordinate transform to the
SPADL 0-105/0-68 pitch frame, the per-period direction, and the GK-derive fallback) and supplying `is_goalkeeper`
from the provider roster (ruling R7: native from the roster position, derived fallback inside `convert_to_frames`).

These unit tests exercise the pure shaping + GK + scorer round-trip on a synthetic gradientsports-shaped frame (no
catalog): the de-risking contract that `compute_pitch_control` runs on the built frame. The catalog-backed
`build_match_frame` read is covered by the Task 11 e2e on a seeded lake.
"""

# Untyped-library boundary (pandas + silly_kicks ship no stubs): relax the strict "unknown"-family rules for this
# test module, matching src/lakehouse/compute/frames.py.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pytest
from silly_kicks.tracking import skillcorner as sk_skillcorner  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking.gradientsports import (  # pyright: ignore[reportMissingTypeStubs]
    EXPECTED_INPUT_COLUMNS,
    convert_to_frames,
)
from silly_kicks.tracking.pitch_control import compute_pitch_control  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking.preprocess import (  # pyright: ignore[reportMissingTypeStubs]
    PreprocessConfig,
    derive_velocities,
    smooth_frames,
)

from lakehouse.compute.frames import (
    build_match_frame,
    resolve_roster_goalkeepers,
    shape_long_input,
    shape_skillcorner_input,
)

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

HOME, AWAY = 100, 200
HOME_GK, AWAY_GK = 1, 11  # player_id of each team's rostered goalkeeper


def _raw_gradientsports() -> pd.DataFrame:
    """A synthetic gradientsports-shaped raw frame set (EXPECTED_INPUT_COLUMNS), centre-origin metres, is_goalkeeper
    seeded False (the roster resolver sets it). Two teams of 3 + a ball row, 8 frames at 25 Hz so velocity derives."""
    rows: list[dict[str, object]] = []
    players = [
        (HOME_GK, HOME, -50.0, 0.0),
        (2, HOME, -20.0, 10.0),
        (3, HOME, 5.0, -5.0),
        (AWAY_GK, AWAY, 50.0, 0.0),
        (12, AWAY, 20.0, -10.0),
        (13, AWAY, -5.0, 5.0),
    ]
    for frame_id in range(8):
        t = frame_id / 25.0
        for pid, team, x, y in players:
            rows.append(
                {
                    "game_id": 1,
                    "period_id": 1,
                    "frame_id": frame_id,
                    "time_seconds": t,
                    "frame_rate": 25.0,
                    "player_id": pid,
                    "team_id": team,
                    "is_ball": False,
                    "is_goalkeeper": False,
                    "x_centered": x + frame_id * 0.3,
                    "y_centered": y,
                    "z": 0.0,
                    "speed_native": np.nan,
                    "ball_state": "alive",
                }
            )
        rows.append(
            {
                "game_id": 1,
                "period_id": 1,
                "frame_id": frame_id,
                "time_seconds": t,
                "frame_rate": 25.0,
                "player_id": pd.NA,
                "team_id": pd.NA,
                "is_ball": True,
                "is_goalkeeper": False,
                "x_centered": 0.0 + frame_id * 0.3,
                "y_centered": 0.0,
                "z": 0.5,
                "speed_native": np.nan,
                "ball_state": "alive",
            }
        )
    df = pd.DataFrame(rows).astype({"player_id": "Int64", "team_id": "Int64"})
    assert EXPECTED_INPUT_COLUMNS <= set(df.columns)  # the fixture matches the converter's input contract
    return df


def test_resolve_roster_goalkeepers_sets_native_flag() -> None:
    raw = _raw_gradientsports()
    out = resolve_roster_goalkeepers(raw, {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    gk = out[out["is_goalkeeper"]]
    gk_pids: Any = gk["player_id"]
    assert set(gk_pids.dropna().astype(int)) == {HOME_GK, AWAY_GK}  # exactly the two rostered keepers
    assert (out.loc[~out["is_ball"] & ~out["player_id"].isin([HOME_GK, AWAY_GK]), "is_goalkeeper"] == False).all()  # noqa: E712 - pandas element-wise comparison
    assert not out.loc[out["is_ball"], "is_goalkeeper"].any()  # the ball is never a goalkeeper


def test_unrostered_team_leaves_flag_for_the_derive_fallback() -> None:
    # a team with no roster GK keeps is_goalkeeper=False on its rows (convert_to_frames' positional fallback fires)
    raw = _raw_gradientsports()
    out = resolve_roster_goalkeepers(raw, {(HOME, HOME_GK)})  # AWAY has no roster GK
    assert out.loc[out["team_id"] == AWAY, "is_goalkeeper"].sum() == 0


def test_built_frame_drives_pitch_control() -> None:
    # the de-risking round-trip: roster GK -> convert_to_frames (coords -> 0-105/0-68 + direction) -> preprocess ->
    # compute_pitch_control runs and returns a surface over the built frame (spec D8).
    raw = resolve_roster_goalkeepers(_raw_gradientsports(), {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    frame, report = convert_to_frames(raw, home_team_id=HOME, home_team_start_left=True, output_convention="ltr")
    assert report.total_output_rows > 0
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()  # SPADL pitch frame
    assert frame.loc[frame["is_goalkeeper"], "is_goalkeeper_source"].isin(["native", "derived"]).all()
    cfg = PreprocessConfig.default()
    one = derive_velocities(smooth_frames(frame, config=cfg), config=cfg)
    frame_4: Any = one[one["frame_id"] == 4]
    surface = compute_pitch_control(frame_4, attacking_team_id=HOME)
    assert np.isfinite(surface.surface).all() and surface.surface.shape[0] > 0


def _v2_views() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Synthetic fct_tracking_frames + fct_tracking_ball rows for one gradientsports match (centre-origin x_mm/y_mm,
    time_seconds period-relative on the ball view per R8)."""
    players = [
        (HOME_GK, HOME, -50_000, 0),
        (2, HOME, -20_000, 10_000),
        (AWAY_GK, AWAY, 50_000, 0),
        (12, AWAY, 20_000, -10_000),
    ]
    frame_rows: list[dict[str, object]] = []
    ball_rows: list[dict[str, object]] = []
    for frame_id in range(8):
        for pid, team, x_mm, y_mm in players:
            frame_rows.append(
                {
                    "canonical_match_key": 1,
                    "canonical_player_key": pid,
                    "team_key": team,
                    "canonical_team_key": team,
                    "provider": "gradientsports",
                    "period": 1,
                    "frame": frame_id,
                    "x_mm": x_mm + frame_id * 300,
                    "y_mm": y_mm,
                    "is_visible": True,
                    "_visibility": "visible",
                }
            )
        ball_rows.append(
            {
                "canonical_match_key": 1,
                "period": 1,
                "frame": frame_id,
                "ball_x_mm": frame_id * 300,
                "ball_y_mm": 0,
                "ball_detected": True,
                "possession_team_key": None,
                "time_seconds": frame_id / 25.0,
            }
        )
    frames = pd.DataFrame(frame_rows).astype({"canonical_player_key": "Int64", "canonical_team_key": "Int64"})
    return frames, pd.DataFrame(ball_rows)


def test_shape_long_input_feeds_convert_to_frames() -> None:
    # v2 gold-view rows -> long EXPECTED_INPUT_COLUMNS (gradientsports/idsse) -> resolve GK -> convert round-trip.
    frames, ball = _v2_views()
    raw = shape_long_input(frames, ball, frame_rate=25.0)
    assert EXPECTED_INPUT_COLUMNS <= set(raw.columns)
    assert raw["is_ball"].sum() == 8  # one ball row per frame
    assert (raw.loc[~raw["is_ball"], "x_centered"].abs() <= 60).all()  # centre-origin metres (x_mm/1000)
    raw = resolve_roster_goalkeepers(raw, {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    frame, report = convert_to_frames(raw, home_team_id=HOME, home_team_start_left=True, output_convention="ltr")
    assert report.total_output_rows > 0
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()


def test_shape_skillcorner_input_feeds_convert_to_frames() -> None:
    # v2 gold-view rows -> WIDE skillcorner EXPECTED_INPUT_COLUMNS (ball as columns, no is_ball) -> convert round-trip.
    frames, ball = _v2_views()
    bronze = shape_skillcorner_input(frames, ball, frame_rate=25.0, pitch_length=105.0, pitch_width=68.0)
    assert set(sk_skillcorner.EXPECTED_INPUT_COLUMNS) <= set(bronze.columns)
    assert "is_ball" not in bronze.columns  # wide shape: the ball is ball_x/ball_y columns, not a row
    bronze = resolve_roster_goalkeepers(bronze, {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    frame, report = sk_skillcorner.convert_to_frames(
        bronze, home_team_id=HOME, output_convention="ltr", home_team_start_left=True
    )
    assert report.total_output_rows > 0
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()


class _FakeResult:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def df(self) -> pd.DataFrame:
        return self._df


class _FakeConn:
    """A stand-in DuckDB connection: dispatches a SELECT to a synthetic table by the table name in the SQL."""

    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self._tables = tables

    def execute(self, sql: str, params: Any = None) -> _FakeResult:
        for name, df in self._tables.items():
            if name in sql:
                return _FakeResult(df)
        raise AssertionError(f"unexpected SQL: {sql}")


def _fake_cat(
    frames: pd.DataFrame,
    ball: pd.DataFrame,
    roster: pd.DataFrame | None = None,
    *,
    extra_time_left: bool | None = None,
    frame_rate: float | None = 25.0,
) -> WorkingCatalog:
    params = pd.DataFrame(
        [
            {
                "home_team_key": HOME,
                "home_team_start_left": True,
                "home_team_start_left_extra_time": extra_time_left,
                "frame_rate": frame_rate,
                "pitch_length": 105.0,
                "pitch_width": 68.0,
            }
        ]
    )
    if roster is None:  # default: one keeper per team; the GK-sub test injects a team's whole keeper set
        roster = pd.DataFrame({"canonical_team_key": [HOME, AWAY], "canonical_player_key": [HOME_GK, AWAY_GK]})
    tables = {
        "fct_tracking_frames": frames,
        "fct_tracking_ball": ball,
        "fct_match_params": params,
        "fct_roster_gk": roster,
    }
    return cast("WorkingCatalog", type("_FakeCat", (), {"conn": _FakeConn(tables)})())


def test_build_match_frame_long_provider() -> None:
    # build_match_frame glue (reads -> long shape -> GK -> convert), on a fake catalog connection.
    frames, ball = _v2_views()
    frame = build_match_frame(_fake_cat(frames, ball), 1, provider="gradientsports", record_keys=(1,))
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()
    assert frame.loc[frame["is_goalkeeper"], "is_goalkeeper_source"].isin(["native", "derived"]).all()


def test_build_match_frame_skillcorner() -> None:
    frames, ball = _v2_views()
    frame = build_match_frame(_fake_cat(frames, ball), 1, provider="skillcorner", record_keys=(1,))
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()


@pytest.mark.parametrize("provider", ["gradientsports", "skillcorner"])
def test_build_match_frame_needs_a_frame_rate(provider: str) -> None:
    """A null `fct_match_params.frame_rate` fails loud here, not deep in silly-kicks (2026-11-06 Mac run: smooth_frames
    "single positional indexer is out-of-bounds", merge_asof "Merge keys contain null values on right side")."""
    frames, ball = _v2_views()
    with pytest.raises(ValueError, match="no frame_rate for match 1"):
        build_match_frame(_fake_cat(frames, ball, frame_rate=None), 1, provider=provider, record_keys=(1,))


HOME_GK2 = 4  # the home keeper who comes on at half-time, replacing HOME_GK


def _v2_views_gk_sub() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Two-period fct_tracking_frames where HOME's keeper changes at half-time: HOME_GK plays period 1, HOME_GK2 plays
    period 2 (HOME_GK absent). Keepers sit deep in their own box; outfielders at midfield. Six frames per half so each
    keeper clears the derive's frame-count floor."""
    frame_rows: list[dict[str, object]] = []
    ball_rows: list[dict[str, object]] = []
    by_period: dict[int, list[tuple[int, int, int, int]]] = {
        1: [(HOME_GK, HOME, -50_000, 0), (2, HOME, 0, 8_000), (AWAY_GK, AWAY, 50_000, 0), (12, AWAY, 0, -8_000)],
        2: [(HOME_GK2, HOME, -50_000, 0), (2, HOME, 0, 8_000), (AWAY_GK, AWAY, 50_000, 0), (12, AWAY, 0, -8_000)],
    }
    for period, players in by_period.items():
        for frame_id in range(6):
            for pid, team, x_mm, y_mm in players:
                frame_rows.append(
                    {
                        "canonical_match_key": 1,
                        "canonical_player_key": pid,
                        "team_key": team,
                        "canonical_team_key": team,
                        "provider": "gradientsports",
                        "period": period,
                        "frame": frame_id,
                        "x_mm": x_mm + frame_id * 200,
                        "y_mm": y_mm,
                        "is_visible": True,
                        "_visibility": "visible",
                    }
                )
            ball_rows.append(
                {
                    "canonical_match_key": 1,
                    "period": period,
                    "frame": frame_id,
                    "ball_x_mm": frame_id * 200,
                    "ball_y_mm": 0,
                    "ball_detected": True,
                    "possession_team_key": None,
                    "time_seconds": frame_id / 25.0,
                }
            )
    frames = pd.DataFrame(frame_rows).astype({"canonical_player_key": "Int64", "canonical_team_key": "Int64"})
    return frames, pd.DataFrame(ball_rows)


def test_build_match_frame_flags_the_substitute_goalkeeper() -> None:
    """A half-time GK change must be substitution-safe (spec D8). The shipped roster-starter path flagged only HOME_GK
    and suppressed convert_to_frames' derive for HOME (it had a rostered keeper), so period-2 frames carried the wrong
    or absent goalkeeper (review B-CALC-IMPL-01). The reconciled contract lets convert_to_frames derive every team, so
    the period-2 keeper HOME_GK2 is flagged on its own frames."""
    frames, ball = _v2_views_gk_sub()
    # fct_roster_gk now carries the team's WHOLE keeper set: HOME's starter AND the half-time substitute.
    roster = pd.DataFrame(
        {"canonical_team_key": [HOME, HOME, AWAY], "canonical_player_key": [HOME_GK, HOME_GK2, AWAY_GK]}
    )
    frame = build_match_frame(_fake_cat(frames, ball, roster), 1, provider="gradientsports", record_keys=(1,))
    gk = frame[
        frame["is_goalkeeper"].astype("boolean").fillna(False) & ~frame["is_ball"].astype("boolean").fillna(False)
    ]
    p1_home: Any = gk.loc[(gk["period_id"] == 1) & (gk["team_id"] == HOME), "player_id"]
    p2_home: Any = gk.loc[(gk["period_id"] == 2) & (gk["team_id"] == HOME), "player_id"]
    assert HOME_GK in set(p1_home.dropna().astype(int))  # the starter is the period-1 keeper
    assert HOME_GK2 in set(
        p2_home.dropna().astype(int)
    )  # the SUB is flagged in period 2 (starter-only path fails here)


def test_build_match_frame_reads_only_declared_sql() -> None:
    """perf plan Task 3 (spec §7.4): every SQL the builder issues is exactly one declared `FRAME_INPUT_READS` read,
    so the input key (which hashes those reads) covers everything the frame is built from. The builder does no
    record-key lookup of its own: it passes the driver's record keys to the frames and ball reads (spec §7.4a)."""
    from lakehouse.compute.frames import FRAME_INPUT_READS

    frames, ball = _v2_views()
    cat = _fake_cat(frames, ball)
    fake: Any = cat.conn
    executed: list[tuple[str, Any]] = []
    original = fake.execute

    def recording(sql: str, params: Any = None) -> _FakeResult:
        executed.append((sql, params))
        return original(sql, params)

    fake.execute = recording
    build_match_frame(cat, 1, provider="gradientsports", record_keys=(1, 70))
    declared = {read.sql for read in FRAME_INPUT_READS}
    sqls = [sql for sql, _ in executed]
    assert sqls and set(sqls) <= declared, f"undeclared reads: {set(sqls) - declared}"
    assert len(sqls) == len(FRAME_INPUT_READS)
    by_sql = dict(executed)
    assert by_sql[FRAME_INPUT_READS[0].sql] == [1, "gradientsports", [1, 70]]
    assert by_sql[FRAME_INPUT_READS[1].sql] == [1, [1, 70]]


def test_build_match_frame_handles_extra_time_periods() -> None:
    """Real-data bug (Mac profile 2026-11-06): a gradientsports match with extra-time periods (3, 4) raised
    `ValueError: ... data contains ET periods ... but home_team_start_left_extratime was not provided`, because the
    builder never passed `fct_match_params.home_team_start_left_extra_time` to the long converter."""
    frames, ball = _v2_views()
    et_frames = frames.assign(period=3, frame=frames["frame"] + 1000)
    et_ball = ball.assign(period=3, frame=ball["frame"] + 1000)
    both_frames = pd.concat([frames, et_frames], ignore_index=True)
    both_ball = pd.concat([ball, et_ball], ignore_index=True)
    frame = build_match_frame(
        _fake_cat(both_frames, both_ball, extra_time_left=False), 1, provider="gradientsports", record_keys=(1,)
    )
    assert set(frame["period_id"].unique()) == {1, 3}
    assert frame["x"].between(0, 105).all() and frame["y"].between(0, 68).all()


# --- duplicate tracking rows fail loud at use (owner 2026-11-08) ------------------------------------------------------


def test_a_duplicate_player_row_fails_the_match() -> None:
    frames, ball = _v2_views()
    doubled = pd.concat([frames, frames.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match=r"duplicate tracking rows for match 1: 1 \(period, frame, player\)"):
        build_match_frame(_fake_cat(doubled, ball), 1, provider="gradientsports", record_keys=(1,))


def test_a_duplicate_ball_row_fails_the_match() -> None:
    frames, ball = _v2_views()
    doubled = pd.concat([ball, ball.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match=r"duplicate tracking rows for match 1: 1 \(period, frame\) ball"):
        build_match_frame(_fake_cat(frames, doubled), 1, provider="gradientsports", record_keys=(1,))
