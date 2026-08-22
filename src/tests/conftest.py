"""Shared test configuration."""

from __future__ import annotations

import json
import sys

# On Windows, jaxlib native extensions fail to load if pandas/matplotlib
# load OpenBLAS DLLs first (DLL load order conflict). Importing jax early
# — before conftest triggers matplotlib — avoids the clash.
if sys.platform == "win32":
    try:
        import jax  # noqa: F401
    except ImportError:
        pass

import matplotlib
import pytest

matplotlib.use("Agg")


@pytest.fixture(autouse=True)
def _restore_pyspark_modules() -> object:
    """Snapshot + restore `pyspark.*` entries in sys.modules around every test.

    Several test modules (test_guards.py, test_guard_conformance.py, etc.)
    inject `MagicMock()` into sys.modules['pyspark.sql'] to exercise guard
    helpers without a real Spark session but do NOT tear the mock down.
    That mock persists into subsequent tests — e.g. test_match_summary_render.py's
    plotly-based tests — where plotly's narwhals dependency tries
    `isinstance(df, pyspark_sql.DataFrame)` and gets a MagicMock (not a type),
    raising `TypeError: isinstance() arg 2 must be a type ...`.

    This autouse fixture snapshots any pyspark entries present before each test
    and restores exactly that state afterwards. Entries added during the test
    are removed; entries replaced are restored to their original object.
    """
    pyspark_entries = {k: v for k, v in sys.modules.items() if k == "pyspark" or k.startswith("pyspark.")}
    yield
    current = [k for k in list(sys.modules) if k == "pyspark" or k.startswith("pyspark.")]
    for k in current:
        if k not in pyspark_entries:
            del sys.modules[k]
        else:
            sys.modules[k] = pyspark_entries[k]


# ---------------------------------------------------------------------------
# Gradient Sports bronze fixture helpers (shared across test modules)
# ---------------------------------------------------------------------------


def _make_gs_bronze_row(
    *,
    match_id: str = "10502",
    game_id: float = 10502.0,
    game_event_id: float = 6498520.0,
    period: float = 1.0,
    start_game_clock: float = 2800.0,
    # Raw absolute event clock (top-level GS scalar). silly-kicks 4.89.0 requires `start_time`
    # (mapped from bronze `startTime`) as the chronological sort tiebreak + foul-time imputation
    # basis; `event_time` (bronze `eventTime`) is the NaN-`start_time` fallback. Default to the
    # game clock so `_make_gs_bronze_df`'s per-row increment keeps them monotone (matches the real
    # feed's 0-inversion property); pass explicitly to exercise divergence.
    start_time: float | None = None,
    event_time: float | None = None,
    player_id: float = 12345.0,
    team_id: float = 366.0,
    home_team: bool = True,
    possession_event_id: float = 8001.0,
    game_event_type: str = "OTB",
    possession_event_type: str = "PA",
    pass_type: str = "Short",  # noqa: S107 — not a password; GS event attribute
    pass_outcome_type: str = "Complete",  # noqa: S107 — not a password
    setpiece_type: str = "O",
    home_team_start_left: bool = True,
    home_team_start_left_extratime: object = None,
    visibility: str | None = None,
    ball_x: float = 10.5,
    ball_y: float = 20.3,
    **overrides: object,
) -> dict:
    """Build a single synthetic bronze row with actual GS dot-notation columns.

    Column names match the real bronze schema (DESCRIBE verified):
    - gameEventId (top-level), NOT possessionEvents.eventId
    - gameEvents.period, NOT possessionEvents.periodId
    - gameEvents.startGameClock, NOT possessionEvents.timeSeconds
    - gameEvents.playerId, NOT possessionEvents.playerId
    - gameEvents.teamId, NOT possessionEvents.teamId
    - gameEvents.setpieceType (lowercase p)
    - gameEvents.homeTeam (boolean) -- used to derive home_team_id
    - ball (JSON string) -- NOT possessionEvents.ballX/ballY
    """
    if start_time is None:
        start_time = start_game_clock
    if event_time is None:
        event_time = start_time
    row: dict = {
        "match_id": match_id,
        # PR-2a R-6b: the converter left-joins bronze.gradientsports_metadata onto the events
        # before groupBy, so every production group frame carries `visibility`. The UDF reads
        # it pre-try and does NOT guard for absence — a tolerant read would mask a broken join
        # by threading None for every match. Defaults to None because that is the CURRENT live
        # state (all 64 metadata rows hold NULL until the _backfill_artifacts run populates
        # them); pass visibility="private"/"public" to exercise a populated feed.
        "visibility": visibility,
        "gameId": game_id,
        "gameEventId": game_event_id,
        "possessionEventId": possession_event_id,
        # Top-level absolute-clock scalars (double in bronze) — silly-kicks 4.89.0 required input.
        "startTime": start_time,
        "eventTime": event_time,
        "gameEvents.gameEventType": game_event_type,
        "gameEvents.period": period,
        "gameEvents.startGameClock": start_game_clock,
        "gameEvents.playerId": player_id,
        "gameEvents.teamId": team_id,
        "gameEvents.homeTeam": home_team,
        "gameEvents.setpieceType": setpiece_type,
        "possessionEvents.possessionEventType": possession_event_type,
        "possessionEvents.passType": pass_type,
        "possessionEvents.passOutcomeType": pass_outcome_type,
        # ball is a JSON string in bronze (serialized by gradientsports_events.py)
        "ball": json.dumps([{"visibility": "VISIBLE", "x": ball_x, "y": ball_y, "z": 0.0}]),
        "stadiumMetadata.homeTeamStartLeft": home_team_start_left,
    }
    # Only include ET column when explicitly provided (it's absent for most matches)
    if home_team_start_left_extratime is not None:
        row["stadiumMetadata.homeTeamStartLeftExtraTime"] = home_team_start_left_extratime
    row.update(overrides)
    return row
