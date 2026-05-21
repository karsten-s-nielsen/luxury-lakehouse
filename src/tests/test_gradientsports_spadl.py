"""Tests for Gradient Sports SPADL conversion pipeline.

Fixtures use synthetic data -- GS is not license-cleared for committing
real bronze slices.

Bronze column mapping verified against:
  DESCRIBE soccer_analytics.bronze.gradientsports_events (264 columns)
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_gs_bronze_row(
    *,
    match_id: str = "10502",
    game_id: float = 10502.0,
    game_event_id: float = 6498520.0,
    period: float = 1.0,
    start_game_clock: float = 2800.0,
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
    row: dict = {
        "match_id": match_id,
        "gameId": game_id,
        "gameEventId": game_event_id,
        "possessionEventId": possession_event_id,
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


def _make_gs_bronze_df(n: int = 5, **kwargs: object) -> pd.DataFrame:
    """Build a synthetic bronze DataFrame with n rows.

    Auto-increments game_event_id and start_game_clock per row to avoid
    identical timestamps/IDs masking ordering or dedup bugs.  If either
    kwarg is explicitly passed, all rows get that fixed value instead.
    """
    rows = []
    for i in range(n):
        row_kwargs = {**kwargs}
        row_kwargs.setdefault("game_event_id", 6498520.0 + i)
        row_kwargs.setdefault("start_game_clock", 2800.0 + i * 10)
        rows.append(_make_gs_bronze_row(**row_kwargs))  # type: ignore[arg-type]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------


class TestAdaptGradientSportsEvents:
    def test_adapt_rename_completeness(self) -> None:
        """All 47 EXPECTED_INPUT_COLUMNS present after adaptation."""
        from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS

        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = _make_gs_bronze_df(n=3)
        adapted = adapt_gradientsports_events(pdf)
        missing = set(EXPECTED_INPUT_COLUMNS) - set(adapted.columns)
        assert not missing, f"Missing columns after adapt: {sorted(missing)}"

    def test_adapt_rename_map_covers_all_expected(self) -> None:
        """_GS_BRONZE_TO_SNAKE target values + derived columns == EXPECTED_INPUT_COLUMNS."""
        from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS

        from ingestion.spadl_adapter import _GS_BRONZE_TO_SNAKE

        mapped = set(_GS_BRONZE_TO_SNAKE.values())
        # These columns are derived (not simple renames) so not in the rename map:
        # event_id, period_id, time_seconds, player_id, team_id, set_piece_type,
        # ball_x, ball_y are produced by adapt_gradientsports_events directly.
        # challenge_winner_team_id, challenger_team_id are NaN-filled (no bronze source).
        derived = {
            "event_id",
            "period_id",
            "time_seconds",
            "player_id",
            "team_id",
            "set_piece_type",
            "ball_x",
            "ball_y",
            "challenge_winner_team_id",
            "challenger_team_id",
        }
        coverage = mapped | derived
        expected = set(EXPECTED_INPUT_COLUMNS)
        missing = expected - coverage
        assert not missing, f"Columns not covered by rename map or derived: {sorted(missing)}"

    def test_adapt_nan_fill_missing_columns(self) -> None:
        """Absent optional columns (tackle/shot) filled with NaN."""
        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = _make_gs_bronze_df(n=1)
        adapted = adapt_gradientsports_events(pdf)
        assert adapted["challenge_type"].isna().all()
        assert adapted["shot_type"].isna().all()
        assert adapted["tackle_attempt_type"].isna().all()

    def test_adapt_ball_json_parsing(self) -> None:
        """ball JSON string parsed to ball_x and ball_y floats."""
        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = _make_gs_bronze_df(n=1, ball_x=18.5, ball_y=-21.33)
        adapted = adapt_gradientsports_events(pdf)
        assert "ball_x" in adapted.columns
        assert "ball_y" in adapted.columns
        assert adapted["ball_x"].iloc[0] == pytest.approx(18.5)
        assert adapted["ball_y"].iloc[0] == pytest.approx(-21.33)

    def test_adapt_ball_json_null_and_malformed(self) -> None:
        """Null and malformed ball JSON values produce NaN, not errors."""
        from ingestion.spadl_adapter import adapt_gradientsports_events

        for bad_ball in [None, "[]", "invalid", ""]:
            row = _make_gs_bronze_row()
            row["ball"] = bad_ball
            pdf = pd.DataFrame([row])
            adapted = adapt_gradientsports_events(pdf)
            assert pd.isna(adapted["ball_x"].iloc[0]), f"ball_x should be NaN for ball={bad_ball!r}"
            assert pd.isna(adapted["ball_y"].iloc[0]), f"ball_y should be NaN for ball={bad_ball!r}"

    def test_adapt_derived_columns_from_game_events(self) -> None:
        """Columns sourced from gameEvents.* (not possessionEvents.*) are correct."""
        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = _make_gs_bronze_df(
            n=1,
            game_event_id=6498520.0,
            period=2.0,
            start_game_clock=3500.0,
            player_id=84.0,
            team_id=361.0,
            setpiece_type="CK",
        )
        adapted = adapt_gradientsports_events(pdf)
        assert adapted["event_id"].iloc[0] == 6498520.0
        assert adapted["period_id"].iloc[0] == 2.0
        assert adapted["time_seconds"].iloc[0] == 3500.0
        assert adapted["player_id"].iloc[0] == 84.0
        assert adapted["team_id"].iloc[0] == 361.0
        assert adapted["set_piece_type"].iloc[0] == "CK"

    def test_adapt_empty_match(self) -> None:
        """Empty DataFrame returns empty with correct columns."""
        from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS

        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = pd.DataFrame()
        adapted = adapt_gradientsports_events(pdf)
        assert len(adapted) == 0
        missing = set(EXPECTED_INPUT_COLUMNS) - set(adapted.columns)
        assert not missing

    def test_adapt_match_id_vs_game_id(self) -> None:
        """match_id (ingestion-added) preserved, gameId renamed to game_id."""
        from ingestion.spadl_adapter import adapt_gradientsports_events

        pdf = _make_gs_bronze_df(n=1, match_id="10502", game_id=10502.0)
        adapted = adapt_gradientsports_events(pdf)
        assert "game_id" in adapted.columns
        assert adapted["game_id"].iloc[0] == 10502.0


class TestExtractGradientSportsMatchMetadata:
    def test_extract_metadata_regular(self) -> None:
        """home_team_id derived from gameEvents.homeTeam + gameEvents.teamId."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        pdf = _make_gs_bronze_df(n=3, team_id=366.0, home_team=True, home_team_start_left=True)
        meta = extract_gradientsports_match_metadata(pdf)
        assert meta["home_team_id"] == 366
        assert meta["home_team_start_left"] is True

    def test_extract_metadata_away_team_rows_skipped(self) -> None:
        """home_team_id derived only from rows where homeTeam is True."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        rows = [
            _make_gs_bronze_row(team_id=366.0, home_team=True),
            _make_gs_bronze_row(team_id=999.0, home_team=False),
            _make_gs_bronze_row(team_id=366.0, home_team=True),
        ]
        pdf = pd.DataFrame(rows)
        meta = extract_gradientsports_match_metadata(pdf)
        assert meta["home_team_id"] == 366

    def test_extract_metadata_et(self) -> None:
        """home_team_start_left_extratime correctly extracted."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        pdf = _make_gs_bronze_df(n=3, home_team_start_left_extratime=False)
        meta = extract_gradientsports_match_metadata(pdf)
        assert meta["home_team_start_left_extratime"] is False

    def test_extract_metadata_et_absent(self) -> None:
        """Returns None for home_team_start_left_extratime when column absent."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        pdf = _make_gs_bronze_df(n=3)
        # ET column is only added to fixture when explicitly provided
        pdf = pdf.drop(columns=["stadiumMetadata.homeTeamStartLeftExtraTime"], errors="ignore")
        meta = extract_gradientsports_match_metadata(pdf)
        assert meta["home_team_start_left_extratime"] is None

    def test_extract_metadata_empty_raises(self) -> None:
        """Empty DataFrame raises ValueError."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        with pytest.raises(ValueError, match="empty"):
            extract_gradientsports_match_metadata(pd.DataFrame())

    def test_extract_metadata_direction_false(self) -> None:
        """home_team_start_left=False is correctly extracted."""
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        pdf = _make_gs_bronze_df(n=3, home_team_start_left=False, team_id=361.0, home_team=True)
        meta = extract_gradientsports_match_metadata(pdf)
        assert meta["home_team_start_left"] is False


class TestGradientSportsUdf:
    """Tests for _make_gradientsports_spadl_udf UDF closure."""

    def _run_udf(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Helper: run the GS UDF on a bronze DataFrame."""
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        udf_fn = _make_gradientsports_spadl_udf()
        return udf_fn(bronze_df)

    def test_udf_output_has_all_spadl_cols(self) -> None:
        """Output has all unified SPADL columns and input rows produce >0 actions."""
        pdf = _make_gs_bronze_df(n=5)
        result = self._run_udf(pdf)
        n_input = len(pdf)
        # Must produce at least 1 action and no more than input rows
        assert 0 < len(result) <= n_input
        # Spot-check critical columns exist
        for col in (
            "match_id",
            "data_source",
            "start_x",
            "start_y",
            "type_id",
            "team_id_native",
            "match_id_native",
            "player_id_native",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_udf_data_source(self) -> None:
        """data_source is 'gradientsports'."""
        pdf = _make_gs_bronze_df(n=3)
        result = self._run_udf(pdf)
        assert (result["data_source"] == "gradientsports").all()

    def test_udf_match_id_hashing(self) -> None:
        """match_id is hashed BIGINT, not the raw string."""
        from ingestion.spadl_adapter import hash_native_id_to_bigint

        pdf = _make_gs_bronze_df(n=3, match_id="10502")
        result = self._run_udf(pdf)
        expected_hash = hash_native_id_to_bigint("10502")
        assert (result["match_id"] == expected_hash).all()

    def test_udf_match_id_native(self) -> None:
        """match_id_native is the raw string."""
        pdf = _make_gs_bronze_df(n=3, match_id="10502")
        result = self._run_udf(pdf)
        assert (result["match_id_native"] == "10502").all()

    def test_udf_team_id_hashing(self) -> None:
        """team_id_native is string, team_id is hashed BIGINT."""
        from ingestion.spadl_adapter import hash_native_id_to_bigint

        pdf = _make_gs_bronze_df(n=3, team_id=366.0)
        result = self._run_udf(pdf)
        # team_id_native should be the string form
        assert result["team_id_native"].iloc[0] is not pd.NA
        # team_id should be the hashed BIGINT
        expected_hash = hash_native_id_to_bigint(str(366))
        assert result["team_id"].iloc[0] == expected_hash

    def test_udf_player_id_null_filled_for_hashing_pattern(self) -> None:
        """player_id overwritten to NA (Kimball hashing uses player_id_native instead)."""
        pdf = _make_gs_bronze_df(n=3)
        result = self._run_udf(pdf)
        assert result["player_id"].isna().all()
        assert result["competition_id"].isna().all()
        assert result["season_id"].isna().all()
        # player_id_native MUST be populated (from apply_player_id_native)
        assert result["player_id_native"].notna().any(), (
            "player_id_native should be populated for rows with valid player_id"
        )

    def test_udf_empty_match(self) -> None:
        """Empty input returns empty DataFrame with correct columns."""
        pdf = pd.DataFrame()
        result = self._run_udf(pdf)
        assert len(result) == 0

    def test_udf_away_first_row_metadata(self) -> None:
        """Metadata extraction works when the first rows are away-team rows."""
        rows = [
            _make_gs_bronze_row(team_id=999.0, home_team=False),
            _make_gs_bronze_row(team_id=999.0, home_team=False),
            _make_gs_bronze_row(team_id=366.0, home_team=True),
            _make_gs_bronze_row(team_id=366.0, home_team=True),
            _make_gs_bronze_row(team_id=999.0, home_team=False),
        ]
        pdf = pd.DataFrame(rows)
        result = self._run_udf(pdf)
        assert len(result) > 0
        # home_team_id_native should be "366" (the home team), not "999"
        assert (result["home_team_id_native"] == "366").all()

    def test_udf_tackle_qualifier_columns_present(self) -> None:
        """Tackle qualifier _native/_key column pairs exist in output."""
        pdf = _make_gs_bronze_df(n=3)
        result = self._run_udf(pdf)
        for col in (
            "tackle_winner_player_id_native",
            "tackle_winner_player_key",
            "tackle_winner_team_id_native",
            "tackle_winner_team_key",
            "tackle_loser_player_id_native",
            "tackle_loser_player_key",
            "tackle_loser_team_id_native",
            "tackle_loser_team_key",
        ):
            assert col in result.columns, f"Missing tackle qualifier column: {col}"

    def test_udf_tackle_challenge_event_produces_values(self) -> None:
        """Challenge events with explicit winner/loser produce non-NA tackle qualifiers."""
        # NOTE: If this assertion fails, the (OTB, CH) event type pair may not map
        # to a recognized tackle action in the silly-kicks converter's dispatch
        # table (gradientsports.py _EVENT_TYPE_MAP). Fix by:
        #   1. Check _EVENT_TYPE_MAP for which (game_event_type, possession_event_type)
        #      pairs produce "tackle" actions
        #   2. Update game_event_type/possession_event_type below to match
        #   3. Verify with real bronze data: SELECT DISTINCT "gameEvents.gameEventType",
        #      "possessionEvents.possessionEventType" FROM ... WHERE challenge columns populated
        pdf = _make_gs_bronze_df(
            n=3,
            game_event_type="OTB",
            possession_event_type="CH",
            **{
                "possessionEvents.challengeType": "Tackle",
                "possessionEvents.challengeOutcomeType": "Win",
                "possessionEvents.challengeWinnerPlayerId": 111.0,
                "possessionEvents.challengerPlayerId": 333.0,
            },
        )
        result = self._run_udf(pdf)
        tackle_rows = result[result["tackle_winner_player_id_native"].notna()]
        assert len(tackle_rows) > 0, "Expected at least 1 row with tackle_winner_player_id_native populated"
        assert (tackle_rows["tackle_winner_player_id_native"] == "111").all()

    def test_udf_coordinates_in_spadl_range(self) -> None:
        """Converted coordinates are in SPADL range [0,105] x [0,68]."""
        pdf = _make_gs_bronze_df(n=5, ball_x=10.5, ball_y=20.3)
        result = self._run_udf(pdf)
        assert len(result) > 0, "UDF must produce at least 1 action from 5 input rows"
        assert result["start_x"].dropna().between(0, 105).all()
        assert result["start_y"].dropna().between(0, 68).all()


class TestGuardIntegration:
    def test_gs_new_key_in_provider_metadata(self) -> None:
        """gs_new key must exist in _PROVIDER_METADATA_KEYS."""
        from ingestion.spadl_vaep import _PROVIDER_METADATA_KEYS

        assert "gs_new" in _PROVIDER_METADATA_KEYS
        assert _PROVIDER_METADATA_KEYS["gs_new"] == "gradientsports"

    def test_gs_in_chunk_sizes(self) -> None:
        """gradientsports must have a chunk size."""
        from ingestion.spadl_vaep import _CHUNK_SIZES

        assert "gradientsports" in _CHUNK_SIZES
        assert _CHUNK_SIZES["gradientsports"] == 50

    def test_gs_in_valid_chunk_providers(self) -> None:
        """gradientsports must be in _VALID_CHUNK_PROVIDERS."""
        from ingestion.spadl_vaep import _VALID_CHUNK_PROVIDERS

        assert "gradientsports" in _VALID_CHUNK_PROVIDERS

    def test_gs_in_run_chunk_converters(self) -> None:
        """gradientsports must be a valid provider in _run_chunk converters dict."""
        # Source inspection: we can't call _run_chunk without a SparkSession,
        # and the converters dict is a local variable, not an exposed attribute.
        # Limitation: a match in a comment or log message would pass vacuously --
        # if the dispatch dict is ever refactored to an importable mapping,
        # switch to direct membership assertion.
        import inspect

        from ingestion import spadl_vaep

        source = inspect.getsource(spadl_vaep._run_chunk)
        assert '"gradientsports"' in source or "'gradientsports'" in source


class TestHfLicenseGate:
    def test_publish_spadl_vaep_excludes_gs(self) -> None:
        """publish_spadl_vaep_hf.py SQL must exclude gradientsports."""
        from pathlib import Path

        script = Path("scripts/publish_spadl_vaep_hf.py").read_text()
        assert "gradientsports" in script, "HF gate missing -- SQL must filter out gradientsports"
        assert "!= 'gradientsports'" in script or "!= \\'gradientsports\\'" in script

    def test_publish_tracking_context_excludes_gs(self) -> None:
        """publish_tracking_context_hf.py SQL must exclude gradientsports."""
        from pathlib import Path

        script = Path("scripts/publish_tracking_context_hf.py").read_text()
        assert "gradientsports" in script, "HF gate missing -- SQL must filter out gradientsports"
        assert "!= 'gradientsports'" in script or "!= \\'gradientsports\\'" in script


class TestExtraTimeIntegration:
    """ET tests: WC2022 has ~5 ET matches (Argentina-France final, etc.)."""

    def test_et_match_does_not_crash(self) -> None:
        """Fixture with period 3/4 + valid ET flag -> conversion succeeds."""
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        # Build a fixture with period 1 + 2 + 3 rows and valid ET flag
        rows = []
        for period in [1.0, 1.0, 2.0, 2.0, 3.0]:
            rows.append(
                _make_gs_bronze_row(
                    period=period,
                    home_team_start_left_extratime=False,
                )
            )
        pdf = pd.DataFrame(rows)
        udf_fn = _make_gradientsports_spadl_udf()
        result = udf_fn(pdf)
        assert len(result) > 0
        assert (result["data_source"] == "gradientsports").all()

    def test_et_match_missing_flag_raises(self) -> None:
        """Fixture with period 3/4 + NULL ET flag -> converter raises ValueError."""
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        rows = []
        for period in [1.0, 1.0, 2.0, 2.0, 3.0]:
            rows.append(_make_gs_bronze_row(period=period))
        pdf = pd.DataFrame(rows)
        # ET column is already absent: _make_gs_bronze_row omits it when
        # home_team_start_left_extratime=None (the default).
        udf_fn = _make_gradientsports_spadl_udf()
        # The silly-kicks converter should raise ValueError via RuntimeError wrapper
        with pytest.raises(RuntimeError, match="GS SPADL conversion failed"):
            udf_fn(pdf)


class TestDotNotationColumnProjection:
    """Validate the _gs_needed_bronze_columns() set covers all columns the UDF reads.

    These tests run WITHOUT PySpark — they verify that the column set
    constructed in ``_convert_gradientsports_from_bronze`` is complete
    relative to what ``adapt_gradientsports_events`` and
    ``extract_gradientsports_match_metadata`` actually read from the
    bronze DataFrame.

    The canonical source is ``_gs_needed_bronze_columns()`` in
    ``spadl_conversion.py``; tests import it to avoid duplication.
    """

    def test_needed_cols_cover_rename_map(self) -> None:
        """Every _GS_BRONZE_TO_SNAKE key is in the needed-columns set."""
        from ingestion.spadl_adapter import _GS_BRONZE_TO_SNAKE
        from ingestion.spadl_conversion import _gs_needed_bronze_columns

        missing = set(_GS_BRONZE_TO_SNAKE.keys()) - _gs_needed_bronze_columns()
        assert not missing, f"Rename-map keys missing from needed cols: {sorted(missing)}"

    def test_dot_to_safe_rename_round_trips(self) -> None:
        """dot→safe→dot rename round-trips to the original column name."""
        from ingestion.spadl_conversion import (
            _gs_dot_to_safe_rename,
            _gs_safe_to_dot_rename,
        )

        dot_to_safe = _gs_dot_to_safe_rename()
        safe_to_dot = _gs_safe_to_dot_rename()

        # Every dot-notation column round-trips
        for orig, safe in dot_to_safe.items():
            assert "." not in safe, f"Safe name still has dot: {safe}"
            assert safe_to_dot[safe] == orig, f"Round-trip failed: {orig} → {safe} → {safe_to_dot.get(safe)}"

        # Sizes match
        assert len(dot_to_safe) == len(safe_to_dot)

    def test_dot_to_safe_rename_covers_all_dot_columns(self) -> None:
        """Every dot-notation column in needed set has a safe rename."""
        from ingestion.spadl_conversion import (
            _gs_dot_to_safe_rename,
            _gs_needed_bronze_columns,
        )

        dot_cols = {c for c in _gs_needed_bronze_columns() if "." in c}
        rename_keys = set(_gs_dot_to_safe_rename().keys())
        assert dot_cols == rename_keys

    def test_needed_cols_cover_derived_columns(self) -> None:
        """All derived columns (gameEventId, gameEvents.*) are in the needed set."""
        from ingestion.spadl_conversion import _gs_needed_bronze_columns

        derived = {
            "gameEventId",
            "gameEvents.period",
            "gameEvents.startGameClock",
            "gameEvents.playerId",
            "gameEvents.teamId",
            "gameEvents.setpieceType",
        }
        needed = _gs_needed_bronze_columns()
        assert derived <= needed, f"Derived columns missing: {derived - needed}"

    def test_needed_cols_cover_metadata_columns(self) -> None:
        """All metadata columns are in the needed set."""
        from ingestion.spadl_conversion import _gs_needed_bronze_columns

        metadata = {
            "gameEvents.homeTeam",
            "gameEvents.teamId",
            "stadiumMetadata.homeTeamStartLeft",
            "stadiumMetadata.homeTeamStartLeftExtraTime",
        }
        needed = _gs_needed_bronze_columns()
        assert metadata <= needed, f"Metadata columns missing: {metadata - needed}"

    def test_needed_cols_cover_fixture_columns(self) -> None:
        """Every column in the synthetic bronze fixture is in the needed set."""
        from ingestion.spadl_conversion import _gs_needed_bronze_columns

        fixture_cols = set(_make_gs_bronze_df(n=1).columns)
        needed = _gs_needed_bronze_columns()
        missing = fixture_cols - needed
        assert not missing, f"Fixture columns not in needed set: {sorted(missing)}"

    def test_udf_succeeds_with_only_needed_cols(self) -> None:
        """UDF produces valid output when given ONLY the projected columns.

        Simulates the full Spark-level path: project needed columns, then
        rename dot-notation to safe names (as Spark does before applyInPandas),
        then run the UDF (which reverses the rename internally).
        """
        from ingestion.spadl_conversion import (
            _gs_dot_to_safe_rename,
            _gs_needed_bronze_columns,
            _make_gradientsports_spadl_udf,
        )

        needed = _gs_needed_bronze_columns()

        # Build full fixture, then restrict to only needed columns
        full_pdf = _make_gs_bronze_df(n=5)
        projected_cols = sorted(needed & set(full_pdf.columns))
        projected_pdf = full_pdf[projected_cols]

        # Apply the same dot→safe rename that Spark does before applyInPandas.
        # The UDF must reverse this rename internally.
        dot_to_safe = _gs_dot_to_safe_rename()
        projected_pdf = projected_pdf.rename(columns=dot_to_safe)

        udf_fn = _make_gradientsports_spadl_udf()
        result = udf_fn(projected_pdf)
        assert len(result) > 0, "UDF must produce actions from projected columns"
        assert (result["data_source"] == "gradientsports").all()


class TestSparkDotNotationColumns:
    """PySpark integration: dot-notation bronze column names require rename.

    Spark interprets `gameEvents.gameEventType` as struct navigation
    (gameEvents → gameEventType).  The GS bronze table stores these as flat
    column names with literal dots.  Backtick quoting fixes .select() but
    NOT applyInPandas (which re-resolves columns from the schema).

    The fix: backtick-quoted .select() + .alias() to rename dots → '___',
    then the UDF reverses the rename at the top.

    These tests exercise the EXACT column-projection + rename + applyInPandas
    path used in ``_convert_gradientsports_from_bronze`` to catch
    UNRESOLVED_COLUMN errors before deployment.
    """

    @pytest.fixture
    def spark(self):
        """Local SparkSession for dot-notation column tests."""
        delta = pytest.importorskip("delta", reason="delta-spark required")
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip("pyspark not installed")
            return

        from unittest.mock import MagicMock

        if isinstance(SparkSession, MagicMock):
            pytest.skip("pyspark.sql is mocked in this test session")
            return

        try:
            builder = (
                SparkSession.builder.appName("test_gs_dot_notation")  # type: ignore[attr-defined]
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .master("local[1]")
            )
            session = delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception as exc:
            pytest.skip(f"Local Spark/Delta not available: {exc}")
            return
        yield session
        session.stop()

    def test_backtick_select_resolves_dot_columns(self, spark) -> None:
        """spark_fn.col('`gameEvents.gameEventType`') resolves a flat dot-named column."""
        from pyspark.sql import functions as spark_fn

        pdf = _make_gs_bronze_df(n=3)
        sdf = spark.createDataFrame(pdf)

        # Verify the schema has literal dot-named columns (not nested structs)
        dot_cols = [f.name for f in sdf.schema.fields if "." in f.name]
        assert len(dot_cols) > 0, "Fixture must have dot-notation columns"

        # Select with backtick quoting — must not raise UNRESOLVED_COLUMN
        selected = sdf.select([spark_fn.col(f"`{c}`") for c in dot_cols])
        assert selected.count() == 3

    def test_backtick_select_covers_needed_cols(self, spark) -> None:
        """The _gs_needed_bronze_columns() set intersects correctly with bronze schema."""
        from pyspark.sql import functions as spark_fn

        from ingestion.spadl_conversion import _gs_needed_bronze_columns

        needed = _gs_needed_bronze_columns()

        pdf = _make_gs_bronze_df(n=2)
        sdf = spark.createDataFrame(pdf)
        bronze_field_names = {f.name for f in sdf.schema.fields}

        # All needed columns that exist in the fixture must be selectable
        cols_to_select = sorted(needed & bronze_field_names)
        assert len(cols_to_select) > 10, "Fixture too sparse — need >10 columns"

        selected = sdf.select([spark_fn.col(f"`{c}`") for c in cols_to_select])
        assert selected.count() == 2

    def test_apply_in_pandas_with_dot_columns(self, spark) -> None:
        """groupBy().applyInPandas() succeeds after dot→safe rename.

        This is the exact failure mode that caused UNRESOLVED_COLUMN on Databricks:
        Spark tries to resolve all input columns in the execution plan for
        applyInPandas.  Backtick quoting alone is NOT sufficient — the
        FlatMapGroupsInPandas operator re-resolves column names from the
        schema, interpreting dots as struct navigation.

        The fix: .select() with backtick quoting + .alias() to rename dots
        to '___'.  The UDF reverses the rename at the top.
        """
        from pyspark.sql import functions as spark_fn
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        from ingestion.spadl_conversion import (
            _gs_dot_to_safe_rename,
            _gs_needed_bronze_columns,
        )

        needed = _gs_needed_bronze_columns()
        dot_to_safe = _gs_dot_to_safe_rename()

        # Create fixture with 2 matches (5 rows each)
        rows_a = [_make_gs_bronze_row(match_id="10502") for _ in range(5)]
        rows_b = [_make_gs_bronze_row(match_id="10503") for _ in range(5)]
        pdf = pd.DataFrame(rows_a + rows_b)
        sdf = spark.createDataFrame(pdf)

        # Project with backtick quoting + rename (the fix under test)
        bronze_field_names = {f.name for f in sdf.schema.fields}
        projected = sdf.select(
            [
                spark_fn.col(f"`{c}`").alias(dot_to_safe.get(c, c))
                for c in sorted(needed)
                if c in bronze_field_names
            ]
        )

        # Verify no dots remain in the projected schema
        dot_cols = [f.name for f in projected.schema.fields if "." in f.name]
        assert dot_cols == [], f"Schema still has dot-notation columns: {dot_cols}"

        # Minimal output schema for the identity UDF
        out_schema = StructType(
            [
                StructField("match_id", StringType()),
                StructField("row_count", LongType()),
            ]
        )

        def count_rows(pdf: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"match_id": [pdf["match_id"].iloc[0]], "row_count": [len(pdf)]})

        # This must NOT raise UNRESOLVED_COLUMN
        result = projected.groupBy("match_id").applyInPandas(count_rows, out_schema)
        rows = result.collect()
        assert len(rows) == 2
        assert {r["match_id"] for r in rows} == {"10502", "10503"}
        assert all(r["row_count"] == 5 for r in rows)

    def test_full_gs_udf_via_spark(self, spark) -> None:
        """End-to-end: GS UDF produces valid SPADL output via Spark applyInPandas.

        Exercises the complete path: backtick projection → dot→safe rename →
        groupBy → UDF (reverses rename) → SPADL output schema.
        This is the integration test that catches UNRESOLVED_COLUMN failures.
        """
        from pyspark.sql import functions as spark_fn
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        from ingestion.spadl_conversion import (
            _gs_dot_to_safe_rename,
            _gs_needed_bronze_columns,
            _make_gradientsports_spadl_udf,
        )

        needed = _gs_needed_bronze_columns()
        dot_to_safe = _gs_dot_to_safe_rename()

        # Build 2-match fixture (to test groupBy dispatches per-match)
        rows_a = []
        rows_b = []
        for i in range(5):
            rows_a.append(
                _make_gs_bronze_row(
                    match_id="10502",
                    game_event_id=6498520.0 + i,
                    start_game_clock=2800.0 + i * 10,
                )
            )
            rows_b.append(
                _make_gs_bronze_row(
                    match_id="10503",
                    game_event_id=7498520.0 + i,
                    start_game_clock=2800.0 + i * 10,
                    team_id=361.0,
                    home_team=False,
                )
            )
        pdf = pd.DataFrame(rows_a + rows_b)
        sdf = spark.createDataFrame(pdf)

        # Project with backtick quoting + dot→safe rename
        bronze_field_names = {f.name for f in sdf.schema.fields}
        projected = sdf.select(
            [
                spark_fn.col(f"`{c}`").alias(dot_to_safe.get(c, c))
                for c in sorted(needed)
                if c in bronze_field_names
            ]
        )

        # SPADL output schema (same as in _convert_gradientsports_from_bronze)
        spadl_schema = StructType(
            [
                StructField("game_id", LongType()),
                StructField("match_id", LongType()),
                StructField("original_event_id", StringType()),
                StructField("period_id", LongType()),
                StructField("time_seconds", DoubleType()),
                StructField("team_id", LongType()),
                StructField("player_id", LongType()),
                StructField("start_x", DoubleType()),
                StructField("start_y", DoubleType()),
                StructField("end_x", DoubleType()),
                StructField("end_y", DoubleType()),
                StructField("type_id", LongType()),
                StructField("result_id", LongType()),
                StructField("bodypart_id", LongType()),
                StructField("action_id", LongType()),
                StructField("competition_id", LongType()),
                StructField("season_id", LongType()),
                StructField("data_source", StringType()),
                StructField("statsbomb_possession_id", LongType()),
                StructField("statsbomb_possession_team_id", LongType()),
                StructField("statsbomb_play_pattern", StringType()),
                StructField("statsbomb_under_pressure", BooleanType()),
                StructField("possession_id_heuristic", LongType()),
                StructField("gk_role", StringType()),
                StructField("gk_was_distributing", BooleanType()),
                StructField("gk_was_engaged", BooleanType()),
                StructField("gk_actions_in_possession", LongType()),
                StructField("defending_gk_player_id", LongType()),
                StructField("team_id_native", StringType()),
                StructField("home_team_id_native", StringType()),
                StructField("competition_native_id", StringType()),
                StructField("season_native_id", StringType()),
                StructField("match_id_native", StringType()),
                StructField("player_id_native", StringType()),
                StructField("tackle_winner_player_id_native", StringType()),
                StructField("tackle_winner_player_key", LongType()),
                StructField("tackle_winner_team_id_native", StringType()),
                StructField("tackle_winner_team_key", LongType()),
                StructField("tackle_loser_player_id_native", StringType()),
                StructField("tackle_loser_player_key", LongType()),
                StructField("tackle_loser_team_id_native", StringType()),
                StructField("tackle_loser_team_key", LongType()),
            ]
        )

        udf_fn = _make_gradientsports_spadl_udf()
        result_sdf = projected.groupBy("match_id").applyInPandas(udf_fn, spadl_schema)  # type: ignore[arg-type]
        result_rows = result_sdf.collect()

        assert len(result_rows) > 0, "UDF must produce at least 1 SPADL action"
        assert all(r["data_source"] == "gradientsports" for r in result_rows)
        # Coordinates in SPADL range
        for r in result_rows:
            if r["start_x"] is not None:
                assert 0 <= r["start_x"] <= 105
            if r["start_y"] is not None:
                assert 0 <= r["start_y"] <= 68


class TestDirectionOfPlay:
    """Direction-of-play normalization coverage."""

    def test_home_start_right(self) -> None:
        """home_team_start_left=False still produces valid SPADL coordinates."""
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        pdf = _make_gs_bronze_df(n=5, home_team_start_left=False)
        udf_fn = _make_gradientsports_spadl_udf()
        result = udf_fn(pdf)
        assert len(result) > 0
        # Coordinates must still be in SPADL range after direction normalization
        assert result["start_x"].dropna().between(0, 105).all()
        assert result["start_y"].dropna().between(0, 68).all()
