# Gradient Sports SPADL/VAEP Conversion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Gradient Sports as the 6th SPADL data source (64 WC2022 matches), gate HF publishers from exporting GS data.

**Architecture:** IDSSE batch-dispatch pattern: single `applyInPandas` UDF extracting metadata from bronze columns at execution time, `groupBy("match_id")` for one Spark job across all 64 matches. Hashed string match IDs via `hash_native_id_to_bigint`. HF license gate via SQL `WHERE` filter.

**Tech Stack:** Python 3.10, silly-kicks (GS converter), PySpark (applyInPandas), Delta Lake, dbt, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/shared/identifiers.py` | Modify | Add GS native ID generators + NamedTuple classmethods |
| `src/tests/test_format_contract.py` | Modify | Add GS format-contract tests |
| `src/ingestion/spadl_adapter.py` | Modify | Add `adapt_gradientsports_events()` + `extract_gradientsports_match_metadata()` + rename map |
| `src/ingestion/spadl_enrichments.py` | Modify | Add `"gradientsports"` to `_VALID_SOURCES` |
| `src/ingestion/spadl_udf_shared.py` | Modify | Update docstrings for GS tackle qualifier handling |
| `src/ingestion/spadl_conversion.py` | Modify | Add GS UDF factory, orchestrator, replaceWhere |
| `src/ingestion/spadl_vaep.py` | Modify | Add GS to guard, chunk config, pipeline dispatch |
| `scripts/publish_spadl_vaep_hf.py` | Modify | Add `WHERE data_source != 'gradientsports'` |
| `scripts/publish_tracking_context_hf.py` | Modify | Add `WHERE data_source != 'gradientsports'` |
| `dbt_project/models/staging/spadl/_spadl__models.yml` | Modify | Add `'gradientsports'` to accepted_values |
| `src/tests/test_gradientsports_spadl.py` | Create | All GS-specific unit + integration tests |

---

### Task 1: Native ID Generators + Format Contract Tests

**Files:**
- Modify: `src/shared/identifiers.py`
- Modify: `src/tests/test_format_contract.py`

GS match/player/team IDs are numeric strings (e.g., `"10502"`, `"12345"`) -- same pattern as SkillCorner. Reuse `_SKILLCORNER_NUMERIC_ID_PATTERN`.

- [ ] **Step 1: Write failing format-contract tests**

Add to `src/tests/test_format_contract.py` at the end of the file:

```python
# ---------------------------------------------------------------------------
# Gradient Sports -- ADR-018 format contracts
# ---------------------------------------------------------------------------


class TestGradientSportsFormatContract:
    def test_gradientsports_match_id_from_string(self) -> None:
        assert gradientsports_native_match_id("10502") == "10502"

    def test_gradientsports_match_id_from_int(self) -> None:
        assert gradientsports_native_match_id(10502) == "10502"

    def test_gradientsports_match_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("gs_10502")

    def test_gradientsports_match_id_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("")

    def test_gradientsports_match_id_rejects_alpha(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("abc123")


class TestGradientSportsPlayerIdFormatContract:
    def test_gradientsports_native_player_id_valid(self) -> None:
        assert gradientsports_native_player_id(38673) == "38673"

    def test_gradientsports_native_player_id_string(self) -> None:
        assert gradientsports_native_player_id("38673") == "38673"

    def test_gradientsports_native_player_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            gradientsports_native_player_id("player_38673")


class TestGradientSportsTeamIdFormatContract:
    def test_gradientsports_native_team_id_valid(self) -> None:
        assert gradientsports_native_team_id(4177) == "4177"

    def test_gradientsports_native_team_id_string(self) -> None:
        assert gradientsports_native_team_id("4177") == "4177"

    def test_gradientsports_native_team_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            gradientsports_native_team_id("team_4177")


class TestGradientSportsNamedTuples:
    def test_native_match_id_gradientsports(self) -> None:
        nid = NativeMatchId.gradientsports("10502")
        assert nid.provider == "gradientsports"
        assert nid.value == "10502"

    def test_native_player_id_gradientsports(self) -> None:
        nid = NativePlayerId.gradientsports("38673")
        assert nid.provider == "gradientsports"
        assert nid.value == "38673"

    def test_native_team_id_gradientsports(self) -> None:
        nid = NativeTeamId.gradientsports("4177")
        assert nid.provider == "gradientsports"
        assert nid.value == "4177"
```

Also add these imports at the top of `test_format_contract.py` alongside the existing imports:

```python
from shared.identifiers import (
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_format_contract.py::TestGradientSportsFormatContract -v`
Expected: ImportError -- `gradientsports_native_match_id` does not exist yet.

- [ ] **Step 3: Implement native ID generators**

Add to `src/shared/identifiers.py` after the SkillCorner section (after line 228), before the NamedTuple section:

```python
# ---------------------------------------------------------------------------
# Gradient Sports (PFF WC2022 open dataset)
# ---------------------------------------------------------------------------
# GS IDs are numeric strings -- same shape as SkillCorner. Reuse the same
# regex pattern.

_GRADIENTSPORTS_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]+$")


def gradientsports_native_match_id(raw_match_id: str | int) -> str:
    """Canonical Gradient Sports native match id -- stringified positive integer."""
    s = str(raw_match_id)
    if not _GRADIENTSPORTS_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid Gradient Sports match id: {raw_match_id!r} (expected numeric string)")
    return s


def gradientsports_native_player_id(raw_player_id: str | int) -> str:
    """Canonical Gradient Sports native player id -- stringified positive integer."""
    s = str(raw_player_id)
    if not _GRADIENTSPORTS_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid Gradient Sports player id: {raw_player_id!r} (expected numeric string)")
    return s


def gradientsports_native_team_id(raw_team_id: str | int) -> str:
    """Canonical Gradient Sports native team id -- stringified positive integer."""
    s = str(raw_team_id)
    if not _GRADIENTSPORTS_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid Gradient Sports team id: {raw_team_id!r} (expected numeric string)")
    return s
```

Also update the module docstring (line 1) from "5 SPADL data sources" to "6 SPADL data sources" and add "Gradient Sports" to the list.

- [ ] **Step 4: Add NamedTuple classmethods**

Add to `NativeMatchId` (after the `skillcorner` classmethod at line 263):

```python
    @classmethod
    def gradientsports(cls, raw: str | int) -> NativeMatchId:
        return cls(provider="gradientsports", value=gradientsports_native_match_id(raw))
```

Add to `NativePlayerId` (after the `skillcorner` classmethod at line 290):

```python
    @classmethod
    def gradientsports(cls, raw: str | int) -> NativePlayerId:
        return cls(provider="gradientsports", value=gradientsports_native_player_id(raw))
```

Add to `NativeTeamId` (after the `skillcorner` classmethod at line 317):

```python
    @classmethod
    def gradientsports(cls, raw: str | int) -> NativeTeamId:
        return cls(provider="gradientsports", value=gradientsports_native_team_id(raw))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_format_contract.py::TestGradientSportsFormatContract src/tests/test_format_contract.py::TestGradientSportsPlayerIdFormatContract src/tests/test_format_contract.py::TestGradientSportsTeamIdFormatContract src/tests/test_format_contract.py::TestGradientSportsNamedTuples -v`
Expected: All PASS.

- [ ] **Step 6: Lint check**

Run: `uv run ruff check src/shared/identifiers.py src/tests/test_format_contract.py`
Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/shared/identifiers.py src/tests/test_format_contract.py
git commit -m "feat(gs-spadl): add Gradient Sports native ID generators + format contracts

ADR-018: gradientsports_native_{match,player,team}_id + NamedTuple classmethods.
Numeric string pattern (same as SkillCorner).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Adapter + Metadata Extraction

**Files:**
- Modify: `src/ingestion/spadl_adapter.py`
- Create: `src/tests/test_gradientsports_spadl.py`

The adapter performs three transformations on GS bronze data before silly-kicks can consume it:

1. **Column rename**: Bronze dot-notation (`gameEvents.period`) → silly-kicks snake_case (`period_id`). The rename map covers ~38 direct 1:1 renames.
2. **Ball JSON parsing**: Bronze `ball` column is a JSON string (`[{"x": 18.5, "y": -21.33, ...}]`); must be parsed to extract `ball_x` and `ball_y` floats.
3. **Derived columns**: `event_id` from top-level `gameEventId`, `player_id` from `gameEvents.playerId`, `team_id` from `gameEvents.teamId`, `period_id` from `gameEvents.period`, `time_seconds` from `gameEvents.startGameClock`, `set_piece_type` from `gameEvents.setpieceType`.

Metadata extraction derives `home_team_id` from `gameEvents.homeTeam` (boolean) + `gameEvents.teamId` (since `stadiumMetadata.homeTeamId` does NOT exist in bronze).

**Bronze column evidence** (verified via `DESCRIBE soccer_analytics.bronze.gradientsports_events`):
- `gameEventId` (top-level float) — NOT `possessionEvents.eventId`
- `gameEvents.period` — NOT `possessionEvents.periodId`
- `gameEvents.startGameClock` — NOT `possessionEvents.timeSeconds`
- `gameEvents.playerId` — NOT `possessionEvents.playerId`
- `gameEvents.teamId` — NOT `possessionEvents.teamId`
- `gameEvents.setpieceType` (lowercase p) — NOT `possessionEvents.setPieceType`
- `ball` (JSON string `[{"x":..,"y":..}]`) — NOT `possessionEvents.ballX`/`ballY`
- `gameEvents.homeTeam` (boolean) + `gameEvents.teamId` — NOT `stadiumMetadata.homeTeamId`
- `possessionEvents.challengerTeamId` — DOES NOT EXIST in bronze (NaN-filled; converter tolerates NaN)
- `possessionEvents.challengeWinnerTeamId` — DOES NOT EXIST in bronze (NaN-filled; converter tolerates NaN)

- [ ] **Step 1: Write failing adapter tests**

Create `src/tests/test_gradientsports_spadl.py`:

```python
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
    pass_type: str = "Short",
    pass_outcome_type: str = "Complete",
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
        derived = {"event_id", "period_id", "time_seconds", "player_id", "team_id",
                    "set_piece_type", "ball_x", "ball_y"}
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

    def test_adapt_derived_columns_from_gameEvents(self) -> None:
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestAdaptGradientSportsEvents -v`
Expected: ImportError -- `adapt_gradientsports_events` does not exist.

- [ ] **Step 3: Implement adapter + metadata extraction**

Add to `src/ingestion/spadl_adapter.py` at the end of the file:

```python
# ---------------------------------------------------------------------------
# Gradient Sports adapter (WC2022 PFF open dataset)
# ---------------------------------------------------------------------------

# Bronze uses json_normalize dot-notation (e.g., "possessionEvents.passType").
# silly-kicks expects 47 snake_case columns (EXPECTED_INPUT_COLUMNS).
#
# CRITICAL: The bronze schema was verified via DESCRIBE (264 columns).
# Several columns live under gameEvents.*, NOT possessionEvents.*:
#   gameEventId (top-level) -> event_id
#   gameEvents.period -> period_id
#   gameEvents.startGameClock -> time_seconds
#   gameEvents.playerId -> player_id
#   gameEvents.teamId -> team_id
#   gameEvents.setpieceType -> set_piece_type
# Ball coordinates are in a JSON string column `ball`, NOT possessionEvents.ballX/Y.
# challenger_team_id / challenge_winner_team_id DO NOT EXIST in bronze
# (the converter tolerates NaN for these).
#
# 1:1 renames (possessionEvents.*, fouls.*, gameEvents.gameEventType, gameId):
_GS_BRONZE_TO_SNAKE: dict[str, str] = {
    # Top-level scalars
    "gameId": "game_id",
    "possessionEventId": "possession_event_id",
    # possessionEvents.* -> snake_case (direct 1:1 renames)
    "possessionEvents.possessionEventType": "possession_event_type",
    "possessionEvents.passType": "pass_type",
    "possessionEvents.passOutcomeType": "pass_outcome_type",
    "possessionEvents.crossType": "cross_type",
    "possessionEvents.crossOutcomeType": "cross_outcome_type",
    "possessionEvents.crossZoneType": "cross_zone_type",
    "possessionEvents.shotType": "shot_type",
    "possessionEvents.shotOutcomeType": "shot_outcome_type",
    "possessionEvents.shotNatureType": "shot_nature_type",
    "possessionEvents.shotInitialHeightType": "shot_initial_height_type",
    "possessionEvents.touchType": "touch_type",
    "possessionEvents.touchOutcomeType": "touch_outcome_type",
    "possessionEvents.challengeType": "challenge_type",
    "possessionEvents.challengeOutcomeType": "challenge_outcome_type",
    "possessionEvents.challengeWinnerPlayerId": "challenge_winner_player_id",
    "possessionEvents.challengerPlayerId": "challenger_player_id",
    "possessionEvents.tackleAttemptType": "tackle_attempt_type",
    "possessionEvents.bodyType": "body_type",
    "possessionEvents.ballHeightType": "ball_height_type",
    "possessionEvents.clearanceOutcomeType": "clearance_outcome_type",
    "possessionEvents.ballCarryOutcome": "ball_carry_outcome",
    "possessionEvents.carryType": "carry_type",
    "possessionEvents.carryIntent": "carry_intent",
    "possessionEvents.carryDefenderPlayerId": "carry_defender_player_id",
    "possessionEvents.keeperTouchType": "keeper_touch_type",
    "possessionEvents.saveHeightType": "save_height_type",
    "possessionEvents.saveReboundType": "save_rebound_type",
    "possessionEvents.reboundOutcomeType": "rebound_outcome_type",
    "possessionEvents.incompletionReasonType": "incompletion_reason_type",
    # gameEvents.* (only gameEventType is a 1:1 rename; period/playerId/
    # teamId/setpieceType are derived, not renamed — see _DERIVED_COLUMNS below)
    "gameEvents.gameEventType": "game_event_type",
    # fouls.*
    "fouls.foulType": "foul_type",
    "fouls.onFieldFoulOutcomeType": "on_field_foul_outcome_type",
    "fouls.finalFoulOutcomeType": "final_foul_outcome_type",
    "fouls.onFieldOffenseType": "on_field_offense_type",
    "fouls.finalOffenseType": "final_offense_type",
}


def _parse_ball_json(ball_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse GS bronze `ball` JSON string column to (ball_x, ball_y) float Series.

    Bronze format: '[{"visibility": "VISIBLE", "x": 18.5, "y": -21.33, "z": 0.0}]'
    Always a single-element JSON array. Returns (NaN, NaN) for null/malformed rows.
    """
    import json as _json

    def _extract(val: object) -> tuple[float, float]:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return (float("nan"), float("nan"))
        try:
            parsed = _json.loads(str(val))
            if isinstance(parsed, list) and len(parsed) > 0:
                return (float(parsed[0]["x"]), float(parsed[0]["y"]))
        except (ValueError, KeyError, TypeError, IndexError):
            pass
        return (float("nan"), float("nan"))

    pairs = ball_series.map(_extract)
    ball_x = pairs.map(lambda p: p[0])
    ball_y = pairs.map(lambda p: p[1])
    return ball_x, ball_y


def adapt_gradientsports_events(pdf: pd.DataFrame) -> pd.DataFrame:
    """Rename + derive bronze columns to match silly-kicks EXPECTED_INPUT_COLUMNS.

    Three transformation categories:
    1. Direct 1:1 renames via _GS_BRONZE_TO_SNAKE (~35 columns)
    2. Derived columns from gameEvents.* namespace (6 columns):
       gameEventId -> event_id, gameEvents.period -> period_id,
       gameEvents.startGameClock -> time_seconds, gameEvents.playerId -> player_id,
       gameEvents.teamId -> team_id, gameEvents.setpieceType -> set_piece_type
    3. Ball JSON parsing: ball -> ball_x, ball_y

    Args:
        pdf: Raw bronze DataFrame from ``gradientsports_events``.

    Returns:
        DataFrame with all 47 ``EXPECTED_INPUT_COLUMNS`` present.
        Missing optional columns are NaN-filled.
    """
    from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS

    if pdf.empty:
        return pd.DataFrame(columns=sorted(EXPECTED_INPUT_COLUMNS))

    # Step 1: Apply 1:1 renames
    rename_map = {k: v for k, v in _GS_BRONZE_TO_SNAKE.items() if k in pdf.columns}
    adapted = pdf.rename(columns=rename_map)

    # Step 2: Derived columns from gameEvents.* namespace
    # These columns live under gameEvents.*, NOT possessionEvents.*
    _DERIVED_COLUMNS: dict[str, str] = {
        "gameEventId": "event_id",
        "gameEvents.period": "period_id",
        "gameEvents.startGameClock": "time_seconds",
        "gameEvents.playerId": "player_id",
        "gameEvents.teamId": "team_id",
        "gameEvents.setpieceType": "set_piece_type",
    }
    for bronze_col, snake_col in _DERIVED_COLUMNS.items():
        if bronze_col in adapted.columns:
            adapted[snake_col] = adapted[bronze_col]
        elif bronze_col in pdf.columns:
            adapted[snake_col] = pdf[bronze_col]
    # Drop source columns to avoid polluting output with dot-notation leftovers
    adapted = adapted.drop(
        columns=[k for k in _DERIVED_COLUMNS if k in adapted.columns],
        errors="ignore",
    )

    # Step 3: Parse ball JSON string -> ball_x, ball_y
    # (O(n) Python loop per match — fine for 64 WC2022 matches; revisit if scaling)
    if "ball" in adapted.columns:
        adapted["ball_x"], adapted["ball_y"] = _parse_ball_json(adapted["ball"])
        adapted = adapted.drop(columns=["ball"], errors="ignore")
    elif "ball" in pdf.columns:
        adapted["ball_x"], adapted["ball_y"] = _parse_ball_json(pdf["ball"])

    # Step 4: NaN-fill any remaining missing expected columns
    # (e.g., challenger_team_id, challenge_winner_team_id which don't exist in bronze)
    for col in EXPECTED_INPUT_COLUMNS:
        if col not in adapted.columns:
            adapted[col] = pd.NA

    return adapted


def extract_gradientsports_match_metadata(pdf: pd.DataFrame) -> dict:
    """Extract match-level metadata from GS bronze rows.

    GS bronze denormalizes match metadata into every event row.
    ``home_team_id`` is derived from ``gameEvents.homeTeam`` (boolean) +
    ``gameEvents.teamId`` because ``stadiumMetadata.homeTeamId`` does NOT
    exist in the bronze schema.

    Args:
        pdf: Raw bronze DataFrame (pre-rename, dot-notation columns).

    Returns:
        Dict with ``home_team_id`` (int), ``home_team_start_left`` (bool),
        ``home_team_start_left_extratime`` (bool | None).

    Raises:
        ValueError: If pdf is empty or no homeTeam=True rows found.
    """
    if pdf.empty:
        raise ValueError("Cannot extract metadata from empty DataFrame")

    # Derive home_team_id: find the first row where gameEvents.homeTeam is True
    home_mask = pdf["gameEvents.homeTeam"] == True  # noqa: E712 — bronze may return string "true"
    if not home_mask.any():
        # Fallback: try string comparison for Spark-serialized booleans
        home_mask = pdf["gameEvents.homeTeam"].astype(str).str.lower() == "true"
    if not home_mask.any():
        raise ValueError("No rows with gameEvents.homeTeam=True found — cannot derive home_team_id")

    home_team_id = int(float(pdf.loc[home_mask, "gameEvents.teamId"].iloc[0]))

    # Direction flag
    row = pdf.iloc[0]
    htsl_val = row["stadiumMetadata.homeTeamStartLeft"]
    if isinstance(htsl_val, str):
        home_team_start_left = htsl_val.lower() == "true"
    else:
        home_team_start_left = bool(htsl_val)

    # Extra-time direction flag -- may be absent or null
    et_col = "stadiumMetadata.homeTeamStartLeftExtraTime"
    if et_col in pdf.columns:
        et_val = row[et_col]
        if pd.notna(et_val):
            if isinstance(et_val, str):
                home_team_start_left_extratime: bool | None = et_val.lower() == "true"
            else:
                home_team_start_left_extratime = bool(et_val)
        else:
            home_team_start_left_extratime = None
    else:
        home_team_start_left_extratime = None

    return {
        "home_team_id": home_team_id,
        "home_team_start_left": home_team_start_left,
        "home_team_start_left_extratime": home_team_start_left_extratime,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestAdaptGradientSportsEvents src/tests/test_gradientsports_spadl.py::TestExtractGradientSportsMatchMetadata -v`
Expected: All PASS.

- [ ] **Step 5: Verify rename map completeness**

Run: `uv run python -c "from ingestion.spadl_adapter import adapt_gradientsports_events, _GS_BRONZE_TO_SNAKE; from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS; mapped = set(_GS_BRONZE_TO_SNAKE.values()); derived = {'event_id', 'period_id', 'time_seconds', 'player_id', 'team_id', 'set_piece_type', 'ball_x', 'ball_y'}; coverage = mapped | derived; expected = set(EXPECTED_INPUT_COLUMNS); missing = expected - coverage; print('Missing:', sorted(missing)); assert not missing, f'Incomplete coverage: {sorted(missing)}'"`
Expected: `Missing: []` (no assertion error). If any columns are missing, add entries to `_GS_BRONZE_TO_SNAKE` or the derived columns dict.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/spadl_adapter.py src/tests/test_gradientsports_spadl.py
git commit -m "feat(gs-spadl): adapter + metadata extraction for GS bronze events

adapt_gradientsports_events: 35-column rename map + 6 derived columns
from gameEvents.* + ball JSON parsing + NaN-fill.
extract_gradientsports_match_metadata: home_team_id derived from
gameEvents.homeTeam boolean + gameEvents.teamId, direction flags, ET.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Enrichment Registration + Docstring Updates

**Files:**
- Modify: `src/ingestion/spadl_enrichments.py` (line 27)
- Modify: `src/ingestion/spadl_udf_shared.py` (lines 93, 30-31)

- [ ] **Step 1: Write failing enrichment test**

Add to `src/tests/test_gradientsports_spadl.py`:

```python
class TestEnrichmentRegistration:
    def test_gradientsports_in_valid_sources(self) -> None:
        """gradientsports must be in _VALID_SOURCES."""
        from ingestion.spadl_enrichments import _VALID_SOURCES

        assert "gradientsports" in _VALID_SOURCES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestEnrichmentRegistration -v`
Expected: FAIL -- `"gradientsports"` not in frozenset.

- [ ] **Step 3: Add gradientsports to _VALID_SOURCES**

Edit `src/ingestion/spadl_enrichments.py` line 27. Change:

```python
_VALID_SOURCES: Final[frozenset[str]] = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner"})
```

to:

```python
_VALID_SOURCES: Final[frozenset[str]] = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports"})
```

Also update the docstring on line 50 from:

```python
            ``"metrica"``.
```

to:

```python
            ``"metrica"``, ``"skillcorner"``, ``"gradientsports"``.
```

- [ ] **Step 4: Update docstrings in spadl_udf_shared.py**

Edit `src/ingestion/spadl_udf_shared.py` line 93. Change:

```python
    """NULL-fill the 8 tackle qualifier columns for non-IDSSE sources."""
```

to:

```python
    """NULL-fill the 8 tackle qualifier columns for sources without native tackle qualifiers (not IDSSE or GradientSports)."""
```

Edit lines 29-31 of the `apply_player_id_native` docstring. After the existing comment about "IDSSE/Metrica: already string-shaped", no code change needed -- the else branch already handles GS correctly (`.astype("string")` works on Int64).

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestEnrichmentRegistration -v`
Expected: PASS.

- [ ] **Step 6: Lint check**

Run: `uv run ruff check src/ingestion/spadl_enrichments.py src/ingestion/spadl_udf_shared.py`
Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/spadl_enrichments.py src/ingestion/spadl_udf_shared.py src/tests/test_gradientsports_spadl.py
git commit -m "feat(gs-spadl): register gradientsports in enrichment sources + docstring updates

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: UDF Factory + Orchestrator + replaceWhere

**Files:**
- Modify: `src/ingestion/spadl_conversion.py`
- Modify: `src/tests/test_gradientsports_spadl.py`

The UDF follows the IDSSE batch pattern: metadata extracted from bronze columns at execution time, tackle qualifier mapping, shared post-processing helpers. Logger must be explicitly imported inside the UDF closure body (module-level `logger` is not visible inside `applyInPandas` closures).

- [ ] **Step 1: Write failing UDF tests**

Add to `src/tests/test_gradientsports_spadl.py`:

```python
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
        for col in ("match_id", "data_source", "start_x", "start_y", "type_id",
                     "team_id_native", "match_id_native", "player_id_native"):
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
        for col in ("tackle_winner_player_id_native", "tackle_winner_player_key",
                     "tackle_winner_team_id_native", "tackle_winner_team_key",
                     "tackle_loser_player_id_native", "tackle_loser_player_key",
                     "tackle_loser_team_id_native", "tackle_loser_team_key"):
            assert col in result.columns, f"Missing tackle qualifier column: {col}"

    def test_udf_tackle_challenge_event_produces_values(self) -> None:
        """Challenge events with explicit winner/loser produce non-NA tackle qualifiers."""
        # Build a CH (challenge) event fixture with explicit tackle IDs.
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
        assert len(tackle_rows) > 0, (
            "Expected at least 1 row with tackle_winner_player_id_native populated"
        )
        assert (tackle_rows["tackle_winner_player_id_native"] == "111").all()

    def test_udf_coordinates_in_spadl_range(self) -> None:
        """Converted coordinates are in SPADL range [0,105] x [0,68]."""
        pdf = _make_gs_bronze_df(n=5, ball_x=10.5, ball_y=20.3)
        result = self._run_udf(pdf)
        assert len(result) > 0, "UDF must produce at least 1 action from 5 input rows"
        assert result["start_x"].dropna().between(0, 105).all()
        assert result["start_y"].dropna().between(0, 68).all()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestGradientSportsUdf::test_udf_data_source -v`
Expected: ImportError -- `_make_gradientsports_spadl_udf` does not exist.

- [ ] **Step 3: Implement UDF factory**

Add to `src/ingestion/spadl_conversion.py` after the SkillCorner section (find the end of SkillCorner code). The UDF follows the IDSSE batch pattern exactly:

```python
# ---------------------------------------------------------------------------
# Gradient Sports SPADL conversion
# ---------------------------------------------------------------------------


def _make_gradientsports_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific Gradient Sports matches."""
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'gradientsports' AND match_id IN ({ids_sql})"


def _make_gradientsports_spadl_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the applyInPandas UDF closure for Gradient Sports SPADL conversion.

    Follows the IDSSE batch pattern: metadata extracted from bronze columns
    at execution time (no per-match closure). Tackle qualifier mapping uses
    the IDSSE pattern (_native/_key pairs), NOT null_fill_tackle_qualifiers.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import logging as _logging

        import pandas as _pd

        _udf_logger = _logging.getLogger(__name__)

        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
        )
        from ingestion.spadl_adapter import (
            adapt_gradientsports_events as _adapt,
        )
        from ingestion.spadl_adapter import (
            extract_gradientsports_match_metadata as _extract_meta,
        )
        from ingestion.spadl_adapter import (
            hash_native_id_to_bigint as _hash_id,
        )
        from shared.identifiers import gradientsports_native_match_id as _gs_match_id

        # Column list must stay in sync with the IDSSE UDF's _spadl_cols
        # (spadl_conversion.py, line ~817). Any column added there must be
        # added here too, or the final reindex will KeyError.
        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "action_id",
                "competition_id",
                "season_id",
                "data_source",
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                "player_id_native",
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.gradientsports as _spadl_gs

        # Match-level metadata from bronze columns (IDSSE batch pattern).
        match_id_str = str(pdf["match_id"].iloc[0])
        metadata = _extract_meta(pdf)

        try:
            adapted = _adapt(pdf)
            actions, _report = _spadl_gs.convert_to_actions(
                adapted,
                home_team_id=metadata["home_team_id"],
                home_team_start_left=metadata["home_team_start_left"],
                home_team_start_left_extratime=metadata["home_team_start_left_extratime"],
            )
        except Exception as exc:
            msg = f"GS SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            _udf_logger.warning(
                "SPADL conversion unrecognized event types for GS match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

        # D8 post-processing helper sequence (spec section D8).
        from ingestion.spadl_udf_shared import (
            apply_match_level_natives as _apply_match_natives,
        )
        from ingestion.spadl_udf_shared import (
            apply_player_id_native as _apply_pid_native,
        )
        from ingestion.spadl_udf_shared import (
            cast_enrichment_dtypes as _cast_enrichment,
        )
        from ingestion.spadl_udf_shared import (
            null_fill_statsbomb_columns as _null_fill_sb,
        )

        # Step 4: player_id_native -- GS player_ids are Int64, else branch
        # in apply_player_id_native handles .astype("string") correctly.
        actions = _apply_pid_native(actions, source="gradientsports")

        # Step 5-6: Hash match_id and team_id to legacy BIGINTs.
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)

        # team_id: GS converter outputs numeric team_id. Map to native string + hash.
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        null_team_mask = actions["team_id_native"].isna() | (actions["team_id_native"] == "<NA>")
        if null_team_mask.any():
            _udf_logger.warning(
                "NULL team_id_native in %d rows for GS match_id=%s. Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")

        # Step 7: NULL-fill legacy BIGINTs
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")

        # Step 8: data_source
        actions["data_source"] = "gradientsports"

        # Step 9: enrichments
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="gradientsports")

        # Step 10: original_event_id to string
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # Step 11-12: null-fill SB columns + cast enrichment dtypes
        actions = _null_fill_sb(actions, n=n)
        actions = _cast_enrichment(actions)

        # Step 13: match-level natives
        actions = _apply_match_natives(
            actions,
            home_team_id_native=str(metadata["home_team_id"]),
            competition_native_id=_pd.NA,
            season_native_id=_pd.NA,
            match_id_native=_gs_match_id(match_id_str),
        )

        # Step 14: Tackle qualifier mapping (IDSSE pattern, NOT null_fill_tackle_qualifiers).
        # GS converter outputs 4 Int64 tackle columns on challenge events.
        from typing import Any as _Any

        # _hash_id already imported above (hash_native_id_to_bigint alias)
        def _hash_or_na(v: _Any) -> _Any:
            if v is None or _pd.isna(v):
                return _pd.NA
            s = str(v)
            return _hash_id(s) if s else _pd.NA

        for native_col, key_col, sk_col in (
            ("tackle_winner_player_id_native", "tackle_winner_player_key", "tackle_winner_player_id"),
            ("tackle_winner_team_id_native", "tackle_winner_team_key", "tackle_winner_team_id"),
            ("tackle_loser_player_id_native", "tackle_loser_player_key", "tackle_loser_player_id"),
            ("tackle_loser_team_id_native", "tackle_loser_team_key", "tackle_loser_team_id"),
        ):
            if sk_col in actions.columns:
                actions[native_col] = actions[sk_col].astype("string")
                actions[key_col] = actions[native_col].map(_hash_or_na).astype("Int64")
            else:
                actions[native_col] = _pd.array([_pd.NA] * len(actions), dtype="string")
                actions[key_col] = _pd.array([_pd.NA] * len(actions), dtype="Int64")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf
```

- [ ] **Step 4: Implement orchestrator**

Add directly after the UDF factory:

```python
def _convert_gradientsports_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
) -> bool:
    """Read GS events from bronze, convert to SPADL via silly-kicks, write Delta.

    IDSSE batch pattern: 1 Spark job for all matches via groupBy.applyInPandas.
    Returns whether any data was written.
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

    from ingestion.spadl_adapter import hash_native_id_to_bigint

    events_table = f"{catalog}.{schema}.gradientsports_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read GS events bronze table")
        return False

    # match_id in bronze is a string (e.g. "10502"); spadl_actions.match_id is
    # a BIGINT via hash_native_id_to_bigint.  We compare hashed values here.
    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    new_match_ids: list[str] = [mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches]
    if match_id_filter is not None:
        new_match_ids = [mid for mid in new_match_ids if hash_native_id_to_bigint(mid) in match_id_filter]

    if not new_match_ids:
        logger.info("GS: all %d matches already converted -- skipping", len(all_match_ids))
        return False

    logger.info("GS: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))

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
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    hashed_new_ids = [hash_native_id_to_bigint(mid) for mid in new_match_ids]
    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=_make_gradientsports_replace_where(hashed_new_ids),
        logger=logger,
    )

    logger.info("GS: SPADL conversion complete for %d matches", len(new_match_ids))
    return True
```

- [ ] **Step 5: Run UDF tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestGradientSportsUdf -v`
Expected: All PASS. The UDF tests call the function directly with pandas DataFrames (no Spark needed).

- [ ] **Step 6: Lint check**

Run: `uv run ruff check src/ingestion/spadl_conversion.py`
Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/spadl_conversion.py src/tests/test_gradientsports_spadl.py
git commit -m "feat(gs-spadl): UDF factory + orchestrator for GS SPADL conversion

IDSSE batch pattern: groupBy(match_id).applyInPandas, metadata extracted
at execution time. Tackle qualifier mapping uses _native/_key pairs.
replaceWhere predicate scoped to data_source='gradientsports'.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: Guard + Pipeline Integration

**Files:**
- Modify: `src/ingestion/spadl_vaep.py`
- Modify: `src/tests/test_gradientsports_spadl.py`

Wire GS into the guard's diff detection, chunk config, and pipeline dispatch.

- [ ] **Step 1: Write failing guard test**

Add to `src/tests/test_gradientsports_spadl.py`:

```python
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
        # This check confirms the string literal exists in the function body.
        # Limitation: a match in a comment or log message would pass vacuously —
        # if the dispatch dict is ever refactored to an importable mapping,
        # switch to direct membership assertion.
        import inspect

        from ingestion import spadl_vaep

        source = inspect.getsource(spadl_vaep._run_chunk)
        assert '"gradientsports"' in source or "'gradientsports'" in source
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestGuardIntegration -v`
Expected: FAIL on `gs_new` not in `_PROVIDER_METADATA_KEYS`.

- [ ] **Step 3: Modify guard check method**

Edit `src/ingestion/spadl_vaep.py`. In the `_VaepGuard.check()` method (around line 184), add after the `sc_new` diff:

```python
        gs_new = self._diff_hashed_source_against_spadl(
            spark,
            bronze_table=f"{catalog}.{schema}.gradientsports_events",
            spadl_table=spadl_table,
            data_source="gradientsports",
        )
```

Update `total_new` (around line 198) to include `len(gs_new)`:

```python
        total_new = len(sb_new) + len(ws_new) + len(idsse_new) + len(metrica_new) + len(sc_new) + len(gs_new) + len(unscored)
```

Add `"gs_new": gs_new` to the metadata dict (around line 216):

```python
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total_new,
            metadata={
                "sb_new": sb_new,
                "ws_new": ws_new,
                "idsse_new": idsse_new,
                "metrica_new": metrica_new,
                "sc_new": sc_new,
                "gs_new": gs_new,
                "unscored_vaep_match_ids": sorted(unscored),
            },
        )
```

Update `_diff_hashed_source_against_spadl` docstring (line 228) from "IDSSE / Metrica / SkillCorner" to "IDSSE / Metrica / SkillCorner / GradientSports".

- [ ] **Step 4: Update chunk config + valid providers**

Edit `_VALID_CHUNK_PROVIDERS` (line 271):

```python
_VALID_CHUNK_PROVIDERS = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports", "score"})
```

Edit `_CHUNK_SIZES` (line 303), add after the `"skillcorner"` entry:

```python
    "gradientsports": 50,
```

Edit `_PROVIDER_METADATA_KEYS` (line 312), add after `"sc_new"`:

```python
    "gs_new": "gradientsports",
```

- [ ] **Step 5: Update `_run_chunk` converters dict**

Edit `_run_chunk` (around line 1051), add to the `converters` dict:

```python
            "gradientsports": _convert_gradientsports_from_bronze,
```

Add the import at the top of `_run_chunk` or at module level. Since `_convert_gradientsports_from_bronze` is defined in the same file, no import needed.

- [ ] **Step 6: Update `run_pipeline` Phase A + unscored_ids union**

Edit `run_pipeline` (around line 766), add after the `sc_wrote` line:

```python
    gs_wrote = _convert_gradientsports_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
```

Update the error message check (around line 768):

```python
    if not (sb_wrote or ws_wrote or idsse_wrote or metrica_wrote or sc_wrote or gs_wrote) and not existing_spadl_matches:
        msg = "No SPADL actions produced from any source (StatsBomb / Wyscout / IDSSE / Metrica / SkillCorner / GradientSports)"
```

Update `unscored_ids` union (around line 744), add:

```python
        | set(filter_result.metadata["gs_new"])
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestGuardIntegration -v`
Expected: All PASS.

- [ ] **Step 8: Lint check**

Run: `uv run ruff check src/ingestion/spadl_vaep.py`
Expected: No errors.

- [ ] **Step 9: Commit**

```bash
git add src/ingestion/spadl_vaep.py src/tests/test_gradientsports_spadl.py
git commit -m "feat(gs-spadl): wire GS into guard, chunk config, pipeline dispatch

gs_new diff detection via _diff_hashed_source_against_spadl.
Chunk size 50 (2 chunks for 64 WC2022 matches).
_run_chunk + run_pipeline Phase A both dispatch GS.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 6: HF License Gate + dbt Model YAML

**Files:**
- Modify: `scripts/publish_spadl_vaep_hf.py`
- Modify: `scripts/publish_tracking_context_hf.py`
- Modify: `dbt_project/models/staging/spadl/_spadl__models.yml`

- [ ] **Step 1: Write failing HF gate test**

Add to `src/tests/test_gradientsports_spadl.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestHfLicenseGate -v`
Expected: FAIL -- "gradientsports" not in either script.

- [ ] **Step 3: Add WHERE filter to publish_spadl_vaep_hf.py**

Edit `scripts/publish_spadl_vaep_hf.py`. Change the SQL query `_ACTION_VALUES_SQL` (around line 96). Add a `WHERE` clause before the closing triple-quote:

Before:
```python
FROM soccer_analytics.dev_gold.fct_action_values
"""
```

After:
```python
FROM soccer_analytics.dev_gold.fct_action_values
WHERE data_source != 'gradientsports'
"""
```

Add a comment above the WHERE:

```python
    -- HF license gate: GS data computed internally but not published
    -- until license secured. Remove this filter when license is in place.
    WHERE data_source != 'gradientsports'
```

- [ ] **Step 4: Add WHERE filter to publish_tracking_context_hf.py**

Edit `scripts/publish_tracking_context_hf.py`. Change `_TRACKING_CONTEXT_SQL` (around line 42):

Before:
```python
_TRACKING_CONTEXT_SQL = """\
SELECT * FROM soccer_analytics.dev_gold.fct_tracking_context
"""
```

After:
```python
_TRACKING_CONTEXT_SQL = """\
SELECT * FROM soccer_analytics.dev_gold.fct_tracking_context
WHERE data_source != 'gradientsports'
"""
```

- [ ] **Step 5: Update dbt model YAML**

Edit `dbt_project/models/staging/spadl/_spadl__models.yml`. At line 152, change:

```yaml
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']
```

to:

```yaml
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner', 'gradientsports']
```

At line 147, update the description:

```yaml
        description: Data provider (statsbomb, wyscout, idsse, metrica, skillcorner, gradientsports)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestHfLicenseGate -v`
Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/publish_spadl_vaep_hf.py scripts/publish_tracking_context_hf.py dbt_project/models/staging/spadl/_spadl__models.yml src/tests/test_gradientsports_spadl.py
git commit -m "feat(gs-spadl): HF license gate + dbt accepted_values for gradientsports

WHERE data_source != 'gradientsports' in both HF publishers.
Reversible gate -- remove when license is secured.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: ET Integration Tests + Direction-of-Play Coverage

**Files:**
- Modify: `src/tests/test_gradientsports_spadl.py`

Extra-time is a critical edge case: the silly-kicks converter raises `ValueError` when `home_team_start_left_extratime` is `None` but period 3/4 rows exist. WC2022 has 5 matches with ET.

Direction-of-play (`home_team_start_left=False`) must also be tested to avoid regressions.

- [ ] **Step 1: Write ET + direction tests**

Add to `src/tests/test_gradientsports_spadl.py`:

```python
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
            rows.append(
                _make_gs_bronze_row(period=period)
            )
        pdf = pd.DataFrame(rows)
        # ET column is already absent: _make_gs_bronze_row omits it when
        # home_team_start_left_extratime=None (the default).
        udf_fn = _make_gradientsports_spadl_udf()
        # The silly-kicks converter should raise ValueError via RuntimeError wrapper
        with pytest.raises(RuntimeError, match="GS SPADL conversion failed"):
            udf_fn(pdf)


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
```

- [ ] **Step 2: Run ET + direction tests**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py::TestExtraTimeIntegration src/tests/test_gradientsports_spadl.py::TestDirectionOfPlay -v`
Expected: All PASS (the converter enforces the ET invariant and normalizes direction).

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_gradientsports_spadl.py
git commit -m "test(gs-spadl): extra-time + direction-of-play integration tests

ET flag enforcement + home_team_start_left=False coverage.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: Full Test Suite Run + Squash

- [ ] **Step 1: Run the full GS test suite**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py src/tests/test_format_contract.py -v`
Expected: All PASS.

- [ ] **Step 2: Run ruff + pyright on all modified files**

```bash
uv run ruff check src/shared/identifiers.py src/ingestion/spadl_adapter.py src/ingestion/spadl_enrichments.py src/ingestion/spadl_udf_shared.py src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py scripts/publish_spadl_vaep_hf.py scripts/publish_tracking_context_hf.py src/tests/test_gradientsports_spadl.py src/tests/test_format_contract.py
```

```bash
uv run ruff format --check src/shared/identifiers.py src/ingestion/spadl_adapter.py src/ingestion/spadl_enrichments.py src/ingestion/spadl_udf_shared.py src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py scripts/publish_spadl_vaep_hf.py scripts/publish_tracking_context_hf.py src/tests/test_gradientsports_spadl.py src/tests/test_format_contract.py
```

```bash
uv run pyright src/shared/identifiers.py src/ingestion/spadl_adapter.py src/ingestion/spadl_enrichments.py src/ingestion/spadl_udf_shared.py
```

Expected: Zero errors.

- [ ] **Step 3: Run existing SPADL tests to verify no regressions**

```bash
uv run pytest src/tests/test_format_contract.py src/tests/test_source_onboarding_contracts.py -v
```

Expected: All existing tests still PASS.

- [ ] **Step 4: Bump wheel version**

This PR modifies dbt YAML (`_spadl__models.yml`), so a wheel bump is required before the dbt build will pick up the change on Databricks:

```bash
uv run python scripts/bump_wheel.py
```

- [ ] **Step 5: Squash commits into single feature commit**

Per user convention (single commit on feature branches), squash all task commits:

```bash
git rebase -i main
# squash all into one commit with message:
```

```
feat(gs-spadl): add Gradient Sports as 6th SPADL data source

- Native ID generators (ADR-018 format contracts)
- Adapter: 35-column rename + 6 derived from gameEvents.* + ball JSON parse + NaN-fill
- Metadata: home_team_id from gameEvents.homeTeam boolean + direction flags
- UDF factory: IDSSE batch pattern, tackle qualifier _native/_key mapping
- Orchestrator: groupBy(match_id).applyInPandas, replaceWhere
- Guard: gs_new diff, chunk size 50, pipeline Phase A dispatch
- HF license gate: WHERE data_source != 'gradientsports' (reversible)
- dbt: gradientsports in accepted_values
- Tests: adapter, ball JSON, derived cols, metadata, UDF, ET, direction, guard, format contracts

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
```

---

## Verification Checklist

After all tasks complete:

| Check | Command |
|-------|---------|
| All GS tests pass | `uv run pytest src/tests/test_gradientsports_spadl.py -v` |
| Format contract tests pass | `uv run pytest src/tests/test_format_contract.py -v` |
| Ruff lint clean | `uv run ruff check src/ scripts/` |
| Ruff format clean | `uv run ruff format --check src/ scripts/` |
| Pyright clean | `uv run pyright src/shared/identifiers.py src/ingestion/spadl_adapter.py src/ingestion/spadl_enrichments.py` |
| No regressions | `uv run pytest src/tests/ -v --ignore=src/tests/test_benchmark*.py` |
| Rename+derived coverage complete | `uv run python -c "from ingestion.spadl_adapter import _GS_BRONZE_TO_SNAKE; from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS; mapped = set(_GS_BRONZE_TO_SNAKE.values()); derived = {'event_id','period_id','time_seconds','player_id','team_id','set_piece_type','ball_x','ball_y'}; assert (mapped | derived) >= set(EXPECTED_INPUT_COLUMNS)"` |

## Important Notes

1. **Bronze column names VERIFIED**: The rename map in Task 2 was verified against `DESCRIBE soccer_analytics.bronze.gradientsports_events` (264 columns). Key differences from a naive assumption:
   - `event_id` comes from top-level `gameEventId`, NOT `possessionEvents.eventId`
   - `period_id`, `time_seconds`, `player_id`, `team_id`, `set_piece_type` come from `gameEvents.*`, NOT `possessionEvents.*`
   - `ball_x`/`ball_y` are parsed from `ball` JSON string (`[{"x":...,"y":...}]`), NOT from `possessionEvents.ballX`/`ballY`
   - `home_team_id` is derived from `gameEvents.homeTeam` (boolean) + `gameEvents.teamId` — `stadiumMetadata.homeTeamId` does NOT exist
   - `challenger_team_id` / `challenge_winner_team_id` DO NOT EXIST in bronze — NaN-filled, converter tolerates this

2. **Fixture limitations**: All tests use synthetic data. The UDF test may not exercise all 47 column paths. Once deployed, verify with real data via `uv run python -c "..."` against the Databricks SQL warehouse.

3. **HF license gate removal**: When the PFF/Gradient Sports license is secured, remove the `WHERE data_source != 'gradientsports'` from both publisher scripts.

4. **Wheel bump**: If this PR modifies dbt YAML (`_spadl__models.yml`), a wheel bump is needed before the dbt build will pick up the change on Databricks. Run `uv run python scripts/bump_wheel.py` after all code changes are final.
