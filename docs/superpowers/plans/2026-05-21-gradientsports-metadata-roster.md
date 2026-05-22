# Gradient Sports Metadata + Roster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest the two missing Gradient Sports API artifacts (metadata, roster) into bronze, expose them through staging views, onboard GS into all four Kimball dimension tables, close the SPADL `competition_native_id`/`season_native_id` pd.NA gap, and fix all related dbt test failures.

**Architecture:** Two new bronze tables (`gradientsports_metadata`, `gradientsports_roster`) ingested via the existing orchestrator loop, two dbt staging views, four dimension CTE additions, SPADL UDF patch for competition/season natives, ADR-018 compliance (identifiers.py generator + 4 dbt singular tests + source onboarding contracts).

**Tech Stack:** Python 3.10, pandas, PySpark, dbt (Databricks adapter), Delta Lake, pining-for-the-data REST API, silly-kicks SPADL.

**Spec:** `docs/superpowers/specs/2026-05-21-gradientsports-metadata-roster-design.md`

---

### Task 1: Add `gradientsports_native_competition_id` to identifiers.py

**Files:**
- Modify: `src/shared/identifiers.py:237-261`
- Modify: `src/tests/test_format_contract.py:327-385`

- [ ] **Step 1: Add the generator function**

In `src/shared/identifiers.py`, after `gradientsports_native_team_id` (line 261), add:

```python
def gradientsports_native_competition_id(raw_competition_id: str | int) -> str:
    """Canonical Gradient Sports native competition id -- stringified positive integer."""
    s = str(raw_competition_id)
    if not _GRADIENTSPORTS_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid Gradient Sports competition id: {raw_competition_id!r} (expected numeric string)")
    return s
```

- [ ] **Step 2: Add format-contract test**

In `src/tests/test_format_contract.py`, inside the GS test classes (after the team ID tests around line 385), add:

```python
class TestGradientSportsCompetitionIdFormatContract:
    def test_gradientsports_native_competition_id_valid_string(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id
        assert gradientsports_native_competition_id("38") == "38"

    def test_gradientsports_native_competition_id_valid_int(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id
        assert gradientsports_native_competition_id(38) == "38"

    def test_gradientsports_native_competition_id_rejects_alpha(self) -> None:
        import pytest
        from shared.identifiers import gradientsports_native_competition_id
        with pytest.raises(ValueError, match="invalid Gradient Sports competition id"):
            gradientsports_native_competition_id("abc")

    def test_gradientsports_native_competition_id_rejects_empty(self) -> None:
        import pytest
        from shared.identifiers import gradientsports_native_competition_id
        with pytest.raises(ValueError, match="invalid Gradient Sports competition id"):
            gradientsports_native_competition_id("")
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_format_contract.py -v -k "GradientSports" --no-header`
Expected: All GS format contract tests PASS (existing + new competition ID tests).

- [ ] **Step 4: Run ruff + pyright**

Run: `uv run ruff check src/shared/identifiers.py src/tests/test_format_contract.py && uv run pyright src/shared/identifiers.py`
Expected: 0 violations.

---

### Task 2: Create `gradientsports_metadata.py` bronze parser

**Files:**
- Create: `src/ingestion/gradientsports_metadata.py`
- Test: `src/tests/test_gradientsports_metadata.py`

- [ ] **Step 1: Write the parser test**

Create `src/tests/test_gradientsports_metadata.py`:

```python
"""Tests for gradientsports_metadata.py parser."""
from __future__ import annotations

import json

import pandas as pd
import pytest


# Minimal valid metadata dict matching the API shape (single-element list wrapper).
_SAMPLE_METADATA = [
    {
        "id": 10502,
        "homeTeam": {"id": 5629, "name": "Qatar", "shortName": "QAT"},
        "awayTeam": {"id": 5765, "name": "Ecuador", "shortName": "ECU"},
        "competition": {"id": 38, "name": "FIFA Men's World Cup"},
        "season": "2022",
        "date": "2022-11-20T16:00:00Z",
        "stadium": {
            "id": 101,
            "name": "Al Bayt Stadium",
            "pitches": [{"id": 1, "length": 105, "width": 68}],
        },
        "homeTeamStartLeft": True,
        "homeTeamStartLeftExtraTime": None,
        "fps": 29.97,
        "halfPeriod": 2712,
        "period1": 2712,
        "period2": 5600,
        "startPeriod1": 0,
        "endPeriod1": 2712,
        "startPeriod2": 2712,
        "endPeriod2": 5600,
        "week": 1,
        "videoUrl": "https://example.com/video",
        "homeTeamKit": {"primary": "#8A1538", "secondary": "#FFFFFF"},
        "awayTeamKit": {"primary": "#FFD100", "secondary": "#00008B"},
    }
]


class TestParseMetadata:
    def test_basic_parse(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        assert len(df) == 1
        assert df["match_id"].iloc[0] == "10502"
        assert "_ingested_at" in df.columns
        # json_normalize flattens homeTeam.id etc.
        assert "homeTeam.id" in df.columns
        assert "competition.id" in df.columns

    def test_from_json_string(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(json.dumps(_SAMPLE_METADATA), match_id="10502")
        assert len(df) == 1

    def test_match_id_validated(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            parse_metadata(_SAMPLE_METADATA, match_id="bad_id")

    def test_int_columns_widened_to_float64(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        int_cols = df.select_dtypes(include=["int64", "int32"]).columns
        assert len(int_cols) == 0, f"Expected no int columns, got: {list(int_cols)}"

    def test_list_fields_serialized_to_json_string(self) -> None:
        from ingestion.gradientsports_metadata import parse_metadata

        df = parse_metadata(_SAMPLE_METADATA, match_id="10502")
        pitches_val = df["stadium.pitches"].iloc[0]
        assert isinstance(pitches_val, str)
        parsed = json.loads(pitches_val)
        assert isinstance(parsed, list)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_gradientsports_metadata.py -v --no-header`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.gradientsports_metadata'`

- [ ] **Step 3: Write the parser module**

Create `src/ingestion/gradientsports_metadata.py`:

```python
"""Gradient Sports metadata ingestion — metadata artifact to bronze.

Parses the metadata artifact from the pining-for-the-data API and writes to
bronze.gradientsports_metadata. One row per match.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table
from shared.identifiers import gradientsports_native_match_id

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Fields that json_normalize leaves as list/dict — serialize to JSON strings.
# Note: homeTeamKit/awayTeamKit are dicts that json_normalize flattens to
# individual columns (homeTeamKit.primary, etc.), so they don't need serialization.
# Only stadium.pitches remains as a list that json_normalize keeps as-is.
_COMPLEX_FIELDS = ("stadium.pitches",)


def parse_metadata(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports metadata into a DataFrame.

    Args:
        source: Raw metadata (JSON string, dict, or list).
        match_id: Native match ID — validated via identifiers.py generator.

    Returns:
        DataFrame with metadata columns + match_id + _ingested_at.
    """
    validated_match_id = gradientsports_native_match_id(match_id)

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # API wraps metadata in a 1-element list
    if isinstance(data, list):
        if len(data) == 0:
            raise ValueError(f"Empty metadata list for match {validated_match_id}")
        data = data[0]

    df = pd.json_normalize(data)  # type: ignore[arg-type]

    # Serialize complex fields to JSON strings for Delta compatibility
    for col in _COMPLEX_FIELDS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)

    # Widen all integer columns to float64 (same pattern as events)
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype("float64")

    df["match_id"] = validated_match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_metadata(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed metadata DataFrame to bronze.gradientsports_metadata."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_metadata",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_metadata",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_metadata.py -v --no-header`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/gradientsports_metadata.py && uv run pyright src/ingestion/gradientsports_metadata.py`
Expected: 0 violations.

---

### Task 3: Create `gradientsports_roster.py` bronze parser

**Files:**
- Create: `src/ingestion/gradientsports_roster.py`
- Test: `src/tests/test_gradientsports_roster.py`

- [ ] **Step 1: Write the parser test**

Create `src/tests/test_gradientsports_roster.py`:

```python
"""Tests for gradientsports_roster.py parser."""
from __future__ import annotations

import json

import pandas as pd
import pytest

_SAMPLE_ROSTER = [
    {
        "player": {"id": "3861", "nickname": "Xavi Simons"},
        "team": {"id": "366", "name": "Netherlands"},
        "positionGroupType": "AM",
        "shirtNumber": "7",
        "started": True,
    },
    {
        "player": {"id": "4200", "nickname": "Memphis Depay"},
        "team": {"id": "366", "name": "Netherlands"},
        "positionGroupType": "CF",
        "shirtNumber": "10",
        "started": True,
    },
]


class TestParseRoster:
    def test_basic_parse(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(_SAMPLE_ROSTER, match_id="10502")
        assert len(df) == 2
        assert df["match_id"].iloc[0] == "10502"
        assert "_ingested_at" in df.columns
        assert "player.id" in df.columns
        assert "team.id" in df.columns

    def test_from_json_string(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(json.dumps(_SAMPLE_ROSTER), match_id="10502")
        assert len(df) == 2

    def test_match_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            parse_roster(_SAMPLE_ROSTER, match_id="bad_id")

    def test_player_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        bad_roster = [
            {
                "player": {"id": "player_3861", "nickname": "Bad ID"},
                "team": {"id": "366", "name": "Netherlands"},
                "positionGroupType": "AM",
                "shirtNumber": "7",
                "started": True,
            },
        ]
        with pytest.raises(ValueError, match="invalid Gradient Sports player id"):
            parse_roster(bad_roster, match_id="10502")

    def test_team_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        bad_roster = [
            {
                "player": {"id": "3861", "nickname": "OK Player"},
                "team": {"id": "team_366", "name": "Netherlands"},
                "positionGroupType": "AM",
                "shirtNumber": "7",
                "started": True,
            },
        ]
        with pytest.raises(ValueError, match="invalid Gradient Sports team id"):
            parse_roster(bad_roster, match_id="10502")

    def test_int_columns_widened_to_float64(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(_SAMPLE_ROSTER, match_id="10502")
        int_cols = df.select_dtypes(include=["int64", "int32"]).columns
        assert len(int_cols) == 0, f"Expected no int columns, got: {list(int_cols)}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_gradientsports_roster.py -v --no-header`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.gradientsports_roster'`

- [ ] **Step 3: Write the parser module**

Create `src/ingestion/gradientsports_roster.py`:

```python
"""Gradient Sports roster ingestion — roster artifact to bronze.

Parses the roster artifact from the pining-for-the-data API and writes to
bronze.gradientsports_roster. One row per player per match (~51 rows/match).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table
from shared.identifiers import (
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_roster(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports roster into a DataFrame.

    Args:
        source: Raw roster data (JSON string or list of dicts).
        match_id: Native match ID — validated via identifiers.py generator.

    Returns:
        DataFrame with roster columns + match_id + _ingested_at.
    """
    validated_match_id = gradientsports_native_match_id(match_id)

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    if isinstance(data, dict):
        data = data.get("roster", data.get("data", []))

    df = pd.json_normalize(data)  # type: ignore[arg-type]

    # Validate player.id and team.id via identifiers.py generators (ADR-018)
    if "player.id" in df.columns:
        for val in df["player.id"].dropna().unique():
            gradientsports_native_player_id(val)
    if "team.id" in df.columns:
        for val in df["team.id"].dropna().unique():
            gradientsports_native_team_id(val)

    # Widen all integer columns to float64 (same pattern as events)
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype("float64")

    df["match_id"] = validated_match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_roster(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed roster DataFrame to bronze.gradientsports_roster."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_roster",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_roster",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_gradientsports_roster.py -v --no-header`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/gradientsports_roster.py && uv run pyright src/ingestion/gradientsports_roster.py`
Expected: 0 violations.

---

### Task 4: Add metadata + roster to orchestrator + backfill flag

**Files:**
- Modify: `src/ingestion/gradientsports.py`
- Test: `src/tests/test_gradientsports_orchestrator_backfill.py`

- [ ] **Step 1: Write the backfill test**

Create `src/tests/test_gradientsports_orchestrator_backfill.py`:

```python
"""Tests for --backfill-artifacts orchestrator flag."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestBackfillArtifactsFlag:
    def test_backfill_skips_guard(self) -> None:
        """--backfill-artifacts should skip the guard entirely."""
        from ingestion.gradientsports import _backfill_artifacts

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(**{"match_id": "10502"}),
        ]

        # Mock match with metadata + roster artifact keys
        mock_match = MagicMock()
        mock_match.id = "10502"
        mock_match.artifacts = ["match_10502_metadata.json", "match_10502_roster.json"]

        with (
            patch("ingestion.gradientsports.resolve_pining_token", return_value="fake"),
            patch("ingestion.gradientsports.fetch_match_list", return_value=[mock_match]),
            patch("ingestion.gradientsports.fetch_artifact") as mock_fetch,
            patch("ingestion.gradientsports.parse_metadata") as mock_parse_meta,
            patch("ingestion.gradientsports.write_metadata") as mock_write_meta,
            patch("ingestion.gradientsports.parse_roster") as mock_parse_roster,
            patch("ingestion.gradientsports.write_roster") as mock_write_roster,
        ):
            import pandas as pd

            mock_fetch.return_value = MagicMock(text='[{}]')
            mock_parse_meta.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_parse_roster.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_write_meta.return_value = 1
            mock_write_roster.return_value = 1

            _backfill_artifacts(mock_spark, "cat", "bronze", MagicMock())

            # Guard not called (no timed_check, no skip_guard)
            mock_write_meta.assert_called_once()
            mock_write_roster.assert_called_once()

    def test_backfill_fetches_only_metadata_and_roster(self) -> None:
        """Backfill must NOT fetch events or tracking artifacts."""
        from ingestion.gradientsports import _backfill_artifacts

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(**{"match_id": "10502"}),
        ]

        # Mock match with all artifact types — backfill should only use metadata + roster
        mock_match = MagicMock()
        mock_match.id = "10502"
        mock_match.artifacts = [
            "match_10502_events.json",
            "match_10502_tracking.json",
            "match_10502_metadata.json",
            "match_10502_roster.json",
        ]

        with (
            patch("ingestion.gradientsports.resolve_pining_token", return_value="fake"),
            patch("ingestion.gradientsports.fetch_match_list", return_value=[mock_match]),
            patch("ingestion.gradientsports.fetch_artifact") as mock_fetch,
            patch("ingestion.gradientsports.parse_metadata") as mock_parse_meta,
            patch("ingestion.gradientsports.write_metadata") as mock_write_meta,
            patch("ingestion.gradientsports.parse_roster") as mock_parse_roster,
            patch("ingestion.gradientsports.write_roster") as mock_write_roster,
        ):
            import pandas as pd

            mock_fetch.return_value = MagicMock(text='[{}]')
            mock_parse_meta.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_parse_roster.return_value = pd.DataFrame({"match_id": ["10502"]})
            mock_write_meta.return_value = 1
            mock_write_roster.return_value = 1

            _backfill_artifacts(mock_spark, "cat", "bronze", MagicMock())

            # fetch_artifact called with metadata + roster keys, NOT events/tracking
            call_args = [c.args[1] for c in mock_fetch.call_args_list]
            for key in call_args:
                assert "event" not in key.lower()
                assert "track" not in key.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_gradientsports_orchestrator_backfill.py -v --no-header`
Expected: FAIL — `ImportError: cannot import name '_backfill_artifacts'`

- [ ] **Step 3: Update the orchestrator**

Modify `src/ingestion/gradientsports.py`:

**3a.** Add imports at the top (after the existing tracking imports, around line 35):

```python
from ingestion.gradientsports_metadata import parse_metadata, write_metadata
from ingestion.gradientsports_roster import parse_roster, write_roster
```

**3b.** In `ingest_gradientsports()`, after tracking write (line 211) and before events write (line 214), add metadata + roster handling. Replace the section from line 169 ("Phase 1") through line 219 (gc.collect) with:

```python
        # --- Phase 1: Download & parse all artifacts (no writes yet) ---
        events_df = None
        for artifact_key in match.artifacts:
            if "event" in artifact_key.lower():
                events_resp = fetch_artifact(mid, artifact_key, token)
                events_df = parse_events(events_resp.text, match_id=mid)
                logger.info("Parsed %d event rows for match %s", len(events_df), mid)
                break
        else:
            logger.warning("No event artifact found for match %s", mid)

        tracking_staged = False
        tracking_row_count = 0
        staging_path = _staging_path(catalog, schema, mid)
        for artifact_key in match.artifacts:
            if "track" in artifact_key.lower():
                tracking_resp = fetch_artifact(mid, artifact_key, token, stream=True)
                from ingestion.utils import ensure_volume_directory

                ensure_volume_directory(staging_path.rsplit("/", 1)[0])
                tracking_row_count = stream_tracking_to_parquet(
                    tracking_resp,
                    match_id=mid,
                    parquet_path=staging_path,
                    log=logger,
                )
                tracking_staged = True
                break
        else:
            logger.warning("No tracking artifact found for match %s", mid)

        metadata_df = None
        for artifact_key in match.artifacts:
            if "metadata" in artifact_key.lower():
                metadata_resp = fetch_artifact(mid, artifact_key, token)
                metadata_df = parse_metadata(metadata_resp.text, match_id=mid)
                logger.info("Parsed metadata for match %s", mid)
                break

        roster_df = None
        for artifact_key in match.artifacts:
            if "roster" in artifact_key.lower():
                roster_resp = fetch_artifact(mid, artifact_key, token)
                roster_df = parse_roster(roster_resp.text, match_id=mid)
                logger.info("Parsed %d roster rows for match %s", len(roster_df), mid)
                break

        # --- Phase 2: Write tracking -> metadata -> roster -> events ---
        if tracking_staged:
            write_tracking(spark, catalog, schema, mid, logger, staging_parquet=staging_path)
            logger.info("Wrote %d tracking rows for match %s", tracking_row_count, mid)

        if metadata_df is not None:
            write_metadata(spark, metadata_df, catalog, schema, mid, logger)
            logger.info("Wrote metadata for match %s", mid)

        if roster_df is not None:
            write_roster(spark, roster_df, catalog, schema, mid, logger)
            logger.info("Wrote %d roster rows for match %s", len(roster_df), mid)

        if events_df is not None:
            write_events(spark, events_df, catalog, schema, mid, logger)
            logger.info("Wrote %d event rows for match %s", len(events_df), mid)
            del events_df

        gc.collect()
```

**3c.** Add the `_backfill_artifacts` function after `run_pipeline()` (before `main()`):

```python
def _backfill_artifacts(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Backfill metadata + roster artifacts for matches already in bronze.

    Skips the guard entirely. Reads the match ID list from existing
    bronze.gradientsports_events, fetches metadata + roster from the API
    (NOT events or tracking), and writes to the new bronze tables.
    """
    token = resolve_pining_token()

    rows = spark.sql(
        f"SELECT DISTINCT match_id FROM {catalog}.{schema}.gradientsports_events"
    ).collect()
    match_ids = [str(r["match_id"]) for r in rows]
    logger.info("Backfill: %d matches to process", len(match_ids))

    all_matches = fetch_match_list(token, updated_since=None)
    match_map = {m.id: m for m in all_matches}

    for i, mid in enumerate(sorted(match_ids)):
        match = match_map.get(mid)
        if match is None:
            logger.warning("Backfill: match %s not in API match list — skipping", mid)
            continue

        logger.info("Backfill match %s (%d/%d)", mid, i + 1, len(match_ids))

        for artifact_key in match.artifacts:
            if "metadata" in artifact_key.lower():
                resp = fetch_artifact(mid, artifact_key, token)
                df = parse_metadata(resp.text, match_id=mid)
                write_metadata(spark, df, catalog, schema, mid, logger)
                logger.info("Wrote metadata for match %s", mid)
                break

        for artifact_key in match.artifacts:
            if "roster" in artifact_key.lower():
                resp = fetch_artifact(mid, artifact_key, token)
                df = parse_roster(resp.text, match_id=mid)
                write_roster(spark, df, catalog, schema, mid, logger)
                logger.info("Wrote %d roster rows for match %s", len(df), mid)
                break

    logger.info("Backfill complete: %d matches processed", len(match_ids))
```

**3d.** Add `--backfill-artifacts` flag to `main()`. In the `extra_args` list (line 257), add a second tuple:

```python
            (
                "--backfill-artifacts",
                {
                    "action": "store_true",
                    "default": False,
                    "help": (
                        "Backfill metadata + roster artifacts for matches already in "
                        "bronze. Skips the guard, fetches only metadata + roster."
                    ),
                },
            ),
```

And add the backfill branch in `main()` after `bootstrap_hooks` (line 278):

```python
    if args.backfill_artifacts:
        _logger.info("Backfill mode: ingesting metadata + roster for existing matches")
        _backfill_artifacts(spark, args.catalog, args.schema, _logger)
        return
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_gradientsports_orchestrator_backfill.py -v --no-header`
Expected: All tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/gradientsports.py && uv run pyright src/ingestion/gradientsports.py`
Expected: 0 violations.

---

### Task 5: Create dbt staging views + sources/models YAML

**Files:**
- Create: `dbt_project/models/staging/gradientsports/stg_gradientsports__metadata.sql`
- Create: `dbt_project/models/staging/gradientsports/stg_gradientsports__roster.sql`
- Create: `dbt_project/models/staging/gradientsports/_gradientsports__sources.yml`
- Create: `dbt_project/models/staging/gradientsports/_gradientsports__models.yml`

- [ ] **Step 1: Create the staging directory**

Run: `ls dbt_project/models/staging/` to verify directory structure, then create files.

- [ ] **Step 2: Create stg_gradientsports__metadata.sql**

```sql
-- stg_gradientsports__metadata.sql
-- Staging view over bronze.gradientsports_metadata.
-- Grain: one row per match. Source: pining-for-the-data metadata artifact.

select
    match_id,
    cast(`homeTeam.id` as string)     as home_team_id,
    `homeTeam.name`                   as home_team_name,
    `homeTeam.shortName`              as home_team_short_name,
    cast(`awayTeam.id` as string)     as away_team_id,
    `awayTeam.name`                   as away_team_name,
    `awayTeam.shortName`              as away_team_short_name,
    cast(`competition.id` as string)  as competition_id,
    `competition.name`                as competition_name,
    `season`                          as season_id,
    cast(`date` as timestamp)         as match_date,
    cast(`stadium.id` as string)      as stadium_id,
    `stadium.name`                    as stadium_name,
    `homeTeamStartLeft`               as home_team_start_left,
    `homeTeamStartLeftExtraTime`      as home_team_start_left_extra_time,
    `fps`,
    cast(`week` as int)               as matchweek,
    _ingested_at
from {{ source('gradientsports', 'gradientsports_metadata') }}
```

- [ ] **Step 3: Create stg_gradientsports__roster.sql**

```sql
-- stg_gradientsports__roster.sql
-- Staging view over bronze.gradientsports_roster.
-- Grain: one row per player per match (~51 rows/match).

select
    match_id,
    cast(`player.id` as string)   as player_id,
    `player.nickname`             as player_nickname,
    cast(`team.id` as string)     as team_id,
    `team.name`                   as team_name,
    `positionGroupType`           as position_group,
    `shirtNumber`                 as shirt_number,
    `started`,
    _ingested_at
from {{ source('gradientsports', 'gradientsports_roster') }}
```

- [ ] **Step 4: Create _gradientsports__sources.yml**

```yaml
version: 2

sources:
  - name: gradientsports
    description: >
      Gradient Sports (PFF) WC2022 open dataset via pining-for-the-data API.
      Center-origin meters coordinate system. 64 matches, all 4 artifact types.
    database: soccer_analytics
    schema: bronze
    loader: aws_lambda
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 30, period: day}

    tables:
      - name: gradientsports_metadata
        description: >
          Match metadata (one row per match). Contains team identities,
          competition, season, date, stadium, direction flags, fps, period
          timestamps. json_normalize flattened from API response.
        columns:
          - name: match_id
            description: Native match ID (numeric string, e.g. '10502')
          - name: homeTeam.id
            description: Home team numeric ID
          - name: homeTeam.name
            description: Home team name
          - name: homeTeam.shortName
            description: Home team short name (3-letter code)
          - name: awayTeam.id
            description: Away team numeric ID
          - name: awayTeam.name
            description: Away team name
          - name: awayTeam.shortName
            description: Away team short name (3-letter code)
          - name: competition.id
            description: Competition numeric ID (38 = FIFA WC)
          - name: competition.name
            description: Competition name
          - name: season
            description: Season identifier (e.g. '2022')
          - name: date
            description: ISO 8601 UTC match timestamp
          - name: stadium.id
            description: Stadium numeric ID
          - name: stadium.name
            description: Stadium name
          - name: homeTeamStartLeft
            description: Whether home team started on the left side
          - name: homeTeamStartLeftExtraTime
            description: Direction flag for extra time (nullable)
          - name: fps
            description: Frame rate (29.97)
          - name: week
            description: Matchweek number
          - name: _ingested_at
            description: UTC timestamp when the row was ingested

      - name: gradientsports_roster
        description: >
          Match roster (one row per player per match, ~51 rows/match).
          Contains player identity, team, position, shirt number, starter flag.
          json_normalize flattened from API response.
        columns:
          - name: match_id
            description: Native match ID (numeric string, e.g. '10502')
          - name: player.id
            description: Player numeric ID (e.g. '3861')
          - name: player.nickname
            description: Player display name
          - name: team.id
            description: Team numeric ID
          - name: team.name
            description: Team name
          - name: positionGroupType
            description: Position group code (GK, CB, AM, CF, etc.)
          - name: shirtNumber
            description: Shirt number (string)
          - name: started
            description: Whether player was in the starting lineup
          - name: _ingested_at
            description: UTC timestamp when the row was ingested
```

- [ ] **Step 5: Create _gradientsports__models.yml**

```yaml
version: 2

models:
  - name: stg_gradientsports__metadata
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Staging view over bronze.gradientsports_metadata. One row per match.
      Typed columns from json_normalize output. Feeds dim_matches,
      dim_teams, dim_competitions.
    columns:
      - name: match_id
        description: Native match ID (numeric string)
        data_tests:
          - not_null
      - name: home_team_id
        description: Home team ID (cast to string)
        data_tests:
          - not_null
      - name: home_team_name
        description: Home team name
        data_tests:
          - not_null
      - name: home_team_short_name
        description: Home team short name (3-letter code)
      - name: away_team_id
        description: Away team ID (cast to string)
        data_tests:
          - not_null
      - name: away_team_name
        description: Away team name
        data_tests:
          - not_null
      - name: away_team_short_name
        description: Away team short name (3-letter code)
      - name: competition_id
        description: Competition ID (cast to string)
        data_tests:
          - not_null
      - name: competition_name
        description: Competition name
        data_tests:
          - not_null
      - name: season_id
        description: Season identifier
      - name: match_date
        description: ISO 8601 UTC match timestamp
      - name: stadium_id
        description: Stadium ID (cast to string)
      - name: stadium_name
        description: Stadium name
      - name: home_team_start_left
        description: Home team direction flag
      - name: home_team_start_left_extra_time
        description: Extra-time direction flag (nullable)
      - name: fps
        description: Frame rate
      - name: matchweek
        description: Matchweek number
      - name: _ingested_at
        description: UTC ingestion timestamp

  - name: stg_gradientsports__roster
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Staging view over bronze.gradientsports_roster. One row per player
      per match (~51 rows/match). Feeds dim_players.
    columns:
      - name: match_id
        description: Native match ID (numeric string)
        data_tests:
          - not_null
      - name: player_id
        description: Player ID (cast to string)
        data_tests:
          - not_null
      - name: player_nickname
        description: Player display name
        data_tests:
          - not_null
      - name: team_id
        description: Team ID (cast to string)
        data_tests:
          - not_null
      - name: team_name
        description: Team name
        data_tests:
          - not_null
      - name: position_group
        description: Position group code (GK, CB, AM, CF, etc.)
      - name: shirt_number
        description: Shirt number (string)
      - name: started
        description: Whether player was in starting lineup
      - name: _ingested_at
        description: UTC ingestion timestamp
```

- [ ] **Step 6: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Parse succeeds with no errors (new models discovered).

---

### Task 6: Add GS CTEs to all four dimension tables

**Files:**
- Modify: `dbt_project/models/marts/dim_matches.sql`
- Modify: `dbt_project/models/marts/dim_teams.sql`
- Modify: `dbt_project/models/marts/dim_players.sql`
- Modify: `dbt_project/models/marts/dim_competitions.sql`

- [ ] **Step 1: Add gradientsports_matches CTE to dim_matches.sql**

After `skillcorner_matches` CTE (line 107) and before `unioned` (line 109), add:

```sql
gradientsports_matches as (

    select
        cast(match_id as string)       as native_match_id,
        'gradientsports'               as provider,
        competition_id,
        season_id,
        cast(match_date as date)       as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_gradientsports__metadata') }}

),
```

Add `select * from gradientsports_matches` to the `unioned` UNION ALL chain (after `skillcorner_matches`).

Update header comment (line 6) to include "SkillCorner, and Gradient Sports".

- [ ] **Step 2: Add gradientsports_teams CTE to dim_teams.sql**

After `skillcorner_teams` CTE (line 156) and before `unioned` (line 158), add:

```sql
gradientsports_teams as (

    -- Gradient Sports teams sourced from stg_gradientsports__metadata (more
    -- authoritative than roster — metadata carries shortName). UNION of home +
    -- away teams with GROUP BY for dedup across 64 matches.
    select
        'gradientsports'                                as provider,
        native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        max(team_name)                                  as team_name,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from (
        select home_team_id as native_team_id, home_team_name as team_name
        from {{ ref('stg_gradientsports__metadata') }}
        union all
        select away_team_id as native_team_id, away_team_name as team_name
        from {{ ref('stg_gradientsports__metadata') }}
    )
    where native_team_id is not null
    group by native_team_id

),
```

Add `select * from gradientsports_teams` to the `unioned` UNION ALL chain (after `skillcorner_teams`).

Update header comment (line 6) to include "SkillCorner, and Gradient Sports".

- [ ] **Step 3: Add gradientsports_players CTE to dim_players.sql**

After `skillcorner_players` CTE (line 161) and before `unioned` (line 163), add:

```sql
gradientsports_players as (

    -- Gradient Sports players sourced from stg_gradientsports__roster.
    -- GROUP BY for dedup (same player appears in multiple matches).
    select
        player_id                                       as native_player_id,
        cast(null as bigint)                            as player_id_legacy,
        max(player_nickname)                            as player_name,
        max(player_nickname)                            as player_display_name,
        max(position_group)                             as primary_position,
        'gradientsports'                                as provider,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_gradientsports__roster') }}
    where player_id is not null
    group by player_id

),
```

Add `select * from gradientsports_players` to the `unioned` UNION ALL chain (after `skillcorner_players`).

Update header comment (line 1-2) to include "Gradient Sports".

- [ ] **Step 4: Add gradientsports_competitions CTE to dim_competitions.sql**

After `metrica_competitions` CTE (line 95) and before `all_competitions` (line 97), add:

```sql
gradientsports_competitions as (

    -- Gradient Sports competition: single WC2022 entry (id=38,
    -- name="FIFA Men's World Cup").
    select distinct
        'gradientsports'                   as provider,
        competition_id                     as native_competition_id,
        cast(null as int)                  as competition_id_legacy,
        competition_name

    from {{ ref('stg_gradientsports__metadata') }}
    where competition_id is not null

),
```

Add `select * from gradientsports_competitions` to the `all_competitions` UNION ALL chain (after `metrica_competitions`).

Update header comment (lines 6-7) to include "Gradient Sports".

- [ ] **Step 5: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Parse succeeds with no errors.

---

### Task 7: Fix dbt test accepted_values + not_null WHERE filters

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Add 'gradientsports' to all accepted_values lists**

Update the following `values:` arrays to add `'gradientsports'`:

| Line | Model.column | Current values |
|------|-------------|----------------|
| 134 | fct_shots.data_source | `['statsbomb', 'wyscout']` |
| 328 | fct_passes.data_source | `['statsbomb', 'wyscout', 'idsse', 'metrica']` |
| 461 | fct_player_stats.data_source | `['statsbomb', 'wyscout', 'idsse', 'metrica']` |
| 884 | fct_action_values.data_source | `['statsbomb', 'wyscout', 'idsse', 'metrica']` |
| 2479 | dim_players.provider | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |
| 2537 | dim_players.data_sources | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |
| 2566 | dim_teams.provider | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |
| 2609 | dim_teams.data_source | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |
| 2616 | dim_teams.team_data_source | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |
| 2663 | dim_competitions.provider | `['statsbomb', 'wyscout', 'idsse', 'metrica']` |
| 4600 | dim_matches.provider | `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner']` |

- [ ] **Step 2: Add 'gradientsports' to not_null WHERE filters**

At line 704, change:
```yaml
data_source IN ('statsbomb', 'wyscout', 'idsse')
```
to:
```yaml
data_source IN ('statsbomb', 'wyscout', 'idsse', 'gradientsports')
```

At line 720, change:
```yaml
data_source IN ('statsbomb', 'wyscout', 'idsse')
```
to:
```yaml
data_source IN ('statsbomb', 'wyscout', 'idsse', 'gradientsports')
```

- [ ] **Step 3: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Parse succeeds.

---

### Task 8: Patch SPADL UDF — inject competition_native_id + season_native_id from metadata

**Files:**
- Modify: `src/ingestion/spadl_conversion.py:2003-2010`
- Modify: `src/tests/test_gradientsports_spadl.py` (remove `_make_gs_bronze_row` definition)
- Modify: `src/tests/conftest.py` (add `_make_gs_bronze_row` shared helper)
- Test: add competition/season injection tests in `src/tests/test_gradientsports_spadl.py`

- [ ] **Step 0: Move `_make_gs_bronze_row` to shared test fixture**

Move the `_make_gs_bronze_row` helper from `src/tests/test_gradientsports_spadl.py` into `src/tests/conftest.py` to avoid cross-test-file import coupling. Both `test_gradientsports_spadl.py` and the new competition/season injection tests need this helper.

In `src/tests/conftest.py`, add the function (copy from `test_gradientsports_spadl.py` lines 22-79). In `test_gradientsports_spadl.py`, replace the function definition with an import:

```python
from tests.conftest import _make_gs_bronze_row
```

Or, since conftest.py is auto-discovered by pytest, just remove the local definition — any test in the `tests/` directory can call fixtures defined in conftest.py. However, `_make_gs_bronze_row` is a plain function (not a `@pytest.fixture`), so it needs an explicit import. Use:

```python
from conftest import _make_gs_bronze_row
```

Run existing tests to confirm no breakage: `uv run pytest src/tests/test_gradientsports_spadl.py -v --no-header -x`

- [ ] **Step 1: Add metadata table read to `_convert_gradientsports_from_bronze`**

The SPADL UDF reads from `bronze.gradientsports_events`, not metadata. `competition.id` and `season` exist only in `bronze.gradientsports_metadata`. The `_convert_gradientsports_from_bronze` function (line 2101) must also read the metadata table to get `competition.id` and `season` per match, then pass those values to the UDF factory.

In `_convert_gradientsports_from_bronze()` (line 2101), after reading the events table (line 2129), add a read of the metadata table:

```python
    # Read competition/season from metadata bronze (populated by backfill).
    metadata_table = f"{catalog}.{schema}.gradientsports_metadata"
    gs_comp_season: dict[str, tuple[str, str]] = {}
    with tolerate_missing_table(logger, "GS metadata table not yet created — competition/season will be NULL"):
        meta_rows = (
            spark.table(metadata_table)
            .select("match_id", "`competition.id`", "season")
            .collect()
        )
        for row in meta_rows:
            mid = str(row["match_id"])
            comp_id = str(row["competition.id"]) if row["competition.id"] is not None else ""
            season = str(row["season"]) if row["season"] is not None else ""
            gs_comp_season[mid] = (comp_id, season)
```

Then pass `gs_comp_season` into the UDF factory:

Change `_make_gradientsports_spadl_udf()` (line 1835) to accept a parameter:

```python
def _make_gradientsports_spadl_udf(
    gs_comp_season: dict[str, tuple[str, str]] | None = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
```

Inside the UDF closure, use the lookup to populate competition/season:

Replace lines 2004-2010:
```python
        # match-level natives
        comp_id_str, season_str = ("", "")
        if _gs_comp_season and match_id_str in _gs_comp_season:
            comp_id_str, season_str = _gs_comp_season[match_id_str]

        actions = _apply_match_natives(
            actions,
            home_team_id_native=str(metadata["home_team_id"]),
            competition_native_id=_gs_comp_id(comp_id_str) if comp_id_str else _pd.NA,  # type: ignore[arg-type]
            season_native_id=season_str if season_str else _pd.NA,  # type: ignore[arg-type]
            match_id_native=_gs_match_id(match_id_str),
        )
```

Add the import inside the closure (near line 1863):
```python
from shared.identifiers import gradientsports_native_competition_id as _gs_comp_id
```

And capture the parameter in the closure:
```python
    _gs_comp_season = gs_comp_season
```

Update the call site in `_convert_gradientsports_from_bronze` (line 2211):
```python
    udf_fn = _make_gradientsports_spadl_udf(gs_comp_season=gs_comp_season)
```

- [ ] **Step 2: Add test for competition/season injection**

In `src/tests/test_gradientsports_spadl.py`, add a test that verifies competition_native_id is populated when `gs_comp_season` is provided:

```python
class TestCompetitionSeasonInjection:
    """Verify SPADL UDF injects competition/season from metadata lookup."""

    def test_competition_native_id_populated_when_metadata_available(self) -> None:
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        gs_comp_season = {"10502": ("38", "2022")}
        udf_fn = _make_gradientsports_spadl_udf(gs_comp_season=gs_comp_season)

        # Build a minimal synthetic bronze DataFrame using the existing fixture helper
        rows = [
            _make_gs_bronze_row(match_id="10502", game_event_type="PA",
                                possession_event_type="PA", pass_type="Short",
                                pass_outcome_type="Complete"),
            _make_gs_bronze_row(match_id="10502", game_event_type="PA",
                                possession_event_type="PA", pass_type="Short",
                                pass_outcome_type="Incomplete", game_event_id=6498521.0,
                                possession_event_id=8002.0, start_game_clock=2810.0),
        ]
        df = pd.DataFrame(rows)

        result = udf_fn(df)
        assert "competition_native_id" in result.columns
        non_null = result["competition_native_id"].dropna()
        assert len(non_null) > 0, "Expected competition_native_id to be populated from gs_comp_season"
        assert non_null.iloc[0] == "38"

        # season_native_id should also be populated
        season_vals = result["season_native_id"].dropna()
        assert len(season_vals) > 0
        assert season_vals.iloc[0] == "2022"

    def test_competition_native_id_na_when_no_metadata(self) -> None:
        from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

        udf_fn = _make_gradientsports_spadl_udf(gs_comp_season=None)

        # Build a minimal bronze DF — UDF should still produce output with pd.NA
        rows = [
            _make_gs_bronze_row(match_id="10502", game_event_type="PA",
                                possession_event_type="PA", pass_type="Short",
                                pass_outcome_type="Complete"),
        ]
        df = pd.DataFrame(rows)

        result = udf_fn(df)
        assert "competition_native_id" in result.columns
        # With no gs_comp_season, competition_native_id should be exactly pd.NA
        # (not None or np.nan — pd.NA is what the UDF sets at line 2007-2008,
        # and downstream Spark behavior differs: None → SQL NULL, pd.NA → depends on dtype)
        assert result["competition_native_id"].iloc[0] is pd.NA
        assert result["season_native_id"].iloc[0] is pd.NA
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py -v --no-header -x`
Expected: All tests PASS (existing + new).

- [ ] **Step 4: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/spadl_conversion.py src/ingestion/spadl_adapter.py && uv run pyright src/ingestion/spadl_conversion.py`
Expected: 0 violations.

---

### Task 9: Add ADR-018 dbt singular JOIN-coverage tests

**Files:**
- Create: `dbt_project/tests/assert_gradientsports_match_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_gradientsports_team_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_gradientsports_player_id_native_join_resolves.sql`
- Create: `dbt_project/tests/assert_gradientsports_competition_native_id_join_resolves.sql`

- [ ] **Step 1: Create match ID test**

```sql
-- assert_gradientsports_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- Asserts bronze.spadl_actions.match_id_native for Gradient Sports rows resolves
-- in dim_matches.native_match_id.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.match_id_native is not null
  and d.match_key is null
```

- [ ] **Step 2: Create team ID test**

```sql
-- assert_gradientsports_team_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.team_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_teams') }} d
    on b.team_id_native = d.native_team_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.team_id_native is not null
  and d.team_key is null
```

- [ ] **Step 3: Create player ID test**

```sql
-- assert_gradientsports_player_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.player_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_players') }} d
    on b.player_id_native = d.native_player_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.player_id_native is not null
  and d.player_key is null
```

- [ ] **Step 4: Create competition native ID test**

```sql
-- assert_gradientsports_competition_native_id_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- Non-vacuous: SPADL UDF injects competition_native_id from metadata
-- bronze (see spec "SPADL UDF Changes" section).

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.competition_native_id
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_competitions') }} d
    on b.competition_native_id = d.native_competition_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.competition_native_id is not null
  and d.competition_key is null
```

- [ ] **Step 5: Validate dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Parse succeeds with 4 new test nodes.

---

### Task 10: Add GS to source onboarding contracts + staging coverage tests

**Files:**
- Modify: `src/tests/test_source_onboarding_contracts.py`
- Modify: `src/tests/test_staging_coverage.py`

- [ ] **Step 1: Add GS parametrization to test_source_onboarding_contracts.py**

Remove the TODO comment (line 6). Add GS to the parametrize list (after skillcorner):

```python
        ("gradientsports", "gs_match_10502.parquet"),
```

Add a GS branch in `test_native_id_columns_present`. GS needs special handling like SkillCorner (the UDF takes a keyword arg). Add after the `if source == "skillcorner"` block:

```python
        elif source == "gradientsports":
            from ingestion.spadl_conversion import _make_gradientsports_spadl_udf
            udf = _make_gradientsports_spadl_udf(gs_comp_season=None)
```

For `test_native_id_format_contract`, add a GS branch:
```python
        elif source == "gradientsports":
            from ingestion.spadl_adapter import (
                adapt_gradientsports_events,
                extract_gradientsports_match_metadata,
            )
            import silly_kicks.spadl.gradientsports as _spadl_gs

            metadata = extract_gradientsports_match_metadata(df)
            adapted = adapt_gradientsports_events(df)
            actions, _ = _spadl_gs.convert_to_actions(
                adapted,
                home_team_id=metadata["home_team_id"],
                home_team_start_left=metadata["home_team_start_left"],
                home_team_start_left_extratime=metadata["home_team_start_left_extratime"],
            )
```

Add GS validator:
```python
            "gradientsports": lambda v: gradientsports_native_player_id(int(v)),
```

- [ ] **Step 1b: Generate the `gs_match_10502.parquet` fixture**

The fixture is generated synthetically using `_make_gs_bronze_row` from `src/tests/conftest.py` (moved there in Task 8 Step 0 below). Create `src/tests/fixtures/silly_kicks_boundary/gs_match_10502.parquet` by adding a generation script step:

```python
# In a temporary script (run once, commit result):
import pandas as pd
from tests.conftest import _make_gs_bronze_row
from ingestion.spadl_conversion import _make_gradientsports_spadl_udf

# Build 10 synthetic bronze rows with varied event types
rows = [
    _make_gs_bronze_row(match_id="10502", game_event_id=6498520.0 + i,
                        possession_event_id=8001.0 + i,
                        start_game_clock=2800.0 + i * 10,
                        game_event_type="PA" if i < 5 else "OTB",
                        possession_event_type="PA" if i < 5 else "OTB",
                        pass_type="Short" if i < 5 else "",
                        pass_outcome_type="Complete" if i < 3 else "Incomplete" if i < 5 else "",
                        player_id=12345.0 + (i % 3),
                        team_id=366.0 if i < 7 else 367.0,
                        home_team=i < 7)
    for i in range(10)
]
df = pd.DataFrame(rows)
udf_fn = _make_gradientsports_spadl_udf(gs_comp_season={"10502": ("38", "2022")})
result = udf_fn(df)
result.to_parquet("src/tests/fixtures/silly_kicks_boundary/gs_match_10502.parquet", index=False)
```

Run this once during implementation and commit the resulting parquet file. Verify the file exists and has >0 rows before proceeding.

- [ ] **Step 2: Add GS to staging coverage tests**

In `src/tests/test_staging_coverage.py`, add to `PROVIDER_COVERAGE` dict (after `"tracking_context"` entry):

```python
    "gradientsports": [
        ("gradientsports_metadata", "stg_gradientsports__metadata"),
        ("gradientsports_roster", "stg_gradientsports__roster"),
    ],
```

Add to `RENAMES` dict:

```python
    ("gradientsports", "gradientsports_metadata"): {
        "homeTeam.id": "home_team_id",
        "homeTeam.name": "home_team_name",
        "homeTeam.shortName": "home_team_short_name",
        "awayTeam.id": "away_team_id",
        "awayTeam.name": "away_team_name",
        "awayTeam.shortName": "away_team_short_name",
        "competition.id": "competition_id",
        "competition.name": "competition_name",
        "season": "season_id",
        "date": "match_date",
        "stadium.id": "stadium_id",
        "stadium.name": "stadium_name",
        "homeTeamStartLeft": "home_team_start_left",
        "homeTeamStartLeftExtraTime": "home_team_start_left_extra_time",
        "week": "matchweek",
    },
    ("gradientsports", "gradientsports_roster"): {
        "player.id": "player_id",
        "player.nickname": "player_nickname",
        "team.id": "team_id",
        "team.name": "team_name",
        "positionGroupType": "position_group",
        "shirtNumber": "shirt_number",
    },
```

- [ ] **Step 3: Run staging coverage tests**

Run: `uv run pytest src/tests/test_staging_coverage.py -v -k "gradientsports" --no-header`
Expected: Tests PASS (once the sources+models YAML from Task 5 are in place).

- [ ] **Step 4: Run ruff**

Run: `uv run ruff check src/tests/test_source_onboarding_contracts.py src/tests/test_staging_coverage.py`
Expected: 0 violations.

---

### Task 11: Update orchestrator docstring + bronze table list

**Files:**
- Modify: `src/ingestion/gradientsports.py` (docstring at line 1-16)

- [ ] **Step 1: Update module docstring**

Update the docstring at the top of `gradientsports.py` to list all 4 bronze tables:

```python
"""Gradient Sports ingestion orchestrator.

Discovers matches via the pining-for-the-data REST API,
downloads events + tracking + metadata + roster, and writes to bronze.

Bronze tables produced:
  - gradientsports_events    (raw events)
  - gradientsports_tracking  (narrow format: one row per player per frame)
  - gradientsports_metadata  (match metadata: one row per match)
  - gradientsports_roster    (player roster: one row per player per match)

Coordinate system (preserved in bronze):
  Center-origin meters. silly-kicks convert_to_frames handles the final transform.

LICENSE GATE: Data approved for internal calibration/training only.
NOT published to HF datasets, gold marts, synced tables, or Taipy UI
until Gradient Sports license confirmed in writing.
"""
```

- [ ] **Step 2: Run ruff**

Run: `uv run ruff check src/ingestion/gradientsports.py`
Expected: 0 violations.

---

### Task 12: Run full test suite + dbt parse validation

**Files:** None (verification only)

- [ ] **Step 1: Run all GS-related unit tests**

Run: `uv run pytest src/tests/test_gradientsports_metadata.py src/tests/test_gradientsports_roster.py src/tests/test_gradientsports_orchestrator_backfill.py src/tests/test_format_contract.py -v --no-header -k "GradientSports or gradientsports or metadata or roster"`
Expected: All PASS.

- [ ] **Step 2: Run ruff + pyright on all modified files**

Run: `uv run ruff check src/ingestion/gradientsports.py src/ingestion/gradientsports_metadata.py src/ingestion/gradientsports_roster.py src/shared/identifiers.py && uv run pyright src/ingestion/gradientsports.py src/ingestion/gradientsports_metadata.py src/ingestion/gradientsports_roster.py src/shared/identifiers.py`
Expected: 0 violations.

- [ ] **Step 3: Run full dbt parse**

Run: `cd dbt_project && uv run dbt parse --profiles-dir .`
Expected: Parse succeeds with all new models + tests.

- [ ] **Step 4: Run full test suite to catch regressions**

Run: `uv run pytest src/tests/ -v --no-header -x --timeout=120`
Expected: All PASS. Changes to `identifiers.py`, `spadl_conversion.py`, and `_marts__models.yml` could affect unrelated tests.

- [ ] **Step 5: Run existing GS SPADL tests to check no regressions**

Run: `uv run pytest src/tests/test_gradientsports_spadl.py -v --no-header -x`
Expected: All PASS.

---

### Task 13: Commit

- [ ] **Step 1: Stage all changes**

```
git add \
  src/shared/identifiers.py \
  src/ingestion/gradientsports.py \
  src/ingestion/gradientsports_metadata.py \
  src/ingestion/gradientsports_roster.py \
  src/ingestion/spadl_conversion.py \
  src/tests/test_format_contract.py \
  src/tests/test_gradientsports_metadata.py \
  src/tests/test_gradientsports_roster.py \
  src/tests/test_gradientsports_orchestrator_backfill.py \
  src/tests/test_gradientsports_spadl.py \
  src/tests/test_source_onboarding_contracts.py \
  src/tests/test_staging_coverage.py \
  src/tests/fixtures/silly_kicks_boundary/gs_match_10502.parquet \
  dbt_project/models/staging/gradientsports/ \
  dbt_project/models/marts/dim_matches.sql \
  dbt_project/models/marts/dim_teams.sql \
  dbt_project/models/marts/dim_players.sql \
  dbt_project/models/marts/dim_competitions.sql \
  dbt_project/models/marts/_marts__models.yml \
  dbt_project/tests/assert_gradientsports_*.sql \
  docs/superpowers/specs/2026-05-21-gradientsports-metadata-roster-design.md
```

- [ ] **Step 2: Commit (with user approval)**

```
feat(gs): ingest metadata + roster, onboard GS into Kimball dimensions

- Add gradientsports_metadata.py + gradientsports_roster.py bronze parsers
- Add --backfill-artifacts flag to orchestrator for existing matches
- Add stg_gradientsports__metadata + stg_gradientsports__roster staging views
- Add GS CTEs to dim_matches, dim_teams, dim_players, dim_competitions
- Patch SPADL UDF to inject competition_native_id + season_native_id from metadata
- Add gradientsports_native_competition_id to identifiers.py (ADR-018)
- Add 4 dbt singular JOIN-coverage tests (ADR-018)
- Fix accepted_values + not_null WHERE filters in _marts__models.yml
- Add GS to staging coverage + source onboarding contracts
```
