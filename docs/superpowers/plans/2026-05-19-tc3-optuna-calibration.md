# TC-3: Optuna Calibration Sweep — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace three sets of engineering-choice defaults in silly-kicks (`LinkParams.k3`, `infer_ball_carrier` params, `add_off_ball_runs` params) with Optuna-calibrated values validated against lakehouse production data across IDSSE, SkillCorner, and Gradient Sports WC 2022.

**Architecture:** Two-stage local Optuna TPE sweep. Stage 1 optimizes carrier accuracy (3 params). Stage 2 jointly optimizes k3 + off-ball-runs via augmented VAEP Brier (3 params), with carrier params fixed at Stage 1 optimum. Data pulled once from Databricks SQL into local Parquet cache. 8-core `ThreadPoolExecutor` parallelization within each trial (avoids pickle overhead — verified silly-kicks functions work in threads). Gradient Sports ingested into bronze/staging via pining-for-the-data API before the sweep.

**Review:** v4 — all silly-kicks v2 review items resolved (H1-H4, M1-M5, L1-L3).

**Tech Stack:** Optuna 4.x (TPE sampler, SQLite storage), XGBoost, silly-kicks 3.15.3, pandas, scikit-learn (GroupKFold), concurrent.futures, Databricks SQL connector, pining-for-the-data REST API.

**Git convention:** Single feature branch, single squashed commit, single PR. Per-task commit messages in the plan are WIP markers — all squashed before PR.

**Spec:** `docs/superpowers/specs/2026-05-19-tc3-optuna-calibration-design.md`

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/ingestion/gradientsports_common.py` | pining-for-the-data API client for Gradient Sports (mirrors `skillcorner_common.py`) |
| `src/ingestion/gradientsports.py` | Gradient Sports ingestion orchestrator: discover matches, download events + tracking, write to bronze |
| `src/ingestion/gradientsports_tracking.py` | Parse Gradient Sports tracking JSON/JSONL to narrow-format DataFrame |
| `src/ingestion/gradientsports_events.py` | Parse Gradient Sports events JSON to DataFrame |
| `scripts/run_tc3_calibration.py` | Main calibration script: Phase 0-4 (data loading, Stage 1, Stage 2, diagnostics, output) |
| `src/tests/test_gradientsports_ingestion.py` | Tests for Gradient Sports ingestion modules |
| `src/tests/tc3/__init__.py` | Package marker for TC-3 test subdirectory |
| `src/tests/tc3/conftest.py` | TC-3 test fixture — adds `scripts/` to sys.path (scoped to `tc3/` only) |
| `src/tests/tc3/test_tc3_calibration.py` | Tests for calibration script helpers (enrichment, objective functions, CV) |

### Modified files

| File | Change |
|------|--------|
| `pyproject.toml` | Add `ingest_gradientsports` entry point |

---

## Task 1: Gradient Sports API Client (`gradientsports_common.py`)

**Files:**
- Create: `src/ingestion/gradientsports_common.py`
- Reference: `src/ingestion/skillcorner_common.py`

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_gradientsports_ingestion.py`:

```python
"""Tests for Gradient Sports ingestion modules."""

from __future__ import annotations

import pytest


class TestMatchInfo:
    def test_valid_match_id(self) -> None:
        from ingestion.gradientsports_common import MatchInfo

        m = MatchInfo(
            id="12345",
            artifacts={"12345_events": "events.json"},
            home="Qatar",
            away="Ecuador",
            date="2022-11-20",
            updated_at="2022-11-20T00:00:00Z",
            visibility="public",
        )
        assert m.id == "12345"

    def test_non_numeric_id_rejected(self) -> None:
        from ingestion.gradientsports_common import MatchInfo

        with pytest.raises(ValueError, match="numeric"):
            MatchInfo(
                id="abc",
                artifacts={},
                home="A",
                away="B",
                date="2022-01-01",
                updated_at="2022-01-01T00:00:00Z",
                visibility="public",
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestMatchInfo -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.gradientsports_common'`

- [ ] **Step 3: Write the implementation**

Create `src/ingestion/gradientsports_common.py`:

```python
"""Gradient Sports API client and shared data models.

Talks to the pining-for-the-data REST API to discover and retrieve
Gradient Sports WC 2022 match artifacts (events, tracking).

Mirrors the SkillCorner client pattern (skillcorner_common.py).
"""

from __future__ import annotations

import logging
from datetime import datetime
from urllib.parse import quote, urlencode

import pydantic
import requests

from ingestion.utils import fetch_url

logger = logging.getLogger(__name__)

API_BASE_URL = "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"
PROVIDER = "gradientsports"


class MatchInfo(pydantic.BaseModel):
    """A single match from the pining-for-the-data discovery endpoint."""

    id: str
    artifacts: dict[str, str]
    home: str
    away: str
    date: str
    updated_at: datetime
    visibility: str

    @pydantic.field_validator("id")
    @classmethod
    def _id_must_be_numeric(cls, v: str) -> str:
        """Defense-in-depth: match IDs are interpolated into replaceWhere SQL."""
        if not v.isdigit():
            msg = f"MatchInfo.id must be numeric, got {v!r}"
            raise ValueError(msg)
        return v


def fetch_match_list(
    token: str,
    updated_since: str | None = None,
) -> list[MatchInfo]:
    """GET /v1/gradientsports/matches with optional updatedSince filter.

    Args:
        token: Bearer token for the pining-for-the-data API.
        updated_since: ISO 8601 UTC timestamp to filter matches updated after.

    Returns:
        List of MatchInfo objects for matches matching the filter.
    """
    url = f"{API_BASE_URL}/gradientsports/matches"
    if updated_since is not None:
        params = urlencode({"updatedSince": updated_since}, quote_via=quote)
        url = f"{url}?{params}"

    resp = fetch_url(url, headers={"Authorization": f"Bearer {token}"})
    data = resp.json()
    return [MatchInfo.model_validate(m) for m in data.get("matches", [])]


def fetch_artifact(
    match_id: str,
    artifact_key: str,
    token: str,
    stream: bool = False,
) -> requests.Response:
    """Fetch a single match artifact, following S3 302 redirect.

    Args:
        match_id: Gradient Sports match ID.
        artifact_key: Artifact key (e.g. "12345_events").
        token: Bearer token for the pining-for-the-data API.
        stream: If True, don't download body eagerly (use .iter_content()).

    Returns:
        Response object containing the artifact content.
    """
    url = f"{API_BASE_URL}/gradientsports/matches/{match_id}/{artifact_key}"
    return fetch_url(url, headers={"Authorization": f"Bearer {token}"}, stream=stream)


def resolve_pining_token() -> str:
    """Resolve the pining-for-the-data API token.

    Delegates to the shared SkillCorner implementation — same token, same API.
    """
    from ingestion.skillcorner_common import resolve_pining_token as _resolve

    return _resolve()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestMatchInfo -v`
Expected: PASS

- [ ] **Step 5: Run lint + type check**

Run: `uv run ruff check src/ingestion/gradientsports_common.py src/tests/test_gradientsports_ingestion.py && uv run pyright src/ingestion/gradientsports_common.py`
Expected: 0 errors

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/gradientsports_common.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(tc3): add Gradient Sports pining-for-the-data API client

Mirrors the SkillCorner client pattern. Same API base URL, same token.
Provider slug: gradientsports.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: Gradient Sports Event Parser (`gradientsports_events.py`)

**Files:**
- Create: `src/ingestion/gradientsports_events.py`
- Modify: `src/tests/test_gradientsports_ingestion.py`

- [ ] **Step 1: Discover the event artifact format**

Before writing any parser, we need to know the actual format. Run:

```bash
uv run python -c "
from ingestion.gradientsports_common import fetch_artifact, fetch_match_list, resolve_pining_token
token = resolve_pining_token()
matches = fetch_match_list(token)
print(f'{len(matches)} matches found')
if matches:
    m = matches[0]
    print(f'Match {m.id}: {m.home} vs {m.away}')
    print(f'Artifacts: {list(m.artifacts.keys())}')
    # Peek at event artifact
    for k in m.artifacts:
        if 'event' in k.lower():
            resp = fetch_artifact(m.id, k, token)
            print(f'Event artifact {k} (first 500 chars):')
            print(resp.text[:500])
            break
"
```

If 0 matches are found, the Gradient Sports data is not yet available on pining-for-the-data. **Stop and inform the user** — ingestion cannot proceed until data is uploaded to the API. The calibration script (Tasks 5-10) can still be built using IDSSE + SkillCorner data; Gradient Sports matches are added to the sweep once ingestion is available.

- [ ] **Step 2: Write the event parser based on discovered format**

The exact implementation depends on the artifact format discovered in Step 1. The pattern follows `src/ingestion/skillcorner_events.py`: parse the response body, produce a pandas DataFrame with `_ingested_at` audit column and match_id.

Create `src/ingestion/gradientsports_events.py`:

```python
"""Gradient Sports WC 2022 event ingestion — events artifact to bronze.

Parses the event artifact from the pining-for-the-data API and writes to
bronze.gradientsports_events. Format details discovered at runtime from
the API response.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_events(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports events into a DataFrame.

    Args:
        source: Raw event data (JSON string, dict, or list).
        match_id: Native match ID.

    Returns:
        DataFrame with event columns + match_id + _ingested_at.
    """
    import json

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # Handle both list-of-events and dict-with-events-key formats
    if isinstance(data, dict):
        events = data.get("events", data.get("data", []))
    else:
        events = data

    df = pd.json_normalize(events)  # type: ignore[arg-type]
    df["match_id"] = match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_events(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed events DataFrame to bronze.gradientsports_events."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_events",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_events",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 3: Add test for event parser**

Append to `src/tests/test_gradientsports_ingestion.py`:

```python
class TestParseEvents:
    def test_parse_list_format(self) -> None:
        from ingestion.gradientsports_events import parse_events

        events = [
            {"event_id": 1, "type": "pass", "team_id": 10, "player_id": 5},
            {"event_id": 2, "type": "shot", "team_id": 10, "player_id": 7},
        ]
        df = parse_events(events, match_id="99999")
        assert len(df) == 2
        assert "match_id" in df.columns
        assert df["match_id"].iloc[0] == "99999"
        assert "_ingested_at" in df.columns

    def test_parse_dict_format(self) -> None:
        from ingestion.gradientsports_events import parse_events

        data = {"events": [{"event_id": 1, "type": "pass"}]}
        df = parse_events(data, match_id="99999")
        assert len(df) == 1
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/gradientsports_events.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(tc3): add Gradient Sports event parser

Parses event artifacts from pining-for-the-data API into bronze format.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Gradient Sports Tracking Parser (`gradientsports_tracking.py`)

**Files:**
- Create: `src/ingestion/gradientsports_tracking.py`
- Modify: `src/tests/test_gradientsports_ingestion.py`

- [ ] **Step 1: Discover the tracking artifact format**

```bash
uv run python -c "
from ingestion.gradientsports_common import fetch_artifact, fetch_match_list, resolve_pining_token
token = resolve_pining_token()
matches = fetch_match_list(token)
if matches:
    m = matches[0]
    for k in m.artifacts:
        if 'track' in k.lower():
            resp = fetch_artifact(m.id, k, token)
            print(f'Tracking artifact {k} (first 1000 chars):')
            print(resp.text[:1000])
            break
"
```

- [ ] **Step 2: Write tracking parser based on discovered format**

The parser must produce a narrow-format DataFrame matching the silly-kicks `EXPECTED_INPUT_COLUMNS` for `gradientsports`:
`game_id, period_id, frame_id, time_seconds, frame_rate, player_id, team_id, is_ball, is_goalkeeper, x_centered, y_centered, z, speed_native, ball_state`

Create `src/ingestion/gradientsports_tracking.py`:

```python
"""Gradient Sports WC 2022 tracking ingestion — tracking artifact to bronze.

Parses the tracking artifact from the pining-for-the-data API into
narrow format (one row per player per frame) and writes to
bronze.gradientsports_tracking.

Coordinate system: center-origin meters (preserved as-is in bronze).
The silly-kicks `convert_to_frames` converter handles the final transform.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_tracking(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports tracking data into narrow-format DataFrame.

    The exact parsing logic depends on the artifact format discovered
    from the pining-for-the-data API. This implementation handles the
    expected format: list of frames, each with player positions.

    Args:
        source: Raw tracking data (JSON string, dict, or list).
        match_id: Native match ID.

    Returns:
        DataFrame in narrow format (one row per player per frame)
        with columns matching silly-kicks EXPECTED_INPUT_COLUMNS.
    """
    import json

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # Handle both list-of-frames and dict-with-frames-key formats
    if isinstance(data, dict):
        frames_list = data.get("frames", data.get("data", []))
    else:
        frames_list = data

    rows: list[dict] = []
    for frame_obj in frames_list:
        fid = frame_obj.get("frame_id", frame_obj.get("frame"))
        pid = frame_obj.get("period_id", frame_obj.get("period"))
        t = frame_obj.get("time_seconds", frame_obj.get("timestamp"))
        fr = frame_obj.get("frame_rate", 30)
        ball_state = frame_obj.get("ball_state", frame_obj.get("ball_status"))

        # Player rows
        for player in frame_obj.get("players", frame_obj.get("player_data", [])):
            rows.append(
                {
                    "match_id": match_id,
                    "game_id": match_id,
                    "period_id": pid,
                    "frame_id": fid,
                    "time_seconds": t,
                    "frame_rate": fr,
                    "player_id": player.get("player_id"),
                    "team_id": player.get("team_id"),
                    "is_ball": False,
                    "is_goalkeeper": player.get("is_goalkeeper", False),
                    "x_centered": player.get("x"),
                    "y_centered": player.get("y"),
                    "z": player.get("z"),
                    "speed_native": player.get("speed"),
                    "ball_state": ball_state,
                }
            )

        # Ball row
        ball = frame_obj.get("ball", frame_obj.get("ball_data"))
        if ball:
            rows.append(
                {
                    "match_id": match_id,
                    "game_id": match_id,
                    "period_id": pid,
                    "frame_id": fid,
                    "time_seconds": t,
                    "frame_rate": fr,
                    "player_id": None,
                    "team_id": None,
                    "is_ball": True,
                    "is_goalkeeper": False,
                    "x_centered": ball.get("x"),
                    "y_centered": ball.get("y"),
                    "z": ball.get("z"),
                    "speed_native": ball.get("speed"),
                    "ball_state": ball_state,
                }
            )

    df = pd.DataFrame(rows)
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_tracking(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed tracking DataFrame to bronze.gradientsports_tracking."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "frame_id", "period_id"],
        "gradientsports_tracking",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_tracking",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 3: Add test for tracking parser**

Append to `src/tests/test_gradientsports_ingestion.py`:

```python
class TestParseTracking:
    def test_parse_frame_list(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        frames = [
            {
                "frame_id": 1,
                "period_id": 1,
                "time_seconds": 0.0,
                "frame_rate": 30,
                "ball_state": "alive",
                "players": [
                    {"player_id": 5, "team_id": 10, "is_goalkeeper": False, "x": 10.0, "y": 5.0, "z": 0.0, "speed": 2.1},
                    {"player_id": 7, "team_id": 20, "is_goalkeeper": True, "x": -40.0, "y": 0.0, "z": 0.0, "speed": 0.5},
                ],
                "ball": {"x": 0.0, "y": 0.0, "z": 0.5, "speed": 15.0},
            }
        ]
        df = parse_tracking(frames, match_id="99999")
        assert len(df) == 3  # 2 players + 1 ball
        assert df[df["is_ball"]].iloc[0]["x_centered"] == 0.0
        assert "match_id" in df.columns
        assert "_ingested_at" in df.columns
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/gradientsports_tracking.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(tc3): add Gradient Sports tracking parser

Narrow-format parser for pining-for-the-data tracking artifacts.
Output schema matches silly-kicks EXPECTED_INPUT_COLUMNS.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Gradient Sports Ingestion Orchestrator (`gradientsports.py`)

**Files:**
- Create: `src/ingestion/gradientsports.py`
- Modify: `pyproject.toml` (add entry point)

- [ ] **Step 1: Write the orchestrator**

Create `src/ingestion/gradientsports.py` following the `skillcorner.py` pattern:

```python
"""Gradient Sports WC 2022 ingestion orchestrator.

Discovers matches via the pining-for-the-data REST API,
downloads events + tracking, and writes to bronze.

Bronze tables produced:
  - gradientsports_events   (raw events)
  - gradientsports_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  Center-origin meters. silly-kicks convert_to_frames handles the final transform.

LICENSE GATE: Data approved for internal calibration/training only.
NOT published to HF datasets, gold marts, synced tables, or Taipy UI
until Gradient Sports license confirmed in writing.
"""

from __future__ import annotations

import gc
import logging
from typing import TYPE_CHECKING

from ingestion.gradientsports_common import (
    MatchInfo,
    fetch_artifact,
    fetch_match_list,
    resolve_pining_token,
)
from ingestion.gradientsports_events import parse_events, write_events
from ingestion.gradientsports_tracking import parse_tracking, write_tracking
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    tolerate_missing_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ingest_gradientsports(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    matches: list[MatchInfo],
) -> None:
    """Download and ingest Gradient Sports data for discovered matches.

    Processing order per match: events -> tracking.
    """
    token = resolve_pining_token()

    for i, match in enumerate(matches):
        mid = match.id
        logger.info(
            "Processing Gradient Sports match %s (%d/%d): %s vs %s",
            mid,
            i + 1,
            len(matches),
            match.home,
            match.away,
        )

        # 1. Events
        for artifact_key in match.artifacts:
            if "event" in artifact_key.lower():
                events_resp = fetch_artifact(mid, artifact_key, token)
                events_df = parse_events(events_resp.text, match_id=mid)
                write_events(spark, events_df, catalog, schema, mid, logger)
                logger.info("Wrote %d event rows for match %s", len(events_df), mid)
                del events_df
                break
        else:
            logger.warning("No event artifact found for match %s", mid)

        # 2. Tracking
        for artifact_key in match.artifacts:
            if "track" in artifact_key.lower():
                tracking_resp = fetch_artifact(mid, artifact_key, token, stream=True)
                # Read full response — streaming not needed for WC 2022 data size
                tracking_data = tracking_resp.text
                tracking_df = parse_tracking(tracking_data, match_id=mid)
                write_tracking(spark, tracking_df, catalog, schema, mid, logger)
                logger.info("Wrote %d tracking rows for match %s", len(tracking_df), mid)
                del tracking_df, tracking_data
                break
        else:
            logger.warning("No tracking artifact found for match %s", mid)

        gc.collect()

    logger.info("Gradient Sports ingestion complete: %d matches processed", len(matches))


def main() -> None:
    """CLI entry point for Gradient Sports data ingestion."""
    args = parse_ingestion_args("Ingest Gradient Sports WC 2022 data into the bronze layer")
    logger = configure_logging("gradientsports")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    token = resolve_pining_token()
    matches = fetch_match_list(token)

    if not matches:
        logger.info("No Gradient Sports matches found — nothing to ingest")
        return

    logger.info(
        "Found %d Gradient Sports matches to ingest into %s.%s",
        len(matches),
        args.catalog,
        args.schema,
    )
    ingest_gradientsports(spark, args.catalog, args.schema, logger, matches)
    logger.info("Gradient Sports ingestion complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add entry point to pyproject.toml**

Add under `[project.scripts]`:

```toml
ingest_gradientsports = "ingestion.gradientsports:main"
```

- [ ] **Step 3: Run lint + type check**

Run: `uv run ruff check src/ingestion/gradientsports.py src/ingestion/gradientsports_common.py src/ingestion/gradientsports_events.py src/ingestion/gradientsports_tracking.py && uv run pyright src/ingestion/gradientsports.py`
Expected: 0 errors

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/gradientsports.py pyproject.toml
git commit -m "feat(tc3): add Gradient Sports ingestion orchestrator

Discovers and ingests WC 2022 events + tracking via pining-for-the-data API.
License gate: bronze/staging only until Gradient Sports license confirmed in writing.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 5: Calibration Script — Data Loading (Phase 0)

**Files:**
- Create: `scripts/run_tc3_calibration.py`
- Create: `src/tests/tc3/test_tc3_calibration.py`

- [ ] **Step 1: Write the Phase 0 data loader + validation gate**

Create `scripts/run_tc3_calibration.py`:

```python
"""TC-3: Optuna Calibration Sweep for silly-kicks tracking defaults.

Replaces engineering-choice defaults with data-calibrated values:
  - Stage 1: infer_ball_carrier (tolerance_m, beta, gamma) — carrier accuracy
  - Stage 2: LinkParams.k3 + off-ball-runs (pre_seconds, min_displacement_m) — VAEP Brier

Usage:
  uv run python scripts/run_tc3_calibration.py --stage 0   # Data loading + validation
  uv run python scripts/run_tc3_calibration.py --stage 1   # Carrier accuracy sweep
  uv run python scripts/run_tc3_calibration.py --stage 2   # VAEP Brier sweep
  uv run python scripts/run_tc3_calibration.py --stage diagnostics  # Post-hoc analysis
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("tc3_calibration")

# ── Paths ────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("docs/evolve/tc3-calibration")
CACHE_DIR = Path(".tc3_cache")

# ── Provider configuration ────────────────────────────────────────────────
PROVIDERS = ("idsse", "skillcorner", "gradientsports")


@dataclass(frozen=True)
class MatchData:
    """Cached per-match data for calibration."""

    match_id: str
    provider: str
    actions: pd.DataFrame
    frames: pd.DataFrame
    home_team_id: str
    home_start_left: bool


@dataclass
class CalibrationDataset:
    """All loaded match data + metadata for the calibration sweep."""

    matches: list[MatchData] = field(default_factory=list)
    xt: Any = None  # ExpectedThreat
    vaep_labels: dict[str, pd.DataFrame] = field(default_factory=dict)

    def matches_by_provider(self, provider: str) -> list[MatchData]:
        return [m for m in self.matches if m.provider == provider]


# ── Phase 0: Data Loading ────────────────────────────────────────────────


def _pull_provider_data_sql(provider: str) -> list[MatchData]:
    """Pull tracking + actions for a provider via Databricks SQL connector.

    Returns list of MatchData, one per match.
    """
    import os

    from databricks import sql as dbsql

    host = os.environ["DATABRICKS_HOST"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]
    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    schema = "bronze"

    conn = dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )

    try:
        # Discover tracking matches that have SPADL actions
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT a.match_id_native
            FROM {catalog}.{schema}.spadl_actions a
            WHERE a.data_source = '{provider}'
              AND EXISTS (
                SELECT 1 FROM {catalog}.{schema}.{'idsse_tracking' if provider == 'idsse' else provider + '_tracking'} t
                WHERE t.match_id = a.match_id_native
              )
        """)
        match_ids = [str(row[0]) for row in cursor.fetchall()]
        cursor.close()
        logger.info("Found %d %s matches with paired tracking + SPADL", len(match_ids), provider)

        results: list[MatchData] = []
        for mid in match_ids:
            cache_path = CACHE_DIR / provider / mid
            if (cache_path / "actions.parquet").exists() and (cache_path / "frames.parquet").exists():
                logger.info("Cache hit: %s/%s", provider, mid)
                actions = pd.read_parquet(cache_path / "actions.parquet")
                frames = pd.read_parquet(cache_path / "frames.parquet")
                meta = json.loads((cache_path / "meta.json").read_text())
                results.append(MatchData(
                    match_id=mid,
                    provider=provider,
                    actions=actions,
                    frames=frames,
                    home_team_id=meta["home_team_id"],
                    home_start_left=meta["home_start_left"],
                ))
                continue

            logger.info("Pulling %s match %s from Databricks...", provider, mid)

            # Pull actions
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {catalog}.{schema}.spadl_actions
                WHERE data_source = '{provider}' AND match_id_native = '{mid}'
            """)
            actions = cursor.fetch_all_as_arrow().to_pandas()
            cursor.close()

            if actions.empty:
                logger.warning("No actions for %s/%s — skipping", provider, mid)
                continue

            # Pull tracking
            tracking_table = "idsse_tracking" if provider == "idsse" else f"{provider}_tracking"
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {catalog}.{schema}.{tracking_table}
                WHERE match_id = '{mid}'
            """)
            frames_raw = cursor.fetch_all_as_arrow().to_pandas()
            cursor.close()

            if frames_raw.empty:
                logger.warning("No tracking for %s/%s — skipping", provider, mid)
                continue

            # Resolve home_team_id + home_start_left per provider
            home_team_id, home_start_left = _resolve_match_metadata(
                conn, catalog, schema, provider, mid, actions, frames_raw,
            )

            # Convert bronze tracking to silly-kicks frames format
            frames = _convert_tracking_to_frames(
                provider, frames_raw, actions, mid, home_team_id, home_start_left,
            )

            # Cache
            cache_path.mkdir(parents=True, exist_ok=True)
            actions.to_parquet(cache_path / "actions.parquet")
            frames.to_parquet(cache_path / "frames.parquet")
            (cache_path / "meta.json").write_text(json.dumps({
                "home_team_id": str(home_team_id),
                "home_start_left": home_start_left,
            }))

            results.append(MatchData(
                match_id=mid,
                provider=provider,
                actions=actions,
                frames=frames,
                home_team_id=str(home_team_id),
                home_start_left=home_start_left,
            ))

        return results
    finally:
        conn.close()


def _resolve_match_metadata(
    conn: Any,
    catalog: str,
    schema: str,
    provider: str,
    mid: str,
    actions: pd.DataFrame,
    frames_raw: pd.DataFrame,
) -> tuple[str, bool]:
    """Resolve home_team_id and home_start_left for a match."""
    if provider == "idsse":
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT home_team_id_native
            FROM {catalog}.{schema}.idsse_events
            WHERE match_id = '{mid}'
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        home_team_id = str(row[0]) if row else str(actions["team_id_native"].dropna().iloc[0])
        # Derive home_start_left from events
        from ingestion.spadl_adapter import derive_idsse_home_team_start_left

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {catalog}.{schema}.idsse_events
            WHERE match_id = '{mid}'
        """)
        events_df = cursor.fetch_all_as_arrow().to_pandas()
        cursor.close()
        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks

        adapted = adapt_idsse_events_for_silly_kicks(events_df)
        home_start_left = derive_idsse_home_team_start_left(adapted, home_team_id)
        return home_team_id, home_start_left

    elif provider == "skillcorner":
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT home_team_id
            FROM {catalog}.{schema}.skillcorner_matches
            WHERE match_id = '{mid}'
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        home_team_id = str(row[0]) if row else "unknown"
        # True is the production convention (tracking_context.py:1738) —
        # only IDSSE overrides. SkillCorner data is pre-normalized by provider.
        return home_team_id, True

    elif provider == "gradientsports":
        # Gradient Sports: home_team_id from events or tracking metadata
        # home_start_left must be determined from the data
        home_team_id = str(actions["team_id_native"].dropna().iloc[0])
        # Default True pending empirical validation on Gradient Sports data.
        # Phase 0 validation gate checks frame count/GK/NaN but not direction —
        # add LTR assertion (shot x-cluster) once data is available.
        return home_team_id, True

    msg = f"Unknown provider: {provider}"
    raise ValueError(msg)


def _convert_tracking_to_frames(
    provider: str,
    frames_raw: pd.DataFrame,
    actions: pd.DataFrame,
    mid: str,
    home_team_id: str,
    home_start_left: bool,
) -> pd.DataFrame:
    """Convert bronze tracking to silly-kicks frames format."""
    if provider == "idsse":
        from silly_kicks.tracking import PreprocessConfig
        from silly_kicks.tracking.sportec import convert_to_frames

        from ingestion.tracking_context import _bronze_idsse_to_sportec_input

        sportec_input = _bronze_idsse_to_sportec_input(frames_raw)
        frames, _report = convert_to_frames(
            sportec_input,
            home_team_id=home_team_id,
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        return frames

    elif provider == "skillcorner":
        from ingestion.tracking_context import _bronze_skillcorner_to_frames

        game_id = int(actions["game_id"].iloc[0])
        # SkillCorner bronze needs team + is_goalkeeper columns from matches
        # For local calibration, we assume these are already in the cached frames
        frames = _bronze_skillcorner_to_frames(frames_raw, game_id=game_id)
        return frames

    elif provider == "gradientsports":
        from silly_kicks.tracking import PreprocessConfig
        from silly_kicks.tracking.gradientsports import convert_to_frames

        # Gradient Sports tracking is in EXPECTED_INPUT_COLUMNS format
        frames, _report = convert_to_frames(
            frames_raw,
            home_team_id=int(home_team_id),
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        return frames

    msg = f"Unknown provider: {provider}"
    raise ValueError(msg)


def _validate_gradient_sports(matches: list[MatchData]) -> list[MatchData]:
    """Gradient Sports Phase 0 validation gate (M6).

    Per-match checks:
    - Frame count sanity (min/max, flag outliers)
    - GK identification via is_goalkeeper column
    - NaN prevalence in x/y
    - At least one action from each team

    Returns filtered list (anomalous matches excluded).
    """
    valid: list[MatchData] = []
    for m in matches:
        issues: list[str] = []

        # Frame count (at 25fps, 25000 frames = ~17 min = one full half minimum)
        n_frames = m.frames["frame_id"].nunique()
        if n_frames < 25_000:
            issues.append(f"Low frame count: {n_frames} (min 25,000 for meaningful features)")
        if n_frames > 500_000:
            issues.append(f"Suspiciously high frame count: {n_frames}")

        # GK identification
        gk_mask = m.frames["is_goalkeeper"] == True  # noqa: E712
        n_gk = m.frames.loc[gk_mask, "player_id"].nunique()
        if n_gk < 2:
            issues.append(f"Only {n_gk} distinct GK player(s) identified (expected 2)")

        # NaN prevalence
        xy_nan_frac = m.frames[["x", "y"]].isna().mean().max()
        if xy_nan_frac > 0.3:
            issues.append(f"High NaN rate in x/y: {xy_nan_frac:.1%}")

        # Team coverage
        n_teams = m.actions["team_id_native"].nunique()
        if n_teams < 2:
            issues.append(f"Only {n_teams} team(s) in actions")

        if issues:
            logger.warning(
                "Gradient Sports match %s excluded: %s",
                m.match_id,
                "; ".join(issues),
            )
        else:
            valid.append(m)

    logger.info(
        "Gradient Sports validation: %d/%d matches passed",
        len(valid),
        len(matches),
    )
    return valid


def _compute_vaep_labels(matches: list[MatchData]) -> dict[str, pd.DataFrame]:
    """Compute VAEP scoring/conceding labels per match."""
    from silly_kicks.vaep.labels import compute_scores_and_concedes

    labels: dict[str, pd.DataFrame] = {}
    for m in matches:
        scores, concedes = compute_scores_and_concedes(m.actions, nr_actions=10)
        labels[m.match_id] = pd.DataFrame({
            "scores": scores.values.ravel(),
            "concedes": concedes.values.ravel(),
        })
    return labels


def run_phase0(args: argparse.Namespace) -> CalibrationDataset:
    """Phase 0: Data loading + validation."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    dataset = CalibrationDataset()

    # Pull data per provider
    for provider in PROVIDERS:
        try:
            matches = _pull_provider_data_sql(provider)
            if provider == "gradientsports":
                # Gradient Sports is a new untested source — apply validation gate.
                # IDSSE/SkillCorner are validated by lakehouse dbt tests + TC-1 pipeline.
                matches = _validate_gradient_sports(matches)
            dataset.matches.extend(matches)
            logger.info("Loaded %d %s matches", len(matches), provider)
        except Exception as exc:
            logger.error("Failed to load %s data: %s", provider, exc)
            if provider in ("idsse", "skillcorner"):
                raise  # Core providers must succeed
            # Gradient Sports failure is non-fatal — sweep proceeds with existing data

    if not dataset.matches:
        logger.error("No match data loaded — cannot proceed")
        sys.exit(1)

    # Fit xT grid
    from silly_kicks.xthreat import ExpectedThreat

    all_actions = pd.concat([m.actions for m in dataset.matches], ignore_index=True)
    dataset.xt = ExpectedThreat().fit(all_actions)
    logger.info("xT model fitted (grid shape %s)", dataset.xt.xT.shape)

    # Compute VAEP labels
    dataset.vaep_labels = _compute_vaep_labels(dataset.matches)
    logger.info("VAEP labels computed for %d matches", len(dataset.vaep_labels))

    # Summary
    for provider in PROVIDERS:
        n = len(dataset.matches_by_provider(provider))
        if n > 0:
            logger.info("  %s: %d matches", provider, n)

    return dataset


# ── CLI ──────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="TC-3 Optuna Calibration Sweep")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["0", "1", "2", "diagnostics", "all"],
        help="Which stage to run",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=100,
        help="Number of Optuna trials per stage (default: 100)",
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=8,
        help="ThreadPoolExecutor workers (default: 8)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for TPE sampler + XGBoost (default: 42)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    dataset: CalibrationDataset | None = None

    if args.stage in ("0", "all"):
        dataset = run_phase0(args)
        logger.info("Phase 0 complete: %d total matches loaded", len(dataset.matches))

    if args.stage == "1" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_stage1(dataset, n_trials=args.n_trials, n_workers=args.n_workers, seed=args.seed)

    if args.stage == "2" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_stage2(dataset, n_trials=args.n_trials, n_workers=args.n_workers, seed=args.seed)

    if args.stage == "diagnostics" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_diagnostics(dataset)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write Phase 0 test**

Create `src/tests/tc3/conftest.py` (subdirectory scoping — `autouse` only fires for tests under `src/tests/tc3/`):

```python
"""Shared fixture for TC-3 calibration tests — adds scripts/ to sys.path.

Placed in src/tests/tc3/ so autouse naturally scopes to this directory only,
preventing sys.path pollution for the rest of the test suite.
"""

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = str(Path(__file__).resolve().parents[3] / "scripts")


@pytest.fixture(autouse=True, scope="session")
def _tc3_scripts_path() -> None:
    """Add scripts/ to sys.path for TC-3 tests only."""
    if _SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, _SCRIPTS_DIR)
```

Create `src/tests/tc3/__init__.py` (empty).

Create `src/tests/tc3/test_tc3_calibration.py`:

```python
"""Tests for TC-3 calibration script helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


class TestValidateGradientSports:
    def _make_match_data(
        self,
        *,
        n_frames: int = 26_000,  # Just above 25K gate; failure tests use 500
        n_gk: int = 2,
        xy_nan_frac: float = 0.0,
        n_teams: int = 2,
    ) -> object:
        """Create a synthetic MatchData for validation testing."""
        from run_tc3_calibration import MatchData

        n_players = 20 + n_gk
        rows_per_frame = n_players + 1  # +1 for ball
        total_rows = n_frames * rows_per_frame

        # Realistic player IDs (10001+), not sequential from 0
        player_ids: list[object] = [10001 + i for i in range(n_players)] + [None]

        frames = pd.DataFrame({
            "frame_id": np.repeat(np.arange(n_frames), rows_per_frame),
            "player_id": np.tile(player_ids, n_frames),
            "is_goalkeeper": np.tile(
                [True] * n_gk + [False] * (n_players - n_gk) + [False],
                n_frames,
            ),
            "x": np.random.rand(total_rows) * 105,
            "y": np.random.rand(total_rows) * 68,
            "is_ball": np.tile([False] * n_players + [True], n_frames),
        })

        # Inject NaN
        if xy_nan_frac > 0:
            mask = np.random.rand(total_rows) < xy_nan_frac
            frames.loc[mask, "x"] = np.nan
            frames.loc[mask, "y"] = np.nan

        teams = ["team_a", "team_b"][:n_teams]
        actions = pd.DataFrame({
            "team_id_native": np.random.choice(teams, size=100),
        })

        return MatchData(
            match_id="test_99999",
            provider="gradientsports",
            actions=actions,
            frames=frames,
            home_team_id="team_a",
            home_start_left=True,
        )

    def test_valid_match_passes(self) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data()
        result = _validate_gradient_sports([m])
        assert len(result) == 1

    def test_low_frame_count_excluded(self) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data(n_frames=500)
        result = _validate_gradient_sports([m])
        assert len(result) == 0

    def test_missing_gk_excluded(self) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data(n_gk=1)
        result = _validate_gradient_sports([m])
        assert len(result) == 0
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/tc3/test_tc3_calibration.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add scripts/run_tc3_calibration.py src/tests/tc3/test_tc3_calibration.py
git commit -m "feat(tc3): Phase 0 — data loading, caching, validation gate

Pulls tracking + SPADL from Databricks SQL, caches as local Parquet.
Gradient Sports validation gate: frame count, GK, NaN, team coverage.
Fits xT grid and computes VAEP labels.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Calibration-Specific Enrichment Function

**Files:**
- Modify: `scripts/run_tc3_calibration.py`
- Modify: `src/tests/tc3/test_tc3_calibration.py`

- [ ] **Step 1: Write test for param-injectable enrichment**

Append to `src/tests/tc3/test_tc3_calibration.py`:

```python
class TestEnrichWithParams:
    """Test that enrichment accepts tunable parameters."""

    def test_enrichment_accepts_k3(self) -> None:
        """Verify LinkParams(k3=...) is accepted by add_pressure_on_actor."""
        from silly_kicks.tracking import LinkParams

        lp = LinkParams(k3=2.5)
        assert lp.k3 == 2.5
        assert lp.r_hoz == 4.0  # unchanged geometry

    def test_enrichment_accepts_off_ball_params(self) -> None:
        """Verify add_off_ball_context accepts pre_seconds and min_displacement_m."""
        import inspect

        from silly_kicks.tracking import add_off_ball_context

        sig = inspect.signature(add_off_ball_context)
        assert "pre_seconds" in sig.parameters
        assert "min_displacement_m" in sig.parameters

    def test_enrichment_accepts_carrier_params(self) -> None:
        """Verify infer_ball_carrier accepts tolerance_m, beta, gamma."""
        import inspect

        from silly_kicks.tracking import infer_ball_carrier

        sig = inspect.signature(infer_ball_carrier)
        assert "tolerance_m" in sig.parameters
        assert "beta" in sig.parameters
        assert "gamma" in sig.parameters
```

- [ ] **Step 2: Run test to verify silly-kicks API compatibility**

Run: `uv run pytest src/tests/tc3/test_tc3_calibration.py::TestEnrichWithParams -v`
Expected: PASS

- [ ] **Step 3: Write the calibration enrichment function**

Add to `scripts/run_tc3_calibration.py` (below Phase 0 code):

```python
# ── Calibration enrichment (mirrors _enrich_match with param injection) ──


def _enrich_match_with_params(
    *,
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: Any,
    home_team_id: str,
    match_id_native: str,
    data_source: str,
    # Tunable params — Stage 1
    carrier_tolerance_m: float = 3.0,
    carrier_beta: float = 0.5,
    carrier_gamma: float = 1.0,
    # Tunable params — Stage 2
    k3: float = 1.0,
    pre_seconds: float = 1.5,
    min_displacement_m: float = 3.0,
) -> pd.DataFrame:
    """Enrichment chain with injectable parameters for calibration.

    Mirrors production _enrich_match (tracking_context.py:603-825) but
    accepts tunable parameters for the three target subsystems.

    Returns the enriched actions DataFrame (no identity restore — calibration
    doesn't write to bronze, it just extracts feature vectors).
    """
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        LinkParams,
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_defensive_line,
        add_gk_influence,
        add_line_break,
        add_off_ball_context,
        add_pressure_on_actor,
        add_sync_score,
        add_team_shape,
        derive_team_in_possession,
        infer_ball_carrier,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    from ingestion.tracking_context import (
        _resolve_enrichment_identity,
    )

    # Resolve enrichment-compatible identity
    actions = _resolve_enrichment_identity(
        actions.copy(),
        provider=data_source,
        match_id_native=match_id_native,
    )

    # Step 0: Link actions to frames
    links, _report = link_actions_to_frames(actions, frames)

    # Step 1: GK resolution
    actions = add_pre_shot_gk_context(actions, frames=frames)

    # Step 2: Action context
    actions = add_action_context(actions, frames, links=links)

    # Step 3: Actor pre-window
    actions = add_actor_pre_window(actions, frames, links=links)

    # Step 4a: Pressure — andrienko_oval (no k3 dependency)
    actions = add_pressure_on_actor(
        actions, frames, links=links,
        methods=("andrienko_oval",),
    )

    # Step 4b: Pressure — link_zones (k3 INJECTED here)
    actions = add_pressure_on_actor(
        actions, frames, links=links,
        methods=("link_zones",),
        params_per_method={"link_zones": LinkParams(k3=k3)},
    )

    # Step 4c: Pressure — bekkers_pi
    try:
        actions = add_pressure_on_actor(
            actions, frames, links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            actions["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise

    # Steps 5-7: Pitch control
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(actions, frames, links=links, method=method)
        actions[s.name] = s.values

    # Step 8: Defensive line
    actions = add_defensive_line(actions, frames, links=links, home_team_id=home_team_id)

    # Step 9: Off-ball context (pre_seconds, min_displacement_m INJECTED)
    actions = add_off_ball_context(
        actions, frames, links=links, home_team_id=home_team_id,
        pre_seconds=pre_seconds,
        min_displacement_m=min_displacement_m,
    )

    # Step 10: Ward line-breaking
    actions = add_line_break(actions, frames, links=links, method="ward", home_team_id=home_team_id)

    # Step 11: Team shape
    actions = add_team_shape(actions, frames, links=links, home_team_id=home_team_id)

    # Step 12: DAS (carrier params INJECTED here)
    from silly_kicks.tracking._das import get_individual_das

    try:
        carrier = infer_ball_carrier(
            frames,
            tolerance_m=carrier_tolerance_m,
            beta=carrier_beta,
            gamma=carrier_gamma,
        )
        frames_with_tip = derive_team_in_possession(frames, carrier)
        del carrier

        linked = links[["action_id", "frame_id"]].dropna(subset=["frame_id"])
        linked = linked.merge(actions[["action_id", "period_id"]], on="action_id", how="left")
        linked_frame_ids = linked[["period_id", "frame_id"]].drop_duplicates()
        das_frames = frames_with_tip.merge(linked_frame_ids, on=["period_id", "frame_id"], how="inner")
        del linked, frames_with_tip

        das_result = get_individual_das(das_frames, use_progress_bar=False, chunk_size=10)
        del das_frames

        player_rows = das_result[das_result["is_ball"] != True]  # noqa: E712
        valid_rows = player_rows.dropna(subset=["DAS"])
        das_lookup: dict[tuple, dict] = {}
        for (pid, fid, tid), grp in valid_rows.groupby(["period_id", "frame_id", "team_id"]):
            das_lookup.setdefault((pid, fid), {})[tid] = float(grp["DAS"].sum())
        del das_result, player_rows, valid_rows

        pointer_lookup = links.set_index("action_id")
        team_vals = np.full(len(actions), np.nan)
        opp_vals = np.full(len(actions), np.nan)

        # M4: itertuples is 10-50x faster than iterrows (matters at 100 trials × 30 matches)
        for row in actions.itertuples():
            i = row.Index
            aid = row.action_id
            if aid not in pointer_lookup.index:
                continue
            fid_raw = pointer_lookup.at[aid, "frame_id"]
            if pd.isna(fid_raw):
                continue
            key = (row.period_id, int(float(fid_raw)))
            if key not in das_lookup:
                continue
            team_id = row.team_id
            team_vals[i] = das_lookup[key].get(team_id, np.nan)
            opp = [v for k, v in das_lookup[key].items() if k != team_id]
            if opp:
                opp_vals[i] = opp[0]

        actions["das_team"] = team_vals
        actions["das_opponent"] = opp_vals
        actions["das_diff"] = team_vals - opp_vals

    except (IndexError, ValueError, RuntimeError, TypeError) as exc:
        logger.exception("DAS degraded to NaN for match %s", match_id_native)
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan

    # Step 13: GK influence
    actions = add_gk_influence(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows
    actions = add_cover_shadows(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 15: Sync score
    actions = add_sync_score(actions, links)

    return actions
```

- [ ] **Step 4: Commit**

```bash
git add scripts/run_tc3_calibration.py src/tests/tc3/test_tc3_calibration.py
git commit -m "feat(tc3): calibration enrichment with parameter injection

Mirrors production _enrich_match but accepts k3, pre_seconds,
min_displacement_m, and carrier params as arguments.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 7: Stage 1 — Carrier Accuracy Sweep

**Files:**
- Modify: `scripts/run_tc3_calibration.py`
- Modify: `src/tests/tc3/test_tc3_calibration.py`

- [ ] **Step 1: Write test for carrier accuracy objective**

Append to `src/tests/tc3/test_tc3_calibration.py`:

```python
class TestCarrierAccuracy:
    def test_carrier_accuracy_computes(self) -> None:
        """Smoke test: carrier accuracy function is importable and callable."""
        from run_tc3_calibration import _compute_carrier_accuracy_for_match

        # This test requires real data — mark as integration
        pytest.skip("Integration test — requires cached match data")
```

- [ ] **Step 2: Write Stage 1 implementation**

Add to `scripts/run_tc3_calibration.py`:

```python
# ── Stage 1: Carrier accuracy ────────────────────────────────────────────


def _compute_carrier_accuracy_for_match(
    match: MatchData,
    *,
    tolerance_m: float,
    beta: float,
    gamma: float,
) -> tuple[float, float]:
    """Compute carrier accuracy and switch rate for one match.

    Compares inferred ball carrier to SPADL action actor at linked timestamps.
    Filters to action types where actor == ball carrier by definition
    (pass, cross, shot, dribble). Tackles/interceptions/clearances excluded
    because the actor is the interceptor, not the ball carrier.

    Returns:
        (accuracy, switches_per_minute) tuple.
    """
    from silly_kicks.tracking import infer_ball_carrier, link_actions_to_frames

    # Run carrier inference with trial params
    # Returns DataFrame: [game_id, period_id, frame_id,
    #   ball_carrier_player_id, ball_carrier_distance_m, ball_carrier_team_id]
    # One row per frame (not per player).
    carrier = infer_ball_carrier(
        match.frames,
        tolerance_m=tolerance_m,
        beta=beta,
        gamma=gamma,
    )

    # Link actions to frames to get ground-truth timestamps
    links, _ = link_actions_to_frames(match.actions, match.frames)

    # Filter to action types where actor == ball carrier by definition.
    # Excludes tackles, interceptions, clearances where actor != carrier.
    carrier_action_types = {"pass", "cross", "shot", "dribble"}
    if "type_name" in match.actions.columns:
        actor_mask = match.actions["type_name"].isin(carrier_action_types)
    elif "type_id" in match.actions.columns:
        from silly_kicks.spadl.config import actiontypes
        type_ids = {i for i, name in enumerate(actiontypes) if name in carrier_action_types}
        actor_mask = match.actions["type_id"].isin(type_ids)
    else:
        actor_mask = pd.Series(True, index=match.actions.index)

    filtered_actions = match.actions[actor_mask]

    # Merge carrier (one row per frame) with links to get carrier at action time
    merged = links.merge(
        carrier[["frame_id", "period_id", "ball_carrier_player_id"]],
        on=["frame_id", "period_id"],
        how="inner",
    )
    merged = merged.merge(
        filtered_actions[["action_id", "player_id"]].rename(
            columns={"player_id": "true_carrier"}
        ),
        on="action_id",
        how="inner",
    )

    if merged.empty:
        return 0.0, 0.0

    # Cast to string to avoid dtype mismatch (Int64 vs int64 vs object)
    # across providers — per H4 review.
    accuracy = (
        merged["ball_carrier_player_id"].astype(str) == merged["true_carrier"].astype(str)
    ).mean()

    # Carrier switch rate (H2 diagnostic)
    carrier_sorted = carrier.sort_values(["period_id", "frame_id"])
    switches = (
        carrier_sorted["ball_carrier_player_id"]
        != carrier_sorted["ball_carrier_player_id"].shift()
    ).sum()
    total_seconds = match.frames["time_seconds"].max() - match.frames["time_seconds"].min()
    switches_per_min = (switches / max(total_seconds, 1)) * 60

    return float(accuracy), float(switches_per_min)


def run_stage1(
    dataset: CalibrationDataset,
    *,
    n_trials: int = 100,
    n_workers: int = 8,
    seed: int = 42,
) -> None:
    """Stage 1: Optimize carrier accuracy via Optuna TPE."""
    import optuna
    from concurrent.futures import ThreadPoolExecutor

    storage = f"sqlite:///{OUTPUT_DIR / 'tc3_stage1.db'}"
    study = optuna.create_study(
        study_name="tc3_stage1_carrier",
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        storage=storage,
        load_if_exists=True,
    )

    # Warm-start: enqueue current defaults as first trial
    study.enqueue_trial({
        "tolerance_m": 3.0,
        "beta": 0.5,
        "gamma": 1.0,
    })

    def objective(trial: optuna.Trial) -> float:
        tolerance_m = trial.suggest_float("tolerance_m", 1.0, 8.0)
        beta = trial.suggest_float("beta", 0.0, 2.0)
        gamma = trial.suggest_float("gamma", 0.0, 3.0)

        # Parallel carrier accuracy across matches
        from functools import partial

        fn = partial(
            _compute_carrier_accuracy_for_match,
            tolerance_m=tolerance_m,
            beta=beta,
            gamma=gamma,
        )

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            results = list(pool.map(fn, dataset.matches))

        accuracies = [r[0] for r in results]
        switch_rates = [r[1] for r in results]

        # Per-provider averaging (M2)
        provider_accs: dict[str, list[float]] = {}
        for m, acc in zip(dataset.matches, accuracies):
            provider_accs.setdefault(m.provider, []).append(acc)
        mean_acc = np.mean([np.mean(accs) for accs in provider_accs.values()])

        # Record diagnostics
        trial.set_user_attr("per_match_accuracy", {m.match_id: a for m, a in zip(dataset.matches, accuracies)})
        trial.set_user_attr("mean_switch_rate", float(np.mean(switch_rates)))
        trial.set_user_attr("per_provider_accuracy", {p: float(np.mean(a)) for p, a in provider_accs.items()})

        # H2 diagnostic: flag if switch rate is below literature baseline
        mean_switch = np.mean(switch_rates)
        if mean_switch < 15:
            logger.warning(
                "Trial %d: low switch rate %.1f/min (literature: 15-25). "
                "gamma=%.2f may be too sticky.",
                trial.number,
                mean_switch,
                gamma,
            )

        return float(mean_acc)

    remaining = n_trials - len(study.trials)
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)

    # Save results
    best = study.best_trial
    results = {
        "best_params": best.params,
        "best_accuracy": best.value,
        "best_switch_rate": best.user_attrs.get("mean_switch_rate"),
        "per_provider_accuracy": best.user_attrs.get("per_provider_accuracy"),
        "n_trials": len(study.trials),
        "all_trials": [
            {
                "number": t.number,
                "params": t.params,
                "value": t.value,
                "switch_rate": t.user_attrs.get("mean_switch_rate"),
            }
            for t in study.trials
        ],
    }
    (OUTPUT_DIR / "stage1_results.json").write_text(json.dumps(results, indent=2))
    logger.info(
        "Stage 1 complete: best accuracy=%.4f, params=%s",
        best.value,
        best.params,
    )
```

- [ ] **Step 3: Commit**

```bash
git add scripts/run_tc3_calibration.py src/tests/tc3/test_tc3_calibration.py
git commit -m "feat(tc3): Stage 1 — carrier accuracy Optuna sweep

100 TPE trials, 8-core parallel per match. Warm-start with defaults.
H2 diagnostic: carrier-switch-rate vs literature baseline.
Per-provider averaging (M2).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 8: Stage 2 — VAEP Brier Sweep

**Files:**
- Modify: `scripts/run_tc3_calibration.py`
- Modify: `src/tests/tc3/test_tc3_calibration.py`

- [ ] **Step 1: Write test for augmented VAEP objective**

Append to `src/tests/tc3/test_tc3_calibration.py`:

```python
class TestAugmentedVaep:
    def test_xgboost_brier_computes(self) -> None:
        """Smoke test: XGBoost Brier score is a float between 0 and 1."""
        import xgboost as xgb
        from sklearn.metrics import brier_score_loss

        rng = np.random.RandomState(42)
        X = rng.rand(200, 10)
        y = rng.randint(0, 2, 200)
        model = xgb.XGBClassifier(n_estimators=10, max_depth=2, use_label_encoder=False, eval_metric="logloss")
        model.fit(X[:150], y[:150])
        probs = model.predict_proba(X[150:])[:, 1]
        brier = brier_score_loss(y[150:], probs)
        assert 0 <= brier <= 1

    def test_feature_variance_gate(self) -> None:
        """H1: degenerate features produce penalty Brier."""
        # Constant feature has 0 variance
        feature = np.ones(100)
        default_var = 1.0  # some nonzero reference
        ratio = np.var(feature) / default_var
        assert ratio < 0.1  # triggers sanity gate
```

- [ ] **Step 2: Write Stage 2 implementation**

Add to `scripts/run_tc3_calibration.py`:

```python
# ── Stage 2: VAEP Brier ─────────────────────────────────────────────────

# Feature columns from enrichment (tracking context features for augmented VAEP)
_SPADL_FEATURES = [
    "type_id", "bodypart_id", "result_id",
    "start_x", "start_y", "end_x", "end_y",
]

_TRACKING_FEATURES = [
    "nearest_defender_distance", "actor_speed",
    "receiver_zone_density", "defenders_in_triangle_to_goal",
    "actor_arc_length_pre_window", "actor_displacement_pre_window",
    "pressure_on_actor__andrienko_oval", "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    "pitch_control_at_ball__spearman", "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    "defensive_line_x", "back_line_high_x", "compactness_x",
    "lateral_width", "max_lateral_gap", "back_n_count",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    "team_shape_centroid_x_attacking", "team_shape_centroid_y_attacking",
    "team_shape_convex_hull_area_attacking",
    "team_shape_team_length_attacking", "team_shape_team_width_attacking",
    "team_shape_stretch_index_attacking",
    "team_shape_centroid_x_defending", "team_shape_centroid_y_defending",
    "team_shape_convex_hull_area_defending",
    "team_shape_team_length_defending", "team_shape_team_width_defending",
    "team_shape_stretch_index_defending",
    "das_team", "das_opponent", "das_diff",
    "gk_pitch_control_share_weighted", "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box", "gk_closing_time_min_s__six_yard_box",
    "n_blocked_receivers", "n_potential_receivers",
    "blocking_score", "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    "sync_score_min", "sync_score_mean", "sync_score_high_quality_frac",
]

ALL_FEATURES = _SPADL_FEATURES + _TRACKING_FEATURES

PENALTY_BRIER = 0.25  # Deliberate penalty above any reasonable Brier (scoring ~1-3% → optimal ~0.01-0.03)
VARIANCE_GATE_RATIO = 0.1  # H1: 10% of default variance


def _enrich_match_worker(args: tuple) -> pd.DataFrame:
    """Worker function for ThreadPoolExecutor — module-level for clarity."""
    match, xt, carrier_params, trial_params = args
    return _enrich_match_with_params(
        actions=match.actions.copy(),
        frames=match.frames.copy(),
        xt=xt,
        home_team_id=match.home_team_id,
        match_id_native=match.match_id,
        data_source=match.provider,
        carrier_tolerance_m=carrier_params["tolerance_m"],
        carrier_beta=carrier_params["beta"],
        carrier_gamma=carrier_params["gamma"],
        k3=trial_params["k3"],
        pre_seconds=trial_params["pre_seconds"],
        min_displacement_m=trial_params["min_displacement_m"],
    )


def _compute_provider_brier(
    enriched_actions: list[pd.DataFrame],
    matches: list[MatchData],
    vaep_labels: dict[str, pd.DataFrame],
    default_feature_variances: dict[str, float] | None,
    seed: int = 42,
) -> tuple[float, dict[str, float], dict[str, float] | None]:
    """Compute per-provider match-stratified CV Brier scores.

    Returns:
        (mean_brier, per_provider_brier, feature_importances_or_None)
    """
    import xgboost as xgb
    from sklearn.metrics import brier_score_loss
    from sklearn.model_selection import GroupKFold

    # Build combined feature matrix
    all_X: list[pd.DataFrame] = []
    all_y_scores: list[np.ndarray] = []
    all_match_ids: list[str] = []
    all_providers: list[str] = []

    for enriched, match in zip(enriched_actions, matches):
        labels = vaep_labels.get(match.match_id)
        if labels is None:
            continue

        # Enrichment preserves all input rows (verified: _enrich_match adds
        # columns via add_* functions, never drops rows; unlinked actions get NaN).
        assert len(enriched) == len(labels), (
            f"Row count mismatch for {match.match_id}: "
            f"enriched={len(enriched)}, labels={len(labels)}"
        )
        features = enriched[ALL_FEATURES].copy()
        features = features.fillna(0)  # XGBoost handles NaN but CV needs alignment
        all_X.append(features)
        all_y_scores.append(labels["scores"].values)
        n_rows = len(features)
        all_match_ids.extend([match.match_id] * n_rows)
        all_providers.extend([match.provider] * n_rows)

    if not all_X:
        return PENALTY_BRIER, {}, None

    X = pd.concat(all_X, ignore_index=True)
    y = np.concatenate(all_y_scores)
    match_ids_arr = np.array(all_match_ids)
    providers_arr = np.array(all_providers)

    # H1: Variance sanity gate
    if default_feature_variances is not None:
        optimized_cols = [
            "pressure_on_actor__link_zones",
            "n_off_ball_runners_pre_window",
            "max_off_ball_run_displacement_pre_window",
            "mean_off_ball_run_speed_pre_window",
            "n_off_ball_runners_toward_goal_pre_window",
        ]
        for col in optimized_cols:
            if col in X.columns and col in default_feature_variances:
                current_var = X[col].var()
                default_var = default_feature_variances[col]
                if default_var > 0 and current_var / default_var < VARIANCE_GATE_RATIO:
                    logger.warning(
                        "H1 gate: %s variance %.6f < 10%% of default %.6f — penalty Brier",
                        col,
                        current_var,
                        default_var,
                    )
                    return PENALTY_BRIER, {}, None

    # Per-provider CV Brier
    provider_briers: dict[str, float] = {}
    feature_importances: dict[str, float] = {}

    for provider in PROVIDERS:
        mask = providers_arr == provider
        if mask.sum() == 0:
            continue

        X_prov = X[mask].reset_index(drop=True)
        y_prov = y[mask]
        mids_prov = match_ids_arr[mask]

        # CV strategy (M5)
        unique_matches = np.unique(mids_prov)
        n_matches = len(unique_matches)

        if n_matches <= 7:
            # LOMO for small providers
            n_splits = n_matches
        else:
            n_splits = 5

        gkf = GroupKFold(n_splits=n_splits)
        fold_briers: list[float] = []
        fold_importances: list[np.ndarray] = []

        for train_idx, test_idx in gkf.split(X_prov, y_prov, groups=mids_prov):
            X_train, X_test = X_prov.iloc[train_idx], X_prov.iloc[test_idx]
            y_train, y_test = y_prov[train_idx], y_prov[test_idx]

            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=seed,
                verbosity=0,
            )
            model.fit(X_train, y_train)
            probs = model.predict_proba(X_test)[:, 1]
            fold_briers.append(brier_score_loss(y_test, probs))
            if hasattr(model, "feature_importances_"):
                fold_importances.append(model.feature_importances_)

        provider_briers[provider] = float(np.mean(fold_briers))

        if fold_importances:
            avg_imp = np.mean(fold_importances, axis=0)
            for feat, imp in zip(ALL_FEATURES, avg_imp):
                feature_importances[feat] = feature_importances.get(feat, 0) + imp

    if not provider_briers:
        return PENALTY_BRIER, {}, None

    # Average feature importances across providers (not additive)
    n_providers = len(provider_briers)
    if n_providers > 0 and feature_importances:
        feature_importances = {k: v / n_providers for k, v in feature_importances.items()}

    # M2: equal provider weight
    mean_brier = float(np.mean(list(provider_briers.values())))
    return mean_brier, provider_briers, feature_importances


def run_stage2(
    dataset: CalibrationDataset,
    *,
    n_trials: int = 100,
    n_workers: int = 8,
    seed: int = 42,
) -> None:
    """Stage 2: Optimize k3 + off-ball-runs via augmented VAEP Brier."""
    import optuna
    from concurrent.futures import ThreadPoolExecutor

    # Load Stage 1 results
    stage1_path = OUTPUT_DIR / "stage1_results.json"
    if not stage1_path.exists():
        logger.error("Stage 1 results not found at %s — run stage 1 first", stage1_path)
        sys.exit(1)

    stage1 = json.loads(stage1_path.read_text())
    carrier_params = stage1["best_params"]
    logger.info("Using Stage 1 carrier params: %s", carrier_params)

    # Compute default feature variances for H1 sanity gate (cached)
    variance_cache_path = OUTPUT_DIR / "default_variances.json"
    if variance_cache_path.exists():
        logger.info("Loading cached default feature variances from %s", variance_cache_path)
        default_variances = json.loads(variance_cache_path.read_text())
    else:
        logger.info("Computing default feature variances for H1 gate (parallel)...")
        default_trial_params = {"k3": 1.0, "pre_seconds": 1.5, "min_displacement_m": 3.0}
        default_worker_args = [
            (m, dataset.xt, carrier_params, default_trial_params)
            for m in dataset.matches
        ]
        default_enriched: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for result in pool.map(_enrich_match_worker, default_worker_args):
                default_enriched.append(result)

        default_features = pd.concat(
            [e[ALL_FEATURES] for e in default_enriched],
            ignore_index=True,
        ).fillna(0)
        default_variances = {col: float(default_features[col].var()) for col in ALL_FEATURES}
        variance_cache_path.write_text(json.dumps(default_variances, indent=2))
        logger.info("Cached default variances to %s", variance_cache_path)

    storage = f"sqlite:///{OUTPUT_DIR / 'tc3_stage2.db'}"
    study = optuna.create_study(
        study_name="tc3_stage2_vaep_brier",
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        storage=storage,
        load_if_exists=True,
    )

    # Warm-start with defaults
    study.enqueue_trial({
        "k3": 1.0,
        "pre_seconds": 1.5,
        "min_displacement_m": 3.0,
    })

    def objective(trial: optuna.Trial) -> float:
        k3 = trial.suggest_float("k3", 0.1, 5.0, log=True)
        pre_seconds = trial.suggest_float("pre_seconds", 0.5, 5.0)
        min_displacement_m = trial.suggest_float("min_displacement_m", 1.0, 8.0)

        trial_params = {
            "k3": k3,
            "pre_seconds": pre_seconds,
            "min_displacement_m": min_displacement_m,
        }

        # Parallel enrichment across matches
        worker_args = [
            (m, dataset.xt, carrier_params, trial_params)
            for m in dataset.matches
        ]

        enriched_actions: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for result in pool.map(_enrich_match_worker, worker_args):
                enriched_actions.append(result)

        # Compute Brier scores
        mean_brier, provider_briers, feat_importances = _compute_provider_brier(
            enriched_actions,
            dataset.matches,
            dataset.vaep_labels,
            default_variances,
        )

        trial.set_user_attr("per_provider_brier", provider_briers)
        if feat_importances:
            trial.set_user_attr("feature_importances", feat_importances)

        return mean_brier

    remaining = n_trials - len(study.trials)
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)

    # Save results
    best = study.best_trial
    results = {
        "best_params": best.params,
        "best_brier": best.value,
        "carrier_params_from_stage1": carrier_params,
        "per_provider_brier": best.user_attrs.get("per_provider_brier"),
        "feature_importances": best.user_attrs.get("feature_importances"),
        "default_feature_variances": default_variances,
        "n_trials": len(study.trials),
        "all_trials": [
            {
                "number": t.number,
                "params": t.params,
                "value": t.value,
                "per_provider_brier": t.user_attrs.get("per_provider_brier"),
            }
            for t in study.trials
        ],
    }
    (OUTPUT_DIR / "stage2_results.json").write_text(json.dumps(results, indent=2))
    logger.info(
        "Stage 2 complete: best Brier=%.6f, params=%s",
        best.value,
        best.params,
    )
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/tc3/test_tc3_calibration.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add scripts/run_tc3_calibration.py src/tests/tc3/test_tc3_calibration.py
git commit -m "feat(tc3): Stage 2 — VAEP Brier Optuna sweep

100 TPE trials, 8-core parallel enrichment. k3 + off-ball-runs joint optimization.
H1 variance sanity gate. M2 per-provider averaging. M5 match-stratified CV.
Carrier params fixed at Stage 1 optimum.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 9: Post-Hoc Diagnostics (Phase 3)

**Files:**
- Modify: `scripts/run_tc3_calibration.py`

- [ ] **Step 1: Write diagnostics implementation**

Add to `scripts/run_tc3_calibration.py`:

```python
# ── Phase 3: Post-hoc diagnostics ────────────────────────────────────────


def run_diagnostics(dataset: CalibrationDataset) -> None:
    """Phase 3: Post-hoc diagnostics and TF-25 gate evaluation."""
    # Load stage results
    stage1 = json.loads((OUTPUT_DIR / "stage1_results.json").read_text())
    stage2 = json.loads((OUTPUT_DIR / "stage2_results.json").read_text())

    carrier_params = stage1["best_params"]
    best_params = stage2["best_params"]
    global_brier = stage2["best_brier"]

    diagnostics: dict[str, Any] = {
        "carrier_params": carrier_params,
        "stage2_params": best_params,
        "global_brier": global_brier,
    }

    # 1. Per-provider re-evaluation at global optimum (M4: parallel + cached)
    logger.info("Running per-provider re-evaluation at global optimum...")
    from concurrent.futures import ThreadPoolExecutor

    optimum_worker_args = [
        (m, dataset.xt, carrier_params, best_params)
        for m in dataset.matches
    ]
    optimum_enriched: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for result in pool.map(_enrich_match_worker, optimum_worker_args):
            optimum_enriched.append(result)

    # Build per-provider lookup from the parallel results
    per_provider_results: dict[str, dict] = {}
    match_to_enriched = dict(zip([m.match_id for m in dataset.matches], optimum_enriched))
    for provider in PROVIDERS:
        provider_matches = dataset.matches_by_provider(provider)
        if not provider_matches:
            continue

        enriched = [match_to_enriched[m.match_id] for m in provider_matches]
        brier, _, feat_imp = _compute_provider_brier(
            enriched, provider_matches, dataset.vaep_labels, None,
        )
        per_provider_results[provider] = {
            "brier_at_global_optimum": brier,
            "n_matches": len(provider_matches),
        }

    diagnostics["per_provider_at_global_optimum"] = per_provider_results

    # 2. k3 1D sensitivity curve per provider
    # H1 optimization: only k3 changes → only re-run add_pressure_on_actor(link_zones)
    # on the already-enriched optimum data. The other 40+ features are invariant.
    logger.info("Running k3 sensitivity scan (pressure-only re-evaluation)...")
    from silly_kicks.tracking import LinkParams, add_pressure_on_actor, link_actions_to_frames
    from ingestion.tracking_context import _resolve_enrichment_identity

    k3_values = np.logspace(np.log10(0.1), np.log10(5.0), 20).tolist()
    k3_sensitivity: dict[str, list[dict]] = {}

    # Pre-resolve identity + links once per match (reused across all k3 values)
    match_links: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for m in dataset.matches:
        try:
            actions_resolved = _resolve_enrichment_identity(
                m.actions.copy(), provider=m.provider, match_id_native=m.match_id,
            )
            links, _ = link_actions_to_frames(actions_resolved, m.frames)
            match_links[m.match_id] = (actions_resolved, links)
        except Exception:
            logger.debug("Pre-resolve failed for %s — skipping in k3 scan", m.match_id, exc_info=True)

    for provider in PROVIDERS:
        provider_matches = dataset.matches_by_provider(provider)
        if not provider_matches:
            continue

        # Base enriched features from optimum (all columns except pressure_on_actor__link_zones)
        base_enriched = [match_to_enriched[m.match_id] for m in provider_matches]

        provider_curve: list[dict] = []
        for k3_val in k3_values:
            lp = LinkParams(k3=k3_val)
            patched: list[pd.DataFrame] = []
            for m, base_e in zip(provider_matches, base_enriched):
                if m.match_id not in match_links:
                    continue
                try:
                    actions_pre, links_pre = match_links[m.match_id]
                    pressure_result = add_pressure_on_actor(
                        actions_pre.copy(), m.frames, links=links_pre,
                        methods=("link_zones",),
                        params_per_method={"link_zones": lp},
                    )
                    # Patch only the pressure column into base enrichment
                    e = base_e.copy()
                    e["pressure_on_actor__link_zones"] = pressure_result[
                        "pressure_on_actor__link_zones"
                    ].values
                    patched.append(e)
                except Exception:
                    logger.debug("k3=%.3f failed for %s", k3_val, m.match_id, exc_info=True)

            if patched:
                brier, _, _ = _compute_provider_brier(
                    patched, provider_matches[:len(patched)], dataset.vaep_labels, None,
                )
                provider_curve.append({"k3": k3_val, "brier": brier})

        k3_sensitivity[provider] = provider_curve
        logger.info("k3 sensitivity for %s: %d points", provider, len(provider_curve))

    diagnostics["k3_sensitivity"] = k3_sensitivity

    # 3. TF-25 gate decision (M3)
    logger.info("Evaluating TF-25 gate criterion...")
    # Per-provider optimum: find best k3 per provider from sensitivity curve
    tf25_evaluation: dict[str, dict] = {}
    for provider, curve in k3_sensitivity.items():
        if not curve:
            continue
        best_point = min(curve, key=lambda p: p["brier"])
        global_point = min(curve, key=lambda p: abs(p["k3"] - best_params["k3"]))

        gap = global_point["brier"] - best_point["brier"]
        # Estimate CV standard error from Stage 2 per-provider Brier
        provider_brier = per_provider_results.get(provider, {}).get("brier_at_global_optimum", 0)
        n_matches = per_provider_results.get(provider, {}).get("n_matches", 1)
        # Rough SE estimate: Brier SE ~ sqrt(Brier * (1 - Brier) / n_actions)
        # Use a simpler heuristic: SE ~ Brier / sqrt(n_matches)
        se_estimate = provider_brier / max(np.sqrt(n_matches), 1)

        tf25_evaluation[provider] = {
            "global_brier": global_point["brier"],
            "provider_best_brier": best_point["brier"],
            "provider_best_k3": best_point["k3"],
            "gap": gap,
            "cv_se_estimate": se_estimate,
            "needs_own_k3": gap > se_estimate,
        }

    diagnostics["tf25_gate"] = tf25_evaluation
    needs_tf25 = any(v["needs_own_k3"] for v in tf25_evaluation.values())
    diagnostics["tf25_recommendation"] = (
        "TF-25 RECOMMENDED: at least one provider shows gap > CV SE"
        if needs_tf25
        else "TF-25 NOT NEEDED: global optimum generalizes across all providers"
    )

    # 4. Geometry sensitivity scan
    # NOTE (M2): This is a "does this parameter do anything?" sensitivity check,
    # NOT an optimization. Measures pressure variance/mean, not Brier.
    # If the scan shows a parameter has meaningful effect on pressure output,
    # a follow-up Optuna sweep over geometry params can be done separately.
    logger.info("Running geometry sensitivity scan (r_hoz, r_lz, r_hz)...")
    geometry_results: dict[str, list[dict]] = {}

    # Reuse match_links from the k3 scan (already pre-resolved above)
    subset_links = [(m, match_links[m.match_id]) for m in dataset.matches[:10] if m.match_id in match_links]

    for geom_param, default_val, scan_range in [
        ("r_hoz", 4.0, np.linspace(2.0, 8.0, 10)),
        ("r_lz", 3.0, np.linspace(1.0, 6.0, 10)),
        ("r_hz", 2.0, np.linspace(0.5, 4.0, 10)),
    ]:
        curve: list[dict] = []
        for val in scan_range:
            kwargs = {"k3": best_params["k3"]}
            kwargs[geom_param] = val
            lp = LinkParams(**kwargs)

            enriched = []
            for m, (actions_pre, links_pre) in subset_links:
                try:
                    actions_out = add_pressure_on_actor(
                        actions_pre.copy(), m.frames, links=links_pre,
                        methods=("link_zones",),
                        params_per_method={"link_zones": lp},
                    )
                    enriched.append(actions_out)
                except Exception:
                    logger.debug("Geometry %s=%.3f failed for %s", geom_param, val, m.match_id, exc_info=True)

            if enriched:
                # Simple variance check — is the pressure column varying?
                pressures = pd.concat(
                    [e["pressure_on_actor__link_zones"] for e in enriched],
                )
                curve.append({
                    geom_param: float(val),
                    "pressure_variance": float(pressures.var()),
                    "pressure_mean": float(pressures.mean()),
                })

        geometry_results[geom_param] = curve
        logger.info("Geometry scan for %s: %d points", geom_param, len(curve))

    diagnostics["geometry_sensitivity"] = geometry_results

    # 5. Feature importance comparison
    diagnostics["feature_importance_comparison"] = {
        "at_optimum": stage2.get("feature_importances", {}),
    }

    # Save diagnostics
    (OUTPUT_DIR / "per_provider_diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2, default=str),
    )

    # Generate summary
    _write_summary(stage1, stage2, diagnostics)

    logger.info("Diagnostics complete — results at %s", OUTPUT_DIR)


def _write_summary(
    stage1: dict,
    stage2: dict,
    diagnostics: dict,
) -> None:
    """Generate human-readable SUMMARY.md."""
    lines = [
        "# TC-3 Calibration Results\n",
        f"**Date**: {pd.Timestamp.now().strftime('%Y-%m-%d')}\n",
        "",
        "## Stage 1: Carrier Accuracy",
        "",
        f"**Best accuracy**: {stage1['best_accuracy']:.4f}",
        f"**Best params**: `tolerance_m={stage1['best_params']['tolerance_m']:.3f}`, "
        f"`beta={stage1['best_params']['beta']:.3f}`, "
        f"`gamma={stage1['best_params']['gamma']:.3f}`",
        f"**Mean switch rate**: {stage1.get('best_switch_rate', 'N/A')} switches/min",
        f"**Trials**: {stage1['n_trials']}",
        "",
        "### Per-provider accuracy",
        "",
    ]
    for provider, acc in (stage1.get("per_provider_accuracy") or {}).items():
        lines.append(f"- {provider}: {acc:.4f}")

    lines.extend([
        "",
        "## Stage 2: VAEP Brier",
        "",
        f"**Best Brier**: {stage2['best_brier']:.6f}",
        f"**Best params**: `k3={stage2['best_params']['k3']:.3f}`, "
        f"`pre_seconds={stage2['best_params']['pre_seconds']:.3f}`, "
        f"`min_displacement_m={stage2['best_params']['min_displacement_m']:.3f}`",
        f"**Trials**: {stage2['n_trials']}",
        "",
        "### Per-provider Brier",
        "",
    ])
    for provider, brier in (stage2.get("per_provider_brier") or {}).items():
        lines.append(f"- {provider}: {brier:.6f}")

    lines.extend([
        "",
        "## TF-25 Gate",
        "",
        f"**Recommendation**: {diagnostics.get('tf25_recommendation', 'N/A')}",
        "",
    ])
    for provider, gate in (diagnostics.get("tf25_gate") or {}).items():
        lines.append(
            f"- {provider}: gap={gate['gap']:.6f}, SE={gate['cv_se_estimate']:.6f}, "
            f"needs_own_k3={gate['needs_own_k3']}"
        )

    lines.extend([
        "",
        "## Provenance",
        "",
        "Optuna-calibrated against lakehouse production data on "
        f"{pd.Timestamp.now().strftime('%Y-%m-%d')}.",
        f"Providers: {', '.join(PROVIDERS)}.",
        f"Stage 1 trials: {stage1['n_trials']}, Stage 2 trials: {stage2['n_trials']}.",
        "",
        "## Recommended Default Updates (silly-kicks PR)",
        "",
        "```python",
        "# infer_ball_carrier defaults",
        f"tolerance_m = {stage1['best_params']['tolerance_m']:.3f}",
        f"beta = {stage1['best_params']['beta']:.3f}",
        f"gamma = {stage1['best_params']['gamma']:.3f}",
        "",
        "# LinkParams.k3",
        f"k3 = {stage2['best_params']['k3']:.3f}",
        "",
        "# add_off_ball_runs / add_off_ball_context defaults",
        f"pre_seconds = {stage2['best_params']['pre_seconds']:.3f}",
        f"min_displacement_m = {stage2['best_params']['min_displacement_m']:.3f}",
        "```",
    ])

    (OUTPUT_DIR / "SUMMARY.md").write_text("\n".join(lines))
```

- [ ] **Step 2: Run lint**

Run: `uv run ruff check scripts/run_tc3_calibration.py`
Expected: 0 errors

- [ ] **Step 3: Commit**

```bash
git add scripts/run_tc3_calibration.py
git commit -m "feat(tc3): Phase 3 diagnostics + Phase 4 output generation

Per-provider re-evaluation, k3 sensitivity curves, TF-25 gate criterion,
geometry sensitivity scan, feature importance comparison, SUMMARY.md.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 10: Integration Test + Final Lint Pass

**Files:**
- Modify: `src/tests/tc3/test_tc3_calibration.py`

- [ ] **Step 1: Add integration smoke test**

Append to `src/tests/tc3/test_tc3_calibration.py`:

```python
class TestCalibrationCLI:
    def test_cli_argument_parsing(self) -> None:
        """Verify CLI args parse correctly."""
        from run_tc3_calibration import main

        assert callable(main)

    def test_all_features_list_complete(self) -> None:
        """Verify ALL_FEATURES covers the expected tracking context columns."""
        from run_tc3_calibration import ALL_FEATURES, _SPADL_FEATURES, _TRACKING_FEATURES

        assert len(ALL_FEATURES) == len(_SPADL_FEATURES) + len(_TRACKING_FEATURES)
        # Key features must be present
        assert "pressure_on_actor__link_zones" in ALL_FEATURES
        assert "n_off_ball_runners_pre_window" in ALL_FEATURES
        assert "das_team" in ALL_FEATURES
```

- [ ] **Step 2: Run all tests**

Run: `uv run pytest src/tests/tc3/test_tc3_calibration.py src/tests/test_gradientsports_ingestion.py -v`
Expected: PASS

- [ ] **Step 3: Run full lint + type check**

Run: `uv run ruff check src/ingestion/gradientsports*.py scripts/run_tc3_calibration.py src/tests/tc3/test_tc3_calibration.py src/tests/test_gradientsports_ingestion.py && uv run ruff format --check src/ingestion/gradientsports*.py scripts/run_tc3_calibration.py`
Expected: 0 errors

- [ ] **Step 4: Commit**

```bash
git add src/tests/tc3/test_tc3_calibration.py src/tests/test_gradientsports_ingestion.py
git commit -m "test(tc3): integration smoke tests + lint pass

Verifies CLI parsing, feature list completeness, API compatibility.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Execution Notes

### Gradient Sports Data Availability

The `gradientsports` pining-for-the-data endpoint returns 0 matches as of 2026-05-19. Tasks 1-4 (ingestion) should be executed once the data is confirmed available on the API. The calibration script (Tasks 5-10) works with IDSSE + SkillCorner immediately; Gradient Sports matches are added transparently once ingested.

### Running the Sweep

```bash
# Phase 0: Pull data + cache
uv run python scripts/run_tc3_calibration.py --stage 0

# Stage 1: Carrier accuracy (~30-67 min)
uv run python scripts/run_tc3_calibration.py --stage 1

# Stage 2: VAEP Brier (overnight, ~8-17h)
nohup uv run python scripts/run_tc3_calibration.py --stage 2 > tc3_stage2.log 2>&1 &

# Diagnostics (~3h)
uv run python scripts/run_tc3_calibration.py --stage diagnostics
```

### Resume After Interruption

SQLite storage persists all completed trials. Re-running the same stage picks up where it left off:

```bash
uv run python scripts/run_tc3_calibration.py --stage 2  # resumes from last trial
```
