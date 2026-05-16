# SkillCorner Ingestion Rewrite — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace kloppy-based SkillCorner tracking-only ingestion with full events + tracking + match metadata from the pining-for-the-data REST API, and wire SPADL conversion via silly-kicks.

**Architecture:** API-driven discovery via `updatedSince` filter. Split by artifact: `skillcorner_common.py` (API client), `skillcorner_events.py`, `skillcorner_tracking.py`, `skillcorner_matches.py`, `skillcorner.py` (orchestrator). SPADL conversion via `applyInPandas` UDF with driver-side match_metadata dict.

**Tech Stack:** Python 3.10, PySpark, Delta Lake, silly-kicks (SkillCorner SPADL converter), pining-for-the-data REST API, dbt, pytest

**Spec:** `docs/superpowers/specs/2026-05-15-skillcorner-ingestion-rewrite-design.md` (rev 3, APPROVED)

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/ingestion/skillcorner_common.py` | API client, `MatchInfo` Pydantic model, shared constants |
| `src/ingestion/skillcorner_events.py` | `dynamic_events.csv` parser → `bronze.skillcorner_events` |
| `src/ingestion/skillcorner_tracking.py` | `tracking_extrapolated.jsonl` parser → `bronze.skillcorner_tracking` |
| `src/ingestion/skillcorner_matches.py` | `match.json` parser → `bronze.skillcorner_matches` |
| `src/tests/test_skillcorner_common.py` | API client unit tests |
| `src/tests/test_skillcorner_events.py` | Events parser unit tests |
| `src/tests/test_skillcorner_tracking.py` | Tracking parser unit tests |
| `src/tests/test_skillcorner_matches.py` | Match parser unit tests |
| `src/tests/test_skillcorner_spadl.py` | SPADL UDF unit tests |
| `src/tests/test_skillcorner_e2e.py` | End-to-end integration test (no Spark) |
| `src/tests/fixtures/silly_kicks_boundary/sc_match_1886347.parquet` | Boundary test events fixture |
| `src/tests/fixtures/silly_kicks_boundary/sc_match_1886347_meta.json` | Boundary test match metadata fixture |
| `src/tests/fixtures/skillcorner/events_subset.csv` | E2E test fixture (~50 rows) |
| `src/tests/fixtures/skillcorner/match.json` | E2E test fixture (match 1886347) |
| `src/tests/fixtures/skillcorner/tracking_subset.jsonl` | E2E test fixture (~20 frames) |
| `dbt_project/models/staging/skillcorner/stg_skillcorner__events.sql` | Events staging model |
| `dbt_project/models/staging/skillcorner/stg_skillcorner__matches.sql` | Matches staging model |
| `scripts/drop_old_skillcorner_tracking.sql` | Drop old kloppy-sourced bronze table (operator-driven, NOT in auto-apply tree) |

### Modified Files

| File | Change |
|------|--------|
| `src/ingestion/skillcorner.py` | Complete rewrite: orchestrator with guard + `@workflow` |
| `src/shared/identifiers.py` | Add SkillCorner generators + NamedTuple class methods |
| `src/ingestion/spadl_conversion.py` | Add `_make_skillcorner_spadl_udf`, `_convert_skillcorner_from_bronze`, `_make_skillcorner_replace_where` |
| `src/ingestion/spadl_enrichments.py` | Add `"skillcorner"` to `_VALID_SOURCES` |
| `src/ingestion/spadl_vaep.py` | Add `_convert_skillcorner_from_bronze` import + dispatch call |
| ~~`src/ingestion/tracking_context.py`~~ | ~~REMOVED — tracking context not worth investing in~~ |
| `src/ingestion/tracking_metadata.py` | Remove SkillCorner section (guard, constants, extractor) |
| `src/tests/test_format_contract.py` | Add SkillCorner format-contract test classes |
| `src/tests/test_silly_kicks_boundary.py` | Add 5th source parametrization |
| ~~`src/tests/test_tracking_context_identity_resolution.py`~~ | ~~REMOVED — tracking context not in scope~~ |
| ~~`src/tests/test_tracking_context_column_projection.py`~~ | ~~REMOVED — tracking context not in scope~~ |
| `src/tests/test_source_onboarding_contracts.py` | Add SkillCorner parametrization |
| `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql` | Rewrite for new bronze schema |
| `dbt_project/models/staging/skillcorner/_skillcorner__sources.yml` | Update source definitions |
| `dbt_project/models/staging/skillcorner/_skillcorner__models.yml` | Update column docs + tests |
| `dbt_project/models/dimensions/dim_teams.sql` | Add SkillCorner branch |
| `dbt_project/models/dimensions/dim_players.sql` | Add SkillCorner branch |
| `dbt_project/models/dimensions/dim_matches.sql` | Add SkillCorner branch |
| `workflow-cards/wf-skillcorner.yaml` | Rewrite to v2.0.0 |
| `pyproject.toml` | Remove kloppy dependency, update entry point |
| `terraform/modules/workflows/main.tf` | Switch env_key `"tracking"` → `"default"`, delete tracking env block |

### Removed Dependencies / Environments

| Item | Reason |
|------|--------|
| `kloppy` extra from `silly-kicks[kloppy,das]` in `pyproject.toml` | SkillCorner was sole consumer |
| `"tracking"` environment block in `terraform/modules/workflows/main.tf` | Only provided kloppy; both consumers switch to `"default"` |

---

## Task Dependency Graph

```
Task 1 (identifiers)  ──┐
                         ├── Task 7 (SPADL conversion) ──┐
Task 2 (common)  ───┐   │                                │
                     ├── Task 6 (orchestrator)            │
Task 3 (events)  ───┤                                    │
                     │                                    │
Task 4 (tracking) ──┤                                    │
                     │                                    │
Task 5 (matches)  ──┘                                    │
                                                          │
Task 9 (deprecation) ──── depends on Tasks 6-7           │
                                                          │
Task 10 (dbt) ──── independent                           │
                                                          │
Task 11 (workflow card) ──── depends on Task 9           │
                                                          │
Task 12 (boundary tests) ──── depends on Tasks 1-11
                                                          │
Task 13 (E2E test) ──── depends on all above
```

Tasks 1-5 can be parallelized. Tasks 7 and 10 can be parallelized. Task 8 REMOVED (tracking context not worth investing in).

---

### Task 1: ADR-018 Identifier Generators

**Files:**
- Modify: `src/shared/identifiers.py` (after line 274)
- Test: `src/tests/test_format_contract.py`

- [ ] **Step 1: Write the failing tests**

Add to `src/tests/test_format_contract.py`:

```python
from shared.identifiers import (
    NativeMatchId,
    NativePlayerId,
    NativeTeamId,
    skillcorner_native_match_id,
    skillcorner_native_player_id,
    skillcorner_native_team_id,
)


class TestSkillCornerFormatContract:
    def test_skillcorner_match_id_from_string(self) -> None:
        assert skillcorner_native_match_id("1886347") == "1886347"

    def test_skillcorner_match_id_from_int(self) -> None:
        assert skillcorner_native_match_id(1886347) == "1886347"

    def test_skillcorner_match_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("skillcorner_1886347")

    def test_skillcorner_match_id_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("")

    def test_skillcorner_match_id_rejects_alpha(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("abc123")


class TestSkillCornerPlayerIdFormatContract:
    def test_skillcorner_native_player_id_valid(self) -> None:
        assert skillcorner_native_player_id(38673) == "38673"

    def test_skillcorner_native_player_id_string(self) -> None:
        assert skillcorner_native_player_id("38673") == "38673"

    def test_skillcorner_native_player_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            skillcorner_native_player_id("player_38673")


class TestSkillCornerTeamIdFormatContract:
    def test_skillcorner_native_team_id_valid(self) -> None:
        assert skillcorner_native_team_id(4177) == "4177"

    def test_skillcorner_native_team_id_string(self) -> None:
        assert skillcorner_native_team_id("4177") == "4177"

    def test_skillcorner_native_team_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            skillcorner_native_team_id("team_4177")


class TestSkillCornerNamedTuples:
    def test_native_match_id_skillcorner(self) -> None:
        nid = NativeMatchId.skillcorner("1886347")
        assert nid.provider == "skillcorner"
        assert nid.value == "1886347"

    def test_native_player_id_skillcorner(self) -> None:
        nid = NativePlayerId.skillcorner("38673")
        assert nid.provider == "skillcorner"
        assert nid.value == "38673"

    def test_native_team_id_skillcorner(self) -> None:
        nid = NativeTeamId.skillcorner("4177")
        assert nid.provider == "skillcorner"
        assert nid.value == "4177"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_format_contract.py -k "SkillCorner" -v`
Expected: FAIL — `ImportError: cannot import name 'skillcorner_native_match_id'`

- [ ] **Step 3: Implement identifier generators**

Add to `src/shared/identifiers.py` after line 197 (after `idsse_native_team_id`):

```python
# ---------------------------------------------------------------------------
# SkillCorner (A-League broadcast tracking)
# ---------------------------------------------------------------------------

_SKILLCORNER_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]+$")


def skillcorner_native_match_id(raw_match_id: str | int) -> str:
    """Canonical SkillCorner native match id -- stringified positive integer."""
    s = str(raw_match_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner match id: {raw_match_id!r} (expected numeric string)")
    return s


def skillcorner_native_player_id(raw_player_id: str | int) -> str:
    """Canonical SkillCorner native player id -- stringified positive integer."""
    s = str(raw_player_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner player id: {raw_player_id!r} (expected numeric string)")
    return s


def skillcorner_native_team_id(raw_team_id: str | int) -> str:
    """Canonical SkillCorner native team id -- stringified positive integer."""
    s = str(raw_team_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner team id: {raw_team_id!r} (expected numeric string)")
    return s
```

Add `.skillcorner()` class methods to all 3 NamedTuple classes. In `NativeMatchId` (after the `.metrica()` method at line 228):

```python
    @classmethod
    def skillcorner(cls, raw: str | int) -> NativeMatchId:
        return cls(provider="skillcorner", value=skillcorner_native_match_id(raw))
```

In `NativePlayerId` (after `.metrica()` at line 251):

```python
    @classmethod
    def skillcorner(cls, raw: str | int) -> NativePlayerId:
        return cls(provider="skillcorner", value=skillcorner_native_player_id(raw))
```

In `NativeTeamId` (after `.metrica()` at line 274):

```python
    @classmethod
    def skillcorner(cls, raw: str | int) -> NativeTeamId:
        return cls(provider="skillcorner", value=skillcorner_native_team_id(raw))
```

Update the module docstring (line 2) to say `5 SPADL data sources` and add SkillCorner.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_format_contract.py -k "SkillCorner" -v`
Expected: PASS (all 12 tests)

- [ ] **Step 5: Run pyright on identifiers.py**

Run: `uv run pyright src/shared/identifiers.py`
Expected: 0 errors

---

### Task 2: Common Module — API Client

**Files:**
- Create: `src/ingestion/skillcorner_common.py`
- Create: `src/tests/test_skillcorner_common.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_skillcorner_common.py`:

```python
"""Unit tests for the SkillCorner API client (skillcorner_common)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.skillcorner_common import (
    API_BASE_URL,
    PROVIDER,
    MatchInfo,
    fetch_artifact,
    fetch_match_list,
)


class TestMatchInfo:
    def test_parse_match_info(self) -> None:
        raw = {
            "id": "1886347",
            "artifacts": {"1886347_dynamic_events": "1886347_dynamic_events.csv"},
            "home": "Auckland FC",
            "away": "Newcastle",
            "date": "2024-11-30",
            "updated_at": "2026-05-04T02:44:12Z",
            "visibility": "public",
        }
        info = MatchInfo.model_validate(raw)
        assert info.id == "1886347"
        assert info.home == "Auckland FC"
        assert info.updated_at == datetime(2026, 5, 4, 2, 44, 12, tzinfo=timezone.utc)


class TestFetchMatchList:
    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_all_matches(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "provider": "skillcorner",
            "matches": [
                {
                    "id": "1886347",
                    "artifacts": {"1886347_match": "1886347_match.json"},
                    "home": "Auckland FC",
                    "away": "Newcastle",
                    "date": "2024-11-30",
                    "updated_at": "2026-05-04T02:44:12Z",
                    "visibility": "public",
                }
            ],
        }
        mock_fetch.return_value = mock_resp

        result = fetch_match_list("fake-token")
        assert len(result) == 1
        assert result[0].id == "1886347"
        assert result[0].home == "Auckland FC"

        # Verify URL construction
        call_url = mock_fetch.call_args[0][0]
        assert call_url == f"{API_BASE_URL}/skillcorner/matches"

    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_with_updated_since(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"provider": "skillcorner", "matches": []}
        mock_fetch.return_value = mock_resp

        fetch_match_list("fake-token", updated_since="2027-01-01T00:00:00Z")

        call_url = mock_fetch.call_args[0][0]
        assert "updatedSince=2027-01-01T00%3A00%3A00Z" in call_url or "updatedSince=2027-01-01T00:00:00Z" in call_url

    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_empty_response(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"provider": "skillcorner", "matches": []}
        mock_fetch.return_value = mock_resp

        result = fetch_match_list("fake-token")
        assert result == []


class TestFetchArtifact:
    @patch("ingestion.skillcorner_common.fetch_url")
    def test_fetch_artifact_constructs_url(self, mock_fetch: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_fetch.return_value = mock_resp

        fetch_artifact("1886347", "1886347_dynamic_events", "fake-token")

        call_url = mock_fetch.call_args[0][0]
        assert "/skillcorner/matches/1886347/1886347_dynamic_events" in call_url


class TestConstants:
    def test_api_base_url_is_https(self) -> None:
        assert API_BASE_URL.startswith("https://")

    def test_provider_name(self) -> None:
        assert PROVIDER == "skillcorner"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_skillcorner_common.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.skillcorner_common'`

- [ ] **Step 3: Implement the common module**

Create `src/ingestion/skillcorner_common.py`:

```python
"""SkillCorner API client and shared data models.

Talks to the pining-for-the-data REST API to discover and retrieve
SkillCorner match artifacts (events, tracking, match metadata).
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
PROVIDER = "skillcorner"


class MatchInfo(pydantic.BaseModel):
    """A single match from the pining-for-the-data discovery endpoint."""

    id: str
    artifacts: dict[str, str]
    home: str
    away: str
    date: str
    updated_at: datetime
    visibility: str


def fetch_match_list(
    token: str,
    updated_since: str | None = None,
) -> list[MatchInfo]:
    """GET /v1/skillcorner/matches with optional updatedSince filter.

    Args:
        token: Bearer token for the pining-for-the-data API.
        updated_since: ISO 8601 UTC timestamp to filter matches updated after.

    Returns:
        List of MatchInfo objects for matches matching the filter.
    """
    url = f"{API_BASE_URL}/skillcorner/matches"
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
        match_id: SkillCorner match ID (e.g. "1886347").
        artifact_key: Artifact key (e.g. "1886347_dynamic_events").
        token: Bearer token for the pining-for-the-data API.
        stream: If True, don't download body eagerly (use .iter_content()).

    Returns:
        Response object containing the artifact content.
    """
    url = f"{API_BASE_URL}/skillcorner/matches/{match_id}/{artifact_key}"
    return fetch_url(url, headers={"Authorization": f"Bearer {token}"}, stream=stream)


def resolve_pining_token() -> str:
    """Resolve the pining-for-the-data API token.

    Resolution order:
        1. ``PINING_FOR_THE_DATA_TOKEN`` environment variable (local dev, CI)
        2. Databricks secret scope ``pining``, key ``token`` (serverless)

    Raises:
        RuntimeError: If token cannot be found in any source.
    """
    import os

    token = os.environ.get("PINING_FOR_THE_DATA_TOKEN", "")
    if token:
        return token

    try:
        import base64

        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

        client = WorkspaceClient()
        resp = client.secrets.get_secret(scope="pining", key="token")
        encoded = resp.value or ""
        if encoded:
            return base64.b64decode(encoded).decode()
    except Exception:  # noqa: BLE001 — multi-source fallback
        pass

    msg = (
        "PINING_FOR_THE_DATA_TOKEN not found in environment or Databricks secrets. "
        "Setup: databricks secrets put-secret --scope pining --key token"
    )
    raise RuntimeError(msg)
```

**PREREQUISITE — `fetch_url` signature extension (verified 2026-05-16):**

`ingestion/utils.py:399` currently has signature `fetch_url(url, timeout=(10,30), max_retries=3)` — no `headers` or `stream` params. The session uses `session.get(url, timeout=timeout)` at line 429.

**Before implementing this task**, add two params to `fetch_url`:

```python
def fetch_url(
    url: str,
    timeout: tuple[int, int] = (10, 30),
    max_retries: int = 3,
    *,
    headers: dict[str, str] | None = None,
    stream: bool = False,
) -> requests.Response:
```

And update line 429:

```python
        response = session.get(url, timeout=timeout, headers=headers, stream=stream)
```

This is backward-compatible (defaults preserve existing behavior). All existing callers pass positional `url` only.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_skillcorner_common.py -v`
Expected: PASS

- [ ] **Step 5: Run pyright**

Run: `uv run pyright src/ingestion/skillcorner_common.py`
Expected: 0 errors

---

### Task 3: Events Parser — `skillcorner_events.py`

**Files:**
- Create: `src/ingestion/skillcorner_events.py`
- Create: `src/tests/test_skillcorner_events.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_skillcorner_events.py`:

```python
"""Unit tests for SkillCorner events ingestion."""

from __future__ import annotations

import io
from datetime import datetime, timezone

import pandas as pd
import pytest

from ingestion.skillcorner_events import parse_events_csv


class TestParseEventsCsv:
    def test_basic_parse(self) -> None:
        csv_content = (
            "event_id,event_type,event_subtype,player_id,team_id,period,"
            "time_start,time_end,x_start,y_start,x_end,y_end,"
            "game_interruption_before,game_interruption_after,end_type,start_type\n"
            "1_0,pass,short_pass,38673,4177,1,"
            "00:01.2,00:03.4,10.5,-5.2,20.1,3.4,"
            ",,successful,\n"
        )
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")

        assert len(df) == 1
        assert df["match_id"].iloc[0] == "1886347"
        assert df["event_id"].iloc[0] == "1_0"
        assert df["player_id"].iloc[0] == 38673
        assert df["team_id"].iloc[0] == 4177
        assert "_ingested_at" in df.columns

    def test_match_id_is_raw_native(self) -> None:
        """match_id must be raw native (e.g. '1886347'), not prefixed."""
        csv_content = (
            "event_id,event_type,player_id,team_id,period\n"
            "1_0,pass,38673,4177,1\n"
        )
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        assert df["match_id"].iloc[0] == "1886347"
        assert not df["match_id"].iloc[0].startswith("skillcorner_")

    def test_ingested_at_is_utc(self) -> None:
        csv_content = "event_id,event_type,player_id,team_id,period\n1_0,pass,38673,4177,1\n"
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        ts = df["_ingested_at"].iloc[0]
        assert ts.tzinfo is not None or isinstance(ts, datetime)

    def test_all_294_columns_preserved(self) -> None:
        """Bronze-completeness: all source columns plus match_id + _ingested_at."""
        cols = [f"col_{i}" for i in range(294)]
        header = ",".join(cols)
        values = ",".join(["x"] * 294)
        csv_content = f"{header}\n{values}\n"
        df = parse_events_csv(io.StringIO(csv_content), match_id="1886347")
        # Source columns + match_id + _ingested_at
        assert len(df.columns) == 296
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_skillcorner_events.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the events parser**

Create `src/ingestion/skillcorner_events.py`:

```python
"""SkillCorner events ingestion -- dynamic_events.csv to bronze.

Reads the dynamic_events CSV artifact from the pining-for-the-data API,
adds match_id and _ingested_at audit column, and writes to Delta.

Bronze table: bronze.skillcorner_events
Coordinate system: POSSESSION_PERSPECTIVE (center-origin meters, preserved as-is).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import IO, TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_events_csv(source: str | IO[str], *, match_id: str) -> pd.DataFrame:
    """Parse a dynamic_events CSV into a pandas DataFrame.

    All source columns are preserved (bronze-completeness principle).
    Adds ``match_id`` (raw native ID) and ``_ingested_at`` (UTC timestamp).

    Args:
        source: File path or file-like object containing the CSV data.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").

    Returns:
        DataFrame with all source columns plus match_id and _ingested_at.
    """
    df = pd.read_csv(source, low_memory=False)
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
    """Write parsed events DataFrame to bronze.skillcorner_events.

    Uses replaceWhere on match_id for idempotent writes.
    """
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "event_id", "event_type"],
        "skillcorner_events",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_events",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_skillcorner_events.py -v`
Expected: PASS

- [ ] **Step 5: Run pyright + ruff**

Run: `uv run pyright src/ingestion/skillcorner_events.py && uv run ruff check src/ingestion/skillcorner_events.py`
Expected: 0 errors

---

### Task 4: Tracking Parser — `skillcorner_tracking.py`

**Files:**
- Create: `src/ingestion/skillcorner_tracking.py`
- Create: `src/tests/test_skillcorner_tracking.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_skillcorner_tracking.py`:

```python
"""Unit tests for SkillCorner tracking ingestion (JSONL parser)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from ingestion.skillcorner_tracking import parse_tracking_jsonl


def _make_frame(frame_num: int, period: int, timestamp: str) -> dict:
    """Build a single JSONL frame dict for testing."""
    return {
        "frame": frame_num,
        "period": period,
        "timestamp": timestamp,
        "player_data": [
            {
                "player_id": 38673,
                "x": 10.5,
                "y": -5.2,
                "is_detected": True,
            },
            {
                "player_id": 44001,
                "x": -20.1,
                "y": 3.4,
                "is_detected": False,
            },
        ],
        "ball_data": {
            "x": 5.0,
            "y": -1.0,
            "z": 0.3,
            "is_detected": True,
        },
    }


class TestParseTrackingJsonl:
    def test_basic_parse(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.20"), _make_frame(2, 1, "00:00:01.30")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text("\n".join(json.dumps(f) for f in frames))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        # 2 frames x 2 players = 4 rows
        assert len(df) == 4
        assert set(df["player_id"].unique()) == {38673, 44001}

    def test_timestamp_parsed_to_float(self, tmp_path: Path) -> None:
        """timestamp 'HH:MM:SS.ms' must be parsed to float seconds."""
        frames = [_make_frame(1, 1, "00:12:34.90")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["timestamp"].dtype == "Float64"
        # 0*3600 + 12*60 + 34.90 = 754.9
        assert abs(df["timestamp"].iloc[0] - 754.9) < 0.01

    def test_is_detected_renamed_to_is_visible(self, tmp_path: Path) -> None:
        """Raw API field 'is_detected' becomes bronze column 'is_visible'."""
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert "is_visible" in df.columns
        assert "is_detected" not in df.columns
        # First player is_detected=True
        row = df[df["player_id"] == 38673].iloc[0]
        assert row["is_visible"] is True

    def test_match_id_is_raw_native(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["match_id"].iloc[0] == "1886347"
        assert not df["match_id"].iloc[0].startswith("skillcorner_")

    def test_ball_columns_present(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert "ball_x" in df.columns
        assert "ball_y" in df.columns
        assert "ball_z" in df.columns
        assert "ball_is_detected" in df.columns
        assert df["ball_x"].iloc[0] == 5.0
        assert df["ball_z"].iloc[0] == 0.3

    def test_frame_rate_is_10(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["frame_rate"].iloc[0] == 10

    def test_schema_columns(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        expected = {
            "match_id", "period", "frame", "timestamp", "player_id",
            "x", "y", "is_visible", "ball_x", "ball_y", "ball_z",
            "ball_is_detected", "frame_rate", "_ingested_at",
        }
        assert set(df.columns) == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_skillcorner_tracking.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the tracking parser**

Create `src/ingestion/skillcorner_tracking.py`:

```python
"""SkillCorner tracking ingestion -- tracking_extrapolated.jsonl to bronze.

Streams the JSONL artifact line-by-line, reshapes to narrow format
(one row per player per frame), normalizes timestamp from string to
float seconds, and renames is_detected -> is_visible.

Bronze table: bronze.skillcorner_tracking
Coordinate system: center-origin meters (preserved as-is).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_FRAME_RATE = 10

_TIMESTAMP_PATTERN = re.compile(r"^(\d+):(\d+):(\d+(?:\.\d+)?)$")

_TRACKING_DTYPE_OVERRIDES: dict[str, str] = {
    "period": "Int64",
    "frame": "Int64",
    "timestamp": "Float64",
    "player_id": "Int64",
    "x": "Float64",
    "y": "Float64",
    "ball_x": "Float64",
    "ball_y": "Float64",
    "ball_z": "Float64",
    "frame_rate": "Int64",
    "is_visible": "boolean",
    "ball_is_detected": "boolean",
}


def _parse_timestamp(ts_str: str) -> float:
    """Parse 'HH:MM:SS.ms' to float seconds.

    Examples:
        '00:12:34.90' -> 754.9
        '01:30:00.00' -> 5400.0
    """
    m = _TIMESTAMP_PATTERN.match(ts_str)
    if m is None:
        raise ValueError(f"Cannot parse timestamp: {ts_str!r}")
    hours = int(m.group(1))
    minutes = int(m.group(2))
    seconds = float(m.group(3))
    return hours * 3600.0 + minutes * 60.0 + seconds


def parse_tracking_jsonl(source: str, *, match_id: str) -> pd.DataFrame:
    """Parse a tracking_extrapolated.jsonl file to narrow-format DataFrame.

    Streams line-by-line (one JSON object per frame). Each frame contains
    player_data (list of player positions) and ball_data. Reshapes to one
    row per player per frame.

    Args:
        source: File path to the JSONL file.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").

    Returns:
        DataFrame in narrow format with columns per spec section 5.2.
    """
    rows: list[dict[str, object]] = []

    with open(source, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            frame_obj = json.loads(line)

            frame_num = frame_obj["frame"]
            period = frame_obj["period"]
            ts_raw = frame_obj["timestamp"]
            timestamp = _parse_timestamp(ts_raw)

            ball = frame_obj.get("ball_data") or {}
            ball_x = ball.get("x")
            ball_y = ball.get("y")
            ball_z = ball.get("z")
            ball_is_detected = ball.get("is_detected")

            for player in frame_obj.get("player_data", []):
                rows.append(
                    {
                        "match_id": match_id,
                        "period": period,
                        "frame": frame_num,
                        "timestamp": timestamp,
                        "player_id": player["player_id"],
                        "x": player.get("x"),
                        "y": player.get("y"),
                        "is_visible": player.get("is_detected"),
                        "ball_x": ball_x,
                        "ball_y": ball_y,
                        "ball_z": ball_z,
                        "ball_is_detected": ball_is_detected,
                        "frame_rate": _FRAME_RATE,
                    }
                )

    df = pd.DataFrame(rows)
    # Apply dtype overrides for Arrow/Spark compatibility
    for col, dtype in _TRACKING_DTYPE_OVERRIDES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
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
    """Write parsed tracking DataFrame to bronze.skillcorner_tracking."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "frame", "period", "player_id", "x", "y"],
        "skillcorner_tracking",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_tracking",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_skillcorner_tracking.py -v`
Expected: PASS (all 7 tests)

- [ ] **Step 5: Run pyright + ruff**

Run: `uv run pyright src/ingestion/skillcorner_tracking.py && uv run ruff check src/ingestion/skillcorner_tracking.py`
Expected: 0 errors

---

### Task 5: Match Parser — `skillcorner_matches.py`

**Files:**
- Create: `src/ingestion/skillcorner_matches.py`
- Create: `src/tests/test_skillcorner_matches.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_skillcorner_matches.py`:

```python
"""Unit tests for SkillCorner match metadata ingestion."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from ingestion.skillcorner_matches import parse_match_json


def _make_match_json() -> dict:
    """Build a minimal but complete match.json fixture."""
    return {
        "id": 1886347,
        "date_time": "2024-11-30T15:00:00Z",
        "pitch_length": 105,
        "pitch_width": 68,
        "stadium": {"name": "Go Media Stadium"},
        "home_team": {"id": 4177, "name": "Auckland FC", "short_name": "AUK"},
        "away_team": {"id": 4262, "name": "Newcastle Jets", "short_name": "NEW"},
        "competition_edition": {
            "competition": {"id": 382, "name": "A-League Men"},
            "season": {"id": 74, "name": "2024/2025"},
        },
        "match_periods": [
            {"period": 1, "start_time": "00:00:00.00", "end_time": "00:47:23.50"},
            {"period": 2, "start_time": "00:00:00.00", "end_time": "00:49:12.30"},
        ],
        "players": [
            {
                "id": 38673,
                "team_id": 4177,
                "short_name": "A. Player",
                "first_name": "Andrew",
                "last_name": "Player",
                "number": 10,
                "player_role": {"name": "Midfielder", "acronym": "MF"},
            },
            {
                "id": 44001,
                "team_id": 4177,
                "short_name": "B. Keeper",
                "first_name": "Bob",
                "last_name": "Keeper",
                "number": 1,
                "player_role": {"name": "Goalkeeper", "acronym": "GK"},
            },
            {
                "id": 50200,
                "team_id": 4262,
                "short_name": "C. Forward",
                "first_name": "Charlie",
                "last_name": "Forward",
                "number": 9,
                "player_role": {"name": "Forward", "acronym": "FW"},
            },
        ],
    }


class TestParseMatchJson:
    def test_roster_row_count(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        # 3 players = 3 rows
        assert len(df) == 3

    def test_match_id_is_raw_native(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        assert df["match_id"].iloc[0] == "1886347"

    def test_player_fields(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df[df["player_id"] == 38673].iloc[0]
        assert row["player_name"] == "A. Player"
        assert row["first_name"] == "Andrew"
        assert row["last_name"] == "Player"
        assert row["jersey_number"] == 10
        assert row["position_name"] == "Midfielder"
        assert row["position_acronym"] == "MF"

    def test_team_resolution(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        # Home player
        home_row = df[df["player_id"] == 38673].iloc[0]
        assert home_row["team_id"] == 4177
        assert home_row["team_name"] == "Auckland FC"
        assert home_row["home_team_id"] == 4177
        assert home_row["away_team_id"] == 4262
        # Away player
        away_row = df[df["player_id"] == 50200].iloc[0]
        assert away_row["team_id"] == 4262
        assert away_row["team_name"] == "Newcastle Jets"

    def test_competition_metadata(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df.iloc[0]
        assert row["competition_id"] == 382
        assert row["competition_name"] == "A-League Men"
        assert row["season_id"] == 74
        assert row["season_name"] == "2024/2025"

    def test_pitch_dimensions(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df.iloc[0]
        assert row["pitch_length"] == 105
        assert row["pitch_width"] == 68

    def test_period_boundaries_serialized(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        periods = json.loads(df["period_boundaries"].iloc[0])
        assert len(periods) == 2
        assert periods[0]["period"] == 1

    def test_goalkeeper_position(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        gk_row = df[df["player_id"] == 44001].iloc[0]
        assert gk_row["position_name"] == "Goalkeeper"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_skillcorner_matches.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the match parser**

Create `src/ingestion/skillcorner_matches.py`:

```python
"""SkillCorner match metadata ingestion -- match.json to bronze.

Parses the match.json artifact, denormalizes to one row per player-match
(roster format), and writes to Delta.

Bronze table: bronze.skillcorner_matches
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_MATCHES_DTYPE_OVERRIDES: dict[str, str] = {
    "player_id": "Int64",
    "team_id": "Int64",
    "jersey_number": "Int64",
    "home_team_id": "Int64",
    "away_team_id": "Int64",
    "competition_id": "Int64",
    "season_id": "Int64",
    "pitch_length": "Int64",
    "pitch_width": "Int64",
}


def parse_match_json(source: str, *, match_id: str) -> pd.DataFrame:
    """Parse match.json content into a roster-format DataFrame.

    Produces one row per player-match with match-level metadata
    denormalized onto every row.

    Args:
        source: JSON string content of the match.json artifact.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").

    Returns:
        DataFrame in roster format per spec section 5.3.
    """
    data = json.loads(source)

    home_team = data["home_team"]
    away_team = data["away_team"]
    comp_edition = data["competition_edition"]
    competition = comp_edition["competition"]
    season = comp_edition["season"]

    # Build team_id -> team info lookup
    team_info: dict[int, dict[str, str]] = {
        home_team["id"]: {"name": home_team["name"], "short_name": home_team.get("short_name", "")},
        away_team["id"]: {"name": away_team["name"], "short_name": away_team.get("short_name", "")},
    }

    period_boundaries = json.dumps(data.get("match_periods", []))

    rows: list[dict[str, object]] = []
    for player in data.get("players", []):
        tid = player["team_id"]
        team = team_info.get(tid, {"name": "Unknown", "short_name": ""})
        role = player.get("player_role") or {}

        rows.append(
            {
                "match_id": match_id,
                "player_id": player["id"],
                "team_id": tid,
                "player_name": player.get("short_name", ""),
                "first_name": player.get("first_name", ""),
                "last_name": player.get("last_name", ""),
                "jersey_number": player.get("number"),
                "position_name": role.get("name", ""),
                "position_acronym": role.get("acronym", ""),
                "team_name": team["name"],
                "team_short_name": team["short_name"],
                "home_team_id": home_team["id"],
                "away_team_id": away_team["id"],
                "competition_id": competition["id"],
                "competition_name": competition["name"],
                "season_id": season["id"],
                "season_name": season["name"],
                "match_date": data.get("date_time", ""),
                "stadium_name": data.get("stadium", {}).get("name", ""),
                "pitch_length": data.get("pitch_length"),
                "pitch_width": data.get("pitch_width"),
                "period_boundaries": period_boundaries,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in _MATCHES_DTYPE_OVERRIDES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_matches(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed matches DataFrame to bronze.skillcorner_matches."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "player_id", "team_id"],
        "skillcorner_matches",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_matches",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_skillcorner_matches.py -v`
Expected: PASS (all 8 tests)

- [ ] **Step 5: Run pyright + ruff**

Run: `uv run pyright src/ingestion/skillcorner_matches.py && uv run ruff check src/ingestion/skillcorner_matches.py`
Expected: 0 errors

---

### Task 6: Orchestrator — Rewrite `skillcorner.py`

**Files:**
- Modify: `src/ingestion/skillcorner.py` (complete rewrite)

- [ ] **Step 1: Rewrite the orchestrator**

Replace the entire contents of `src/ingestion/skillcorner.py`:

```python
"""SkillCorner A-League ingestion orchestrator.

Discovers new/modified matches via the pining-for-the-data REST API,
downloads events + tracking + match metadata, and writes to bronze.

Bronze tables produced:
  - skillcorner_matches  (roster format: one row per player per match)
  - skillcorner_events   (raw dynamic_events CSV, all 294+ source columns)
  - skillcorner_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  SkillCorner center-origin meters. Staging transforms to 120x80.
"""

from __future__ import annotations

import gc
import io
import logging
import os
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.skillcorner_common import PROVIDER, MatchInfo, fetch_artifact, fetch_match_list, resolve_pining_token
from ingestion.skillcorner_events import parse_events_csv, write_events
from ingestion.skillcorner_matches import parse_match_json, write_matches
from ingestion.skillcorner_tracking import parse_tracking_jsonl, write_tracking
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    tolerate_missing_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _SkillcornerGuard:
    workflow_id = "wf-skillcorner"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Discover new/modified SkillCorner matches via API.

        Queries MAX(_ingested_at) from bronze.skillcorner_events to determine
        the updatedSince cutoff. Calls the discovery API. Returns match count.
        """
        import logging as _logging
        from datetime import datetime, timezone

        _guard_logger = _logging.getLogger(__name__)
        token = resolve_pining_token()

        # Determine last ingested timestamp
        updated_since: str | None = None
        with tolerate_missing_table(_guard_logger, "SkillCorner events table missing -- full ingestion needed"):
            from pyspark.sql import functions as F

            row = (
                spark.table(f"{catalog}.bronze.skillcorner_events")
                .select(F.max("_ingested_at").alias("max_ts"))
                .collect()[0]
            )
            if row["max_ts"] is not None:
                ts: datetime = row["max_ts"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                updated_since = ts.isoformat()

        matches = fetch_match_list(token, updated_since=updated_since)
        if not matches:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(matches),
            metadata={"matches": [m.model_dump() for m in matches]},
        )


skip_guard = _SkillcornerGuard()


def ingest_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    matches: list[MatchInfo],
) -> None:
    """Download and ingest SkillCorner data for discovered matches.

    Processing order per match: matches -> events -> tracking.
    Matches first because downstream SPADL needs roster data.
    """
    token = resolve_pining_token()

    for i, match in enumerate(matches):
        mid = match.id
        logger.info(
            "Processing SkillCorner match %s (%d/%d): %s vs %s",
            mid, i + 1, len(matches), match.home, match.away,
        )

        # 1. Match metadata (needed by SPADL conversion)
        match_resp = fetch_artifact(mid, f"{mid}_match", token)
        match_df = parse_match_json(match_resp.text, match_id=mid)
        write_matches(spark, match_df, catalog, schema, mid, logger)
        logger.info("Wrote %d roster rows for match %s", len(match_df), mid)

        # 2. Events (needed by SPADL conversion)
        events_resp = fetch_artifact(mid, f"{mid}_dynamic_events", token)
        events_df = parse_events_csv(io.StringIO(events_resp.text), match_id=mid)
        write_events(spark, events_df, catalog, schema, mid, logger)
        logger.info("Wrote %d event rows for match %s", len(events_df), mid)

        # 3. Tracking (JSONL -- stream to temp file to avoid holding full response in memory)
        tracking_resp = fetch_artifact(mid, f"{mid}_tracking_extrapolated", token, stream=True)
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".jsonl", delete=False) as tmp:
            for chunk in tracking_resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        try:
            tracking_df = parse_tracking_jsonl(tmp_path, match_id=mid)
            write_tracking(spark, tracking_df, catalog, schema, mid, logger)
            logger.info("Wrote %d tracking rows for match %s", len(tracking_df), mid)
        finally:
            os.unlink(tmp_path)

        del match_df, events_df, tracking_df
        gc.collect()

    logger.info("SkillCorner ingestion complete: %d matches processed", len(matches))


@workflow("wf-skillcorner", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest SkillCorner A-League match data from pining-for-the-data API."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    # Reconstruct MatchInfo objects from guard metadata
    raw_matches = filter_result.metadata.get("matches", [])  # type: ignore[union-attr]
    matches = [MatchInfo.model_validate(m) for m in raw_matches]

    ingest_skillcorner(spark, catalog, schema, logger, matches)
    return 0


def main() -> None:
    """CLI entry point for SkillCorner data ingestion."""
    args = parse_ingestion_args("Ingest SkillCorner A-League data into the bronze layer")
    logger = configure_logging("skillcorner")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting SkillCorner ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("SkillCorner ingestion complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run pyright + ruff**

Run: `uv run pyright src/ingestion/skillcorner.py && uv run ruff check src/ingestion/skillcorner.py`
Expected: 0 errors

- [ ] **Step 3: Verify entry point in pyproject.toml**

The existing entry point `ingest_skillcorner = "ingestion.skillcorner:main"` at `pyproject.toml:112` is unchanged — same module path, same `main()` function. No edit needed.

---

### Task 7: SPADL Conversion — UDF + Converter + Dispatch

**Files:**
- Modify: `src/ingestion/spadl_conversion.py` (add functions after Metrica section, ~line 1443)
- Modify: `src/ingestion/spadl_enrichments.py:27` (add "skillcorner" to `_VALID_SOURCES`)
- Modify: `src/ingestion/spadl_vaep.py` (add import + dispatch call)
- Create: `src/tests/test_skillcorner_spadl.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_skillcorner_spadl.py`:

```python
"""Unit tests for SkillCorner SPADL UDF closure."""

from __future__ import annotations

import pandas as pd
import pytest


class TestSkillCornerSpadlUdf:
    """Test _make_skillcorner_spadl_udf closure logic (no Spark)."""

    def _make_fixture_events(self) -> pd.DataFrame:
        """Minimal events DataFrame matching bronze.skillcorner_events shape."""
        return pd.DataFrame(
            {
                "event_id": ["1_0", "2_0"],
                "event_type": ["pass", "reception"],
                "event_subtype": ["short_pass", ""],
                "player_id": [38673, 44001],
                "team_id": [4177, 4177],
                "period": [1, 1],
                "time_start": ["00:01.2", "00:03.4"],
                "time_end": ["00:03.4", "00:04.0"],
                "x_start": [10.5, 20.1],
                "y_start": [-5.2, 3.4],
                "x_end": [20.1, 25.0],
                "y_end": [3.4, 5.0],
                "game_interruption_before": [None, None],
                "game_interruption_after": [None, None],
                "end_type": ["successful", "successful"],
                "start_type": ["", ""],
                "match_id": ["1886347", "1886347"],
            }
        )

    def _make_match_metadata(self) -> dict:
        return {
            "id": "1886347",
            "pitch_length": 105,
            "pitch_width": 68,
            "home_team": {"id": 4177},
        }

    def test_udf_produces_spadl_columns(self) -> None:
        """UDF output must have all 40 SPADL schema columns."""
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert "data_source" in result.columns
        assert "match_id_native" in result.columns
        assert "player_id_native" in result.columns
        assert "team_id_native" in result.columns
        assert result["data_source"].iloc[0] == "skillcorner"

    def test_udf_uses_adr018_generators(self) -> None:
        """Native IDs must use ADR-018 canonical generators, not bare str()."""
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        # match_id_native must be the canonical format (pure numeric string)
        mid_native = result["match_id_native"].iloc[0]
        assert mid_native == "1886347"
        assert not mid_native.startswith("skillcorner_")

    def test_udf_null_fills_statsbomb_columns(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert result["statsbomb_possession_id"].isna().all()
        assert result["statsbomb_play_pattern"].isna().all()

    def test_udf_null_fills_tackle_qualifiers(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert result["tackle_winner_player_id_native"].isna().all()

    def test_udf_applies_enrichments(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_spadl_udf

        match_metadata = self._make_match_metadata()
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)
        events = self._make_fixture_events()
        result = udf_fn(events)

        assert "possession_id_heuristic" in result.columns
        assert "gk_role" in result.columns


class TestSkillCornerReplaceWhere:
    def test_replace_where_format(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_replace_where

        result = _make_skillcorner_replace_where([123456, 789012])
        assert "data_source = 'skillcorner'" in result
        assert "123456" in result
        assert "789012" in result

    def test_replace_where_rejects_empty(self) -> None:
        from ingestion.spadl_conversion import _make_skillcorner_replace_where

        with pytest.raises(ValueError):
            _make_skillcorner_replace_where([])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_skillcorner_spadl.py -v`
Expected: FAIL — `ImportError: cannot import name '_make_skillcorner_spadl_udf'`

- [ ] **Step 3: Add "skillcorner" to _VALID_SOURCES**

In `src/ingestion/spadl_enrichments.py:27`, change:

```python
_VALID_SOURCES: Final[frozenset[str]] = frozenset({"statsbomb", "wyscout", "idsse", "metrica"})
```

to:

```python
_VALID_SOURCES: Final[frozenset[str]] = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner"})
```

Also update the docstring of `apply_spadl_enrichments` (line 49-50) to include `"skillcorner"` in the valid sources list.

- [ ] **Step 4: Implement the SPADL functions**

Add to `src/ingestion/spadl_conversion.py` after the Metrica section (after `_convert_metrica_from_bronze`, approximately line 1443):

```python
# ---------------------------------------------------------------------------
# SkillCorner SPADL conversion
# ---------------------------------------------------------------------------


def _make_skillcorner_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific SkillCorner matches."""
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'skillcorner' AND match_id IN ({ids_sql})"


def _make_skillcorner_spadl_udf(*, match_metadata: dict[str, object]) -> object:
    """Build the applyInPandas UDF closure for SkillCorner SPADL conversion.

    The silly-kicks SkillCorner converter API differs from other providers:
    - Takes (events, match_metadata) instead of (events, home_team_id)
    - Uses POSSESSION_PERSPECTIVE convention (to_spadl_ltr is a no-op)
    - No home_team_start_left kwarg needed

    Args:
        match_metadata: Dict with keys "id", "pitch_length", "pitch_width",
            "home_team" (nested: {"id": int}). Built driver-side from
            bronze.skillcorner_matches. Captured in closure for executors.
    """
    _match_meta = match_metadata  # frozen closure capture

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one SkillCorner match's events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import hash_native_id_to_bigint as _hash_id

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

        import silly_kicks.spadl.skillcorner as _spadl_sc

        match_id_str = str(pdf["match_id"].iloc[0])

        try:
            actions, _report = _spadl_sc.convert_to_actions(pdf, _match_meta)
        except Exception as exc:
            msg = f"SkillCorner SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            # NOTE: `logger` here is captured from the outer scope. Inside an applyInPandas
            # UDF, Python logging routes to executor stderr (visible in Spark driver logs),
            # NOT the structured JSON pipeline logger. This is acceptable for diagnostics.
            logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

        # ADR-018: native IDs via canonical generators
        from shared.identifiers import (
            skillcorner_native_match_id,
            skillcorner_native_player_id,
            skillcorner_native_team_id,
        )

        actions["team_id_native"] = actions["team_id"].apply(
            lambda tid: skillcorner_native_team_id(tid) if _pd.notna(tid) else _pd.NA
        ).astype("string")

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives,
            apply_player_id_native,
            cast_enrichment_dtypes,
            null_fill_statsbomb_columns,
            null_fill_tackle_qualifiers,
        )

        # player_id_native MUST precede legacy BIGINT NULL-fill
        actions = apply_player_id_native(actions, source="skillcorner")

        # Hash match_id for legacy BIGINT; NULL-fill other legacy BIGINTs
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "skillcorner"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="skillcorner")
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        actions = null_fill_statsbomb_columns(actions, n=n)
        actions = cast_enrichment_dtypes(actions)
        actions = apply_match_level_natives(
            actions,
            home_team_id_native=str(_match_meta["home_team"]["id"]),  # type: ignore[index]
            competition_native_id=_pd.NA,  # SkillCorner has no competition_native_id in events
            season_native_id=_pd.NA,
            match_id_native=skillcorner_native_match_id(match_id_str),
        )
        actions = null_fill_tackle_qualifiers(actions, n=n)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_skillcorner_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read SkillCorner events from bronze, convert to SPADL, write Delta.

    Unlike IDSSE/Metrica, the SkillCorner converter needs a match_metadata
    dict built from bronze.skillcorner_matches. This is resolved driver-side
    per match, then captured in the UDF closure.

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

    events_table = f"{catalog}.{schema}.skillcorner_events"
    matches_table = f"{catalog}.{schema}.skillcorner_matches"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read SkillCorner events bronze table")
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    new_match_ids: list[str] = [
        mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches
    ]

    if not new_match_ids:
        logger.info("SkillCorner: all %d matches already converted -- skipping", len(all_match_ids))
        return False

    logger.info("SkillCorner: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    wrote_any = False
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

    # Per-match processing: each match needs its own match_metadata dict.
    # TRADEOFF: This loops N applyInPandas calls (one per match) instead of
    # batching all matches in a single groupBy("match_id").applyInPandas like
    # IDSSE/Metrica. The overhead is ~1-2s Spark job-submission latency per match.
    # Acceptable because: (a) A-League has ~27 matches/season, not thousands;
    # (b) each match needs a unique match_metadata dict in the closure; (c)
    # batching would require a UDF that dispatches on match_id at runtime,
    # which is more complex for negligible gain at this scale.
    for mid in new_match_ids:
        # Build match_metadata from bronze.skillcorner_matches (driver-side)
        matches_pdf = (
            spark.table(matches_table)
            .filter(spark_fn.col("match_id") == mid)
            .select("match_id", "pitch_length", "pitch_width", "home_team_id")
            .limit(1)
            .toPandas()
        )

        if matches_pdf.empty:
            logger.warning("SkillCorner: no match metadata for %s -- skipping SPADL", mid)
            continue

        row = matches_pdf.iloc[0]
        match_metadata: dict[str, object] = {
            "id": str(row["match_id"]),
            "pitch_length": int(row["pitch_length"]),
            "pitch_width": int(row["pitch_width"]),
            "home_team": {"id": int(row["home_team_id"])},
        }

        # Build UDF with this match's metadata
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)

        # Filter events for this match
        match_events_sdf = events_sdf.filter(spark_fn.col("match_id") == mid)

        spadl_sdf = match_events_sdf.groupBy("match_id").applyInPandas(
            udf_fn,  # type: ignore[arg-type]
            schema=spadl_schema,
        )

        hashed_id = hash_native_id_to_bigint(mid)
        write_delta_table(
            spadl_sdf,
            catalog,
            schema,
            _SPADL_TABLE,
            replace_where=_make_skillcorner_replace_where([hashed_id]),
            logger=logger,
        )
        wrote_any = True
        logger.info("SkillCorner: SPADL conversion complete for match %s", mid)

    return wrote_any
```

- [ ] **Step 5: Wire dispatch in spadl_vaep.py**

In `src/ingestion/spadl_vaep.py`, add to the import block (after line 34):

```python
from ingestion.spadl_conversion import _convert_skillcorner_from_bronze
```

In the dispatch section (after line 691, the `metrica_wrote = ...` call):

```python
    sc_wrote = _convert_skillcorner_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
```

Update the `if not (...)` check at line 693 to include `sc_wrote`:

```python
    if not (sb_wrote or ws_wrote or idsse_wrote or metrica_wrote or sc_wrote) and not existing_spadl_matches:
```

Update the error message to include SkillCorner:

```python
        msg = "No SPADL actions produced from any source (StatsBomb / Wyscout / IDSSE / Metrica / SkillCorner)"
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_skillcorner_spadl.py -v`
Expected: PASS

**NOTE:** The `test_udf_produces_spadl_columns` test may fail if the silly-kicks converter rejects the minimal 2-row fixture. In that case, the implementer should download a real events subset from the API (e.g., first 50 rows of match 1886347) and use that as the fixture instead. See Task 13 (E2E test) for fixture creation.

- [ ] **Step 7: Run pyright + ruff**

Run: `uv run pyright src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/spadl_enrichments.py`
Expected: 0 errors

---

### ~~Task 8: Tracking Context~~ — REMOVED

> **Dropped:** `spadl_tracking_context` is half-baked and not worth investing in.
> The `NotImplementedError` in `_resolve_enrichment_identity` stays as-is.
> SkillCorner matches will not get tracking context enrichment.
> No code changes to `tracking_context.py` in this PR.

---

### Task 9: Deprecation — Remove kloppy, Old Table, tracking_metadata SkillCorner Section

**Files:**
- Modify: `src/ingestion/tracking_metadata.py` (remove SkillCorner section)
- Modify: `pyproject.toml` (remove kloppy)
- Create: `scripts/migrations/2026-05-16_drop_old_skillcorner_tracking.sql`

- [ ] **Step 1: Remove SkillCorner from tracking_metadata.py**

Remove the following:
1. `_SKILLCORNER_MATCH_IDS` constant (lines 110-121)
2. `_extract_skillcorner_metadata` function (lines 239-301)
3. SkillCorner branch in guard `check()` (lines 72-80): remove `skillcorner_ids` variable and `skillcorner_source` logic. Update `all_ids` to only use `idsse_ids`.

After removal, the guard's `check()` should look like:

```python
        # Check IDSSE matches
        idsse_ids: list[str] = []
        idsse_source = f"{catalog}.bronze.idsse_tracking"
        if spark.catalog.tableExists(idsse_source):
            idsse_ids = find_new_ids(
                spark,
                source_table=idsse_source,
                results_table=results_table,
            )

        if not idsse_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(idsse_ids),
            metadata={"new_match_ids": idsse_ids},
        )
```

Also remove the `_extract_skillcorner_metadata` call in the pipeline function (search for where it's called in the `run_pipeline` or `_run_tracking_metadata_pipeline` function and remove that branch).

- [ ] **Step 2: Remove kloppy extra from pyproject.toml**

In `pyproject.toml`, kloppy is NOT a direct dependency — it's an optional extra of silly-kicks. Change:
```toml
    "silly-kicks[kloppy,das]>=3.15.1,<4",
```
to:
```toml
    "silly-kicks[das]>=3.15.1,<4",
```

Also remove the ruff per-file-ignores comment referencing kloppy at `src/ingestion/tracking_metadata.py` and the NOTE comment about kloppy running in a separate environment (search for "kloppy" in pyproject.toml).

- [ ] **Step 3: Update Terraform — switch environment_key + remove kloppy env**

In `terraform/modules/workflows/main.tf`:

1. **Change `ingest_skillcorner` environment** (line 875):
```hcl
    environment_key = "default"
```
(Was `"tracking"` which depended on kloppy.)

2. **Change `extract_tracking_metadata` environment** (line 707):
```hcl
    environment_key = "default"
```
(Was `"tracking"` — after removing the SkillCorner kloppy section from tracking_metadata.py, this task only uses stdlib + pandas + project internals.)

3. **Delete the "tracking" environment block entirely** (lines 1183-1195):
```hcl
  # ── Environment for SkillCorner tracking task (kloppy for open data download)
  environment {
    environment_key = "tracking"
    ...
  }
```
No other task uses this environment (verified: only `ingest_skillcorner` + `extract_tracking_metadata`).

4. **Update comment on `extract_tracking_metadata`** (lines 681-683):
```hcl
  # ── Task: Extract tracking player metadata ─────────────────────────────
  # Reads IDSSE DFL match info XMLs to populate tracking_player_metadata
  # bronze table with player/team names.
```
(Remove "and SkillCorner kloppy metadata" from the comment.)

- [ ] **Step 4: Verify token resolution + provision secret**

`resolve_pining_token()` was already implemented in Task 2 (`skillcorner_common.py`). The orchestrator (`skillcorner.py`) imports and calls it in both the guard and ingestor. Confirm it works:

Run: `PINING_FOR_THE_DATA_TOKEN=test python -c "from ingestion.skillcorner_common import resolve_pining_token; assert resolve_pining_token() == 'test'"`
Expected: No error.

**Operator prerequisite (one-time, before first DAG run):**
```bash
databricks secrets create-scope --scope pining
databricks secrets put-secret --scope pining --key token
# Paste the pining-for-the-data Bearer token when prompted
```

- [ ] **Step 5: Create migration script to drop old bronze table**

Create `scripts/drop_old_skillcorner_tracking.sql` (operator-driven, NOT in `scripts/migrations/` auto-apply tree — this is a destructive DROP):

```sql
-- Drop the old kloppy-sourced bronze.skillcorner_tracking table.
-- This table is replaced by the new pining-for-the-data API source.
-- The new table (same name) is created by skillcorner_tracking.py.
--
-- This migration is idempotent: DROP TABLE IF EXISTS is a no-op if
-- the table was already dropped or never existed.
--
-- Run manually BEFORE the first new DAG execution:
--   databricks sql-cli execute --statement "$(cat scripts/drop_old_skillcorner_tracking.sql)"
DROP TABLE IF EXISTS ${catalog}.bronze.skillcorner_tracking;
```

**NOTE:** Check the migrations runner syntax for `${catalog}` variable substitution. If the runner uses a different pattern (e.g., Jinja `{{ catalog }}`), adjust accordingly.

- [ ] **Step 6: Verify kloppy is fully gone**

Run: `uv run ruff check src/ && grep -r "kloppy" src/ --include="*.py" | grep -v "__pycache__" | grep -v ".pyc"`
Expected: Zero references to kloppy in any Python source file under `src/`.

- [ ] **Step 7: Run pyright on tracking_metadata.py**

Run: `uv run pyright src/ingestion/tracking_metadata.py`
Expected: 0 errors

---

### Task 10: dbt — Staging, Sources, Dimensions

**Files:**
- Create: `dbt_project/models/staging/skillcorner/stg_skillcorner__events.sql`
- Create: `dbt_project/models/staging/skillcorner/stg_skillcorner__matches.sql`
- Modify: `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql`
- Modify: `dbt_project/models/staging/skillcorner/_skillcorner__sources.yml`
- Modify: `dbt_project/models/staging/skillcorner/_skillcorner__models.yml`
- Modify: `dbt_project/models/dimensions/dim_teams.sql`
- Modify: `dbt_project/models/dimensions/dim_players.sql`
- Modify: `dbt_project/models/dimensions/dim_matches.sql`

This task is dbt-only. The implementer should:

- [ ] **Step 1: Read existing dbt files**

Read the current `stg_skillcorner__tracking.sql`, `_skillcorner__sources.yml`, `_skillcorner__models.yml`, and all 3 `dim_*.sql` files to understand the existing CTE structure and SkillCorner branches.

- [ ] **Step 2: Update `_skillcorner__sources.yml`**

Add source table definitions for `skillcorner_events` and `skillcorner_matches` alongside the existing `skillcorner_tracking`. Update the `skillcorner_tracking` column list to match the new bronze schema (remove `team`, `home_team_id`, `away_team_id`, `ball_state`, `ball_owning_team_id`, `is_goalkeeper`, `position_name`; add `ball_z`, `ball_is_detected`).

- [ ] **Step 3: Create `stg_skillcorner__events.sql`**

Minimal staging — source ref + rename any columns per convention:

```sql
with source as (
    select * from {{ source('skillcorner', 'skillcorner_events') }}
)

select * from source
```

- [ ] **Step 4: Create `stg_skillcorner__matches.sql`**

```sql
with source as (
    select * from {{ source('skillcorner', 'skillcorner_matches') }}
)

select
    match_id,
    player_id,
    team_id,
    player_name,
    first_name,
    last_name,
    jersey_number,
    position_name,
    position_acronym,
    team_name,
    team_short_name,
    home_team_id,
    away_team_id,
    competition_id,
    competition_name,
    season_id,
    season_name,
    match_date,
    stadium_name,
    pitch_length,
    pitch_width,
    period_boundaries,
    _ingested_at
from source
```

- [ ] **Step 5: Rewrite `stg_skillcorner__tracking.sql`**

The key change: no more `team` column in bronze. JOIN to `stg_skillcorner__matches` to get `team_id`, `home_team_id`, `away_team_id`. Coordinate transform unchanged. `match_id` is now raw native (no `skillcorner_` prefix). `is_visible` column name preserved.

The implementer should read the current staging SQL and adjust the JOINs and column references.

- [ ] **Step 6: Update `_skillcorner__models.yml`**

Add model definitions for `stg_skillcorner__events` and `stg_skillcorner__matches`. Update `stg_skillcorner__tracking` column descriptions.

- [ ] **Step 7: Add SkillCorner branches to dim_teams.sql, dim_players.sql, dim_matches.sql**

Each dim model has provider-specific CTEs that UNION ALL together. Add SkillCorner CTEs sourcing from `stg_skillcorner__matches`:

**dim_teams.sql** — SkillCorner CTE:
```sql
skillcorner_teams as (
    select distinct
        team_id as native_team_id,
        team_name,
        team_short_name,
        'skillcorner' as data_source
    from {{ ref('stg_skillcorner__matches') }}
),
```

**dim_players.sql** — SkillCorner CTE:
```sql
skillcorner_players as (
    select distinct
        cast(player_id as string) as native_player_id,
        player_name as display_name,
        first_name,
        last_name,
        jersey_number,
        position_name,
        'skillcorner' as data_source
    from {{ ref('stg_skillcorner__matches') }}
),
```

**dim_matches.sql** — SkillCorner CTE:
```sql
skillcorner_matches as (
    select distinct
        match_id as native_match_id,
        match_date,
        competition_name,
        season_name,
        stadium_name,
        home_team_id,
        away_team_id,
        cast(competition_id as string) as native_competition_id,
        cast(season_id as string) as native_season_id,
        'skillcorner' as data_source
    from {{ ref('stg_skillcorner__matches') }}
),
```

Then add each CTE to the UNION ALL in the final select.

- [ ] **Step 8: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select tag:skillcorner+`
Expected: Compiles without errors

---

### Task 11: Workflow Card Update

**Files:**
- Modify: `workflow-cards/wf-skillcorner.yaml`

- [ ] **Step 1: Rewrite workflow card**

Replace the entire contents of `workflow-cards/wf-skillcorner.yaml` with the YAML from spec section 10 (the full workflow card). See the spec for the complete content.

- [ ] **Step 2: Verify YAML syntax**

Run: `python -c "import yaml; yaml.safe_load(open('workflow-cards/wf-skillcorner.yaml'))" && echo OK`
Expected: `OK`

---

### Task 12: Boundary Tests + Source Onboarding

**Files:**
- Modify: `src/tests/test_silly_kicks_boundary.py`
- Modify: `src/tests/test_source_onboarding_contracts.py`
- Create: `src/tests/fixtures/silly_kicks_boundary/sc_match_1886347.parquet`
- Create: `src/tests/fixtures/silly_kicks_boundary/sc_match_1886347_meta.json`

- [ ] **Step 1: Download and create fixtures**

The implementer needs to download real data from the API for fixtures:

```python
# Run interactively or as a script to create fixtures
import os
import json
import pandas as pd
from ingestion.skillcorner_common import fetch_artifact

token = os.environ["PINING_FOR_THE_DATA_TOKEN"]

# Events fixture
events_resp = fetch_artifact("1886347", "1886347_dynamic_events", token)
events_df = pd.read_csv(io.StringIO(events_resp.text), low_memory=False)
events_df.to_parquet("src/tests/fixtures/silly_kicks_boundary/sc_match_1886347.parquet")

# Match metadata fixture
match_resp = fetch_artifact("1886347", "1886347_match", token)
with open("src/tests/fixtures/silly_kicks_boundary/sc_match_1886347_meta.json", "w") as f:
    json.dump(match_resp.json(), f, indent=2)
```

**IMPORTANT:** While creating the match fixture, inspect `match_resp.json()["players"]` to verify the exact GK role string:
```python
for p in match_resp.json()["players"]:
    if p.get("player_role", {}).get("acronym") == "GK":
        print(f"GK role name: {p['player_role']['name']!r}")
```
If it's not `"Goalkeeper"`, update the `player_gk_map` predicate in the SPADL converter (Task 7) accordingly.

Also create E2E test fixtures (smaller subsets):
```python
# Events subset (~50 rows)
events_df.head(50).to_csv("src/tests/fixtures/skillcorner/events_subset.csv", index=False)

# Match metadata (full — it's small)
with open("src/tests/fixtures/skillcorner/match.json", "w") as f:
    json.dump(match_resp.json(), f, indent=2)

# Tracking subset (~20 frames)
tracking_resp = fetch_artifact("1886347", "1886347_tracking_extrapolated", token)
lines = tracking_resp.text.strip().split("\n")[:20]
with open("src/tests/fixtures/skillcorner/tracking_subset.jsonl", "w") as f:
    f.write("\n".join(lines))
```

- [ ] **Step 2: Add 5th source to boundary test**

In `src/tests/test_silly_kicks_boundary.py`, add to `_PARAMETRIZE` (around line 31-42):

```python
("skillcorner", "sc_match_1886347.parquet"),
```

Add `_adapt_input` branch:

```python
if source == "skillcorner":
    return df, None
```

Add `_call_converter` branch:

```python
if source == "skillcorner":
    import json
    import silly_kicks.spadl.skillcorner as _sc
    meta_path = _FIXTURE_DIR / "sc_match_1886347_meta.json"
    with open(meta_path) as f:
        match_metadata = json.load(f)
    return _sc.convert_to_actions(df, match_metadata)
```

- [ ] **Step 3: Add SkillCorner to source onboarding contracts**

In `src/tests/test_source_onboarding_contracts.py`, add `"skillcorner"` to the parametrization (lines 19-27) and add the UDF map entry:

```python
"skillcorner": _make_skillcorner_spadl_udf,
```

(The implementer needs to check the exact test structure and adjust — the UDF factory needs `match_metadata` which isn't available in the parametrized fixture. May need a fixture-specific wrapper.)

- [ ] **Step 4: Run boundary tests**

Run: `uv run pytest src/tests/test_silly_kicks_boundary.py -v`
Expected: PASS (5 sources x invariants)

---

### Task 13: E2E Integration Test

**Files:**
- Create: `src/tests/test_skillcorner_e2e.py`

- [ ] **Step 1: Create E2E test**

Create `src/tests/test_skillcorner_e2e.py`:

```python
"""End-to-end integration test for SkillCorner ingestion pipeline (no Spark).

Tests the full flow: parse events/tracking/matches -> SPADL conversion.
Uses fixture subsets of match 1886347.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "skillcorner"


@pytest.fixture
def match_metadata() -> dict:
    with open(_FIXTURE_DIR / "match.json") as f:
        return json.load(f)


@pytest.fixture
def events_df() -> pd.DataFrame:
    return pd.read_csv(_FIXTURE_DIR / "events_subset.csv", low_memory=False)


@pytest.fixture
def tracking_df() -> pd.DataFrame:
    from ingestion.skillcorner_tracking import parse_tracking_jsonl

    return parse_tracking_jsonl(str(_FIXTURE_DIR / "tracking_subset.jsonl"), match_id="1886347")


@pytest.fixture
def matches_df(match_metadata: dict) -> pd.DataFrame:
    from ingestion.skillcorner_matches import parse_match_json

    return parse_match_json(json.dumps(match_metadata), match_id="1886347")


class TestSkillCornerE2E:
    def test_events_parse(self, events_df: pd.DataFrame) -> None:
        assert len(events_df) > 0
        assert "event_id" in events_df.columns
        assert "event_type" in events_df.columns

    def test_tracking_parse(self, tracking_df: pd.DataFrame) -> None:
        assert len(tracking_df) > 0
        assert "player_id" in tracking_df.columns
        assert "timestamp" in tracking_df.columns
        assert tracking_df["timestamp"].dtype == "Float64"
        assert "is_visible" in tracking_df.columns
        assert "is_detected" not in tracking_df.columns

    def test_matches_parse(self, matches_df: pd.DataFrame) -> None:
        assert len(matches_df) > 0
        assert "player_id" in matches_df.columns
        assert "position_name" in matches_df.columns
        assert "pitch_length" in matches_df.columns

    def test_spadl_conversion(self, events_df: pd.DataFrame, match_metadata: dict) -> None:
        import silly_kicks.spadl.skillcorner as sc

        events_df["match_id"] = "1886347"
        actions, report = sc.convert_to_actions(events_df, match_metadata)

        assert len(actions) > 0
        assert "type_id" in actions.columns
        assert "start_x" in actions.columns

        # Apply enrichments
        from ingestion.spadl_enrichments import apply_spadl_enrichments

        enriched = apply_spadl_enrichments(actions, source="skillcorner")
        assert "possession_id_heuristic" in enriched.columns
        assert "gk_role" in enriched.columns

    # NOTE: test_tracking_context_frames REMOVED — tracking context not in scope

    def test_identity_columns(self, events_df: pd.DataFrame, match_metadata: dict) -> None:
        """SPADL actions must have correct identity columns after full UDF logic."""
        import silly_kicks.spadl.skillcorner as sc

        events_df["match_id"] = "1886347"
        actions, _ = sc.convert_to_actions(events_df, match_metadata)

        from shared.identifiers import skillcorner_native_match_id, skillcorner_native_team_id

        # Verify native ID generators work on actual data
        for tid in actions["team_id"].dropna().unique():
            validated = skillcorner_native_team_id(tid)
            assert validated == str(tid)

        validated_mid = skillcorner_native_match_id("1886347")
        assert validated_mid == "1886347"
```

- [ ] **Step 2: Run E2E tests**

Run: `uv run pytest src/tests/test_skillcorner_e2e.py -v`
Expected: PASS (all 6 tests)

---

### Task 14: Final Validation + Version Bump

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=120 2>&1 | head -100`
Expected: No NEW failures (pre-existing failures per `project_known_pretest_failures_on_main_2026_05_04.md` are acceptable)

- [ ] **Step 2: Run pyright on all modified files**

Run: `uv run pyright src/ingestion/skillcorner.py src/ingestion/skillcorner_common.py src/ingestion/skillcorner_events.py src/ingestion/skillcorner_tracking.py src/ingestion/skillcorner_matches.py src/ingestion/spadl_conversion.py src/ingestion/spadl_enrichments.py src/ingestion/spadl_vaep.py src/ingestion/tracking_metadata.py src/shared/identifiers.py`
Expected: 0 errors

- [ ] **Step 3: Run ruff**

Run: `uv run ruff check src/ && uv run ruff format --check src/`
Expected: 0 violations

- [ ] **Step 4: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`
Expected: Version bumped (never edit pyproject.toml `version =` manually)

- [ ] **Step 5: Commit (awaiting user approval)**

Stage all new + modified files. Proposed commit message:

```
feat: SkillCorner ingestion rewrite — pining-for-the-data API + SPADL

Replace kloppy-based tracking-only ingestion with full events + tracking +
match metadata from the pining-for-the-data REST API. Wire SPADL conversion
via silly-kicks SkillCorner converter.

- 5 new modules: skillcorner_common, _events, _tracking, _matches, orchestrator
- ADR-018 identifier generators for SkillCorner (identifiers.py)
- SPADL UDF with driver-side match_metadata dict (spadl_conversion.py)
- Drop kloppy dependency, remove old bronze table
- dbt staging + dimension resolution for SkillCorner
- E2E integration test + boundary test 5th source
```
