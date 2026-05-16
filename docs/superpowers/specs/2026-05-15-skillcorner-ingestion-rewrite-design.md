# SkillCorner Ingestion Rewrite — Design Spec

**Date:** 2026-05-15
**Status:** APPROVED (rev 3 — second review fixes)
**Scope:** Replace kloppy-based SkillCorner tracking-only ingestion with full events + tracking + match metadata ingestion from the pining-for-the-data REST API, wire SPADL conversion via silly-kicks, and unblock the tracking context pipeline.

## 1. Context & Motivation

The lakehouse currently ingests SkillCorner data via **kloppy** from the SkillCorner GitHub open data repository. This path has three fundamental limitations:

1. **Tracking only** — no events data. `bronze.skillcorner_events` does not exist.
2. **No SPADL conversion** — `tracking_context.py:549` raises `NotImplementedError` because no SPADL actions exist for SkillCorner.
3. **Identity mismatch** — kloppy tracking uses `"home"/"away"` team strings while `home_team_id` is a kloppy numeric ID (e.g., `"31"`), blocking tracking context identity resolution.

The **pining-for-the-data** REST API now serves all 4 SkillCorner artifacts per match (`dynamic_events.csv`, `match.json`, `tracking_extrapolated.jsonl`, `phases_of_play.csv`), and **silly-kicks** has shipped the SkillCorner SPADL converter (`silly_kicks.spadl.skillcorner.convert_to_actions`). This spec designs the lakehouse-side integration that connects these two.

### Success Criteria

- All available SkillCorner matches ingested into 3 bronze tables (events, tracking, matches)
- SPADL actions produced for all matches (`data_source = 'skillcorner'` in `bronze.spadl_actions`)
- Tracking context pipeline processes SkillCorner matches (no longer skipped)
- `dim_teams` / `dim_players` / `dim_matches` resolve SkillCorner native IDs to real names
- Old kloppy-sourced `bronze.skillcorner_tracking` dropped
- No hardcoded match IDs — API-driven discovery of new/modified matches

## 2. API Contract (pining-for-the-data)

### Discovery

```
GET /v1/skillcorner/matches?updatedSince={ISO8601}
Authorization: Bearer {token}
```

Returns matches with `updated_at` after the given timestamp. ISO 8601 UTC format: `YYYY-MM-DDTHH:MM:SSZ`. Omitting `updatedSince` returns all matches. Additional filters `dateFrom` and `dateTo` (YYYY-MM-DD) are available but not used by this design.

Response shape:
```json
{
  "provider": "skillcorner",
  "matches": [
    {
      "id": "1886347",
      "artifacts": {
        "1886347_dynamic_events": "1886347_dynamic_events.csv",
        "1886347_match": "1886347_match.json",
        "1886347_tracking_extrapolated": "1886347_tracking_extrapolated.jsonl",
        "1886347_phases_of_play": "1886347_phases_of_play.csv"
      },
      "date": "2024-11-30",
      "home": "Auckland FC",
      "away": "Newcastle",
      "updated_at": "2026-05-04T02:44:12Z",
      "visibility": "public"
    }
  ]
}
```

### Artifact Retrieval

```
GET /v1/skillcorner/matches/{match_id}/{artifact_key}
Authorization: Bearer {token}
```

Returns 302 redirect to S3 presigned URL (1-hour expiry). Client must follow the redirect. `ingestion.utils.fetch_url` handles this via `requests` with retry-with-backoff; however, if a single match's tracking JSONL download exceeds 1 hour, the presigned URL will expire mid-stream. For the current 10-match corpus this is not a concern (largest JSONL is ~15 MB, downloads in seconds). If match count grows significantly, add per-artifact URL refresh on 403/expiry.

Artifact keys per match: `{id}_dynamic_events`, `{id}_match`, `{id}_tracking_extrapolated`.
`{id}_phases_of_play` exists but is not consumed by this design (see §12, L1).

### Authentication

Token sourced from environment variable `PINING_FOR_THE_DATA_TOKEN` (Databricks secret on the job, `.env` for local dev).

### Verified Empirically (2026-05-15)

- `updatedSince=2027-01-01T00:00:00Z` → 0 matches (correct filtering)
- `updatedSince=2026-05-01T00:00:00Z` → 10 matches (all updated after that date)
- Artifact redirect pattern confirmed: API returns 302 → S3 presigned URL with 1-hour expiry
- Parameter name is camelCase `updatedSince`, not snake_case

## 3. Architecture — Module Layout

Split by artifact, following the Metrica pattern:

| Module | Responsibility |
|--------|---------------|
| `skillcorner_common.py` | API client, `MatchInfo` model, shared constants |
| `skillcorner_events.py` | `dynamic_events.csv` → `bronze.skillcorner_events` |
| `skillcorner_tracking.py` | `tracking_extrapolated.jsonl` → `bronze.skillcorner_tracking` |
| `skillcorner_matches.py` | `match.json` → `bronze.skillcorner_matches` |
| `skillcorner.py` | Orchestrator: guard + `@workflow` entry point |

SPADL conversion added to existing `spadl_conversion.py`.
Identity resolution added to existing `tracking_context.py`.

## 4. Common Module — `skillcorner_common.py`

### API Client

```python
API_BASE_URL = "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"
PROVIDER = "skillcorner"

class MatchInfo(pydantic.BaseModel):
    id: str
    artifacts: dict[str, str]
    home: str
    away: str
    date: str
    updated_at: datetime
    visibility: str

def fetch_match_list(token: str, updated_since: str | None = None) -> list[MatchInfo]:
    """GET /v1/skillcorner/matches with optional updatedSince filter."""

def fetch_artifact(match_id: str, artifact_key: str, token: str) -> requests.Response:
    """Fetch artifact, following S3 302 redirect. Uses ingestion.utils.fetch_url."""
```

Token resolution: `os.environ["PINING_FOR_THE_DATA_TOKEN"]`.

No response caching — incremental skip guards prevent redundant downloads.

## 5. Bronze Table Schemas

### 5.1 `bronze.skillcorner_events`

Source: `dynamic_events.csv` (294 columns per match).

All 294 source columns preserved at bronze (bronze-completeness principle). Key columns consumed by the SPADL converter:

- `event_id`, `event_type`, `event_subtype`, `player_id`, `team_id`, `period`
- `time_start`, `time_end`, `x_start`, `y_start`, `x_end`, `y_end`
- `game_interruption_before`, `game_interruption_after`
- `end_type`, `start_type`

Added at ingestion:
- `match_id: str` — raw native SkillCorner match ID (e.g., `"1886347"`)
- `_ingested_at: timestamp` — UTC audit column

Coordinate system: POSSESSION_PERSPECTIVE (center-origin meters, positive x toward attacking goal). Stored as-is per bronze stability principle.

### 5.2 `bronze.skillcorner_tracking`

Source: `tracking_extrapolated.jsonl` (one JSON object per frame).

**JSONL parsing strategy:** Stream line-by-line using `open()` iterator — do NOT load entire file into memory. Parse each line with `json.loads()`, extract player rows, append to a list. Build DataFrame after all lines for a match are parsed. Estimated memory budget per match: ~54K frames × 22 players × ~200 bytes/row ≈ **~230 MB peak** per match (list of dicts + final DataFrame). Well within the 16 GB driver limit. `gc.collect()` after each match's DataFrame is written to Delta.

Reshaped to narrow format (one row per player per frame):

| Column | Type | Source |
|--------|------|--------|
| `match_id` | string | Raw native SkillCorner ID |
| `period` | Int64 | Frame `period` field |
| `frame` | Int64 | Frame `frame` field |
| `timestamp` | Float64 | Frame `timestamp` field — **parsed from `"HH:MM:SS.ms"` string to float seconds at ingestion time** (e.g., `"00:12:34.90"` → `754.9`). Normalized at bronze so all downstream consumers (tracking context, dbt staging) receive a numeric type consistent with IDSSE/Metrica bronze. Parser: split on `:`, compute `hours*3600 + minutes*60 + seconds`. |
| `player_id` | Int64 | `player_data[].player_id` (native SkillCorner ID) |
| `x` | Float64 | `player_data[].x` (center-origin meters) |
| `y` | Float64 | `player_data[].y` (center-origin meters) |
| `is_visible` | boolean | `player_data[].is_detected` — **renamed to `is_visible` at ingestion time** for schema continuity with the current bronze table and dbt staging (`stg_skillcorner__tracking.sql:67`, `_skillcorner__sources.yml:83`, `_skillcorner__models.yml:106`). The raw API field name is `is_detected`; the bronze column name is `is_visible`. |
| `ball_x` | Float64 | `ball_data.x` |
| `ball_y` | Float64 | `ball_data.y` |
| `ball_z` | Float64 | `ball_data.z` |
| `ball_is_detected` | boolean | `ball_data.is_detected` |
| `frame_rate` | Int64 | Constant 10 |
| `_ingested_at` | timestamp | UTC audit column |

No `team` column — team membership resolved via `bronze.skillcorner_matches` (see §8).

**Smoothing removal rationale (Chesterton's Fence):** The current kloppy path applies Savitzky-Golay smoothing at bronze ingestion (`skillcorner.py:123-131`) because kloppy delivered noisy raw positions. The new source is `tracking_extrapolated.jsonl` — the `_extrapolated` qualifier indicates SkillCorner has already applied server-side smoothing/extrapolation to fill gaps and reduce noise. Applying Savitzky-Golay on top of already-smoothed data would over-smooth and degrade signal quality. The tracking context pipeline independently applies Savitzky-Golay velocity derivation (`_derive_velocities_savgol` at `tracking_context.py:1283`) on the frames output, so velocity smoothing is preserved. Therefore: **intentionally drop bronze-level Savitzky-Golay smoothing.** Downstream velocity derivation is unaffected.

### 5.3 `bronze.skillcorner_matches`

Source: `match.json` (one JSON file per match).

Denormalized to one row per player-match (roster format):

| Column | Type | Source |
|--------|------|--------|
| `match_id` | string | `id` field |
| `player_id` | Int64 | `players[].id` |
| `team_id` | Int64 | `players[].team_id` |
| `player_name` | string | `players[].short_name` |
| `first_name` | string | `players[].first_name` |
| `last_name` | string | `players[].last_name` |
| `jersey_number` | Int64 | `players[].number` |
| `position_name` | string | `players[].player_role.name` |
| `position_acronym` | string | `players[].player_role.acronym` |
| `team_name` | string | `home_team.name` or `away_team.name` (resolved by `team_id`) |
| `team_short_name` | string | `home_team.short_name` or `away_team.short_name` |
| `home_team_id` | Int64 | `home_team.id` |
| `away_team_id` | Int64 | `away_team.id` |
| `competition_id` | Int64 | `competition_edition.competition.id` |
| `competition_name` | string | `competition_edition.competition.name` |
| `season_id` | Int64 | `competition_edition.season.id` |
| `season_name` | string | `competition_edition.season.name` |
| `match_date` | string | `date_time` |
| `stadium_name` | string | `stadium.name` |
| `pitch_length` | Int64 | `pitch_length` (needed by SPADL converter) |
| `pitch_width` | Int64 | `pitch_width` (needed by SPADL converter) |
| `period_boundaries` | string | JSON-serialized `match_periods` array |
| `_ingested_at` | timestamp | UTC audit column |

### 5.4 ID Chain (verified empirically 2026-05-15)

All three artifacts use the same SkillCorner numeric IDs:

| Artifact | Player ID | Team ID |
|----------|-----------|---------|
| `match.json` | `players[].id` (e.g., `38673`) | `players[].team_id` (e.g., `4177`) |
| `dynamic_events.csv` | `player_id` column | `team_id` column |
| `tracking_extrapolated.jsonl` | `player_data[].player_id` | Resolve via match metadata |

No ID translation needed anywhere. Native IDs flow end-to-end.

### 5.5 Schema Migration — Old → New Bronze Column Mapping

Complete column mapping between old kloppy-sourced `bronze.skillcorner_tracking` and the new pining-for-the-data schema. Every column rename is traced through staging + tracking context to verify no downstream breakage.

| Old (kloppy) Column | New Column | Change | Downstream Impact |
|---------------------|-----------|--------|-------------------|
| `match_id` (prefixed: `skillcorner_1886347`) | `match_id` (raw native: `1886347`) | **Format change** — prefix removed | `_SKILLCORNER_TRACKING_SELECT_COLS`, `tracking_context.py:1630-1632` filter, `tracking_metadata.py:250`, `stg_skillcorner__tracking.sql` all updated |
| `timestamp` (Float64, seconds) | `timestamp` (Float64, seconds) | **No change** — JSONL string parsed to float at ingestion time | No downstream impact |
| `is_visible` (boolean, always NULL via kloppy) | `is_visible` (boolean, from `is_detected`) | **Renamed at ingestion** — raw API name `is_detected` mapped to `is_visible` | `stg_skillcorner__tracking.sql:67`, `_skillcorner__sources.yml:83`, `_skillcorner__models.yml:106` — no change needed |
| `team` (string: `"home"`/`"away"`) | *Removed* — resolved via `bronze.skillcorner_matches` JOIN | **Removed** | `_SKILLCORNER_CONSUMED_COLS`, `_SKILLCORNER_TRACKING_SELECT_COLS`, `_bronze_skillcorner_to_frames` all rewritten (see §8) |
| `home_team_id` (string: kloppy numeric ID) | *Removed* — sourced from `bronze.skillcorner_matches` | **Removed** | `_SKILLCORNER_TRACKING_SELECT_COLS` updated, driver-side metadata query rewritten |
| `away_team_id` (string) | *Removed* | **Removed** | Not consumed downstream |
| `ball_state` (string) | *Removed* | **Removed** — not in JSONL source | Not consumed by tracking context |
| `ball_owning_team_id` (string) | *Removed* | **Removed** — not in JSONL source | Not consumed downstream |
| `is_goalkeeper` (boolean) | *Removed* — resolved via `bronze.skillcorner_matches.position_name` | **Removed** | Tracking context derives GK status from matches metadata |
| `position_name` (string) | *Removed* — available in `bronze.skillcorner_matches` | **Removed** | Not consumed by tracking context converter |
| *N/A* | `ball_z` (Float64) | **New** | `ball_data.z` from JSONL |
| *N/A* | `ball_is_detected` (boolean) | **New** | `ball_data.is_detected` from JSONL |

## 6. Ingestion Orchestration

### Guard

`_SkillcornerGuard.check()`:
1. Query `MAX(_ingested_at)` from `bronze.skillcorner_events` (or epoch zero if table doesn't exist)
2. Call `fetch_match_list(token, updated_since=max_ingested_at)`
3. If 0 matches returned → `FilterResult(count=0)` (skip)
4. Otherwise → `FilterResult(count=len(matches))` with match list in metadata

### Pipeline Ordering

`@workflow("wf-skillcorner")` calls ingestors in sequence:
1. **Matches first** — needed for downstream identity resolution
2. **Events second** — SPADL conversion reads from this
3. **Tracking third** — tracking context reads from this

Per-match processing with `gc.collect()` between matches (same as current pattern).

### `replaceWhere` Idempotency

All three writers use `replaceWhere` keyed on `match_id` for idempotent writes. A modified match (API returns it via `updatedSince`) gets its bronze rows fully replaced.

## 7. SPADL Conversion

Added to existing `spadl_conversion.py`.

### New Functions

- `_make_skillcorner_spadl_udf()` — `applyInPandas` UDF closure
- `_make_skillcorner_replace_where(match_ids)` — `data_source = 'skillcorner' AND match_id IN (...)`
- `_convert_skillcorner_from_bronze(spark, catalog, schema, logger, existing_matches)` — orchestrates read → convert → write

### UDF Flow

**Driver-side pre-computation (BEFORE building the UDF closure):**
1. For each match, query `bronze.skillcorner_matches` to build `match_metadata: dict`. The silly-kicks converter accesses nested keys, so the dict must have this exact shape:
   ```python
   # From bronze.skillcorner_matches (one row is sufficient — match-level fields are denormalized)
   row = matches_pdf.iloc[0]
   match_metadata = {
       "id": str(row["match_id"]),
       "pitch_length": int(row["pitch_length"]),
       "pitch_width": int(row["pitch_width"]),
       "home_team": {"id": int(row["home_team_id"])},
   }
   # silly-kicks accesses:
   #   match_metadata["pitch_length"]           → coordinate rescale
   #   match_metadata["pitch_width"]            → coordinate rescale
   #   str(match_metadata["home_team"]["id"])    → LTR normalization (no-op for POSSESSION_PERSPECTIVE)
   #   str(match_metadata.get("id", "unknown")) → game_id column
   ```
2. Capture `match_metadata` dict in the frozen UDF closure (per `docs/engineering/databricks-serverless.md` — serverless executors cannot query Delta tables)

**Inside the UDF closure (executor-side):**
1. Receive one match's `bronze.skillcorner_events` rows
2. Call `silly_kicks.spadl.skillcorner.convert_to_actions(events, match_metadata)` — the converter handles coordinate normalization (POSSESSION_PERSPECTIVE → SPADL LTR) internally using `home_team.id` from `match_metadata`. **No `home_team_start_left` kwarg needed** — the SkillCorner converter uses `POSSESSION_PERSPECTIVE` convention where `to_spadl_ltr` is a no-op (see silly-kicks `orientation.py:178-180`).
3. Set identity columns using ADR-018 canonical generators (§7.1):
   - `match_id_native = skillcorner_native_match_id(match_id)` (validates format)
   - `team_id_native = skillcorner_native_team_id(team_id)` (validates format)
   - `player_id_native = skillcorner_native_player_id(player_id)` (validates format)
   - `data_source = "skillcorner"`
4. Legacy BIGINT `match_id` via `hash_native_id_to_bigint(match_id_native)`
5. Legacy BIGINT `team_id`, `player_id`, `competition_id`, `season_id` = `pd.NA`
6. Apply `apply_spadl_enrichments(actions, source="skillcorner")`
7. Null-fill StatsBomb-specific columns (`statsbomb_possession_id`, etc.)

### 7.1 ADR-018 Identifier Generators (C1 fix)

Add to `src/shared/identifiers.py`:

```python
# ---------------------------------------------------------------------------
# SkillCorner (A-League broadcast tracking)
# ---------------------------------------------------------------------------

_SKILLCORNER_NUMERIC_ID_PATTERN = re.compile(r"^[0-9]+$")

def skillcorner_native_match_id(raw_match_id: str | int) -> str:
    """Canonical SkillCorner native match id — stringified positive integer."""
    s = str(raw_match_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner match id: {raw_match_id!r} (expected numeric string)")
    return s

def skillcorner_native_player_id(raw_player_id: str | int) -> str:
    """Canonical SkillCorner native player id — stringified positive integer."""
    s = str(raw_player_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner player id: {raw_player_id!r} (expected numeric string)")
    return s

def skillcorner_native_team_id(raw_team_id: str | int) -> str:
    """Canonical SkillCorner native team id — stringified positive integer."""
    s = str(raw_team_id)
    if not _SKILLCORNER_NUMERIC_ID_PATTERN.match(s):
        raise ValueError(f"invalid SkillCorner team id: {raw_team_id!r} (expected numeric string)")
    return s
```

Add `NativeMatchId.skillcorner()`, `NativePlayerId.skillcorner()`, `NativeTeamId.skillcorner()` class methods.

Add format-contract tests in `test_format_contract.py`:

```python
class TestSkillCornerFormatContract:
    def test_skillcorner_match_id_format_matches_dim(self) -> None:
        assert skillcorner_native_match_id("1886347") == "1886347"
        assert skillcorner_native_match_id(1886347) == "1886347"

    def test_skillcorner_match_id_rejects_bad_format(self) -> None:
        with pytest.raises(ValueError):
            skillcorner_native_match_id("skillcorner_1886347")  # no prefix allowed

class TestSkillCornerPlayerIdFormatContract:
    def test_skillcorner_native_player_id_valid(self) -> None:
        assert skillcorner_native_player_id(38673) == "38673"

class TestSkillCornerTeamIdFormatContract:
    def test_skillcorner_native_team_id_valid(self) -> None:
        assert skillcorner_native_team_id(4177) == "4177"
```

### Wiring

`compute_spadl_vaep` dispatches per provider. Add SkillCorner alongside IDSSE/Metrica/Wyscout/StatsBomb.

## 8. Tracking Context Identity Resolution

Replace the `NotImplementedError` at `tracking_context.py:574`.

### Identity Resolution (`_resolve_identity`, SkillCorner branch)

Direct match — tracking frames and SPADL actions both use native SkillCorner `player_id`:
```python
actions["player_id"] = actions["player_id_native"]
```
Same pattern as Metrica.

For team_id, SkillCorner native IDs are numeric strings (e.g., `"4177"`). The SPADL converter outputs `team_id` as the same native SkillCorner `team_id` string. No reverse mapping needed (unlike Metrica which maps `metrica_Sample_Game_1_home` → `"Home"`):
```python
actions["team_id"] = actions["team_id_native"]
```

### Team Resolution for Tracking Frames — Concrete Design (C4 fix)

The new `bronze.skillcorner_tracking` has no `team` column. The `_bronze_skillcorner_to_frames` converter requires a team assignment per player row. Resolution:

**Driver-side (before `applyInPandas` dispatch):**
1. Query `bronze.skillcorner_matches` for the current match: `SELECT DISTINCT player_id, team_id, home_team_id, position_name FROM bronze.skillcorner_matches WHERE match_id = '{match_id}'`
2. Build `player_team_map: dict[int, str]` — maps `player_id → str(team_id)` (native SkillCorner team ID)
3. Build `player_gk_map: dict[int, bool]` — maps `player_id → (position_name == "Goalkeeper")`. **Empirical verification required during implementation:** inspect `match.json → players[].player_role.name` for the exact GK role string. If SkillCorner uses a different string (e.g., `"GK"`, `"Keeper"`), adjust the predicate accordingly.
4. Extract `home_team_id: str` from the same query
5. Capture all three (`player_team_map`, `player_gk_map`, `home_team_id`) in the UDF closure (frozen dataclass per serverless convention)

**Inside `_bronze_skillcorner_to_frames` (executor-side):**
1. Input schema: `player_id` (Int64), `x`, `y`, `frame`, `period`, `timestamp`, `ball_x`, `ball_y`, `frame_rate`
2. Assign team: `trk_pdf["team_id"] = trk_pdf["player_id"].map(player_team_map)` — maps player to native team_id string
3. Assign GK flag: `trk_pdf["is_goalkeeper"] = trk_pdf["player_id"].map(player_gk_map).fillna(False)` — required by `TRACKING_FRAMES_COLUMNS` and consumed by pitch control downstream
4. Determine home/away: `trk_pdf["is_home"] = trk_pdf["team_id"] == home_team_id`
5. Rename `timestamp` → `time_seconds` (already Float64 from bronze normalization — see §5.2)
6. Continue with coordinate transform and ball row deduplication as before (ball rows get `is_goalkeeper = False`)

### Updated Constants

**`_SKILLCORNER_TRACKING_SELECT_COLS`** (new value — replaces current lines 87-101):
```python
_SKILLCORNER_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "frame",
    "period",
    "timestamp",
    "player_id",
    "x",
    "y",
    "is_visible",
    "frame_rate",
    "ball_x",
    "ball_y",
    "ball_z",
    "ball_is_detected",
)
```

**`_SKILLCORNER_CONSUMED_COLS`** (new value — replaces current lines 1185-1198):
```python
_SKILLCORNER_CONSUMED_COLS: frozenset[str] = frozenset(
    {
        "frame",
        "period",
        "timestamp",
        "player_id",
        "x",
        "y",
        "frame_rate",
        "ball_x",
        "ball_y",
    }
)
```

Convention: `_CONSUMED_COLS` = exactly what `_bronze_skillcorner_to_frames` reads from the DataFrame. `_SELECT_COLS` is the superset (Spark projection from bronze).

Removed from `_CONSUMED_COLS` vs current:
- `team` — resolved via closure-captured `player_team_map`
- `is_goalkeeper` — resolved via closure-captured `player_gk_map`
- `is_visible` — not consumed by the converter (stays in `_SELECT_COLS` for future use)

Present in `_SELECT_COLS` but not `_CONSUMED_COLS`: `is_visible`, `ball_z`, `ball_is_detected` — projected from Spark for schema completeness but not read by the converter.

### Driver-Side Metadata Resolution (Updated)

Replace `tracking_context.py:1668-1677` (which reads `home_team_id` from `bronze.skillcorner_tracking`):
```python
elif provider == "skillcorner":
    # Query matches table for player→team mapping, GK flags, and home_team_id
    matches_pdf = (
        spark.table(f"{catalog}.bronze.skillcorner_matches")
        .filter(F.col("match_id") == match_id)
        .select("player_id", "team_id", "home_team_id", "position_name")
        .distinct()
        .toPandas()
    )
    player_team_map = dict(zip(matches_pdf["player_id"], matches_pdf["team_id"].astype(str)))
    player_gk_map = dict(zip(matches_pdf["player_id"], matches_pdf["position_name"] == "Goalkeeper"))
    home_team_id = str(matches_pdf["home_team_id"].iloc[0])
```

### Skip Guard

Once SPADL conversion ships, SkillCorner matches appear in `bronze.spadl_actions` with `data_source = 'skillcorner'`. The existing `_spadl_match_ids_by_provider` guard naturally includes them — no guard code changes needed.

## 9. dbt Staging & Downstream Changes

### New dbt Models

- `stg_skillcorner__events.sql` — source declaration + staging for `bronze.skillcorner_events`
- `stg_skillcorner__matches.sql` — source declaration + staging for `bronze.skillcorner_matches`

### Modified dbt Models

- `stg_skillcorner__tracking.sql` — rewrite for new bronze schema:
  - JOIN to `stg_skillcorner__matches` on `(match_id, player_id)` to get `team_id`
  - `match_id` is raw native ID (no `skillcorner_` prefix)
  - Coordinate transform unchanged (center-origin meters → 120×80)
  - `home_team_id` / `away_team_id` sourced from matches staging
  - `is_visible` column name preserved (mapped from `is_detected` at bronze ingestion)
- `_skillcorner__sources.yml` — update source table definitions for new bronze schemas
- `_skillcorner__models.yml` — update column descriptions and tests

### Dimension Resolution

- `dim_teams.sql` — add SkillCorner branch using `stg_skillcorner__matches` (real team names, native IDs)
- `dim_players.sql` — add SkillCorner branch (real player names, positions, jersey numbers)
- `dim_matches.sql` — add SkillCorner branch (competition, season, date, stadium)

These replace the kloppy-sourced `tracking_player_metadata` entries for SkillCorner.

### Downstream — No Changes Needed

`fct_tracking_frames.sql`, `int_tracking__match_side_team_bridge.sql`, and `int_tracking__player_match_team_bridge.sql` consume from `stg_skillcorner__tracking` and pick up the new data automatically.

## 10. Deprecation & Removal

### Removed in This PR

- `skillcorner.py` (current kloppy-based) — entirely replaced
- `tracking_metadata.py` — remove SkillCorner section (`_extract_skillcorner_metadata` at lines 239-301, `_SKILLCORNER_MATCH_IDS` at lines 110-121). IDSSE section stays. Guard (`_TrackingMetadataGuard.check`) updated to remove SkillCorner branch (lines 72-80). **Verified clean separation:** `_extract_skillcorner_metadata` is fully self-contained (uses `kloppy`, `_SKILLCORNER_MATCH_IDS`, and its own `logger`). Nothing in the IDSSE section references SkillCorner functions or constants.
- `bronze.skillcorner_tracking` (old kloppy-sourced table) — drop immediately via `scripts/migrations/` SQL file
- `kloppy` dependency — **definitively safe to remove.** Verified: kloppy appears in 12 files, all SkillCorner-related code paths (`skillcorner.py`, `tracking_metadata.py`, `tracking_context.py`) or their tests. No other module imports or references kloppy. Remove from `pyproject.toml`.

### Test Migration

- Delete `test_skillcorner_raises_not_implemented` — replace with real identity resolution test
- Update `test_tracking_context_preflight.py` — SkillCorner now has SPADL actions, appears in guard output
- Update `test_tracking_context_column_projection.py` — new bronze schema projection constants
- Extend `test_silly_kicks_boundary.py` — full 5th-source parametrization (see §10.1)

### 10.1 `test_silly_kicks_boundary.py` — 5th Source Fixture (C2 fix)

The existing test has `4 sources × 5 invariants = 20 tests` with a `_PARAMETRIZE` fixture (lines 31-42). SkillCorner requires a full 5th fixture row.

**Fixture creation:**
1. Download `1886347_dynamic_events.csv` via the pining-for-the-data API
2. Convert to Parquet: `pd.read_csv(csv, low_memory=False).to_parquet("sc_match_1886347.parquet")`
3. Store at `src/tests/fixtures/silly_kicks_boundary/sc_match_1886347.parquet`

**Parametrization addition:**
```python
("skillcorner", silly_kicks.spadl.skillcorner, "sc_match_1886347.parquet"),
```

**`_adapt_input` branch:**
```python
if source == "skillcorner":
    # No adapter needed — silly-kicks converter takes raw CSV DataFrame directly.
    # Build match_metadata dict for the converter.
    return df, None  # home_team_id resolved inside converter via match_metadata
```

**`_call_converter` branch:**
```python
if source == "skillcorner":
    # silly-kicks SkillCorner converter has a different API: takes match_metadata dict,
    # not (home_team_id, home_team_start_left). Direction of play is POSSESSION_PERSPECTIVE
    # (no-op in to_spadl_ltr), so no home_team_start_left derivation needed.
    import json
    meta_path = _FIXTURE_DIR / "sc_match_1886347_meta.json"
    with open(meta_path) as f:
        match_metadata = json.load(f)
    return converter.convert_to_actions(adapted, match_metadata)
```

Requires a second fixture: `sc_match_1886347_meta.json` (the `match.json` for match 1886347).

### Workflow Card Update (M6 fix)

Update `workflow-cards/wf-skillcorner.yaml`:

```yaml
---
name: SkillCorner A-League Ingestion
id: wf-skillcorner
version: "2.0.0"
status: production
type: ingestion
domain: soccer-analytics
owners:
  - karsten
tags:
  - skillcorner
  - ingestion
  - tracking-data
  - events-data
  - a-league
  - broadcast-tracking

references:
  - citation: "SkillCorner Open Data via pining-for-the-data API"
    role: dataset

inputs:
  datasets:
    - id: "pining-for-the-data API (SkillCorner provider)"
      source: rest-api
      description: "Events, tracking, and match metadata for A-League matches"

outputs:
  tables:
    - id: "{catalog}.bronze.skillcorner_events"
      destination: delta-table
    - id: "{catalog}.bronze.skillcorner_tracking"
      destination: delta-table
    - id: "{catalog}.bronze.skillcorner_matches"
      destination: delta-table

execution:
  ingestion:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: ingest_skillcorner
    module: ingestion.skillcorner
    distribution: driver-bound
    timeout: "900s"
    environment: analytics

depends_on: []

idempotency:
  strategy: replace-where
  key: match_id
  description: "Uses replaceWhere on match_id for idempotent per-match writes. Guard queries MAX(_ingested_at) and uses updatedSince API filter."

performance:
  inference_timeout: "900s"
  memory_ceiling: "16 GB driver"

cost:
  ingestion:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 40
    typical_cost_usd: 2.8

monitoring:
  freshness_sla_hours: 168

links:
  source_code:
    - "src/ingestion/skillcorner.py"
    - "src/ingestion/skillcorner_common.py"
    - "src/ingestion/skillcorner_events.py"
    - "src/ingestion/skillcorner_tracking.py"
    - "src/ingestion/skillcorner_matches.py"
---

## Overview

Ingests SkillCorner A-League match data (events, tracking, match metadata)
from the pining-for-the-data REST API. API-driven match discovery via
`updatedSince` filter. Three bronze tables written with `replaceWhere`
idempotency. SPADL conversion wired via silly-kicks SkillCorner converter.
Tracking context pipeline consumes the output.
```

## 11. Testing Strategy

### Unit Tests (New)

| Test file | Coverage |
|-----------|----------|
| `test_skillcorner_common.py` | API client: mock `fetch_url`, test `fetch_match_list` parsing, `updatedSince` construction, redirect handling, token sourcing |
| `test_skillcorner_events.py` | Events parser: fixture CSV subset, verify `match_id` = raw native ID (no `skillcorner_` prefix), dtype overrides |
| `test_skillcorner_tracking.py` | Tracking parser: fixture JSONL (2-3 frames), verify narrow reshape, native `player_id`, ball extraction, `timestamp` string→float parsing, `is_detected`→`is_visible` rename |
| `test_skillcorner_matches.py` | Match parser: fixture JSON, verify roster flattening, team/competition metadata, `pitch_length`/`pitch_width` extraction |
| `test_skillcorner_spadl.py` | SPADL UDF: mock converter, verify identity columns use ADR-018 generators (not bare `str()`), `hash_native_id_to_bigint`, enrichments |

### Modified Tests

- `test_tracking_context_identity_resolution.py` — real identity resolution test replaces `NotImplementedError` test
- `test_tracking_context_preflight.py` — SkillCorner in guard output
- `test_tracking_context_column_projection.py` — new projection constants (`_SKILLCORNER_TRACKING_SELECT_COLS`, `_SKILLCORNER_CONSUMED_COLS`)
- `test_silly_kicks_boundary.py` — full 5th-source parametrization (§10.1)
- `test_format_contract.py` — SkillCorner identifier format contracts (§7.1)

### Additional Validations

- `_VALID_SOURCES` in `spadl_enrichments.py` — add `"skillcorner"` to the frozenset (line 27). Without this, `apply_spadl_enrichments(actions, source="skillcorner")` raises `ValueError` immediately.
- Verify no residual `skillcorner_` prefix references in tracking context skip guard code paths after match_id format change (§5.5).

### Integration Validation (Post-Deploy)

- All available matches land in all 3 bronze tables
- SPADL actions produced for all matches (`data_source = 'skillcorner'`)
- Tracking context pipeline processes SkillCorner matches
- Dimension tables resolve SkillCorner native IDs to real names

**E2E integration test — ships with this PR (Option A, user-approved).**

`test_skillcorner_e2e.py`:
1. Mock the pining-for-the-data API responses using fixture files (subset of match 1886347: `dynamic_events.csv` truncated to ~50 rows, `match.json`, `tracking_extrapolated.jsonl` truncated to ~20 frames)
2. Mock `fetch_url` to return fixture data instead of HTTP calls
3. Run the full ingestion pipeline: `skillcorner_matches.py` → `skillcorner_events.py` → `skillcorner_tracking.py` writer functions with pandas DataFrames (no Spark — unit-test-scoped)
4. Run SPADL conversion: feed events DataFrame + `match_metadata` dict into `silly_kicks.spadl.skillcorner.convert_to_actions`, then `apply_spadl_enrichments`
5. Run tracking context converter: feed tracking DataFrame + closure maps into `_bronze_skillcorner_to_frames`
6. Assert: all 3 bronze DataFrames have expected schemas and non-zero row counts, SPADL actions have correct `data_source`/identity columns, tracking frames have `is_goalkeeper`/`team_id` populated

## 12. Out of Scope

- **`phases_of_play.csv` ingestion** — not consumed by any downstream pipeline today. The only potential consumer would be a possession-phase-aware analytics model; no such model exists or is planned. Can be added later as `skillcorner_phases` using the same module pattern when a consumer materializes.
- Taipy UI changes — no new pages or widgets
- `updatedSince` optimization for per-table granularity — single `MAX(_ingested_at)` from events table is sufficient for 10 matches; revisit if match count grows significantly

## Appendix A: Review Response Log

### Rev 3 fixes (5 items from second review, 2026-05-15)

| ID | Severity | Fix Applied |
|----|----------|-------------|
| N1 | CRITICAL | §8 — Added `position_name` to driver-side query, `player_gk_map: dict[int, bool]` built and captured in closure, GK assignment added to `_bronze_skillcorner_to_frames` converter steps |
| N2 | MEDIUM | §8 — Removed `is_visible` from `_CONSUMED_COLS` (converter doesn't read it). Clarified `_CONSUMED_COLS` vs `_SELECT_COLS` convention |
| N3 | MEDIUM | §7.1 — Renamed `_SKILLCORNER_MATCH_ID_PATTERN` → `_SKILLCORNER_NUMERIC_ID_PATTERN` (shared pattern for match/player/team) |
| N4 | LOW | §11 — Replaced unilateral deferral with Option A / Option B for user decision |
| N5 | LOW | §7 — Added explicit `match_metadata` dict construction showing nested `{"home_team": {"id": ...}}` structure with accessor comments |

### Rev 2 fixes (18 items from first review, 2026-05-15)

| ID | Severity | Fix Applied |
|----|----------|-------------|
| C1 | CRITICAL | §7.1 — ADR-018 identifier generators + format-contract tests added |
| C2 | CRITICAL | §10.1 — Full 5th-source boundary test fixture + parametrization specified |
| C3 | CRITICAL | §7 UDF Flow — **Reviewer concern partially incorrect**: the silly-kicks SkillCorner converter uses `POSSESSION_PERSPECTIVE` convention (no `home_team_start_left` needed). Unlike Sportec/Metrica converters which take `home_team_id` + `home_team_start_left`, the SkillCorner converter takes `match_metadata: dict` and handles LTR normalization internally via `to_spadl_ltr(... POSSESSION_PERSPECTIVE ...)` which is a no-op. The original spec was correct that no direction-of-play derivation is needed, but the rationale was wrong ("match.json states home_team.id" ≠ "direction of play is not needed"). Corrected rationale: SkillCorner events use POSSESSION_PERSPECTIVE convention — each team's events are already in the team's own attacking frame, so LTR normalization requires no coordinate flip. |
| C4 | CRITICAL | §8 — Concrete team resolution design: driver-side `player_id→team_id` dict from `bronze.skillcorner_matches`, captured in UDF closure. Updated `_SKILLCORNER_CONSUMED_COLS`, `_SKILLCORNER_TRACKING_SELECT_COLS`, `_bronze_skillcorner_to_frames` rewrite spec |
| H1 | HIGH | §5.2, §5.5 — `is_detected` renamed to `is_visible` at ingestion time for schema continuity. Full column mapping table added |
| H2 | HIGH | §5.5 — Complete old→new column mapping with downstream impact trace. `match_id` prefix removal safe (zero tracking context rows exist for skillcorner) |
| H3 | HIGH | §5.2 — `timestamp` parsed from `"HH:MM:SS.ms"` string to Float64 seconds at bronze ingestion time. Parser formula specified. All downstream consumers receive numeric type |
| H4 | HIGH | §8 — New values for `_SKILLCORNER_TRACKING_SELECT_COLS` and `_SKILLCORNER_CONSUMED_COLS` enumerated |
| H5 | HIGH | §5.2 — JSONL streaming strategy (line-by-line) and memory budget (~230 MB peak per match) specified |
| M1 | MEDIUM | §7 — UDF Flow restructured: driver-side pre-computation explicitly separated from executor-side closure. `match_metadata` query happens on driver, captured in frozen closure |
| M2 | MEDIUM | §11 — `_VALID_SOURCES` update in `spadl_enrichments.py` explicitly called out |
| M3 | MEDIUM | §5.2 — Smoothing removal justified: `tracking_extrapolated.jsonl` is already server-side smoothed; downstream velocity derivation preserved in tracking context |
| M4 | MEDIUM | §10 — Clean separation verified and documented: `_extract_skillcorner_metadata` is self-contained, IDSSE section has zero SkillCorner references |
| M5 | MEDIUM | §10 — kloppy removal scope definitively stated: 12 files, all SkillCorner-related. Safe to remove from `pyproject.toml` |
| M6 | MEDIUM | §10 — Full workflow card YAML specified with all structured fields |
| L1 | LOW | §12 — `phases_of_play.csv` exclusion rationale added: no downstream consumer exists |
| L2 | LOW | §2 — S3 presigned URL expiry noted with mitigation strategy |
| L3 | LOW | §11 — E2E integration test gap acknowledged; deferred as follow-up with rationale |
