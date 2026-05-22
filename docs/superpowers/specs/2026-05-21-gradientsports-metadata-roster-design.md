# Gradient Sports Metadata + Roster Ingestion

## Goal

Ingest the two missing Gradient Sports API artifacts (metadata, roster) into
bronze, expose them through staging views, and onboard Gradient Sports into all
four Kimball dimension tables (dim_matches, dim_teams, dim_players,
dim_competitions). This resolves the NULL cascade in fct_action_values where
all 90,013 GS rows have NULL match_key, team_key, player_key, and
competition_key.

Additionally, close the `competition_native_id` / `season_native_id` pd.NA gap
in the SPADL UDF so that the standard ADR-018 JOIN-coverage test pattern
provides real (non-vacuous) coverage for GS.

## Context

The pining-for-the-data API serves 4 artifact types per GS match:

| Artifact | Status | Grain |
|----------|--------|-------|
| `events` | Ingested -> bronze.gradientsports_events | One row per event |
| `tracking` | Ingested -> bronze.gradientsports_tracking | One row per player per frame |
| `metadata` | **NOT ingested** | One dict per match (wrapped in a 1-element list) |
| `roster` | **NOT ingested** | One dict per player per match (~51/match) |

All 64 WC2022 matches have all 4 artifacts. Schema is stable across matches
(verified on 5 matches including extra-time Croatia vs Brazil).

### What the missing artifacts contain

**metadata** (21 keys per match):
- `homeTeam` / `awayTeam`: `{id, name, shortName}`
- `competition`: `{id: "38", name: "FIFA Men's World Cup"}`
- `season`: `"2022"`
- `date`: ISO 8601 timestamp
- `stadium`: `{id, name, pitches: [{id, length, width, startDate, endDate}]}`
- `homeTeamStartLeft` / `homeTeamStartLeftExtraTime`: boolean
- `fps`: frame rate (29.97)
- Period timestamps: `startPeriod1/2`, `endPeriod1/2`, `halfPeriod`, `period1/2`
- `week`: matchweek number
- `videoUrl`: Epitome video link
- Kit dicts: `homeTeamKit`, `awayTeamKit` (colors, names)

**roster** (~51 entries per match):
- `player`: `{id: "3861", nickname: "Xavi Simons"}`
- `team`: `{id: "366", name: "Netherlands"}`
- `positionGroupType`: position code (GK, CB, AM, CF, etc.)
- `shirtNumber`: string
- `started`: boolean

## Architecture

### Decision: Two separate bronze tables (not combined)

Metadata and roster have different grains (1 row/match vs ~51 rows/match).
Combining them into one denormalized table adds ingestion complexity for no
benefit. Bronze preserves source structure per project convention.

### Decision: Same orchestrator (gradientsports.py)

Both artifacts come from the same API, same auth, same per-match loop. Adding
a separate module would duplicate guard logic and API auth. The existing
orchestrator loop already iterates match artifacts by key.

### Decision: json_normalize for full capture

Following the `gradientsports_events.py` pattern, both parsers use
`pd.json_normalize` to flatten the full API response. This captures every
field automatically — if the API adds fields later, they flow through without
code changes. Nested list/dict fields that json_normalize cannot flatten
(e.g., `stadium.pitches`, kit dicts) are serialized to JSON strings.

Staging views SELECT the specific typed columns they need from the full bronze,
using backtick-quoted dot-notation column names.

### Decision: Close the SPADL competition/season pd.NA gap

`spadl_conversion.py:2007-2008` currently sets `competition_native_id=pd.NA`
and `season_native_id=pd.NA` for GS rows because the events artifact has no
competition metadata. Post-backfill, `bronze.gradientsports_metadata` will
have `competition.id` ("38") and `season` ("2022") for every match.

The SPADL UDF already receives a `metadata` dict (line 2006 references
`metadata["home_team_id"]`). Update `_apply_match_natives` to inject:
- `competition_native_id` from metadata's `competition.id` (via
  `gradientsports_native_competition_id` generator)
- `season_native_id` from metadata's `season` field

This closes the pd.NA gap and makes the standard ADR-018 JOIN-coverage test
(`WHERE competition_native_id IS NOT NULL`) provide real coverage for all
90,013 GS rows instead of vacuously passing on 0 rows.

The metadata dict is already loaded per-match in the SPADL UDF scope
(`spadl_conversion.py` GS branch). The change is two line replacements:
`competition_native_id=_gs_comp_id(str(metadata["competition_id"]))` and
`season_native_id=str(metadata.get("season", ""))`. Requires the metadata
bronze table to be populated first (backfill runs before SPADL recompute in
the deployment sequence).

## Bronze Tables

### gradientsports_metadata

One row per match. `pd.json_normalize(metadata[0])` flattened, plus:
- `match_id` (STRING): injected from orchestrator, validated via
  `gradientsports_native_match_id()` (ADR-018 defense-in-depth)
- `_ingested_at` (TIMESTAMP): UTC ingestion time
- List/dict fields serialized to JSON strings: `stadium.pitches`,
  `homeTeamKit` (if present as dict), `awayTeamKit`
- All integer columns widened to float64 (same pattern as events, prevents
  BIGINT/DOUBLE schema divergence across matches)
- `replaceWhere` partitioned on `match_id`

Expected columns after json_normalize (dot notation):
`id`, `homeTeam.id`, `homeTeam.name`, `homeTeam.shortName`,
`awayTeam.id`, `awayTeam.name`, `awayTeam.shortName`,
`competition.id`, `competition.name`, `season`, `date`,
`stadium.id`, `stadium.name`, `homeTeamStartLeft`,
`homeTeamStartLeftExtraTime`, `fps`, `halfPeriod`,
`period1`, `period2`, `startPeriod1`, `endPeriod1`,
`startPeriod2`, `endPeriod2`, `week`, `videoUrl`,
`homeTeamKit.*` (5 sub-fields), `awayTeamKit.*` (5 sub-fields),
`stadium.pitches` (serialized JSON string)

### gradientsports_roster

One row per player per match. `pd.json_normalize(roster)` flattened, plus:
- `match_id` (STRING): injected from orchestrator, validated via
  `gradientsports_native_match_id()` (ADR-018 defense-in-depth)
- `_ingested_at` (TIMESTAMP): UTC ingestion time
- All integer columns widened to float64
- `replaceWhere` partitioned on `match_id`

Expected columns after json_normalize:
`player.id`, `player.nickname`, `team.id`, `team.name`,
`positionGroupType`, `shirtNumber`, `started`

## Ingestion Modules

### gradientsports_metadata.py

Mirrors `gradientsports_events.py` exactly:

- `parse_metadata(source: str | dict | list, *, match_id: str) -> pd.DataFrame`
  - `json.loads` if string, extract `metadata[0]` (API wraps in 1-element list)
  - `pd.json_normalize` on the single metadata dict
  - Serialize list/dict fields (`stadium.pitches`, kit dicts) to JSON strings
  - Widen int columns to float64
  - Validate `match_id` via `gradientsports_native_match_id()` (fail loud on
    malformed ID)
  - Add validated `match_id` + `_ingested_at`

- `write_metadata(spark, df, catalog, schema, match_id, logger) -> int`
  - `validate_dataframe` + `write_delta_table` with `replaceWhere`

### gradientsports_roster.py

Same pattern:

- `parse_roster(source: str | dict | list, *, match_id: str) -> pd.DataFrame`
  - `json.loads` if string
  - `pd.json_normalize` on the roster list
  - Widen int columns to float64
  - Validate `match_id` via `gradientsports_native_match_id()` (fail loud)
  - Validate `player.id` values via `gradientsports_native_player_id()` and
    `team.id` values via `gradientsports_native_team_id()` after normalize
    (defense-in-depth: if the API ever sends a malformed ID like
    "player_3861" instead of "3861", the parser fails at ingestion time
    rather than silently writing a value that won't join)
  - Add validated `match_id` + `_ingested_at`

- `write_roster(spark, df, catalog, schema, match_id, logger) -> int`
  - `validate_dataframe` + `write_delta_table` with `replaceWhere`

### gradientsports.py orchestrator changes

Per-match loop write order: tracking -> metadata -> roster -> events (events
last — skip guard watermark lives on `events._ingested_at`).

Add artifact handling for `metadata` and `roster` keys alongside existing
`event` and `track` handling.

**Partial failure handling**: If metadata or roster write fails mid-match,
the match's events are NOT written (watermark not advanced). On re-run, the
guard will re-process that match. Each artifact write is independently
idempotent via `replaceWhere` on `match_id`, so partial writes are safe.

### Backfill mechanism

The existing skip guard (`_GradientSportsGuard`) discovers matches by
anti-joining the API match list against `bronze.gradientsports_events`. All 64
matches already have events, so the guard reports 0 new matches.

Add a `--backfill-artifacts` flag to `main()`:
- Skip the guard entirely
- Query `SELECT DISTINCT match_id FROM bronze.gradientsports_events` to get
  the match ID list
- For each match, fetch metadata + roster from the API (skip events + tracking)
- Write to the new bronze tables

This is reusable if the API adds more artifact types in the future.

## SPADL UDF Changes

### Close competition_native_id / season_native_id gap

In `spadl_conversion.py` GS branch (~line 2003-2009), the `_apply_match_natives`
call currently passes `competition_native_id=pd.NA` and
`season_native_id=pd.NA`. The SPADL UDF's `metadata` dict already carries
`competition_id` and `season` (populated from `stg_gradientsports__metadata`
via the match-level metadata lookup).

Replace:
```python
competition_native_id=_pd.NA,  # GS has no competition_native_id in events
season_native_id=_pd.NA,
```

With:
```python
competition_native_id=_gs_comp_id(str(metadata["competition_id"])),
season_native_id=str(metadata.get("season", "")),
```

Where `_gs_comp_id` is imported from `shared.identifiers` as
`gradientsports_native_competition_id`.

This requires the metadata bronze table to be populated first (deployment
sequence: backfill runs before SPADL recompute). The metadata dict is already
loaded per-match in the SPADL UDF scope.

## Staging Views

### stg_gradientsports__metadata

Source: `bronze.gradientsports_metadata`. One row per match.

```sql
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
    cast(`date` as timestamp)         as match_date,  -- ISO 8601 UTC from API
    cast(`stadium.id` as string)      as stadium_id,
    `stadium.name`                    as stadium_name,
    `homeTeamStartLeft`               as home_team_start_left,
    `homeTeamStartLeftExtraTime`      as home_team_start_left_extra_time,
    `fps`,
    cast(`week` as int)               as matchweek,
    _ingested_at
from {{ source('gradientsports', 'gradientsports_metadata') }}
```

### stg_gradientsports__roster

Source: `bronze.gradientsports_roster`. One row per player per match.

```sql
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

## Dimension CTEs

All four Kimball dimension tables get a new `gradientsports_*` CTE added to
their UNION ALL chain, following established patterns. Update dim SQL header
comments to list Gradient Sports (and SkillCorner where missing).

### dim_matches — gradientsports_matches CTE

Source: `stg_gradientsports__metadata`.

```sql
gradientsports_matches as (
    select
        cast(match_id as string)     as native_match_id,
        'gradientsports'             as provider,
        competition_id,
        season_id,
        cast(match_date as date)     as match_date,
        home_team_name,
        away_team_name
    from {{ ref('stg_gradientsports__metadata') }}
)
```

### dim_teams — gradientsports_teams CTE

Source: `stg_gradientsports__metadata` (more authoritative for team identity
than roster — metadata carries shortName). UNION of home + away teams with
GROUP BY for dedup across 64 matches.

```sql
gradientsports_teams as (
    select
        'gradientsports'             as provider,
        native_team_id,
        cast(null as bigint)         as team_id_legacy,
        max(team_name)               as team_name,
        false                        as is_synthesized,
        cast(null as boolean)        as is_anonymized,
        cast(null as string)         as synthesis_reason
    from (
        select home_team_id as native_team_id, home_team_name as team_name
        from {{ ref('stg_gradientsports__metadata') }}
        union all
        select away_team_id as native_team_id, away_team_name as team_name
        from {{ ref('stg_gradientsports__metadata') }}
    )
    where native_team_id is not null
    group by native_team_id
)
```

### dim_players — gradientsports_players CTE

Source: `stg_gradientsports__roster`. GROUP BY for dedup (same player appears
in multiple matches).

```sql
gradientsports_players as (
    select
        player_id                    as native_player_id,
        cast(null as bigint)         as player_id_legacy,
        max(player_nickname)         as player_name,
        max(player_nickname)         as player_display_name,
        max(position_group)          as primary_position,
        'gradientsports'             as provider,
        false                        as is_synthesized,
        false                        as is_anonymized,
        cast(null as string)         as synthesis_reason,
        cast(null as string)         as birth_date,
        cast(null as string)         as nationality
    from {{ ref('stg_gradientsports__roster') }}
    where player_id is not null
    group by player_id
)
```

### dim_competitions — gradientsports_competitions CTE

Source: `stg_gradientsports__metadata`. Single competition for WC2022:
id=38, name="FIFA Men's World Cup".

```sql
gradientsports_competitions as (
    select distinct
        'gradientsports'             as provider,
        competition_id               as native_competition_id,
        cast(null as int)            as competition_id_legacy,
        competition_name
    from {{ ref('stg_gradientsports__metadata') }}
    where competition_id is not null
)
```

## ADR-018 Compliance

### identifiers.py

Three GS generators already exist (`gradientsports_native_match_id`,
`_player_id`, `_team_id`) at `src/shared/identifiers.py:232-262`, with
NamedTuple wrappers at lines 298-362. Format-contract Python tests exist at
`src/tests/test_format_contract.py:327-385`.

**Add**: `gradientsports_native_competition_id(raw_competition_id: str | int)`
generator + format-contract test. Same numeric-string pattern as the existing
three.

### dbt singular JOIN-coverage tests

Per ADR-018, every new provider/dim touchpoint needs a dbt singular test
asserting JOIN coverage from `stg_spadl__action_values` to `dim_*`. Pattern:
`dbt_project/tests/assert_<provider>_<entity>_native_id_join_resolves.sql`
with `config(tags=['post_deploy_only'], enabled=var('include_post_deploy_tests', false))`.

Add 4 tests (filename convention matches existing `competition_native_id`
ordering, NOT `competition_id_native`):
- `assert_gradientsports_match_id_native_join_resolves.sql`
- `assert_gradientsports_team_id_native_join_resolves.sql`
- `assert_gradientsports_player_id_native_join_resolves.sql`
- `assert_gradientsports_competition_native_id_join_resolves.sql`

Follow the existing `assert_statsbomb_*` pattern (LEFT JOIN from
stg_spadl__action_values WHERE data_source = 'gradientsports' to dim_*,
return orphan native IDs). The competition test is non-vacuous because the
SPADL UDF change (see "SPADL UDF Changes" section) populates
`competition_native_id` from metadata.

### Source onboarding contracts

`src/tests/test_source_onboarding_contracts.py:7` has an explicit TODO for GS.
Add `'gradientsports'` to the parametrized source list (line 19-27). Requires
a GS-specific fixture:

- **Source match**: match 10502 (Qatar vs Ecuador, first WC2022 match — same
  match used for manual API verification during spec development)
- **Generation**: Run the GS SPADL UDF on match 10502's events, save output
  as `src/tests/fixtures/silly_kicks_boundary/gs_match_10502.parquet`
- **Expected**: ~2,200 rows (one match worth of SPADL actions), columns
  matching `_SPADL_SCHEMA` + `_VAEP_SCHEMA` parity (tested by
  `spadl_schema_parity`)

## Test Fixes

### accepted_values audit

Add `'gradientsports'` to every provider enumeration where GS data flows:

| Line | Model | Action |
|------|-------|--------|
| 134 | fct_shots.data_source | Add `gradientsports` |
| 328 | fct_passes.data_source | Add `gradientsports` |
| 461 | fct_player_stats.data_source | Add `gradientsports` |
| 884 | fct_action_values.data_source | Add `gradientsports` |
| 2479, 2537 | dim_players.provider/data_source | Add `gradientsports` |
| 2566, 2609, 2616 | dim_teams.provider/data_source | Add `gradientsports` |
| 2663 | dim_competitions.provider | Add `gradientsports` |
| 4600 | dim_matches.provider | Add `gradientsports` |

### not_null WHERE filter updates

Add `'gradientsports'` to data_source IN filters:
- fct_action_values.team_key (line 704)
- fct_action_values.player_key (line 720)

### New staging YAML

`dbt_project/models/staging/gradientsports/_gradientsports__models.yml`:

Source definitions for both bronze tables with freshness config (following
`_skillcorner__sources.yml` pattern):

```yaml
sources:
  - name: gradientsports
    schema: "{{ var('bronze_schema', 'bronze') }}"
    loaded_at_field: _ingested_at
    freshness:
      warn_after: {count: 30, period: day}
    tables:
      - name: gradientsports_metadata
      - name: gradientsports_roster
```

Schema + not_null tests for both staging views (columns, types, not_null on
key columns).

### Coverage tests

Per project convention (`feedback_coverage_test_pattern.md`), add:

1. **Bronze coverage tests** (DESCRIBE-based): Verify bronze tables exist and
   contain expected columns. One test per table (`gradientsports_metadata`,
   `gradientsports_roster`).

2. **Staging coverage entries**: Add both staging views to the coverage test
   registry alongside existing providers.

### Backfill integration test

Add a test for the `--backfill-artifacts` flag that verifies:
- Guard is skipped when flag is set
- Only metadata + roster artifacts are fetched (not events/tracking)
- Match ID list is sourced from existing bronze events table

## Deployment Sequence

1. Run backfill ingestion (`--backfill-artifacts`) for all 64 matches
   (metadata + roster only — events and tracking already ingested)
2. `dbt_build_input_marts` — rebuilds all 4 dimensions with new GS CTEs
3. DELETE GS rows from `bronze.spadl_actions` (forces SPADL recompute with
   competition_native_id / season_native_id populated from new metadata)
4. DELETE GS rows from `fct_action_values` (90,013 rows)
5. `dbt_build_intermediate_marts` — SPADL recompute + incremental re-inserts
   GS rows with resolved Kimball keys (match_key, team_key, player_key,
   competition_key all non-NULL)
6. `dbt_build_output_marts` + `refresh_synced_tables`

## Out of Scope

- GS data in Taipy UI (license gate: "NOT published to HF datasets, gold
  marts, synced tables, or Taipy UI until Gradient Sports license confirmed
  in writing" per gradientsports.py docstring)
- Entity resolution (cross-provider xref for GS teams/players)
- HF dataset publishing for GS data
- SkillCorner event pipeline (separate roadmap item)
