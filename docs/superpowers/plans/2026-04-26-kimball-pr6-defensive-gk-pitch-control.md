# Kimball PR 6 — Defensive + Goalkeeper Mart Migration + IDSSE `is_progressive` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan. The user prefers inline execution (no subagent dispatch per `feedback_agent_tool_requires_per_call_approval`). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate 5 defensive/goalkeeper marts onto Kimball-conformed `match_key`/`team_key`/`player_key` BIGINT FKs, populate IDSSE `is_progressive` via tracking-frame ball lookup, and promote `stg_pitch_control__values` to first-class treatment as a published HF dataset source.

**Architecture:** Additive column migration during the 2026-07-22 dual-column window — every mart gains the new keys + (where missing) `data_source`, with surrogate IDs rehashed to include `data_source` for multi-provider correctness. IDSSE `is_progressive` derives end coordinates by JOIN-ing `stg_idsse__passes.end_frame` to `stg_idsse__tracking.ball_x/ball_y`, then applies the existing cross-provider `distance_to_goal` rule. Pitch-control staging gains `data_source` (prefix-CASE) + `match_key` (LEFT JOIN dim_matches). LEFT JOIN with `relationships severity: warn` for all FKs to preserve row counts during the window.

**Tech Stack:** dbt-core 1.10+, dbt-databricks, Spark SQL, Lakebase Postgres synced tables, Python 3.10 (uv), pytest, ruff, pyright, GitHub Actions, HuggingFace Hub.

**Spec:** `docs/superpowers/specs/2026-04-26-kimball-pr6-design.md`

**Branch:** `kimball-pr6-defensive-gk-pitch-control`

**HEAD at start:** `65986b4` (or later — confirm in Phase 0).

---

## File Structure

### Files to create
- `src/tests/test_marts_kimball_contracts.py` — parameterized live-invariant test (renamed from `test_marts_player_key_contracts.py`)
- `src/tests/test_idsse_is_progressive_coverage.py` — IDSSE `is_progressive` non-NULL rate live test
- `src/tests/test_pitch_control_bronze_coverage.py` — bronze→staging completeness parser-level test

### Files to delete
- `src/tests/test_marts_player_key_contracts.py` (renamed → `test_marts_kimball_contracts.py`)

### Files to modify (dbt SQL)
- `dbt_project/models/staging/idsse/stg_idsse__passes.sql` — derive end_x/end_y + is_progressive
- `dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql` — prefix CASE + dim_matches LEFT JOIN
- `dbt_project/models/marts/fct_defensive_values.sql` — new keys, new surrogate hash, on_schema_change
- `dbt_project/models/marts/fct_defcon_actions.sql` — new keys (defender + action_player), on_schema_change
- `dbt_project/models/marts/fct_defcon_pressure.sql` — new keys, new surrogate hash, on_schema_change
- `dbt_project/models/marts/fct_goalkeeper_stats.sql` — data_source propagation, new keys, new surrogate hash, retire dim_matches bridges
- `dbt_project/models/marts/fct_gk_actions_detail.sql` — new keys via provider CASE

### Files to modify (dbt YAML)
- `dbt_project/models/staging/pitch_control/_pitch_control__sources.yml` — add `_ingested_at` declaration
- `dbt_project/models/staging/pitch_control/_pitch_control__models.yml` — add new columns, schema tests, fix docstring
- `dbt_project/models/marts/_marts__models.yml` — add new columns + relationships + unique_combination_of_columns on the 5 marts

### Files to modify (Python tests)
- `src/tests/test_bronze_live_schema.py` — add `bronze.pitch_control_values` entry
- `src/tests/test_marts_live_schema.py` — add 5 PR-6 marts
- `src/tests/test_staging_coverage.py` — add `stg_pitch_control__values` entry
- `src/tests/test_dbt_passes_kimball_migration.py` — extend for IDSSE is_progressive

### Files to modify (Taipy — verified in Phase 0)
- `hf_taipy_app/src/queries/defensive.py` — dual-read (queries fct_defensive_values / fct_defcon_*)
- `hf_taipy_app/src/queries/goalkeepers.py` — dual-read (queries fct_goalkeeper_stats / fct_gk_actions_detail)
- `hf_taipy_app/src/state/goalkeeper.py` — helper `resolve_gk_keys()` if needed

### Files to modify (HF cards)
- `docs/huggingface/dataset-cards/pitch-control-tracking.md` — 2026-07-22 dual-column window stanza
- `docs/huggingface/model-cards/defcon.md` — one-line edit re: new key columns

### Files to modify (memory + project state)
- `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` — staged-rollout table PR 6 row
- `~/.claude/projects/.../memory/project_kimball_migration_cycle.md`
- `~/.claude/projects/.../memory/project_kimball_pr6_shipped.md` (NEW)
- `~/.claude/projects/.../memory/MEMORY.md`

---

## Phase 0 — Pre-implementation verification

These verifications resolve open implementation-time questions in spec §10. They run BEFORE any code change so subsequent task code is correct.

### Task 0.1: Verify `fct_action_values.action_value_id` encodes `data_source`

**Files:**
- Read: `dbt_project/models/marts/fct_action_values.sql`

- [ ] **Step 1:** Grep the surrogate construction.

```bash
grep -n "action_value_id\|generate_surrogate_key" dbt_project/models/marts/fct_action_values.sql | head -20
```

Expected: a line like `{{ dbt_utils.generate_surrogate_key([...]) }} as action_value_id` listing hash inputs.

- [ ] **Step 2:** Record finding.

If `data_source` (or equivalent provider column) is in the hash inputs → Task 4.2 keeps `gk_action_id = cast(action_value_id as string)` passthrough. Mark verification ✓.

If `data_source` is NOT in the hash inputs → Task 4.2 must compute `gk_action_id = {{ dbt_utils.generate_surrogate_key(['action_value_id', 'data_source']) }}`. Note this in Task 4.2's code block before executing it.

### Task 0.2: Hyrum's Law grep for hardcoded surrogate IDs

**Files:**
- Search: entire repo

- [ ] **Step 1:** Grep for the three surrogate column names AS literal IDs (not column references).

```bash
grep -rn "defensive_value_id\s*=\s*['\"]" --include="*.py" --include="*.md" --include="*.sql" .
grep -rn "pressure_id\s*=\s*['\"]" --include="*.py" --include="*.md" --include="*.sql" .
grep -rn "gk_stat_id\s*=\s*['\"]" --include="*.py" --include="*.md" --include="*.sql" .
```

Expected: zero hits (these are internal row identifiers).

- [ ] **Step 2:** If any hits found, surface to user before proceeding to Phase 3 / 4. Decide: rebuild affected fixtures, or version-bump artifacts.

### Task 0.3: Narrow Taipy consumer list

**Files:**
- Search: `hf_taipy_app/src/`

- [ ] **Step 1:** Grep each of the 8 Taipy files for actual queries against the 5 in-scope mart names.

```bash
for f in hf_taipy_app/src/queries/defensive.py hf_taipy_app/src/state/goalkeeper.py hf_taipy_app/src/queries/goalkeepers.py hf_taipy_app/src/state/pitch_control.py hf_taipy_app/src/queries/tracking.py hf_taipy_app/src/filters.py hf_taipy_app/src/main.py hf_taipy_app/src/test_render.py; do
  echo "=== $f ==="
  grep -n "fct_defensive_values\|fct_defcon_actions\|fct_defcon_pressure\|fct_goalkeeper_stats\|fct_gk_actions_detail" "$f" || echo "(no query — import-only)"
done
```

- [ ] **Step 2:** Record which files actually query (need dual-read) vs. import-only (no change). Update Phase 7 task list to match.

### Task 0.4: Synced-table dual-defense audit

**Files:**
- Read: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1:** For each of the 5 marts, check whether `unique_combination_of_columns` schema test already exists.

```bash
grep -n "unique_combination_of_columns" dbt_project/models/marts/_marts__models.yml | head -30
grep -B1 "fct_defensive_values\|fct_defcon_actions\|fct_defcon_pressure\|fct_goalkeeper_stats\|fct_gk_actions_detail" dbt_project/models/marts/_marts__models.yml | grep -A20 "data_tests" | grep -A5 "unique_combination" || echo "Need to add"
```

- [ ] **Step 2:** Note which marts need the test added in Task 3.4 / 4.3.

- [ ] **Step 3:** For each mart, check whether the SQL has terminal `QUALIFY ROW_NUMBER() OVER (...) = 1`.

```bash
grep -n "QUALIFY ROW_NUMBER" dbt_project/models/marts/fct_defensive_values.sql dbt_project/models/marts/fct_defcon_actions.sql dbt_project/models/marts/fct_defcon_pressure.sql dbt_project/models/marts/fct_goalkeeper_stats.sql dbt_project/models/marts/fct_gk_actions_detail.sql
```

Expected: zero hits (none have it today). The 3 defcon marts and fct_goalkeeper_stats need terminal QUALIFY added since their grains can multiply across the new key dimensions.

### Task 0.5: `on_schema_change` config audit

**Files:**
- Read: 3 incremental defcon marts

- [ ] **Step 1:** Confirm each incremental mart has `on_schema_change` configured.

```bash
grep -n "on_schema_change\|materialized" dbt_project/models/marts/fct_defensive_values.sql dbt_project/models/marts/fct_defcon_actions.sql dbt_project/models/marts/fct_defcon_pressure.sql
```

- [ ] **Step 2:** Confirm `materialized='incremental'` on all three. Confirm `on_schema_change='append_new_columns'` is MISSING (will be added in Phase 3).

### Task 0.6: SkillCorner `match_id` prefix discovery

**Files:**
- Databricks SQL warehouse query

- [ ] **Step 1:** Run a sample query on `bronze.pitch_control_values` filtered to non-IDSSE non-Metrica rows.

```bash
uv run python -c "
import os
from databricks import sql

with sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
) as conn:
    cur = conn.cursor()
    cur.execute(\"\"\"
        SELECT DISTINCT match_id
        FROM soccer_analytics.bronze.pitch_control_values
        WHERE match_id NOT LIKE 'idsse_%' AND match_id NOT LIKE 'Sample_Game_%'
        LIMIT 50
    \"\"\")
    for row in cur.fetchall():
        print(row[0])
"
```

- [ ] **Step 2:** Identify the SkillCorner prefix pattern (likely a numeric ID or `sk_*`). Record the discovered prefix for Task 2.1's CASE clause.

If SkillCorner match_ids are pure numeric (no prefix), the CASE in Task 2.1 uses `ELSE 'skillcorner'` as the residual branch. If SkillCorner uses a distinct prefix, the CASE adds an explicit `WHEN match_id LIKE '<prefix>%'` branch.

### Task 0.7: dim_players coverage on Metrica anonymized DEFCON defenders

**Files:**
- Databricks SQL warehouse query

- [ ] **Step 1:** Run coverage check.

```bash
uv run python -c "
import os
from databricks import sql

with sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
) as conn:
    cur = conn.cursor()
    cur.execute(\"\"\"
        SELECT
            d.data_source,
            COUNT(*) AS total,
            COUNT(dp.player_key) AS resolved
        FROM soccer_analytics.dev_silver.stg_defcon__results d
        LEFT JOIN soccer_analytics.dev_gold.dim_players dp
          ON dp.provider = CASE d.data_source
                             WHEN 'statsbomb_360' THEN 'statsbomb'
                             WHEN 'metrica_tracking' THEN 'metrica'
                           END
         AND dp.native_player_id = CAST(d.defender_player_id AS STRING)
        GROUP BY d.data_source
    \"\"\")
    for row in cur.fetchall():
        print(row)
"
```

- [ ] **Step 2:** Record the resolution rate per data_source. Sets the threshold floor for `test_marts_kimball_contracts.py` parameterization on `fct_defensive_values.player_key` and `fct_defcon_pressure.player_key`. 360-synthetic defenders typically lower the rate; legitimate floor is the rate observed here.

---

## Phase 1 — IDSSE `is_progressive` (staging)

### Task 1.1: Update `stg_idsse__passes.sql` with ball-frame join

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__passes.sql`

- [ ] **Step 1:** Add `ball_at_end_frame` CTE between `hydrated` and `final`.

Insert AFTER the `hydrated as (...)` block, BEFORE `final as (...)`:

```sql
ball_at_end_frame as (

    select distinct
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,
        cast(period as int)                                     as period,
        cast(frame as int)                                      as frame,
        ball_x,
        ball_y
    from {{ ref('stg_idsse__tracking') }}
    where ball_x is not null
      and ball_y is not null

),

with_end_coords as (

    select
        h.*,
        {{ normalize_x('h.x', 'pitch_m') }}                     as _start_x_normalized,
        {{ normalize_y('h.y', 'pitch_m') }}                     as _start_y_normalized,
        bef.ball_x                                              as _end_x_normalized,
        bef.ball_y                                              as _end_y_normalized
    from hydrated h
    left join ball_at_end_frame bef
        on  bef.match_id = h.native_match_id
       and bef.period   = cast(h.period as int)
       and bef.frame    = cast(h.end_frame as int)

),
```

- [ ] **Step 2:** Update the `final` SELECT to source from `with_end_coords` and use the new normalized coords.

Replace:

```sql
final as (

    select
        cast(event_id as string)                                as event_id,
        ...
        {{ normalize_x('x', 'pitch_m') }}                       as start_x,
        {{ normalize_y('y', 'pitch_m') }}                       as start_y,
        cast(null as double)                                    as end_x,
        cast(null as double)                                    as end_y,
        ...
        false                                                   as is_progressive,
        ...
    from hydrated
)
```

with:

```sql
final as (

    select
        cast(event_id as string)                                as event_id,
        native_match_id                                         as match_id,
        cast(null as int)                                       as player_id,
        cast(null as int)                                       as team_id,
        cast(null as int)                                       as pass_recipient_id,
        cast(play_player as string)                             as player_id_native,
        cast(bridge_team_id as string)                          as team_id_native,
        cast(play_recipient as string)                          as pass_recipient_id_native,
        cast(team as string)                                    as team_side,
        cast(period as int)                                     as period,
        cast(floor(timestamp_seconds / 60.0) as int)            as minute,
        cast(cast(timestamp_seconds as int) % 60 as int)        as second,

        _start_x_normalized                                     as start_x,
        _start_y_normalized                                     as start_y,
        _end_x_normalized                                       as end_x,
        _end_y_normalized                                       as end_y,

        pass_direction                                          as pass_type,
        play_height                                             as pass_height,
        cast(null as string)                                    as body_part,
        cast(null as double)                                    as pass_length,
        radians(try_cast(play_play_angle as double))            as pass_angle_radians,

        case play_evaluation
            when 'successfullyCompleted' then 'Complete'
            when 'successful'            then 'Complete'
            when 'unsuccessful'          then 'Incomplete'
            else 'Unknown'
        end                                                     as pass_outcome,

        case
            when play_flat_cross is null then null
            when play_flat_cross = 'true' then true
            else false
        end                                                     as is_cross,
        cast(null as boolean)                                   as is_switch,
        pass_direction = 'throughBall'                          as is_through_ball,

        -- PR 6 (ADR-011): is_progressive derived via ball-frame tracking
        -- lookup. NULL when end_frame is null OR the tracking lookup misses
        -- (preserves "unknown" semantics rather than false-positive).
        case
            when _end_x_normalized is null or _end_y_normalized is null
                then cast(null as boolean)
            else {{ distance_to_goal('_end_x_normalized', '_end_y_normalized') }}
                 < {{ var('progressive_pass_ratio') }}
                   * {{ distance_to_goal('_start_x_normalized', '_start_y_normalized') }}
        end                                                     as is_progressive,

        play_ball_possession_phase,
        play_distance,
        play_evaluation,
        play_flat_cross,
        play_from_open_play,
        play_goal_keeper_action,
        play_penalty_box,
        play_play_angle,
        play_play_origin,
        play_rotation,
        play_semi_field,

        pass_direction,
        pass_free_kick_layup,
        pass_one_two,

        cross_side,

        timestamp_seconds,
        start_frame,
        end_frame,
        calculated_frame,
        calculated_timestamp,
        event_time,
        match_id_raw,
        x_source_position,
        y_source_position,
        x_position_from_tracking,
        y_position_from_tracking,
        kickoff_team_left,
        kickoff_team_right,
        kickoff_game_section,

        'idsse'                                                 as data_source

    from with_end_coords

)
```

- [ ] **Step 3:** Update the file's docstring "Known gaps" section. Replace:

```sql
--   * end_x / end_y NULL — the DFL <Play> row carries a start location
--     only. ELASTIC event-tracking sync (`stg_idsse__elastic_sync`,
--     pausa_enabled-gated) could enrich end coords; not in PR 2 scope.
--
--   * is_progressive = FALSE (requires end coordinates to evaluate).
```

with:

```sql
--   * end_x / end_y derived in PR 6 via ball-frame tracking lookup
--     (LEFT JOIN stg_idsse__tracking on (match_id, period, end_frame=frame)
--     to recover ball position at end-of-pass). NULL when end_frame is
--     null or the tracking lookup misses.
--
--   * is_progressive evaluated via the standard cross-provider rule
--     (distance_to_goal end < progressive_pass_ratio * distance_to_goal start)
--     applied to the derived end coords. NULL-preserving.
```

### Task 1.2: Verify dbt parse + schema test compile

- [ ] **Step 1:** Run dbt parse.

```bash
DATABRICKS_HOST=https://x.cloud.databricks.com \
DATABRICKS_HTTP_PATH=//sql/1.0/warehouses/x \
DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

(Placeholder env values are accepted because `dbt parse` does not connect to the warehouse.)

Expected: `Encountered N errors and 0 warnings` where N = 0. If errors mention `stg_idsse__passes`, fix and re-run.

- [ ] **Step 2:** Run dbt compile on the affected model.

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt compile --select stg_idsse__passes --project-dir dbt_project --profiles-dir dbt_project
```

Expected: compilation succeeds. Inspect `dbt_project/target/compiled/.../stg_idsse__passes.sql` to confirm the macros expanded correctly.

---

## Phase 2 — Pitch-control staging promotion

### Task 2.1: Update `stg_pitch_control__values.sql` with prefix CASE + dim_matches LEFT JOIN

**Files:**
- Modify: `dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql`

- [ ] **Step 1:** Replace the existing file content with the following (preserves dedup logic, adds provider derivation + match_key resolution).

Note: substitute `<SKILLCORNER_PREFIX>` with the value discovered in Task 0.6. If SkillCorner match_ids are pure numeric, replace the SkillCorner branch with `WHEN match_id RLIKE '^[0-9]+$' THEN 'skillcorner'`.

```sql
-- stg_pitch_control__values.sql
-- Clean and deduplicate pitch control values from the bronze layer +
-- derive Kimball-conformed FKs (PR 6, ADR-011).
--
-- Dedup: ROW_NUMBER partitioned by tracking_id, latest _ingested_at wins.
--
-- Provider derivation: data_source is derived from the match_id prefix
-- (idsse_*, Sample_Game_*, <SKILLCORNER_PREFIX>*). PR 7 will collapse this
-- to a passthrough once pitch_control_batch.py emits data_source natively.
--
-- match_key resolved via LEFT JOIN dim_matches on (provider, native_match_id).
-- LEFT JOIN with severity:warn — preserves row counts during the
-- 2026-07-22 dual-column window.
--
-- Consumer: notebooks/publish_datasets.py:248 INNER JOINs on tracking_id
-- to publish luxury-lakehouse/pitch-control-tracking. Additive columns
-- don't break the JOIN.

with source as (

    select * from {{ source('pitch_control', 'pitch_control_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by tracking_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

with_provider as (

    select
        cast(tracking_id as string)              as tracking_id,
        cast(match_id as string)                 as match_id,
        cast(pitch_control_value as double)      as pitch_control_value,
        _ingested_at,
        case
            when match_id like 'idsse_%'       then 'idsse'
            when match_id like 'Sample_Game_%' then 'metrica'
            -- SkillCorner branch — confirmed in Phase 0 Task 0.6:
            when match_id rlike '^[0-9]+$'     then 'skillcorner'
            else cast(null as string)
        end                                      as data_source

    from deduplicated
    where _row_num = 1

),

cleaned as (

    select
        wp.tracking_id,
        wp.match_id,
        wp.pitch_control_value,
        wp._ingested_at,
        wp.data_source,
        dm.match_key

    from with_provider wp
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = wp.data_source
       and dm.native_match_id = regexp_replace(
               wp.match_id,
               '^(idsse_|Sample_Game_)',
               ''
           )

)

select * from cleaned
```

- [ ] **Step 2:** Run dbt parse to confirm.

```bash
DATABRICKS_HOST=x DATABRICKS_HTTP_PATH=x DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 0 errors.

### Task 2.2: Update `_pitch_control__sources.yml` with `_ingested_at`

**Files:**
- Modify: `dbt_project/models/staging/pitch_control/_pitch_control__sources.yml`

- [ ] **Step 1:** Replace the file content.

```yaml
version: 2

sources:
  - name: pitch_control
    description: >
      Per-player per-frame pitch control values computed by the Spearman 2017
      physics-based model. Writer: src/ingestion/pitch_control_batch.py.
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 72, period: hour}

    tables:
      - name: pitch_control_values
        description: >
          Per-player per-frame pitch control probability [0,1] at the
          player's position. Grain: one row per (match, period, frame, player).
        columns:
          - name: tracking_id
            description: "FK to fct_tracking_frames.tracking_id (one row per player-frame)."
          - name: match_id
            description: >
              Match identifier — provider prefix encodes the source
              (idsse_*, Sample_Game_*, numeric for SkillCorner).
          - name: pitch_control_value
            description: >
              Home-team control probability [0,1] at this player's position
              under the Spearman 2017 model.
          - name: _ingested_at
            description: "UTC timestamp when this row was written to bronze."
```

### Task 2.3: Update `_pitch_control__models.yml` with new columns + tests + docstring

**Files:**
- Modify: `dbt_project/models/staging/pitch_control/_pitch_control__models.yml`

- [ ] **Step 1:** Replace the file content.

```yaml
version: 2

models:
  - name: stg_pitch_control__values
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Deduplicated pitch control values from bronze layer with Kimball-conformed
      FKs (PR 6, ADR-011). Grain: one row per (match, period, frame, player) —
      the same grain as fct_tracking_frames. Consumer: notebooks/publish_datasets.py
      INNER JOINs on tracking_id to publish luxury-lakehouse/pitch-control-tracking.
    columns:
      - name: tracking_id
        description: "Primary key — FK to fct_tracking_frames.tracking_id (one player-frame)."
        data_tests:
          - unique
          - not_null
      - name: match_id
        description: "Native match identifier with provider prefix (idsse_*, Sample_Game_*, numeric for SkillCorner)."
        data_tests:
          - not_null
      - name: pitch_control_value
        description: >
          Home-team control probability [0,1] at this player's position under
          the Spearman 2017 model. Per-player grain (NOT per-team).
        data_tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0.0
                max_value: 1.0
      - name: data_source
        description: >
          Provider derived from match_id prefix (PR 6). Sunset when
          pitch_control_batch.py emits data_source natively (PR 7).
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse', 'metrica', 'skillcorner']
      - name: match_key
        description: >
          Kimball surrogate FK to dim_matches.match_key (PR 6, ADR-011).
          Resolved via LEFT JOIN on (provider, native_match_id).
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn
                where: "match_key IS NOT NULL"
      - name: _ingested_at
        description: "UTC timestamp when this row was written to bronze."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - match_id
            - tracking_id
```

### Task 2.4: Add `bronze.pitch_control_values` to `test_bronze_live_schema.py`

**Files:**
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1:** Read the current test structure to identify the insertion point.

```bash
grep -n "BRONZE_TABLES\|bronze\\." src/tests/test_bronze_live_schema.py | head -20
```

- [ ] **Step 2:** Add a new entry mirroring an existing bronze table. The exact insertion depends on the file's structure (parametrize list, dict, etc.). Add an entry like:

```python
("pitch_control_values", {
    "tracking_id": "string",
    "match_id": "string",
    "pitch_control_value": "double",
    "_ingested_at": "timestamp",
}),
```

(Adapt to the file's existing pattern — pull a similar bronze entry as the template.)

- [ ] **Step 3:** Verify the test imports and structure remain valid.

```bash
uv run python -m py_compile src/tests/test_bronze_live_schema.py
```

Expected: silent success.

### Task 2.5: Create `test_pitch_control_bronze_coverage.py` (parser-level)

**Files:**
- Create: `src/tests/test_pitch_control_bronze_coverage.py`

- [ ] **Step 1:** Write the test file.

```python
# ruff: noqa: S608 — test_bronze_coverage uses dbt parse-level introspection.
"""Parser-level bronze→staging completeness test for pitch_control.

Asserts every bronze.pitch_control_values column is surfaced in
stg_pitch_control__values. Per feedback_coverage_test_pattern.

Mirrors test_idsse_bronze_coverage.py / test_metrica_bronze_coverage.py
shape; uses the same coverage_utils helpers.
"""

from __future__ import annotations

import pytest

from tests.coverage_utils import (
    assert_bronze_columns_surfaced_in_staging,
    parse_dbt_manifest,
)


@pytest.fixture(scope="module")
def manifest() -> dict:
    return parse_dbt_manifest()


def test_bronze_columns_surfaced(manifest: dict) -> None:
    """Every bronze.pitch_control_values column must appear in stg_pitch_control__values."""
    assert_bronze_columns_surfaced_in_staging(
        manifest,
        bronze_source=("pitch_control", "pitch_control_values"),
        staging_model="stg_pitch_control__values",
        # Columns expected to be derived (added in staging beyond bronze):
        derived_in_staging={"data_source", "match_key"},
    )
```

- [ ] **Step 2:** Run the test to verify it passes (assuming `coverage_utils.assert_bronze_columns_surfaced_in_staging` already exists per `reference_coverage_utils_module`).

```bash
uv run pytest src/tests/test_pitch_control_bronze_coverage.py -v
```

Expected: PASS. If `coverage_utils` lacks the helper, follow the existing `test_idsse_bronze_coverage.py` pattern as a template.

### Task 2.6: Add `stg_pitch_control__values` to `test_staging_coverage.py`

**Files:**
- Modify: `src/tests/test_staging_coverage.py`

- [ ] **Step 1:** Read the file to identify the staging model registry.

```bash
grep -n "stg_idsse\|stg_metrica\|STAGING_MODELS" src/tests/test_staging_coverage.py | head -20
```

- [ ] **Step 2:** Add an entry for `stg_pitch_control__values` following the existing pattern (likely a parametrize list of (model_name, expected_columns) tuples or similar).

The expected staging columns (from Task 2.3 YAML):

```python
"stg_pitch_control__values": [
    "tracking_id",
    "match_id",
    "pitch_control_value",
    "data_source",
    "match_key",
    "_ingested_at",
],
```

### Task 2.7: Update HF dataset card `pitch-control-tracking.md`

**Files:**
- Modify: `docs/huggingface/dataset-cards/pitch-control-tracking.md`

- [ ] **Step 1:** Add a 2026-07-22 dual-column window stanza after the existing "Limitations" section. Insert before "## Citation":

```markdown
## Dual-Column Window (2026-04-26 → 2026-07-22)

The lakehouse is migrating to Kimball-conformed surrogate keys per ADR-011.
The upstream `stg_pitch_control__values` model now carries `match_key` (BIGINT,
FK to `dim_matches`) and `data_source` (`idsse`, `metrica`, `skillcorner`)
alongside the existing `match_id`. **The published HF dataset payload remains
unchanged in this window** — current consumers see exactly the columns
documented above.

The next dataset version (planned 2026-07-22, alongside PR 8) will add
`match_key` and `data_source` to the published parquet payload, and
deprecate `match_id` in favour of `match_key`. Schema changes will be
announced in the dataset's HF revision history.
```

---

## Phase 3 — Defensive marts migration

### Task 3.1: Update `fct_defensive_values.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_defensive_values.sql`

- [ ] **Step 1:** Replace the file content.

```sql
{{ config(
    materialized='incremental',
    unique_key='defensive_value_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_defensive_values.sql
-- Per-defender per-match defensive valuation summary.
--
-- Aggregates DEFCON-lite credits into per-match totals and breakdowns
-- by credit type. Enables defender ranking and comparison.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per match per data_source.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key BIGINT FK → dim_matches.match_key. Resolved via provider CASE
--     on data_source ('statsbomb_360' → 'statsbomb', 'metrica_tracking' → 'metrica').
--   - team_key BIGINT FK → dim_teams.team_key (defender's team).
--   - player_key BIGINT FK → dim_players.player_key (defender).
--   - data_source folded into surrogate hash (defensive_value_id) — multi-provider
--     correctness fix; existing IDs CHANGE on first --full-refresh rebuild.
-- LEFT JOIN with relationships severity:warn during 2026-07-22 dual-column window.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

defcon_with_provider as (

    select
        *,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
        end as _provider
    from defcon

),

credit_agg as (

    select
        defender_player_id,
        match_id,
        competition_id,
        season_id,
        defender_team_id,
        data_source,
        _provider,

        sum(defcon_value)                                           as total_defcon_value,
        count(*)                                                    as total_credits,

        sum(case when credit_type = 'intercept' then defcon_value else 0 end) as intercept_value,
        sum(case when credit_type = 'concede' then defcon_value else 0 end)   as concede_value,
        sum(case when credit_type = 'disturb' then defcon_value else 0 end)   as disturb_value,
        sum(case when credit_type = 'deter' then defcon_value else 0 end)     as deter_value,

        sum(case when credit_type = 'intercept' then 1 else 0 end)           as intercept_count,
        sum(case when credit_type = 'concede' then 1 else 0 end)             as concede_count,
        sum(case when credit_type = 'disturb' then 1 else 0 end)             as disturb_count,
        sum(case when credit_type = 'deter' then 1 else 0 end)               as deter_count,

        sum(case when confidence = 'high' then 1 else 0 end)                 as high_confidence_count,
        sum(case when confidence = 'approximate' then 1 else 0 end)           as approx_confidence_count

    from defcon_with_provider
    group by defender_player_id, match_id, competition_id, season_id,
             defender_team_id, data_source, _provider

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'ca.defender_player_id',
            'ca.match_id',
            'ca.data_source'
        ]) }}                                                       as defensive_value_id,

        ca.defender_player_id                                       as player_id,
        ca.match_id,
        ca.competition_id,
        ca.season_id,
        ca.defender_team_id                                         as team_id,
        ca.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dt.team_key,
        dp.player_key,

        ca.total_defcon_value,
        ca.total_credits,

        ca.intercept_value,
        ca.concede_value,
        ca.disturb_value,
        ca.deter_value,

        ca.intercept_count,
        ca.concede_count,
        ca.disturb_count,
        ca.deter_count,

        ca.high_confidence_count,
        ca.approx_confidence_count,

        current_timestamp()                                         as _loaded_at

    from credit_agg ca
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = ca._provider
       and dm.native_match_id = ca.match_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = ca._provider
       and dt.native_team_id = cast(ca.defender_team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = ca._provider
       and dp.native_player_id = cast(ca.defender_player_id as string)

)

select * from final

{% else %}

-- DEFCON-lite not enabled — produce empty table with correct schema
select
    cast(null as string)    as defensive_value_id,
    cast(null as int)       as player_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as int)       as team_id,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
    cast(null as double)    as total_defcon_value,
    cast(null as int)       as total_credits,
    cast(null as double)    as intercept_value,
    cast(null as double)    as concede_value,
    cast(null as double)    as disturb_value,
    cast(null as double)    as deter_value,
    cast(null as int)       as intercept_count,
    cast(null as int)       as concede_count,
    cast(null as int)       as disturb_count,
    cast(null as int)       as deter_count,
    cast(null as int)       as high_confidence_count,
    cast(null as int)       as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
```

### Task 3.2: Update `fct_defcon_actions.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_defcon_actions.sql`

- [ ] **Step 1:** Replace the file content (preserves event-level grain; surrogate stable; adds 4 keys including `action_player_key`).

```sql
{{ config(
    materialized='incremental',
    unique_key='defcon_action_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_defcon_actions.sql
-- Per-defender per-action defensive credits for timeline visualization.
--
-- Contains the full granularity of DEFCON-lite results: one row per
-- defender per credited action. Powers the Match Timeline view in
-- the Streamlit Defensive Valuation page.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per defender per action.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key, team_key (defender), player_key (defender),
--     action_player_key (action-performing player) all LEFT JOIN-resolved
--     via provider CASE on data_source.
--   - defcon_action_id surrogate UNCHANGED — already includes data_source.
-- LEFT JOIN with relationships severity:warn during 2026-07-22 dual-column window.
-- 360-synthetic defenders: defender_player_key may be NULL (synthetic IDs
-- don't resolve in dim_players); action_player_key resolves cleanly.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

defcon_with_provider as (

    select
        *,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
        end as _provider
    from defcon

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'd.event_id',
            'd.defender_player_id',
            'd.data_source'
        ]) }}                                                       as defcon_action_id,

        d.event_id,
        d.match_id,
        d.competition_id,
        d.season_id,
        d.defender_player_id                                        as player_id,
        d.defender_team_id                                          as team_id,
        d.defender_x,
        d.defender_y,
        d.action_player_id,
        d.action_type,
        d.action_x,
        d.action_y,
        d.credit_type,
        d.confidence,
        d.defcon_value,
        d.dist_to_ball,
        d.pitch_control_at_action,
        d.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dt.team_key,
        dp_def.player_key,
        dp_act.player_key                                           as action_player_key,

        current_timestamp()                                         as _loaded_at

    from defcon_with_provider d
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = d._provider
       and dm.native_match_id = d.match_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = d._provider
       and dt.native_team_id = cast(d.defender_team_id as string)
    left join {{ ref('dim_players') }} dp_def
        on  dp_def.provider = d._provider
       and dp_def.native_player_id = cast(d.defender_player_id as string)
    left join {{ ref('dim_players') }} dp_act
        on  dp_act.provider = d._provider
       and dp_act.native_player_id = cast(d.action_player_id as string)

)

select * from final

{% else %}

select
    cast(null as string)    as defcon_action_id,
    cast(null as string)    as event_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as int)       as player_id,
    cast(null as int)       as team_id,
    cast(null as double)    as defender_x,
    cast(null as double)    as defender_y,
    cast(null as int)       as action_player_id,
    cast(null as string)    as action_type,
    cast(null as double)    as action_x,
    cast(null as double)    as action_y,
    cast(null as string)    as credit_type,
    cast(null as string)    as confidence,
    cast(null as double)    as defcon_value,
    cast(null as double)    as dist_to_ball,
    cast(null as double)    as pitch_control_at_action,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
    cast(null as bigint)    as action_player_key,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
```

### Task 3.3: Update `fct_defcon_pressure.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_defcon_pressure.sql`

- [ ] **Step 1:** Replace the file content.

```sql
{{ config(
    materialized='incremental',
    unique_key='pressure_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_defcon_pressure.sql
-- Per-attacker per-match defensive pressure summary.
--
-- Aggregates DEFCON-lite credits by action_player_id (the real player
-- who performed the action) rather than defender_player_id (synthetic
-- for 360 freeze-frame data). This provides a "pressure received" view:
-- how much defensive attention each attacker attracted.
--
-- Coordinate system: SPADL 105x68 meters.
-- One row per attacker per match per data_source.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key + player_key (action attacker) LEFT JOIN-resolved
--     via provider CASE on data_source.
--   - data_source folded into surrogate hash (pressure_id) — existing IDs
--     CHANGE on first --full-refresh rebuild.
-- No team_key — action_team_id is not present on the source grain.
-- LEFT JOIN with relationships severity:warn during 2026-07-22 dual-column window.

{% if var('defcon_enabled', false) %}

with defcon as (

    select * from {{ ref('stg_defcon__results') }}
    where action_player_id is not null
    {% if is_incremental() %}
    and match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

defcon_with_provider as (

    select
        *,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
        end as _provider
    from defcon

),

pressure_agg as (

    select
        action_player_id,
        match_id,
        competition_id,
        season_id,
        data_source,
        _provider,

        sum(defcon_value)                                               as total_pressure,
        count(*)                                                        as total_defensive_actions,

        sum(case when credit_type = 'intercept' then defcon_value else 0 end) as intercept_pressure,
        sum(case when credit_type = 'concede' then defcon_value else 0 end)   as concede_pressure,
        sum(case when credit_type = 'disturb' then defcon_value else 0 end)   as disturb_pressure,
        sum(case when credit_type = 'deter' then defcon_value else 0 end)     as deter_pressure,

        sum(case when credit_type = 'intercept' then 1 else 0 end)           as intercept_count,
        sum(case when credit_type = 'concede' then 1 else 0 end)             as concede_count,
        sum(case when credit_type = 'disturb' then 1 else 0 end)             as disturb_count,
        sum(case when credit_type = 'deter' then 1 else 0 end)               as deter_count,

        sum(case when confidence = 'high' then 1 else 0 end)                 as high_confidence_count,
        sum(case when confidence = 'approximate' then 1 else 0 end)           as approx_confidence_count

    from defcon_with_provider
    group by action_player_id, match_id, competition_id, season_id, data_source, _provider

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pa.action_player_id',
            'pa.match_id',
            'pa.data_source'
        ]) }}                                                           as pressure_id,

        pa.action_player_id                                             as player_id,
        pa.match_id,
        pa.competition_id,
        pa.season_id,
        pa.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        dm.match_key,
        dp.player_key,

        pa.total_pressure,
        pa.total_defensive_actions,

        pa.intercept_pressure,
        pa.concede_pressure,
        pa.disturb_pressure,
        pa.deter_pressure,

        pa.intercept_count,
        pa.concede_count,
        pa.disturb_count,
        pa.deter_count,

        pa.high_confidence_count,
        pa.approx_confidence_count,

        current_timestamp()                                             as _loaded_at

    from pressure_agg pa
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = pa._provider
       and dm.native_match_id = pa.match_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = pa._provider
       and dp.native_player_id = cast(pa.action_player_id as string)

)

select * from final

{% else %}

select
    cast(null as string)    as pressure_id,
    cast(null as int)       as player_id,
    cast(null as string)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as player_key,
    cast(null as double)    as total_pressure,
    cast(null as int)       as total_defensive_actions,
    cast(null as double)    as intercept_pressure,
    cast(null as double)    as concede_pressure,
    cast(null as double)    as disturb_pressure,
    cast(null as double)    as deter_pressure,
    cast(null as int)       as intercept_count,
    cast(null as int)       as concede_count,
    cast(null as int)       as disturb_count,
    cast(null as int)       as deter_count,
    cast(null as int)       as high_confidence_count,
    cast(null as int)       as approx_confidence_count,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
```

### Task 3.4: Update `_marts__models.yml` for the 3 defcon marts

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1:** Append `match_key`, `team_key` (defensive_values + defcon_actions only), `player_key`, `action_player_key` (defcon_actions only) to each mart's columns block. Add `unique_combination_of_columns` schema test on each mart's grain.

For `fct_defensive_values` — add after `_loaded_at`:

```yaml
      - name: match_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_matches.match_key (PR 6, ADR-011).
          Resolved via provider CASE on data_source.
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn
                where: "match_key IS NOT NULL"
      - name: team_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_teams.team_key (defender's team).
        data_tests:
          - relationships:
              to: ref('dim_teams')
              field: team_key
              config:
                severity: warn
                where: "team_key IS NOT NULL"
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players.player_key (defender).
          May be NULL for 360-synthetic defenders.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
                where: "player_key IS NOT NULL"
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - player_id
            - match_id
            - data_source
```

For `fct_defcon_actions` — add corresponding entries (match_key, team_key, player_key, action_player_key). Grain test:

```yaml
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - event_id
            - player_id
            - data_source
```

For `fct_defcon_pressure` — add match_key, player_key (no team_key). Grain test:

```yaml
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - player_id
            - match_id
            - data_source
```

- [ ] **Step 2:** Run dbt parse to verify YAML validity.

```bash
DATABRICKS_HOST=x DATABRICKS_HTTP_PATH=x DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 0 errors.

---

## Phase 4 — Goalkeeper marts migration

### Task 4.1: Update `fct_goalkeeper_stats.sql` — data_source propagation + new keys + retire dim_matches bridges

**Files:**
- Modify: `dbt_project/models/marts/fct_goalkeeper_stats.sql`

- [ ] **Step 1:** Update the `gk_actions` CTE to project `data_source`.

Replace:

```sql
gk_actions as (

    select
        av.match_id,
        av.player_id,
        av.team_id,
        av.competition_id,
        av.season_id,
        av.action_type,
        av.action_result,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y

    from {{ ref('fct_action_values') }} av
    inner join gk_players gk
        on av.player_id = gk.player_id

),
```

with:

```sql
gk_actions as (

    select
        av.match_id,
        av.match_key,                       -- PR 6: source already has this from PR 4b migration
        av.player_id,
        av.team_id,
        av.competition_id,
        av.season_id,
        av.action_type,
        av.action_result,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,
        av.data_source                      -- PR 6: PROPAGATE data_source (was dropped here pre-PR-6)

    from {{ ref('fct_action_values') }} av
    inner join gk_players gk
        on av.player_id = gk.player_id

),
```

- [ ] **Step 2:** Update `gk_matches` to group by `data_source` and project `match_key`.

Replace:

```sql
gk_matches as (

    select
        player_id,
        match_id,
        min(team_id)        as team_id,
        min(competition_id) as competition_id,
        min(season_id)      as season_id
    from gk_actions
    group by player_id, match_id

),
```

with:

```sql
gk_matches as (

    -- PR 6: data_source added to grain. min(match_key) is safe because
    -- match_key is functionally determined by (data_source, match_id).
    select
        player_id,
        match_id,
        data_source,
        min(match_key)      as match_key,
        min(team_id)        as team_id,
        min(competition_id) as competition_id,
        min(season_id)      as season_id
    from gk_actions
    group by player_id, match_id, data_source

),
```

- [ ] **Step 3:** Update every CTE that JOINs to `gk_matches` to include `data_source` in the equality.

Pattern: every `on gm.player_id = X.player_id and gm.match_id = X.match_id` becomes `on gm.player_id = X.player_id and gm.match_id = X.match_id and gm.data_source = X.data_source`. The dependent CTEs are: `pass_stats` (joins via gk_actions which carries data_source), `collection_stats` (same), `sweeper_stats` (same), `psxg_agg` (joins gk_matches directly), `save_stats` (joins gk_matches + sub-CTEs), `minutes` (downstream of gk_actions).

Update `pass_stats`, `collection_stats`, `sweeper_stats` — they group by `(player_id, match_id)` — change to `(player_id, match_id, data_source)` to match grain:

```sql
pass_stats as (
    select
        player_id,
        match_id,
        data_source,                                                   -- PR 6
        ... (existing aggregates)
    from gk_passes
    group by player_id, match_id, data_source                          -- PR 6
),
```

(Apply the same pattern to `collection_stats`, `sweeper_stats`, and any other gk_actions-derived CTE.)

- [ ] **Step 4:** Retire the dim_matches bridges in `shot_save_stats` and `psxg_shots`.

Replace:

```sql
shot_save_stats as (

    select
        gm.player_id,
        gm.match_id,
        cast(count(*) as bigint)                                        as saves

    from (
        select s.*, try_cast(dm.native_match_id as bigint) as match_id
        from {{ ref('fct_shots') }} s
        left join {{ ref('dim_matches') }} dm on s.match_key = dm.match_key
    ) s
    inner join gk_matches gm
        on s.match_id = gm.match_id
        and s.team_id != gm.team_id
    where s.shot_outcome in ('Saved', 'Saved Off Target', 'Saved to Post')
    group by gm.player_id, gm.match_id

),
```

with (joins on match_key directly):

```sql
shot_save_stats as (

    -- PR 6: dim_matches bridge retired — gk_matches now carries match_key
    -- from fct_action_values (PR 4b). JOIN fct_shots directly.
    select
        gm.player_id,
        gm.match_id,
        gm.data_source,
        cast(count(*) as bigint)                                        as saves

    from {{ ref('fct_shots') }} s
    inner join gk_matches gm
        on s.match_key = gm.match_key
        and s.team_id != gm.team_id
    where s.shot_outcome in ('Saved', 'Saved Off Target', 'Saved to Post')
    group by gm.player_id, gm.match_id, gm.data_source

),
```

Apply the same simplification to `psxg_shots`:

```sql
psxg_shots as (

    -- PR 6: dim_matches bridge retired — gk_matches carries match_key.
    select
        psxg.event_id,
        psxg.match_id,
        psxg.psxg,
        shots.team_id    as shooter_team_id,
        shots.shot_outcome
    from {{ ref('stg_psxg__predictions') }} psxg
    inner join {{ ref('fct_shots') }} shots
        on shots.shot_id = psxg.event_id
        and cast(shots.match_key as string) = cast(psxg.match_id as string)  -- TODO verify psxg match_id semantics

),
```

NOTE: `stg_psxg__predictions.match_id` is currently a STRING per existing code. Verify whether it carries native_match_id or match_key during implementation; if native, route through dim_matches, else simplify as above.

- [ ] **Step 5:** Update the `final` SELECT to include `data_source`, `match_key`, `team_key`, `player_key`, and update `gk_stat_id` hash.

Replace:

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'gm.player_id',
            'gm.match_id'
        ]) }}                                                           as gk_stat_id,

        gm.player_id,
        gm.match_id,
        gm.team_id,
        gm.competition_id,
        gm.season_id,
        ...
```

with:

```sql
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'gm.player_id',
            'gm.match_id',
            'gm.data_source'
        ]) }}                                                           as gk_stat_id,

        gm.player_id,
        gm.match_id,
        gm.team_id,
        gm.competition_id,
        gm.season_id,
        gm.data_source,                                                 -- PR 6: NEW PERMANENT column

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        gm.match_key,
        dt.team_key,
        dp.player_key,

        ...
```

- [ ] **Step 6:** Add LEFT JOINs to `dim_teams` and `dim_players` in the `final` FROM clause.

Append before the closing `)` of `final`:

```sql
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = gm.data_source
       and dt.native_team_id = cast(gm.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = gm.data_source
       and dp.native_player_id = cast(gm.player_id as string)
```

- [ ] **Step 7:** Update the `else` branch (goalkeeper not enabled) to include the new columns.

Add to the empty schema:

```sql
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
```

- [ ] **Step 8:** Run dbt parse to verify.

```bash
DATABRICKS_HOST=x DATABRICKS_HTTP_PATH=x DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt compile --select fct_goalkeeper_stats --project-dir dbt_project --profiles-dir dbt_project
```

Expected: compile succeeds. Inspect `dbt_project/target/compiled/.../fct_goalkeeper_stats.sql` to verify all gk_matches references include data_source.

### Task 4.2: Update `fct_gk_actions_detail.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_gk_actions_detail.sql`

- [ ] **Step 1:** Verify the surrogate construction for `gk_action_id` per Task 0.1 finding.

If `fct_action_values.action_value_id` already encodes `data_source` (Task 0.1 ✓) → keep `cast(action_value_id as string)` passthrough.

If NOT → replace with `{{ dbt_utils.generate_surrogate_key(['action_value_id', 'data_source']) }}`.

- [ ] **Step 2:** Replace the file content (assuming Task 0.1 ✓; adjust the surrogate line otherwise):

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['match_key']
) }}
-- fct_gk_actions_detail.sql
-- Pre-filtered goalkeeper pass and goalkick actions for the Taipy Goalkeeper
-- Analytics "Distribution" sub-view.
--
-- (Header comment unchanged — see prior version for measurement context.)
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - match_key inherited from fct_action_values (PR 4b migration).
--   - team_key + player_key LEFT JOIN-resolved via provider CASE on data_source.

with gk_players as (

    select distinct player_id
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'

),

gk_actions as (

    select
        av.action_value_id,
        av.match_id,
        av.match_key,                                                   -- PR 6
        av.competition_id,
        av.season_id,
        av.team_id,
        av.player_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,
        av.action_type,
        av.action_result,
        av.data_source
    from {{ ref('fct_action_values') }} av
    inner join gk_players gk on av.player_id = gk.player_id
    where av.action_type in ('goalkick', 'pass')

),

final as (

    select
        cast(action_value_id as string)               as gk_action_id,
        cast(match_id as bigint)                      as match_id,
        match_key,                                                      -- PR 6
        cast(competition_id as int)                   as competition_id,
        cast(season_id as int)                        as season_id,
        cast(team_id as int)                          as team_id,
        cast(player_id as int)                        as player_id,
        dt.team_key,                                                    -- PR 6
        dp.player_key,                                                  -- PR 6
        cast(period as int)                           as period,
        cast(time_seconds as double)                  as time_seconds,
        cast(minute as int)                           as minute,
        cast(second as int)                           as second,
        cast(start_x as double)                       as start_x,
        cast(start_y as double)                       as start_y,
        cast(end_x as double)                         as end_x,
        cast(end_y as double)                         as end_y,
        cast(action_type as string)                   as action_type,
        cast(action_result as string)                 as action_result,
        cast(data_source as string)                   as data_source,
        current_timestamp()                           as _loaded_at

    from gk_actions ga
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = ga.data_source
       and dt.native_team_id = cast(ga.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = ga.data_source
       and dp.native_player_id = cast(ga.player_id as string)

)

select * from final
```

### Task 4.3: Update `_marts__models.yml` for both GK marts

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1:** Add `data_source`, `match_key`, `team_key`, `player_key` to `fct_goalkeeper_stats` columns block.

After the existing columns, before the closing of the model block:

```yaml
      - name: data_source
        data_type: string
        description: >
          Provider attribution (PR 6 — ADR-011). Closes a latent multi-provider
          correctness gap: pre-PR-6 SB and WS BIGINT match_ids could collide
          on (player_id, match_id).
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout']
      - name: match_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_matches.match_key (PR 6, ADR-011).
          Inherited from fct_action_values via gk_matches (PR 4b migration
          of fct_action_values + PR 6 propagation).
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn
                where: "match_key IS NOT NULL"
      - name: team_key
        data_type: bigint
        description: "Kimball surrogate FK to dim_teams.team_key (GK's team)."
        data_tests:
          - relationships:
              to: ref('dim_teams')
              field: team_key
              config:
                severity: warn
                where: "team_key IS NOT NULL"
      - name: player_key
        data_type: bigint
        description: "Kimball surrogate FK to dim_players.player_key (the GK)."
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
                where: "player_key IS NOT NULL"
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - player_id
            - match_id
            - data_source
```

- [ ] **Step 2:** Add same key columns to `fct_gk_actions_detail` (no `data_source` since it's already declared).

Add after the existing `data_source` column entry:

```yaml
      - name: match_key
        data_type: bigint
        description: "Kimball surrogate FK inherited from fct_action_values."
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn
                where: "match_key IS NOT NULL"
      - name: team_key
        data_type: bigint
        description: "Kimball surrogate FK to dim_teams.team_key."
        data_tests:
          - relationships:
              to: ref('dim_teams')
              field: team_key
              config:
                severity: warn
                where: "team_key IS NOT NULL"
      - name: player_key
        data_type: bigint
        description: "Kimball surrogate FK to dim_players.player_key (GK)."
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
                where: "player_key IS NOT NULL"
```

- [ ] **Step 3:** Run dbt parse.

```bash
DATABRICKS_HOST=x DATABRICKS_HTTP_PATH=x DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 0 errors.

---

## Phase 5 — Test harness rename + new tests

### Task 5.1: Rename + parameterize `test_marts_player_key_contracts.py` → `test_marts_kimball_contracts.py`

**Files:**
- Delete: `src/tests/test_marts_player_key_contracts.py`
- Create: `src/tests/test_marts_kimball_contracts.py`

- [ ] **Step 1:** Delete the old file.

```bash
git rm src/tests/test_marts_player_key_contracts.py
```

(Note: `git rm` is fine here — single-commit convention means the deletion lands in the same commit as the new file; no orphaned commit.)

- [ ] **Step 2:** Create `src/tests/test_marts_kimball_contracts.py`.

Use the threshold values measured in Phase 0 Task 0.7. Default to 0.99 unless the measurement shows lower.

```python
# ruff: noqa: S608 — _CASES are module-level tuples, not user input.
"""PR 6 live invariants — every Kimball-keyed mart's surrogate FKs must
be populated.

Parameterized over (mart, key_column, threshold) tuples covering PR 5b's
six embedding marts (player_key) + PR 6's five defcon/GK marts (player_key,
team_key, match_key, action_player_key where present).

Skips when DATABRICKS_* env vars are absent (air-gapped CI). Otherwise
runs against dev_gold via the standard SQL warehouse connection.
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

# (mart, key_column, non_null_rate_threshold)
# Threshold of 0.99 is the default; lower thresholds (e.g. 0.70 for
# defender_player_key on fct_defcon_actions) reflect floors measured at
# implementation time per spec §10 #8.
_CASES: tuple[tuple[str, str, float], ...] = (
    # PR 5b — player_key on six embedding marts
    ("fct_player_embeddings", "player_key", 0.99),
    ("fct_player_embeddings_season", "player_key", 0.99),
    ("fct_player_embeddings_career", "player_key", 0.99),
    ("fct_player_embeddings_season_360", "player_key", 0.99),
    ("fct_player_embeddings_career_360", "player_key", 0.99),
    ("fct_player_percentiles", "player_key", 0.99),
    # PR 6 — defensive marts
    ("fct_defensive_values", "match_key", 0.99),
    ("fct_defensive_values", "team_key", 0.99),
    ("fct_defensive_values", "player_key", 0.99),  # raise/lower per Task 0.7
    ("fct_defcon_actions", "match_key", 0.99),
    ("fct_defcon_actions", "team_key", 0.99),
    ("fct_defcon_actions", "player_key", 0.70),    # 360-synthetic floor — adjust per Task 0.7
    ("fct_defcon_actions", "action_player_key", 0.99),
    ("fct_defcon_pressure", "match_key", 0.99),
    ("fct_defcon_pressure", "player_key", 0.99),
    # PR 6 — goalkeeper marts
    ("fct_goalkeeper_stats", "match_key", 0.99),
    ("fct_goalkeeper_stats", "team_key", 0.99),
    ("fct_goalkeeper_stats", "player_key", 0.99),
    ("fct_gk_actions_detail", "match_key", 0.99),
    ("fct_gk_actions_detail", "team_key", 0.99),
    ("fct_gk_actions_detail", "player_key", 0.99),
)


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


@requires_databricks
@pytest.mark.parametrize(("mart", "key_column", "threshold"), _CASES)
def test_kimball_key_populated(conn, mart: str, key_column: str, threshold: float) -> None:
    """Each (mart, key) pair must have non-NULL rate >= threshold on dev_gold."""
    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    cur = conn.cursor()
    cur.execute(f"SELECT count(*) AS total, count({key_column}) AS non_null FROM {table}")
    row = cur.fetchall()[0]
    assert row is not None, f"empty result on {table}"
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        # Mart unbuilt (e.g., feature flag off) — skip.
        pytest.skip(f"{mart} has zero rows — feature gate may be off")

    rate = non_null / total
    assert rate >= threshold, (
        f"{mart}.{key_column}: non-NULL rate {rate:.4f} below {threshold} threshold "
        f"(total={total}, non_null={non_null}). Investigate dim resolution."
    )
```

- [ ] **Step 3:** Compile-check.

```bash
uv run python -m py_compile src/tests/test_marts_kimball_contracts.py
```

Expected: silent success.

### Task 5.2: Create `test_idsse_is_progressive_coverage.py`

**Files:**
- Create: `src/tests/test_idsse_is_progressive_coverage.py`

- [ ] **Step 1:** Write the file. Use the threshold measured at first dev rebuild (target ≥0.95 per spec §4.1; tightened post-measurement).

```python
# ruff: noqa: S608 — module-level constant interpolation, not user input.
"""PR 6 live invariant — IDSSE rows in fct_passes must have non-NULL
is_progressive populated by the ball-frame tracking lookup.

Pre-PR-6 IDSSE rows had `false` literal (start coords only, no end coords).
Post-PR-6 they evaluate the standard cross-provider distance_to_goal rule
on derived end coords. Threshold reflects coverage achievable given
end_frame availability and tracking-frame match-up rate.
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

# Threshold tuned at first dev rebuild — see spec §10 #7. Raise after measurement.
_THRESHOLD = 0.95


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


@requires_databricks
def test_idsse_is_progressive_coverage(conn) -> None:
    """At least 95% of IDSSE rows in fct_passes must have non-NULL is_progressive."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) AS total, count(is_progressive) AS non_null "
        "FROM soccer_analytics.dev_gold.fct_passes "
        "WHERE data_source = 'idsse'"
    )
    row = cur.fetchall()[0]
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        pytest.skip("fct_passes has zero IDSSE rows — pipeline not yet built")

    rate = non_null / total
    assert rate >= _THRESHOLD, (
        f"IDSSE is_progressive non-NULL rate {rate:.4f} below {_THRESHOLD} threshold "
        f"(total={total}, non_null={non_null}). Investigate ball_at_end_frame JOIN."
    )
```

- [ ] **Step 2:** Compile-check.

```bash
uv run python -m py_compile src/tests/test_idsse_is_progressive_coverage.py
```

Expected: silent success.

### Task 5.3: Add 5 PR-6 marts to `test_marts_live_schema.py`

**Files:**
- Modify: `src/tests/test_marts_live_schema.py`

- [ ] **Step 1:** Read the file structure.

```bash
grep -n "MARTS\|marts\\.\|expected_columns" src/tests/test_marts_live_schema.py | head -20
```

- [ ] **Step 2:** Add expected-column entries for the 5 PR-6 marts following the existing pattern. The expected columns include the additive new ones (`match_key`, `team_key`, `player_key`, `data_source` for fct_goalkeeper_stats, `action_player_key` for fct_defcon_actions).

(Exact shape depends on the file's existing pattern — adapt accordingly. Mirror the entries for fct_player_stats / fct_player_embeddings to ensure consistency.)

### Task 5.4: Extend `test_dbt_passes_kimball_migration.py` for IDSSE

**Files:**
- Modify: `src/tests/test_dbt_passes_kimball_migration.py`

- [ ] **Step 1:** Read the existing test structure.

```bash
grep -n "def test_\|stg_idsse\|int_unified_passes" src/tests/test_dbt_passes_kimball_migration.py | head -30
```

- [ ] **Step 2:** Add a new test function asserting that `int_unified_passes` IDSSE branch produces non-NULL `is_progressive` post-migration. Mirror the file's existing test patterns (likely a parser-level introspection of the compiled SQL).

```python
def test_idsse_is_progressive_no_longer_literal_false():
    """PR 6: stg_idsse__passes.is_progressive derived via ball-frame join.

    Pre-PR-6, the staging model emitted `false as is_progressive`.
    Post-PR-6, the SELECT projects a CASE expression involving
    distance_to_goal on derived end coords.
    """
    compiled_sql = _read_compiled_sql("stg_idsse__passes")
    # The literal 'false   as is_progressive' pattern is gone:
    assert "false                                                   as is_progressive" not in compiled_sql, (
        "stg_idsse__passes.is_progressive still emits literal false; ball-frame join missing"
    )
    # The distance_to_goal-based CASE is present:
    assert "_end_x_normalized is null" in compiled_sql, "is_progressive NULL guard missing"
    assert "distance_to_goal" in compiled_sql.lower() or "sqrt(power" in compiled_sql.lower(), (
        "distance_to_goal expansion missing — verify macro expanded correctly"
    )
```

(Adapt `_read_compiled_sql` helper to match the file's existing utility — likely already imported.)

---

## Phase 6 — Documentation drift fix

### Task 6.1: Update `docs/huggingface/model-cards/defcon.md`

**Files:**
- Modify: `docs/huggingface/model-cards/defcon.md`

- [ ] **Step 1:** Identify the section describing output marts.

```bash
grep -n "fct_defensive_values\|fct_defcon\|output\|writes to" docs/huggingface/model-cards/defcon.md | head -20
```

- [ ] **Step 2:** Add a one-line note about the new key columns in the output-marts description. Example insertion (adapt to the file's existing wording):

```markdown
**PR 6 (2026-04-26):** Output marts (`fct_defensive_values`, `fct_defcon_actions`, `fct_defcon_pressure`) now carry Kimball-conformed FKs (`match_key`, `team_key`, `player_key`, `action_player_key` where applicable) per ADR-011. Legacy native columns (`match_id`, `team_id`, `player_id`) coexist during the 2026-07-22 dual-column window.
```

---

## Phase 7 — Taipy consumer dual-read

### Task 7.1: Add dual-read to `queries/defensive.py`

**Files:**
- Modify: `hf_taipy_app/src/queries/defensive.py` (per Phase 0 Task 0.3 verification)

- [ ] **Step 1:** Locate the queries that select from the migrated marts.

```bash
grep -n "fct_defensive_values\|fct_defcon" hf_taipy_app/src/queries/defensive.py
```

- [ ] **Step 2:** For each query, add an optional `match_key: int | None = None` parameter that, when provided, filters on `match_key` instead of `match_id`. The implementation pattern follows PR 5b's `fetch_player_embedding_vector` from `queries/players.py`:

```python
def fetch_defcon_summary(
    competition_id: int,
    team_id: int | None = None,
    match_id: int | None = None,
    match_key: int | None = None,  # PR 6 forward-compat (preferred when provided)
) -> pd.DataFrame:
    """..."""
    if match_key is not None:
        # Preferred: dual-read on match_key (Kimball-conformed)
        sql = f"... WHERE match_key = %s ..."  # noqa: S608
        params = (match_key,)
    elif match_id is not None:
        # Legacy fallback during dual-column window
        sql = f"... WHERE match_id = %s ..."  # noqa: S608
        params = (match_id,)
    else:
        ...
```

- [ ] **Step 3:** Compile-check.

```bash
uv run python -m py_compile hf_taipy_app/src/queries/defensive.py
```

### Task 7.2: Add dual-read to `queries/goalkeepers.py` + helper in `state/goalkeeper.py`

**Files:**
- Modify: `hf_taipy_app/src/queries/goalkeepers.py`
- Modify: `hf_taipy_app/src/state/goalkeeper.py`

- [ ] **Step 1:** In `queries/goalkeepers.py`, add `match_key: int | None = None` parameters to functions querying `fct_goalkeeper_stats` and `fct_gk_actions_detail`. Same pattern as Task 7.1.

- [ ] **Step 2:** In `state/goalkeeper.py`, add a `resolve_gk_match_keys()` helper similar to `resolve_player_identity` from PR 5b:

```python
def resolve_gk_match_key(state: Any, match_label: str) -> int | None:
    """Resolve a GK match label to its match_key (PR 6 forward-compat).

    Returns None when no resolution is found (legacy fallback path applies).
    Cached on first call per page; cleared on competition/team change.
    """
    # Implementation mirrors state/shared.py::resolve_player_identity.
    ...
```

- [ ] **Step 3:** Wire the helper into the existing GK page state callbacks where match selection happens.

- [ ] **Step 4:** Compile-check.

```bash
uv run python -m py_compile hf_taipy_app/src/queries/goalkeepers.py hf_taipy_app/src/state/goalkeeper.py
```

### Task 7.3: Annotation-only updates to `state/pitch_control.py` + `queries/tracking.py`

**Files:**
- Modify: `hf_taipy_app/src/state/pitch_control.py`
- Modify: `hf_taipy_app/src/queries/tracking.py`

- [ ] **Step 1:** Add a leading docstring note acknowledging the staging-side promotion (no behaviour change).

In each file, add at module top-level:

```python
# PR 6 (ADR-011): stg_pitch_control__values now carries match_key + data_source.
# This module computes pitch-control on-demand from raw tracking frames
# (does not consume the staging mart) — annotation only.
```

---

## Phase 8 — Pre-push gates

### Task 8.1: Run ruff lint + format check

- [ ] **Step 1:** Lint.

```bash
uv run ruff check src/ scripts/ dbt_project/
```

Expected: `All checks passed!`

- [ ] **Step 2:** Format check.

```bash
uv run ruff format --check src/ scripts/
```

Expected: clean (no diffs).

If failures: fix and re-run.

### Task 8.2: Run pyright

- [ ] **Step 1:** Type check.

```bash
uv run pyright src/
```

Expected: 0 errors, 0 warnings (basic mode).

If failures: fix and re-run.

### Task 8.3: Run dbt parse + compile

- [ ] **Step 1:** Full dbt parse.

```bash
DATABRICKS_HOST=x DATABRICKS_HTTP_PATH=x DATABRICKS_TOKEN=x \
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 0 errors.

- [ ] **Step 2:** Compile the touched models to confirm SQL macros expand cleanly.

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt compile \
    --select stg_idsse__passes stg_pitch_control__values fct_defensive_values fct_defcon_actions fct_defcon_pressure fct_goalkeeper_stats fct_gk_actions_detail \
    --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 7 models compiled successfully.

### Task 8.4: Run pytest (excluding live tests)

- [ ] **Step 1:** Run full unit test suite.

```bash
uv run pytest src/tests/ -v \
    --ignore=src/tests/test_marts_kimball_contracts.py \
    --ignore=src/tests/test_idsse_is_progressive_coverage.py \
    --ignore=src/tests/test_marts_live_schema.py \
    --ignore=src/tests/test_bronze_live_schema.py \
    --ignore=src/tests/test_staging_coverage.py
```

Expected: all tests pass.

(Live tests skipped without DATABRICKS_* env or run on dev_gold post-merge.)

- [ ] **Step 2:** Run `test_pitch_control_bronze_coverage.py` (parser-level, no live env needed).

```bash
uv run pytest src/tests/test_pitch_control_bronze_coverage.py -v
```

Expected: PASS.

- [ ] **Step 3:** Run `test_dbt_passes_kimball_migration.py` (parser-level).

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py -v
```

Expected: all tests including the new `test_idsse_is_progressive_no_longer_literal_false` PASS.

---

## Phase 9 — Commit + PR (USER-APPROVAL GATES)

### Task 9.1: Pause for user approval to commit

- [ ] **Step 1:** Show the diff summary to the user.

```bash
git status --short
git diff --stat
```

- [ ] **Step 2:** WAIT for explicit user approval before proceeding to Step 3.

### Task 9.2: Single commit

- [ ] **Step 1:** Stage all changes.

```bash
git add -A
```

- [ ] **Step 2:** Commit with the canonical PR-6 message.

```bash
git commit -m "$(cat <<'EOF'
feat(kimball-pr6): defensive + GK + pitch-control mart migration + IDSSE is_progressive

ADR-011 staged Kimball migration, PR 6 of 8.

- 5 marts gain match_key/team_key/player_key Kimball FKs:
  fct_defensive_values, fct_defcon_actions, fct_defcon_pressure,
  fct_goalkeeper_stats, fct_gk_actions_detail.
- fct_goalkeeper_stats: data_source promoted to permanent column
  (closes latent multi-provider correctness gap).
- fct_defcon_actions: action_player_key added alongside defender player_key.
- IDSSE fct_passes.is_progressive populated via end_frame x tracking.ball
  lookup + standard cross-provider distance_to_goal rule.
- stg_pitch_control__values promoted to first-class: data_source + match_key
  + bronze/staging coverage tests + dual-defense schema test.
- HF dataset card pitch-control-tracking.md gets 2026-07-22 dual-column
  window stanza. defcon model card edited for new key columns.
- test_marts_player_key_contracts.py renamed and parameterized as
  test_marts_kimball_contracts.py covering 11 (mart, key) pairs.
- New test_idsse_is_progressive_coverage.py + test_pitch_control_bronze_coverage.py.
- Surrogate hashes on fct_defensive_values/pressure/goalkeeper now include
  data_source — existing rows get new IDs on first --full-refresh rebuild.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3:** Verify the commit.

```bash
git log -1 --stat
```

Expected: clean commit with all PR-6 files.

### Task 9.3: Pause for user approval to push

- [ ] **Step 1:** WAIT for explicit user approval before pushing.

### Task 9.4: Push

- [ ] **Step 1:** Push the branch.

```bash
git push -u origin kimball-pr6-defensive-gk-pitch-control
```

### Task 9.5: Pause for user approval to create PR

- [ ] **Step 1:** WAIT for explicit user approval before creating PR.

### Task 9.6: `gh pr create`

- [ ] **Step 1:** Create PR.

```bash
gh pr create --title "feat(kimball-pr6): defensive + GK + pitch-control + IDSSE is_progressive" --body "$(cat <<'EOF'
## Summary

ADR-011 staged Kimball migration, PR 6 of 8.

- 5 marts gain `match_key` / `team_key` / `player_key` Kimball FKs (defensive_values, defcon_actions, defcon_pressure, goalkeeper_stats, gk_actions_detail).
- `fct_goalkeeper_stats.data_source` promoted to permanent column (latent multi-provider correctness fix).
- IDSSE `fct_passes.is_progressive` populated via end_frame × tracking.ball lookup + cross-provider rule.
- `stg_pitch_control__values` promoted to first-class with `data_source` + `match_key` + dual-defense.

## Spec

`docs/superpowers/specs/2026-04-26-kimball-pr6-design.md` (LOCAL-ONLY).

## Test plan

- [ ] CI green: validate / semgrep / lint-and-test / live-build
- [ ] Post-merge: dbt full-refresh on 5 marts + stg_pitch_control
- [ ] Post-merge: synced-table refresh + grants
- [ ] Post-merge: `test_marts_kimball_contracts.py` 21 cases ≥ thresholds
- [ ] Post-merge: `test_idsse_is_progressive_coverage.py` ≥ implementation-measured threshold
- [ ] Post-merge: HF dataset publish smoke test (no JOIN regression)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2:** Capture the PR URL.

### Task 9.7: Wait for CI + triage

- [ ] **Step 1:** Wait for CI completion (validate / semgrep / lint-and-test / live-build).

```bash
gh pr checks --watch
```

- [ ] **Step 2:** If failures:

For lint/type/unit-test failures: fix on branch, re-push (folds into the same single commit via amend, or as a 2nd commit per `feedback_single_commit_squash` if the user prefers — squash-merge collapses).

For live-CI cascade failures: triage per spec §5 #8 (Path X authority approved). Compile errors → fix; data-test failures on PR-6-untouched columns → warn-suppress with YAML pointer to resolving PR.

### Task 9.8: Pause for user approval to merge

- [ ] **Step 1:** WAIT for explicit user approval before merging.

### Task 9.9: `gh pr merge`

- [ ] **Step 1:** Squash-merge.

```bash
gh pr merge <PR_NUMBER> --squash --delete-branch=false
```

(Branch deletion is gated separately at Phase 12.)

- [ ] **Step 2:** Capture the squash commit hash.

```bash
git fetch origin main
git log -1 origin/main --pretty=format:'%H %s'
```

---

## Phase 10 — Post-merge dev deploy (autonomous)

These steps proceed without per-step user approval per `feedback_only_git_gates_need_approval`.

### Task 10.1: dbt full-refresh on PR-6 marts + stg_pitch_control

- [ ] **Step 1:** Switch to main.

```bash
git checkout main && git pull
```

- [ ] **Step 2:** Run `--full-refresh` on the 5 marts + the staging promotion. Background-execute due to >30s runtime per `feedback_bash_long_running_rule`.

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt run \
    --select fct_defensive_values+ fct_defcon_actions+ fct_defcon_pressure+ fct_goalkeeper_stats+ fct_gk_actions_detail+ stg_pitch_control__values+ \
    --full-refresh \
    --target dev \
    --project-dir dbt_project --profiles-dir dbt_project \
    > /tmp/pr6_dbt_run.log 2>&1
```

Run with `run_in_background: true`. Poll `/tmp/pr6_dbt_run.log` every 30s.

Expected: WARN=0, ERROR=0.

- [ ] **Step 3:** Run dbt test on the same selection.

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt test \
    --select fct_defensive_values+ fct_defcon_actions+ fct_defcon_pressure+ fct_goalkeeper_stats+ fct_gk_actions_detail+ stg_pitch_control__values+ \
    --target dev \
    --project-dir dbt_project --profiles-dir dbt_project \
    > /tmp/pr6_dbt_test.log 2>&1
```

Expected: WARN may be non-zero (relationships severity:warn during dual-column window); ERROR must be 0.

### Task 10.2: Refresh synced tables (parallel-poll via PR #204)

- [ ] **Step 1:** Refresh the 5 PR-6 synced tables.

```bash
uv run python scripts/refresh_synced_tables.py \
    --tables fct_defensive_values_synced fct_defcon_actions_synced fct_defcon_pressure_synced fct_goalkeeper_stats_synced fct_gk_actions_detail_synced \
    --wait \
    > /tmp/pr6_synced_refresh.log 2>&1
```

Run with `run_in_background: true`. Expected: all 5 transition to `ONLINE_NO_PENDING_UPDATE`.

### Task 10.3: maintain_synced_tables (grants + indexes)

- [ ] **Step 1:** Run grants + indexes (skip refresh — already done in 10.2).

```bash
uv run python scripts/maintain_synced_tables.py --skip-refresh \
    > /tmp/pr6_maintain.log 2>&1
```

Expected: Steps 0.5 (grants) + 2 (create_indexes) + 3 (verify_indexes) all green.

### Task 10.4: Live invariant tests

- [ ] **Step 1:** Run the Kimball contracts test.

```bash
uv run --with databricks-sql-connector pytest src/tests/test_marts_kimball_contracts.py -v
```

Expected: 21/21 PASS at the committed thresholds.

If any (mart, key) is below threshold: investigate. If a 360-synthetic-defender-style legitimate floor, lower the threshold and document in the test file.

- [ ] **Step 2:** Run the IDSSE is_progressive test.

```bash
uv run --with databricks-sql-connector pytest src/tests/test_idsse_is_progressive_coverage.py -v
```

Expected: PASS at the committed threshold (≥0.95 default; tighten post-measurement to actual).

- [ ] **Step 3:** Run the bronze schema tests.

```bash
uv run --with databricks-sql-connector pytest src/tests/test_bronze_live_schema.py src/tests/test_marts_live_schema.py src/tests/test_staging_coverage.py -v
```

Expected: all PASS.

### Task 10.5: IDSSE `is_progressive` smoke check

- [ ] **Step 1:** Run a Databricks SQL query.

```bash
uv run python -c "
import os
from databricks import sql

with sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
) as conn:
    cur = conn.cursor()
    cur.execute('''
        SELECT data_source,
               count(*) AS total,
               sum(case when is_progressive then 1 else 0 end) AS prog_count,
               sum(case when is_progressive is null then 1 else 0 end) AS null_count
        FROM soccer_analytics.dev_gold.fct_passes
        GROUP BY data_source
        ORDER BY data_source
    ''')
    for row in cur.fetchall():
        print(row)
"
```

Expected: `idsse` row shows non-zero `prog_count` (literal `false` is gone). Other providers' counts match pre-PR-6 distributions.

### Task 10.6: HF dataset publish smoke check

- [ ] **Step 1:** Re-run `notebooks/publish_datasets.py` for the pitch-control-tracking dataset in dev mode (or run a SQL smoke that mimics its query).

```bash
# Smoke version — confirm the JOIN still produces ~38M rows.
uv run python -c "
import os
from databricks import sql

with sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
) as conn:
    cur = conn.cursor()
    cur.execute('''
        SELECT count(*) AS row_count
        FROM soccer_analytics.dev_gold.fct_tracking_frames tf
        INNER JOIN soccer_analytics.dev_silver.stg_pitch_control__values pc
          ON pc.tracking_id = tf.tracking_id
    ''')
    print(cur.fetchone())
"
```

Expected: ~38M rows (within ±5% of the dataset card's documented count). If significantly lower: investigate the JOIN — possibly a dim_matches dropout for SkillCorner.

---

## Phase 11 — Documentation + memory

### Task 11.1: Update `project_kimball_migration_cycle.md`

**Files:**
- Modify: `~/.claude/projects/D--Development-karstenskyt--luxury-lakehouse-d32/memory/project_kimball_migration_cycle.md`

- [ ] **Step 1:** Update the cycle table — PR 6 row goes from "Planned — NEXT" to **MERGED** with the squash commit hash from Task 9.9.

- [ ] **Step 2:** Move "PR 7 — Tracking + formations + pausa + tail facts" up to "Planned — NEXT" status. Add a note: "PR 7 must update `pitch_control_batch.py` to remove the staging-derived data_source/match_key bridge (PR 6 §4.7 dependency)."

- [ ] **Step 3:** Update the description and "originSessionId" fields per the memory format.

### Task 11.2: Write `project_kimball_pr6_shipped.md`

**Files:**
- Create: `~/.claude/projects/D--Development-karstenskyt--luxury-lakehouse-d32/memory/project_kimball_pr6_shipped.md`

- [ ] **Step 1:** Write a memory entry mirroring `project_kimball_pr5b_shipped.md` shape:

```markdown
---
name: Kimball PR 6 defensive + goalkeeper + pitch-control mart migrations — SHIPPED + DEPLOYED
description: PR #<NUM> squash-merged 2026-04-XX (`<HASH>` on main); 5 marts + IDSSE is_progressive + pitch-control promotion + HF card update + 2 new test files. Live invariants 21/21 PASS on dev_gold. Don't re-run — see DON'T list.
type: project
originSessionId: kimball-pr6-ship-2026-04-XX
---
**State as of 2026-04-XX:** PR 6 shipped, merged to `main`, deployed to dev_gold. Live invariant tests 21/21 PASS asserting `match_key` + `team_key` + `player_key` (+ `action_player_key`) ≥ committed thresholds on the five PR-6 marts. IDSSE `is_progressive` populated at <X>% non-NULL. Pitch-control staging promoted to first-class (data_source + match_key + dual-defense + bronze coverage tests).

**Why memory entry exists:** PR 6 was the defensive + GK + IDSSE + pitch-control conversion step. Same-cycle issues / fixes captured. Don't-re-run list documented.

**Final delivered scope:** [...]

**Key numbers:** [...]

**Don't re-run:** [...]

**Open follow-ups:** [...]
```

Fill in actuals from Task 9.9 (commit hash), Task 10.4 (test counts), Task 10.5 (IDSSE coverage rate).

### Task 11.3: Update ADR-011 staged-rollout table

**Files:**
- Modify: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`

- [ ] **Step 1:** Update the PR 6 row.

```markdown
| PR 6 | Defensive + goalkeeper + pitch control migration | Shipped (2026-04-XX, <HASH>) |
```

- [ ] **Step 2:** This is the only ADR commit in the cycle (per `project_kimball_pr5b_shipped.md` precedent: only ADRs are committed under docs/superpowers/, specs/plans stay local-only). Stage and commit:

```bash
git checkout main
git add docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md
git commit -m "docs(adr-011): mark PR 6 shipped"
```

- [ ] **Step 3:** Pause for user approval to push the ADR commit.

- [ ] **Step 4:** After approval: `git push origin main`.

### Task 11.4: Update MEMORY.md index

**Files:**
- Modify: `~/.claude/projects/D--Development-karstenskyt--luxury-lakehouse-d32/memory/MEMORY.md`

- [ ] **Step 1:** Add a new entry for `project_kimball_pr6_shipped.md` in the index.

- [ ] **Step 2:** Update the `project_kimball_migration_cycle.md` description in the index to reflect "PR 6 shipped; PR 7 next."

---

## Phase 12 — Branch cleanup (USER-APPROVAL GATE)

### Task 12.1: Pause for user approval

- [ ] **Step 1:** WAIT for explicit user approval before deleting the branch.

### Task 12.2: Delete the branch

- [ ] **Step 1:** Local branch deletion.

```bash
git branch -d kimball-pr6-defensive-gk-pitch-control
```

- [ ] **Step 2:** Remote branch deletion.

```bash
git push origin --delete kimball-pr6-defensive-gk-pitch-control
```

---

## Self-review checklist (run by author before declaring plan ready)

- [ ] Every spec §3.1 in-scope item maps to at least one task. (Confirmed: 5 marts → Phases 3+4; IDSSE → Phase 1; pitch-control promotion → Phase 2; test rename → Phase 5; defcon.md → Phase 6; Taipy → Phase 7; deploy → Phase 10; memory → Phase 11.)
- [ ] No "TBD", "TODO", "fill in details" placeholders. (One exception: Task 0.6's `<SKILLCORNER_PREFIX>` is intentional — gets resolved IN Task 0.6 itself.)
- [ ] Type / column / function names consistent across tasks. (`gk_stat_id` hashes `(player_id, match_id, data_source)` everywhere; `data_source` lowercase string; `match_key` BIGINT.)
- [ ] Code blocks complete — no "see Task N" cross-references that defer code.
- [ ] User-approval gates explicit and limited to git operations (Phase 9 commit/push/PR/merge, Phase 11 ADR commit/push, Phase 12 branch delete).
- [ ] Single commit per branch convention preserved (no per-task commits in Phases 0-8).
- [ ] Long-running commands flagged for `run_in_background: true` (Phase 10 dbt run, refresh_synced_tables).

---

## Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-26-kimball-pr6-defensive-gk-pitch-control.md` (LOCAL-ONLY, untracked per project convention).**

Per the user's standing preferences (`feedback_agent_tool_requires_per_call_approval`), default execution mode is **Inline Execution** via `superpowers:executing-plans` — Write/Edit/Bash directly, no Agent tool dispatch unless explicitly requested.

If user prefers subagent-driven mode, that requires explicit opt-in (the writing-plans skill's default suggestion is overridden by user feedback memory).
