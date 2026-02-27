# Tech Debt & Terraform Security — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resolve all 9 dbt tech debt items and 2 Terraform security items identified in the design document.

**Architecture:** Incremental changes in dependency order. Each task produces a self-contained, testable commit. dbt models are refactored bottom-up (staging → intermediate → marts) so downstream consumers always have a clean upstream.

**Tech Stack:** dbt-core 1.11+ on Databricks, Terraform with databricks/databricks provider >= 1.98.0

---

### Task 1: Extract Hardcoded Thresholds to dbt Vars

**Files:**
- Modify: `dbt_project/dbt_project.yml:45-51`
- Modify: `dbt_project/models/marts/fct_player_embeddings.sql:57-64`
- Modify: `dbt_project/models/intermediate/int_unified_passes.sql:59,102`
- Modify: `dbt_project/models/marts/fct_passes.sql:83-84`
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql:40-41,46-47`
- Modify: `dbt_project/macros/shot_angle.sql:41-42`
- Modify: `dbt_project/models/marts/fct_player_stats.sql:106,111,115,119`

**Step 1: Add new vars to dbt_project.yml**

In `dbt_project/dbt_project.yml`, extend the `vars:` block (after line 51):

```yaml
vars:
  # Standard pitch dimensions used across macros (StatsBomb coordinate system)
  pitch_length: 120
  pitch_width: 80
  # Goal center coordinates (center of the goal line)
  goal_x: 120
  goal_y: 40
  # Domain thresholds (documented in Soccermatics curriculum)
  sprint_speed_threshold: 7.0        # m/s — standard sports science sprint threshold
  progressive_pass_ratio: 0.75       # end distance < 75% of start distance to goal
  pass_direction_threshold: 5        # yards — forward/backward deadband
  frame_duration_seconds: 0.04       # 1/25fps — Metrica tracking frame interval
  goal_width: 8                      # yards — standard goal width (IFAB Laws of the Game)
  minutes_per_match: 90              # standard match length for per-90 normalization
```

**Step 2: Replace hardcoded values in fct_player_embeddings.sql**

Replace line 58:
```sql
        sum(case when speed > {{ var('sprint_speed_threshold') }} then 1 else 0 end)   as sprint_count,
```

Replace lines 62-64 (pitch thirds derived from pitch_length):
```sql
        avg(case when x < {{ var('pitch_length') / 3 }} then 1.0 else 0.0 end)    as pct_defensive_third,
        avg(case when x >= {{ var('pitch_length') / 3 }} and x < {{ 2 * var('pitch_length') / 3 }} then 1.0 else 0.0 end) as pct_middle_third,
        avg(case when x >= {{ 2 * var('pitch_length') / 3 }} then 1.0 else 0.0 end)   as pct_attacking_third,
```

**Step 3: Replace hardcoded values in int_unified_passes.sql**

Replace line 59:
```sql
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('e.location_x', 'e.location_y') }}
```

Replace line 102:
```sql
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x', 'start_y') }}
```

**Step 4: Replace hardcoded values in fct_passes.sql**

Replace lines 83-84:
```sql
            when unified_passes.end_x > unified_passes.start_x + {{ var('pass_direction_threshold') }} then 'forward'
            when unified_passes.end_x < unified_passes.start_x - {{ var('pass_direction_threshold') }} then 'backward'
```

**Step 5: Replace hardcoded values in fct_tracking_frames.sql**

Replace lines 40-41:
```sql
        (x - lag(x) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }} as velocity_x,
        (y - lag(y) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }} as velocity_y,
```

Replace lines 46-51 (the speed calculation):
```sql
        sqrt(
            power(
                (x - lag(x) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }},
                2
            )
            + power(
                (y - lag(y) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }},
                2
            )
        )                                               as speed,
```

**Step 6: Replace hardcoded values in macros/shot_angle.sql**

Replace lines 41-42:
```sql
    {%- set goal_width = var('goal_width') -%}
    {%- set half_goal_width = var('goal_width') / 2 -%}
```

**Step 7: Replace hardcoded values in fct_player_stats.sql**

Replace the four `* 90` occurrences (lines 106, 111, 115, 119):
```sql
            then round((coalesce(s.total_goals, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
```
```sql
            then round((coalesce(s.total_xg, 0) / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
```
```sql
            then round((coalesce(p.total_passes, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
```
```sql
            then round((coalesce(p.progressive_passes, 0) * 1.0 / m.total_minutes_played) * {{ var('minutes_per_match') }}, 2)
```

**Step 8: Commit**

```bash
git add dbt_project/dbt_project.yml dbt_project/models/marts/fct_player_embeddings.sql dbt_project/models/intermediate/int_unified_passes.sql dbt_project/models/marts/fct_passes.sql dbt_project/models/marts/fct_tracking_frames.sql dbt_project/macros/shot_angle.sql dbt_project/models/marts/fct_player_stats.sql
git commit -m "refactor: extract hardcoded domain thresholds to dbt vars"
```

---

### Task 2: Expand stg_statsbomb__events and Refactor Dual-Source Models

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/stg_statsbomb__events.sql`
- Modify: `dbt_project/models/staging/statsbomb/stg_statsbomb__shots.sql`
- Modify: `dbt_project/models/intermediate/int_unified_passes.sql`
- Modify: `dbt_project/models/intermediate/int_minutes_played.sql`
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml`

**Step 1: Add missing columns to stg_statsbomb__events.sql**

Add these columns to the `flattened` CTE (before `from source`), after line 57:

```sql
        -- Shot-specific fields (pass-through for downstream shot/pass models)
        shot_end_location,
        shot_freeze_frame,
        shot_outcome,
        shot_technique,
        shot_body_part,
        shot_type,
        shot_statsbomb_xg,
        shot_first_time,
        shot_one_on_one,

        -- Pass-specific fields (pass-through for downstream pass models)
        pass_end_location,
        pass_type,
        pass_height,
        pass_body_part,
        pass_length,
        pass_angle,
        pass_outcome,
        pass_cross,
        pass_switch,
        pass_through_ball,

        -- Substitution fields
        substitution_replacement_id
```

**Step 2: Refactor stg_statsbomb__shots.sql to use only ref()**

Replace the entire file with:

```sql
-- stg_statsbomb__shots.sql
-- Extract shot-specific attributes from StatsBomb event data.
--
-- All raw columns are now available from stg_statsbomb__events,
-- so this model uses only ref() — no direct source() access needed.

with events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

shots as (

    select
        -- Keys
        event_id,
        match_id,
        team_id,
        team_name,
        player_id,
        player_name,
        period,
        minute,
        second,

        -- Shot location (already parsed in events model)
        location_x,
        location_y,

        -- Shot-specific fields (pass-through from events)
        shot_outcome,
        shot_technique,
        shot_body_part,
        shot_type,
        shot_statsbomb_xg                                  as statsbomb_xg,
        shot_first_time                                    as is_first_time,
        shot_one_on_one                                    as is_one_on_one,

        -- End location (parse JSON string "[x, y, z]" — use get() for safe access)
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 0) as end_location_x,
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 1) as end_location_y,
        get(from_json(shot_end_location, 'ARRAY<DOUBLE>'), 2) as end_location_z,

        -- Computed geometry features
        {{ distance_to_goal('location_x', 'location_y') }} as distance_to_goal,
        {{ shot_angle('location_x', 'location_y') }}       as shot_angle,

        -- Number of defenders/teammates in freeze frame
        coalesce(
            size(filter(
                from_json(shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = false
            )),
            0
        )                                                   as defenders_in_frame,
        coalesce(
            size(filter(
                from_json(shot_freeze_frame, 'ARRAY<STRUCT<teammate:BOOLEAN>>'),
                f -> f.teammate = true
            )),
            0
        )                                                   as teammates_in_frame

    from events
    where event_type = 'Shot'

)

select * from shots
```

**Step 3: Refactor int_unified_passes.sql to use only ref()**

Replace the `statsbomb_raw` CTE and the `statsbomb_passes` CTE with:

```sql
statsbomb_events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

statsbomb_passes as (

    select
        event_id,
        match_id,
        player_id,
        team_id,
        period,
        minute,
        second,
        location_x                                          as start_x,
        location_y                                          as start_y,

        -- Parse pass end location from JSON string (use get() for safe access)
        get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 0) as end_x,
        get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 1) as end_y,

        -- Pass attributes (pass-through from events)
        pass_type,
        pass_height,
        pass_body_part                                      as body_part,
        pass_length,
        pass_angle                                          as pass_angle_radians,
        pass_outcome,
        coalesce(pass_cross, false)                         as is_cross,
        coalesce(pass_switch, false)                        as is_switch,
        coalesce(pass_through_ball, false)                  as is_through_ball,

        -- Progressive pass flag
        {{ distance_to_goal(
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 0)',
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 1)'
        ) }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('location_x', 'location_y') }}
                                                            as is_progressive,

        'statsbomb'                                         as data_source

    from statsbomb_events
    where event_type = 'Pass'

),
```

Remove the `statsbomb_raw` CTE entirely. The `statsbomb_events` CTE now reads from `ref('stg_statsbomb__events')` which has all needed columns.

**Step 4: Refactor int_minutes_played.sql to use only ref()**

Replace the `substitution_on` CTE (lines 65-74):

```sql
substitution_on as (

    select
        match_id,
        cast(substitution_replacement_id as int)            as player_id,
        minute                                              as on_minute
    from {{ ref('stg_statsbomb__events') }}
    where event_type = 'Substitution'
      and substitution_replacement_id is not null

),
```

This replaces `{{ source('statsbomb', 'statsbomb_events') }}` with `{{ ref('stg_statsbomb__events') }}` and `type` with `event_type`.

**Step 5: Commit**

```bash
git add dbt_project/models/staging/statsbomb/stg_statsbomb__events.sql dbt_project/models/staging/statsbomb/stg_statsbomb__shots.sql dbt_project/models/intermediate/int_unified_passes.sql dbt_project/models/intermediate/int_minutes_played.sql
git commit -m "refactor: expand stg_statsbomb__events, eliminate dual source/ref pattern"
```

---

### Task 3: DRY from_json() Calls

**Files:**
- Modify: `dbt_project/models/staging/wyscout/stg_wyscout__events.sql`
- Modify: `dbt_project/models/staging/statsbomb/stg_statsbomb__lineups.sql`

**Step 1: Refactor stg_wyscout__events.sql**

Replace the `cleaned` CTE with a two-stage CTE that parses JSON once:

```sql
with source as (

    select * from {{ source('wyscout', 'wyscout_events') }}

),

-- Parse JSON columns once for reuse
parsed as (

    select
        *,
        from_json(positions, 'ARRAY<STRUCT<x:DOUBLE, y:DOUBLE>>') as parsed_positions,
        from_json(tags, 'ARRAY<STRUCT<id:INT>>')                  as parsed_tags
    from source

),

cleaned as (

    select
        -- Primary key
        cast(id as string)                                  as event_sk,
        eventId                                             as event_id,
        matchId                                             as match_id,

        -- Event classification
        eventName                                           as event_type,
        subEventName                                        as sub_event_type,

        -- Team and player
        playerId                                            as player_id,
        teamId                                              as team_id,

        -- Temporal fields
        case matchPeriod
            when '1H' then 1
            when '2H' then 2
            when 'E1' then 3
            when 'E2' then 4
            when 'P'  then 5
        end                                                 as period,
        eventSec                                            as event_sec,

        -- Start location (scaled to 120x80, use get() for safe access)
        get(parsed_positions, 0).x / 100.0 * {{ var('pitch_length') }}.0 as start_x,
        get(parsed_positions, 0).y / 100.0 * {{ var('pitch_width') }}.0  as start_y,

        -- End location (scaled to 120x80, may be NULL if positions has only 1 element)
        get(parsed_positions, 1).x / 100.0 * {{ var('pitch_length') }}.0 as end_x,
        get(parsed_positions, 1).y / 100.0 * {{ var('pitch_width') }}.0  as end_y,

        -- Tag-derived boolean flags
        exists(parsed_tags, t -> t.id = 101)                as is_goal,
        exists(parsed_tags, t -> t.id = 102)                as is_own_goal,
        exists(parsed_tags, t -> t.id = 301)                as is_assist,
        exists(parsed_tags, t -> t.id = 401)                as is_key_pass,
        exists(parsed_tags, t -> t.id = 1801)               as is_accurate,

        -- Data provenance
        'wyscout'                                           as data_source

    from parsed

)

select * from cleaned
```

**Step 2: Refactor stg_statsbomb__lineups.sql**

Replace the `flattened` CTE with a two-stage CTE:

```sql
with source as (

    select * from {{ source('statsbomb', 'statsbomb_lineups') }}

),

-- Parse JSON columns once for reuse
parsed as (

    select
        *,
        from_json(
            positions,
            'ARRAY<STRUCT<position:STRING, position_id:INT, `from`:STRING, `to`:STRING>>'
        )                                                   as parsed_positions,
        from_json(cards, 'ARRAY<STRUCT<card_type:STRING>>')  as parsed_cards
    from source

),

flattened as (

    select
        -- Surrogate key (no team_id column; use team_name)
        {{ dbt_utils.generate_surrogate_key(['match_id', 'team_name', 'player_id']) }} as lineup_id,

        -- Match and team context
        match_id,
        competition_id,
        season_id,
        team_name,

        -- Player info (already flat columns)
        cast(player_id as int)                              as player_id,
        player_name,
        player_nickname,
        cast(jersey_number as int)                          as jersey_number,

        -- Starting position (first element of parsed positions array)
        get(parsed_positions, 0).position_id                as position_id,
        get(parsed_positions, 0).position                   as position_name,

        -- Cards summary
        coalesce(
            size(filter(parsed_cards, c -> c.card_type = 'Yellow Card')),
            0
        )                                                   as yellow_cards,
        coalesce(
            size(filter(parsed_cards, c -> c.card_type IN ('Red Card', 'Second Yellow'))),
            0
        )                                                   as red_cards

    from parsed

)

select * from flattened
```

**Step 3: Commit**

```bash
git add dbt_project/models/staging/wyscout/stg_wyscout__events.sql dbt_project/models/staging/statsbomb/stg_statsbomb__lineups.sql
git commit -m "refactor: DRY from_json() calls — parse JSON once in CTE"
```

---

### Task 4: Enable use_materialization_v2

**Files:**
- Modify: `dbt_project/dbt_project.yml`

**Step 1: Add the flag**

Add after `config-version: 2` (line 3):

```yaml
flags:
  use_materialization_v2: true
```

**Step 2: Commit**

```bash
git add dbt_project/dbt_project.yml
git commit -m "chore: enable dbt use_materialization_v2 flag"
```

---

### Task 5: Nest Test Arguments Under arguments Property

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml`
- Modify: `dbt_project/models/staging/metrica/_metrica__models.yml`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Modify: `dbt_project/models/marts/_marts__models.yml`

The dbt 1.11+ deprecation requires custom test arguments to be nested under `arguments:`. The `config:` key stays at the top level.

**Migration pattern:**

Before (deprecated):
```yaml
data_tests:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 0
      max_value: 120
      row_condition: "location_x is not null"
      config:
        severity: warn
```

After (1.11+ compliant):
```yaml
data_tests:
  - dbt_expectations.expect_column_values_to_be_between:
      arguments:
        min_value: 0
        max_value: 120
        row_condition: "location_x is not null"
      config:
        severity: warn
```

**The same applies to `accepted_values`:**

Before:
```yaml
  - accepted_values:
      values: ['Goal', 'Saved']
```

After:
```yaml
  - accepted_values:
      arguments:
        values: ['Goal', 'Saved']
```

**Step 1: Migrate _statsbomb__models.yml**

Every `dbt_expectations.expect_column_values_to_be_between` test and every `accepted_values` test needs `arguments:` nesting. The built-in `unique`, `not_null` tests have no arguments and are unchanged.

Apply this pattern to all tests in the file. Specifically:
- `stg_statsbomb__events`: 3 range tests (location_x, location_y, period)
- `stg_statsbomb__shots`: 1 accepted_values (shot_outcome), 3 range tests (statsbomb_xg, location_x, location_y)
- `stg_statsbomb__matches`: no custom tests to migrate
- `stg_statsbomb__lineups`: no custom tests to migrate

**Step 2: Migrate _metrica__models.yml**

- `stg_metrica__tracking`: 2 accepted_values (period, team), 4 range tests (x, y, ball_x, ball_y)
- `stg_metrica__events`: 4 range tests (start_x, start_y, end_x, end_y)

**Step 3: Migrate _wyscout__models.yml**

- `stg_wyscout__events`: 1 accepted_values (data_source), 5 range tests (period, start_x, start_y, end_x, end_y)

**Step 4: Migrate _intermediate__models.yml**

- `int_unified_passes`: 1 accepted_values (data_source)
- `int_unified_shots`: 1 accepted_values (data_source)
- `int_minutes_played`: 1 range test (total_minutes_played)

**Step 5: Migrate _marts__models.yml**

- `fct_shots`: 1 accepted_values (is_goal), 1 accepted_values (data_source), 3 range tests (location_x, location_y, statsbomb_xg), 1 range test (period)
- `fct_passes`: 1 accepted_values (pass_direction), 2 range tests (start_x, start_y), 1 range test (period)
- `fct_player_stats`: 1 range test (pass_completion_pct)
- `fct_match_summary`: 1 accepted_values (match_result), 3 range tests (home_possession_pct, home_pass_completion_pct, away_pass_completion_pct)
- `fct_player_embeddings`: 1 range test (total_frames)
- `dim_teams`: 1 accepted_values (data_source)
- `dim_competitions`: 1 accepted_values (gender)

**Step 6: Commit**

```bash
git add dbt_project/models/staging/statsbomb/_statsbomb__models.yml dbt_project/models/staging/metrica/_metrica__models.yml dbt_project/models/staging/wyscout/_wyscout__models.yml dbt_project/models/intermediate/_intermediate__models.yml dbt_project/models/marts/_marts__models.yml
git commit -m "chore: nest test arguments under arguments property (dbt 1.11+ deprecation)"
```

---

### Task 6: Add Missing accepted_values Tests

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml`
- Modify: `dbt_project/models/staging/metrica/_metrica__models.yml`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Modify: `dbt_project/models/marts/_marts__models.yml`

**Step 1: Add to stg_statsbomb__shots**

Add `accepted_values` tests (using `arguments:` nesting from Task 5):

```yaml
      - name: shot_body_part
        description: Body part used for the shot
        data_tests:
          - accepted_values:
              arguments:
                values: ['Right Foot', 'Left Foot', 'Head', 'No Touch']
              config:
                severity: warn
      - name: shot_technique
        description: Shot technique used
        data_tests:
          - accepted_values:
              arguments:
                values: ['Normal', 'Volley', 'Half Volley', 'Lob', 'Overhead Kick', 'Diving Header', 'Backheel']
              config:
                severity: warn
      - name: shot_type
        description: Shot type classification
        data_tests:
          - accepted_values:
              arguments:
                values: ['Open Play', 'Free Kick', 'Penalty', 'Corner', 'Kick Off']
              config:
                severity: warn
```

**Step 2: Add to stg_statsbomb__events**

```yaml
      - name: play_pattern
        description: Play pattern name (Regular Play, From Corner, etc.)
        data_tests:
          - accepted_values:
              arguments:
                values: ['Regular Play', 'From Corner', 'From Free Kick', 'From Goal Kick', 'From Keeper', 'From Kick Off', 'From Throw In', 'Other']
              config:
                severity: warn
```

**Step 3: Add to stg_wyscout__events**

```yaml
      - name: event_type
        description: Event type name (Pass, Shot, Duel, Free Kick, etc.)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Duel', 'Foul', 'Free Kick', 'Goalkeeper leaving line', 'Interruption', 'Offside', 'Others on the ball', 'Pass', 'Shot', 'Save attempt']
              config:
                severity: warn
```

**Step 4: Add to stg_metrica__events**

```yaml
      - name: period
        description: Match half
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 2]
```

**Step 5: Add to fct_passes**

```yaml
      - name: pass_outcome
        description: Pass result (Complete, Incomplete, Out, etc.)
        data_tests:
          - accepted_values:
              arguments:
                values: ['Complete', 'Incomplete', 'Out', 'Pass Offside', 'Injury Clearance', 'Unknown']
              config:
                severity: warn
```

**Step 6: Add to dim_players**

```yaml
      - name: data_source
        description: Data provider
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb']
```

**Step 7: Commit**

```bash
git add dbt_project/models/staging/statsbomb/_statsbomb__models.yml dbt_project/models/staging/wyscout/_wyscout__models.yml dbt_project/models/staging/metrica/_metrica__models.yml dbt_project/models/intermediate/_intermediate__models.yml dbt_project/models/marts/_marts__models.yml
git commit -m "test: add missing accepted_values tests on categorical columns"
```

---

### Task 7: Add Missing Range Tests

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml`
- Modify: `dbt_project/models/marts/_marts__models.yml`

**Step 1: Add range tests to stg_statsbomb__shots**

```yaml
      - name: distance_to_goal
        description: Euclidean distance from shot to goal center (yards)
        data_tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 170
                row_condition: "distance_to_goal is not null"
              config:
                severity: warn
      - name: shot_angle
        description: Angle subtended at the goal posts (radians)
        data_tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 3.15
                row_condition: "shot_angle is not null"
              config:
                severity: warn
```

**Step 2: Add range tests to fct_shots**

```yaml
      - name: distance_to_goal
        description: Euclidean distance from shot location to goal center (yards)
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 170
                row_condition: "distance_to_goal is not null"
              config:
                severity: warn
      - name: shot_angle
        description: Angle subtended at the goal posts (radians)
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 3.15
                row_condition: "shot_angle is not null"
              config:
                severity: warn
```

**Step 3: Add range tests to fct_player_stats**

```yaml
      - name: goals_per_90
        description: Goals scored per 90 minutes
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 5
                row_condition: "goals_per_90 is not null"
              config:
                severity: warn
      - name: xg_per_90
        description: Expected goals per 90 minutes
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 5
                row_condition: "xg_per_90 is not null"
              config:
                severity: warn
```

**Step 4: Add range tests to fct_match_summary**

```yaml
      - name: home_xg
        description: Total home team expected goals
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 20
                row_condition: "home_xg is not null"
              config:
                severity: warn
      - name: away_xg
        description: Total away team expected goals
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 20
                row_condition: "away_xg is not null"
              config:
                severity: warn
      - name: home_score
        description: Final home team score
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 30
                row_condition: "home_score is not null"
      - name: away_score
        description: Final away team score
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 30
                row_condition: "away_score is not null"
```

**Step 5: Add range tests to fct_player_embeddings**

```yaml
      - name: avg_speed
        description: Average player speed during the match (yards/second)
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 15
                row_condition: "avg_speed is not null"
              config:
                severity: warn
      - name: max_speed
        description: Maximum player speed during the match (yards/second)
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 15
                row_condition: "max_speed is not null"
              config:
                severity: warn
      - name: sprint_count
        description: Number of frames exceeding sprint speed threshold
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 5000
                row_condition: "sprint_count is not null"
```

**Step 6: Commit**

```bash
git add dbt_project/models/staging/statsbomb/_statsbomb__models.yml dbt_project/models/marts/_marts__models.yml
git commit -m "test: add missing range tests on numeric columns"
```

---

### Task 8: Document Undocumented YAML Columns

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Modify: `dbt_project/models/marts/_marts__models.yml`

**Step 1: Add missing columns to stg_statsbomb__events**

Add after existing columns:
```yaml
      - name: duration
        description: Duration in seconds that the event lasted
      - name: index
        description: Ordering index within a possession sequence
      - name: team_name
        description: Name of the team performing the event
      - name: player_name
        description: Name of the player performing the event
```

Note: The pass-through columns added in Task 2 (`shot_end_location`, `pass_end_location`, etc.) should also be documented. Add:
```yaml
      - name: shot_end_location
        description: Raw JSON array of shot end location coordinates (pass-through for stg_statsbomb__shots)
      - name: shot_freeze_frame
        description: Raw JSON array of freeze frame data (pass-through for stg_statsbomb__shots)
      - name: shot_outcome
        description: Shot outcome string (pass-through for stg_statsbomb__shots)
      - name: shot_technique
        description: Shot technique string (pass-through for stg_statsbomb__shots)
      - name: shot_body_part
        description: Shot body part string (pass-through for stg_statsbomb__shots)
      - name: shot_type
        description: Shot type string (pass-through for stg_statsbomb__shots)
      - name: shot_statsbomb_xg
        description: StatsBomb xG value (pass-through for stg_statsbomb__shots)
      - name: shot_first_time
        description: Whether shot was taken first-time (pass-through for stg_statsbomb__shots)
      - name: shot_one_on_one
        description: Whether shot was a one-on-one (pass-through for stg_statsbomb__shots)
      - name: pass_end_location
        description: Raw JSON array of pass end location coordinates (pass-through for int_unified_passes)
      - name: pass_type
        description: Pass type classification (pass-through for int_unified_passes)
      - name: pass_height
        description: Pass height classification (pass-through for int_unified_passes)
      - name: pass_body_part
        description: Body part used for the pass (pass-through for int_unified_passes)
      - name: pass_length
        description: Pass length in yards (pass-through for int_unified_passes)
      - name: pass_angle
        description: Pass angle in radians (pass-through for int_unified_passes)
      - name: pass_outcome
        description: Pass outcome classification (pass-through for int_unified_passes)
      - name: pass_cross
        description: Whether the pass was a cross (pass-through for int_unified_passes)
      - name: pass_switch
        description: Whether the pass was a switch of play (pass-through for int_unified_passes)
      - name: pass_through_ball
        description: Whether the pass was a through ball (pass-through for int_unified_passes)
      - name: substitution_replacement_id
        description: Player ID of the substitution replacement (pass-through for int_minutes_played)
```

**Step 2: Add missing columns to stg_statsbomb__shots**

```yaml
      - name: team_name
        description: Name of the team taking the shot
      - name: player_name
        description: Name of the player taking the shot
      - name: period
        description: Match period (1=first half, 2=second half, etc.)
      - name: minute
        description: Match minute of the shot
      - name: second
        description: Match second of the shot
      - name: end_location_z
        description: Z coordinate of shot end location (height in yards)
      - name: shot_type
        description: Shot type classification (Open Play, Free Kick, Penalty, etc.)
      - name: is_first_time
        description: Whether the shot was taken first-time without controlling the ball
      - name: is_one_on_one
        description: Whether the shot was a one-on-one with the goalkeeper
      - name: defenders_in_frame
        description: Number of defenders visible in the freeze frame
      - name: teammates_in_frame
        description: Number of teammates visible in the freeze frame
```

**Step 3: Add missing columns to stg_statsbomb__matches**

```yaml
      - name: home_manager
        description: Home team manager name
      - name: away_manager
        description: Away team manager name
      - name: referee_name
        description: Match referee name
      - name: stadium_name
        description: Stadium where the match was played
      - name: match_status
        description: Match status (available, collecting, etc.)
      - name: match_week
        description: Match week number within the competition
      - name: competition_stage
        description: Competition stage (Group Stage, Round of 16, Final, etc.)
      - name: data_version
        description: StatsBomb data version identifier
```

**Step 4: Add missing columns to stg_statsbomb__lineups**

```yaml
      - name: competition_id
        description: Competition identifier (from match metadata)
      - name: season_id
        description: Season identifier (from match metadata)
      - name: player_nickname
        description: Player nickname or short name
      - name: yellow_cards
        description: Number of yellow cards received in the match
      - name: red_cards
        description: Number of red cards received in the match (including second yellow)
```

**Step 5: Add missing columns to stg_wyscout__events**

```yaml
      - name: is_own_goal
        description: Whether the event resulted in an own goal (from tag 102)
      - name: is_assist
        description: Whether the event was an assist (from tag 301)
      - name: is_key_pass
        description: Whether the event was a key pass (from tag 401)
```

**Step 6: Add missing columns to int_unified_passes**

```yaml
      - name: period
        description: Match period (1=first half, 2=second half, etc.)
      - name: minute
        description: Match minute of the pass
      - name: second
        description: Match second of the pass
      - name: pass_type
        description: Pass type classification
      - name: pass_height
        description: Pass height classification (Ground Pass, Low Pass, High Pass)
      - name: body_part
        description: Body part used for the pass
      - name: pass_length
        description: Pass length in yards (Euclidean distance)
      - name: pass_angle_radians
        description: Pass angle in radians
      - name: pass_outcome
        description: Pass outcome (Complete, Incomplete, Out, etc.)
      - name: is_cross
        description: Whether the pass was a cross
      - name: is_switch
        description: Whether the pass was a switch of play
      - name: is_through_ball
        description: Whether the pass was a through ball
```

**Step 7: Add missing columns to int_unified_shots**

```yaml
      - name: player_id
        description: Player performing the shot
      - name: team_id
        description: Team performing the shot
      - name: period
        description: Match period
      - name: minute
        description: Match minute of the shot
      - name: second
        description: Match second of the shot
      - name: end_location_x
        description: X coordinate where the shot ended
      - name: end_location_y
        description: Y coordinate where the shot ended
      - name: shot_body_part
        description: Body part used for the shot
      - name: shot_technique
        description: Shot technique classification
      - name: shot_type
        description: Shot type classification
      - name: statsbomb_xg
        description: StatsBomb xG value (NULL for Wyscout)
      - name: is_first_time
        description: Whether the shot was first-time (NULL for Wyscout)
```

**Step 8: Add missing columns to fct_shots**

```yaml
      - name: competition_id
        description: Foreign key to dim_competitions
      - name: season_id
        description: Season identifier
      - name: minute
        description: Match minute of the shot
      - name: second
        description: Match second of the shot
      - name: end_location_x
        description: X coordinate where the shot ended
      - name: end_location_y
        description: Y coordinate where the shot ended
      - name: shot_body_part
        description: Body part used for the shot
      - name: shot_technique
        description: Shot technique classification
      - name: shot_type
        description: Shot type classification (Open Play, Free Kick, Penalty, etc.)
      - name: is_first_time
        description: Whether the shot was taken first-time
```

**Step 9: Add missing columns to fct_passes**

```yaml
      - name: competition_id
        description: Foreign key to dim_competitions
      - name: season_id
        description: Season identifier
      - name: minute
        description: Match minute of the pass
      - name: second
        description: Match second of the pass
      - name: pass_type
        description: Pass type classification
      - name: pass_height
        description: Pass height (Ground Pass, Low Pass, High Pass)
      - name: body_part
        description: Body part used for the pass
      - name: pass_length
        description: Pass length in yards
      - name: pass_angle_radians
        description: Pass angle in radians
      - name: is_cross
        description: Whether the pass was a cross
      - name: is_switch
        description: Whether the pass was a switch of play
      - name: is_through_ball
        description: Whether the pass was a through ball
```

**Step 10: Add missing columns to fct_player_stats**

```yaml
      - name: shots_on_target
        description: Total shots on target
      - name: total_passes
        description: Total passes attempted
      - name: completed_passes
        description: Total passes completed
      - name: progressive_passes
        description: Total progressive passes
      - name: assists_per_90
        description: Assists per 90 minutes (NULL until assist data available)
      - name: xg_overperformance
        description: Goals minus xG (positive = clinical finisher)
```

**Step 11: Add missing columns to fct_match_summary**

```yaml
      - name: home_team_name
        description: Home team display name
      - name: away_team_name
        description: Away team display name
      - name: home_shots
        description: Total home team shots
      - name: home_goals
        description: Total home team goals (from shot events)
      - name: home_shots_on_target
        description: Home team shots on target
      - name: away_shots
        description: Total away team shots
      - name: away_goals
        description: Total away team goals (from shot events)
      - name: away_shots_on_target
        description: Away team shots on target
      - name: home_total_passes
        description: Total home team passes
      - name: home_completed_passes
        description: Home team completed passes
      - name: home_progressive_passes
        description: Home team progressive passes
      - name: away_total_passes
        description: Total away team passes
      - name: away_completed_passes
        description: Away team completed passes
      - name: away_progressive_passes
        description: Away team progressive passes
      - name: xg_difference
        description: Home xG minus away xG (positive = home advantage)
```

**Step 12: Add missing columns to fct_tracking_frames**

```yaml
      - name: period
        description: Match period (1 or 2)
      - name: frame
        description: Frame number within the period
      - name: timestamp_seconds
        description: Timestamp in seconds from period start
      - name: team
        description: Team affiliation (home or away)
      - name: x
        description: Player X coordinate (120-yard scale)
      - name: y
        description: Player Y coordinate (80-yard scale)
      - name: ball_x
        description: Ball X coordinate (120-yard scale)
      - name: ball_y
        description: Ball Y coordinate (80-yard scale)
      - name: velocity_x
        description: Player velocity in X direction (yards/second)
      - name: velocity_y
        description: Player velocity in Y direction (yards/second)
      - name: voronoi_area
        description: Voronoi cell area controlled by player (NULL until spatial pipeline)
```

**Step 13: Add missing columns to fct_player_embeddings**

```yaml
      - name: avg_x
        description: Average X position during the match
      - name: avg_y
        description: Average Y position during the match
      - name: stddev_x
        description: Standard deviation of X position (positional spread)
      - name: stddev_y
        description: Standard deviation of Y position (positional spread)
      - name: pct_defensive_third
        description: Percentage of frames in the defensive third
      - name: pct_middle_third
        description: Percentage of frames in the middle third
      - name: pct_attacking_third
        description: Percentage of frames in the attacking third
      - name: avg_distance_to_ball
        description: Average distance to ball during the match
```

**Step 14: Commit**

```bash
git add dbt_project/models/staging/statsbomb/_statsbomb__models.yml dbt_project/models/staging/wyscout/_wyscout__models.yml dbt_project/models/intermediate/_intermediate__models.yml dbt_project/models/marts/_marts__models.yml
git commit -m "docs: add descriptions for all undocumented YAML columns"
```

---

### Task 9: Integrate position_mapping.csv into dim_players

**Files:**
- Modify: `dbt_project/models/marts/dim_players.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

**Step 1: Modify dim_players.sql to join position_mapping**

Replace the `final` CTE:

```sql
final as (

    select
        sp.player_id,
        sp.player_name,
        -- Use nickname if available, otherwise full name
        coalesce(sp.player_nickname, sp.player_name)        as player_display_name,
        sp.primary_position,
        -- Map position to group via seed (Goalkeeper, Defender, Midfielder, Forward)
        pm.position_group,
        sp.data_source

    from statsbomb_players sp
    left join {{ ref('position_mapping') }} pm
        on sp.primary_position = pm.position_name
    where sp.rn = 1

)
```

**Step 2: Add position_group to _marts__models.yml**

Under `dim_players.columns`, add:

```yaml
      - name: position_group
        description: >
          Broad position category (Goalkeeper, Defender, Midfielder, Forward)
          mapped from primary_position via position_mapping seed
        data_tests:
          - accepted_values:
              arguments:
                values: ['Goalkeeper', 'Defender', 'Midfielder', 'Forward']
              config:
                severity: warn
```

**Step 3: Commit**

```bash
git add dbt_project/models/marts/dim_players.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat: integrate position_mapping seed into dim_players for position_group"
```

---

### Task 10: Terraform Security — App Resources + Lakebase Access

**Files:**
- Modify: `terraform/modules/app/main.tf`
- Modify: `terraform/modules/app/variables.tf`
- Modify: `terraform/environments/dev/main.tf`

**Step 1: Add variables to app module**

In `terraform/modules/app/variables.tf`, add:

```hcl
variable "sql_warehouse_id" {
  description = "SQL warehouse ID for the app to use"
  type        = string
  default     = ""
}

variable "lakebase_instance_name" {
  description = "Lakebase instance name for the app to connect to"
  type        = string
  default     = ""
}
```

**Step 2: Add resources block to databricks_app**

Replace `terraform/modules/app/main.tf`:

```hcl
# ──────────────────────────────────────────────────────────────────────────────
# Module: App — Databricks Apps (Streamlit Dashboard)
# ──────────────────────────────────────────────────────────────────────────────
# Deploys the soccer analytics Streamlit dashboard as a Databricks App.
#
# Databricks Apps provides:
#   - Managed hosting with workspace-level authentication
#   - Direct access to Unity Catalog data via Lakebase/SQL warehouse
#   - Automatic HTTPS and SSO integration
#   - No separate infrastructure to manage
#
# The resources block grants the app's service principal explicit access to
# the SQL warehouse and Lakebase instance — no broader permissions needed.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_app" "streamlit" {
  name        = "soccer-analytics-dashboard-${var.environment}"
  description = "Soccer analytics Streamlit dashboard — explore shots, passes, player stats, and match summaries with interactive visualizations."

  resources {
    name = "sql-warehouse"
    sql_warehouse {
      id         = var.sql_warehouse_id
      permission = "CAN_USE"
    }
  }
}
```

Note: The Lakebase `database_instance` resource type may not yet be supported in the `databricks_app.resources` block as of provider 1.98. If the Terraform provider does not support a `database_instance` resource type inside `resources`, skip it and add a TODO comment. The Lakebase connection restriction (S1) for OAuth-only access is already enforced at the instance level (`effective_enable_pg_native_login = false`).

**Step 3: Wire variables in dev main.tf**

Replace the app module block in `terraform/environments/dev/main.tf`:

```hcl
module "app" {
  source = "../../modules/app"

  environment            = var.environment
  sql_warehouse_id       = module.sql_warehouse.warehouse_id
  lakebase_instance_name = module.lakebase.instance_name
}
```

**Step 4: Add instance_name output to lakebase module if missing**

Check if `terraform/modules/lakebase/outputs.tf` exports `instance_name`. If not, create it:

```hcl
output "instance_name" {
  description = "Name of the Lakebase database instance"
  value       = databricks_database_instance.soccer_analytics.name
}
```

**Step 5: Commit**

```bash
git add terraform/modules/app/main.tf terraform/modules/app/variables.tf terraform/environments/dev/main.tf
git commit -m "security: add resources block to databricks_app for least-privilege access"
```

---

### Final Step: Update TODO.md

**Files:**
- Modify: `TODO.md`

Mark all completed tech debt items as done. Update the security items to reflect what was completed vs deferred. Commit:

```bash
git add TODO.md
git commit -m "docs: update TODO.md — tech debt resolved, security items updated"
```
