# OPT-2: dbt Mart Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add liquid clustering to 5 embedding marts, create provider-agnostic `int_minutes_played_per_match`, fix SkillCorner bronze-completeness, create `stg_wyscout__lineups`, refactor downstream minutes consumers, and prune `select *` in 5 marts.

**Architecture:** Single PR with independent sub-items (a), (b), (c). Sub-item (b) has a dependency chain: (b.1) SkillCorner ingestion fix → (b.2) Wyscout lineups staging → (b) intermediate model → (b.3)/(b.4) downstream refactors. All dbt SQL; one Python ingestion fix with bronze migration.

**Tech Stack:** dbt (Databricks SQL), Python (ingestion), Delta Lake bronze migrations

**Spec:** `docs/superpowers/specs/2026-05-19-opt-2-dbt-mart-cleanup-design.md` (v5)

---

## File Map

### New files
- `dbt_project/models/intermediate/int_minutes_played_per_match.sql` — provider-agnostic per-match minutes (4 provider legs + dim JOINs)
- `dbt_project/models/staging/wyscout/stg_wyscout__lineups.sql` — extract lineup/sub data from `teams_data_parsed`
- `dbt_project/tests/assert_minutes_played_range.sql` — singular test: 0 ≤ minutes ≤ 130
- `dbt_project/tests/assert_minutes_played_roster_count.sql` — singular test: 22–32 players per match
- `dbt_project/tests/assert_idsse_minutes_roster_vs_tracking_context.sql` — singular test: IDSSE roster subset
- `dbt_project/tests/assert_skillcorner_minutes_parity.sql` — singular test: SC passthrough parity
- `dbt_project/tests/assert_wyscout_lineups_starter_count.sql` — singular test: 22 starters per match
- `scripts/migrations/2026-05-19-skillcorner-matches-add-playing-time-cols.sql` — bronze migration

### Modified files
- `src/ingestion/skillcorner_matches.py` — add 12 dropped fields to `parse_match_json()`
- `src/tests/test_skillcorner_e2e.py` — add assertions for new fields
- `dbt_project/models/staging/skillcorner/stg_skillcorner__matches.sql` — add passthrough columns
- `dbt_project/models/staging/skillcorner/_skillcorner__sources.yml` — add source column docs
- `dbt_project/models/staging/wyscout/_wyscout__models.yml` — append `stg_wyscout__lineups` YAML contract
- `dbt_project/models/intermediate/int_minutes_played.sql` — refactor to aggregate from `int_minutes_played_per_match`
- `dbt_project/models/intermediate/_intermediate__models.yml` — add `int_minutes_played_per_match` YAML + update `int_minutes_played` grain
- `dbt_project/models/marts/fct_goalkeeper_stats.sql` — delete inline minutes CTEs, JOIN intermediate
- `dbt_project/models/marts/fct_player_stats.sql` — add `data_source` to `int_minutes_played` JOIN
- `dbt_project/models/marts/fct_player_embeddings.sql` — add `liquid_clustered_by`
- `dbt_project/models/marts/fct_player_embeddings_career.sql` — add `liquid_clustered_by`
- `dbt_project/models/marts/fct_player_embeddings_season.sql` — add `liquid_clustered_by`
- `dbt_project/models/marts/fct_player_embeddings_career_360.sql` — add `liquid_clustered_by`
- `dbt_project/models/marts/fct_player_embeddings_season_360.sql` — add `liquid_clustered_by`
- `dbt_project/models/marts/fct_action_values.sql` — replace `select *` with explicit columns
- `dbt_project/models/marts/fct_defcon_pressure.sql` — replace `select *` with explicit columns
- `dbt_project/models/marts/fct_defcon_actions.sql` — replace `select *` with explicit columns
- `dbt_project/models/marts/fct_defensive_values.sql` — replace `select *` with explicit columns
- `dbt_project/models/marts/fct_formation_labels.sql` — replace `select *` with explicit columns

---

## Task 1: Embedding Mart Liquid Clustering (sub-item a)

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings.sql:1-6`
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql:1-5`
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql:1-5`
- Modify: `dbt_project/models/marts/fct_player_embeddings_career_360.sql:1-5`
- Modify: `dbt_project/models/marts/fct_player_embeddings_season_360.sql:1-5`

- [ ] **Step 1: Add `liquid_clustered_by` to `fct_player_embeddings.sql`**

Change the config block from:
```sql
{{ config(
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart']
) }}
```
to:
```sql
{{ config(
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['canonical_player_id', 'match_key'],
    tags=['marts', 'output_mart']
) }}
```

- [ ] **Step 2: Add `liquid_clustered_by` to the 4 aggregation marts**

For `fct_player_embeddings_career.sql`, change:
```sql
{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false),
    tags=['marts', 'output_mart']
) }}
```
to:
```sql
{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false),
    liquid_clustered_by=['canonical_player_id'],
    tags=['marts', 'output_mart']
) }}
```

Apply the same change (adding `liquid_clustered_by=['canonical_player_id'],`) to:
- `fct_player_embeddings_season.sql`
- `fct_player_embeddings_career_360.sql`
- `fct_player_embeddings_season_360.sql`

All 4 have identical config blocks; add the line between `enabled=...` and `tags=...`.

- [ ] **Step 3: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select fct_player_embeddings fct_player_embeddings_career fct_player_embeddings_season fct_player_embeddings_career_360 fct_player_embeddings_season_360 --vars '{embeddings_enabled: true}'`

Expected: Compiles without errors.

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/marts/fct_player_embeddings*.sql
git commit -m "feat(dbt): add liquid_clustered_by to 5 embedding marts"
```

---

## Task 2: SkillCorner Bronze-Completeness Fix (sub-item b.1)

**Files:**
- Modify: `src/ingestion/skillcorner_matches.py:25-98`
- Modify: `src/tests/test_skillcorner_e2e.py`
- Create: `scripts/migrations/2026-05-19-skillcorner-matches-add-playing-time-cols.sql`

- [ ] **Step 1: Write failing test for new fields**

In `src/tests/test_skillcorner_e2e.py`, add a new test method inside `TestSkillCornerE2E`:

```python
    def test_matches_playing_time_fields(self, matches_df: pd.DataFrame) -> None:
        """parse_match_json must preserve all playing_time fields from match.json."""
        required_cols = [
            "minutes_played",
            "start_frame",
            "end_frame",
            "minutes_tip",
            "minutes_otip",
            "start_time",
            "end_time",
            "yellow_card",
            "red_card",
            "injured",
            "goal",
            "own_goal",
            "trackable_object",
            "birthday",
            "gender",
            "team_player_id",
        ]
        for col in required_cols:
            assert col in matches_df.columns, f"Missing column: {col}"

        # Spot-check first player's minutes_played is non-null and reasonable
        assert matches_df["minutes_played"].notna().any()
        assert (matches_df["minutes_played"].dropna() >= 0).all()
        assert (matches_df["minutes_played"].dropna() <= 130).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_skillcorner_e2e.py::TestSkillCornerE2E::test_matches_playing_time_fields -v`

Expected: FAIL — columns not present.

- [ ] **Step 3: Add new fields to `parse_match_json()`**

In `src/ingestion/skillcorner_matches.py`, update the `_MATCHES_DTYPE_OVERRIDES` dict (line 25) to add:

```python
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
    # Playing time fields (b.1 bronze-completeness fix)
    "start_frame": "Int64",
    "end_frame": "Int64",
    "yellow_card": "Int64",
    "red_card": "Int64",
    "goal": "Int64",
    "own_goal": "Int64",
    "trackable_object": "Int64",
    "team_player_id": "Int64",
}
```

Then update the `rows.append(...)` call (lines 73-98) to add all 12 dropped fields. After the existing `"period_boundaries": period_boundaries,` line, add:

```python
                # b.1 bronze-completeness: playing_time + player-level metadata
                "start_time": player.get("start_time", ""),
                "end_time": player.get("end_time", ""),
                "minutes_played": (player.get("playing_time") or {}).get("total", {}).get("minutes_played"),
                "start_frame": (player.get("playing_time") or {}).get("total", {}).get("start_frame"),
                "end_frame": (player.get("playing_time") or {}).get("total", {}).get("end_frame"),
                "minutes_tip": (player.get("playing_time") or {}).get("total", {}).get("minutes_tip"),
                "minutes_otip": (player.get("playing_time") or {}).get("total", {}).get("minutes_otip"),
                "yellow_card": player.get("yellow_card"),
                "red_card": player.get("red_card"),
                "injured": player.get("injured"),
                "goal": player.get("goal"),
                "own_goal": player.get("own_goal"),
                "trackable_object": player.get("trackable_object"),
                "birthday": player.get("birthday", ""),
                "gender": player.get("gender", ""),
                "team_player_id": player.get("team_player_id"),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_skillcorner_e2e.py::TestSkillCornerE2E::test_matches_playing_time_fields -v`

Expected: PASS

- [ ] **Step 5: Run full SkillCorner test suite**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_skillcorner_e2e.py -v`

Expected: All tests pass (existing tests unaffected).

- [ ] **Step 6: Create bronze migration**

Create `scripts/migrations/2026-05-19-skillcorner-matches-add-playing-time-cols.sql`:

```sql
-- OPT-2 sub-item b.1: Add 16 dropped fields to skillcorner_matches bronze table.
-- ALTER TABLE ADD COLUMNS is idempotent on Databricks (existing rows get NULL).
ALTER TABLE soccer_analytics.bronze.skillcorner_matches
ADD COLUMNS (
    start_time STRING,
    end_time STRING,
    minutes_played DOUBLE,
    start_frame BIGINT,
    end_frame BIGINT,
    minutes_tip DOUBLE,
    minutes_otip DOUBLE,
    yellow_card INT,
    red_card INT,
    injured BOOLEAN,
    goal INT,
    own_goal INT,
    trackable_object BIGINT,
    birthday STRING,
    gender STRING,
    team_player_id BIGINT
);
```

- [ ] **Step 7: Add staging passthrough columns**

In `dbt_project/models/staging/skillcorner/stg_skillcorner__matches.sql`, add the new columns to the SELECT list (after `period_boundaries,` and before `_ingested_at`):

```sql
    -- b.1 bronze-completeness: playing_time + player metadata
    start_time,
    end_time,
    minutes_played,
    start_frame,
    end_frame,
    minutes_tip,
    minutes_otip,
    yellow_card,
    red_card,
    injured,
    goal,
    own_goal,
    trackable_object,
    birthday,
    gender,
    team_player_id,
```

- [ ] **Step 8: Add source column docs**

In `dbt_project/models/staging/skillcorner/_skillcorner__sources.yml`, under the `skillcorner_matches` source table's `columns:` list (after the `_ingested_at` column entry), add:

```yaml
    - name: start_time
      description: Player entry time on pitch (HH:MM:SS format)
    - name: end_time
      description: Player exit time from pitch (HH:MM:SS format)
    - name: minutes_played
      description: Pre-computed total minutes played (from playing_time.total.minutes_played)
    - name: start_frame
      description: First tracking frame for this player (from playing_time.total.start_frame)
    - name: end_frame
      description: Last tracking frame for this player (from playing_time.total.end_frame)
    - name: minutes_tip
      description: Minutes team in possession (from playing_time.total.minutes_tip)
    - name: minutes_otip
      description: Minutes team out of possession (from playing_time.total.minutes_otip)
    - name: yellow_card
      description: Number of yellow cards received
    - name: red_card
      description: Number of red cards received
    - name: injured
      description: Whether the player was injured during the match
    - name: goal
      description: Number of goals scored
    - name: own_goal
      description: Number of own goals
    - name: trackable_object
      description: SkillCorner tracking system object ID
    - name: birthday
      description: Player date of birth (ISO date string)
    - name: gender
      description: Player gender
    - name: team_player_id
      description: Team-specific player identifier
```

- [ ] **Step 9: Commit**

```bash
git add src/ingestion/skillcorner_matches.py src/tests/test_skillcorner_e2e.py \
  scripts/migrations/2026-05-19-skillcorner-matches-add-playing-time-cols.sql \
  dbt_project/models/staging/skillcorner/stg_skillcorner__matches.sql \
  dbt_project/models/staging/skillcorner/_skillcorner__sources.yml
git commit -m "fix(skillcorner): restore 12 source fields (16 cols after flattening) in parse_match_json (b.1)"
```

---

## Task 3: Wyscout Lineups Staging View (sub-item b.2)

**Files:**
- Create: `dbt_project/models/staging/wyscout/stg_wyscout__lineups.sql`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml` (append lineups model)
- Create: `dbt_project/tests/assert_wyscout_lineups_starter_count.sql`

- [ ] **Step 1: Create `stg_wyscout__lineups.sql`**

The `teams_data_parsed` MAP in `stg_wyscout__matches` has this structure:
```
MAP<STRING, STRUCT<side, teamId, coachId, score, scoreET, scoreP, hasFormation,
  formation: STRUCT<
    lineup: ARRAY<STRUCT<playerId: BIGINT, ...>>,
    bench: ARRAY<STRUCT<playerId: BIGINT, ...>>,
    substitutions: ARRAY<STRUCT<playerIn: BIGINT, playerOut: BIGINT, minute: BIGINT>>
  >
>>
```

Create `dbt_project/models/staging/wyscout/stg_wyscout__lineups.sql`:

```sql
-- stg_wyscout__lineups.sql
-- Extract per-player lineup participation from Wyscout match metadata.
--
-- Source: stg_wyscout__matches.teams_data_parsed MAP.
-- Grain: one row per (match_id, team_id, player_id).
-- Each row carries pre-resolved minute_on/minute_off for direct minutes calculation.
--
-- Starters: minute_on = 0, minute_off = substitution minute (or NULL if played to end).
-- Subs entering: minute_on = substitution minute, minute_off = NULL (or next sub minute).
-- Bench players who never enter are excluded (they have no minutes).

with matches as (

    select
        match_id,
        teams_data_parsed
    from {{ ref('stg_wyscout__matches') }}
    where teams_data_parsed is not null

),

-- Explode the MAP to get one row per team per match.
teams as (

    select
        m.match_id,
        cast(t.key as string)             as team_id,
        t.value.formation                 as formation
    from matches m
    lateral view explode(m.teams_data_parsed) t as key, value
    where t.value.formation is not null

),

-- Starters from lineup array.
-- Type: playerId is BIGINT in the from_json MAP schema (stg_wyscout__matches:61).
-- Keep as BIGINT for consistency with stg_wyscout__events.player_id.
starters as (

    select
        t.match_id,
        t.team_id,
        cast(p.playerId as bigint)        as player_id,
        true                              as is_starter,
        0                                 as minute_on
    from teams t
    lateral view explode(t.formation.lineup) l as p

),

-- Substitutes entering from substitutions array.
subs_in as (

    select
        t.match_id,
        t.team_id,
        cast(s.playerIn as bigint)        as player_id,
        false                             as is_starter,
        cast(s.minute as int)             as minute_on
    from teams t
    lateral view explode(t.formation.substitutions) sub as s

),

-- Substitution-off events (starters leaving).
-- Assumption: each player appears at most once in subs_off per (match, team).
-- The unique_combination_of_columns test on (match_id, team_id, player_id)
-- catches any Wyscout data quality issue with duplicate substitution entries.
-- Standard single-chain subs (A→B at 60', B→C at 75') resolve correctly:
-- A gets minute_off=60, B gets minute_on=60 + minute_off=75.
subs_off as (

    select
        t.match_id,
        t.team_id,
        cast(s.playerOut as bigint)       as player_id,
        cast(s.minute as int)             as minute_off
    from teams t
    lateral view explode(t.formation.substitutions) sub as s

),

-- Combine starters + subs entering, then LEFT JOIN sub-off minute.
combined as (

    select * from starters
    union all
    select * from subs_in

),

final as (

    select
        c.match_id,
        c.team_id,
        c.player_id,
        c.is_starter,
        c.minute_on,
        so.minute_off
    from combined c
    left join subs_off so
        on  c.match_id = so.match_id
        and c.team_id = so.team_id
        and c.player_id = so.player_id

)

select * from final
```

- [ ] **Step 2: Add YAML contract to `_wyscout__models.yml`**

Append the following model entry to the end of `dbt_project/models/staging/wyscout/_wyscout__models.yml` (Wyscout staging uses a single combined models YAML file — do NOT create a separate `_wyscout__lineups.yml`):

```yaml
  - name: stg_wyscout__lineups
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Per-player lineup participation extracted from stg_wyscout__matches.teams_data_parsed.
      One row per (match_id, team_id, player_id). Pre-resolves minute_on/minute_off from
      substitution events so downstream consumers compute minutes as
      COALESCE(minute_off, match_end) - minute_on.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - match_id
              - team_id
              - player_id
    columns:
      - name: match_id
        description: Wyscout native match ID
        data_tests:
          - not_null
      - name: team_id
        description: Wyscout team ID (MAP key from teams_data_parsed)
        data_tests:
          - not_null
      - name: player_id
        description: Wyscout player ID from lineup or bench array
        data_tests:
          - not_null
      - name: is_starter
        description: True for lineup players, false for substitutes entering
        data_tests:
          - not_null
      - name: minute_on
        description: Minute player entered (0 for starters, sub minute for subs)
        data_tests:
          - not_null
      - name: minute_off
        description: Minute player exited (NULL if played to end of match)
```

- [ ] **Step 3: Create starter-count test**

Create `dbt_project/tests/assert_wyscout_lineups_starter_count.sql`:

```sql
-- assert_wyscout_lineups_starter_count.sql
-- Per match, exactly 22 starters (11 per team). Hard failure.

select
    match_id,
    count(*) as starter_count
from {{ ref('stg_wyscout__lineups') }}
where is_starter = true
group by match_id
having count(*) != 22
```

- [ ] **Step 4: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select stg_wyscout__lineups`

Expected: Compiles without errors.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/models/staging/wyscout/stg_wyscout__lineups.sql \
  dbt_project/models/staging/wyscout/_wyscout__models.yml \
  dbt_project/tests/assert_wyscout_lineups_starter_count.sql
git commit -m "feat(dbt): add stg_wyscout__lineups staging view (b.2)"
```

---

## Task 4: Create `int_minutes_played_per_match` (sub-item b)

**Files:**
- Create: `dbt_project/models/intermediate/int_minutes_played_per_match.sql`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Create: `dbt_project/tests/assert_minutes_played_range.sql`
- Create: `dbt_project/tests/assert_minutes_played_roster_count.sql`
- Create: `dbt_project/tests/assert_idsse_minutes_roster_vs_tracking_context.sql`
- Create: `dbt_project/tests/assert_skillcorner_minutes_parity.sql`

- [ ] **Step 1: Create `int_minutes_played_per_match.sql`**

Create `dbt_project/models/intermediate/int_minutes_played_per_match.sql`:

```sql
{{ config(
    materialized='view',
    tags=['intermediate_mart']
) }}
-- int_minutes_played_per_match.sql
-- Provider-agnostic per-match minutes played per player.
--
-- Grain: one row per (match_key, player_key).
-- Outputs surrogates only — no native IDs. IDSSE uses DFL string IDs
-- (e.g. "DFL-MAT-J03WN9") that cannot be cast to BIGINT, so a uniform
-- surrogate-only contract avoids the type mismatch across providers.
--
-- Provider legs:
--   StatsBomb: event-based (lineup + substitution events + max-minute duration)
--   Wyscout:   event-based (stg_wyscout__lineups minute_on/minute_off + last event_sec)
--   IDSSE:     event-based (tracking_context roster + substitution events + FinalWhistle)
--   SkillCorner: metadata-direct (pre-computed minutes_played from match.json)
--   Metrica:   excluded (anonymized sample data, no substitution events)

-- ===== StatsBomb leg =====

with sb_lineups as (

    select
        match_id,
        player_id
    from {{ ref('stg_statsbomb__lineups') }}
    where position_name is not null

),

sb_events as (

    select
        match_id,
        minute,
        event_type,
        player_id,
        substitution_replacement_id
    from {{ ref('stg_statsbomb__events') }}

),

sb_match_duration as (

    select
        match_id,
        max(minute) + 1                                 as match_end_minute
    from sb_events
    group by match_id

),

sb_sub_off as (

    select
        match_id,
        player_id,
        minute                                           as off_minute
    from sb_events
    where event_type = 'Substitution'

),

sb_sub_on as (

    select
        match_id,
        cast(substitution_replacement_id as int)         as player_id,
        minute                                           as on_minute
    from sb_events
    where event_type = 'Substitution'
      and substitution_replacement_id is not null

),

sb_player_minutes as (

    -- Starting XI
    select
        l.match_id,
        l.player_id,
        coalesce(so.off_minute, md.match_end_minute)     as minutes_played
    from sb_lineups l
    inner join sb_match_duration md
        on l.match_id = md.match_id
    left join sb_sub_off so
        on l.match_id = so.match_id
        and l.player_id = so.player_id

    union all

    -- Substitutes coming on
    select
        son.match_id,
        son.player_id,
        coalesce(soff.off_minute, md.match_end_minute) - son.on_minute as minutes_played
    from sb_sub_on son
    inner join sb_match_duration md
        on son.match_id = md.match_id
    left join sb_sub_off soff
        on son.match_id = soff.match_id
        and son.player_id = soff.player_id

),

sb_deduped as (

    select
        match_id,
        player_id,
        'statsbomb'                                      as data_source,
        cast(max(minutes_played) as double)              as minutes_played
    from sb_player_minutes
    group by match_id, player_id

),

-- ===== Wyscout leg =====

ws_lineups as (

    select
        match_id,
        player_id,
        minute_on,
        minute_off
    from {{ ref('stg_wyscout__lineups') }}

),

ws_match_duration as (

    -- Last event second per match, converted to minutes.
    -- Fallback 90 applied only when the match has zero events.
    select
        match_id,
        coalesce(max(event_sec) / 60.0, 90.0)           as match_end_minute
    from {{ ref('stg_wyscout__events') }}
    group by match_id

),

ws_player_minutes as (

    select
        wl.match_id,
        wl.player_id,
        'wyscout'                                        as data_source,
        cast(
            coalesce(wl.minute_off, wd.match_end_minute) - wl.minute_on
        as double)                                       as minutes_played
    from ws_lineups wl
    inner join ws_match_duration wd
        on wl.match_id = wd.match_id

),

-- ===== IDSSE leg =====

idsse_roster as (

    -- Player roster per match from tracking context (TC-1).
    -- Every player who generated at least one SPADL action appears here.
    select distinct
        native_match_id,
        player_id_native
    from {{ ref('stg_spadl__tracking_context') }}
    where data_source = 'idsse'

),

idsse_events as (

    select
        cast(match_id as string)                         as native_match_id,
        event_type,
        period,
        timestamp_seconds,
        sub_player_in,
        sub_player_out,
        sub_team
    from {{ ref('stg_idsse__events') }}

),

idsse_period_duration as (

    -- FinalWhistle timestamp_seconds per period (period-local).
    select
        native_match_id,
        period,
        max(timestamp_seconds)                           as period_end_seconds
    from idsse_events
    where event_type = 'FinalWhistle'
    group by native_match_id, period

),

idsse_match_duration as (

    -- Total match duration = sum of all period durations.
    select
        native_match_id,
        sum(period_end_seconds)                          as match_end_seconds
    from idsse_period_duration
    group by native_match_id

),

idsse_period1_duration as (

    -- Period 1 duration for converting period 2 timestamps to match-absolute.
    select
        native_match_id,
        period_end_seconds                               as p1_end_seconds
    from idsse_period_duration
    where period = 1

),

idsse_subs as (

    -- Substitution events with match-absolute seconds.
    select
        e.native_match_id,
        cast(e.sub_player_in as string)                  as player_in_native,
        cast(e.sub_player_out as string)                 as player_out_native,
        case
            when e.period = 1 then e.timestamp_seconds
            else coalesce(p1.p1_end_seconds, 0) + e.timestamp_seconds
        end                                              as sub_absolute_seconds
    from idsse_events e
    left join idsse_period1_duration p1
        on e.native_match_id = p1.native_match_id
    where e.event_type = 'Substitution'
      and e.sub_player_in is not null

),

idsse_sub_on as (

    select
        native_match_id,
        player_in_native                                 as player_id_native,
        sub_absolute_seconds                             as on_seconds
    from idsse_subs

),

idsse_sub_off as (

    select
        native_match_id,
        player_out_native                                as player_id_native,
        sub_absolute_seconds                             as off_seconds
    from idsse_subs

),

idsse_corrected as (

    select
        r.native_match_id,
        r.player_id_native,
        'idsse'                                          as data_source,
        cast(case
            when son.on_seconds is not null
            -- Substitute: played from sub entry to sub exit or match end.
            then (coalesce(soff.off_seconds, md.match_end_seconds) - son.on_seconds) / 60.0
            -- Starter: played from 0 to sub exit or match end.
            else coalesce(soff.off_seconds, md.match_end_seconds) / 60.0
        end as double)                                   as minutes_played
    from idsse_roster r
    inner join idsse_match_duration md
        on r.native_match_id = md.native_match_id
    left join idsse_sub_on son
        on  r.native_match_id = son.native_match_id
        and r.player_id_native = son.player_id_native
    left join idsse_sub_off soff
        on  r.native_match_id = soff.native_match_id
        and r.player_id_native = soff.player_id_native

),

-- ===== SkillCorner leg =====

sc_player_minutes as (

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        'skillcorner'                                    as data_source,
        cast(minutes_played as double)                   as minutes_played
    from {{ ref('stg_skillcorner__matches') }}
    where minutes_played is not null

),

-- ===== UNION all legs (native IDs as STRING for uniform dim resolution) =====

unioned as (

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        data_source,
        minutes_played
    from sb_deduped

    union all

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        data_source,
        minutes_played
    from ws_player_minutes

    union all

    select
        native_match_id,
        player_id_native,
        data_source,
        minutes_played
    from idsse_corrected

    union all

    select
        native_match_id,
        player_id_native,
        data_source,
        minutes_played
    from sc_player_minutes

),

-- ===== Single dim resolution — surrogate-only output =====
-- IDSSE native IDs are DFL strings (e.g. "DFL-MAT-J03WN9") that cannot
-- be cast to BIGINT. Outputting surrogates only avoids the type mismatch.
-- Downstream consumers JOIN on (match_key, player_key).

final as (

    select
        dm.match_key,
        dp.player_key,
        u.data_source,
        u.minutes_played
    from unioned u
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = u.data_source
        and dm.native_match_id = u.native_match_id
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = u.data_source
        and dp.native_player_id = u.player_id_native

)

select
    match_key,
    player_key,
    data_source,
    minutes_played
from final
where minutes_played is not null
  and minutes_played >= 0
```

- [ ] **Step 2: Add YAML contract in `_intermediate__models.yml`**

Insert the following block BEFORE the existing `int_minutes_played` entry (line 269). Find the line `  - name: int_minutes_played` and insert before it:

```yaml
  - name: int_minutes_played_per_match
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Provider-agnostic per-match minutes played per player. Four provider legs:
      StatsBomb (event-based), Wyscout (event-based), IDSSE (event-based from
      tracking_context roster + substitution events), SkillCorner (metadata-direct
      from pre-computed playing_time). Metrica excluded. Grain: one row per
      (match_key, player_key). Materialized as view.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - match_key
              - player_key
    columns:
      - name: match_key
        description: FK to dim_matches (Kimball surrogate)
        data_tests:
          - not_null
      - name: player_key
        description: FK to dim_players (Kimball surrogate)
        data_tests:
          - not_null
      - name: data_source
        description: Provider name (statsbomb, wyscout, idsse, skillcorner)
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'skillcorner']
      - name: minutes_played
        description: Minutes on pitch (0.0–130.0, extra time included)
        data_tests:
          - not_null

```

- [ ] **Step 3: Create singular tests**

Create `dbt_project/tests/assert_minutes_played_range.sql`:

```sql
-- assert_minutes_played_range.sql
-- Minutes must be between 0 and 130 (extra time max).
select
    match_key,
    player_key,
    data_source,
    minutes_played
from {{ ref('int_minutes_played_per_match') }}
where minutes_played < 0
   or minutes_played > 130
```

Create `dbt_project/tests/assert_minutes_played_roster_count.sql`:

```sql
-- assert_minutes_played_roster_count.sql
-- Per match, 22–32 players with minutes > 0 (22 starters + up to 5 subs per team).
select
    match_key,
    data_source,
    count(*) as player_count
from {{ ref('int_minutes_played_per_match') }}
where minutes_played > 0
group by match_key, data_source
having count(*) < 22
    or count(*) > 32
```

Create `dbt_project/tests/assert_idsse_minutes_roster_vs_tracking_context.sql`:

```sql
-- assert_idsse_minutes_roster_vs_tracking_context.sql
-- IDSSE players in int_minutes_played_per_match must be a subset of
-- tracking_context roster. Uses surrogates (match_key, player_key) since
-- the intermediate outputs surrogates only — IDSSE native IDs are DFL
-- strings that cannot be cast to BIGINT.

with minutes_players as (

    select distinct
        match_key,
        player_key
    from {{ ref('int_minutes_played_per_match') }}
    where data_source = 'idsse'

),

tc_players as (

    -- Resolve tracking_context native IDs to surrogates via dim JOINs.
    select distinct
        dm.match_key,
        dp.player_key
    from {{ ref('stg_spadl__tracking_context') }} tc
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'idsse'
        and dm.native_match_id = tc.native_match_id
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = 'idsse'
        and dp.native_player_id = tc.player_id_native
    where tc.data_source = 'idsse'

)

select
    mp.match_key,
    mp.player_key
from minutes_players mp
left join tc_players tc
    on  mp.match_key = tc.match_key
    and mp.player_key = tc.player_key
where tc.player_key is null
```

Create `dbt_project/tests/assert_skillcorner_minutes_parity.sql`:

```sql
-- assert_skillcorner_minutes_parity.sql
-- SkillCorner minutes are a direct passthrough — SUM per match must match source.
-- Uses match_key (surrogate) since int_minutes_played_per_match outputs surrogates only.

with new_agg as (

    select
        match_key,
        sum(minutes_played) as total_minutes
    from {{ ref('int_minutes_played_per_match') }}
    where data_source = 'skillcorner'
    group by match_key

),

source_agg as (

    -- Resolve SkillCorner native match_id to match_key for comparison.
    select
        dm.match_key,
        sum(cast(sm.minutes_played as double)) as total_minutes
    from {{ ref('stg_skillcorner__matches') }} sm
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'skillcorner'
        and dm.native_match_id = cast(sm.match_id as string)
    where sm.minutes_played is not null
    group by dm.match_key

)

select
    n.match_key,
    n.total_minutes as new_total,
    s.total_minutes as source_total,
    abs(n.total_minutes - s.total_minutes) as delta
from new_agg n
inner join source_agg s
    on n.match_key = s.match_key
where abs(n.total_minutes - s.total_minutes) > 0.01
```

- [ ] **Step 4: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select int_minutes_played_per_match --vars '{pausa_enabled: true}'`

Expected: Compiles without errors. The `pausa_enabled` var is set project-wide in `dbt_project.yml:94` but is passed explicitly as a safety measure since `stg_idsse__events` (consumed by the IDSSE leg) has `enabled=var('pausa_enabled', false)` as a fallback default.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/models/intermediate/int_minutes_played_per_match.sql \
  dbt_project/models/intermediate/_intermediate__models.yml \
  dbt_project/tests/assert_minutes_played_range.sql \
  dbt_project/tests/assert_minutes_played_roster_count.sql \
  dbt_project/tests/assert_idsse_minutes_roster_vs_tracking_context.sql \
  dbt_project/tests/assert_skillcorner_minutes_parity.sql
git commit -m "feat(dbt): add int_minutes_played_per_match with 4 provider legs (b)"
```

---

## Task 5: Refactor `fct_goalkeeper_stats` (sub-item b.3)

**Files:**
- Modify: `dbt_project/models/marts/fct_goalkeeper_stats.sql`

This task has two parts:
1. Propagate `player_key` through the GK CTE chain so surrogate JOINs work.
2. Replace the inline StatsBomb minutes derivation with the new intermediate.

- [ ] **Step 1: Add `player_key` to `gk_players` CTE**

In `fct_goalkeeper_stats.sql`, find the `gk_players` CTE (lines 39-46):
```sql
gk_players as (
    select
        player_id
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'
),
```

Replace with:
```sql
gk_players as (
    select
        player_id,
        player_key
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'
),
```

- [ ] **Step 2: Add `player_key` to `gk_actions` CTE**

Find the `gk_actions` CTE (lines 48-68). Add `gk.player_key` to the SELECT list:
```sql
gk_actions as (
    select
        av.match_id,
        av.match_key,
        av.player_id,
        gk.player_key,
        -- ... rest of existing columns ...
```

(The `gk_actions` CTE already JOINs `gk_players gk` on `av.player_id = gk.player_id` — just add the column projection. Note: this native-ID JOIN will break when IDSSE/SkillCorner VAEP data flows through `fct_action_values` because `dim_players.player_id` is NULL for string-ID providers. Out of scope for this PR — flagged for future work.)

- [ ] **Step 3: Propagate `player_key` to `gk_matches` CTE**

Find the `gk_matches` CTE (lines 255-267). This is a `GROUP BY player_id, match_id, data_source` CTE — bare column references fail Spark validation. Add `min(player_key) as player_key` (same pattern as the existing `min(match_key) as match_key` at line 261):

```sql
gk_matches as (

    select
        player_id,
        match_id,
        data_source,
        min(match_key)      as match_key,
        min(player_key)     as player_key,
        min(team_id)        as team_id,
        min(competition_id) as competition_id,
        min(season_id)      as season_id
    from gk_actions
    group by player_id, match_id, data_source

),
```

- [ ] **Step 4: Replace inline minutes CTEs**

Delete lines 129-226 (the `events`, `lineups`, `match_duration`, `substitution_off`, `substitution_on`, `player_match_minutes`, and `minutes` CTEs). Replace them with:

```sql
-- Provider-agnostic minutes from int_minutes_played_per_match.
-- Surrogate-only: JOINs on (match_key, player_key).
minutes as (

    select
        imp.match_key,
        imp.player_key,
        imp.data_source,
        imp.minutes_played
    from {{ ref('int_minutes_played_per_match') }} imp

),
```

The old `minutes` CTE (lines 209-226) had an `INNER JOIN gk_players` filter — this was a performance optimization (only compute minutes for GKs). The new CTE drops this filter because `int_minutes_played_per_match` is a view over all players. The downstream JOINs on `(match_key, player_key)` naturally filter to GKs only.

- [ ] **Step 5: Update `sweeper_stats` minutes JOIN to surrogates**

Find the `sweeper_stats` CTE (lines 315-341). Change the minutes INNER JOIN from:
```sql
    from gk_actions ga
    inner join minutes m
        on ga.player_id = m.player_id
        and ga.match_id = m.match_id
        and ga.data_source = m.data_source
```
to:
```sql
    from gk_actions ga
    inner join minutes m
        on  ga.match_key = m.match_key
        and ga.player_key = m.player_key
        and ga.data_source = m.data_source
```

Note: alias is `ga` (gk_actions), NOT `gm` (gk_matches). Join type is `INNER`, NOT `LEFT`. `ga` has `match_key` (line 53) and `player_key` (added in Step 2).

- [ ] **Step 6: Update `final` CTE minutes JOIN to surrogates**

Find the `final` CTE (lines 467-491). Change the minutes LEFT JOIN from:
```sql
    left join minutes m
        on  gm.player_id = m.player_id
        and gm.match_id = m.match_id
        and gm.data_source = m.data_source
```
to:
```sql
    left join minutes m
        on  gm.match_key = m.match_key
        and gm.player_key = m.player_key
        and gm.data_source = m.data_source
```

- [ ] **Step 7: Remove redundant `dim_players` LEFT JOIN from `final`**

Find the `dim_players` LEFT JOIN in the `final` CTE (lines 498-500):
```sql
    left join {{ ref('dim_players') }} dp
        on  dp.provider = gm.data_source
       and dp.native_player_id = cast(gm.player_id as string)
```

Delete these lines. In the SELECT list (line 434), replace `dp.player_key` with `gm.player_key` (now propagated through the CTE chain via `min(player_key)` in `gk_matches`).

- [ ] **Step 8: Update file header comment**

Remove lines 32-35 (the comment about minutes being StatsBomb-only). Replace with:
```sql
--   - minutes from int_minutes_played_per_match (provider-agnostic).
```

- [ ] **Step 9: Verify the file compiles**

Run: `cd dbt_project && dbt compile --select fct_goalkeeper_stats --vars '{goalkeeper_enabled: true, pausa_enabled: true}'`

Expected: Compiles without errors.

- [ ] **Step 10: Commit**

```bash
git add dbt_project/models/marts/fct_goalkeeper_stats.sql
git commit -m "refactor(dbt): fct_goalkeeper_stats surrogate minutes from int_minutes_played_per_match (b.3)"
```

---

## Task 6: Refactor `int_minutes_played` + `fct_player_stats` JOIN (sub-item b.4)

**Files:**
- Modify: `dbt_project/models/intermediate/int_minutes_played.sql` (full rewrite)
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml:269-305`
- Modify: `dbt_project/models/marts/fct_player_stats.sql:240-243`
**Dependencies:**
- `stg_idsse__events` requires `pausa_enabled: true` (set project-wide in `dbt_project.yml:94`).
  All `dbt compile` commands below pass `--vars '{pausa_enabled: true}'` as a safety measure.

- [ ] **Step 1: Rewrite `int_minutes_played.sql`**

Replace the entire file with:

```sql
-- int_minutes_played.sql
-- Aggregate per-match minutes to per-player per-competition per-season totals.
--
-- Materialized as ephemeral (CTE).
-- Consumes int_minutes_played_per_match (surrogate-only) and resolves native
-- player_id via dim_players for downstream consumers that still JOIN on
-- native IDs (fct_player_stats).
--
-- Grain: one row per (player_id, data_source, competition_id, season_id).
-- Note: IDSSE rows get player_id = NULL because IDSSE native player IDs are
-- DFL strings (e.g. "DFL-OBJ-0028GH") that cannot be cast to BIGINT.
-- try_cast returns NULL for non-numeric strings (safe); BIGINT avoids
-- overflow for SkillCorner IDs that may exceed INT range (~2.1B).
-- The WHERE filter drops NULL rows — no regression since int_minutes_played
-- was previously StatsBomb-only and had no IDSSE rows.

with per_match as (

    select
        try_cast(dp.native_player_id as bigint)            as player_id,
        imp.data_source,
        dm.competition_id,
        dm.season_id,
        imp.minutes_played
    from {{ ref('int_minutes_played_per_match') }} imp
    inner join {{ ref('dim_matches') }} dm
        on imp.match_key = dm.match_key
    inner join {{ ref('dim_players') }} dp
        on imp.player_key = dp.player_key

)

select
    player_id,
    data_source,
    competition_id,
    season_id,
    sum(minutes_played) as total_minutes_played
from per_match
where player_id is not null
group by player_id, data_source, competition_id, season_id
```

- [ ] **Step 2: Update YAML contract**

In `_intermediate__models.yml`, find the `int_minutes_played` section (line 269). Make these changes:

1. Update the description:
```yaml
    description: >
      Per-player per-competition per-season total minutes played, aggregated from
      int_minutes_played_per_match. Provider-agnostic (StatsBomb, Wyscout, IDSSE,
      SkillCorner). Materialized as ephemeral (CTE). Uses try_cast(... as bigint)
      for player_id resolution — IDSSE DFL strings produce NULL (filtered out),
      SkillCorner large numeric IDs are safe from INT overflow. No regression
      from the previously StatsBomb-only implementation.
```

2. Update the uniqueness test to include `data_source`:
```yaml
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - player_id
              - data_source
              - competition_id
              - season_id
```

3. Add the `data_source` column definition (after `player_id`):
```yaml
      - name: data_source
        description: Provider name (statsbomb, wyscout, idsse, skillcorner)
        data_tests:
          - not_null
```

- [ ] **Step 3: Add `data_source` to `fct_player_stats` JOIN**

In `fct_player_stats.sql`, find lines 240-243:
```sql
    left join minutes m
        on coalesce(s.player_id, p.player_id) = m.player_id
        and coalesce(s.competition_id, p.competition_id) = m.competition_id
        and coalesce(s.season_id, p.season_id) = m.season_id
```

Replace with:
```sql
    left join minutes m
        on coalesce(s.player_id, p.player_id) = m.player_id
        and coalesce(s.data_source, p.data_source) = m.data_source
        and coalesce(s.competition_id, p.competition_id) = m.competition_id
        and coalesce(s.season_id, p.season_id) = m.season_id
```

- [ ] **Step 4: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select int_minutes_played fct_player_stats --vars '{pausa_enabled: true}'`

Expected: Compiles without errors.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/models/intermediate/int_minutes_played.sql \
  dbt_project/models/intermediate/_intermediate__models.yml \
  dbt_project/models/marts/fct_player_stats.sql
git commit -m "refactor(dbt): int_minutes_played aggregates from per-match, fix fct_player_stats JOIN (b.4)"
```

---

## Task 7: Column Pruning (sub-item c)

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql:37`
- Modify: `dbt_project/models/marts/fct_defcon_pressure.sql:32`
- Modify: `dbt_project/models/marts/fct_defcon_actions.sql:32`
- Modify: `dbt_project/models/marts/fct_defensive_values.sql:34`
- Modify: `dbt_project/models/marts/fct_formation_labels.sql:43`

- [ ] **Step 1: Replace `select *` in `fct_action_values.sql`**

Find the `action_values` CTE (lines 35-42):
```sql
with action_values as (
    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }} where match_id is not null)
    {% endif %}
)
```

Replace with:
```sql
with action_values as (
    select
        match_id,
        player_id,
        team_id,
        original_event_id,
        action_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        type_id,
        action_type,
        result_id,
        action_result,
        bodypart_id,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        data_source,
        competition_id,
        season_id,
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure,
        possession_id_heuristic,
        gk_role,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        defending_gk_player_id,
        team_id_native,
        home_team_id_native,
        competition_native_id,
        season_native_id,
        match_id_native,
        player_id_native,
        tackle_winner_player_id_native,
        tackle_winner_player_key,
        tackle_winner_team_id_native,
        tackle_winner_team_key,
        tackle_loser_player_id_native,
        tackle_loser_player_key,
        tackle_loser_team_id_native,
        tackle_loser_team_key
    from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }} where match_id is not null)
    {% endif %}
)
```

- [ ] **Step 2: Replace `select *` in `fct_defcon_pressure.sql`**

Find the `defcon` CTE (lines 30-38):
```sql
with defcon as (
    select * from {{ ref('stg_defcon__results') }}
    where action_player_id is not null
    {% if is_incremental() %}
    and match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

Replace with:
```sql
with defcon as (
    select
        match_id,
        competition_id,
        season_id,
        action_player_id,
        credit_type,
        confidence,
        defcon_value,
        data_source
    from {{ ref('stg_defcon__results') }}
    where action_player_id is not null
    {% if is_incremental() %}
    and match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

- [ ] **Step 3: Replace `select *` in `fct_defcon_actions.sql`**

Find the `defcon` CTE (lines 30-37):
```sql
with defcon as (
    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

Replace with (all 18 staging columns are used):
```sql
with defcon as (
    select
        event_id,
        match_id,
        competition_id,
        season_id,
        defender_player_id,
        defender_team_id,
        defender_x,
        defender_y,
        action_player_id,
        action_type,
        action_x,
        action_y,
        credit_type,
        confidence,
        defcon_value,
        dist_to_ball,
        pitch_control_at_action,
        data_source
    from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

- [ ] **Step 4: Replace `select *` in `fct_defensive_values.sql`**

Find the `defcon` CTE (lines 32-38):
```sql
with defcon as (
    select * from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

Replace with:
```sql
with defcon as (
    select
        match_id,
        competition_id,
        season_id,
        defender_player_id,
        defender_team_id,
        credit_type,
        confidence,
        defcon_value,
        data_source
    from {{ ref('stg_defcon__results') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)
```

- [ ] **Step 5: Replace `select *` in `fct_formation_labels.sql`**

Find the `formation_labels` CTE (lines 41-48):
```sql
formation_labels as (
    select * from {{ ref('stg_formations__labels') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
)
```

Replace with (all 10 staging columns are used):
```sql
formation_labels as (
    select
        match_id,
        period,
        team,
        window_start_s,
        window_end_s,
        formation_label,
        cost,
        detector,
        source_provider,
        _ingested_at
    from {{ ref('stg_formations__labels') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
)
```

- [ ] **Step 6: Verify column lists match staging output**

Before compiling, cross-check each explicit column list against the staging model it reads from. Open each staging model and verify every column you listed exists in its output. Also verify that each downstream CTE's column references are satisfied by the explicit list (e.g., `fct_action_values.sql` resolves `match_key` from the separate `match_attrs` CTE via `dim_matches`, NOT from the `action_values` CTE — so `match_key` is correctly absent from the column list):

- `stg_spadl__action_values` → `fct_action_values.sql` column list
- `stg_defcon__results` → `fct_defcon_pressure.sql`, `fct_defcon_actions.sql`, `fct_defensive_values.sql` column lists
- `stg_formations__labels` → `fct_formation_labels.sql` column list

If any column was renamed, added, or removed since the plan was written, update the column list before proceeding.

- [ ] **Step 7: Verify all 5 files compile**

Run: `cd dbt_project && dbt compile --select fct_action_values fct_defcon_pressure fct_defcon_actions fct_defensive_values fct_formation_labels --vars '{pausa_enabled: true}'`

Expected: Compiles without errors.

- [ ] **Step 8: Commit**

```bash
git add dbt_project/models/marts/fct_action_values.sql \
  dbt_project/models/marts/fct_defcon_pressure.sql \
  dbt_project/models/marts/fct_defcon_actions.sql \
  dbt_project/models/marts/fct_defensive_values.sql \
  dbt_project/models/marts/fct_formation_labels.sql
git commit -m "refactor(dbt): replace select * with explicit columns in 5 marts (c)"
```

---

## Task 8: Final Verification + Spec Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-05-19-opt-2-dbt-mart-cleanup-design.md` (status update)

- [ ] **Step 1: Run full dbt compile**

Run: `cd dbt_project && dbt compile --vars '{pausa_enabled: true, goalkeeper_enabled: true, embeddings_enabled: true}'`

Expected: Zero compilation errors.

- [ ] **Step 2: Run Python linting**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/skillcorner_matches.py src/tests/test_skillcorner_e2e.py`

Expected: No violations.

- [ ] **Step 3: Run pyright**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pyright src/ingestion/skillcorner_matches.py`

Expected: No errors.

- [ ] **Step 4: Run SkillCorner unit tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_skillcorner_e2e.py -v`

Expected: All pass, including the new `test_matches_playing_time_fields`.

- [ ] **Step 5: Update spec status**

In `docs/superpowers/specs/2026-05-19-opt-2-dbt-mart-cleanup-design.md`, change:
```
**Status**: Draft (v5 — ...)
```
to:
```
**Status**: Implemented
```

- [ ] **Step 6: Commit spec + plan bundled**

```bash
git add docs/superpowers/specs/2026-05-19-opt-2-dbt-mart-cleanup-design.md \
  docs/superpowers/plans/2026-05-19-opt-2-dbt-mart-cleanup.md
git commit -m "docs: OPT-2 spec + implementation plan"
```

---

## Post-Merge Operator Steps (NOT automated in this plan)

These require Databricks connectivity and are performed manually after the PR merges:

1. **Bronze migration**: auto-applied by `dbt-live-ci.yml` on merge (new file in `scripts/migrations/`).
2. **SkillCorner re-ingestion**: query `SELECT DISTINCT match_id FROM soccer_analytics.bronze.skillcorner_matches`, DELETE those rows, re-trigger `ingest_skillcorner` mega-job task.
3. **dbt build**: `dbt build --select int_minutes_played_per_match+ stg_wyscout__lineups+ --full-refresh --vars '{pausa_enabled: true, goalkeeper_enabled: true}'` (the `+` selects downstream dependents including `fct_goalkeeper_stats`, `fct_player_stats`; `dbt build` runs both models and tests, so singular tests that `ref()` these models — `assert_minutes_played_range`, `assert_minutes_played_roster_count`, `assert_idsse_minutes_roster_vs_tracking_context`, `assert_skillcorner_minutes_parity`, `assert_wyscout_lineups_starter_count` — are automatically included in the graph selection).
4. **Embedding mart rebuild**: `dbt run --select fct_player_embeddings fct_player_embeddings_career fct_player_embeddings_season fct_player_embeddings_career_360 fct_player_embeddings_season_360 --full-refresh --vars '{embeddings_enabled: true}'` (required for liquid clustering to take effect).
5. **Verify clustering**: `DESCRIBE DETAIL soccer_analytics.dev_gold.fct_player_embeddings` — confirm `clusteringColumns` populated.
6. **StatsBomb minutes parity spot-check** (manual): Run in SQL editor:
   ```sql
   -- Compare new intermediate vs old inline GK minutes (should return 0 rows).
   SELECT gk.match_id, gk.player_id, gk.minutes_played AS gk_min, imp.minutes_played AS imp_min
   FROM soccer_analytics.dev_gold.fct_goalkeeper_stats gk
   JOIN soccer_analytics.dev_gold.dim_matches dm ON dm.provider = 'statsbomb' AND dm.native_match_id = CAST(gk.match_id AS STRING)
   JOIN soccer_analytics.dev_gold.dim_players dp ON dp.provider = 'statsbomb' AND dp.native_player_id = CAST(gk.player_id AS STRING)
   JOIN soccer_analytics.dev_gold.int_minutes_played_per_match imp ON imp.match_key = dm.match_key AND imp.player_key = dp.player_key AND imp.data_source = 'statsbomb'
   WHERE gk.data_source = 'statsbomb' AND gk.minutes_played IS NOT NULL AND ABS(gk.minutes_played - imp.minutes_played) > 0.01
   ```
   **Operator note:** After the PR deploys, both sides of this query derive from `int_minutes_played_per_match` — so this validates JOIN plumbing (no NULL minutes where values should exist), NOT that the derivation logic matches the old inline version. The old inline version is deleted by the PR.
