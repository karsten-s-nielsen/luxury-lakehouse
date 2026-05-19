# OPT-2: dbt Mart Cleanup — Embedding Clustering, Provider-Agnostic Minutes, Column Pruning

**Date**: 2026-05-19
**Status**: Implemented
**Origin**: Optimization audit Cycle D (`memory/project_optimization_audit_cycles_b_to_f.md`)

---

## 1. Problem Statement

Three independent dbt-layer quality gaps identified in the late-April optimization audit:

1. **Five embedding marts lack liquid clustering** — queries on `canonical_player_id` (player-similarity workload) and `match_key` (per-match retrieval) do full scans instead of file pruning.
2. **`fct_goalkeeper_stats` has a StatsBomb-only minutes derivation** — 100 lines of inline logic reading `stg_statsbomb__events` + `stg_statsbomb__lineups` directly, duplicating `int_minutes_played`. No other provider gets per-90 GK stats. The existing `int_minutes_played` model has the same problem: StatsBomb-only, and aggregated to competition/season grain (useless for per-match consumers).
3. **Five mart CTEs use `select *` on staging refs** — benign today (simple reads with immediate filtering), but explicit column lists catch upstream drift at compile time.

Additionally, the SkillCorner ingestion rewrite (PR #290, 2026-05-17) violated bronze-completeness: `parse_match_json()` drops 12 player-level fields from `match.json` including pre-computed `playing_time.total.minutes_played`. This must be fixed as a prerequisite for the SkillCorner minutes leg.

## 2. Scope

### In scope

- **(a)** Add `liquid_clustered_by` to 5 embedding marts
- **(b)** Create `int_minutes_played_per_match` — provider-agnostic intermediate with 4 provider legs
- **(b.1)** Fix SkillCorner `parse_match_json()` bronze-completeness gap (prerequisite for SkillCorner leg)
- **(b.2)** Create `stg_wyscout__lineups` — extract lineup + substitution data from `teams_data_parsed` (prerequisite for Wyscout leg)
- **(b.3)** Refactor `fct_goalkeeper_stats` to consume `int_minutes_played_per_match`
- **(b.4)** Refactor `int_minutes_played` to aggregate from `int_minutes_played_per_match`
- **(c)** Replace `select *` with explicit column lists in 5 audit-flagged mart CTEs

### Out of scope

- Metrica minutes derivation — anonymized sample data with no substitution events, no lineup metadata, no pre-computed minutes. Tracking-frame-scan for 3 sample games of synthetic player IDs is not worth the complexity. Metrica players get NULL `minutes_played` (status quo).
- `competition_id_mapping` seed removal — the TODO already dropped this from OPT-2 scope (refs are documentation-only, not active queries).
- Embedding mart contract enforcement — already in place (audit finding was wrong, verified at `_marts__models.yml:1447/1537/1616`).
- Remaining ~25 `select *` instances in other marts — see §5 for boundary rationale.

## 3. Sub-item (a): Embedding Mart Liquid Clustering

### Current state

| Mart | Materialization | `liquid_clustered_by` | Contract |
|------|-----------------|-----------------------|----------|
| `fct_player_embeddings` | incremental | Missing | Enforced |
| `fct_player_embeddings_career` | table | Missing | Enforced |
| `fct_player_embeddings_season` | table | Missing | Enforced |
| `fct_player_embeddings_career_360` | table | Missing | Enforced |
| `fct_player_embeddings_season_360` | table | Missing | Enforced |

### Target state

| Mart | Clustering Key | Rationale |
|------|----------------|-----------|
| `fct_player_embeddings` | `['canonical_player_id', 'match_key']` | Per-match retrieval + player-similarity |
| `fct_player_embeddings_career` | `['canonical_player_id']` | One row per player — PK is clustering key |
| `fct_player_embeddings_season` | `['canonical_player_id']` | Player-similarity is dominant query pattern |
| `fct_player_embeddings_career_360` | `['canonical_player_id']` | Same as career |
| `fct_player_embeddings_season_360` | `['canonical_player_id']` | Same as season |

### Verification

Post-deploy: `DESCRIBE DETAIL <table>` confirms clustering columns populated. Predictive Optimization (catalog-level) handles compaction automatically.

## 4. Sub-item (b): Provider-Agnostic `int_minutes_played_per_match`

### 4.1 Provider Capabilities (Verified)

| Provider | Method | Starting XI | Substitutions | Match Duration | Minutes Source |
|----------|--------|-------------|---------------|----------------|----------------|
| **StatsBomb** | Event-based | `stg_statsbomb__lineups` (explicit) | `event_type='Substitution'` in `stg_statsbomb__events` (`substitution_replacement_id` for player coming on) | MAX(minute) from events | Derived from lineup/sub/duration |
| **Wyscout** | Event-based | `stg_wyscout__lineups` (new — extracted from `teams_data_parsed.formation.lineup`) | `stg_wyscout__lineups` substitution columns (extracted from `teams_data_parsed.formation.substitutions`) | Last event minute in `stg_wyscout__events` (fallback: 90) | Derived from lineup/sub/duration |
| **IDSSE** | Event-based | `stg_spadl__tracking_context` (TC-1 shipped — has `player_id_native` per match from silly-kicks `derive_goalkeepers()` roster) | `sub_player_in`/`sub_player_out`/`sub_team` at `timestamp_seconds` in `stg_idsse__events` | `FinalWhistle` event `timestamp_seconds` | Derived from roster + sub events + duration |
| **SkillCorner** | Metadata-direct | N/A — pre-computed | N/A — pre-computed | N/A — pre-computed | `playing_time.total.minutes_played` from `match.json` (per-player) |
| **Metrica** | Excluded | N/A | N/A | N/A | NULL (anonymized sample data) |

### 4.2 Output Schema

```sql
match_key       BIGINT NOT NULL   -- FK to dim_matches (Kimball surrogate)
player_key      BIGINT NOT NULL   -- FK to dim_players (Kimball surrogate)
data_source     STRING NOT NULL   -- 'statsbomb' | 'wyscout' | 'idsse' | 'skillcorner'
minutes_played  DOUBLE NOT NULL   -- minutes on pitch (0.0–130.0 range, extra time included)
```

Grain: one row per (match_key, player_key).

**Surrogate-only output** — no native IDs. IDSSE uses DFL string IDs (e.g. `"DFL-MAT-J03WN9"`) that cannot be cast to BIGINT, so a uniform surrogate-only contract avoids the type mismatch across providers. Downstream consumers JOIN on `(match_key, player_key)`:
- `fct_goalkeeper_stats` (§4.4) is refactored to propagate `player_key` through its CTE chain and JOIN minutes on surrogates
- `int_minutes_played` (§4.4) resolves native `player_id` from `dim_players` via `player_key` for its `fct_player_stats` join contract

The `derivation_method` column from v1 is dropped — it's debug metadata with no query consumer. The derivation method is implicit from `data_source`.

### 4.3 Model Structure

Single `int_minutes_played_per_match.sql` with provider CTEs unioned (follows existing `int_unified_passes`/`int_unified_shots` pattern).

**Materialization**: `view` — the model is ~100K rows (5K StatsBomb matches x ~22 players + Wyscout + 7 IDSSE + 10 SkillCorner). A view avoids the full-refresh-breaks-synced-table class of issues. Promote to `table` only if downstream consumers show measurable performance degradation.

**Tags**: `intermediate_mart` (per ADR-019 classification).

#### StatsBomb leg

Refactors the logic currently duplicated in `fct_goalkeeper_stats:129-226` and `int_minutes_played:13-129`:

1. Starting XI from `stg_statsbomb__lineups` WHERE `position_name IS NOT NULL`
2. Match duration from MAX(`minute`) + 1 in `stg_statsbomb__events`
3. Substitution off: player_id on `event_type='Substitution'` row -> off at `minute`
4. Substitution on: `substitution_replacement_id` -> on at `minute`
5. Starters: minutes = COALESCE(sub_off_minute, match_end_minute)
6. Subs on: minutes = COALESCE(sub_off_minute, match_end_minute) - sub_on_minute
7. MAX() deduplicates (formation changes create multiple lineup entries per player-match)
8. All 4 legs cast native IDs to STRING and UNION into a single `unioned` CTE
9. Single `final` CTE resolves `match_key` via `dim_matches` JOIN on `(provider, native_match_id)` and `player_key` via `dim_players` JOIN on `(provider, native_player_id)`
10. Project surrogates only: `(match_key, player_key, data_source, minutes_played)`

#### Wyscout leg

Consumes the new `stg_wyscout__lineups` model (§4.6), which pre-resolves `minute_on`/`minute_off` per player.

1. Read all players from `stg_wyscout__lineups` (starters + substitutes who entered)
2. Match duration from last event minute in `stg_wyscout__events` (fallback: 90 — applied only when the match has zero events, not as a general default for missing data)
3. Compute `minutes_played = COALESCE(minute_off, match_end_minute) - minute_on`
4. Cast native IDs to STRING for uniform dim resolution in `unioned` CTE

#### IDSSE leg

Uses `stg_idsse__events` for substitution timing and `stg_spadl__tracking_context` for the player roster.

**Starting XI**: `stg_spadl__tracking_context` carries `player_id_native` per match (TC-1 shipped this — silly-kicks `derive_goalkeepers()` 3-tier identification produces a full player roster per match, not just GKs). This is ground truth from tracking data — every player who appears on the pitch has a tracking record, regardless of whether they generated a DFL event. Using event data for starting XI would miss players who play 90 minutes without a recorded event (centre-backs in low-event matches).

**Limitation**: `stg_spadl__tracking_context` is populated by the `ingest_spadl` pipeline, which runs `convert_to_actions()` on events. A player must have generated at least one SPADL action to appear in the tracking context roster. In practice this covers all outfield players and GKs in IDSSE (Bundesliga) — the DFL event feed is dense enough that even the quietest centre-back produces at least one pass or clearance. But this is not unconditionally guaranteed. The `assert_idsse_minutes_roster_vs_tracking_context.sql` test (§6) will catch any match where a player appears in minutes but not in the roster.

1. Player roster per match from `stg_spadl__tracking_context` — DISTINCT `(data_source, native_match_id, player_id_native)` WHERE `data_source = 'idsse'`
2. Substitution events: filter `stg_idsse__events` WHERE `event_type = 'Substitution'` -> `sub_player_out` exits at `timestamp_seconds`, `sub_player_in` enters at `timestamp_seconds`
3. Match duration: `FinalWhistle` event `timestamp_seconds` (per-period; sum periods for total)
4. Cross-reference roster against substitutions: players in roster but NOT in `sub_player_in` = starters. Players in `sub_player_in` = substitutes entering at `sub_timestamp_seconds`.
5. Starters: minutes = COALESCE(sub_off_seconds, match_end_seconds) / 60.0
6. Subs on: minutes = (COALESCE(sub_off_seconds, match_end_seconds) - sub_on_seconds) / 60.0
7. Native IDs already STRING — passed directly to `unioned` CTE

**Caveat**: `timestamp_seconds` is period-local in IDSSE events. Must add period offset (period 1 duration) for period 2 events to get match-absolute seconds. Period 1 duration = the `FinalWhistle` event `timestamp_seconds` in period 1.

#### SkillCorner leg

Requires sub-item (b.1) to land first (bronze-completeness fix).

1. Read `minutes_played` directly from `stg_skillcorner__matches` (after ingestion fix adds the column)
2. Cast native IDs to STRING for uniform dim resolution in `unioned` CTE

No lineup/sub derivation needed — SkillCorner's tracking system pre-computes `playing_time.total.minutes_played` per player.

### 4.4 Downstream Refactors

#### `fct_goalkeeper_stats`

Two-part refactor:

**Part 1 — Propagate `player_key` through the CTE chain:**
- Add `player_key` to `gk_players` CTE (line 39) — already reads `dim_players`, just add the column
- Propagate `player_key` through `gk_actions` → `gk_matches` CTEs
- Update `sweeper_stats` and `final` minutes JOINs from `(player_id, match_id, data_source)` to `(match_key, player_key, data_source)`
- Remove the redundant `dim_players` LEFT JOIN in `final` CTE (lines 498-500) — `player_key` is now available from `gk_matches`

**Part 2 — Replace inline minutes derivation:**

Delete lines 129-226 (the `events`, `lineups`, `match_duration`, `substitution_off`, `substitution_on`, `player_match_minutes`, `minutes` CTEs). Replace with:

```sql
minutes as (

    select
        imp.match_key,
        imp.player_key,
        imp.data_source,
        imp.minutes_played
    from {{ ref('int_minutes_played_per_match') }} imp

)
```

All downstream CTEs now JOIN minutes on surrogates `(match_key, player_key, data_source)` instead of native IDs.

This makes `fct_goalkeeper_stats` multi-provider: any provider with minutes in `int_minutes_played_per_match` automatically gets per-90 sweeper-keeper stats (previously only StatsBomb).

#### `int_minutes_played`

Refactor from 129 lines of StatsBomb-specific logic to an aggregation over `int_minutes_played_per_match`. Since the intermediate now outputs surrogates only, `int_minutes_played` must resolve native `player_id` from `dim_players` via `player_key`:

```sql
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

**Note**: `try_cast(... as bigint)` returns NULL for IDSSE DFL strings (e.g. `"DFL-OBJ-0028GH"`) and safely handles large SkillCorner numeric IDs that could overflow INT (~2.1B). The `WHERE player_id IS NOT NULL` filter drops non-numeric rows — no regression since `int_minutes_played` was previously StatsBomb-only and had no IDSSE rows.

**Grain change**: the current grain is `(player_id, competition_id, season_id)` with a `unique_combination_of_columns` test in `_intermediate__models.yml:279-284`. The new grain adds `data_source` to prevent cross-provider `player_id` collisions — StatsBomb player_id 1234 and Wyscout player_id 1234 are different people. When the model was StatsBomb-only this was impossible, but with multi-provider input it's a correctness requirement.

**YAML contract update**: change the uniqueness test from `(player_id, competition_id, season_id)` to `(player_id, data_source, competition_id, season_id)`.

**`fct_player_stats` impact — latent bug fix**: `fct_player_stats:141` joins `int_minutes_played` on `(player_id, competition_id, season_id)` WITHOUT `data_source`. When the model was StatsBomb-only this was safe (no cross-provider `player_id` collisions possible). But this is a **pre-existing latent bug**: if any two providers share a numeric `player_id` for different people, the join produces wrong results. This was invisible because only one provider existed.

Adding `data_source` to `int_minutes_played`'s grain (required for correctness — §4.4 above) surfaces this latent bug and forces the fix. The fix is adding `data_source` to the JOIN:

```sql
-- Before (latent bug — cross-provider collision possible):
left join {{ ref('int_minutes_played') }} mp
    on av.player_id = mp.player_id
    and av.competition_id = mp.competition_id
    and av.season_id = mp.season_id

-- After (explicit provider scoping — latent bug fixed):
left join {{ ref('int_minutes_played') }} mp
    on av.player_id = mp.player_id
    and av.data_source = mp.data_source
    and av.competition_id = mp.competition_id
    and av.season_id = mp.season_id
```

This is not a side-effect of the refactor — it is a correctness fix that the refactor makes necessary to address.

### 4.5 SkillCorner Bronze-Completeness Fix (b.1)

**Bug**: `src/ingestion/skillcorner_matches.py:parse_match_json()` (lines 73-98) manually cherry-picks 17 fields from `match.json` players array, dropping 12 available fields:

| Dropped field | Type | Value for minutes |
|---------------|------|-------------------|
| `start_time` | string ("HH:MM:SS") | Player entry time |
| `end_time` | string ("HH:MM:SS") | Player exit time |
| `playing_time` | nested object | **`total.minutes_played`** (pre-computed, float) |
| `yellow_card` | int | Card count |
| `red_card` | int | Card count |
| `injured` | bool | Injury flag |
| `goal` | int | Goals scored |
| `own_goal` | int | Own goals |
| `trackable_object` | int | Tracking system object ID |
| `birthday` | string (ISO date) | Player DOB |
| `gender` | string | Player gender |
| `team_player_id` | int | Team-specific player ID |

**Fix**:

1. Add all 12 fields to `parse_match_json()` row builder. Flatten `playing_time` to:
   - `minutes_played` (float, from `playing_time.total.minutes_played`)
   - `start_frame` (int, from `playing_time.total.start_frame`)
   - `end_frame` (int, from `playing_time.total.end_frame`)
   - `minutes_tip` (float, time in possession)
   - `minutes_otip` (float, time out of possession)
2. Add dtype overrides for new numeric columns.
3. Add bronze migration `scripts/migrations/2026-05-19-skillcorner-matches-add-playing-time-cols.sql`:
   ```sql
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
   (`ALTER TABLE ADD COLUMNS` is idempotent on Databricks — existing rows get NULL for new columns.)
4. Add staging passthrough columns to `stg_skillcorner__matches.sql`.
5. Force re-ingestion of existing A-League matches. First, query the concrete match IDs:
   ```sql
   SELECT DISTINCT match_id FROM soccer_analytics.bronze.skillcorner_matches;
   ```
   Then delete and re-ingest:
   ```sql
   DELETE FROM soccer_analytics.bronze.skillcorner_matches
   WHERE match_id IN (<list from query above>);
   ```
   Then re-trigger the `ingest_skillcorner` mega-job task. The `_SkillCornerGuard.check()` counts distinct match_ids in `skillcorner_tracking`, `skillcorner_events`, and `spadl_actions` — it does NOT check `skillcorner_matches`. So deleting matches rows does NOT trip the guard. Re-ingestion will re-parse `match.json` from the pining-for-the-data API cache with the updated parser, writing the enriched rows via `replaceWhere` on `match_id`.

   Tracking and events data do NOT need re-ingestion — they were ingested with full bronze-completeness (events: all 294 columns; tracking: all fields). Only the matches parser dropped fields.

### 4.6 Wyscout Lineup Extraction (b.2)

**New model**: `stg_wyscout__lineups` — a staging view that extracts lineup and substitution data from `stg_wyscout__matches.teams_data_parsed`.

**Rationale**: `teams_data_parsed` contains general-purpose roster information (lineup, bench, substitutions). Extracting to a dedicated staging model follows the established pattern where StatsBomb has `stg_statsbomb__lineups`. If any future consumer needs "did this player start?" they read from this model rather than re-implementing the MAP extraction.

The `teams_data_parsed` MAP structure is:
```
MAP<STRING, STRUCT<
  side: STRING,
  score: INT,
  formation: STRUCT<
    lineup: ARRAY<STRUCT<playerId: INT, ...>>,
    bench: ARRAY<STRUCT<playerId: INT, ...>>,
    substitutions: ARRAY<STRUCT<playerIn: INT, playerOut: INT, minute: INT, ...>>
  >
>>
```

**Output schema for `stg_wyscout__lineups`**:
```sql
match_id        INT     NOT NULL   -- Wyscout native match ID
team_id         STRING  NOT NULL   -- MAP key (Wyscout team ID)
player_id       INT     NOT NULL   -- playerId from lineup or bench array
is_starter      BOOLEAN NOT NULL   -- true for lineup, false for bench
minute_on       INT     NOT NULL   -- 0 for starters, sub minute for substitutes entering
minute_off      INT                -- sub minute for players leaving (NULL if played to end)
```

Grain: one row per (match_id, team_id, player_id). Each row represents a single player's participation in the match with pre-resolved on/off times. The substitution JOIN is done inside this staging model: starters get `minute_on = 0`; a sub entering gets `minute_on = substitutions[].minute`; a starter leaving gets `minute_off = substitutions[].minute`. This keeps the downstream `int_minutes_played_per_match` Wyscout leg simple: `minutes_played = COALESCE(minute_off, match_end_minute) - minute_on`.

`int_minutes_played_per_match` Wyscout leg reads from this model rather than doing inline MAP extraction.

## 5. Sub-item (c): `select *` Column Pruning

Replace `select *` with explicit column lists in 5 audit-flagged mart CTEs that read from staging/intermediate refs:

| File | Line | CTE | Current Pattern | Fix |
|------|------|-----|-----------------|-----|
| `fct_action_values.sql` | 37 | `action_values` | `select * from stg_spadl__action_values` | Explicit column list matching downstream usage |
| `fct_defcon_pressure.sql` | 32 | `defcon` | `select * from stg_defcon__results` | Explicit column list |
| `fct_defcon_actions.sql` | 32 | `defcon` | `select * from stg_defcon__results` | Explicit column list |
| `fct_defensive_values.sql` | 34 | `defcon` | `select * from stg_defcon__results` | Explicit column list |
| `fct_formation_labels.sql` | 43 | `formation_labels` | `select * from stg_formations__labels` | Explicit column list |

**Scope boundary**: These are the 5 instances flagged by the original optimization audit as `select *` on staging refs that feed into incremental or contract-enforced marts. ~25 additional `select *` instances exist in other marts (e.g., `fct_player_stats`, `fct_passes`, `fct_shots`, dimension models). These are NOT in scope because: (a) many follow the benign `select * from final` last-CTE convention, (b) expanding to all ~30 instances changes this from a targeted audit fix to a codebase-wide refactor — a different work item. Future cycles can extend the pattern if needed.

`fct_workflow_costs.sql:104` is dropped from this list — it's a `SELECT * FROM (subquery with ROW_NUMBER()) WHERE rn = 1` dedup wrapper around its own inner query, not a staging ref read. The inner SELECT is in the same file, so column drift is impossible.

## 6. Testing Strategy

### New tests

- **`int_minutes_played_per_match` YAML contract**: `unique` + `not_null` on composite `(match_key, player_key)`, `not_null` on all columns.
- **`assert_minutes_played_range.sql`** (singular): `minutes_played BETWEEN 0 AND 130` (no negative, no > extra-time max).
- **StatsBomb parity spot-check** (manual, post-deploy): In single-PR workflow the parity test has a zero-width window (created and deleted in the same commit). Replaced with a manual SQL spot-check in post-merge operator steps comparing `fct_goalkeeper_stats.minutes_played` against `int_minutes_played_per_match` for StatsBomb GKs.
- **`assert_minutes_played_roster_count.sql`** (singular): per match, count of players with `minutes_played > 0` must be BETWEEN 22 AND 32 (22 starters + up to 5 subs per team = 32 max under modern rules; lower bound is 22 starters with zero subs). Hard failure, not a warning. Covers all providers.
- **`assert_idsse_minutes_roster_vs_tracking_context.sql`** (singular): for IDSSE matches, the set of `player_id_native` values in `int_minutes_played_per_match` must be a subset of the distinct `player_id_native` values in `stg_spadl__tracking_context` for the same match. Catches any drift between the roster source and the minutes derivation.
- **`assert_skillcorner_minutes_parity.sql`** (singular): for SkillCorner matches, verify that `SUM(minutes_played)` per match from `int_minutes_played_per_match` matches `SUM(minutes_played)` from `stg_skillcorner__matches` (the source metadata). Delta must be zero — SkillCorner minutes are a direct passthrough, not a derivation, so any discrepancy indicates a parsing or join bug.
- **`stg_wyscout__lineups` YAML contract**: `unique` + `not_null` on `(match_id, team_id, player_id)`, `not_null` on `is_starter` + `minute_on`.
- **`assert_wyscout_lineups_starter_count.sql`** (singular): per match, count of players WHERE `is_starter = true` must be 22 (11 per team). Hard failure.
- **`int_minutes_played` YAML contract update**: change uniqueness test from `(player_id, competition_id, season_id)` to `(player_id, data_source, competition_id, season_id)`.

### Existing tests (must still pass)

- `fct_goalkeeper_stats` contract tests in `_marts__models.yml`
- `fct_player_stats` downstream (consumes `int_minutes_played`)
- Embedding mart contracts (unchanged, just adding clustering)

### Integration verification

- Post-deploy: `DESCRIBE DETAIL` on 5 embedding marts confirms clustering columns
- Post-deploy: spot-check `int_minutes_played_per_match` row counts per provider match expectations (StatsBomb ~5K matches x ~22 players, Wyscout similar, IDSSE 7 matches x ~22, SkillCorner 10 matches x ~22)
- Post-deploy: `fct_goalkeeper_stats` non-NULL `minutes_played` count increases (currently only StatsBomb rows have non-NULL)

## 7. Migration / Sequencing

The sub-items have dependency chains:

```
(b.1) SkillCorner ingestion fix (Python + migration)
(b.2) stg_wyscout__lineups (new staging view)
  |
  v
(b) int_minutes_played_per_match (all 4 legs)
  |
  +-> (b.3) fct_goalkeeper_stats refactor (delete inline minutes, JOIN intermediate)
  +-> (b.4) int_minutes_played refactor + fct_player_stats JOIN update

(a) Embedding clustering — independent
(c) Column pruning — independent
```

(a) and (c) can ship independently or bundled with (b). All fit in a single PR.

## 8. Stale Findings (Dropped)

Per the TODO entry, these were verified wrong or out of scope:

- "Embedding marts no `contract: enforced: true`" — **wrong**; all 5 enforced (verified `_marts__models.yml:1447/1537/1616` + 360 variants).
- "`competition_id_mapping` seed unused" — **wrong**; documented as retained for cross-provider competition equivalence in `stg_wyscout__matches:20-25`. No active `ref()` consumers, but removal is not in this cycle's scope.
