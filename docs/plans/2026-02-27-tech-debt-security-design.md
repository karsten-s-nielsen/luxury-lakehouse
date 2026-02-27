# Tech Debt Resolution + Terraform Security — Design Document

**Date**: 2026-02-27
**Scope**: 9 dbt tech debt items + 2 Terraform security items
**Approach**: Incremental (Option A) — dependency-ordered, commit after each logical group

---

## Execution Order

### Step 1 — Extract Hardcoded Thresholds to dbt Vars

Add to `dbt_project.yml` vars:

| Var | Value | Used By |
|-----|-------|---------|
| `sprint_speed_threshold` | `7.0` | `fct_player_embeddings` |
| `progressive_pass_ratio` | `0.75` | `int_unified_passes` |
| `pass_direction_threshold` | `5` | `fct_passes` |
| `frame_duration_seconds` | `0.04` | `fct_tracking_frames` |
| `goal_width` | `8` | `macros/shot_angle.sql` |
| `minutes_per_match` | `90` | `fct_player_stats` |

Pitch third boundaries (`40`, `80`) derive from `pitch_length` var: compute as `{{ var('pitch_length') / 3 }}` and `{{ 2 * var('pitch_length') / 3 }}`.

**Files touched**: `dbt_project.yml`, `fct_player_embeddings.sql`, `int_unified_passes.sql`, `fct_passes.sql`, `fct_tracking_frames.sql`, `macros/shot_angle.sql`, `fct_player_stats.sql`

### Step 2 — Expand `stg_statsbomb__events` + Refactor Dual-Source Models

Expose missing columns in `stg_statsbomb__events.sql`: `shot_end_location`, `shot_freeze_frame`, `pass_end_location`, `substitution_replacement_id`, `duration`, `index`.

Refactor these models to use only `ref('stg_statsbomb__events')`:
- `stg_statsbomb__shots.sql`
- `int_unified_passes.sql`
- `int_minutes_played.sql`

**Files touched**: `stg_statsbomb__events.sql`, `stg_statsbomb__shots.sql`, `int_unified_passes.sql`, `int_minutes_played.sql`, `_statsbomb__models.yml`

### Step 3 — DRY `from_json()` Calls

- `stg_wyscout__events.sql`: Parse `positions` once in CTE, `get()` for start/end. Same for `tags`.
- `stg_statsbomb__lineups.sql`: Parse `positions` and `cards` once each in sub-CTEs.

**Files touched**: `stg_wyscout__events.sql`, `stg_statsbomb__lineups.sql`

### Step 4 — Enable `use_materialization_v2`

Add `use_materialization_v2: true` to `dbt_project.yml`.

**Files touched**: `dbt_project.yml`

### Step 5 — Nest Test Arguments

Migrate ~60 tests across 7 YAML files from deprecated flat syntax to nested `arguments` property (dbt 1.11+).

**Files touched**: All `_*__models.yml` and `_*__sources.yml` files

### Step 6 — Add Missing `accepted_values` Tests

| Column | Values |
|--------|--------|
| `shot_body_part` | Right Foot, Left Foot, Head, No Touch |
| `shot_technique` | Normal, Volley, Half Volley, Lob, Overhead Kick, Diving Header |
| `play_pattern` | Regular Play, From Corner, From Free Kick, From Goal Kick, From Keeper, From Kick Off, From Throw In, Other |
| `pass_outcome` | Complete, Incomplete, Out, Pass Offside, Injury Clearance, Unknown |
| `event_type` (wyscout) | Duel, Foul, Free Kick, Goalkeeper leaving line, Interruption, Offside, Others on the ball, Pass, Shot, Save attempt |
| `sub_event_type` (wyscout) | constrained set from data |
| `dim_players.data_source` | statsbomb |

**Files touched**: `_statsbomb__models.yml`, `_wyscout__models.yml`, `_intermediate__models.yml`, `_marts__models.yml`

### Step 7 — Add Missing Range Tests

| Column | Min | Max |
|--------|-----|-----|
| `distance_to_goal` | 0 | 170 |
| `shot_angle` | 0 | 3.15 |
| `home_xg` / `away_xg` | 0 | 20 |
| `home_score` / `away_score` | 0 | 30 |
| `avg_speed` / `max_speed` | 0 | 15 |
| `sprint_count` | 0 | 5000 |
| `goals_per_90` / `xg_per_90` | 0 | 5 |
| `stg_metrica__events.period` | 1 | 5 |

**Files touched**: `_statsbomb__models.yml`, `_marts__models.yml`, `_metrica__models.yml`

### Step 8 — Document Undocumented YAML Columns

Add descriptions to ~100 columns across all YAML files. Covers staging, intermediate, and marts layers.

**Files touched**: All `_*__models.yml` files

### Step 9 — Integrate `position_mapping.csv` into `dim_players`

Join `{{ ref('position_mapping') }}` in `dim_players.sql` on `position_name`. Add `position_group` column (Goalkeeper, Defender, Midfielder, Forward). Add YAML description and `accepted_values` test.

**Files touched**: `dim_players.sql`, `_marts__models.yml`, seed YAML if needed

### Step 10 — Terraform Security

- **S1**: Restrict Lakebase connections to Streamlit app service principal
- **S10**: Add `resources` block to `databricks_app` granting Lakebase + SQL warehouse permissions

**Files touched**: `terraform/modules/lakebase/main.tf`, `terraform/modules/app/main.tf`

---

## Deferred to Phase 5

| ID | Item |
|----|------|
| S2 | Connection pooling with 55min recycle (`psycopg2.pool`) |
| S3 | Databricks App OAuth M2M auth in code |
| S4 | Parameterized queries only |
| S5 | Input validation against dimension tables |
| S6 | Session security (no credentials in `st.session_state`) |
| S7 | KMS encryption on Terraform state (prod) |
| S8 | Pin GitHub Actions to SHA digests (prod) |
| S9 | Service principal migration + PAT rotation (prod) |
