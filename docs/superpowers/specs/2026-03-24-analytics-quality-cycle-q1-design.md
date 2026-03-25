# Analytics Quality Cycle Q1 — Design Spec

**Branch:** `feature/analytics-quality-cycle-q1`
**Date:** 2026-03-24
**Items:** D25, U1, O1, D19, D24

---

## D25: PAUSA Minimum Activity Filter

### Problem

PAUSA rankings include all players regardless of activity level. Low-sample-size entries (1–3 passes per match) inflate rankings. Minho Lee (PAUSA author) confirmed the paper excludes players with fewer than 150 successful passes; he recommends 50+ or 100+ for the 7-match DFL dataset.

### Design Decision: Quality Proxy via `actual_obso`

Pass outcome (success/failure) does not exist for IDSSE matches — the DFL XML format lacks a pass accuracy attribute, and IDSSE was never run through the SPADL pipeline. Rather than a binary success filter, we use `actual_obso > 0` as a quality proxy: passes that generated realized off-ball value. This is arguably more precise than binary completion and requires zero pipeline changes.

### dbt Layer

**New model: `fct_pausa_rankings`**

- Grain: `(player_id)` — one row per player, aggregated across all matches.
- Source: `int_pausa__pass_quality` (same upstream as `fct_pass_timing`).
- CTE structure:
  1. `pass_quality` — reads from `int_pausa__pass_quality` (all individual pass rows)
  2. `physical_minutes` — pre-aggregates `fct_physical_stats` to `(player_id)` grain: `SELECT player_id, SUM(minutes_played) as total_minutes FROM fct_physical_stats GROUP BY player_id`. Note: `fct_physical_stats` only covers tracking-data matches (IDSSE). Since PAUSA is also IDSSE-only, this join is complete for the current dataset. If PAUSA expands to non-tracking sources, this will need revisiting.
  3. `aggregated` — aggregates `pass_quality` to `(player_id)` grain, LEFT JOINs `physical_minutes`.
- Columns:
  - `player_id` (string)
  - `player_display_name` (string)
  - `total_matches` (int) — COUNT(DISTINCT match_id)
  - `total_passes` (int) — COUNT(*)
  - `passes_with_value` (int) — COUNT(*) WHERE actual_obso > 0
  - `avg_pausa` (double) — AVG(pausa_score) across all rows (direct AVG, naturally weighted by pass count)
  - `avg_temporal_judgment` (double)
  - `avg_spatial_selection` (double)
  - `median_pausa` (double) — percentile_approx across all rows
  - `total_minutes` (double) — from `physical_minutes` CTE
  - `_loaded_at` (timestamp)
- Gated: `{% if var('pausa_enabled', false) %}`
- Config: `materialized='table'`, `liquid_clustered_by=['player_id']`
- Contract enforced in `_marts__models.yml`.

**Synced table:** `fct_pausa_rankings_synced` — Lakebase SNAPSHOT, PK `player_id`.

**PG index:** btree on `passes_with_value` (filter column). Note: table will be small (~hundreds of rows for current dataset). Index is future-proofing for when PAUSA expands beyond 7 IDSSE matches.

### Taipy Layer

**Aggregate rankings view (new, primary):**
- Query `fct_pausa_rankings_synced` with `WHERE passes_with_value >= :threshold`.
- Default threshold slider: 50 (Minho's lower recommendation).
- Secondary slider: `min_minutes` (default 0, user-adjustable).
- Tooltip on threshold slider: "Minimum passes that generated off-ball value (actual OBSO > 0). Paper default: 150; adjusted for 7-match dataset."

**Per-match view (existing, enhanced):**
- Add `min_passes` slider to existing `fct_pass_timing` query (default 5).
- Tooltip: "Minimum total passes in this match. Filters low-activity appearances."

---

## U1: Calibration Anchors — Percentile Ranks

### Problem

Raw metric values (xG/90, VAEP/90, PAUSA) are uninterpretable without league context. "0.347 VAEP/90" means nothing without knowing the distribution. CHI-AUDIT-180 flagged this as a gap.

### dbt Layer

**New model: `fct_player_percentiles`**

- Grain: `(player_id, competition_id, season_id)` — same as `fct_player_stats`.
- CTE structure:
  1. `player_stats` — reads from `fct_player_stats` (all per-90 metrics, grain: player × competition × season)
  2. `physical_by_comp` — aggregates `fct_physical_stats` to `(player_id, competition_id, season_id)` grain via JOIN through `fct_match_summary` for `competition_id` linkage: `SELECT ps.player_id, ms.competition_id, ms.season_id, AVG(ps.distance_per_minute_m) as avg_distance_per_minute, AVG(ps.max_speed_ms) as avg_max_speed FROM fct_physical_stats ps JOIN fct_match_summary ms ON ps.match_id = ms.match_id GROUP BY 1, 2, 3`
  3. `pausa_joined` (gated behind `pausa_enabled`) — LEFT JOIN `fct_pausa_rankings` (D25) on `player_id`. Note: `fct_pausa_rankings` grain is `(player_id)` with no competition dimension. The same PAUSA avg appears for a player in every competition — this is correct because PAUSA is IDSSE-only (single competition). The `PERCENT_RANK()` window still produces meaningful competition-relative percentiles if multiple players share the competition.
  4. `enriched` — joins `player_stats` + `physical_by_comp` + `pausa_joined`
  5. `percentiled` — applies `PERCENT_RANK()` windows
- Source: `fct_player_stats` as primary, LEFT JOINs per CTE structure above.
- Percentile columns (via `PERCENT_RANK() OVER (PARTITION BY competition_id, season_id ORDER BY metric)`):
  - `xg_per_90_pctile`
  - `goals_per_90_pctile`
  - `passes_per_90_pctile`
  - `progressive_passes_per_90_pctile`
  - `pass_completion_pct_pctile`
  - `vaep_per_90_pctile`
  - `offensive_vaep_per_90_pctile`
  - `defensive_vaep_per_90_pctile`
  - `line_breaking_per_90_pctile`
  - `defcon_per_90_pctile` (gated behind `defcon_enabled`)
  - `avg_pausa_pctile` (gated behind `pausa_enabled`)
  - `distance_per_minute_pctile` (NULL for non-tracking matches)
  - `max_speed_pctile` (NULL for non-tracking matches)
- Config: `materialized='table'`, `liquid_clustered_by=['competition_id']`
- Contract enforced in `_marts__models.yml`.

**Synced table:** `fct_player_percentiles_synced` — Lakebase SNAPSHOT, PK `(player_id, competition_id, season_id)`.

**PG index:** composite btree on `(competition_id, season_id, player_id)`.

### Taipy Layer Consumption

| Page | Change |
|------|--------|
| **Player Comparison** (radar) | Replace hardcoded axis ranges with percentile values (0–1 scale). Radar becomes "where does this player sit in the competition." |
| **Player Impact** (rankings) | Add percentile column next to VAEP/90 values. Tooltip: "Percentile rank within this competition." |
| **Defensive Impact** (rankings) | Add percentile column next to DEFCON/90. |
| **Pass Timing** (rankings) | Add percentile column next to avg PAUSA (from aggregate view). |
| **Match Summary** | Add league average reference text on key metrics (xG, possession, pass completion). Computed as simple AVG in the percentiles model or a separate lightweight query. |

### Minimum Appearance Filtering

`PERCENT_RANK()` on `fct_player_stats` will rank all players including those with trivial appearances. Rather than filtering in the dbt model (which would remove rows and lose the raw data), filtering is applied at the Taipy query layer: `WHERE minutes_played >= :min_minutes` (default 90, matching "at least one full match"). This parallels D25's approach where the threshold is a user-adjustable slider. The dbt model stores all percentiles; the app filters the noise.

---

## O1: `fct_match_summary` Incremental Materialization

### Problem

`fct_match_summary` is a full table replacement on every dbt run despite being append-only by nature (`match_id` is the stable key). Pattern inconsistency with `fct_shots` and `fct_passes` which are already incremental.

### Change

Add `{{ config() }}` block:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='match_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
```

New `existing_matches` CTE (follows `fct_physical_stats` pattern — CTE for existing IDs, then `NOT IN`):
```sql
{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}
```

All four event-sourced CTEs (`match_team_ids`, `defensive_actions`, `opponent_passes_in_def_zone`, plus the `shots`/`passes` refs) add:
```sql
{% if is_incremental() %}
where match_id not in (select match_id from existing_matches)
{% endif %}
```

### Constraints

- No schema change. Same 27 columns, no `_loaded_at` added.
- Contract untouched.
- 3,464 rows — ROI is pattern consistency, not performance.

### Testing

- Full `dbt build --select fct_match_summary` — verify row count matches current.
- Second run — verify zero new rows processed (idempotent).

---

## D19: Team Shape Spatial Metrics Module

### Problem

No spatial shape analysis exists. Team centroid, convex hull, stretch index, defensive line height, and inter-line gaps are foundational team shape metrics needed for D20 (formation detection) and D21 (Team Shape page).

### Module: `src/analytics/team_shape.py`

Pure NumPy/scipy. No Spark, no I/O, no new dependencies.

**Params dataclass:**
```python
@dataclass(frozen=True)
class TeamShapeParams:
    n_defensive_lines: int = 3       # Ward clustering cluster count
    min_players: int = 3             # Minimum for convex hull
    pitch_length: float = 120.0      # StatsBomb x-axis
    pitch_width: float = 80.0        # StatsBomb y-axis
```

**Result dataclass:**
```python
@dataclass(frozen=True)
class TeamShapeResult:
    centroid_x: float                # Team centroid x
    centroid_y: float                # Team centroid y
    convex_hull_area: float          # scipy ConvexHull area
    team_length: float               # Max spread along attacking axis
    team_width: float                # Max spread along lateral axis
    stretch_index: float             # Mean distance from centroid (Clemente et al.)
    defensive_line_height: float     # Mean position of deepest cluster
    inter_line_gaps: tuple[float, ...] # Distances between cluster centroids
```

**Public API:**

1. `compute_team_shape(players_x, players_y, params=None) -> TeamShapeResult`
   - Single team, single frame.
   - `players_x`, `players_y`: 1-D NumPy arrays.

2. `compute_team_shape_frame(players_df, params=None) -> dict[str, TeamShapeResult]`
   - Convenience wrapper. DataFrame with `x`, `y`, `team` columns.
   - Returns `{"home": TeamShapeResult, "away": TeamShapeResult}`.
   - Only processes rows where `team in ("home", "away")`. Other values (e.g., ball data) are silently ignored.

**Edge cases:**
- < `min_players`: return TeamShapeResult with NaN fields.
- Collinear players: ConvexHull raises `QhullError` — catch, return `convex_hull_area=0.0`.
- Single cluster fallback if Ward can't form `n_defensive_lines` clusters.

**Academic reference:** Clemente, F.M. et al. (2013). "Collective tactical behaviour in football." — stretch index definition.

### Tests: `src/tests/test_team_shape.py`

- `TestTeamShapeParams` — defaults and overrides.
- `TestComputeTeamShape` — synthetic formations (`_make_442`, `_make_352`) with analytically known geometry (e.g., rectangle → known hull area, known centroid).
- `TestComputeTeamShapeFrame` — DataFrame wrapper, both teams.
- `TestEdgeCases` — empty array, 1 player, 2 players, collinear, all same position.

No benchmark in this cycle. Benchmarks relevant when wired into batch pipeline (D21).

---

## D24: Numba Evaluation for Pitch Control

### Problem

The ROADMAP resolved "No Numba — JAX has native GPU, already in codebase." But the question was never definitively benchmarked. D24 produces hard numbers to either close the door or open it.

### Scope

Benchmark-only evaluation. No production code changes unless results are conclusive.

### New Dependency

`numba>=0.60.0` in `[dev]` extras only. Not a production dependency.

### Implementation: `src/analytics/pitch_control_numba.py`

Numba JIT versions of the two hot-path kernels:
- `tti_numba(player_pos, player_vel, targets, params)` — `@numba.njit`, mirrors `_tti_numpy`
- `influence_numba(team_tti, opponent_min_tti, sigma)` — `@numba.njit`, mirrors `_influence_numpy`

Separate file to avoid Numba import in production code paths.

### Benchmark Additions: `src/tests/test_benchmarks.py`

New `TestNumbaBenchmarks` class (consistent with existing `TestJaxBenchmarks`), skip if Numba not installed:
- `test_bench_numba_pitch_control_cold` — first call including JIT compile
- `test_bench_numba_pitch_control_warm` — post-warmup, against 5ms NumPy budget

Separate non-benchmark parity test (outside benchmark class, no `benchmark` fixture — avoids skewing timing reports):
- `test_numba_numpy_parity` — numerical equivalence check (`atol=1e-10`), lives in `TestNumbaBenchmarks` or a dedicated `TestNumbaParity` class

Same 22-player × 22-target fixture as existing benchmarks.

### Evaluation Criteria

| Comparison | Threshold for adoption |
|------------|----------------------|
| Numba warm vs NumPy warm | ≥2x speedup to justify dependency |
| Numba cold vs JAX cold | Faster compile = value for short-lived processes |
| Numba warm vs JAX warm | Competitive or clearly inferior |

### Decision Gate

- If Numba warm ≥2x faster than NumPy: earns a third dispatch tier (`NumPy → Numba → JAX`) in `pitch_control.py`.
- If not: evaluation documented, `pitch_control_numba.py` removed, D24 entry in TODO updated to "Resolved — No Numba."
- Findings written to `docs/decisions/d24-numba-evaluation.md`.

---

## Implementation Order

D25 must complete before U1 (U1 depends on `fct_pausa_rankings`). The other items are independent.

| Order | Item | Dependency |
|-------|------|-----------|
| 1 | D19 | Independent — pure analytics module |
| 2 | D24 | Independent — benchmark evaluation |
| 3 | O1 | Independent — dbt incremental change |
| 4 | D25 | Independent — but must precede U1 |
| 5 | U1 | Depends on D25 (`fct_pausa_rankings`) |

Items 1–4 can be parallelized. Item 5 must wait for D25.

---

## Infrastructure Summary

### New Synced Tables (2)

| Table | PK | Indexes |
|-------|-----|---------|
| `fct_pausa_rankings_synced` | `player_id` | btree on `passes_with_value` |
| `fct_player_percentiles_synced` | `(player_id, competition_id, season_id)` | composite btree on `(competition_id, season_id, player_id)` |

### New dbt Models (2)

| Model | Materialization | Grain |
|-------|----------------|-------|
| `fct_pausa_rankings` | table | player_id |
| `fct_player_percentiles` | table | (player_id, competition_id, season_id) |

### Modified dbt Models (1)

| Model | Change |
|-------|--------|
| `fct_match_summary` | table → incremental (merge on match_id) |

### New Files (3)

| File | Purpose |
|------|---------|
| `src/analytics/team_shape.py` | Team shape spatial metrics |
| `src/tests/test_team_shape.py` | Unit tests for team shape |
| `src/analytics/pitch_control_numba.py` | Numba JIT evaluation (may be removed) |

### Modified Files (estimated)

| File | Change |
|------|--------|
| `dbt_project/models/marts/fct_match_summary.sql` | Incremental config |
| `dbt_project/models/marts/_marts__models.yml` | Contracts for 2 new models |
| `hf_taipy_app/src/state/pass_timing.py` | Aggregate rankings query + sliders |
| `hf_taipy_app/src/pages/pass_timing.py` | Slider widgets in PageConfig |
| `hf_taipy_app/src/state/player_radar.py` | Percentile-based radar scaling |
| `hf_taipy_app/src/state/action_values.py` | Percentile column in rankings |
| `hf_taipy_app/src/state/defensive_valuation.py` | Percentile column in rankings |
| `hf_taipy_app/src/state/match_summary.py` | League average reference |
| `hf_taipy_app/src/template.py` | Glossary terms for new concepts |
| `src/tests/test_benchmarks.py` | Numba benchmark additions |
| `pyproject.toml` | `numba` in `[dev]` extras |
| `scripts/create_indexes.py` | 2 new synced table indexes |

### Dependency Changes

| Package | Extra group | Action |
|---------|------------|--------|
| `numba>=0.60.0` | `[dev]` | Add (evaluation only) |

---

## Out of Scope

- D20 (EFPI formation detection) — external dependency, separate cycle
- D21 (Team Shape page) — depends on D19 + D20
- D22 (NannyML CBPE) — deferred, no current ground-truth gap
- PAUSA+ integration — future, tracked in memory
- Detail drilldown on AI/ML Workflows page — deferred (TODO comment in code)
- New compute pipelines / HF Jobs scripts — none needed this cycle
