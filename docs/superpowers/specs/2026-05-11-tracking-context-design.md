# TC-1 — Unified Action-Coupled Tracking Features via silly-kicks

| Field | Value |
|-------|-------|
| **Status** | Draft |
| **Author** | Karsten Skyt Nielsen |
| **Date** | 2026-05-11 |
| **TODO row** | TC-1 |
| **silly-kicks version** | `>=3.11.2,<4` (with `[kloppy,das]` extras) |
| **Providers** | IDSSE (Sportec), Metrica, SkillCorner |
| **Excluded** | Gradient Sports (license pending), StatsBomb (no tracking converter) |

## 1. Goal

Compute all silly-kicks action-coupled tracking features in a single `applyInPandas` pass per match and materialize as one wide, provider-agnostic table. The table is the integration surface for every tracking feature silly-kicks has shipped to date (15 `add_*` functions + `add_pre_shot_gk_context`). Initially additive --- existing tables and consumers are untouched.

## 2. Data Flow

```
bronze.idsse_tracking -------+
bronze.metrica_tracking -----+--> silly-kicks converter --> TRACKING_FRAMES (105x68 LTR)
bronze.skillcorner_tracking -+    (per provider)                  |
                                                                  |
bronze.spadl_actions ------> filter by match_id ----------------> |
                                                                  v
                                                   link_actions_to_frames()
                                                   + add_pre_shot_gk_context()
                                                   + all 15 add_* enrichments
                                                   + xT fitted on driver
                                                                  |
                                                                  v
                                                   bronze.spadl_tracking_context
                                                   (replaceWhere match_id)
                                                                  |
                                                                  v
                                                   stg_spadl__tracking_context (view)
                                                                  |
                                                                  v
                                                   fct_tracking_context (gold mart)
                                                   Kimball FKs, contract enforced,
                                                   liquid clustered by match_key
                                                                  |
                                                   +--------------+--------------+
                                                   |                             |
                                                   v                             v
                                            Lakebase synced              HF dataset
                                            table + indexes       spadl-tracking-context
```

## 3. Provider Converters

All converters produce the same `TRACKING_FRAMES_COLUMNS` schema. The output is provider-agnostic; a `data_source` column discriminates the underlying provider.

| Provider | Converter | `home_team_id` source | `home_team_start_left` source |
|----------|-----------|----------------------|-------------------------------|
| IDSSE | `silly_kicks.tracking.sportec.convert_to_frames` | `bronze.idsse_events` home team field | `derive_idsse_home_team_start_left` (ADR-022) |
| Metrica | `silly_kicks.tracking.kloppy.convert_to_frames` | `dataset.metadata.teams[0]` (kloppy internal) | kloppy internal |
| SkillCorner | `silly_kicks.tracking.kloppy.convert_to_frames` | `dataset.metadata.teams[0]` (kloppy internal) | kloppy internal |

All converters are called with `output_convention="ltr"` and `preprocess=PreprocessConfig(derive_velocity=True)` so that:
- Frames are in SPADL 105x68 left-to-right convention (play-direction normalized).
- Velocity columns (`vx`, `vy`) are derived, enabling Spearman and Fernandez-Bornn pitch control methods and `bekkers_pi` pressure.

### 3.1 Kloppy dataset construction

The kloppy converter takes a `kloppy.domain.TrackingDataset` object, not raw DataFrames. For Metrica and SkillCorner, the ingestion module must construct the kloppy dataset from the bronze table rows before calling the converter.

The preferred path is to re-load from the original data source via kloppy rather than reconstructing the internal kloppy domain model from bronze narrow-form rows:

- **Metrica:** `kloppy.metrica.load_tracking_csv(...)`. Re-fetch from the Metrica open data URLs (the same ones `src/ingestion/metrica_tracking.py` uses).
- **SkillCorner:** `kloppy.skillcorner.load_tracking(...)`. Re-load from the SkillCorner open data source via kloppy.

This avoids reverse-engineering kloppy's internal `TrackingDataset` from bronze columns. The re-fetch is acceptable because these are small open datasets (3 + 10 matches).

### 3.2 Sportec (IDSSE) raw frame construction

`sportec.convert_to_frames` takes a raw DataFrame matching `SPORTEC_TRACKING_FRAMES_COLUMNS` (or `EXPECTED_INPUT_COLUMNS`). The existing `bronze.idsse_tracking` stores narrow-form rows that must be shaped to match the expected input schema. The adapter reads bronze rows for a given match and constructs the input DataFrame.

## 4. Compute Model

### 4.1 Spark execution

Single Databricks workflow task. `applyInPandas` grouped by `(data_source, match_id)`. Each UDF call processes one match:

1. Receives the match's bronze tracking rows + SPADL actions (pre-joined on the Spark side).
2. Dispatches to the appropriate silly-kicks converter based on `data_source`.
3. Chains all enrichments (see §4.3).
4. Returns the enriched actions DataFrame matching the bronze schema.

### 4.2 Driver-side setup

Before the `applyInPandas` call:

- **xT model:** `xt = ExpectedThreat().fit(all_spadl_actions)` --- fitted once on the driver using all SPADL actions from `bronze.spadl_actions`. The fitted model is a 12x16 numpy grid (~1.5 KB). `ExpectedThreat` is a regular Python class (not a dataclass); it is captured as a read-only closure reference in the UDF. Since `applyInPandas` on Databricks serverless does not pickle closures (same-process execution), no serialization issue arises. The grid is deterministic for the same action set (Markov chain fixed-point iteration with `eps=1e-5`). Re-fitted each pipeline run to incorporate newly-ingested actions. No serialized artifact; no external table dependency.
- **`home_team_id` lookup:** Pre-built `dict[str, str]` mapping `match_id -> home_team_id` for IDSSE matches (from `bronze.idsse_events`). Metrica and SkillCorner resolve `home_team_id` inside the UDF from the kloppy dataset metadata.
- **`home_team_start_left` lookup:** Pre-built `dict[str, bool]` mapping `match_id -> home_team_start_left` for IDSSE matches (from `derive_idsse_home_team_start_left`).

### 4.3 Enrichment chain (inside UDF)

Order matters: later steps depend on columns added by earlier steps.

**Column rename:** silly-kicks SPADL uses `game_id`; this table uses `match_id` (native string). The UDF renames `match_id -> game_id` on input (so silly-kicks functions that group by `game_id` work), then renames back to `match_id` on output. The `game_id` value inside the UDF is the native match ID string, not the lakehouse BIGINT `game_id` from `bronze.spadl_actions`.

**Linkage cost note:** 6 of the 15 `add_*` functions (`add_actor_pre_window`, `add_pressure_on_actor`, `add_defensive_line`, `add_line_break`, `add_team_shape`, `add_cover_shadows`) call `link_actions_to_frames` internally. For a match with ~3K actions and ~50K frames, each `link_actions_to_frames` call takes <100ms (O(n log n) sorted-merge). At 6 redundant calls, total overhead is <600ms per match --- acceptable for a batch pipeline processing 20 matches. If profiling reveals this as a bottleneck, silly-kicks may add internal linkage caching in a future version.

**Provenance column safety:** silly-kicks `>=3.11.2` adds skip guards to all `add_*` functions that merge linkage-provenance columns (`frame_id`, `time_offset_seconds`, `link_quality_score`, `n_candidate_frames`). If those columns already exist on the input DataFrame, they are preserved rather than re-merged. This eliminates the `_x`/`_y` suffix collision cascade that would otherwise occur when chaining multiple enrichments. Prior versions (3.11.1 and earlier) lacked this guard on `add_action_context`, `add_actor_pre_window`, `add_pressure_on_actor`, and `add_pre_shot_gk_position`.

**Pitch control optimization:** Instead of calling `add_pitch_control` three times (each copies the full DataFrame), use the lower-level `pitch_control_at_action` function directly:

```python
for method in ("spearman", "fernandez_bornn", "voronoi"):
    s = pitch_control_at_action(actions, frames, method=method)
    actions[s.name] = s.values
```

This avoids 3× DataFrame copies and 3× iteration passes while producing identical column values.

| Step | Function | New columns | Dependencies |
|------|----------|-------------|--------------|
| 0 | `links, _report = link_actions_to_frames(actions, frames)` | `links` DataFrame (kept aside for step 15) | - |
| 1 | `add_pre_shot_gk_context(actions, frames=frames)` | `defending_gk_player_id`, `gk_was_distributing`, `gk_was_engaged`, `gk_actions_in_possession`, `pre_shot_gk_x`, `pre_shot_gk_y`, `pre_shot_gk_distance_to_goal`, `pre_shot_gk_distance_to_shot`, `pre_shot_gk_angle_to_shot_trajectory`, `pre_shot_gk_angle_off_goal_line`, `frame_id`, `time_offset_seconds`, `link_quality_score`, `n_candidate_frames` | - |
| 2 | `add_action_context(actions, frames)` | `nearest_defender_distance`, `actor_speed`, `receiver_zone_density`, `defenders_in_triangle_to_goal` | provenance cols already present from step 1 (skip guard) |
| 3 | `add_actor_pre_window(actions, frames)` | `actor_arc_length_pre_window`, `actor_displacement_pre_window` | provenance skip guard |
| 4 | `add_pressure_on_actor(actions, frames, methods=("andrienko_oval", "link_zones", "bekkers_pi"))` | `pressure_on_actor__andrienko_oval`, `pressure_on_actor__link_zones`, `pressure_on_actor__bekkers_pi` | vx/vy (from preprocess) for `bekkers_pi`; provenance skip guard |
| 5--7 | `pitch_control_at_action(actions, frames, method=<m>)` for spearman, fernandez_bornn, voronoi | `pitch_control_at_ball__spearman`, `pitch_control_at_ball__fernandez_bornn`, `pitch_control_at_ball__voronoi` | vx/vy for spearman + fernandez_bornn |
| 8 | `add_defensive_line(home_team_id=...)` | `defensive_line_x`, `back_line_high_x`, `compactness_x`, `lateral_width`, `max_lateral_gap`, `back_n_count` | home_team_id |
| 9 | `add_off_ball_context(home_team_id=...)` | `line_break`, `n_attackers_behind_line`, `n_off_ball_runners_pre_window`, `max_off_ball_run_displacement_pre_window`, `mean_off_ball_run_speed_pre_window`, `n_off_ball_runners_toward_goal_pre_window` | home_team_id |
| 10 | `add_line_break(method="ward", home_team_id=...)` | `line_break__ward`, `lines_broken__ward`, `line_breaking_type__ward` | home_team_id |
| 11 | `add_team_shape(home_team_id=...)` | 14 cols: `team_shape_{metric}_{attacking|defending}` for centroid_x/y, convex_hull_area, team_length, team_width, stretch_index, n_outfield_players | home_team_id |
| 12 | `add_das()` wrapped in UDF-level try/except (see §4.5) | `das_team`, `das_opponent`, `das_diff` | `accessible-space` package |
| 13 | `add_gk_influence(actions, frames, xt, home_team_id=...)` | `gk_pitch_control_share_weighted`, `gk_reachable_area_m2`, `gk_closing_time_mean_s__six_yard_box`, `gk_closing_time_min_s__six_yard_box` | xT model, home_team_id |
| 14 | `add_cover_shadows(actions, frames, xt, home_team_id=...)` | `n_blocked_receivers`, `n_potential_receivers`, `blocking_score`, `blocked_threat_fraction`, `max_single_defender_blocking_score` | xT model, home_team_id |
| 15 | `add_sync_score(actions, links)` | `sync_score_min`, `sync_score_mean`, `sync_score_high_quality_frac` | `links` from step 0 |

**Steps 13+14 from prior spec revision (explicit `add_pre_shot_gk_position` + `add_pre_shot_gk_angle`) are removed.** Step 1 (`add_pre_shot_gk_context` with `frames=frames`) internally delegates to both functions (lines 220--221 of `spadl/utils.py`). All 6 GK spatial columns + 4 linkage-provenance columns are populated by step 1. The GK spatial columns are shot-only (NaN for non-shot rows) by design.

**Dual line-breaking intent (steps 9 + 10):** `add_off_ball_context` (step 9) includes a boolean `line_break` column from a simple threshold heuristic (n attackers behind defensive line > 0). `add_line_break(method="ward")` (step 10) uses 1D Ward hierarchical clustering for richer output: `lines_broken__ward` (0--3 int), `line_breaking_type__ward` (categorical: `"between_lines"` / `"around_line"` / `None`). Both are retained because they serve different analytical questions --- the threshold boolean for binary classification, the Ward output for detailed pass-type taxonomy.

### 4.4 GK resolution strategy

`add_pre_shot_gk_context` (from `silly_kicks.spadl.utils`) resolves `defending_gk_player_id` from the SPADL event stream (lookback over recent `keeper_*` actions), with optional tracking-frame GK position when `frames` is supplied. This is more robust than the tracking-only `defending_gk_from_frames` because:

1. It works even when tracking data has gaps (missing frames near shots).
2. It adds 3 event-based context columns (`gk_was_distributing`, `gk_was_engaged`, `gk_actions_in_possession`) that pure tracking cannot provide.
3. When `frames` is supplied, it internally delegates to both `add_pre_shot_gk_position` and `add_pre_shot_gk_angle` for all 6 GK spatial columns.

**Note on `add_pre_shot_gk_angle` signature:** The `frames` parameter is keyword-only (`add_pre_shot_gk_angle(actions, *, frames=frames)`), unlike `add_pre_shot_gk_position` where it is positional. This asymmetry is upstream in silly-kicks; irrelevant for TC-1 since `add_pre_shot_gk_context` handles both calls internally.

### 4.5 DAS defensive wrapper

The `accessible-space` package (`>=2.0,<3`) is a required dependency via `silly-kicks[das]`. However, `add_das` has an internal try/except that catches `(ValueError, RuntimeError, ImportError)` but **not** `IndexError`. Per silly-kicks operational history (PR-S32), `accessible-space` can crash with `IndexError` on certain data shapes. The UDF wraps `add_das` in a broader try/except:

```python
try:
    actions = add_das(actions, frames)
except Exception:
    actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

The 3 DAS columns are nullable `float64` in the bronze schema. If `accessible-space` is unavailable in the Databricks serverless environment or crashes, the columns will be all-NaN. The dependency must be bundled in the wheel's `[spadl]` extra to ensure availability.

### 4.6 Wall-clock time estimate

The enrichment chain contains several expensive per-action iteration loops:

| Feature group | Per-action cost | Est. per match (3K actions) |
|---------------|----------------|----------------------------|
| Pitch control × 3 methods | Per-action frame-level PC compute | ~30--60s |
| GK influence | Per-action PC + zone closing time | ~10--20s |
| Cover shadows | Per-action lane control + blocking score | ~20--40s |
| DAS | Pre-compute on all frames, then per-action | ~10--20s |
| Other 8 enrichments | Lightweight vectorized or O(n) | ~5--10s |

**Conservative estimate: 2--5 minutes per match.** For 20 matches with `applyInPandas` parallelism (Databricks auto-scales executors), total wall-clock is **15--40 minutes** depending on cluster sizing. Within the 2-hour pipeline task timeout.

### 4.7 Memory budget

Single-match tracking data: largest IDSSE match ~680K tracking rows x 20 cols = ~110 MB. SPADL actions per match: ~3K rows. Enriched output: ~3K rows x 83 cols = ~2 MB. Total per-UDF peak well under 800 MB budget (1 GB limit minus overhead).

## 5. Bronze Schema

**Table:** `bronze.spadl_tracking_context`

**Grain:** one row per SPADL action (per match).

**Write pattern:** `replaceWhere` on `match_id` for idempotent per-match overwrites.

**StructType constant:** `_TRACKING_CONTEXT_SCHEMA` defined as a module-level `StructType` in `src/ingestion/tracking_context.py`. Parity with the `CREATE TABLE` DDL enforced by `test_tracking_context_schema_parity.py` (same pattern as `_SPADL_SCHEMA` / `_VAEP_SCHEMA`).

### 5.1 Identity columns (12)

| Column | Type | Description |
|--------|------|-------------|
| `data_source` | string | Provider: `idsse`, `metrica`, `skillcorner` |
| `match_id` | string | Native match ID (provider-specific). Renamed from silly-kicks `game_id` at output (see §4.3 column rename note). |
| `action_id` | int64 | SPADL action index within match |
| `period_id` | int64 | Match period |
| `time_seconds` | float64 | Action timestamp (seconds from period start) |
| `team_id` | string | Acting team native ID |
| `player_id` | string | Acting player native ID |
| `type_name` | string | SPADL action type |
| `start_x` | float64 | Action start x (SPADL 105x68) |
| `start_y` | float64 | Action start y |
| `end_x` | float64 | Action end x |
| `end_y` | float64 | Action end y |

### 5.2 Linkage-provenance columns (4)

| Column | Type | Description |
|--------|------|-------------|
| `frame_id` | Int64 | Linked tracking frame ID (NaN if unlinked) |
| `time_offset_seconds` | float64 | Action time minus frame time (NaN if unlinked) |
| `link_quality_score` | float64 | `1 - abs(dt)/tolerance` (NaN if unlinked) |
| `n_candidate_frames` | int64 | Frames in same period within tolerance |

### 5.3 Feature columns (66 columns)

See §4.3 enrichment chain for the complete column list per feature group. Includes:
- 1 `defending_gk_player_id` (GK resolution)
- 3 event-based GK context (`gk_was_distributing`, `gk_was_engaged`, `gk_actions_in_possession`)
- 4 pre-shot GK position, 2 pre-shot GK angle (shot-only; NaN for non-shot rows)
- 4 action context, 2 actor pre-window, 3 pressure, 3 pitch control, 6 defensive line
- 6 off-ball context, 3 Ward line-break, 14 team shape, 3 DAS
- 4 GK influence, 5 cover shadows, 3 sync score

### 5.4 Audit column (1)

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | timestamp | UTC ingestion timestamp |

### 5.5 Total: 83 columns

12 identity + 4 linkage + 66 features + 1 `_ingested_at` = 83 columns. No duplicates (verified programmatically).

## 6. dbt Layering

### 6.1 Staging view: `stg_spadl__tracking_context`

Passthrough view on `bronze.spadl_tracking_context`. Type casting where needed. Exposes `match_id` as `native_match_id`, `player_id` as `player_id_native`, `team_id` as `team_id_native` for Kimball JOIN resolution.

### 6.2 Gold mart: `fct_tracking_context`

| Property | Value |
|----------|-------|
| Materialization | incremental |
| `on_schema_change` | `append_new_columns` |
| Unique key | `(match_key, action_id)` |
| Contract | enforced |
| Liquid clustering | `match_key` |

**Kimball FK resolution:**

- `INNER JOIN dim_matches ON native_match_id` -> `match_key`
- `INNER JOIN dim_players ON player_id_native` -> `player_key`
- `INNER JOIN dim_teams ON team_id_native` -> `team_key`

Pure Kimball from day one --- no legacy BIGINT `match_id` / `player_id` / `team_id` columns. No dual-column deprecation window needed (new table).

**dbt YAML tests:**

- `dbt_utils.unique_combination_of_columns` on `(match_key, action_id)`
- `not_null` on `match_key`, `player_key`, `team_key`, `action_id`, `data_source`
- `accepted_values` on `data_source`: `['idsse', 'metrica', 'skillcorner']`

**Terminal deduplication guard:**

`QUALIFY ROW_NUMBER() OVER (PARTITION BY match_key, action_id ORDER BY _ingested_at DESC) = 1` (per synced-table PG PK dual-defense convention).

## 7. Lakebase Synced Table

| Property | Value |
|----------|-------|
| Source table | `fct_tracking_context` |
| CDF | `delta.enableChangeDataFeed = true` on source |
| PK | `(match_key, action_id)` |
| Indexes | `idx_tracking_context_match_key` on `(match_key)`, `idx_tracking_context_player_key` on `(player_key)` |
| Grants | Added to `maintain_synced_tables.py` registry |

No `ON ONLY` indexes (cascade to partitions). Managed by `scripts/create_indexes.py`, auto-reapplied by daily `lakebase-grants.yml` workflow.

## 8. Workflow Card

**File:** `workflow-cards/wf-tracking-context.yaml`

| Field | Value |
|-------|-------|
| Inputs | `wf-spadl`, `wf-idsse`, `wf-metrica`, `wf-skillcorner` |
| Output | `bronze.spadl_tracking_context` |
| `dbt_model` | `fct_tracking_context` |
| `for_each_task` | per-match fan-out |

## 9. Skip Guard

Anti-join pattern: skip matches already present in `bronze.spadl_tracking_context`. Same pattern as `wf-line-breaking` and `wf-pitch-control`. Registered in:

- Guard conformance test (`test_guard_conformance.py`)
- Watermark system (`TestWatermarkGuardHasCardInputs`)

## 10. HF Dataset Publish

**Script:** `scripts/publish_tracking_context_hf.py` (PEP 723)

**HF repo:** `luxury-lakehouse/spadl-tracking-context`

**Card:** `docs/huggingface/dataset-cards/spadl-tracking-context.md`

- Partitioned parquet by `data_source`
- ADR-014 compliant: `upload_hf_readme` + `get_hf_card_path`
- Card documents all feature columns with units, ranges, and academic references
- Registered in `_HF_JOBS_SCRIPT_TO_CARD` dict in `test_card_parity_with_terraform.py`
- Card parity enforced by `test_hf_publish_parity.py`

## 11. Testing

| Test | Purpose |
|------|---------|
| `test_tracking_context_schema_parity.py` | Bronze DDL constant <-> UDF output column set parity (same pattern as `test_spadl_vaep_writer_parity.py`) |
| `test_tracking_context_bronze_coverage.py` | Per-provider fixture-based coverage (IDSSE, Metrica, SkillCorner) |
| Entry in `test_staging_coverage.py` | Staging model covers all bronze cols |
| `test_format_contract.py` extension | Native ID format contracts for TC-1 joins |
| Existing ADR-018 singular SQL tests | `assert_<source>_<entity>_native_join_resolves` (covers TC-1 joins) |
| Benchmark | At least one IDSSE match end-to-end enrichment timing |
| `test_hf_publish_parity.py` extension | New publisher registered |
| `test_card_parity_with_terraform.py` extension | New script-to-card mapping |

## 12. Dependency Changes

In `pyproject.toml`, the `spadl` extra changes from:

```toml
spadl = [
    "silly-kicks>=3.11.1,<4",
]
```

to:

```toml
spadl = [
    "silly-kicks[kloppy,das]>=3.11.2,<4",
]
```

This transitively pulls `kloppy>=3.18.0` and `accessible-space>=2.0,<3`.

The `>=3.11.2` lower bound is required for the linkage-provenance skip guard fix (silly-kicks ships the fix in 3.11.2).

Wheel rebuild required (new workflow card + dbt YAML bundled). Version bump via `bump_wheel.py`.

## 13. xT Model Sourcing

`ExpectedThreat` (from `silly_kicks.xthreat`) is a Markov chain iterative solver that requires `.fit(actions)` on SPADL data. It is **not** a simple lookup.

The xT model is fitted inline at the start of the pipeline:

1. Load all SPADL actions from `bronze.spadl_actions` on the driver.
2. `xt = ExpectedThreat().fit(actions)` --- takes seconds, produces a 12x16 numpy grid.
3. The grid is deterministic for a given action set (fixed-point iteration with `eps=1e-5`). Re-fitted each pipeline run to incorporate newly-ingested actions.
4. Capture `xt` as a read-only closure reference in the UDF (not a frozen dataclass --- `ExpectedThreat` is a regular class).
5. Pass into `add_gk_influence` and `add_cover_shadows` per match.

No external table join. No serialized model artifact. Self-contained.

## 14. Out of Scope

These are confirmed follow-up cycles, not deferred work:

- Wiring `fct_tracking_context` into VAEP training as additional features
- Taipy UI pages consuming the new mart
- Gradient Sports provider support (pending license)

### 14.1 Overlapping table deprecation

`fct_tracking_context` subsumes the grain and feature coverage of three existing tables:

| Table | Overlap | Deprecation trigger |
|-------|---------|-------------------|
| `fct_line_breaking_results` | Same grain (per-action), `line_break` + Ward columns duplicated | After per-match validation confirms parity |
| `fct_pitch_control_values` | Same grain, 3 pitch control columns duplicated | After per-match validation confirms parity |
| `fct_tracking_frames` | Different grain (per-frame, 120x80), but downstream consumers can migrate to silly-kicks converter path (105x68 LTR) | After all consumers migrated |

Deprecation is a separate cycle. During the validation phase, both old and new tables coexist. Lakehouse analytics modules (`pitch_control.py`, `line_breaking.py`, `team_shape.py`, `off_ball_xt.py`, `expected_threat.py`) are deprecated after per-match validation against silly-kicks equivalents.

## 15. Provider Coverage

| Provider | Matches | Tracking rows (est.) | Actions (est.) |
|----------|---------|---------------------|----------------|
| IDSSE | 7 | ~4.7M | ~5K |
| Metrica | 3 | ~1.5M | ~2K |
| SkillCorner | 10 | ~2M | ~10K |
| **Total** | **20** | **~8.2M** | **~17K** |

## 16. References

- Andrienko, Gennady et al. "Visual analysis of pressure in football." Data Mining and Knowledge Discovery, 2017. (pressure oval model)
- Anzer, Gabriel and Bauer, Pascal. "A goal scoring probability model for shots based on synchronized positional and event data." Frontiers in Sports and Active Living, 2021. (GK positioning framework)
- Bekkers, Jesse and Robberechts, Pieter. "Defining pressure in association football." KU Leuven, 2023. (pi pressure index)
- Bischofberger, Aron and Baca, Arnold. "Accessible Space." J. Big Data, 2026. (DAS)
- Clemente, Filipe et al. "Collective behaviour analysis." 2013. (team shape)
- Fernandez, Javier and Bornn, Luke. "Wide Open Spaces." 2018. (F/B pitch control)
- Karakus, Oguzhan and Arkadas, Ersin. "Line-breaking passes." 2025. (Ward method)
- Power, Paul et al. "Not All Passes Are Created Equal." KDD, 2017. (off-ball runs, line-breaking threshold)
- Singh, Karun. "Introducing Expected Threat (xT)." 2019. (xT grid solver)
- Spearman, William. "Beyond Expected Goals." 2018. (pitch control)
