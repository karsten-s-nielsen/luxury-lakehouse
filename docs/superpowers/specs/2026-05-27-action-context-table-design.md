# Unified Action Context Table Design

**Date**: 2026-05-27
**Status**: Approved design, pending implementation planning
**Scope**: Single wide fact table at action grain replacing `spadl_tracking_context` + 4 individual metric tables, published to HuggingFace
**Depends on**: silly-kicks 3.23.0 (shipped — adds `snapshot_to_tracking_frames` converter)

---

## 1. Problem Statement

The lakehouse currently produces action-level analytics features across 5+ separate pipelines:

| Pipeline | Bronze Table | dbt Mart | Columns |
|---|---|---|---|
| Tracking context | `spadl_tracking_context` | `fct_tracking_context` | 83 |
| PAUSA | `pausa_values` | `fct_pausa_values` | 16 |
| Line breaking | `line_breaking_results` | `fct_line_breaking_results` | 6 |
| ELASTIC sync | `elastic_sync_results` | (staging only) | 5 |
| Off-ball xT | `off_ball_xt_results` | `fct_off_ball_xt` | 7 |

All share the same grain (one row per SPADL action, keyed by `(data_source, match_id, action_id)`) yet live in separate tables. This is a split-fact anti-pattern:

- Consumers must join 2-5 tables to get the full feature set
- Each pipeline independently reads tracking data and re-links actions to frames (duplicated O(n) work)
- 5 bronze tables, 5 staging models, 5 marts, 5 synced tables, 5 Databricks tasks — operational overhead

silly-kicks 3.23.0 ships the complete composable `add_*` enrichment chain plus `snapshot_to_tracking_frames` for freeze-frame data. One `applyInPandas` call per match can now produce ALL action-level features in a single pass, including single-frame features from StatsBomb 360 freeze-frames.

## 2. Design

### 2.1 One Table, One Grain

Replace all action-grain pipelines with a single `bronze.spadl_action_context` table. One row per SPADL action across ALL providers. Tracking-dependent columns are NULL for event-only providers (sparse fact table — standard dimensional modeling practice; Parquet compresses NULL columns to near-zero).

### 2.2 Provider Tiers

```
Provider detection:
  ┌── Event-only (Wyscout, StatsBomb non-360) ─► add_game_state + add_pre_shot_gk_context (~5 new cols, rest NULL)
  │
  ├── SB360 (StatsBomb w/ freeze-frames) ──────► above + snapshot_to_tracking_frames → single-frame add_* features
  │
  └── Tracking (IDSSE, Metrica, SC, GS) ───────► full add_* chain (~102 cols populated)
```

Three tiers: event-only (SPADL actions only), SB360 (actions + per-event freeze-frame snapshots converted to synthetic tracking frames via `snapshot_to_tracking_frames`), and tracking (actions + continuous tracking frames). The SB360 tier populates single-frame positional features (Ward line-breaking, team shape, defensive line, action context) while velocity/temporal features degrade to NaN (cover shadows, DAS, pitch control, actor pre-window). Detection: a match is SB360 if `data_source == "statsbomb"` and freeze-frame data exists in `bronze.statsbomb_360`.

### 2.3 Pipeline Architecture

```
bronze.spadl_actions ────────────┐
bronze.<provider>_tracking ──────┤─► applyInPandas(enrich_match) ─► bronze.spadl_action_context
bronze.expected_threat_grids ────┘

bronze.spadl_action_context ─► stg_action_context__values ─► fct_action_context ─► synced table
                                                                                  ─► HF dataset
```

## 3. Column Schema (~115 columns)

### 3.1 Identity (12 columns, ALL providers)

| Column | Type | Description |
|---|---|---|
| `data_source` | STRING | Provider identifier |
| `match_id` | STRING | Native match identifier |
| `action_id` | BIGINT | Sequential action number within match |
| `period_id` | BIGINT | Match period (1-5) |
| `time_seconds` | DOUBLE | Seconds from period start |
| `team_id` | STRING | Acting team native identifier |
| `player_id` | STRING | Acting player native identifier |
| `type_name` | STRING | SPADL action type |
| `start_x` | DOUBLE | Action start x (meters, 0-105) |
| `start_y` | DOUBLE | Action start y (meters, 0-68) |
| `end_x` | DOUBLE | Action end x |
| `end_y` | DOUBLE | Action end y |

### 3.2 Game State (1 column, ALL providers)

| Column | Type | Description |
|---|---|---|
| `game_state` | STRING | "winning", "losing", or "drawing" from acting team's perspective |

Derived from `silly_kicks.spadl.add_game_state`. Pure SPADL — no tracking needed.

### 3.3 Frame Linkage (4 columns, tracking providers only)

| Column | Type | Description |
|---|---|---|
| `frame_id` | BIGINT | Matched tracking frame |
| `time_offset_seconds` | DOUBLE | Temporal offset between event and matched frame |
| `link_quality_score` | DOUBLE | Linkage confidence (0-1) |
| `n_candidate_frames` | BIGINT | Number of candidate frames in search window |

### 3.4 GK Resolution (4 columns, ALL providers)

| Column | Type | Description |
|---|---|---|
| `defending_gk_player_id_native` | STRING | Defending goalkeeper identifier |
| `gk_was_distributing` | BOOLEAN | GK had the ball in recent possession |
| `gk_was_engaged` | BOOLEAN | GK was active in possession sequence |
| `gk_actions_in_possession` | BIGINT | Count of GK actions in current possession |

Pure SPADL derivation via `silly_kicks.spadl.utils.add_pre_shot_gk_context` — works without tracking data. Populated for ALL providers including event-only (StatsBomb, Wyscout). Valuable for xG/PSxG feature engineering.

### 3.5 GK Spatial (6 columns, shot-only + tracking)

| Column | Type | Description |
|---|---|---|
| `pre_shot_gk_x` | DOUBLE | GK x-position at shot moment |
| `pre_shot_gk_y` | DOUBLE | GK y-position at shot moment |
| `pre_shot_gk_distance_to_goal` | DOUBLE | GK distance to goal center (m) |
| `pre_shot_gk_distance_to_shot` | DOUBLE | GK distance to shooter (m) |
| `pre_shot_gk_angle_to_shot_trajectory` | DOUBLE | GK angle relative to shot vector |
| `pre_shot_gk_angle_off_goal_line` | DOUBLE | GK displacement from goal line |

### 3.6 Action Context (4 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `nearest_defender_distance` | DOUBLE | Distance to closest opponent (m) |
| `actor_speed` | DOUBLE | Actor speed at action moment (m/s) |
| `receiver_zone_density` | BIGINT | Defenders within receiver zone radius |
| `defenders_in_triangle_to_goal` | BIGINT | Defenders in triangle from ball to goal posts |

### 3.7 Actor Pre-Window (2 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `actor_arc_length_pre_window` | DOUBLE | Distance actor traveled in pre-action window (m) |
| `actor_displacement_pre_window` | DOUBLE | Straight-line displacement in pre-action window (m) |

### 3.8 Pressure (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `pressure_on_actor__andrienko_oval` | DOUBLE | Andrienko oval pressure model |
| `pressure_on_actor__link_zones` | DOUBLE | Link/zone-based pressure model |
| `pressure_on_actor__bekkers_pi` | DOUBLE | Bekkers PI pressure index |

### 3.9 Pitch Control (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `pitch_control_at_ball__spearman` | DOUBLE | Spearman pitch control at ball location |
| `pitch_control_at_ball__fernandez_bornn` | DOUBLE | Fernandez-Bornn pitch control at ball |
| `pitch_control_at_ball__voronoi` | DOUBLE | Voronoi-based pitch control at ball |

### 3.10 Defensive Line (6 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `defensive_line_x` | DOUBLE | Deepest defender cluster centroid x |
| `back_line_high_x` | DOUBLE | Highest x of back line cluster |
| `compactness_x` | DOUBLE | Longitudinal compactness |
| `lateral_width` | DOUBLE | Back line lateral width (m) |
| `max_lateral_gap` | DOUBLE | Largest gap between adjacent defenders (m) |
| `back_n_count` | BIGINT | Number of players in back line cluster |

### 3.11 Off-Ball Context (6 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `line_break` | BOOLEAN | Threshold-based line break detection |
| `n_attackers_behind_line` | BIGINT | Attackers beyond defensive line |
| `n_off_ball_runners_pre_window` | BIGINT | Off-ball runners in pre-window |
| `max_off_ball_run_displacement_pre_window` | DOUBLE | Max run displacement (m) |
| `mean_off_ball_run_speed_pre_window` | DOUBLE | Mean run speed (m/s) |
| `n_off_ball_runners_toward_goal_pre_window` | BIGINT | Runners moving goalward |

### 3.12 Ward Line-Breaking (3 columns, tracking OR 360)

| Column | Type | Description |
|---|---|---|
| `line_break__ward` | BOOLEAN | Ward-clustering line break detection |
| `lines_broken__ward` | BIGINT | Number of lines broken (0-3) |
| `line_breaking_type__ward` | STRING | "between_lines" or "around_line" (NULL when no lines broken) |

Available for tracking providers AND StatsBomb 360 matches (via `snapshot_to_tracking_frames` synthetic frames). Velocity-dependent features in the same frame degrade to NaN on SB360; positional features (Ward line-breaking, team shape, defensive line) populate correctly.

### 3.13 Team Shape (14 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `team_shape_{metric}_{attacking\|defending}` | DOUBLE/BIGINT | 7 metrics × 2 teams |

Metrics: `centroid_x`, `centroid_y`, `convex_hull_area`, `team_length`, `team_width`, `stretch_index`, `n_outfield_players`.

### 3.14 DAS (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `das_team` | DOUBLE | Dominant area share for acting team |
| `das_opponent` | DOUBLE | Dominant area share for opponent |
| `das_diff` | DOUBLE | DAS differential (team - opponent) |

### 3.15 GK Influence (4 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `gk_pitch_control_share_weighted` | DOUBLE | GK weighted share of pitch control |
| `gk_reachable_area_m2` | DOUBLE | GK reachable area (m²) |
| `gk_closing_time_mean_s__six_yard_box` | DOUBLE | Mean closing time to 6-yard box (s) |
| `gk_closing_time_min_s__six_yard_box` | DOUBLE | Min closing time to 6-yard box (s) |

### 3.16 Cover Shadows (5 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `n_blocked_receivers` | BIGINT | Teammates in cover shadow |
| `n_potential_receivers` | BIGINT | Total potential pass receivers |
| `blocking_score` | DOUBLE | Aggregate blocking score (0-1) |
| `blocked_threat_fraction` | DOUBLE | Fraction of xT blocked |
| `max_single_defender_blocking_score` | DOUBLE | Max individual defender blocking |

### 3.17 Sync Score (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `sync_score_min` | DOUBLE | Minimum sync score in window |
| `sync_score_mean` | DOUBLE | Mean sync score in window |
| `sync_score_high_quality_frac` | DOUBLE | Fraction of high-quality sync frames |

### 3.18 OBSO (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `obso_actual` | DOUBLE | OBSO at actual pass destination |
| `obso_peak` | DOUBLE | Peak OBSO in temporal window |
| `obso_optimal` | DOUBLE | Best available OBSO at pass moment |

### 3.19 PAUSA (3 columns, tracking, pass-only)

| Column | Type | Description |
|---|---|---|
| `pausa_temporal` | DOUBLE | Temporal judgment (actual/peak OBSO) |
| `pausa_spatial` | DOUBLE | Spatial selection (actual/optimal OBSO) |
| `pausa_composite` | DOUBLE | Composite PAUSA (temporal × spatial) |

NaN for non-pass actions.

### 3.20 Space Creation (2 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `space_created_m2_team` | DOUBLE | Space created by acting team (m²) |
| `space_created_m2_opponent` | DOUBLE | Space created by opponent team (m²) |

### 3.21 ELASTIC Sync (3 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `elastic_frame_id` | BIGINT | ELASTIC-aligned frame (may differ from linkage frame_id) |
| `elastic_confidence` | DOUBLE | Alignment confidence score |
| `elastic_error_seconds` | DOUBLE | Estimated alignment error (s) |

### 3.22 Shape Graph (6 columns, tracking)

| Column | Type | Description |
|---|---|---|
| `shape_graph_{density\|n_edges\|mean_stability}_{attacking\|defending}` | DOUBLE/BIGINT | 3 metrics × 2 teams |

### 3.23 Audit (1 column)

| Column | Type | Description |
|---|---|---|
| `_ingested_at` | TIMESTAMP | UTC write timestamp |

### 3.24 Column Count Summary

| Group | Columns | Event-only | SB360 | Tracking |
|---|---|---|---|---|
| Identity | 12 | Populated | Populated | Populated |
| Game state | 1 | Populated | Populated | Populated |
| Frame linkage | 4 | NULL | Populated | Populated |
| GK resolution | 4 | Populated | Populated | Populated |
| GK spatial | 6 | NULL | NULL | Populated |
| Action context | 4 | NULL | Partial (actor_speed=NaN) | Populated |
| Actor pre-window | 2 | NULL | NULL | Populated |
| Pressure | 3 | NULL | NULL | Populated |
| Pitch control | 3 | NULL | NULL | Populated |
| Defensive line | 6 | NULL | Populated | Populated |
| Off-ball context | 6 | NULL | NULL | Populated |
| Ward line-breaking | 3 | NULL | Populated | Populated |
| Team shape | 14 | NULL | Populated | Populated |
| DAS | 3 | NULL | NULL | Populated |
| GK influence | 4 | NULL | NULL | Populated |
| Cover shadows | 5 | NULL | NULL | Populated |
| Sync score | 3 | NULL | NULL | Populated |
| OBSO | 3 | NULL | NULL | Populated |
| PAUSA | 3 | NULL | NULL | Populated |
| Space creation | 2 | NULL | NULL | Populated |
| ELASTIC sync | 3 | NULL | NULL | Populated |
| Shape graph | 6 | NULL | NULL | Populated |
| Audit | 1 | Populated | Populated | Populated |
| **Total** | **~102** | **~18-21** | **~48-51** | **~102** |

## 4. Pipeline Implementation

### 4.1 Module: `src/ingestion/action_context.py`

Structurally mirrors `src/ingestion/tracking_context.py`. Key characteristics:

- **Guard**: `wf-action-context`, source = `bronze.spadl_actions`, results = `bronze.spadl_action_context`. Uses `find_new_ids` on `match_id`.
- **Frame batching**: Same `_FRAME_BATCH_SIZE = 250` for IDSSE (avoids 1 GB UDF group cap).
- **Provider dispatch**: `data_source` determines which `add_*` calls execute.
- **Write pattern**: `replaceWhere` on `match_id IN (...)` for idempotency.
- **xT grid**: Loaded once at driver level as a `silly_kicks.xthreat.ExpectedThreat` instance, broadcast to executors via frozen dataclass closure. Passed as positional `xt` argument to `add_gk_influence` and `add_cover_shadows`.
- **home_team_id**: Resolved per match from `dim_matches` join on `match_id` (same mechanism as `tracking_context.py`). Passed as keyword argument to all enrichments that need team-relative context.
- **Schema constant**: `_RESULT_COLUMNS: list[str]` defined at module level — the column contract between Python writer and dbt staging. Plan Task 0 deliverable.

### 4.2 Enrichment Chain (Tracking Providers)

**Identity resolution wrapper**: Before calling the enrichment chain, the caller MUST run `_resolve_enrichment_identity(actions_df, provider, match_id_native)` to overwrite `team_id`/`player_id` with silly-kicks-compatible values (e.g., Metrica `"Home"`/`"Away"` labels → tracking frame IDs). After enrichment, `_restore_native_identity(out)` restores lakehouse native IDs for bronze output. Without this, IDSSE/Metrica actions silently produce wrong team-perspective features and fail action-to-frame joins. Pattern copied from `tracking_context.py:649-653 + 808-812`.

**Post-enrichment output handling**: After the enrichment chain returns, the writer applies three transforms before bronze write:
1. `game_id` → `match_id` rename (silly-kicks uses `game_id`, lakehouse uses `match_id`)
2. `defending_gk_player_id` → `defending_gk_player_id_native` rename (ADR-018 convention)
3. Column selection/ordering to `_RESULT_COLUMNS` with `NaN` fill for any missing columns

```python
def _enrich_tracking_match(actions_df, tracking_df, xt, home_team_id):
    """Full enrichment chain for tracking providers.

    Args:
        actions_df: SPADL actions for one match.
        tracking_df: Tracking frames for the same match.
        xt: silly_kicks.tracking.ExpectedThreat instance (loaded once at driver).
        home_team_id: Home team identifier (resolved from dim_matches).
    """
    import numpy as np

    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.spadl import add_game_state
    from silly_kicks.tracking import (
        add_action_context, add_actor_pre_window, add_cover_shadows,
        add_das, add_defensive_line, add_elastic_sync, add_gk_influence,
        add_line_break, add_obso, add_off_ball_context,
        add_pausa, add_pre_shot_gk_angle,
        add_pre_shot_gk_position, add_pressure_on_actor, add_shape_graph,
        add_space_creation, add_sync_score, add_team_shape,
        link_actions_to_frames, pitch_control_at_action,
    )

    # Step 0: Actions-only enrichments (no tracking needed)
    out = add_game_state(actions_df)

    # Step 1: Frame linkage — returns tuple[DataFrame, LinkReport].
    # Computed ONCE; `links` passed to every subsequent add_* call that
    # accepts links= to avoid 20+ redundant O(n) re-linkage passes.
    links, _report = link_actions_to_frames(out, tracking_df)

    # Step 2: GK resolution (pure SPADL + tracking; no links kwarg).
    # MUST precede add_pre_shot_gk_position — it adds defending_gk_player_id
    # which add_pre_shot_gk_position requires (raises ValueError if absent).
    out = add_pre_shot_gk_context(out, frames=tracking_df)

    # Step 3: Action context
    out = add_action_context(out, tracking_df, links=links)

    # Step 4: Actor pre-window
    out = add_actor_pre_window(out, tracking_df, links=links)

    # Step 5a: Pressure — andrienko_oval + link_zones (no ball rows needed)
    out = add_pressure_on_actor(
        out, tracking_df, links=links,
        methods=("andrienko_oval", "link_zones"),
    )

    # Step 5b: Pressure — bekkers_pi (needs is_ball=True rows in tracking;
    # not all providers supply them). Degrade to NaN if absent.
    try:
        out = add_pressure_on_actor(
            out, tracking_df, links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            logger.error("bekkers_pi degraded to NaN: %s", exc)
            out["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise

    # Step 6: Pitch control — 3 methods via Series API (avoids 3× full-copy
    # from add_pitch_control; no home_team_id param on this API).
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(out, tracking_df, links=links, method=method)
        out[s.name] = s.values

    # Step 7: Defensive line
    out = add_defensive_line(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 8: Off-ball context (umbrella — adds threshold line_break,
    # n_attackers_behind_line, AND the 4 off-ball-run columns).
    # Do NOT call add_off_ball_runs separately — add_off_ball_context covers it.
    out = add_off_ball_context(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 9: Ward line-breaking (separate from the threshold line_break in Step 8)
    out = add_line_break(out, tracking_df, links=links, method="ward", home_team_id=home_team_id)

    # Step 10: Team shape
    out = add_team_shape(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 11: DAS (chunk_size=10 prevents OOM on large IDSSE matches
    # under the 1 GB applyInPandas group cap)
    out = add_das(out, tracking_df, links=links, chunk_size=10)

    # Step 12: GK spatial (pre-shot position + angle).
    # add_pre_shot_gk_position requires defending_gk_player_id (from Step 2).
    out = add_pre_shot_gk_position(out, tracking_df, links=links)
    out = add_pre_shot_gk_angle(out, frames=tracking_df, links=links)

    # Step 13: GK influence (needs xt as positional arg)
    out = add_gk_influence(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows (needs xt as positional arg)
    out = add_cover_shadows(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 15: Shape graph
    out = add_shape_graph(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 16: OBSO — MUST precede add_pausa (Step 17).
    # add_pausa requires OBSO columns to exist; if missing, it internally
    # recomputes OBSO via add_obso — but that fallback path may diverge from
    # the direct call. Explicit ordering prevents silent double-computation.
    out = add_obso(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 17: PAUSA (depends on OBSO columns from Step 16)
    out = add_pausa(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 18: Space creation
    out = add_space_creation(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 19: ELASTIC sync
    out = add_elastic_sync(out, tracking_df)

    # Step 20: Sync score
    out = add_sync_score(out, links)

    return out
```

### 4.3 Enrichment Chain (Event-Only Providers)

```python
def _enrich_event_only_match(actions_df):
    """Minimal enrichment for event-only providers."""
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context

    out = add_game_state(actions_df)
    # GK resolution is pure SPADL — works without tracking data.
    # Populates defending_gk_player_id_native, gk_was_distributing,
    # gk_was_engaged, gk_actions_in_possession — valuable for xG/PSxG models.
    out = add_pre_shot_gk_context(out)
    # All tracking columns remain NULL (initialized by schema)
    return out
```

### 4.4 StatsBomb 360 Freeze-Frame Enrichment (SB360 Tier)

```python
def _enrich_sb360_match(
    actions_df: pd.DataFrame,
    freeze_frames: pd.DataFrame,
    home_team_id: str,
) -> pd.DataFrame:
    """Enrichment chain for StatsBomb 360 matches.

    Uses snapshot_to_tracking_frames to convert per-event freeze-frame
    snapshots into synthetic tracking frames, then runs single-frame
    add_* features. Velocity/temporal features degrade to NaN.

    Args:
        actions_df: SPADL actions for one match.
        freeze_frames: StatsBomb 360 freeze-frame data. Caller must
            pre-process from SB360 format to the converter's input
            contract: (action_id, team_id, is_goalkeeper, x, y).
            Coordinates must be in SPADL coordinate system.
        home_team_id: Home team identifier (from dim_matches).
    """
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_defensive_line,
        add_line_break,
        add_team_shape,
        snapshot_to_tracking_frames,
    )

    # Step 0: Actions-only enrichments
    out = add_game_state(actions_df)
    # GK resolution — SPADL-only (no frames=). Snapshot frames lack temporal
    # continuity for GK tracking fallback; positional features run post-conversion.
    out = add_pre_shot_gk_context(out)

    # Step 1: Convert freeze-frames to synthetic tracking frames + links.
    # Links are pre-built (1:1 action→frame, time_offset=0, quality=1.0).
    frames, links = snapshot_to_tracking_frames(freeze_frames, out)

    if len(frames) == 0:
        return out  # No freeze-frame data — event-only fallback

    # Step 2: Single-frame positional features (work on snapshot-derived frames).
    # action_context: nearest_defender_distance, defenders_in_triangle_to_goal,
    #   receiver_zone_density populate; actor_speed → NaN (reads speed column).
    out = add_action_context(out, frames, links=links)

    # Step 3: Defensive line
    out = add_defensive_line(out, frames, links=links, home_team_id=home_team_id)

    # Step 4: Ward line-breaking — the primary SB360 value-add
    out = add_line_break(out, frames, links=links, method="ward", home_team_id=home_team_id)

    # Step 5: Team shape
    out = add_team_shape(out, frames, links=links, home_team_id=home_team_id)

    # Velocity/temporal features (cover_shadows, DAS, pitch_control, actor_pre_window,
    # OBSO, PAUSA, space_creation, elastic_sync, pressure) are NOT called — they
    # require vx/vy or multi-frame windows that snapshot-derived frames cannot provide.
    # These columns remain NULL (initialized by _build_output schema).

    return out
```

**Freeze-frame input contract:** The caller reads `bronze.statsbomb_360` and maps provider-specific columns (`teammate`, `actor`, `keeper`) to the converter's input schema `(action_id, team_id, is_goalkeeper, x, y)`. Coordinate transform from StatsBomb 120×80 to SPADL is the caller's responsibility (same pattern as existing StatsBomb SPADL conversion). The `snapshot_to_tracking_frames` converter accepts provider-agnostic input only.

**Graceful degradation:** On SB360 frames, `actor_speed` degrades to NaN (reads `speed` column which is NaN on snapshots). The other 3 `add_action_context` columns (`nearest_defender_distance`, `defenders_in_triangle_to_goal`, `receiver_zone_density`) populate correctly because they use only positional data.

## 5. dbt Layer

### 5.1 Staging

`models/staging/action_context/_action_context__sources.yml` — defines `bronze.spadl_action_context`.

`models/staging/action_context/stg_action_context__values.sql` — passthrough with type coercion where needed.

### 5.2 Mart

`models/marts/fct_action_context.sql`:
- `contract: enforced: true`
- Liquid clustered by `(data_source, match_id)`
- Tags: `output_mart`
- Direct passthrough from staging (no joins — `game_state` is computed in Python, not via `int_running_score`)

### 5.3 dbt Tests

- Schema tests: not_null on identity columns, accepted_values for `game_state` and `line_breaking_type__ward`
- Referential integrity: `match_id` resolves to `dim_matches`

## 6. Lakebase (Synced Table + Indexes)

- Synced table: `fct_action_context_synced` via SDK `w.postgres.create_synced_table()`
- Scheduling: TRIGGERED (CDF-enabled source)
- Indexes: composite btree on `(data_source, match_id)`, `(data_source, player_id)`, `(data_source, team_id, match_id)`

## 7. HuggingFace Dataset Publication

- **Repo**: `luxury-lakehouse/spadl-action-context`
- **Script**: `scripts/publish_action_context_hf.py`
- **SQL**: `SELECT * FROM soccer_analytics.dev_gold.fct_action_context WHERE data_source != 'gradientsports'`
- **Partitioning**: Hive-style by `data_source` (Parquet)
- **README card**: `docs/huggingface/dataset-cards/spadl-action-context.md`
- **Registered in**: HF Space footer, `docs/huggingface/org-card.md`

Replaces the existing `luxury-lakehouse/spadl-tracking-context` dataset (which will be deprecated after the new dataset is validated).

## 8. Operational Lessons from `tracking_context` Deployment

These lessons were learned the hard way during the tracking_context, PAUSA, line-breaking, and synced-table migration cycles. The action-context deployment MUST apply all of them from day one.

### 8.1 Bronze Table Creation

- **CDF from birth**: Enable `delta.enableChangeDataFeed = true` on `bronze.spadl_action_context` at `CREATE TABLE` time (not as a post-hoc `ALTER TABLE`). TRIGGERED synced tables require CDF. Omitting it causes `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` if the table is ever dropped/recreated. (Incident: PR #258, 2026-05-05.)
- **Liquid clustering**: Configure `CLUSTER BY (data_source, match_id)` on the bronze table, matching the `replaceWhere` partition key. Standard mart-table default per `CLAUDE.md § Databricks Performance`.
- **autoOptimize + deletion vectors**: Set `delta.autoOptimize.optimizeWrite = true` and `delta.enableDeletionVectors = true` in table properties. Needed for idempotent `replaceWhere` without partition skew.
- **Schema constant parity**: `_RESULT_COLUMNS` in `action_context.py` must exactly match the `_ACTION_CONTEXT_DDL` `CREATE TABLE` column list AND the dbt contract. Enforced by a pytest that parses the DDL (same pattern as `test_cost_hook_integration.py`). Without this, `DELTA_MERGE_UNRESOLVED_EXPRESSION` silently fails every MERGE (ADR-002 §4).

### 8.2 dbt Contract

- **`contract: enforced: true`** on `fct_action_context`. Passthrough columns from staging MUST have explicit casts matching the contract types (e.g. `round()` → `double`, not implicit). Incremental builds mask contract mismatches that `--full-refresh` exposes. (Lesson from SK3-MIG-A.)
- **Incremental skip guard**: The mart uses `match_id`-based incremental logic. After any upstream library bump that changes existing-match output shape, run `dbt run --select fct_action_context --full-refresh`. Otherwise stale rows persist silently. (CLAUDE.md § `dbt_incremental_match_id_skip_silent_stale`.)

### 8.3 Synced Table + Lakebase

- **SDK creation** via `w.postgres.create_synced_table()` (not UI, not TF). Use `SyncedTableConfig` in `scripts/migrate_synced_tables.py`. (SDK migration shipped 2026-05-23.)
- **TRIGGERED scheduling** for `fct_action_context_synced` — it's a large append-mostly fact table (same tier as `fct_tracking_context_synced`).
- **PK uniqueness**: Lakebase rejects non-unique or NULL PKs. Verify `(data_source, match_id, action_id)` uniqueness in a dbt test before creating the synced table.
- **Indexes applied post-creation**: Custom PG indexes are dropped when a synced table is recreated. `scripts/create_indexes.py` reapplies them (daily GH Action + manual via `--skip-refresh`). Add `fct_action_context_synced` index entries to the script BEFORE first creation.
- **Grants**: `scripts/run_lakebase_grants.py` must include the new synced table. Without explicit grants, the Taipy app's Lakebase connection cannot read it.
- **"Please contact Databricks support"** errors during synced table creation are always on our side (PK issues, schema issues, CDF missing). Never file a ticket — investigate per the 6-step diagnostic in memory.
- **Post-recreation maintenance**: After any synced table delete + recreate cycle, run `scripts/create_indexes.py --verify` + `scripts/run_lakebase_grants.py` to restore indexes and grants.

### 8.4 HF Publication

- **Dataset card must exist before first publish** — `upload_hf_readme(...)` after `upload_folder(...)`. Parity test `test_hf_publish_parity.py` catches orphan repos without cards.
- **Register in ALL artifact-list locations**: HF Space footer, `docs/huggingface/org-card.md`, `README.md`. (CLAUDE.md § HF artifact link completeness.)
- **Exclude private data**: `WHERE data_source != 'gradientsports'` in the HF publish SQL (same as tracking-context publisher).

## 9. Phase-In Strategy

### Phase 1: Deploy (no retirement)

1. Add `src/ingestion/action_context.py` with full enrichment chain
2. Create `bronze.spadl_action_context` with CDF + liquid clustering + autoOptimize from birth
3. Register `wf-action-context` task in mega-job
4. Add dbt staging + mart models with `contract: enforced: true`
5. Create synced table (SDK, TRIGGERED) + indexes + grants
6. Run pipeline for all providers — new table populates alongside existing ones
7. Add HF publish script + dataset card, register in artifact lists

### Phase 2: Parity Validation

`src/tests/test_action_context_parity.py` asserts:

- For every `(data_source, match_id, action_id)` present in BOTH `spadl_tracking_context` and `spadl_action_context`: all 83 shared columns match within floating-point tolerance (`atol=1e-6`). The relaxed tolerance accounts for enrichment chain ordering differences between the old and new pipelines (pitch control, OBSO, and pressure are order-sensitive under floating-point arithmetic).
- For every `(match_id, action_id)` in `pausa_values`: `pausa_temporal`, `pausa_spatial`, `pausa_composite` match
- For every `(match_id, action_id)` in `line_breaking_results`: `line_break__ward`, `lines_broken__ward`, `line_breaking_type__ward` match
- For every `(match_id, event_id)` in `elastic_sync_results`: `elastic_confidence`, `elastic_error_seconds` match
- Event-only providers: `game_state` is NOT NULL, tracking columns ARE NULL

### Phase 3: Downstream Migration

- Update Taipy app queries to read `fct_action_context`
- Update any dbt downstream consumers

### Phase 4: Retirement (separate PR)

- Delete `src/ingestion/tracking_context.py`
- Delete `src/ingestion/pausa.py`, `line_breaking.py`, `line_breaking_tracking.py`, `line_breaking_360.py`, `line_breaking_common.py`, `elastic_sync.py`
- Delete corresponding dbt staging/mart models
- Delete synced tables for retired marts
- Remove retired tasks from mega-job
- Deprecate HF `spadl-tracking-context` (add redirect notice to README)

## 10. What Stays Separate

| Table | Reason |
|---|---|
| `bronze.off_ball_xt_results` | Player×match grain (aggregate), not action grain |
| `bronze.pitch_control_values` | Frame×grid grain (surface), not action grain |
| `bronze.defcon_results` | Action grain but experimental (defcon_lite not stable enough for consolidation) |
| `bronze.player_positions` | Frame×player grain (formations), not action grain |

## 11. Risk Register

| Risk | Mitigation |
|---|---|
| Full enrichment chain exceeds 15-min task budget for large matches | Frame batching (250 frames/batch) already proven in tracking_context.py. Profile early with IDSSE 90-min matches. |
| Parity failures due to silly-kicks API differences from lakehouse code | Golden-fixture comparison during development; fix discrepancies before declaring parity. Tolerance `atol=1e-6` for float ordering effects. |
| Event-only providers produce very sparse rows (wasted storage) | Parquet NULL compression makes this near-zero cost. Verified empirically: 100K sparse rows < 1 MB. |
| Retirement breaks downstream consumers | Phase-in strategy: new table runs in parallel for full validation cycle before any deletion. |
| Schema drift between Python writer and dbt contract | `_RESULT_COLUMNS` constant + DDL string in Python; dbt `contract: enforced: true`; parity test in `src/tests/`. (§8.1) |
| Synced table creation fails with "contact support" | Follow 6-step diagnostic (§8.3): PK uniqueness, CDF enabled, schema match. Never file Databricks ticket first. |
| Stale rows after library bump | `--full-refresh` after any silly-kicks bump that changes existing-match output. Incremental guards mask it otherwise (§8.2). |
| bekkers_pi crashes on providers without ball rows | Split pressure into two calls; bekkers_pi wrapped in try/except degrading to NaN with ERROR-level log (§4.2 Step 5b). |

## 12. Success Criteria

- All 6 providers produce rows in `fct_action_context` (StatsBomb + Wyscout event-only sparse, IDSSE + Metrica + SkillCorner + GradientSports tracking-dense). GradientSports is populated in the table but gated from HF publication (`WHERE data_source != 'gradientsports'`).
- `game_state` populated for ALL providers with zero NULL
- Parity test passes for all shared columns between old and new tables
- Per-match enrichment completes within task budget (< 15 min for largest matches)
- HF dataset `spadl-action-context` published with all providers, Hive-partitioned
- After retirement: 5 fewer Databricks tasks, 4 fewer bronze tables, 3 fewer dbt marts
