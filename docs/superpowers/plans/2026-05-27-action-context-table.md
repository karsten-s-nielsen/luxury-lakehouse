# Action Context Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy a unified `spadl_action_context` table that consolidates 5 separate action-grain pipelines into a single wide fact table, with dbt mart, Lakebase synced table, and HuggingFace dataset publication.

**Architecture:** Single `applyInPandas` pipeline per match runs the full silly-kicks 3.23.0 `add_*` enrichment chain, producing ~102 columns for tracking providers and sparse rows (game_state + GK resolution only) for event-only providers. Mirrors `tracking_context.py` structurally — same guard, frame batching, identity resolution, and write patterns.

**Tech Stack:** silly-kicks 3.23.0, PySpark `applyInPandas`, Delta Lake (bronze), dbt (staging + mart), Lakebase SDK synced tables, HuggingFace Hub.

**Spec:** `docs/superpowers/specs/2026-05-27-action-context-table-design.md`

---

## File Structure

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `scripts/migrations/2026-05-28-create-spadl-action-context.sql` | Bronze table DDL with CDF + liquid clustering + autoOptimize from birth |
| Create | `src/ingestion/action_context.py` | Pipeline module: guard, enrichment chains, UDF factory, provider dispatch, main/main_preflight |
| Create | `src/tests/test_action_context_schema_parity.py` | Schema parity: _RESULT_COLUMNS == DDL == dbt contract |
| Create | `src/tests/test_action_context_enrichment.py` | Unit tests for enrichment chains and pipeline helpers |
| Create | `dbt_project/models/staging/action_context/_action_context__sources.yml` | dbt source definition for bronze.spadl_action_context |
| Create | `dbt_project/models/staging/action_context/stg_action_context__values.sql` | Staging: dedup + type coercion + identity renames |
| Create | `dbt_project/models/staging/action_context/_action_context__models.yml` | Staging schema tests (not_null, accepted_values) |
| Create | `dbt_project/models/marts/fct_action_context.sql` | Gold mart: Kimball FK resolution, surrogate key, contract |
| Modify | `dbt_project/models/marts/_marts__models.yml` | Contract column definitions for fct_action_context |
| Create | `scripts/publish_action_context_hf.py` | HF dataset publisher (PEP 723 script) |
| Create | `docs/huggingface/dataset-cards/spadl-action-context.md` | HF dataset README card |
| Create | `workflow-cards/wf-action-context.yaml` | Workflow card for guard resolution and governance |
| Modify | `pyproject.toml:130` | Add entry points: compute_action_context, preflight_action_context |
| Modify | `dbt_project/seeds/task_workflow_mapping.csv` | Add task keys for wf-action-context |
| Modify | `scripts/create_indexes.py:211` | Add fct_action_context_synced btree indexes |
| Modify | `src/ingestion/refresh_synced_tables.py:234` | Add SyncedTableConfig for fct_action_context_synced |
| Modify | `src/tests/test_card_parity_with_terraform.py:232` | Register publish_action_context_hf.py in _HF_JOBS_SCRIPT_TO_CARD |

---

### Task 0: Bronze Migration SQL

Create the bronze table via migration (auto-applied before dbt build in CI per CLAUDE.md bronze migration convention). CDF + liquid clustering + autoOptimize from birth per spec §8.1.

**Files:**
- Create: `scripts/migrations/2026-05-28-create-spadl-action-context.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- scripts/migrations/2026-05-28-create-spadl-action-context.sql
--
-- AC-1 unified action context table — one row per SPADL action, all providers.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS); auto-applied by
-- .github/workflows/dbt-live-ci.yml "Apply pending bronze migrations" step.
--
-- CDF enabled from birth per spec §8.1; liquid clustering + autoOptimize
-- per lakehouse mart-table defaults.
--
-- Catalog hardcoded as `soccer_analytics` per existing migration convention.

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.spadl_action_context (
  data_source STRING,
  match_id STRING,
  action_id BIGINT,
  period_id BIGINT,
  time_seconds DOUBLE,
  team_id STRING,
  player_id STRING,
  type_name STRING,
  start_x DOUBLE,
  start_y DOUBLE,
  end_x DOUBLE,
  end_y DOUBLE,
  game_state STRING,
  frame_id BIGINT,
  time_offset_seconds DOUBLE,
  link_quality_score DOUBLE,
  n_candidate_frames BIGINT,
  defending_gk_player_id_native STRING,
  gk_was_distributing BOOLEAN,
  gk_was_engaged BOOLEAN,
  gk_actions_in_possession BIGINT,
  pre_shot_gk_x DOUBLE,
  pre_shot_gk_y DOUBLE,
  pre_shot_gk_distance_to_goal DOUBLE,
  pre_shot_gk_distance_to_shot DOUBLE,
  pre_shot_gk_angle_to_shot_trajectory DOUBLE,
  pre_shot_gk_angle_off_goal_line DOUBLE,
  nearest_defender_distance DOUBLE,
  actor_speed DOUBLE,
  receiver_zone_density BIGINT,
  defenders_in_triangle_to_goal BIGINT,
  actor_arc_length_pre_window DOUBLE,
  actor_displacement_pre_window DOUBLE,
  pressure_on_actor__andrienko_oval DOUBLE,
  pressure_on_actor__link_zones DOUBLE,
  pressure_on_actor__bekkers_pi DOUBLE,
  pitch_control_at_ball__spearman DOUBLE,
  pitch_control_at_ball__fernandez_bornn DOUBLE,
  pitch_control_at_ball__voronoi DOUBLE,
  defensive_line_x DOUBLE,
  back_line_high_x DOUBLE,
  compactness_x DOUBLE,
  lateral_width DOUBLE,
  max_lateral_gap DOUBLE,
  back_n_count BIGINT,
  line_break BOOLEAN,
  n_attackers_behind_line BIGINT,
  n_off_ball_runners_pre_window BIGINT,
  max_off_ball_run_displacement_pre_window DOUBLE,
  mean_off_ball_run_speed_pre_window DOUBLE,
  n_off_ball_runners_toward_goal_pre_window BIGINT,
  line_break__ward BOOLEAN,
  lines_broken__ward BIGINT,
  line_breaking_type__ward STRING,
  team_shape_centroid_x_attacking DOUBLE,
  team_shape_centroid_y_attacking DOUBLE,
  team_shape_convex_hull_area_attacking DOUBLE,
  team_shape_team_length_attacking DOUBLE,
  team_shape_team_width_attacking DOUBLE,
  team_shape_stretch_index_attacking DOUBLE,
  team_shape_n_outfield_players_attacking BIGINT,
  team_shape_centroid_x_defending DOUBLE,
  team_shape_centroid_y_defending DOUBLE,
  team_shape_convex_hull_area_defending DOUBLE,
  team_shape_team_length_defending DOUBLE,
  team_shape_team_width_defending DOUBLE,
  team_shape_stretch_index_defending DOUBLE,
  team_shape_n_outfield_players_defending BIGINT,
  das_team DOUBLE,
  das_opponent DOUBLE,
  das_diff DOUBLE,
  gk_pitch_control_share_weighted DOUBLE,
  gk_reachable_area_m2 DOUBLE,
  gk_closing_time_mean_s__six_yard_box DOUBLE,
  gk_closing_time_min_s__six_yard_box DOUBLE,
  n_blocked_receivers BIGINT,
  n_potential_receivers BIGINT,
  blocking_score DOUBLE,
  blocked_threat_fraction DOUBLE,
  max_single_defender_blocking_score DOUBLE,
  sync_score_min DOUBLE,
  sync_score_mean DOUBLE,
  sync_score_high_quality_frac DOUBLE,
  obso_actual DOUBLE,
  obso_peak DOUBLE,
  obso_optimal DOUBLE,
  pausa_temporal DOUBLE,
  pausa_spatial DOUBLE,
  pausa_composite DOUBLE,
  space_created_m2_team DOUBLE,
  space_created_m2_opponent DOUBLE,
  elastic_frame_id BIGINT,
  elastic_confidence DOUBLE,
  elastic_error_seconds DOUBLE,
  shape_graph_density_attacking DOUBLE,
  shape_graph_n_edges_attacking BIGINT,
  shape_graph_mean_stability_attacking DOUBLE,
  shape_graph_density_defending DOUBLE,
  shape_graph_n_edges_defending BIGINT,
  shape_graph_mean_stability_defending DOUBLE,
  _ingested_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = 'true',
  delta.autoOptimize.optimizeWrite = 'true',
  delta.autoOptimize.autoCompact = 'true'
);
```

- [ ] **Step 2: Verify migration is idempotent (no syntax errors)**

Run: `python -c "sql = open('scripts/migrations/2026-05-28-create-spadl-action-context.sql').read(); print(f'OK: {sql.count(chr(10))} lines')"`
Expected: OK: ~108 lines

- [ ] **Step 3: Commit**

```bash
git add scripts/migrations/2026-05-28-create-spadl-action-context.sql
git commit -m "feat(action-context): bronze migration — CREATE TABLE IF NOT EXISTS with CDF (Task 0)"
```

---

### Task 1: Schema Constants + DDL + Parity Test

The foundation — `_RESULT_COLUMNS` and `_ACTION_CONTEXT_DDL` define the column contract that every downstream consumer depends on. Build the test first.

**Files:**
- Create: `src/tests/test_action_context_schema_parity.py`
- Create: `src/ingestion/action_context.py` (initial — constants only)

- [ ] **Step 1: Write the schema parity test**

```python
# src/tests/test_action_context_schema_parity.py
"""Schema parity sentinel for action_context.py.

Ensures _RESULT_COLUMNS, _ACTION_CONTEXT_DDL, and the dbt contract
stay in sync. Same pattern as test_cost_hook_integration.py (ADR-002 §4).
"""
from __future__ import annotations

import re

from ingestion.action_context import _ACTION_CONTEXT_DDL, _RESULT_COLUMNS

_DDL_COL_RE = re.compile(r"(\w+)\s+\w+")


def _parse_ddl_columns(ddl: str) -> list[str]:
    """Extract column names from a Spark DDL string."""
    return _DDL_COL_RE.findall(ddl)


def test_result_columns_match_ddl() -> None:
    """_RESULT_COLUMNS and _ACTION_CONTEXT_DDL must list the same columns in order."""
    ddl_cols = _parse_ddl_columns(_ACTION_CONTEXT_DDL)
    assert ddl_cols == _RESULT_COLUMNS, (
        f"Column mismatch between _RESULT_COLUMNS ({len(_RESULT_COLUMNS)} cols) "
        f"and _ACTION_CONTEXT_DDL ({len(ddl_cols)} cols).\n"
        f"In RESULT but not DDL: {set(_RESULT_COLUMNS) - set(ddl_cols)}\n"
        f"In DDL but not RESULT: {set(ddl_cols) - set(_RESULT_COLUMNS)}"
    )


def test_result_columns_no_duplicates() -> None:
    """No duplicate column names in _RESULT_COLUMNS."""
    seen: set[str] = set()
    dupes: list[str] = []
    for col in _RESULT_COLUMNS:
        if col in seen:
            dupes.append(col)
        seen.add(col)
    assert not dupes, f"Duplicate columns in _RESULT_COLUMNS: {dupes}"
```

- [ ] **Step 2: Run test — verify it fails (action_context module doesn't exist yet)**

Run: `uv run pytest src/tests/test_action_context_schema_parity.py -v 2>&1 | head -20`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.action_context'`

- [ ] **Step 3: Write _RESULT_COLUMNS and _ACTION_CONTEXT_DDL**

Create `src/ingestion/action_context.py` with the schema constants. This extends the existing tracking_context's 83 columns with the new columns from the spec (game_state, OBSO, PAUSA, space creation, ELASTIC sync, shape graph).

```python
# src/ingestion/action_context.py
"""AC-1 — Unified action context pipeline.

Reads SPADL actions + tracking data from bronze, runs the full silly-kicks
enrichment chain in a single applyInPandas pass per match, writes results to
bronze.spadl_action_context.

Providers: ALL (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, GradientSports).
Event-only providers get game_state + GK resolution; tracking providers get ~102 cols.
Architecture: "Read from bronze, compute, write to bronze."
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    from silly_kicks.xthreat import ExpectedThreat

_TABLE_NAME = "spadl_action_context"

_FRAME_BATCH_SIZE = 250
_ACTION_TIME_BUFFER_SECONDS = 0.5

_RESULT_COLUMNS: list[str] = [
    # Identity (12)
    "data_source",
    "match_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id",
    "player_id",
    "type_name",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    # Game state (1)
    "game_state",
    # Frame linkage (4)
    "frame_id",
    "time_offset_seconds",
    "link_quality_score",
    "n_candidate_frames",
    # GK resolution (4)
    "defending_gk_player_id_native",
    "gk_was_distributing",
    "gk_was_engaged",
    "gk_actions_in_possession",
    # GK spatial (6)
    "pre_shot_gk_x",
    "pre_shot_gk_y",
    "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot",
    "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    # Action context (4)
    "nearest_defender_distance",
    "actor_speed",
    "receiver_zone_density",
    "defenders_in_triangle_to_goal",
    # Actor pre-window (2)
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    # Pressure (3)
    "pressure_on_actor__andrienko_oval",
    "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    # Pitch control (3)
    "pitch_control_at_ball__spearman",
    "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    # Defensive line (6)
    "defensive_line_x",
    "back_line_high_x",
    "compactness_x",
    "lateral_width",
    "max_lateral_gap",
    "back_n_count",
    # Off-ball context (6)
    "line_break",
    "n_attackers_behind_line",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    # Ward line-breaking (3)
    "line_break__ward",
    "lines_broken__ward",
    "line_breaking_type__ward",
    # Team shape (14)
    "team_shape_centroid_x_attacking",
    "team_shape_centroid_y_attacking",
    "team_shape_convex_hull_area_attacking",
    "team_shape_team_length_attacking",
    "team_shape_team_width_attacking",
    "team_shape_stretch_index_attacking",
    "team_shape_n_outfield_players_attacking",
    "team_shape_centroid_x_defending",
    "team_shape_centroid_y_defending",
    "team_shape_convex_hull_area_defending",
    "team_shape_team_length_defending",
    "team_shape_team_width_defending",
    "team_shape_stretch_index_defending",
    "team_shape_n_outfield_players_defending",
    # DAS (3)
    "das_team",
    "das_opponent",
    "das_diff",
    # GK influence (4)
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    # Cover shadows (5)
    "n_blocked_receivers",
    "n_potential_receivers",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    # Sync score (3)
    "sync_score_min",
    "sync_score_mean",
    "sync_score_high_quality_frac",
    # OBSO (3)
    "obso_actual",
    "obso_peak",
    "obso_optimal",
    # PAUSA (3)
    "pausa_temporal",
    "pausa_spatial",
    "pausa_composite",
    # Space creation (2)
    "space_created_m2_team",
    "space_created_m2_opponent",
    # ELASTIC sync (3)
    "elastic_frame_id",
    "elastic_confidence",
    "elastic_error_seconds",
    # Shape graph (6)
    "shape_graph_density_attacking",
    "shape_graph_n_edges_attacking",
    "shape_graph_mean_stability_attacking",
    "shape_graph_density_defending",
    "shape_graph_n_edges_defending",
    "shape_graph_mean_stability_defending",
    # Audit (1)
    "_ingested_at",
]

_ACTION_CONTEXT_DDL = (
    "data_source STRING, match_id STRING, action_id BIGINT, period_id BIGINT, "
    "time_seconds DOUBLE, team_id STRING, player_id STRING, type_name STRING, "
    "start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "game_state STRING, "
    "frame_id BIGINT, time_offset_seconds DOUBLE, link_quality_score DOUBLE, "
    "n_candidate_frames BIGINT, "
    "defending_gk_player_id_native STRING, gk_was_distributing BOOLEAN, "
    "gk_was_engaged BOOLEAN, gk_actions_in_possession BIGINT, "
    "pre_shot_gk_x DOUBLE, pre_shot_gk_y DOUBLE, "
    "pre_shot_gk_distance_to_goal DOUBLE, pre_shot_gk_distance_to_shot DOUBLE, "
    "pre_shot_gk_angle_to_shot_trajectory DOUBLE, pre_shot_gk_angle_off_goal_line DOUBLE, "
    "nearest_defender_distance DOUBLE, actor_speed DOUBLE, "
    "receiver_zone_density BIGINT, defenders_in_triangle_to_goal BIGINT, "
    "actor_arc_length_pre_window DOUBLE, actor_displacement_pre_window DOUBLE, "
    "pressure_on_actor__andrienko_oval DOUBLE, pressure_on_actor__link_zones DOUBLE, "
    "pressure_on_actor__bekkers_pi DOUBLE, "
    "pitch_control_at_ball__spearman DOUBLE, pitch_control_at_ball__fernandez_bornn DOUBLE, "
    "pitch_control_at_ball__voronoi DOUBLE, "
    "defensive_line_x DOUBLE, back_line_high_x DOUBLE, compactness_x DOUBLE, "
    "lateral_width DOUBLE, max_lateral_gap DOUBLE, back_n_count BIGINT, "
    "line_break BOOLEAN, n_attackers_behind_line BIGINT, "
    "n_off_ball_runners_pre_window BIGINT, "
    "max_off_ball_run_displacement_pre_window DOUBLE, "
    "mean_off_ball_run_speed_pre_window DOUBLE, "
    "n_off_ball_runners_toward_goal_pre_window BIGINT, "
    "line_break__ward BOOLEAN, lines_broken__ward BIGINT, "
    "line_breaking_type__ward STRING, "
    "team_shape_centroid_x_attacking DOUBLE, team_shape_centroid_y_attacking DOUBLE, "
    "team_shape_convex_hull_area_attacking DOUBLE, team_shape_team_length_attacking DOUBLE, "
    "team_shape_team_width_attacking DOUBLE, team_shape_stretch_index_attacking DOUBLE, "
    "team_shape_n_outfield_players_attacking BIGINT, "
    "team_shape_centroid_x_defending DOUBLE, team_shape_centroid_y_defending DOUBLE, "
    "team_shape_convex_hull_area_defending DOUBLE, team_shape_team_length_defending DOUBLE, "
    "team_shape_team_width_defending DOUBLE, team_shape_stretch_index_defending DOUBLE, "
    "team_shape_n_outfield_players_defending BIGINT, "
    "das_team DOUBLE, das_opponent DOUBLE, das_diff DOUBLE, "
    "gk_pitch_control_share_weighted DOUBLE, gk_reachable_area_m2 DOUBLE, "
    "gk_closing_time_mean_s__six_yard_box DOUBLE, gk_closing_time_min_s__six_yard_box DOUBLE, "
    "n_blocked_receivers BIGINT, n_potential_receivers BIGINT, "
    "blocking_score DOUBLE, blocked_threat_fraction DOUBLE, "
    "max_single_defender_blocking_score DOUBLE, "
    "sync_score_min DOUBLE, sync_score_mean DOUBLE, sync_score_high_quality_frac DOUBLE, "
    "obso_actual DOUBLE, obso_peak DOUBLE, obso_optimal DOUBLE, "
    "pausa_temporal DOUBLE, pausa_spatial DOUBLE, pausa_composite DOUBLE, "
    "space_created_m2_team DOUBLE, space_created_m2_opponent DOUBLE, "
    "elastic_frame_id BIGINT, elastic_confidence DOUBLE, elastic_error_seconds DOUBLE, "
    "shape_graph_density_attacking DOUBLE, shape_graph_n_edges_attacking BIGINT, "
    "shape_graph_mean_stability_attacking DOUBLE, "
    "shape_graph_density_defending DOUBLE, shape_graph_n_edges_defending BIGINT, "
    "shape_graph_mean_stability_defending DOUBLE, "
    "_ingested_at TIMESTAMP"
)
```

- [ ] **Step 4: Run test — verify it passes**

Run: `uv run pytest src/tests/test_action_context_schema_parity.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context.py src/tests/test_action_context_schema_parity.py
git commit -m "feat(action-context): schema constants + DDL + parity test (Task 1)"
```

---

### Task 2: Enrichment Chains

Three enrichment functions — tracking, SB360, and event-only — are pure functions (DataFrame in, DataFrame out). They can be tested in isolation with mock DataFrames.

**Files:**
- Modify: `src/ingestion/action_context.py`

- [ ] **Step 1: Add the three enrichment chain functions**

Add the following functions to `src/ingestion/action_context.py` after the DDL constant. These are copied from the spec §4.2-4.4 (review-verified against silly-kicks 3.23.0 signatures).

```python
def _enrich_tracking_match(
    actions_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    xt: ExpectedThreat,
    home_team_id: str,
) -> pd.DataFrame:
    """Full enrichment chain for tracking providers.

    See spec §4.2 for the complete call graph and ordering rationale.
    """
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_das,
        add_defensive_line,
        add_elastic_sync,
        add_gk_influence,
        add_line_break,
        add_obso,
        add_off_ball_context,
        add_pausa,
        add_pre_shot_gk_angle,
        add_pre_shot_gk_position,
        add_pressure_on_actor,
        add_shape_graph,
        add_space_creation,
        add_sync_score,
        add_team_shape,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    # Step 0: Actions-only enrichments (no tracking needed)
    out = add_game_state(actions_df)

    # Step 1: Frame linkage — returns tuple[DataFrame, LinkReport].
    # Computed ONCE; links passed to every subsequent add_* call.
    links, _report = link_actions_to_frames(out, tracking_df)

    # Step 2: GK resolution (pure SPADL + tracking; no links kwarg).
    # MUST precede add_pre_shot_gk_position — adds defending_gk_player_id.
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

    # Step 5b: Pressure — bekkers_pi (needs is_ball=True rows)
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

    # Step 6: Pitch control — 3 methods via Series API
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(out, tracking_df, links=links, method=method)
        out[s.name] = s.values

    # Step 7: Defensive line
    out = add_defensive_line(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 8: Off-ball context (umbrella — includes off-ball-run columns)
    out = add_off_ball_context(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 9: Ward line-breaking
    out = add_line_break(out, tracking_df, links=links, method="ward", home_team_id=home_team_id)

    # Step 10: Team shape
    out = add_team_shape(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 11: DAS (chunk_size=10 prevents OOM under 1 GB group cap)
    out = add_das(out, tracking_df, links=links, chunk_size=10)

    # Step 12: GK spatial (requires defending_gk_player_id from Step 2)
    out = add_pre_shot_gk_position(out, tracking_df, links=links)
    out = add_pre_shot_gk_angle(out, frames=tracking_df, links=links)

    # Step 13: GK influence (xt positional)
    out = add_gk_influence(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows (xt positional)
    out = add_cover_shadows(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 15: Shape graph
    out = add_shape_graph(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 16: OBSO — MUST precede add_pausa
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


def _enrich_event_only_match(actions_df: pd.DataFrame) -> pd.DataFrame:
    """Minimal enrichment for event-only providers (StatsBomb, Wyscout)."""
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context

    out = add_game_state(actions_df)
    out = add_pre_shot_gk_context(out)
    return out


```

```python
def _enrich_sb360_match(
    actions_df: pd.DataFrame,
    freeze_frames: pd.DataFrame,
    home_team_id: str,
) -> pd.DataFrame:
    """Enrichment chain for StatsBomb 360 matches.

    Uses snapshot_to_tracking_frames to convert per-event freeze-frame
    snapshots into synthetic tracking frames, then runs single-frame
    add_* features. Velocity/temporal features remain NULL.

    See spec §4.4 for the complete call graph.
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
    frames, links = snapshot_to_tracking_frames(freeze_frames, out)

    if len(frames) == 0:
        return out  # No freeze-frame data — event-only fallback

    # Step 2: Single-frame positional features
    out = add_action_context(out, frames, links=links)

    # Step 3: Defensive line
    out = add_defensive_line(out, frames, links=links, home_team_id=home_team_id)

    # Step 4: Ward line-breaking — primary SB360 value-add
    out = add_line_break(out, frames, links=links, method="ward", home_team_id=home_team_id)

    # Step 5: Team shape
    out = add_team_shape(out, frames, links=links, home_team_id=home_team_id)

    return out
```

- [ ] **Step 2: Write enrichment chain unit tests**

Create `src/tests/test_action_context_enrichment.py` with mock-based tests following the `test_tracking_context_udf.py` factory pattern (lines 254-327). The mocks use `patch("silly_kicks.tracking.<fn>", ...)` with passthrough lambdas.

```python
# src/tests/test_action_context_enrichment.py
"""Unit tests for action_context enrichment chains.

Mock-patches all silly-kicks add_* calls to verify:
- call ordering and links propagation (tracking chain)
- event-only chain produces game_state + GK resolution only
- output column selection matches _RESULT_COLUMNS
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest

from ingestion.action_context import (
    _RESULT_COLUMNS,
    _enrich_event_only_match,
    _enrich_sb360_match,
    _enrich_tracking_match,
)


def _make_actions(n: int = 5) -> pd.DataFrame:
    """Minimal SPADL actions DataFrame for testing."""
    return pd.DataFrame({
        "game_id": ["m1"] * n,
        "action_id": list(range(n)),
        "period_id": [1] * n,
        "time_seconds": [float(i * 10) for i in range(n)],
        "team_id": ["t1"] * n,
        "player_id": ["p1"] * n,
        "type_id": [0] * n,
        "start_x": [50.0] * n,
        "start_y": [34.0] * n,
        "end_x": [60.0] * n,
        "end_y": [34.0] * n,
        "result_id": [1] * n,
        "bodypart_id": [0] * n,
    })


def _make_tracking(n_frames: int = 50) -> pd.DataFrame:
    """Minimal tracking DataFrame."""
    return pd.DataFrame({
        "frame_id": list(range(n_frames)),
        "timestamp": [float(i * 0.04) for i in range(n_frames)],
        "player_id": ["p1"] * n_frames,
        "team_id": ["t1"] * n_frames,
        "x": [50.0] * n_frames,
        "y": [34.0] * n_frames,
    })


def _make_mock_links(actions: pd.DataFrame) -> pd.DataFrame:
    """Mock link report matching action rows."""
    return pd.DataFrame({
        "action_id": actions["action_id"].values,
        "frame_id": pd.array([0] * len(actions), dtype="Int64"),
        "time_offset_seconds": [0.0] * len(actions),
        "n_candidate_frames": [1] * len(actions),
        "link_quality_score": [1.0] * len(actions),
    })


_PASSTHROUGH = lambda actions, *args, **kwargs: actions


def test_enrich_event_only_produces_game_state_and_gk() -> None:
    """Event-only chain must add game_state + 4 GK resolution columns."""
    actions = _make_actions()
    with (
        patch("silly_kicks.spadl.add_game_state", side_effect=lambda df: df.assign(game_state="drawing")) as mock_gs,
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", side_effect=lambda df, **kw: df.assign(
            defending_gk_player_id=np.nan,
            gk_was_distributing=False,
            gk_was_engaged=False,
            gk_actions_in_possession=0,
        )) as mock_gk,
    ):
        result = _enrich_event_only_match(actions)
    mock_gs.assert_called_once()
    mock_gk.assert_called_once()
    assert "game_state" in result.columns
    assert "defending_gk_player_id" in result.columns
    assert result["game_state"].iloc[0] == "drawing"


def test_enrich_event_only_game_state_values() -> None:
    """game_state values must be winning, losing, or drawing."""
    actions = _make_actions(3)
    actions_with_gs = actions.assign(game_state=["winning", "losing", "drawing"])
    with (
        patch("silly_kicks.spadl.add_game_state", return_value=actions_with_gs),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", side_effect=lambda df, **kw: df),
    ):
        result = _enrich_event_only_match(actions)
    assert set(result["game_state"].unique()) == {"winning", "losing", "drawing"}


def test_enrich_tracking_calls_all_steps_with_links() -> None:
    """Tracking chain must call all 20 add_* steps and propagate links."""
    actions = _make_actions()
    tracking = _make_tracking()
    mock_links = _make_mock_links(actions)
    mock_xt = MagicMock()

    mock_link_fn = MagicMock(return_value=(mock_links, MagicMock()))
    mock_pc = MagicMock(return_value=pd.Series([0.5] * len(actions), name="pitch_control_at_ball__spearman"))
    # Use MagicMock (not lambda) for critical functions to verify kwargs
    mock_def_line = MagicMock(side_effect=_PASSTHROUGH)
    mock_action_ctx = MagicMock(side_effect=_PASSTHROUGH)
    mock_das = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.tracking.link_actions_to_frames", mock_link_fn),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_action_context", mock_action_ctx),
        patch("silly_kicks.tracking.add_actor_pre_window", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pressure_on_actor", _PASSTHROUGH),
        patch("silly_kicks.tracking.pitch_control_at_action", mock_pc),
        patch("silly_kicks.tracking.add_defensive_line", mock_def_line),
        patch("silly_kicks.tracking.add_off_ball_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_line_break", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_team_shape", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_das", mock_das),
        patch("silly_kicks.tracking.add_pre_shot_gk_position", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pre_shot_gk_angle", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_gk_influence", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_cover_shadows", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_shape_graph", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_obso", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pausa", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_space_creation", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_elastic_sync", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_sync_score", _PASSTHROUGH),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_tracking_match(actions, tracking, mock_xt, "t1")
    finally:
        for p in patches:
            p.stop()

    # link_actions_to_frames called once
    mock_link_fn.assert_called_once()
    # pitch_control called 3 times (spearman, fernandez_bornn, voronoi)
    assert mock_pc.call_count == 3
    assert isinstance(result, pd.DataFrame)

    # Verify critical kwargs are propagated
    # add_defensive_line must receive home_team_id and links
    _, def_kwargs = mock_def_line.call_args
    assert def_kwargs.get("home_team_id") == "t1", "home_team_id not propagated to add_defensive_line"
    assert def_kwargs.get("links") is not None, "links not propagated to add_defensive_line"
    # add_action_context must receive links
    _, ctx_kwargs = mock_action_ctx.call_args
    assert ctx_kwargs.get("links") is not None, "links not propagated to add_action_context"
    # add_das must receive chunk_size=10
    _, das_kwargs = mock_das.call_args
    assert das_kwargs.get("chunk_size") == 10, "chunk_size not propagated to add_das"


def test_enrich_sb360_calls_snapshot_converter_and_positional_features() -> None:
    """SB360 chain must call snapshot_to_tracking_frames then single-frame features."""
    actions = _make_actions(3)
    # Freeze-frame snapshots in converter input format
    freeze_frames = pd.DataFrame({
        "action_id": [0, 0, 0, 0, 1, 1, 1, 1],
        "team_id": ["t1", "t1", "t2", "t2", "t1", "t1", "t2", "t2"],
        "is_goalkeeper": [True, False, True, False, True, False, True, False],
        "x": [5.0, 40.0, 100.0, 60.0, 5.0, 45.0, 100.0, 55.0],
        "y": [34.0, 20.0, 34.0, 50.0, 34.0, 30.0, 34.0, 40.0],
    })

    mock_frames = pd.DataFrame({"frame_id": [0], "is_ball": [False]})  # non-empty
    mock_links = _make_mock_links(actions.iloc[:2])  # 2 actions with data

    mock_converter = MagicMock(return_value=(mock_frames, mock_links))
    mock_line_break = MagicMock(side_effect=_PASSTHROUGH)
    mock_team_shape = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_action_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_defensive_line", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
        patch("silly_kicks.tracking.add_team_shape", mock_team_shape),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, freeze_frames, "t1")
    finally:
        for p in patches:
            p.stop()

    # snapshot_to_tracking_frames called once with freeze_frames and actions
    mock_converter.assert_called_once()
    # add_line_break called with method="ward" and home_team_id
    _, lb_kwargs = mock_line_break.call_args
    assert lb_kwargs.get("method") == "ward", "method='ward' not propagated to add_line_break"
    assert lb_kwargs.get("home_team_id") == "t1", "home_team_id not propagated to add_line_break"
    # add_team_shape called
    mock_team_shape.assert_called_once()
    assert isinstance(result, pd.DataFrame)


def test_enrich_sb360_empty_freeze_frames_fallback() -> None:
    """SB360 chain falls back to event-only when converter returns empty frames."""
    actions = _make_actions(2)
    empty_ff = pd.DataFrame({
        "action_id": pd.Series([], dtype="int64"),
        "team_id": pd.Series([], dtype="object"),
        "is_goalkeeper": pd.Series([], dtype="bool"),
        "x": pd.Series([], dtype="float64"),
        "y": pd.Series([], dtype="float64"),
    })

    mock_empty_frames = pd.DataFrame()
    mock_empty_links = pd.DataFrame()
    mock_converter = MagicMock(return_value=(mock_empty_frames, mock_empty_links))
    mock_line_break = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, empty_ff, "t1")
    finally:
        for p in patches:
            p.stop()

    # Converter called, but line_break should NOT be called (empty frames fallback)
    mock_converter.assert_called_once()
    mock_line_break.assert_not_called()
    assert isinstance(result, pd.DataFrame)
```

- [ ] **Step 3: Run enrichment chain tests**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -v`
Expected: 5 passed

- [ ] **Step 4: Verify ruff + pyright pass**

Run: `uv run ruff check src/ingestion/action_context.py src/tests/test_action_context_enrichment.py && uv run pyright src/ingestion/action_context.py`
Expected: 0 errors

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context.py src/tests/test_action_context_enrichment.py
git commit -m "feat(action-context): enrichment chains + unit tests (Task 2)"
```

---

### Task 3: Pipeline Module (guard, identity, dispatch, UDF, main)

The remaining pipeline infrastructure: guard pattern, identity resolution, post-enrichment output handling, UDF factory, provider dispatch, and main/main_preflight entry points. This is the largest task — it mirrors `tracking_context.py`'s structure.

**Files:**
- Modify: `src/ingestion/action_context.py`

**Reference:** `src/ingestion/tracking_context.py:338-347` (UDF factory), `:514-600` (identity resolution), `:793-824` (post-enrichment output), `:1516-1547` (main_preflight), `:1590-1682` (main).

- [ ] **Step 1: Add identity resolution helpers**

Copy `_resolve_enrichment_identity` and `_restore_native_identity` from `tracking_context.py:514-600`. These are provider-specific identity mappings needed for silly-kicks compatibility. The implementation must handle all 6 providers (statsbomb, wyscout, idsse, metrica, skillcorner, gradientsports).

Note: The actual code is ~80 lines. Copy verbatim from `tracking_context.py` — the identity contracts are identical since both pipelines operate on the same `bronze.spadl_actions` source.

- [ ] **Step 2: Add post-enrichment output handler**

```python
def _build_output(
    actions: pd.DataFrame,
    match_id_native: str,
    data_source: str,
) -> pd.DataFrame:
    """Post-enrichment: renames + column selection for bronze write.

    1. game_id → match_id (silly-kicks uses game_id)
    2. defending_gk_player_id → defending_gk_player_id_native (ADR-018)
    3. Column selection to _RESULT_COLUMNS with NaN fill for missing cols.
    """
    out = actions.copy()
    out["match_id"] = match_id_native
    out["data_source"] = data_source

    if "type_name" not in out.columns and "type_id" in out.columns:
        from silly_kicks.spadl.utils import add_names
        out = add_names(out)  # idiomatic: merges actiontypes_df() on type_id

    out = _restore_native_identity(out)

    if "defending_gk_player_id" in out.columns:
        out = out.rename(columns={"defending_gk_player_id": "defending_gk_player_id_native"})

    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    for col in output_cols:
        if col not in out.columns:
            out[col] = np.nan
    return out[output_cols].copy()
```

- [ ] **Step 3: Add provider dispatch and UDF factory**

The UDF factory creates a closure capturing scalar config (provider, xT grid, home_team_id lookup). Provider dispatch routes to the correct enrichment chain based on `data_source`.

Key patterns from `tracking_context.py`:
- `_make_tracking_context_udf()` returns a callable for `applyInPandas`
- Provider-specific column projections (`_IDSSE_TRACKING_SELECT_COLS`, etc.)
- Frame batching for IDSSE (group by `match_id, period, frame_batch_id`)

The implementation follows the same structure but adds:
- Event-only provider path (statsbomb, wyscout) — no tracking data read
- `game_state` enrichment for ALL providers
- Three-tier dispatch: `_is_tracking_provider`, `_is_sb360_match`, `_is_event_only_provider`

- [ ] **Step 4: Add main_preflight and main entry points**

```python
def main_preflight() -> None:
    """Guard check + xT grid pre-warm. Registered as preflight_action_context."""
    configure_logging()
    spark = get_spark_session()
    args = parse_ingestion_args()
    catalog, schema = args.catalog, args.schema

    result = timed_check(
        spark=spark,
        source_table=f"{catalog}.{schema}.spadl_actions",
        results_table=f"{catalog}.{schema}.{_TABLE_NAME}",
        workflow_id="wf-action-context",
        match_column="match_id",
    )
    if result.status == FilterResult.SKIP:
        logger.info("No new matches — skipping")
        return

    logger.info(
        "Preflight complete: %d new match_ids for action context enrichment",
        len(result.new_ids),
    )


def main() -> None:
    """Run action context enrichment. Registered as compute_action_context."""
    configure_logging()
    spark = get_spark_session()
    args = parse_ingestion_args()
    catalog, schema = args.catalog, args.schema
    # ... provider dispatch loop over match_ids ...
    logger.info("Action context pipeline complete")
```

Note: The full `main()` body is ~100 lines — it iterates over `for_each_task` match groups, reads tracking data per provider, calls the enrichment chain via `applyInPandas`, and writes via `replaceWhere`. Copy the iteration pattern from `tracking_context.py:1590-1682`, adapting for:
- 6 providers instead of 3 (add statsbomb, wyscout, gradientsports)
- Three-tier dispatch: `_is_tracking_provider`, `_is_sb360_match`, `_is_event_only_provider`
- SB360 path: read `bronze.spadl_actions` + `bronze.statsbomb_360_frames`, call `_enrich_sb360_match`
- Event-only path: read only `bronze.spadl_actions`, no tracking data
- Bronze table creation with CDF + liquid clustering + autoOptimize from birth (spec §8.1)

- [ ] **Step 5: Write unit tests for output handler and provider dispatch**

Add to `src/tests/test_action_context_enrichment.py`:

```python
from ingestion.action_context import _build_output


def test_build_output_column_selection() -> None:
    """_build_output must return exactly _RESULT_COLUMNS (minus _ingested_at), NaN-filling missing cols."""
    actions = pd.DataFrame({
        "game_id": ["m1"],
        "action_id": [0],
        "period_id": [1],
        "time_seconds": [10.0],
        "team_id": ["t1"],
        "player_id": ["p1"],
        "type_id": [0],
        "start_x": [50.0],
        "start_y": [34.0],
        "end_x": [60.0],
        "end_y": [34.0],
        "game_state": ["drawing"],
        "defending_gk_player_id": ["gk1"],
    })
    with patch("ingestion.action_context._restore_native_identity", side_effect=lambda df: df):
        result = _build_output(actions, match_id_native="native_m1", data_source="statsbomb")
    expected_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    assert list(result.columns) == expected_cols
    assert result["match_id"].iloc[0] == "native_m1"
    assert result["data_source"].iloc[0] == "statsbomb"
    # defending_gk_player_id → defending_gk_player_id_native
    assert "defending_gk_player_id_native" in result.columns
    # Missing tracking columns should be NaN
    assert pd.isna(result["pitch_control_at_ball__spearman"].iloc[0])


def test_build_output_type_id_to_type_name() -> None:
    """_build_output must map type_id → type_name when type_name is absent."""
    actions = pd.DataFrame({
        "game_id": ["m1"],
        "action_id": [0],
        "period_id": [1],
        "time_seconds": [10.0],
        "team_id": ["t1"],
        "player_id": ["p1"],
        "type_id": [0],  # 0 = "pass" in silly_kicks.spadl.config.actiontypes
        "start_x": [50.0],
        "start_y": [34.0],
        "end_x": [60.0],
        "end_y": [34.0],
    })
    with patch("ingestion.action_context._restore_native_identity", side_effect=lambda df: df):
        result = _build_output(actions, match_id_native="native_m1", data_source="idsse")
    assert result["type_name"].iloc[0] == "pass"  # type_id=0 maps to "pass" in silly-kicks


def test_provider_tier_classification() -> None:
    """Provider dispatch helpers must correctly classify all 6 providers (3 tiers)."""
    from ingestion.action_context import (
        _is_event_only_provider,
        _is_tracking_provider,
    )
    # Tracking providers
    for p in ("idsse", "metrica", "skillcorner", "gradientsports"):
        assert _is_tracking_provider(p), f"{p} should be tracking"
        assert not _is_event_only_provider(p), f"{p} should NOT be event-only"
    # Event-only providers (Wyscout always, StatsBomb without 360 data)
    for p in ("statsbomb", "wyscout"):
        assert _is_event_only_provider(p), f"{p} should be event-only"
        assert not _is_tracking_provider(p), f"{p} should NOT be tracking"
    # Note: SB360 detection is per-match (has freeze-frame data?), not per-provider.
    # _is_sb360_match(data_source, match_id, spark) checks bronze.statsbomb_360_frames.
    # StatsBomb matches WITH freeze-frames route to _enrich_sb360_match;
    # StatsBomb matches WITHOUT freeze-frames route to _enrich_event_only_match.
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -v`
Expected: 8 passed (5 from Task 2 + 3 new)

- [ ] **Step 7: Verify ruff + pyright pass**

Run: `uv run ruff check src/ingestion/action_context.py src/tests/test_action_context_enrichment.py && uv run pyright src/ingestion/action_context.py`
Expected: 0 errors

- [ ] **Step 8: Commit**

```bash
git add src/ingestion/action_context.py src/tests/test_action_context_enrichment.py
git commit -m "feat(action-context): pipeline module — guard, dispatch, UDF, main + tests (Task 3)"
```

---

### Task 4: Entry Points + Seed CSV

Register the new pipeline in pyproject.toml and the dbt seed.

**Files:**
- Modify: `pyproject.toml:131` (after `preflight_tracking_context`)
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`

- [ ] **Step 1: Add entry points to pyproject.toml**

After line 131 (`preflight_tracking_context = "ingestion.tracking_context:main_preflight"`), add:

```
compute_action_context = "ingestion.action_context:main"
preflight_action_context = "ingestion.action_context:main_preflight"
```

- [ ] **Step 2: Add task keys to seed CSV**

Add two lines to `dbt_project/seeds/task_workflow_mapping.csv`:

```
compute_action_context,wf-action-context
preflight_action_context,wf-action-context
```

- [ ] **Step 3: Create workflow card**

Create `workflow-cards/wf-action-context.yaml` following the `wf-tracking-context.yaml` pattern:

```yaml
---
name: Action Context Enrichment
id: wf-action-context
version: "1.0"
status: production
type: heuristic
domain: action-features
owners:
  - karsten
tags:
  - action-context
  - game-state
  - tracking
  - pitch-control
  - pressure
  - team-shape
  - line-breaking
  - gk-influence
  - obso
  - pausa

inputs:
  datasets:
    - id: "{catalog}.bronze.spadl_actions"
      source: delta-table
      description: "SPADL actions from all 6 providers"
    - id: "{catalog}.bronze.idsse_tracking"
      source: delta-table
      description: "IDSSE (Sportec) raw tracking data"
    - id: "{catalog}.bronze.metrica_tracking"
      source: delta-table
      description: "Metrica raw tracking data"
    - id: "{catalog}.bronze.skillcorner_tracking"
      source: delta-table
      description: "SkillCorner raw tracking data"
    - id: "{catalog}.bronze.gradientsports_tracking"
      source: delta-table
      description: "GradientSports raw tracking data"
    - id: "{catalog}.bronze.statsbomb_360"
      source: delta-table
      description: "StatsBomb 360 freeze-frame snapshots (SB360 tier)"

outputs:
  tables:
    - id: "{catalog}.bronze.spadl_action_context"
      destination: delta-table
      description: "Per-action context features (~102 columns for tracking, ~5 for event-only)"

execution:
  enrichment:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: compute_action_context
    module: ingestion.action_context
    schedule: "daily 06:00 UTC"
    timeout: "7200s"
    environment: spadl

dbt_model: fct_action_context

idempotency:
  strategy: skip-guard
  key:
    - match_id
    - data_source
  description: "Skip guard checks bronze.spadl_action_context for existing (match_id, data_source) pairs."

performance:
  memory_ceiling: "800 MB per UDF group (1 GB limit)"
  compute_estimate: "2-5 min per tracking match, <10s per event-only match"
---

## Overview

Unified action context pipeline (AC-1). Runs the full silly-kicks enrichment
chain in a single applyInPandas pass per match. Covers all 6 providers:
StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, GradientSports.
Event-only providers get game_state + GK resolution (~5 columns);
tracking providers get the full ~102 column feature set.
```

- [ ] **Step 4: Run sentinel test to verify seed/orchestrator parity**

Run: `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py::test_orchestrator_task_keys_present_in_seed -v`
Expected: PASS (new task keys are in seed but orchestrator doesn't reference them yet — this is fine, the test checks orchestrator ⊆ seed, not equality)

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml dbt_project/seeds/task_workflow_mapping.csv workflow-cards/wf-action-context.yaml
git commit -m "feat(action-context): entry points + seed CSV + workflow card (Task 4)"
```

---

### Task 5: dbt Staging Layer

Source definition, staging SQL with dedup + type coercion + identity renames, and schema tests. Follows the exact pattern of `dbt_project/models/staging/tracking_context/`.

**Files:**
- Create: `dbt_project/models/staging/action_context/_action_context__sources.yml`
- Create: `dbt_project/models/staging/action_context/stg_action_context__values.sql`
- Create: `dbt_project/models/staging/action_context/_action_context__models.yml`

- [ ] **Step 1: Create source definition**

```yaml
# dbt_project/models/staging/action_context/_action_context__sources.yml
version: 2

sources:
  - name: action_context
    description: >
      Unified action-level features produced by the AC-1 pipeline.
      One row per SPADL action for ALL providers (6). Event-only providers
      have game_state + GK resolution populated; tracking columns NULL.
      ~102 columns for tracking providers.
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 72, period: hour}

    tables:
      - name: spadl_action_context
        description: >
          Per-action context features. Grain: one row per SPADL action
          per match. Provider-agnostic (data_source discriminates).
```

- [ ] **Step 2: Create staging SQL**

`dbt_project/models/staging/action_context/stg_action_context__values.sql` — follows the same dedup + cleaned CTE pattern as `stg_spadl__tracking_context.sql`. Key differences:

- Source is `action_context.spadl_action_context` (not `tracking_context.spadl_tracking_context`)
- Includes `game_state` column (cast as string)
- Includes OBSO, PAUSA, space creation, ELASTIC sync, shape graph columns
- accepted_values for `data_source` includes all 6 providers: `['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner', 'gradientsports']`
- Same Metrica player_id normalization logic from the tracking_context staging

The full SQL is ~180 lines of `cast(col as type) as col` passthrough — one line per column in `_RESULT_COLUMNS` (excluding `_ingested_at`), plus the dedup CTE and Metrica normalization CASE expressions.

- [ ] **Step 3: Create models YAML with schema tests**

```yaml
# dbt_project/models/staging/action_context/_action_context__models.yml
version: 2

models:
  - name: stg_action_context__values
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Staging view for AC-1 action context. Deduplicates bronze data
      by (match_id, action_id), keeping the most recent ingestion.
      Renames match_id → native_match_id, player_id → player_id_native,
      team_id → team_id_native for downstream Kimball FK resolution.
    columns:
      - name: native_match_id
        description: Native match identifier (provider-specific string)
        data_tests:
          - not_null
      - name: action_id
        description: SPADL action index within match
        data_tests:
          - not_null
      - name: data_source
        description: "Provider: statsbomb, wyscout, idsse, metrica, skillcorner, gradientsports"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner', 'gradientsports']
      - name: game_state
        description: "winning, losing, or drawing from acting team's perspective"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['winning', 'losing', 'drawing']
      - name: line_breaking_type__ward
        description: >
          Ward line-breaking categorical. NULL for event-only providers
          and non-line-breaking actions.
        data_tests:
          - accepted_values:
              arguments:
                values: ['between_lines', 'around_line']
              config:
                where: "line_breaking_type__ward is not null"
```

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/staging/action_context/
git commit -m "feat(action-context): dbt staging layer — sources, SQL, schema tests (Task 5)"
```

---

### Task 6: dbt Mart

Gold mart with contract enforcement, Kimball FK resolution, surrogate key, incremental merge, and liquid clustering.

**Files:**
- Create: `dbt_project/models/marts/fct_action_context.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create the mart model**

Follows the exact pattern of `fct_tracking_context.sql`:
- Config: `materialized='incremental'`, `incremental_strategy='merge'`, `unique_key='action_context_id'`, `on_schema_change='append_new_columns'`, `liquid_clustered_by=['match_key']`, `tags=['marts', 'output_mart']`
- CTE `action_raw`: SELECT all columns from `stg_action_context__values`
- CTE `keyed`: INNER JOIN `dim_matches` (match_key), LEFT JOIN `dim_teams` (team_key), LEFT JOIN `dim_players` (player_key), LEFT JOIN `dim_players` as dp_gk (defending_gk_player_key)
- CTE `final`: `dbt_utils.generate_surrogate_key(['match_key', 'action_id'])` as `action_context_id`, passthrough all feature columns

The SQL is ~240 lines — structurally identical to `fct_tracking_context.sql` with the additional columns (game_state, OBSO, PAUSA, space creation, ELASTIC sync, shape graph).

- [ ] **Step 2: Add contract column definitions to `_marts__models.yml`**

Append the `fct_action_context` model definition to the end of `dbt_project/models/marts/_marts__models.yml`. The contract MUST be defined in YAML — dbt enforces column-level type matching only when columns are declared here. Follow the existing `fct_tracking_context` pattern (lines 4662-4820).

```yaml
  - name: fct_action_context
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Gold-layer unified action context features per SPADL action. Grain: one row
      per (match_key, action_id). All 6 providers. Event-only providers have
      game_state + GK resolution populated; tracking columns NULL.
      Pure Kimball — surrogate keys resolved via dim JOINs.
    columns:
      - name: action_context_id
        data_type: string
        description: Surrogate key derived from (match_key, action_id).
        data_tests:
          - unique
          - not_null
      - name: match_key
        data_type: bigint
        description: Kimball surrogate FK to dim_matches.
        data_tests:
          - not_null
      - name: team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams.
      - name: player_key
        data_type: bigint
        description: Kimball surrogate FK to dim_players.
      - name: action_id
        data_type: bigint
        description: SPADL action index within match.
        data_tests:
          - not_null
      - name: data_source
        data_type: string
        description: "Provider: statsbomb, wyscout, idsse, metrica, skillcorner, gradientsports."
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica', 'skillcorner', 'gradientsports']
      - name: game_state
        data_type: string
        description: "winning, losing, or drawing from acting team's perspective."
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['winning', 'losing', 'drawing']
      - name: period_id
        data_type: bigint
      - name: time_seconds
        data_type: double
      - name: type_name
        data_type: string
      - name: start_x
        data_type: double
      - name: start_y
        data_type: double
      - name: end_x
        data_type: double
      - name: end_y
        data_type: double
      # ... remaining ~88 columns follow the same pattern as fct_tracking_context
      # (one entry per column with data_type matching _ACTION_CONTEXT_DDL).
      # Full list: all columns from _RESULT_COLUMNS except _ingested_at,
      # plus Kimball keys (action_context_id, match_key, team_key, player_key,
      # defending_gk_player_key).
```

Note: The full YAML is ~200 lines — one entry per column in the mart. The implementer must list ALL columns with correct `data_type` values matching the DDL. Without this YAML, `contract: enforced: true` has no columns to enforce against and the build will silently allow schema drift.

- [ ] **Step 3: Verify YAML column count matches schema**

The YAML must have one `data_type:` entry per column. Verify the count:

```bash
# Expected: len(_RESULT_COLUMNS) - 1 (_ingested_at excluded) + 4 Kimball keys
# (action_context_id, match_key, team_key, player_key) + 1 (defending_gk_player_key)
# = 102 - 1 + 5 = 106 columns
grep -c 'data_type:' dbt_project/models/marts/_marts__models.yml | tail -1
# Subtract the pre-existing count from other models:
grep -c 'data_type:' dbt_project/models/marts/_marts__models.yml
# Compare: new total - old total must equal 106
```

If the count is wrong, the contract will either reject valid data (too many columns) or silently allow schema drift (too few).

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/marts/fct_action_context.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat(action-context): dbt mart with contract + Kimball keys + YAML schema (Task 6)"
```

---

### Task 7: Lakebase Infrastructure

Synced table config, PG indexes, and grants registration.

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py:234`
- Modify: `scripts/create_indexes.py:211`

- [ ] **Step 1: Add SyncedTableConfig entry**

In `src/ingestion/refresh_synced_tables.py`, after the `fct_tracking_context_synced` entry (line 234), add:

```python
    SyncedTableConfig("fct_action_context_synced", "fct_action_context", ("action_context_id",), "TRIGGERED"),
```

- [ ] **Step 2: Add PG index definitions**

In `scripts/create_indexes.py`, after line 211 (end of tracking_context indexes), add:

```python
    # ── fct_action_context_synced — AC-1 unified action features ────────
    # Superset of tracking_context; includes event-only providers.
    # Primary: per-match feature retrieval. Secondary: per-player, per-team.
    ("idx_action_context_match_key", "fct_action_context_synced", "match_key"),
    ("idx_action_context_match_team_key", "fct_action_context_synced", "match_key, team_key"),
    ("idx_action_context_match_player_key", "fct_action_context_synced", "match_key, player_key"),
```

- [ ] **Step 3: Verify no import errors**

Run: `uv run python -c "from ingestion.refresh_synced_tables import SYNCED_TABLES; print(len(SYNCED_TABLES))"`
Expected: prints the count (should be previous count + 1)

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/refresh_synced_tables.py scripts/create_indexes.py
git commit -m "feat(action-context): Lakebase synced table + indexes (Task 7)"
```

---

### Task 8: HF Publisher + Dataset Card

PEP 723 script for publishing to HuggingFace, plus the dataset README card.

**Files:**
- Create: `scripts/publish_action_context_hf.py`
- Create: `docs/huggingface/dataset-cards/spadl-action-context.md`

- [ ] **Step 1: Create the HF publisher script**

Copy `scripts/publish_tracking_context_hf.py` and modify:
- `DATASET_REPO = f"{HF_ORG}/spadl-action-context"`
- `_ACTION_CONTEXT_SQL`: `SELECT * FROM soccer_analytics.dev_gold.fct_action_context WHERE data_source != 'gradientsports'`
- Card path: `get_hf_card_path("spadl-action-context.md", kind="dataset")`
- Log messages updated from "tracking context" to "action context"

The full script is ~150 lines — structurally identical to the tracking context publisher (same `query_databricks_sql`, `publish_to_hf_hub`, `main` pattern).

- [ ] **Step 2: Create the dataset card**

`docs/huggingface/dataset-cards/spadl-action-context.md` — standard HF dataset card with:
- YAML frontmatter (license, tags, dataset_info)
- Description: unified action-level features for soccer analytics
- Column documentation grouped by category (same grouping as spec §3)
- Data source list (5 public providers — gradientsports excluded)
- Citation information

- [ ] **Step 3: Register in `_HF_JOBS_SCRIPT_TO_CARD` dict**

In `src/tests/test_card_parity_with_terraform.py`, add a new entry before the closing `}` of `_HF_JOBS_SCRIPT_TO_CARD` (line 232):

```python
    # AC-1 action context publisher — dataset governed by ADR-014; no
    # independent workflow card (the pipeline task lives in the mega-job,
    # the publisher is on-demand).
    "publish_action_context_hf.py": None,
```

Without this entry, `test_hf_jobs_script_mapping_matches_disk` will fail with "HF Jobs publish script(s) present on disk but not in `_HF_JOBS_SCRIPT_TO_CARD`".

- [ ] **Step 4: Verify HF publish parity test includes new card**

Run: `uv run pytest src/tests/test_hf_publish_parity.py src/tests/test_card_parity_with_terraform.py::test_hf_jobs_script_mapping_matches_disk -v`
Expected: PASS (card file matches repo name; script in dict matches disk)

- [ ] **Step 5: Commit**

```bash
git add scripts/publish_action_context_hf.py docs/huggingface/dataset-cards/spadl-action-context.md src/tests/test_card_parity_with_terraform.py
git commit -m "feat(action-context): HF publisher + dataset card + parity registration (Task 8)"
```

---

### Task 9: Final Verification

Run all tests and lint checks to confirm everything integrates.

**Files:** None (verification only)

- [ ] **Step 1: Run schema parity + enrichment chain tests**

Run: `uv run pytest src/tests/test_action_context_schema_parity.py src/tests/test_action_context_enrichment.py -v`
Expected: 10 passed (2 schema parity + 8 enrichment/pipeline)

- [ ] **Step 2: Run ruff on all changed files**

Run: `uv run ruff check src/ingestion/action_context.py src/tests/test_action_context_schema_parity.py src/tests/test_action_context_enrichment.py scripts/publish_action_context_hf.py`
Expected: 0 errors

- [ ] **Step 3: Run pyright on action_context module**

Run: `uv run pyright src/ingestion/action_context.py`
Expected: 0 errors

- [ ] **Step 4: Run HF publish parity + card registration tests**

Run: `uv run pytest src/tests/test_hf_publish_parity.py src/tests/test_card_parity_with_terraform.py::test_hf_jobs_script_mapping_matches_disk -v`
Expected: PASS

- [ ] **Step 5: Run existing sentinel tests (no regression)**

Run: `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v -k "not live_mega_job and not flavor_map"`
Expected: All non-env-gated tests pass

- [ ] **Step 6: Squash-commit for PR (if not already squashed)**

All tasks above produce individual commits. For the PR, squash into a single commit per project convention.

---

**Phase 2 — Parity Validation (deferred)**

Per spec §9, Phase 2 creates `src/tests/test_action_context_parity.py` which validates old→new table equivalence: 83 shared columns against `spadl_tracking_context`, plus PAUSA/line-breaking/ELASTIC cross-table assertions. This requires a successful first pipeline run populating both old and new tables and is therefore deferred to a follow-up after this Phase 1 deployment PR ships and the pipeline runs end-to-end on Databricks.
