# TC-1 Tracking Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a unified action-coupled tracking features table (`fct_tracking_context`) that computes all 15 silly-kicks enrichments in a single `applyInPandas` pass per match across IDSSE, Metrica, and SkillCorner providers.

**Architecture:** Python ingestion module with provider-agnostic UDF → bronze Delta → dbt staging view → gold mart with Kimball FKs → Lakebase synced table → HF dataset publish. Same bronze+dbt pattern as `fct_line_breaking_results` but wider (83 columns, 15 enrichments).

**Tech Stack:** silly-kicks `>=3.11.2` (tracking features + kloppy + DAS), PySpark `applyInPandas`, dbt incremental mart, Lakebase synced table, HF dataset publish via PEP 723 script.

**Spec:** `docs/superpowers/specs/2026-05-11-tracking-context-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/ingestion/tracking_context.py` | Main module: skip guard, UDF factory, `run_pipeline()`, `main()` |
| `workflow-cards/wf-tracking-context.yaml` | Workflow card with inputs/outputs/execution config |
| `dbt_project/models/staging/tracking_context/_tracking_context__sources.yml` | Bronze source declaration |
| `dbt_project/models/staging/tracking_context/_tracking_context__models.yml` | Staging model YAML + tests |
| `dbt_project/models/staging/tracking_context/stg_spadl__tracking_context.sql` | Staging passthrough view |
| `dbt_project/models/marts/fct_tracking_context.sql` | Gold mart with Kimball FKs |
| `dbt_project/models/marts/_marts__models.yml` | Mart YAML entry (append) |
| `scripts/publish_tracking_context_hf.py` | PEP 723 HF dataset publisher |
| `docs/huggingface/dataset-cards/spadl-tracking-context.md` | HF dataset card |
| `src/tests/test_tracking_context_schema_parity.py` | DDL ↔ UDF output parity test |
| `src/tests/test_tracking_context_enrichment.py` | UDF enrichment chain integration test |

**Modified files:**

| File | Change |
|------|--------|
| `pyproject.toml` | Bump `spadl` extra to `silly-kicks[kloppy,das]>=3.11.2,<4`; add entry point |
| `src/ingestion/guards.py` | Add `"ingestion.tracking_context"` to `_GUARD_MODULES` |
| `src/tests/test_guard_conformance.py` | No change needed (auto-discovered from `_GUARD_MODULES`) |
| `src/tests/test_staging_coverage.py` | Add `tracking_context` to `PROVIDER_COVERAGE` |
| `src/tests/test_card_parity_with_terraform.py` | Add script→card mapping to `_HF_JOBS_SCRIPT_TO_CARD` |
| `scripts/create_indexes.py` | Add tracking_context indexes to `INDEXES` list |

---

## Task 1: Dependency + entry point changes

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Update `spadl` extra to include kloppy + DAS and bump pin**

```toml
# In [project.optional-dependencies], change:
spadl = [
    "silly-kicks>=3.11.0,<4",
]
# To:
spadl = [
    "silly-kicks[kloppy,das]>=3.11.2,<4",
]
```

- [ ] **Step 2: Add entry point for the new ingestion module**

In `[project.scripts]` section, add:

```toml
compute_tracking_context = "ingestion.tracking_context:main"
```

- [ ] **Step 3: Run `uv lock --upgrade-package silly-kicks && uv sync`**

Run: `uv lock --upgrade-package silly-kicks && uv sync`
Expected: Resolves silly-kicks 3.11.2 with kloppy + accessible-space extras.

- [ ] **Step 4: Verify installation**

Run: `uv run python -c "import silly_kicks; print(silly_kicks.__version__); from silly_kicks.tracking import add_action_context, add_pressure_on_actor; print('OK')"`
Expected: `3.11.2` and `OK`.

---

## Task 2: Bronze schema constant + UDF factory

**Files:**
- Create: `src/ingestion/tracking_context.py`
- Test: `src/tests/test_tracking_context_schema_parity.py`

- [ ] **Step 1: Write the schema parity test**

Create `src/tests/test_tracking_context_schema_parity.py`:

```python
"""TC-1 — Bronze DDL ↔ UDF output column-set parity test.

Same pattern as test_spadl_vaep_writer_parity.py: parse the DDL string,
compare columns against the StructType used by applyInPandas.
"""

from __future__ import annotations

import re

import pytest


_DDL_TYPE_TO_SPARK_NAME = {
    "STRING": "string",
    "BIGINT": "long",
    "INT": "integer",
    "DOUBLE": "double",
    "FLOAT": "float",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}


def _parse_ddl(ddl: str) -> dict[str, str]:
    """Return {col_name: spark_type_name} from a CREATE-TABLE-style DDL."""
    out: dict[str, str] = {}
    for raw in ddl.split(","):
        tok = raw.strip()
        if not tok:
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+([A-Z]+)\b", tok)
        if not m:
            raise AssertionError(f"unparseable DDL fragment: {tok!r}")
        col, ddl_type = m.group(1), m.group(2)
        if ddl_type not in _DDL_TYPE_TO_SPARK_NAME:
            raise AssertionError(f"unknown DDL type {ddl_type!r} for column {col!r}")
        out[col] = _DDL_TYPE_TO_SPARK_NAME[ddl_type]
    return out


class TestTrackingContextSchemaParity:
    """Bronze DDL constant must match UDF output schema."""

    def test_ddl_columns_match_result_columns(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS, _TRACKING_CONTEXT_DDL

        ddl_cols = set(_parse_ddl(_TRACKING_CONTEXT_DDL).keys())
        result_cols = set(_RESULT_COLUMNS)
        assert ddl_cols == result_cols, (
            f"DDL vs _RESULT_COLUMNS mismatch.\n"
            f"  In DDL only: {ddl_cols - result_cols}\n"
            f"  In _RESULT_COLUMNS only: {result_cols - ddl_cols}"
        )

    def test_ddl_has_no_duplicates(self) -> None:
        from ingestion.tracking_context import _TRACKING_CONTEXT_DDL

        cols = [tok.strip().split()[0] for tok in _TRACKING_CONTEXT_DDL.split(",") if tok.strip()]
        seen: set[str] = set()
        dupes = [c for c in cols if c in seen or seen.add(c)]  # type: ignore[func-returns-value]
        assert not dupes, f"Duplicate columns in DDL: {dupes}"

    def test_column_count(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS

        assert len(_RESULT_COLUMNS) == 83, f"Expected 83 columns, got {len(_RESULT_COLUMNS)}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_schema_parity.py -v`
Expected: FAIL — `ingestion.tracking_context` not found.

- [ ] **Step 3: Create the ingestion module with schema constants**

Create `src/ingestion/tracking_context.py`:

```python
"""TC-1 — Unified action-coupled tracking features pipeline.

Reads tracking data + SPADL actions from bronze, runs all silly-kicks
enrichments in a single applyInPandas pass per match, writes results to
bronze.spadl_tracking_context.

Providers: IDSSE (Sportec), Metrica, SkillCorner.
Architecture: "Read from bronze, compute, write to bronze."
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

_TABLE_NAME = "spadl_tracking_context"

# ── Column ordering ────────────────────────────────────────────────────
# Identity (12) + linkage (4) + features (66) + audit (1) = 83 columns.

_RESULT_COLUMNS: list[str] = [
    # Identity
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
    # Linkage provenance
    "frame_id",
    "time_offset_seconds",
    "link_quality_score",
    "n_candidate_frames",
    # GK resolution (event-based)
    "defending_gk_player_id",
    "gk_was_distributing",
    "gk_was_engaged",
    "gk_actions_in_possession",
    # GK spatial (shot-only, from add_pre_shot_gk_context with frames)
    "pre_shot_gk_x",
    "pre_shot_gk_y",
    "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot",
    "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    # Action context
    "nearest_defender_distance",
    "actor_speed",
    "receiver_zone_density",
    "defenders_in_triangle_to_goal",
    # Actor pre-window (TF-3)
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    # Pressure (TF-2, all 3 methods)
    "pressure_on_actor__andrienko_oval",
    "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    # Pitch control (3 methods)
    "pitch_control_at_ball__spearman",
    "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    # Defensive line
    "defensive_line_x",
    "back_line_high_x",
    "compactness_x",
    "lateral_width",
    "max_lateral_gap",
    "back_n_count",
    # Off-ball context (threshold line-break + runs)
    "line_break",
    "n_attackers_behind_line",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    # Ward line-breaking
    "line_break__ward",
    "lines_broken__ward",
    "line_breaking_type__ward",
    # Team shape (14: 7 metrics × 2 teams)
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
    # DAS (nullable)
    "das_team",
    "das_opponent",
    "das_diff",
    # GK influence
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    # Cover shadows
    "n_blocked_receivers",
    "n_potential_receivers",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    # Sync score
    "sync_score_min",
    "sync_score_mean",
    "sync_score_high_quality_frac",
    # Audit
    "_ingested_at",
]

_TRACKING_CONTEXT_DDL = (
    "data_source STRING, match_id STRING, action_id BIGINT, period_id BIGINT, "
    "time_seconds DOUBLE, team_id STRING, player_id STRING, type_name STRING, "
    "start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "frame_id BIGINT, time_offset_seconds DOUBLE, link_quality_score DOUBLE, "
    "n_candidate_frames BIGINT, "
    "defending_gk_player_id DOUBLE, gk_was_distributing BOOLEAN, "
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
    "_ingested_at TIMESTAMP"
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_schema_parity.py -v`
Expected: 3 tests PASS.

---

## Task 3: UDF enrichment chain

**Files:**
- Modify: `src/ingestion/tracking_context.py`
- Test: `src/tests/test_tracking_context_enrichment.py`

- [ ] **Step 1: Write the enrichment chain integration test**

Create `src/tests/test_tracking_context_enrichment.py`:

```python
"""TC-1 — UDF enrichment chain integration test.

Validates that _enrich_match produces the correct output column set
using synthetic data. Does NOT require Spark or Databricks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _make_synthetic_actions(n: int = 20) -> pd.DataFrame:
    """Minimal SPADL actions with game_id for silly-kicks compat."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "game_id": [1] * n,
        "action_id": list(range(n)),
        "period_id": [1] * n,
        "time_seconds": np.linspace(0, 90 * 60, n),
        "team_id": rng.choice([100, 200], n),
        "player_id": rng.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], n),
        "type_id": rng.choice([0, 1, 2, 3], n),
        "result_id": rng.choice([0, 1], n),
        "bodypart_id": [0] * n,
        "start_x": rng.uniform(0, 105, n),
        "start_y": rng.uniform(0, 68, n),
        "end_x": rng.uniform(0, 105, n),
        "end_y": rng.uniform(0, 68, n),
        "original_event_id": [f"evt_{i}" for i in range(n)],
    })


def _make_synthetic_frames(n_frames: int = 100) -> pd.DataFrame:
    """Minimal tracking frames in TRACKING_FRAMES_COLUMNS schema.

    Column names match silly_kicks.tracking.schema.TRACKING_FRAMES_COLUMNS:
    - is_goalkeeper (NOT is_gk)
    - time_seconds (NOT timestamp)
    - game_id must match actions' game_id
    - is_ball column (True for ball row, False for players)
    """
    rng = np.random.default_rng(42)
    rows = []
    for f in range(n_frames):
        t = f * 0.04  # 25 fps
        for p in range(1, 23):  # 22 players
            rows.append({
                "game_id": 1,  # Must match _make_synthetic_actions game_id
                "frame_id": f,
                "period_id": 1,
                "time_seconds": t,
                "player_id": p,
                "team_id": 100 if p <= 11 else 200,
                "x": rng.uniform(0, 105),
                "y": rng.uniform(0, 68),
                "vx": rng.uniform(-5, 5),
                "vy": rng.uniform(-5, 5),
                "is_goalkeeper": p in (1, 12),
                "is_ball": False,
            })
        # Ball row — is_ball=True, is_goalkeeper=False
        rows.append({
            "game_id": 1,
            "frame_id": f,
            "period_id": 1,
            "time_seconds": t,
            "player_id": None,
            "team_id": None,
            "x": rng.uniform(0, 105),
            "y": rng.uniform(0, 68),
            "vx": rng.uniform(-5, 5),
            "vy": rng.uniform(-5, 5),
            "is_goalkeeper": False,
            "is_ball": True,
        })
    df = pd.DataFrame(rows)
    # All required TRACKING_FRAMES_COLUMNS — link_actions_to_frames
    # hard-selects source_provider, so KeyError without it.
    df["source_provider"] = "sportec"
    df["is_goalkeeper_source"] = "native"
    df["frame_rate"] = 25.0
    df["z"] = np.nan
    df["speed"] = np.sqrt(df["vx"] ** 2 + df["vy"] ** 2)
    df["speed_source"] = "derived"
    df["ball_state"] = "alive"
    df["team_attacking_direction"] = None
    df["confidence"] = None
    df["visibility"] = None
    return df


class TestEnrichmentChain:
    """Verify _enrich_match output matches _RESULT_COLUMNS."""

    @pytest.fixture
    def actions(self) -> pd.DataFrame:
        return _make_synthetic_actions()

    @pytest.fixture
    def frames(self) -> pd.DataFrame:
        return _make_synthetic_frames()

    def test_output_columns_match_spec(self, actions: pd.DataFrame, frames: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        from ingestion.tracking_context import _RESULT_COLUMNS, _enrich_match

        # Minimal xT stub (12x16 grid of zeros)
        from silly_kicks.xthreat import ExpectedThreat

        xt = ExpectedThreat(l=16, w=12)
        # Fit on synthetic actions (won't converge, but produces valid grid)
        xt.fit(actions)

        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=xt,
            home_team_id=100,
            match_id_native="test_match_1",
            data_source="idsse",
        )

        expected = set(_RESULT_COLUMNS) - {"_ingested_at"}
        actual = set(result.columns)
        missing = expected - actual
        extra = actual - expected
        assert not missing, f"Missing columns: {missing}"
        assert not extra, f"Extra columns: {extra}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_enrichment.py -v`
Expected: FAIL — `_enrich_match` not found in `ingestion.tracking_context`.

- [ ] **Step 3: Implement `_enrich_match` function**

Append to `src/ingestion/tracking_context.py`:

```python
def _enrich_match(
    *,
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: object,
    home_team_id: int | str,
    match_id_native: str,
    data_source: str,
) -> pd.DataFrame:
    """Run the full silly-kicks enrichment chain for one match.

    Args:
        actions: SPADL actions with game_id column (silly-kicks convention).
        frames: Tracking frames in TRACKING_FRAMES_COLUMNS schema (105x68 LTR).
        xt: Fitted ExpectedThreat model.
        home_team_id: Home team identifier for directional features.
        match_id_native: Native match ID string for the output.
        data_source: Provider name (idsse, metrica, skillcorner).

    Returns:
        DataFrame with all _RESULT_COLUMNS except _ingested_at.
    """
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_das,
        add_defensive_line,
        add_gk_influence,
        add_line_break,
        add_off_ball_context,
        add_pressure_on_actor,
        add_sync_score,
        add_team_shape,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    # Step 0: Link actions to frames (keep links aside for sync_score)
    links, _report = link_actions_to_frames(actions, frames)

    # Step 1: GK resolution (events + tracking)
    actions = add_pre_shot_gk_context(actions, frames=frames)

    # Step 2: Action context (provenance skip guard in 3.11.2+)
    actions = add_action_context(actions, frames)

    # Step 3: Actor pre-window (TF-3)
    actions = add_actor_pre_window(actions, frames)

    # Step 4: Pressure (TF-2, all 3 methods)
    actions = add_pressure_on_actor(
        actions, frames,
        methods=("andrienko_oval", "link_zones", "bekkers_pi"),
    )

    # Steps 5-7: Pitch control (3 methods, using Series API to avoid 3x copies)
    # TODO: TC-2 — pre-link once, pass linked frames to avoid ~14× redundant
    # link_actions_to_frames calls. Every aggregator (steps 1-14) re-links
    # internally (~2-5s each on 3000 actions × 150k frames = 30-70s/match).
    # At 20 matches, that's 10-20 min of pure overhead. Accepted for v1;
    # silly-kicks upstream optimization to expose a pointers kwarg is tracked.
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(actions, frames, method=method)
        actions[s.name] = s.values

    # Step 8: Defensive line
    actions = add_defensive_line(actions, frames, home_team_id=home_team_id)

    # Step 9: Off-ball context (threshold line-break + 4 off-ball-run columns)
    # NOTE (M1): add_off_ball_context is an umbrella that ALSO adds the threshold
    # line_break + n_attackers_behind_line columns. Step 10 (add_line_break with
    # method="ward") is separate and adds the Ward-specific columns.
    actions = add_off_ball_context(actions, frames, home_team_id=home_team_id)

    # Step 10: Ward line-breaking
    actions = add_line_break(actions, frames, method="ward", home_team_id=home_team_id)

    # Step 11: Team shape
    actions = add_team_shape(actions, frames, home_team_id=home_team_id)

    # Step 12: DAS (defensive wrapper — accessible-space can IndexError)
    try:
        actions = add_das(actions, frames)
    except Exception:
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan

    # Step 13: GK influence
    actions = add_gk_influence(actions, frames, xt, home_team_id=home_team_id)

    # Step 14: Cover shadows
    actions = add_cover_shadows(actions, frames, xt, home_team_id=home_team_id)

    # Step 15: Sync score
    actions = add_sync_score(actions, links)

    # ── Build output ───────────────────────────────────────────────────
    # Rename game_id → match_id (silly-kicks uses game_id, we use match_id)
    out = actions.copy()
    out["match_id"] = match_id_native
    out["data_source"] = data_source

    # Map silly-kicks type_id → type_name
    # NOTE: ACTION_TYPES does not exist in silly_kicks.spadl.schema.
    # The correct import is actiontypes from silly_kicks.spadl.config.
    if "type_name" not in out.columns and "type_id" in out.columns:
        from silly_kicks.spadl.config import actiontypes

        type_map = {i: name for i, name in enumerate(actiontypes)}
        out["type_name"] = out["type_id"].map(type_map)

    # Cast team_id and player_id to string (output schema is STRING)
    out["team_id"] = out["team_id"].astype(str)
    out["player_id"] = out["player_id"].astype(str)

    # Select and order output columns (excluding _ingested_at — added by write_delta_table)
    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    for col in output_cols:
        if col not in out.columns:
            out[col] = np.nan
    return out[output_cols]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_enrichment.py -v`
Expected: PASS (may take 10-30s due to enrichment computation).

- [ ] **Step 5: Run schema parity test again to confirm consistency**

Run: `uv run pytest src/tests/test_tracking_context_schema_parity.py src/tests/test_tracking_context_enrichment.py -v`
Expected: All tests PASS.

---

## Task 4: Skip guard + pipeline orchestration

**Files:**
- Modify: `src/ingestion/tracking_context.py`
- Modify: `src/ingestion/guards.py`

- [ ] **Step 1: Add skip guard class and pipeline to tracking_context.py**

Add to `src/ingestion/tracking_context.py` after the `_enrich_match` function:

```python
# ── Skip Guard ─────────────────────────────────────────────────────────


class _TrackingContextGuard:
    """SkipGuard adapter for tracking context pipeline."""

    workflow_id = "wf-tracking-context"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check each provider's tracking table for unprocessed matches."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _TRACKING_CONTEXT_DDL)

        idsse_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.idsse_tracking",
            results_table,
            results_filter="data_source = 'idsse'",
        )
        metrica_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.metrica_tracking",
            results_table,
            results_filter="data_source = 'metrica'",
        )
        skillcorner_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.skillcorner_tracking",
            results_table,
            results_filter="data_source = 'skillcorner'",
        )

        total = len(idsse_ids) + len(metrica_ids) + len(skillcorner_ids)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            metadata={
                "idsse_ids": idsse_ids,
                "metrica_ids": metrica_ids,
                "skillcorner_ids": skillcorner_ids,
            },
        )


skip_guard = _TrackingContextGuard()


# ── Pipeline orchestration ─────────────────────────────────────────────


@workflow("wf-tracking-context", phase="enrichment")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the tracking context enrichment pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import StructField, StructType, BooleanType, DoubleType, LongType, StringType

    from silly_kicks.xthreat import ExpectedThreat

    # ── Driver-side setup ──────────────────────────────────────────────
    # Fit xT model on SPADL actions from tracking providers only (M2).
    # The xT grid converges quickly; restricting to tracking providers
    # keeps driver memory bounded as the lakehouse grows.
    spadl_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
        .select("game_id", "action_id", "period_id", "time_seconds",
                "team_id", "player_id", "type_id", "result_id", "bodypart_id",
                "start_x", "start_y", "end_x", "end_y", "original_event_id")
        .toPandas()
    )
    xt = ExpectedThreat().fit(spadl_pdf)
    logger.info("xT model fitted on %d actions (grid shape %s)", len(spadl_pdf), xt.xT.shape)

    # Home team lookups for IDSSE (Metrica/SkillCorner resolve from bronze)
    idsse_ids = filter_result.metadata.get("idsse_ids", [])
    metrica_ids = filter_result.metadata.get("metrica_ids", [])
    skillcorner_ids = filter_result.metadata.get("skillcorner_ids", [])

    total_written = 0

    # Process each provider separately (different converter paths)
    if idsse_ids:
        rows = _process_idsse(spark, catalog, schema, logger, xt, idsse_ids)
        total_written += rows

    if metrica_ids:
        rows = _process_metrica(spark, catalog, schema, logger, xt, metrica_ids)
        total_written += rows

    if skillcorner_ids:
        rows = _process_skillcorner(spark, catalog, schema, logger, xt, skillcorner_ids)
        total_written += rows

    logger.info("Tracking context pipeline complete — %d total rows written", total_written)
    return total_written


# ── Entry point ────────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for tracking context enrichment."""
    args = parse_ingestion_args("Compute action-coupled tracking features")
    logger = configure_logging("tracking_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting tracking context pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add bronze-to-frames helpers and provider processing functions**

Add to `src/ingestion/tracking_context.py` between `_enrich_match` and the guard class:

```python
# ── Provider processing ────────────────────────────────────────────────


def _process_idsse(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: object,
    new_ids: list[str],
) -> int:
    """Process IDSSE matches via sportec.convert_to_frames from bronze."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.spadl_adapter import (
        adapt_idsse_events_for_silly_kicks,
        derive_idsse_home_team_start_left,
    )
    from ingestion.utils import write_delta_table
    from silly_kicks.tracking import PreprocessConfig
    from silly_kicks.tracking.sportec import convert_to_frames

    total = 0
    for match_id in new_ids:
        logger.info("Processing IDSSE match %s", match_id)

        # Load tracking frames from bronze (H3: Column expressions, not f-strings)
        trk_pdf = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter(F.col("match_id") == match_id)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for IDSSE match %s", match_id)
            continue

        # Load SPADL actions from bronze (H2: game_id included via select)
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter(
                (F.col("match_id_native") == match_id)
                & (F.col("data_source") == "idsse")
            )
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for IDSSE match %s", match_id)
            continue

        # Derive home team info from bronze events
        events_pdf = (
            spark.table(f"{catalog}.bronze.idsse_events")
            .filter(F.col("match_id") == match_id)
            .toPandas()
        )
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)

        # Convert tracking to silly-kicks frames (105×68 LTR)
        frames, _report = convert_to_frames(
            trk_pdf,
            home_team_id=home_team_id,
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )

        # Align game_id: sportec converter uses DFL string ID, but SPADL
        # actions carry a BIGINT hash. Must match for aggregators that group
        # by game_id (e.g. compute_defensive_line in batch mode).
        frames["game_id"] = int(actions_pdf["game_id"].iloc[0])

        # Enrich
        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="idsse",
        )

        # Write to bronze.
        # NOTE (H2): replace_where is Spark's .option("replaceWhere", ...) which
        # accepts ONLY SQL string predicates — Column expressions are not supported.
        # match_id comes from find_new_ids() (our own Delta tables), not user input.
        # All 29 existing callers in the codebase use this same f-string pattern.
        result_sdf = spark.createDataFrame(result)
        written = write_delta_table(
            result_sdf, catalog, schema, _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written

    return total


# ── Bronze → silly-kicks frames helpers ──────────────────────────────


def _derive_velocities_savgol(
    frames: pd.DataFrame, provider: str, frame_rate: int,
) -> None:
    """Derive vx/vy/speed via Savitzky-Golay smoothed differentiation (in-place).

    NOTE: silly-kicks uses a two-pass pipeline (smooth_frames → derive_velocities
    on smoothed positions). This helper applies a single SG derivative pass on raw
    positions — numerically slightly noisier but practically equivalent for
    well-formed data. Acceptable for v1; align with two-pass if velocity quality
    proves insufficient on SkillCorner 10fps data.

    Uses silly-kicks per-provider defaults from _provider_defaults_generated.py:
    - Metrica:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps
    - SkillCorner: sg_window_seconds=1.0, sg_poly_order=3 → window=11 at 10fps
    - Sportec:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps

    Ball velocity IS derived (silly-kicks groups by [period_id, is_ball, player_id]).

    Args:
        frames: Must have columns [player_id, is_ball, x, y] sorted by time
                within each player/ball group.
        provider: "metrica" or "skillcorner" — selects SG parameters.
        frame_rate: Tracking data frame rate (Hz).
    """
    from scipy.signal import savgol_filter

    # Per-provider SG defaults matching silly-kicks _provider_defaults_generated.py
    _SG_DEFAULTS: dict[str, tuple[float, int]] = {
        "metrica": (0.4, 3),      # sg_window_seconds, sg_poly_order
        "skillcorner": (1.0, 3),
        "sportec": (0.4, 3),      # IDSSE uses convert_to_frames, but fallback
    }
    sg_window_s, polyorder = _SG_DEFAULTS.get(provider, (0.4, 3))

    dt = 1.0 / frame_rate
    window = max(round(sg_window_s * frame_rate) | 1, polyorder + 2)
    if window % 2 == 0:
        window += 1

    # Initialize with NaN (not 0.0 — 0.0 implies stationary, NaN implies unknown)
    frames["vx"] = np.nan
    frames["vy"] = np.nan

    # Group by (period_id, is_ball, player_id) — matching silly-kicks pipeline.
    # Ball rows ARE processed (pid=None, is_ball=True).
    for _key, idx in frames.groupby(["period_id", "is_ball", "player_id"]).groups.items():
        group = frames.loc[idx]
        x_raw = group["x"].values.astype(float)
        y_raw = group["y"].values.astype(float)
        nan_mask = np.isnan(x_raw) | np.isnan(y_raw)

        if nan_mask.all():
            continue

        # Short groups: np.gradient fallback (matches silly-kicks _velocity.py)
        if len(group) < window:
            x_safe = np.where(nan_mask, 0.0, x_raw)
            y_safe = np.where(nan_mask, 0.0, y_raw)
            vx_g = np.gradient(x_safe, dt)
            vy_g = np.gradient(y_safe, dt)
            vx_g[nan_mask] = np.nan
            vy_g[nan_mask] = np.nan
            frames.loc[idx, "vx"] = vx_g
            frames.loc[idx, "vy"] = vy_g
            continue

        # Interpolate NaN positions before SG filtering (linear interp across gaps),
        # then re-mask original NaN positions back to NaN in the output.
        # Matches silly-kicks derive_velocities (_velocity.py:84-124).
        valid_idx = np.where(~nan_mask)[0]
        x_filled = np.interp(np.arange(len(group)), valid_idx, x_raw[~nan_mask])
        y_filled = np.interp(np.arange(len(group)), valid_idx, y_raw[~nan_mask])

        vx_g = savgol_filter(x_filled, window, polyorder, deriv=1, delta=dt)
        vy_g = savgol_filter(y_filled, window, polyorder, deriv=1, delta=dt)
        vx_g[nan_mask] = np.nan
        vy_g[nan_mask] = np.nan

        frames.loc[idx, "vx"] = vx_g
        frames.loc[idx, "vy"] = vy_g

    # Compute speed from velocity (matches silly-kicks derive_velocities output)
    frames["speed"] = np.sqrt(frames["vx"] ** 2 + frames["vy"] ** 2)


def _bronze_metrica_to_frames(trk_pdf: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Convert Metrica bronze tracking (frame-level JSON) to silly-kicks frames.

    Bronze schema: period, frame, timestamp, ball_x, ball_y,
    home_players (JSON), away_players (JSON), gk_jersey_numbers (JSON),
    pitch_length_m, pitch_width_m, frame_rate.

    COORDINATE CONVERSION: Metrica 0-1 normalized → SPADL 105×68 meters.
    - x_spadl = x_01 * 105.0
    - y_spadl = (1 - y_01) * 68.0  (Metrica y-axis is flipped: 0=top, 1=bottom)

    Do NOT use metrica_to_statsbomb() — that produces 120×80 StatsBomb yards,
    not 105×68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)×(0,68).
    """
    import json

    # Parse GK jersey numbers (match-level constant)
    gk_jerseys: set[str] = set()
    if "gk_jersey_numbers" in trk_pdf.columns:
        gk_raw = trk_pdf["gk_jersey_numbers"].dropna()
        if not gk_raw.empty:
            parsed = json.loads(gk_raw.iloc[0]) if isinstance(gk_raw.iloc[0], str) else gk_raw.iloc[0]
            gk_jerseys = {str(j) for j in parsed} if parsed else set()

    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 25

    rows: list[dict] = []
    for _, row in trk_pdf.iterrows():
        # Skip rows with NaN period (e.g. pre-match warmup data)
        if pd.isna(row["period"]):
            continue
        fid = int(row["frame"])
        pid = int(row["period"])
        t = float(row["timestamp"])

        # Home and away player rows from JSON
        for team_label, json_col in [("Home", "home_players"), ("Away", "away_players")]:
            raw = row.get(json_col)
            if pd.isna(raw) or raw is None:
                continue
            players = json.loads(raw) if isinstance(raw, str) else raw
            for jersey, coords in players.items():
                if isinstance(coords, dict) and "x" in coords and "y" in coords:
                    # Direct Metrica 0-1 → SPADL 105×68 (NOT StatsBomb 120×80)
                    x_spadl = float(coords["x"]) * 105.0
                    y_spadl = (1.0 - float(coords["y"])) * 68.0
                    rows.append({
                        "game_id": game_id, "frame_id": fid,
                        "period_id": pid, "time_seconds": t,
                        "player_id": f"{team_label}_{jersey}",
                        "team_id": team_label,
                        "x": x_spadl, "y": y_spadl,
                        "is_goalkeeper": str(jersey) in gk_jerseys,
                        "is_ball": False,
                    })

        # Ball row
        bx, by = row.get("ball_x"), row.get("ball_y")
        if not pd.isna(bx) and not pd.isna(by):
            rows.append({
                "game_id": game_id, "frame_id": fid,
                "period_id": pid, "time_seconds": t,
                "player_id": None, "team_id": None,
                "x": float(bx) * 105.0,
                "y": (1.0 - float(by)) * 68.0,
                "is_goalkeeper": False, "is_ball": True,
            })

    frames = pd.DataFrame(rows)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "metrica"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None       # Metrica bronze doesn't carry ball state
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="metrica", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _bronze_skillcorner_to_frames(trk_pdf: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Convert SkillCorner bronze tracking (narrow) to silly-kicks frames.

    Bronze schema (narrow, one row per player per frame):
    period, frame, timestamp, player_id, team, x, y, ball_x, ball_y,
    ball_z, is_goalkeeper, home_team_id, away_team_id, frame_rate.

    COORDINATE CONVERSION: center-origin meters → SPADL 105×68 meters.
    - x_spadl = x_center + 52.5
    - y_spadl = y_center + 34.0

    Do NOT use center_m_to_statsbomb() — that produces 120×80 StatsBomb yards,
    not 105×68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)×(0,68).
    """
    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 10

    # Player rows — rename to match TRACKING_FRAMES_COLUMNS
    players = trk_pdf[["frame", "period", "timestamp", "player_id",
                        "team", "x", "y", "is_goalkeeper"]].copy()
    players.rename(columns={
        "frame": "frame_id", "period": "period_id",
        "timestamp": "time_seconds", "team": "team_id",
    }, inplace=True)
    # Direct center-origin meters → SPADL 105×68 (NOT StatsBomb 120×80)
    players["x"] = players["x"] + 52.5
    players["y"] = players["y"] + 34.0
    players["is_ball"] = False
    players["game_id"] = game_id

    # Ball rows — deduplicate (ball_x/ball_y are on every player row)
    ball_src = trk_pdf[["frame", "period", "timestamp", "ball_x", "ball_y"]].copy()
    ball_src = ball_src.drop_duplicates(subset=["frame", "period"])
    ball_src.rename(columns={
        "frame": "frame_id", "period": "period_id",
        "timestamp": "time_seconds", "ball_x": "x", "ball_y": "y",
    }, inplace=True)
    ball_src["x"] = ball_src["x"] + 52.5
    ball_src["y"] = ball_src["y"] + 34.0
    ball_src["player_id"] = None
    ball_src["team_id"] = None
    ball_src["is_goalkeeper"] = False
    ball_src["is_ball"] = True
    ball_src["game_id"] = game_id

    frames = pd.concat([players, ball_src], ignore_index=True)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "skillcorner"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="skillcorner", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _process_metrica(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: object,
    new_ids: list[str],
) -> int:
    """Process Metrica matches from bronze tables (NOT from internet).

    Reads bronze.metrica_tracking (frame-level JSON) and converts to
    silly-kicks TRACKING_FRAMES_COLUMNS via _bronze_metrica_to_frames().
    home_team_id = "Home" (matches SPADL convention for Metrica).
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing Metrica match %s", match_id)

        # Read tracking from bronze — no network dependency
        trk_pdf = (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter(F.col("match_id") == match_id)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for Metrica match %s", match_id)
            continue

        # Load SPADL actions from bronze
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter(
                (F.col("match_id_native") == match_id)
                & (F.col("data_source") == "metrica")
            )
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for Metrica match %s", match_id)
            continue

        # Convert bronze tracking → silly-kicks frames (105×68 LTR)
        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_metrica_to_frames(trk_pdf, game_id=game_id)

        # home_team_id = "Home" — Metrica SPADL convention
        # (matches adapt_metrica_events_for_silly_kicks → home_team_id="Home")
        home_team_id = "Home"

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="metrica",
        )

        result_sdf = spark.createDataFrame(result)
        written = write_delta_table(
            result_sdf, catalog, schema, _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written

    return total


def _process_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: object,
    new_ids: list[str],
) -> int:
    """Process SkillCorner matches from bronze tables (NOT from internet).

    Reads bronze.skillcorner_tracking (narrow format) and converts to
    silly-kicks TRACKING_FRAMES_COLUMNS via _bronze_skillcorner_to_frames().
    home_team_id derived from bronze home_team_id column.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing SkillCorner match %s", match_id)

        # Read tracking from bronze — no network dependency
        trk_pdf = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter(F.col("match_id") == match_id)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for SkillCorner match %s", match_id)
            continue

        # Load SPADL actions from bronze
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter(
                (F.col("match_id_native") == match_id)
                & (F.col("data_source") == "skillcorner")
            )
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for SkillCorner match %s", match_id)
            continue

        # Convert bronze tracking → silly-kicks frames (105×68 LTR)
        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_skillcorner_to_frames(trk_pdf, game_id=game_id)

        # Derive home_team_id from bronze column (L2: not from kloppy metadata)
        home_team_id = str(trk_pdf["home_team_id"].dropna().iloc[0])

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="skillcorner",
        )

        result_sdf = spark.createDataFrame(result)
        written = write_delta_table(
            result_sdf, catalog, schema, _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written

    return total
```

- [ ] **Step 3: Register guard in `_GUARD_MODULES`**

In `src/ingestion/guards.py`, add to the `_GUARD_MODULES` list (after `"ingestion.hf_sync"`):

```python
    "ingestion.tracking_context",
```

- [ ] **Step 4: Run lint + type check**

Run: `uv run ruff check src/ingestion/tracking_context.py && uv run ruff format --check src/ingestion/tracking_context.py`
Expected: No errors. Fix any issues.

- [ ] **Step 5: Run all TC-1 tests**

Run: `uv run pytest src/tests/test_tracking_context_schema_parity.py src/tests/test_tracking_context_enrichment.py -v`
Expected: All PASS.

---

## Task 5: Workflow card

**Files:**
- Create: `workflow-cards/wf-tracking-context.yaml`

- [ ] **Step 1: Create workflow card**

Create `workflow-cards/wf-tracking-context.yaml`:

```yaml
name: wf-tracking-context
description: >
  Compute all silly-kicks action-coupled tracking features in a single pass
  per match and write to bronze.spadl_tracking_context. Covers IDSSE, Metrica,
  and SkillCorner providers. 83-column output including pitch control, pressure,
  team shape, line-breaking, GK influence, cover shadows, and DAS.

datasets:
  inputs:
    - name: spadl_actions
      source: delta-table
      description: SPADL actions from all providers
    - name: idsse_tracking
      source: delta-table
      description: IDSSE (Sportec) raw tracking data
    - name: idsse_events
      source: delta-table
      description: IDSSE event data (for home_team_id derivation)
    - name: metrica_tracking
      source: delta-table
      description: Metrica raw tracking data
    - name: skillcorner_tracking
      source: delta-table
      description: SkillCorner raw tracking data
  outputs:
    - name: spadl_tracking_context
      destination: delta-table
      description: Per-action tracking features (83 columns)

execution:
  enrichment:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: compute_tracking_context
    module: ingestion.tracking_context
    schedule: "daily 06:00 UTC"
    timeout: "7200s"
    environment: spadl

dbt_model: fct_tracking_context

idempotency:
  strategy: skip-guard
  key: [match_id, data_source]

gk_influence:
  zone_names: ["six_yard_box"]  # Default produces 4 columns; adding "penalty_area" = 8 columns (schema change)

performance:
  memory_ceiling: "800 MB per UDF group (1 GB limit)"
  compute_estimate: "2-5 min per match, 15-40 min total for 20 matches"
```

- [ ] **Step 2: Validate card parses**

Run: `uv run python -c "import yaml; yaml.safe_load(open('workflow-cards/wf-tracking-context.yaml')); print('OK')"`
Expected: `OK`.

---

## Task 6: dbt staging layer

**Files:**
- Create: `dbt_project/models/staging/tracking_context/_tracking_context__sources.yml`
- Create: `dbt_project/models/staging/tracking_context/_tracking_context__models.yml`
- Create: `dbt_project/models/staging/tracking_context/stg_spadl__tracking_context.sql`

- [ ] **Step 1: Create staging directory and source YAML**

Create `dbt_project/models/staging/tracking_context/_tracking_context__sources.yml`:

```yaml
version: 2

sources:
  - name: tracking_context
    description: >
      Action-coupled tracking features produced by the TC-1 pipeline.
      One row per SPADL action for matches with tracking data (IDSSE, Metrica,
      SkillCorner). 83 columns covering pitch control, pressure, team shape,
      line-breaking, GK influence, cover shadows, DAS, and sync score.
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 72, period: hour}

    tables:
      - name: spadl_tracking_context
        description: >
          Per-action tracking features. Grain: one row per SPADL action
          per match. Provider-agnostic (data_source discriminates).
```

- [ ] **Step 2: Create staging SQL**

Create `dbt_project/models/staging/tracking_context/stg_spadl__tracking_context.sql`:

```sql
-- stg_spadl__tracking_context.sql
-- Passthrough staging for TC-1 bronze tracking context.
-- Deduplicates by (match_id, action_id), latest _ingested_at wins.
-- Renames identity columns for Kimball FK resolution downstream.

with source as (

    select * from {{ source('tracking_context', 'spadl_tracking_context') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)         as data_source,
        cast(match_id as string)            as native_match_id,
        cast(action_id as bigint)           as action_id,
        cast(period_id as bigint)           as period_id,
        cast(time_seconds as double)        as time_seconds,
        cast(team_id as string)             as team_id_native,
        cast(player_id as string)           as player_id_native,
        cast(type_name as string)           as type_name,
        cast(start_x as double)             as start_x,
        cast(start_y as double)             as start_y,
        cast(end_x as double)              as end_x,
        cast(end_y as double)              as end_y,
        -- Linkage provenance
        cast(frame_id as bigint)            as frame_id,
        cast(time_offset_seconds as double) as time_offset_seconds,
        cast(link_quality_score as double)  as link_quality_score,
        cast(n_candidate_frames as bigint)  as n_candidate_frames,
        -- GK resolution (L1: DOUBLE in bronze → BIGINT for dim_players join)
        cast(defending_gk_player_id as bigint) as defending_gk_player_id,
        cast(gk_was_distributing as boolean) as gk_was_distributing,
        cast(gk_was_engaged as boolean)     as gk_was_engaged,
        cast(gk_actions_in_possession as bigint) as gk_actions_in_possession,
        -- GK spatial
        cast(pre_shot_gk_x as double)      as pre_shot_gk_x,
        cast(pre_shot_gk_y as double)      as pre_shot_gk_y,
        cast(pre_shot_gk_distance_to_goal as double) as pre_shot_gk_distance_to_goal,
        cast(pre_shot_gk_distance_to_shot as double) as pre_shot_gk_distance_to_shot,
        cast(pre_shot_gk_angle_to_shot_trajectory as double) as pre_shot_gk_angle_to_shot_trajectory,
        cast(pre_shot_gk_angle_off_goal_line as double) as pre_shot_gk_angle_off_goal_line,
        -- Action context
        cast(nearest_defender_distance as double) as nearest_defender_distance,
        cast(actor_speed as double)         as actor_speed,
        cast(receiver_zone_density as bigint) as receiver_zone_density,
        cast(defenders_in_triangle_to_goal as bigint) as defenders_in_triangle_to_goal,
        -- Actor pre-window
        cast(actor_arc_length_pre_window as double) as actor_arc_length_pre_window,
        cast(actor_displacement_pre_window as double) as actor_displacement_pre_window,
        -- Pressure
        cast(pressure_on_actor__andrienko_oval as double) as pressure_on_actor__andrienko_oval,
        cast(pressure_on_actor__link_zones as double) as pressure_on_actor__link_zones,
        cast(pressure_on_actor__bekkers_pi as double) as pressure_on_actor__bekkers_pi,
        -- Pitch control
        cast(pitch_control_at_ball__spearman as double) as pitch_control_at_ball__spearman,
        cast(pitch_control_at_ball__fernandez_bornn as double) as pitch_control_at_ball__fernandez_bornn,
        cast(pitch_control_at_ball__voronoi as double) as pitch_control_at_ball__voronoi,
        -- Defensive line
        cast(defensive_line_x as double)    as defensive_line_x,
        cast(back_line_high_x as double)    as back_line_high_x,
        cast(compactness_x as double)       as compactness_x,
        cast(lateral_width as double)       as lateral_width,
        cast(max_lateral_gap as double)     as max_lateral_gap,
        cast(back_n_count as bigint)        as back_n_count,
        -- Off-ball context
        cast(line_break as boolean)         as line_break,
        cast(n_attackers_behind_line as bigint) as n_attackers_behind_line,
        cast(n_off_ball_runners_pre_window as bigint) as n_off_ball_runners_pre_window,
        cast(max_off_ball_run_displacement_pre_window as double) as max_off_ball_run_displacement_pre_window,
        cast(mean_off_ball_run_speed_pre_window as double) as mean_off_ball_run_speed_pre_window,
        cast(n_off_ball_runners_toward_goal_pre_window as bigint) as n_off_ball_runners_toward_goal_pre_window,
        -- Ward line-breaking
        cast(line_break__ward as boolean)   as line_break__ward,
        cast(lines_broken__ward as bigint)  as lines_broken__ward,
        cast(line_breaking_type__ward as string) as line_breaking_type__ward,
        -- Team shape
        cast(team_shape_centroid_x_attacking as double) as team_shape_centroid_x_attacking,
        cast(team_shape_centroid_y_attacking as double) as team_shape_centroid_y_attacking,
        cast(team_shape_convex_hull_area_attacking as double) as team_shape_convex_hull_area_attacking,
        cast(team_shape_team_length_attacking as double) as team_shape_team_length_attacking,
        cast(team_shape_team_width_attacking as double) as team_shape_team_width_attacking,
        cast(team_shape_stretch_index_attacking as double) as team_shape_stretch_index_attacking,
        cast(team_shape_n_outfield_players_attacking as bigint) as team_shape_n_outfield_players_attacking,
        cast(team_shape_centroid_x_defending as double) as team_shape_centroid_x_defending,
        cast(team_shape_centroid_y_defending as double) as team_shape_centroid_y_defending,
        cast(team_shape_convex_hull_area_defending as double) as team_shape_convex_hull_area_defending,
        cast(team_shape_team_length_defending as double) as team_shape_team_length_defending,
        cast(team_shape_team_width_defending as double) as team_shape_team_width_defending,
        cast(team_shape_stretch_index_defending as double) as team_shape_stretch_index_defending,
        cast(team_shape_n_outfield_players_defending as bigint) as team_shape_n_outfield_players_defending,
        -- DAS
        cast(das_team as double)            as das_team,
        cast(das_opponent as double)        as das_opponent,
        cast(das_diff as double)            as das_diff,
        -- GK influence
        cast(gk_pitch_control_share_weighted as double) as gk_pitch_control_share_weighted,
        cast(gk_reachable_area_m2 as double) as gk_reachable_area_m2,
        cast(gk_closing_time_mean_s__six_yard_box as double) as gk_closing_time_mean_s__six_yard_box,
        cast(gk_closing_time_min_s__six_yard_box as double) as gk_closing_time_min_s__six_yard_box,
        -- Cover shadows
        cast(n_blocked_receivers as bigint) as n_blocked_receivers,
        cast(n_potential_receivers as bigint) as n_potential_receivers,
        cast(blocking_score as double)      as blocking_score,
        cast(blocked_threat_fraction as double) as blocked_threat_fraction,
        cast(max_single_defender_blocking_score as double) as max_single_defender_blocking_score,
        -- Sync score
        cast(sync_score_min as double)      as sync_score_min,
        cast(sync_score_mean as double)     as sync_score_mean,
        cast(sync_score_high_quality_frac as double) as sync_score_high_quality_frac

    from deduplicated
    where _row_num = 1

)

select * from cleaned
```

- [ ] **Step 3: Create staging models YAML**

Create `dbt_project/models/staging/tracking_context/_tracking_context__models.yml`:

```yaml
version: 2

models:
  - name: stg_spadl__tracking_context
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Staging view for TC-1 tracking context. Deduplicates bronze data
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
        description: "Provider: idsse, metrica, skillcorner"
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse', 'metrica', 'skillcorner']
      - name: line_breaking_type__ward
        description: >
          Ward line-breaking categorical. Values are "between_lines"
          (pass crosses both defensive+midfield lines) or "around_line"
          (pass crosses exactly one formation line). NULL when no
          line-breaking detected.
        data_tests:
          - accepted_values:
              arguments:
                values: ['between_lines', 'around_line']
              config:
                where: "line_breaking_type__ward is not null"
```

---

## Task 7: dbt gold mart

**Files:**
- Create: `dbt_project/models/marts/fct_tracking_context.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create gold mart SQL**

Create `dbt_project/models/marts/fct_tracking_context.sql`:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tracking_context_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_tracking_context.sql
-- Gold-layer unified tracking features per SPADL action.
-- Pure Kimball from day one — no legacy BIGINT identity columns.
-- Grain: one row per (match_key, action_id).

with tracking_raw as (

    select
        native_match_id,
        data_source,
        action_id,
        period_id,
        time_seconds,
        team_id_native,
        player_id_native,
        type_name,
        start_x,
        start_y,
        end_x,
        end_y,
        frame_id,
        time_offset_seconds,
        link_quality_score,
        n_candidate_frames,
        defending_gk_player_id,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        pre_shot_gk_x,
        pre_shot_gk_y,
        pre_shot_gk_distance_to_goal,
        pre_shot_gk_distance_to_shot,
        pre_shot_gk_angle_to_shot_trajectory,
        pre_shot_gk_angle_off_goal_line,
        nearest_defender_distance,
        actor_speed,
        receiver_zone_density,
        defenders_in_triangle_to_goal,
        actor_arc_length_pre_window,
        actor_displacement_pre_window,
        pressure_on_actor__andrienko_oval,
        pressure_on_actor__link_zones,
        pressure_on_actor__bekkers_pi,
        pitch_control_at_ball__spearman,
        pitch_control_at_ball__fernandez_bornn,
        pitch_control_at_ball__voronoi,
        defensive_line_x,
        back_line_high_x,
        compactness_x,
        lateral_width,
        max_lateral_gap,
        back_n_count,
        line_break,
        n_attackers_behind_line,
        n_off_ball_runners_pre_window,
        max_off_ball_run_displacement_pre_window,
        mean_off_ball_run_speed_pre_window,
        n_off_ball_runners_toward_goal_pre_window,
        line_break__ward,
        lines_broken__ward,
        line_breaking_type__ward,
        team_shape_centroid_x_attacking,
        team_shape_centroid_y_attacking,
        team_shape_convex_hull_area_attacking,
        team_shape_team_length_attacking,
        team_shape_team_width_attacking,
        team_shape_stretch_index_attacking,
        team_shape_n_outfield_players_attacking,
        team_shape_centroid_x_defending,
        team_shape_centroid_y_defending,
        team_shape_convex_hull_area_defending,
        team_shape_team_length_defending,
        team_shape_team_width_defending,
        team_shape_stretch_index_defending,
        team_shape_n_outfield_players_defending,
        das_team,
        das_opponent,
        das_diff,
        gk_pitch_control_share_weighted,
        gk_reachable_area_m2,
        gk_closing_time_mean_s__six_yard_box,
        gk_closing_time_min_s__six_yard_box,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac
    from {{ ref('stg_spadl__tracking_context') }}

),

keyed as (

    select
        dm.match_key,
        dt.team_key,
        dp.player_key,
        tr.*
    from tracking_raw tr
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = tr.data_source
       and dm.native_match_id = tr.native_match_id
    left join {{ ref('dim_teams') }} dt
        on dt.provider = tr.data_source
       and dt.native_team_id = tr.team_id_native
    left join {{ ref('dim_players') }} dp
        on dp.provider = tr.data_source
       and dp.native_player_id = tr.player_id_native

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_key', 'action_id']) }} as tracking_context_id,
        match_key,
        team_key,
        player_key,
        action_id,
        data_source,
        period_id,
        time_seconds,
        type_name,
        start_x,
        start_y,
        end_x,
        end_y,
        frame_id,
        time_offset_seconds,
        link_quality_score,
        n_candidate_frames,
        defending_gk_player_id,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        pre_shot_gk_x,
        pre_shot_gk_y,
        pre_shot_gk_distance_to_goal,
        pre_shot_gk_distance_to_shot,
        pre_shot_gk_angle_to_shot_trajectory,
        pre_shot_gk_angle_off_goal_line,
        nearest_defender_distance,
        actor_speed,
        receiver_zone_density,
        defenders_in_triangle_to_goal,
        actor_arc_length_pre_window,
        actor_displacement_pre_window,
        pressure_on_actor__andrienko_oval,
        pressure_on_actor__link_zones,
        pressure_on_actor__bekkers_pi,
        pitch_control_at_ball__spearman,
        pitch_control_at_ball__fernandez_bornn,
        pitch_control_at_ball__voronoi,
        defensive_line_x,
        back_line_high_x,
        compactness_x,
        lateral_width,
        max_lateral_gap,
        back_n_count,
        line_break,
        n_attackers_behind_line,
        n_off_ball_runners_pre_window,
        max_off_ball_run_displacement_pre_window,
        mean_off_ball_run_speed_pre_window,
        n_off_ball_runners_toward_goal_pre_window,
        line_break__ward,
        lines_broken__ward,
        line_breaking_type__ward,
        team_shape_centroid_x_attacking,
        team_shape_centroid_y_attacking,
        team_shape_convex_hull_area_attacking,
        team_shape_team_length_attacking,
        team_shape_team_width_attacking,
        team_shape_stretch_index_attacking,
        team_shape_n_outfield_players_attacking,
        team_shape_centroid_x_defending,
        team_shape_centroid_y_defending,
        team_shape_convex_hull_area_defending,
        team_shape_team_length_defending,
        team_shape_team_width_defending,
        team_shape_stretch_index_defending,
        team_shape_n_outfield_players_defending,
        das_team,
        das_opponent,
        das_diff,
        gk_pitch_control_share_weighted,
        gk_reachable_area_m2,
        gk_closing_time_mean_s__six_yard_box,
        gk_closing_time_min_s__six_yard_box,
        n_blocked_receivers,
        n_potential_receivers,
        blocking_score,
        blocked_threat_fraction,
        max_single_defender_blocking_score,
        sync_score_min,
        sync_score_mean,
        sync_score_high_quality_frac

    from keyed
    -- No QUALIFY needed: staging dedup + single Kimball join = guaranteed unique grain.
    -- Original had `qualify row_number() over (partition by match_key, action_id
    -- order by match_key) = 1` which was a no-op (ordering by partition key).

)

select * from final
```

- [ ] **Step 2: Add fct_tracking_context entry to _marts__models.yml**

Append to `dbt_project/models/marts/_marts__models.yml` before the closing of the models list. Add the entry after the last existing model. Key columns to include:

```yaml
  # ── Tracking Context ──────────────────────────────────────────────────

  - name: fct_tracking_context
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Gold-layer unified tracking features per SPADL action. Grain: one row
      per (match_key, action_id). Pure Kimball — no legacy BIGINT columns.
      Covers IDSSE, Metrica, SkillCorner providers. 83 input columns with
      Kimball FKs (match_key, team_key, player_key) resolved via dim JOINs.
    columns:
      - name: tracking_context_id
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
        description: "Provider: idsse, metrica, skillcorner."
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse', 'metrica', 'skillcorner']
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
      - name: frame_id
        data_type: bigint
      - name: time_offset_seconds
        data_type: double
      - name: link_quality_score
        data_type: double
      - name: n_candidate_frames
        data_type: bigint
      - name: defending_gk_player_id
        data_type: bigint
      - name: gk_was_distributing
        data_type: boolean
      - name: gk_was_engaged
        data_type: boolean
      - name: gk_actions_in_possession
        data_type: bigint
      - name: pre_shot_gk_x
        data_type: double
      - name: pre_shot_gk_y
        data_type: double
      - name: pre_shot_gk_distance_to_goal
        data_type: double
      - name: pre_shot_gk_distance_to_shot
        data_type: double
      - name: pre_shot_gk_angle_to_shot_trajectory
        data_type: double
      - name: pre_shot_gk_angle_off_goal_line
        data_type: double
      - name: nearest_defender_distance
        data_type: double
      - name: actor_speed
        data_type: double
      - name: receiver_zone_density
        data_type: bigint
      - name: defenders_in_triangle_to_goal
        data_type: bigint
      - name: actor_arc_length_pre_window
        data_type: double
      - name: actor_displacement_pre_window
        data_type: double
      - name: pressure_on_actor__andrienko_oval
        data_type: double
      - name: pressure_on_actor__link_zones
        data_type: double
      - name: pressure_on_actor__bekkers_pi
        data_type: double
      - name: pitch_control_at_ball__spearman
        data_type: double
      - name: pitch_control_at_ball__fernandez_bornn
        data_type: double
      - name: pitch_control_at_ball__voronoi
        data_type: double
      - name: defensive_line_x
        data_type: double
      - name: back_line_high_x
        data_type: double
      - name: compactness_x
        data_type: double
      - name: lateral_width
        data_type: double
      - name: max_lateral_gap
        data_type: double
      - name: back_n_count
        data_type: bigint
      - name: line_break
        data_type: boolean
      - name: n_attackers_behind_line
        data_type: bigint
      - name: n_off_ball_runners_pre_window
        data_type: bigint
      - name: max_off_ball_run_displacement_pre_window
        data_type: double
      - name: mean_off_ball_run_speed_pre_window
        data_type: double
      - name: n_off_ball_runners_toward_goal_pre_window
        data_type: bigint
      - name: line_break__ward
        data_type: boolean
      - name: lines_broken__ward
        data_type: bigint
      - name: line_breaking_type__ward
        data_type: string
      - name: team_shape_centroid_x_attacking
        data_type: double
      - name: team_shape_centroid_y_attacking
        data_type: double
      - name: team_shape_convex_hull_area_attacking
        data_type: double
      - name: team_shape_team_length_attacking
        data_type: double
      - name: team_shape_team_width_attacking
        data_type: double
      - name: team_shape_stretch_index_attacking
        data_type: double
      - name: team_shape_n_outfield_players_attacking
        data_type: bigint
      - name: team_shape_centroid_x_defending
        data_type: double
      - name: team_shape_centroid_y_defending
        data_type: double
      - name: team_shape_convex_hull_area_defending
        data_type: double
      - name: team_shape_team_length_defending
        data_type: double
      - name: team_shape_team_width_defending
        data_type: double
      - name: team_shape_stretch_index_defending
        data_type: double
      - name: team_shape_n_outfield_players_defending
        data_type: bigint
      - name: das_team
        data_type: double
      - name: das_opponent
        data_type: double
      - name: das_diff
        data_type: double
      - name: gk_pitch_control_share_weighted
        data_type: double
      - name: gk_reachable_area_m2
        data_type: double
      - name: gk_closing_time_mean_s__six_yard_box
        data_type: double
      - name: gk_closing_time_min_s__six_yard_box
        data_type: double
      - name: n_blocked_receivers
        data_type: bigint
      - name: n_potential_receivers
        data_type: bigint
      - name: blocking_score
        data_type: double
      - name: blocked_threat_fraction
        data_type: double
      - name: max_single_defender_blocking_score
        data_type: double
      - name: sync_score_min
        data_type: double
      - name: sync_score_mean
        data_type: double
      - name: sync_score_high_quality_frac
        data_type: double
```

- [ ] **Step 3: Validate dbt parse**

Run: `cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with "dbt-databricks>=1.10.0,<1.12.0" dbt parse --profiles-dir .`
Expected: Parse succeeds with no errors.

---

## Task 8: Test registrations

**Files:**
- Modify: `src/tests/test_staging_coverage.py`
- Modify: `src/tests/test_card_parity_with_terraform.py`
- Modify: `scripts/create_indexes.py`

- [ ] **Step 1: Add tracking_context to staging coverage**

In `src/tests/test_staging_coverage.py`, add to `PROVIDER_COVERAGE` dict:

```python
    "tracking_context": [
        ("spadl_tracking_context", "stg_spadl__tracking_context"),
    ],
```

And add to `RENAMES` dict:

```python
    ("tracking_context", "spadl_tracking_context"): {
        "match_id": "native_match_id",
        "team_id": "team_id_native",
        "player_id": "player_id_native",
    },
```

- [ ] **Step 2: Add HF publisher to card parity test**

In `src/tests/test_card_parity_with_terraform.py`, add to `_HF_JOBS_SCRIPT_TO_CARD`:

```python
    "publish_tracking_context_hf.py": None,
```

- [ ] **Step 3: Add Lakebase indexes**

In `scripts/create_indexes.py`, add to `INDEXES` list:

```python
    ("idx_tracking_context_match_key", "fct_tracking_context_synced", "match_key"),
    ("idx_tracking_context_player_key", "fct_tracking_context_synced", "player_key"),
```

- [ ] **Step 4: Run registration tests**

Run: `uv run pytest src/tests/test_guard_conformance.py -k "tracking_context or TestWatermark" -v --no-header 2>&1 | head -30`
Expected: Guard auto-discovered, tests pass.

---

## Task 9: HF dataset card + publish script

**Files:**
- Create: `docs/huggingface/dataset-cards/spadl-tracking-context.md`
- Create: `scripts/publish_tracking_context_hf.py`

- [ ] **Step 1: Create HF dataset card**

Create `docs/huggingface/dataset-cards/spadl-tracking-context.md`:

```markdown
---
language: [en]
license: mit
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - football
  - soccer
  - tracking
  - spadl
  - pitch-control
  - team-shape
  - line-breaking
  - expected-threat
size_categories:
  - 10K<n<100K
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# SPADL Tracking Context

Unified action-coupled tracking features for football matches. One row per SPADL action, enriched with 66 tracking-derived features from the [silly-kicks](https://github.com/ML-KULeuven/silly-kicks) library.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/spadl-tracking-context")
df = ds["train"].to_pandas()
print(df.columns.tolist())  # 83 columns
```

## Feature Groups

| Group | Columns | Reference |
|-------|---------|-----------|
| Action context | nearest_defender_distance, actor_speed, receiver_zone_density, defenders_in_triangle_to_goal | — |
| Actor pre-window | actor_arc_length_pre_window, actor_displacement_pre_window | — |
| Pressure | pressure_on_actor__andrienko_oval, __link_zones, __bekkers_pi | Andrienko 2017, Bekkers 2023 |
| Pitch control | pitch_control_at_ball__spearman, __fernandez_bornn, __voronoi | Spearman 2018, Fernandez & Bornn 2018 |
| Defensive line | defensive_line_x, back_line_high_x, compactness_x, lateral_width, max_lateral_gap, back_n_count | — |
| Off-ball context | line_break, n_attackers_behind_line, n_off_ball_runners_*, ... | Power 2017 |
| Ward line-breaking | line_break__ward, lines_broken__ward, line_breaking_type__ward | Karakus & Arkadas 2025 |
| Team shape | team_shape_{metric}_{attacking/defending} (14 cols) | Clemente 2013 |
| DAS | das_team, das_opponent, das_diff | Bischofberger & Baca 2026 |
| GK influence | gk_pitch_control_share_weighted, gk_reachable_area_m2, gk_closing_time_* | Anzer & Bauer 2021 |
| Cover shadows | n_blocked_receivers, blocking_score, blocked_threat_fraction, ... | — |
| Sync score | sync_score_min, sync_score_mean, sync_score_high_quality_frac | — |
| GK context | defending_gk_player_id, gk_was_distributing, gk_was_engaged, gk_actions_in_possession | — |

## Providers

| Provider | Matches |
|----------|---------|
| IDSSE (Bundesliga) | 7 |
| Metrica (open data) | 3 |
| SkillCorner (open data) | 10 |

## Data Fields

All feature columns are `float64` (nullable NaN) unless noted. Identity columns: `data_source` (string), `match_id` (string), `action_id` (int), `period_id` (int), `time_seconds` (float), `team_id` (string), `player_id` (string), `type_name` (string), `start_x`/`start_y`/`end_x`/`end_y` (float, SPADL 105x68).

## License

MIT — see repository for details.
```

- [ ] **Step 2: Create PEP 723 publish script**

Create `scripts/publish_tracking_context_hf.py`. Read `scripts/publish_line_breaking_passes_hf.py` first and follow the same pattern: PEP 723 header with wheel dependency, Databricks SQL query from `fct_tracking_context`, Arrow stream fetch, parquet write partitioned by `data_source`, `upload_hf_readme` call at the end.

The SQL query:

```sql
SELECT * FROM soccer_analytics.dev_gold.fct_tracking_context
```

The HF repo: `luxury-lakehouse/spadl-tracking-context`

The card path: `get_hf_card_path("spadl-tracking-context", kind="dataset")`

- [ ] **Step 3: Verify card file exists at correct path**

Run: `ls docs/huggingface/dataset-cards/spadl-tracking-context.md`
Expected: File exists.

---

## Task 10: Wheel bump + lint + full test suite

**Files:**
- Modify: `pyproject.toml` (version bump)
- Run: `scripts/bump_wheel.py`

- [ ] **Step 1: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`
Expected: Reports files updated with new version.

- [ ] **Step 2: Run ruff on all new/modified files**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_schema_parity.py src/tests/test_tracking_context_enrichment.py && uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_schema_parity.py src/tests/test_tracking_context_enrichment.py`
Expected: No errors. Fix any.

- [ ] **Step 3: Run pyright on the ingestion module**

Run: `uv run pyright src/ingestion/tracking_context.py`
Expected: 0 errors in basic mode.

- [ ] **Step 4: Run all TC-1 tests**

Run: `uv run pytest src/tests/test_tracking_context_schema_parity.py src/tests/test_tracking_context_enrichment.py -v`
Expected: All PASS.

- [ ] **Step 5: Run existing test suite to check for regressions**

Run: `uv run pytest src/tests/ -v --ignore=src/tests/test_taipy_pages.py -x -q 2>&1 | tail -20`
Expected: No regressions introduced.

---

## Task 11: Commit

- [ ] **Step 1: Stage all new and modified files**

```bash
git add \
  pyproject.toml uv.lock \
  src/ingestion/tracking_context.py \
  src/tests/test_tracking_context_schema_parity.py \
  src/tests/test_tracking_context_enrichment.py \
  src/ingestion/guards.py \
  workflow-cards/wf-tracking-context.yaml \
  dbt_project/models/staging/tracking_context/ \
  dbt_project/models/marts/fct_tracking_context.sql \
  dbt_project/models/marts/_marts__models.yml \
  scripts/publish_tracking_context_hf.py \
  docs/huggingface/dataset-cards/spadl-tracking-context.md \
  src/tests/test_staging_coverage.py \
  src/tests/test_card_parity_with_terraform.py \
  scripts/create_indexes.py \
  docs/superpowers/specs/2026-05-11-tracking-context-design.md \
  src/shared/wheel.py
```

- [ ] **Step 2: Verify diff looks correct**

Run: `git diff --cached --stat`
Expected: ~15 files changed, all TC-1 related.

- [ ] **Step 3: USER APPROVAL REQUIRED — Commit**

```bash
git commit -m "$(cat <<'EOF'
feat(tracking-context): TC-1 unified action-coupled tracking features table

Add bronze.spadl_tracking_context with all 15 silly-kicks enrichments
(83 columns) across IDSSE, Metrica, and SkillCorner providers. Includes
dbt staging view + gold mart (fct_tracking_context) with Kimball FKs,
workflow card, skip guard, HF dataset card + publisher, Lakebase indexes.

silly-kicks pin bumped to >=3.11.2 (provenance skip guard fix).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Deploy + verify on Databricks

- [ ] **Step 1: Push branch and trigger CI**

USER APPROVAL REQUIRED — push to remote.

- [ ] **Step 2: Run pipeline on Databricks**

After CI passes, trigger the tracking context pipeline:

```bash
databricks jobs run-now 302697362345215 --no-wait
```

Or run directly via the entry point in a Databricks notebook.

- [ ] **Step 3: Verify bronze table**

```sql
SELECT data_source, count(*) as n_actions
FROM soccer_analytics.bronze.spadl_tracking_context
GROUP BY data_source
```

Expected: Rows for idsse, metrica, skillcorner.

- [ ] **Step 4: Run dbt build for TC-1 mart**

```bash
dbt build --select fct_tracking_context --full-refresh
```

- [ ] **Step 5: Verify gold mart**

```sql
SELECT data_source, count(*) as n, count(match_key) as n_match_key
FROM soccer_analytics.dev_gold.fct_tracking_context
GROUP BY data_source
```

Expected: All rows have non-null match_key.

- [ ] **Step 6: Create Lakebase synced table**

Via Databricks UI: create synced table from `fct_tracking_context` with PK `(tracking_context_id)`. Then run:

```bash
uv run python scripts/maintain_synced_tables.py --skip-refresh
```

- [ ] **Step 7: Publish HF dataset**

Run the PEP 723 publish script via HF Jobs or locally.

- [ ] **Step 8: Verify HF dataset**

```python
from datasets import load_dataset
ds = load_dataset("luxury-lakehouse/spadl-tracking-context")
print(len(ds["train"]), "rows")
print(ds["train"].column_names)
```
