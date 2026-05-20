# OPT-3: Spark UDF + Driver-OOM Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate latent OOM risk in `xg_model_v2` by regrouping from `competition_id` (21K shots/group) to `match_key` (25-50 shots/group), harden DEFCON input projections with module-level column constants, and add a `tracemalloc` regression guard for SPADL/VAEP.

**Architecture:** Three independent sub-items: (c) xG v2 regrouping + temp table removal (HIGH), (a) DEFCON column-constant projection hardening (LOW), (b) SPADL/VAEP tracemalloc smoke test (test-only). TDD throughout — tests first, then production changes.

**Tech Stack:** PySpark `applyInPandas`, Delta Lake `replaceWhere`, XGBoost, `tracemalloc`, `pytest`

**Spec:** `docs/superpowers/specs/2026-05-19-opt-3-spark-udf-driver-oom-hardening-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/ingestion/xg_model_v2.py` | Modify | Regroup to `match_key`, remove temp table, NULL guard, bulk write, update docstring |
| `src/ingestion/defcon_lite_common.py` | Modify | Add `_VALUE_UDF_INPUT_COLS` constant |
| `src/ingestion/defcon_lite_360.py` | Modify | Add `_CREDITS_UDF_INPUT_COLS_360` constant + `.select()` projections |
| `src/ingestion/defcon_lite_tracking.py` | Modify | Add `_CREDITS_UDF_INPUT_COLS_TRACKING` constant + `.select()` projections |
| `src/tests/test_xg_v2_regrouping.py` | Create | Regrouping correctness: row count, competition_id preservation, shot_id uniqueness, source-code guards |
| `src/tests/test_defcon_projection_parity.py` | Create | Column constants vs StructType field names parity |
| `src/tests/test_spadl_vaep_memory.py` | Create | tracemalloc smoke test at p99 group size |

---

### Task 1: xG v2 Regrouping Tests (TDD — write tests first)

**Files:**
- Create: `src/tests/test_xg_v2_regrouping.py`

- [ ] **Step 1: Write the regrouping correctness + source-code guard tests**

This test file covers:
1. Source-code assertion: `_load_shots_with_context` SQL includes `s.match_key`
2. Source-code assertion: `run_pipeline` uses `groupBy("match_key")`, not `groupBy("competition_id")`
3. Source-code assertion: no `_xg_v2_scored_temp` temp table reference
4. Source-code assertion: module docstring says "match_key" not "grouped by `competition_id`"
5. UDF correctness using the **real** `_make_v2_scoring_udf` with fixture model weights: competition_id preserved in per-match groups, shot_id unique, row count preserved. This is the load-bearing assertion — it exercises the actual UDF closure, not a mock.

```python
"""Regrouping correctness tests for xG v2 (OPT-3 sub-item c).

Verifies that the groupBy key is ``match_key`` (bounded at 25-50 shots/group)
and that the temp-table materialization hack has been removed. Also tests
UDF output preservation of ``competition_id`` across per-match groups.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Source-code structural guards
# ---------------------------------------------------------------------------


class TestXgV2SourceCodeGuards:
    """Source-code assertions that prevent regression to competition_id grouping."""

    def test_load_shots_includes_match_key(self) -> None:
        """_load_shots_with_context SQL must SELECT s.match_key."""
        from ingestion.xg_model_v2 import _load_shots_with_context

        source = inspect.getsource(_load_shots_with_context)
        assert "s.match_key" in source, (
            "_load_shots_with_context must include s.match_key in the SELECT list "
            "so that groupBy('match_key') has a column to group on."
        )

    def test_groupby_uses_match_key(self) -> None:
        """run_pipeline must groupBy('match_key'), not 'competition_id'."""
        from ingestion.xg_model_v2 import run_pipeline

        source = inspect.getsource(run_pipeline)
        assert 'groupBy("match_key")' in source, (
            "run_pipeline must use groupBy('match_key') for bounded group sizing."
        )
        assert 'groupBy("competition_id")' not in source, (
            "run_pipeline must NOT use groupBy('competition_id') — "
            "competition 11 has 21,186 shots in one group (OOM risk)."
        )

    def test_no_temp_table_reference(self) -> None:
        """run_pipeline must not reference _xg_v2_scored_temp."""
        from ingestion.xg_model_v2 import run_pipeline

        source = inspect.getsource(run_pipeline)
        assert "_xg_v2_scored_temp" not in source, (
            "Temp table materialization was a workaround for per-competition "
            "DAG re-execution. With match_key grouping + single bulk write, "
            "the temp table is unnecessary."
        )

    def test_docstring_says_match_key(self) -> None:
        """Module docstring must reference match_key, not competition_id grouping."""
        import ingestion.xg_model_v2 as mod

        docstring = mod.__doc__ or ""
        assert "match_key" in docstring, (
            "Module docstring must mention 'match_key' as the grouping key."
        )
        assert "grouped by `competition_id`" not in docstring, (
            "Module docstring still says 'grouped by competition_id' — stale."
        )


# ---------------------------------------------------------------------------
# UDF correctness: competition_id preserved across per-match groups
# ---------------------------------------------------------------------------


def _make_synthetic_shots(n: int = 500, *, random_state: int = 42) -> pd.DataFrame:
    """Create realistic synthetic shot data for testing.

    Mirrors ``test_xg_model_v2._make_synthetic_shots`` but adds ``match_key``
    and ``shot_id`` columns needed for regrouping tests.
    """
    rng = np.random.default_rng(random_state)

    body_parts = ["Right Foot", "Left Foot", "Head"]
    techniques = ["Normal", "Volley", "Half Volley", "Overhead Kick"]
    shot_types = ["Open Play", "Free Kick", "Penalty", "Corner"]
    play_patterns: list[str | None] = ["Regular Play", "From Corner", "From Free Kick", None]

    distance = rng.uniform(5, 50, n)
    angle = rng.uniform(0.05, 1.5, n)
    base_prob = np.clip(0.3 - 0.005 * distance + 0.1 * angle, 0.02, 0.95)
    is_goal = rng.binomial(1, base_prob)

    return pd.DataFrame(
        {
            "shot_id": [f"shot_{i}" for i in range(n)],
            "competition_id": rng.choice([11, 2, 7], n),
            "match_key": rng.choice([1001, 1002, 1003, 2001, 2002, 2003], n),
            "player_id": rng.integers(1000, 9999, n),
            "team_id": rng.choice([10, 20, 30, 40], n),
            "distance_to_goal": distance,
            "shot_angle": angle,
            "location_x": rng.uniform(90, 120, n),
            "location_y": rng.uniform(10, 70, n),
            "end_location_x": rng.uniform(118, 121, n),
            "end_location_y": rng.uniform(30, 50, n),
            "period": rng.choice([1, 2], n),
            "minute": rng.integers(0, 90, n),
            "is_first_time": rng.choice(np.array([True, False, None], dtype=object), n),
            "shot_body_part": rng.choice(body_parts, n),
            "shot_technique": rng.choice(techniques, n),
            "shot_type": rng.choice(shot_types, n),
            "play_pattern": rng.choice(np.array(play_patterns, dtype=object), n),
            "is_goal": is_goal,
            "data_source": ["statsbomb"] * n,
            "shot_freeze_frame": [None] * n,
        }
    )


class TestUdfPreservesCompetitionId:
    """applyInPandas with groupBy('match_key') must preserve competition_id."""

    def test_competition_id_preserved_per_shot(self) -> None:
        """Every shot's competition_id must survive the UDF round-trip."""
        from analytics.xg_model import (
            XGModelConfig,
            build_features,
            serialize_xgboost_model,
            train_xgboost_model,
        )

        # Train a tiny XGBoost model for the UDF
        train_shots = _make_synthetic_shots(100, random_state=0)
        config = XGModelConfig()
        x, y = build_features(train_shots, config)
        model = train_xgboost_model(x, y, config)
        xgboost_bytes = serialize_xgboost_model(model)

        # Build dummy v2 weights
        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        cc = next(iter(model.calibrated_classifiers_))
        xgb_features = list(cc.estimator.get_booster().feature_names)
        tabular_dim = len(xgb_features)

        # Inline minimal v2 weights builder (avoid cross-test import coupling)
        import json

        v2_weights = {
            "set_encoder": np.zeros((11, 64)).tolist(),
            "attention": np.zeros((64, 64)).tolist(),
            "fc1": np.zeros((tabular_dim + 64, 128)).tolist(),
            "fc1_bias": np.zeros(128).tolist(),
            "fc2": np.zeros((128, 1)).tolist(),
            "fc2_bias": np.zeros(1).tolist(),
            "feature_names": [f"feat_{i}" for i in range(tabular_dim)],
            "tabular_dim": tabular_dim,
        }
        v2_weights_bytes = json.dumps(v2_weights).encode("utf-8")

        scoring_udf = _make_v2_scoring_udf(v2_weights_bytes, xgboost_bytes)

        # Build a test DataFrame: 3 competitions, 2 matches each, ~5 shots/match
        test_shots = _make_synthetic_shots(30, random_state=99)
        # Assign deterministic match_key → competition_id mapping
        test_shots["match_key"] = [1001, 1002, 2001, 2002, 3001, 3002] * 5
        test_shots["competition_id"] = test_shots["match_key"].map(
            {1001: 11, 1002: 11, 2001: 2, 2002: 2, 3001: 7, 3002: 7}
        )

        # Simulate what groupBy("match_key").applyInPandas does:
        # call the UDF once per match_key group
        results = []
        for _mk, group in test_shots.groupby("match_key"):
            result = scoring_udf(group)
            results.append(result)
        output = pd.concat(results, ignore_index=True)

        # Assert: output row count == input row count
        assert len(output) == len(test_shots), (
            f"Row count mismatch: input={len(test_shots)}, output={len(output)}"
        )

        # Assert: every competition_id from input appears in output
        assert set(output["competition_id"]) == set(test_shots["competition_id"]), (
            "competition_id values in output don't match input"
        )

        # Assert: no duplicate shot_ids (uniqueness preserved)
        assert output["shot_id"].is_unique, "Duplicate shot_ids in output"

        # Assert: competition_id values match per shot_id
        input_map = dict(zip(test_shots["shot_id"], test_shots["competition_id"], strict=True))
        output_map = dict(zip(output["shot_id"], output["competition_id"], strict=True))
        for sid in input_map:
            assert input_map[sid] == output_map[sid], (
                f"competition_id mismatch for shot {sid}: "
                f"input={input_map[sid]}, output={output_map[sid]}"
            )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest src/tests/test_xg_v2_regrouping.py -v`
Expected: `TestXgV2SourceCodeGuards` tests FAIL (source code hasn't been changed yet). `TestUdfPreservesCompetitionId` may pass since the UDF already reads `pdf["competition_id"]` — that's fine, it's a permanent regression guard.

- [ ] **Step 3: Commit the test file**

```bash
git add src/tests/test_xg_v2_regrouping.py
git commit -m "test(xg-v2): add regrouping correctness + source-code guards (OPT-3c, red)"
```

---

### Task 2: xG v2 Production Changes (make tests pass)

**Files:**
- Modify: `src/ingestion/xg_model_v2.py`

- [ ] **Step 1: Update module docstring (line 6)**

Change line 6 from:
```
``stg_statsbomb__events``, grouped by ``competition_id`` on Spark executors.
```
to:
```
``stg_statsbomb__events``, grouped by ``match_key`` on Spark executors.
```

- [ ] **Step 2: Add `s.match_key` to the SELECT in `_load_shots_with_context` (line 183)**

Add `s.match_key,` after `s.data_source,` so the SELECT becomes:

```python
    query = f"""
        SELECT s.shot_id, s.competition_id, s.player_id, s.team_id,
               s.location_x, s.location_y, s.end_location_x, s.end_location_y,
               s.distance_to_goal, s.shot_angle, s.shot_body_part, s.shot_technique,
               s.shot_type, s.play_pattern, s.is_first_time, s.period, s.minute,
               s.is_goal, s.data_source,
               s.match_key,
               e.shot_freeze_frame
        FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
        ...
```

- [ ] **Step 3: Replace groupBy + temp table + per-competition loop with NULL guard + bulk write**

Replace lines 427-462 (from `# 5. Build UDF` through `logger.debug(...)`) with:

```python
    # 5. Build UDF and distribute scoring across executors
    scoring_udf = _make_v2_scoring_udf(v2_weights_bytes, xgboost_bytes)

    # Defense-in-depth: NULL match_key would silently create one shared group
    # containing all unmatched shots -- recreating the exact OOM this fix
    # eliminates.  The dbt surrogate key macro guarantees non-NULL today, but
    # this guard prevents silent reintroduction of the OOM class.
    null_count = shots_filtered.where("match_key IS NULL").count()
    if null_count > 0:
        logger.error("match_key IS NULL for %d shots -- invariant broken", null_count)
        raise RuntimeError(f"{null_count} shots have NULL match_key")

    output_schema = "shot_id STRING, competition_id INT, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE"
    scored_df = shots_filtered.groupBy("match_key").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # 6. Single bulk write for all new competitions (replaceWhere on competition_id).
    # More atomic than the previous per-competition loop -- either the whole
    # write succeeds or fails.  On failure, the guard re-discovers the same
    # competition set on retry.
    new_comp_list = ", ".join(str(c) for c in new_comps)
    row_count = write_delta_table(
        scored_df,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"competition_id IN ({new_comp_list})",
        logger=logger,
    )
    logger.info("Wrote %d v2 predictions across %d competitions", row_count, len(new_comps))
    return 0
```

- [ ] **Step 4: Run the regrouping tests to verify they pass**

Run: `uv run pytest src/tests/test_xg_v2_regrouping.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run existing xG v2 tests to verify no regression**

Run: `uv run pytest src/tests/test_xg_model_v2.py -v`
Expected: ALL PASS (UDF is unchanged, only grouping + write path changed)

- [ ] **Step 6: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/xg_model_v2.py && uv run pyright src/ingestion/xg_model_v2.py`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/xg_model_v2.py
git commit -m "fix(xg-v2): regroup to match_key, remove temp table, bulk write (OPT-3c)"
```

---

### Task 3: DEFCON Input Projection Tests (TDD — write tests first)

**Files:**
- Create: `src/tests/test_defcon_projection_parity.py`

- [ ] **Step 1: Write the projection parity tests**

These tests assert that each module-level column constant matches the corresponding StructType field names. Same pattern as `test_defcon_schema_parity.py`.

```python
"""Input-projection parity: column constants must match StructType field names.

OPT-3 sub-item (a). Each module-level ``_*_INPUT_COLS`` constant declares which
columns the UDF closure actually reads from the joined DataFrame. The test
asserts parity between the constant and the corresponding ``StructType`` so
that column-list drift between the join output and what the UDF reads is
caught at test time, not at Spark execution time.

Same defensive pattern as ``test_defcon_schema_parity.py`` and
``test_spadl_vaep_writer_parity.py``.
"""

from __future__ import annotations

import pytest


def _replay_credits_schema_360():
    """Replay the credits_schema StructType from defcon_lite_360."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    StructField = pyspark_types.StructField
    StructType = pyspark_types.StructType
    DoubleType = pyspark_types.DoubleType
    LongType = pyspark_types.LongType
    StringType = pyspark_types.StringType

    return StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", LongType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("offensive_value", DoubleType(), nullable=True),
            StructField("vaep_target", DoubleType(), nullable=True),
        ]
    )


def _replay_credits_schema_tracking():
    """Replay the credits_schema StructType from defcon_lite_tracking.

    Identical to 360 credits_schema — both Pass 1 outputs share the same shape.
    """
    return _replay_credits_schema_360()


def _replay_valued_schema():
    """Replay the valued_schema StructType (shared by 360 + tracking Pass 2)."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    StructField = pyspark_types.StructField
    StructType = pyspark_types.StructType
    DoubleType = pyspark_types.DoubleType
    FloatType = pyspark_types.FloatType
    LongType = pyspark_types.LongType
    StringType = pyspark_types.StringType

    return StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", LongType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("defcon_value", FloatType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )


class TestCreditsInputCols360:
    """_CREDITS_UDF_INPUT_COLS_360 must match the joined DF column names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_360 import _CREDITS_UDF_INPUT_COLS_360

        assert isinstance(_CREDITS_UDF_INPUT_COLS_360, tuple)
        assert len(_CREDITS_UDF_INPUT_COLS_360) > 0

    def test_constant_covers_pre_join_columns(self) -> None:
        """Every column in the constant must be one of the pre-join aliases."""
        from ingestion.defcon_lite_360 import _CREDITS_UDF_INPUT_COLS_360

        # 360 join output: 10 act_* + 4 ff_* base + 2 ff_velocity + 1 ff_player_id + 1 ff_team_id = 18
        # But ff_event_id is dropped by the join. So: 10 act + 3 ff_base + 2 ff_velocity + 1 ff_player_id + 1 ff_team_id = 17
        expected_act = {
            "act_event_id", "act_match_id", "act_competition_id", "act_season_id",
            "act_player_id", "act_team_id", "act_action_type",
            "act_start_x", "act_start_y", "act_offensive_value",
        }
        expected_ff = {
            "ff_teammate", "ff_x", "ff_y",
            "ff_velocity_x", "ff_velocity_y", "ff_player_id", "ff_team_id",
        }
        expected_all = expected_act | expected_ff
        actual = set(_CREDITS_UDF_INPUT_COLS_360)
        assert actual == expected_all, (
            f"Column mismatch.\n"
            f"  Missing from constant: {expected_all - actual}\n"
            f"  Extra in constant: {actual - expected_all}"
        )


class TestCreditsInputColsTracking:
    """_CREDITS_UDF_INPUT_COLS_TRACKING must match the joined DF column names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_tracking import _CREDITS_UDF_INPUT_COLS_TRACKING

        assert isinstance(_CREDITS_UDF_INPUT_COLS_TRACKING, tuple)
        assert len(_CREDITS_UDF_INPUT_COLS_TRACKING) > 0

    def test_constant_covers_pre_join_columns(self) -> None:
        """Every column in the constant must be one of the pre-join aliases."""
        from ingestion.defcon_lite_tracking import _CREDITS_UDF_INPUT_COLS_TRACKING

        # Tracking join output: 10 act_* + 8 trk_* (trk_match_id dropped by join) = 18
        expected_act = {
            "act_event_id", "act_match_id", "act_competition_id", "act_season_id",
            "act_player_id", "act_team_id", "act_action_type",
            "act_start_x", "act_start_y", "act_offensive_value",
        }
        expected_trk = {
            "trk_player_id", "trk_team", "trk_x", "trk_y",
            "trk_velocity_x", "trk_velocity_y", "trk_frame", "trk_period",
        }
        expected_all = expected_act | expected_trk
        actual = set(_CREDITS_UDF_INPUT_COLS_TRACKING)
        assert actual == expected_all, (
            f"Column mismatch.\n"
            f"  Missing from constant: {expected_all - actual}\n"
            f"  Extra in constant: {actual - expected_all}"
        )


class TestValueUdfInputCols:
    """_VALUE_UDF_INPUT_COLS must match the credits_schema field names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        assert isinstance(_VALUE_UDF_INPUT_COLS, tuple)
        assert len(_VALUE_UDF_INPUT_COLS) > 0

    def test_constant_matches_credits_schema_fields(self) -> None:
        """Pass 2 input is Pass 1 output (credits_schema). Column constant must match."""
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        credits_fields = {f.name for f in _replay_credits_schema_360().fields}
        actual = set(_VALUE_UDF_INPUT_COLS)
        assert actual == credits_fields, (
            f"Column mismatch with credits_schema.\n"
            f"  Missing from constant: {credits_fields - actual}\n"
            f"  Extra in constant: {actual - credits_fields}"
        )

    def test_constant_matches_udf_empty_cols(self) -> None:
        """_VALUE_UDF_INPUT_COLS must match the _empty_cols inside _make_values_udf.

        The UDF's _empty_cols (which defines the output columns) is a DIFFERENT
        set than the input columns (it has defcon_value + data_source, but no
        offensive_value or vaep_target). This test verifies the INPUT constant
        matches the credits_schema, not the output columns.
        """
        # This is already covered by test_constant_matches_credits_schema_fields,
        # included for explicitness.
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        # The input to Pass 2 is the full credits_schema output from Pass 1
        credits_fields = {f.name for f in _replay_credits_schema_360().fields}
        assert set(_VALUE_UDF_INPUT_COLS) == credits_fields
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest src/tests/test_defcon_projection_parity.py -v`
Expected: `test_constant_exists` tests FAIL with `ImportError` (constants don't exist yet)

- [ ] **Step 3: Commit the test file**

```bash
git add src/tests/test_defcon_projection_parity.py
git commit -m "test(defcon): add input-projection parity tests (OPT-3a, red)"
```

---

### Task 4: DEFCON Production Changes (make tests pass)

**Files:**
- Modify: `src/ingestion/defcon_lite_common.py`
- Modify: `src/ingestion/defcon_lite_360.py`
- Modify: `src/ingestion/defcon_lite_tracking.py`

- [ ] **Step 1: Add `_VALUE_UDF_INPUT_COLS` to `defcon_lite_common.py`**

Add after line 80 (after `_try_load_champion_defcon` function, before `_make_values_udf`):

```python
# Module-level column contract: the 18 columns Pass 2 receives from Pass 1
# output (credits_schema). Guards against column-list drift between the join
# output and what the UDF actually reads. Same LL1 latent-bug class as PR #230.
_VALUE_UDF_INPUT_COLS: tuple[str, ...] = (
    "event_id", "match_id", "competition_id", "season_id",
    "defender_player_id", "defender_team_id", "defender_x", "defender_y",
    "action_player_id", "action_type", "action_x", "action_y",
    "credit_type", "confidence", "dist_to_ball", "pitch_control_at_action",
    "offensive_value", "vaep_target",
)
```

- [ ] **Step 2: Add `_CREDITS_UDF_INPUT_COLS_360` and `.select()` to `defcon_lite_360.py`**

Add the constant near the top of the file, after the imports (before `_make_credits_udf_360`):

```python
# Module-level column contract: the 17 columns the Pass 1 UDF reads from the
# joined actions x freeze-frames DataFrame. 10 act_* + 7 ff_* (ff_event_id
# dropped by the join). Guards against column-list drift.
_CREDITS_UDF_INPUT_COLS_360: tuple[str, ...] = (
    "act_event_id", "act_match_id", "act_competition_id", "act_season_id",
    "act_player_id", "act_team_id", "act_action_type",
    "act_start_x", "act_start_y", "act_offensive_value",
    "ff_teammate", "ff_x", "ff_y",
    "ff_velocity_x", "ff_velocity_y", "ff_player_id", "ff_team_id",
)
```

Then add `.select(*_CREDITS_UDF_INPUT_COLS_360)` before the Pass 1 `groupBy` at line 339. Change:

```python
    credits_sdf = joined.groupBy("act_match_id").applyInPandas(
```

to:

```python
    credits_sdf = joined.select(*_CREDITS_UDF_INPUT_COLS_360).groupBy("act_match_id").applyInPandas(
```

And add `.select(*_VALUE_UDF_INPUT_COLS)` before the Pass 2 `groupBy` at line 387. Add the import and change:

```python
    from ingestion.defcon_lite_common import _make_values_udf
```

to:

```python
    from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS, _make_values_udf
```

Then change:

```python
    valued_sdf = credits_sdf.groupBy("match_id").applyInPandas(
```

to:

```python
    valued_sdf = credits_sdf.select(*_VALUE_UDF_INPUT_COLS).groupBy("match_id").applyInPandas(
```

- [ ] **Step 3: Add `_CREDITS_UDF_INPUT_COLS_TRACKING` and `.select()` to `defcon_lite_tracking.py`**

Add the constant near the top (after imports, before `_make_credits_udf_tracking`):

```python
# Module-level column contract: the 18 columns the Pass 1 UDF reads from the
# joined actions x tracking DataFrame. 10 act_* + 8 trk_* (trk_match_id
# dropped by the join). Guards against column-list drift.
_CREDITS_UDF_INPUT_COLS_TRACKING: tuple[str, ...] = (
    "act_event_id", "act_match_id", "act_competition_id", "act_season_id",
    "act_player_id", "act_team_id", "act_action_type",
    "act_start_x", "act_start_y", "act_offensive_value",
    "trk_player_id", "trk_team", "trk_x", "trk_y",
    "trk_velocity_x", "trk_velocity_y", "trk_frame", "trk_period",
)
```

Then add `.select(*_CREDITS_UDF_INPUT_COLS_TRACKING)` before the Pass 1 `groupBy` at line 348:

```python
    credits_sdf = joined.select(*_CREDITS_UDF_INPUT_COLS_TRACKING).groupBy("act_match_id").applyInPandas(
```

And add `.select(*_VALUE_UDF_INPUT_COLS)` before the Pass 2 `groupBy` at line 390:

```python
    from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS, _make_values_udf
```

```python
    valued_sdf = credits_sdf.select(*_VALUE_UDF_INPUT_COLS).groupBy("match_id").applyInPandas(
```

**Note:** All three constants (`_VALUE_UDF_INPUT_COLS`, `_CREDITS_UDF_INPUT_COLS_360`, `_CREDITS_UDF_INPUT_COLS_TRACKING`) now exist. The `ImportError` failures from Task 3 Step 2 are resolved. The parity test below confirms each constant matches its corresponding StructType.

- [ ] **Step 4: Run the parity tests to verify they pass**

Run: `uv run pytest src/tests/test_defcon_projection_parity.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run existing DEFCON tests to verify no regression**

Run: `uv run pytest src/tests/test_defcon_schema_parity.py -v`
Expected: ALL PASS

- [ ] **Step 6: Run ruff + pyright on all 3 files**

Run: `uv run ruff check src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py && uv run pyright src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py
git commit -m "fix(defcon): add module-level column constants + input projections (OPT-3a)"
```

---

### Task 5: SPADL/VAEP Tracemalloc Smoke Test

**Files:**
- Create: `src/tests/test_spadl_vaep_memory.py`

This is a test-only task — no production code changes.

- [ ] **Step 1: Write the tracemalloc test with a placeholder threshold**

The test trains tiny XGBClassifier models on synthetic SPADL data (must use the real silly_kicks feature pipeline to get correct feature count), builds a p99-sized synthetic group (2,711 rows), runs the UDF body under `tracemalloc`, and asserts peak allocation is under a threshold.

```python
"""tracemalloc smoke test for SPADL/VAEP scoring UDF (OPT-3 sub-item b).

Verified safe: max group = 3,236 rows/match (~1 MB), already grouped by
(match_id, data_source). This test provides a regression guard — if a
future change inflates per-group memory, the test catches it before
production OOM on the 800 MB serverless cap.

Threshold derivation: measured actual peak, set at 2x measured baseline.
"""

from __future__ import annotations

import tracemalloc

import numpy as np
import pandas as pd
import pytest


def _build_synthetic_spadl_group(n_rows: int = 2711, *, random_state: int = 42) -> pd.DataFrame:
    """Build a synthetic SPADL DataFrame at p99 group size.

    Must include all columns the scoring UDF reads: the full set from
    ``_VAEP_SCHEMA`` minus ``_ingested_at`` (not passed through applyInPandas).
    """
    rng = np.random.default_rng(random_state)

    action_types = list(range(22))  # silly_kicks action type IDs
    result_ids = [0, 1]  # fail/success
    bodypart_ids = [0, 1, 2]  # foot, head, other

    return pd.DataFrame(
        {
            "game_id": np.int64(1001),
            "match_id": np.int64(1001),
            "original_event_id": [f"evt_{i}" for i in range(n_rows)],
            "period_id": rng.choice([1, 2], n_rows).astype(np.int64),
            "time_seconds": np.sort(rng.uniform(0, 5400, n_rows)),
            "team_id": rng.choice([10, 20], n_rows).astype(np.int64),
            "player_id": rng.choice(range(100, 122), n_rows).astype(np.int64),
            "start_x": rng.uniform(0, 105, n_rows),
            "start_y": rng.uniform(0, 68, n_rows),
            "end_x": rng.uniform(0, 105, n_rows),
            "end_y": rng.uniform(0, 68, n_rows),
            "type_id": rng.choice(action_types, n_rows).astype(np.int64),
            "result_id": rng.choice(result_ids, n_rows).astype(np.int64),
            "bodypart_id": rng.choice(bodypart_ids, n_rows).astype(np.int64),
            "action_id": rng.integers(0, 100000, n_rows).astype(np.int64),
            "competition_id": np.int64(11),
            "season_id": np.int64(90),
            "data_source": "statsbomb",
            # StatsBomb-native fields (NULL for non-StatsBomb, but we test StatsBomb path)
            "statsbomb_possession_id": rng.integers(1, 200, n_rows).astype(np.int64),
            "statsbomb_possession_team_id": rng.choice([10, 20], n_rows).astype(np.int64),
            "statsbomb_play_pattern": rng.choice(
                ["Regular Play", "From Corner", "From Free Kick"], n_rows
            ),
            "statsbomb_under_pressure": rng.choice([True, False, None], n_rows),
            # Enrichment columns
            "possession_id_heuristic": rng.integers(1, 200, n_rows).astype(np.int64),
            "gk_role": rng.choice(["goalkeeper", None], n_rows),
            "gk_was_distributing": rng.choice([True, False, None], n_rows),
            "gk_was_engaged": rng.choice([True, False, None], n_rows),
            "gk_actions_in_possession": rng.integers(0, 5, n_rows).astype(np.int64),
            "defending_gk_player_id": rng.choice([100, 110, None], n_rows),
            # Native string identifiers
            "team_id_native": rng.choice(["10", "20"], n_rows),
            "home_team_id_native": "10",
            "competition_native_id": "11",
            "season_native_id": "90",
            "match_id_native": "1001",
            "player_id_native": [str(rng.integers(100, 122)) for _ in range(n_rows)],
            # Tackle qualifier columns (NULL for StatsBomb)
            "tackle_winner_player_id_native": None,
            "tackle_winner_player_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_winner_team_id_native": None,
            "tackle_winner_team_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_loser_player_id_native": None,
            "tackle_loser_player_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_loser_team_id_native": None,
            "tackle_loser_team_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
        }
    )


@pytest.fixture(scope="module")
def vaep_model_bytes() -> tuple[bytes, bytes]:
    """Train tiny XGBClassifier models on synthetic SPADL data.

    Uses the real silly_kicks feature pipeline to ensure feature count
    alignment with what the production UDF expects.
    """
    import silly_kicks.spadl as spadl
    import silly_kicks.vaep.features as fs
    import silly_kicks.vaep.labels as labels
    from xgboost import XGBClassifier

    # Build a small synthetic game with enough actions for gamestates
    pdf = _build_synthetic_spadl_group(200, random_state=0)
    named = spadl.add_names(pdf)

    gamestates = fs.gamestates(named, nb_prev_actions=3)
    feature_fns = [
        fs.actiontype_onehot, fs.result_onehot, fs.bodypart_onehot,
        fs.time, fs.startlocation, fs.endlocation,
        fs.startpolar, fs.endpolar, fs.movement, fs.team, fs.time_delta,
    ]
    x = pd.concat([fn(gamestates) for fn in feature_fns], axis=1)
    y = labels.scores(named, nr_actions=10)
    y_concedes = labels.concedes(named, nr_actions=10)

    # Align lengths (labels may be shorter)
    min_len = min(len(x), len(y), len(y_concedes))
    x = x.iloc[:min_len]
    y = y.iloc[:min_len]
    y_concedes = y_concedes.iloc[:min_len]

    m_scores = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_scores.fit(x, y.values.ravel())
    scores_raw = m_scores.get_booster().save_raw("json")

    m_concedes = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_concedes.fit(x, y_concedes.values.ravel())
    concedes_raw = m_concedes.get_booster().save_raw("json")

    return scores_raw, concedes_raw


class TestSyntheticGroupColumnParity:
    """Synthetic builder must match _SPADL_SCHEMA (minus _ingested_at)."""

    def test_columns_match_spadl_schema(self) -> None:
        from ingestion.spadl_vaep import _SPADL_SCHEMA

        # _SPADL_SCHEMA is a DDL string; parse column names from it
        import re

        expected = {
            m.group(1)
            for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]+", _SPADL_SCHEMA)
        } - {"_ingested_at"}
        actual = set(_build_synthetic_spadl_group(10).columns)
        assert actual == expected, (
            f"Synthetic builder drifted from _SPADL_SCHEMA.\n"
            f"  Missing: {expected - actual}\n"
            f"  Extra: {actual - expected}"
        )


class TestSpadlVaepMemory:
    """Peak memory of VAEP scoring UDF must stay under threshold."""

    def test_peak_memory_at_p99_group_size(
        self, vaep_model_bytes: tuple[bytes, bytes]
    ) -> None:
        """Run UDF body at p99 group size (2,711 rows) under tracemalloc."""
        scores_raw, concedes_raw = vaep_model_bytes

        from ingestion.spadl_vaep import _make_scoring_udf

        scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)

        pdf = _build_synthetic_spadl_group(2711, random_state=42)

        tracemalloc.start()

        _ = scoring_udf(pdf)

        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)
        # Print so the executor can read the measured value for threshold tuning
        print(f"\n  VAEP UDF peak memory: {peak_mb:.1f} MB at 2,711 rows")

        # Empirically-derived threshold: measure actual peak first, then set
        # at 2x measured baseline. Update this comment with the measured value:
        #
        # Measured baseline: <TO BE FILLED AFTER FIRST RUN> MB
        # Threshold: 2x baseline = <TO BE FILLED> MB
        #
        # Placeholder: 200 MB (generous initial cap; tighten after measurement).
        threshold_mb = 200.0

        assert peak_mb < threshold_mb, (
            f"VAEP scoring UDF peak memory {peak_mb:.1f} MB exceeds "
            f"threshold {threshold_mb:.1f} MB at p99 group size (2,711 rows). "
            f"The 800 MB serverless cap is shared with Spark overhead, Python "
            f"runtime, and model cache — per-UDF budget must leave room."
        )
```

- [ ] **Step 2: Run the test and record the measured baseline**

Run: `uv run pytest src/tests/test_spadl_vaep_memory.py -v -s`
Expected: PASS. Read the `peak_mb` from the test output.

- [ ] **Step 3: Update the threshold to 2x measured baseline**

Edit the test: replace the placeholder `threshold_mb = 200.0` with `2 * measured_baseline`. Update the comment with the measured value.

For example, if measured peak is 35 MB:
```python
        # Measured baseline: 35 MB (2026-05-19, p99=2711 rows, XGBClassifier n_est=5)
        # Threshold: 2x baseline = 70 MB
        threshold_mb = 70.0
```

- [ ] **Step 4: Re-run to confirm it passes with the tightened threshold**

Run: `uv run pytest src/tests/test_spadl_vaep_memory.py -v`
Expected: PASS

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check src/tests/test_spadl_vaep_memory.py`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/tests/test_spadl_vaep_memory.py
git commit -m "test(spadl-vaep): add tracemalloc smoke test at p99 group size (OPT-3b)"
```

---

### Task 6: Full Verification

**Benchmarks intentionally deferred:** The spec §3 mentions `pytest-benchmark` baselines as a verification step. The UDF body (`_make_v2_scoring_udf`) is unchanged by this PR — only the grouping key and write path change. Since no hot-path code is modified, new benchmarks would measure the same code. If a future PR modifies the UDF internals, benchmarks should be added then via `mad-scientist-skills:measure-before-optimize`.

- [ ] **Step 1: Run the complete test suite**

Run: `uv run pytest src/tests/ -v`
Expected: All new tests pass. Pre-existing failures (see `memory/project_known_pretest_failures_on_main_2026_05_04.md`) are not regressions.

- [ ] **Step 2: Run ruff + pyright on all changed files**

Run:
```bash
uv run ruff check src/ingestion/xg_model_v2.py src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py src/tests/test_xg_v2_regrouping.py src/tests/test_defcon_projection_parity.py src/tests/test_spadl_vaep_memory.py
uv run pyright src/ingestion/xg_model_v2.py src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py
```
Expected: No errors

- [ ] **Step 3: Verify net line delta matches spec expectation**

Run: `git diff --stat HEAD~4` (or however many commits back to start)
Expected: ~175 lines added, ~25 removed (per spec §6).
