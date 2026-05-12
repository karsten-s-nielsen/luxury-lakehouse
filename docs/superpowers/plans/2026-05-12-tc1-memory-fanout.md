# TC-1 Memory Fix + for_each_task Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix OOM crash on Databricks serverless (16 GB driver) by projecting only needed columns before `.toPandas()` and converting the single `compute_tracking_context` task into a `preflight_tracking_context` + `for_each_task` fan-out pattern (one match per IDSSE iteration, two per Metrica/SkillCorner).

**Baseline:** The column-mapping fix (`_bronze_idsse_to_sportec_input`) shipped in commit `92bc7c7` on `main`. The daily job now passes the column-mapping stage but OOMs on memory. This plan starts from that baseline — the column mapping is NOT part of the work.

**Architecture:** Per-provider consumed-column frozensets define the minimum Spark `.select()` set. The guard partitions discovered matches into `provider:match_id1,match_id2` chunks. A new `preflight_tracking_context` entry point runs the guard and writes chunks as a Databricks task value. The existing `compute_tracking_context` becomes a `for_each_task` that spawns one serverless environment per chunk, each fitting its own xT model (~2 s, deterministic — value iteration on zone transition counts, row-order-independent) and processing only its assigned matches. `gc.collect()` between matches within each iteration reclaims intermediates.

**xT fitting trade-off:** Fitting xT per iteration costs ~20s total across 10 iterations (2s each) and ~20 MB query IO. Against the 7200s budget and $179/day warehouse cost, this is noise. Serializing the xT grid to a task value or UC Volume adds complexity (serialization, deserialization, cleanup) with no material benefit. Keeping per-iteration for simplicity.

**Commit convention:** All tasks produce a single squash commit on the feature branch (per project convention). Intermediate checkpoints during development are TDD hygiene — they get squash-merged into the PR.

**Tech Stack:** Python 3.10, PySpark, Databricks serverless `for_each_task`, Terraform HCL, silly-kicks 3.11.3, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/ingestion/tracking_context.py` | Modify | Add consumed/projection constants, staged lifecycle, `--match-ids` CLI support, guard chunking, preflight entry point |
| `src/tests/test_tracking_context_column_projection.py` | Create | Parity tests: projection ⊇ consumed for each provider |
| `src/tests/test_tracking_context_preflight.py` | Create | Unit tests for preflight, chunk serialisation, `--match-ids` parsing |
| `terraform/modules/workflows/main.tf` | Modify | Replace single `compute_tracking_context` task with `preflight_tracking_context` + `for_each_task` |
| `dbt_project/seeds/task_workflow_mapping.csv` | Modify | Add `preflight_tracking_context,wf-tracking-context` row |
| `pyproject.toml` | Modify | Add `preflight_tracking_context` entry point |

---

### Task 1: Consumed + Projection Constants with Non-Tautological Parity Tests

**Files:**
- Modify: `src/ingestion/tracking_context.py` (after line 28, before `_RESULT_COLUMNS`; and next to each `_bronze_*` converter function)
- Create: `src/tests/test_tracking_context_column_projection.py`

Each provider has TWO constants:
- **Consumed** (`_*_CONSUMED_COLS`): frozenset defined next to the converter function, used by the converter to filter `trk_pdf` at entry (runtime assertion — drift crashes immediately).
- **Projection** (`_*_TRACKING_SELECT_COLS`): tuple at module top, passed to Spark `.select()`.

The parity test asserts `projection ⊇ consumed`. This is non-tautological because:
- Consumed is enforced by the converter at runtime (adding a column without updating consumed → KeyError).
- Projection is enforced by the test (consumed grows → test fails if projection didn't grow too).

#### Consumed column derivation (verified against live code)

**IDSSE** — `_bronze_idsse_to_sportec_input` (lines 493-606) reads:
- Player rows: `match_id` (→game_id), `period` (→period_id), `frame` (→frame_id), `timestamp` (→time_seconds), `x` (→x_centered), `y` (→y_centered), `s` (→speed_native), `ball_status` (→ball_state), `frame_rate`, `player_id`, `team_id`, `is_goalkeeper`
- Ball rows: `frame`, `period`, `timestamp`, `ball_x`, `ball_y`, `ball_z`, `ball_s`, `ball_status`, `match_id`, `frame_rate`
- Union = 16 columns

**Metrica** — `_bronze_metrica_to_frames` (lines 609-708) reads:
- `gk_jersey_numbers` (line 629), `frame_rate` (line 635)
- Per-row iteration: `period` (640), `frame` (642), `timestamp` (644), `home_players` JSON (647), `away_players` JSON (647), `ball_x` (673), `ball_y` (673)
- = 9 columns. `match_id` is NOT consumed (game_id passed as parameter). `home_team_id` is NOT consumed (hardcoded as "Home" in `_process_metrica`).

**SkillCorner** — `_bronze_skillcorner_to_frames` (lines 711-785) reads:
- Player selection (730): `frame`, `period`, `timestamp`, `player_id`, `team` (**NOT** `team_id` — renamed to `team_id` inside converter), `x`, `y`, `is_goalkeeper`
- Ball selection (747): `frame`, `period`, `timestamp`, `ball_x`, `ball_y`
- `frame_rate` (727)
- = 12 columns. `home_team_id` is consumed by `_process_skillcorner` (line 900) before the converter, so included in projection. `match_id` and `is_ball` are NOT consumed.

- [ ] **Step 1: Write the failing parity tests**

Create `src/tests/test_tracking_context_column_projection.py`:

```python
"""Parity tests — projection constants ⊇ consumed constants.

Non-tautological: each consumed constant is used by its converter function
to filter trk_pdf at entry (runtime assertion). If someone adds a column
reference without updating consumed, the converter crashes at runtime.
The test then catches if projection didn't grow to match.
"""

from __future__ import annotations


def test_idsse_projection_covers_consumed() -> None:
    """_IDSSE_TRACKING_SELECT_COLS ⊇ _IDSSE_CONSUMED_COLS."""
    from ingestion.tracking_context import (
        _IDSSE_CONSUMED_COLS,
        _IDSSE_TRACKING_SELECT_COLS,
    )

    missing = _IDSSE_CONSUMED_COLS - set(_IDSSE_TRACKING_SELECT_COLS)
    assert not missing, (
        f"_IDSSE_TRACKING_SELECT_COLS missing columns from _IDSSE_CONSUMED_COLS: "
        f"{sorted(missing)}. Update the projection constant."
    )


def test_metrica_projection_covers_consumed() -> None:
    """_METRICA_TRACKING_SELECT_COLS ⊇ _METRICA_CONSUMED_COLS."""
    from ingestion.tracking_context import (
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
    )

    missing = _METRICA_CONSUMED_COLS - set(_METRICA_TRACKING_SELECT_COLS)
    assert not missing, (
        f"_METRICA_TRACKING_SELECT_COLS missing columns from _METRICA_CONSUMED_COLS: "
        f"{sorted(missing)}. Update the projection constant."
    )


def test_skillcorner_projection_covers_consumed() -> None:
    """_SKILLCORNER_TRACKING_SELECT_COLS ⊇ _SKILLCORNER_CONSUMED_COLS."""
    from ingestion.tracking_context import (
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    missing = _SKILLCORNER_CONSUMED_COLS - set(_SKILLCORNER_TRACKING_SELECT_COLS)
    assert not missing, (
        f"_SKILLCORNER_TRACKING_SELECT_COLS missing columns from "
        f"_SKILLCORNER_CONSUMED_COLS: {sorted(missing)}. Update the projection constant."
    )


def test_projection_constants_are_tuples() -> None:
    """Projection constants must be tuples (immutable), consumed must be frozensets."""
    from ingestion.tracking_context import (
        _IDSSE_CONSUMED_COLS,
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    assert isinstance(_IDSSE_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_METRICA_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_SKILLCORNER_TRACKING_SELECT_COLS, tuple)
    assert isinstance(_IDSSE_CONSUMED_COLS, frozenset)
    assert isinstance(_METRICA_CONSUMED_COLS, frozenset)
    assert isinstance(_SKILLCORNER_CONSUMED_COLS, frozenset)


def test_projection_is_not_wasteful() -> None:
    """Projection should not include columns not in consumed (catches stale entries).

    This is a soft check — extra columns waste memory but don't break correctness.
    If a column was removed from consumed, it should be removed from projection too.
    """
    from ingestion.tracking_context import (
        _IDSSE_CONSUMED_COLS,
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    for name, proj, consumed in [
        ("IDSSE", _IDSSE_TRACKING_SELECT_COLS, _IDSSE_CONSUMED_COLS),
        ("Metrica", _METRICA_TRACKING_SELECT_COLS, _METRICA_CONSUMED_COLS),
        ("SkillCorner", _SKILLCORNER_TRACKING_SELECT_COLS, _SKILLCORNER_CONSUMED_COLS),
    ]:
        extra = set(proj) - consumed
        assert not extra, (
            f"{name} projection has columns not in consumed: {sorted(extra)}. "
            f"Remove from projection or add to consumed."
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_tracking_context_column_projection.py -v`
Expected: FAIL with `ImportError` (constants don't exist yet).

- [ ] **Step 3: Add projection constants to module top and consumed constants next to converters**

**3a.** In `src/ingestion/tracking_context.py`, after line 28 (`_TABLE_NAME = "spadl_tracking_context"`), add the projection constants:

```python
# ── Column projection constants ───────────────────────────────────────
# Minimum Spark .select() set per provider. Each tuple matches the
# corresponding _*_CONSUMED_COLS frozenset defined next to the converter
# function. Parity enforced by test_tracking_context_column_projection.py.
#
# IDSSE: 31 bronze cols → 16 projected (saves ~50% driver memory on 3.1M rows).
# Metrica: 14 bronze cols → 9 projected.
# SkillCorner: 20 bronze cols → 12 projected.
#
# NOTE: Spark filter columns (match_id for all providers) do NOT need to be
# in the select — Catalyst pushes predicates below projections.

_IDSSE_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "period",
    "frame",
    "timestamp",
    "x",
    "y",
    "s",
    "ball_status",
    "frame_rate",
    "player_id",
    "team_id",
    "is_goalkeeper",
    "ball_x",
    "ball_y",
    "ball_z",
    "ball_s",
)

_METRICA_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "period",
    "frame",
    "timestamp",
    "frame_rate",
    "gk_jersey_numbers",
    "home_players",
    "away_players",
    "ball_x",
    "ball_y",
)

_SKILLCORNER_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "frame",
    "period",
    "timestamp",
    "player_id",
    "team",
    "x",
    "y",
    "is_goalkeeper",
    "frame_rate",
    "home_team_id",
    "ball_x",
    "ball_y",
)
```

**3b.** Next to `_bronze_idsse_to_sportec_input` (before line 493), add the consumed constant:

```python
_IDSSE_CONSUMED_COLS: frozenset[str] = frozenset({
    "match_id", "period", "frame", "timestamp", "x", "y", "s",
    "ball_status", "frame_rate", "player_id", "team_id", "is_goalkeeper",
    "ball_x", "ball_y", "ball_z", "ball_s",
})
"""Columns consumed by _bronze_idsse_to_sportec_input from bronze.idsse_tracking."""
```

And at the top of `_bronze_idsse_to_sportec_input`, add the runtime filter:

```python
    # Filter to consumed columns — runtime assertion against drift.
    # If a column is referenced below but missing from _IDSSE_CONSUMED_COLS,
    # this line crashes immediately with KeyError.
    trk_pdf = trk_pdf[list(_IDSSE_CONSUMED_COLS)].copy()
```

(This replaces the existing `.copy()` at line 540 — the rename already operates on `trk_pdf`, and now the copy happens here with column filtering.)

**3c.** Next to `_bronze_metrica_to_frames` (before line 609), add:

```python
_METRICA_CONSUMED_COLS: frozenset[str] = frozenset({
    "period", "frame", "timestamp", "frame_rate",
    "gk_jersey_numbers", "home_players", "away_players",
    "ball_x", "ball_y",
})
"""Columns consumed by _bronze_metrica_to_frames from bronze.metrica_tracking."""
```

And at the top of `_bronze_metrica_to_frames`, after the `import` statements:

```python
    trk_pdf = trk_pdf[list(_METRICA_CONSUMED_COLS)].copy()
```

**3d.** Next to `_bronze_skillcorner_to_frames` (before line 711), add:

```python
_SKILLCORNER_CONSUMED_COLS: frozenset[str] = frozenset({
    "frame", "period", "timestamp", "player_id", "team",
    "x", "y", "is_goalkeeper", "frame_rate",
    "ball_x", "ball_y",
})
"""Columns consumed by _bronze_skillcorner_to_frames from bronze.skillcorner_tracking.

NOTE: ``home_team_id`` is consumed by ``_process_skillcorner`` (not the converter),
so it appears in the projection constant but NOT here.
"""
```

And at the top of `_bronze_skillcorner_to_frames`:

```python
    trk_pdf = trk_pdf[list(_SKILLCORNER_CONSUMED_COLS)].copy()
```

**Important:** The SkillCorner projection includes `home_team_id` (consumed by `_process_skillcorner` line 900, before calling the converter) but the consumed constant does NOT — it only covers the converter's needs. The parity test checks `projection ⊇ consumed` (superset), so `home_team_id` being in projection but not consumed is fine. The `test_projection_is_not_wasteful` test would flag it, so update that test to accept `_process_*`-level columns:

```python
def test_projection_is_not_wasteful() -> None:
    """Projection should not include columns not consumed by converter or _process_*."""
    from ingestion.tracking_context import (
        _IDSSE_CONSUMED_COLS,
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    # SkillCorner projection includes home_team_id (consumed by _process_skillcorner,
    # not the converter). This is intentional — the projection covers both converter
    # and process-function needs.
    sc_process_extra = {"home_team_id"}

    for name, proj, consumed, process_extra in [
        ("IDSSE", _IDSSE_TRACKING_SELECT_COLS, _IDSSE_CONSUMED_COLS, set()),
        ("Metrica", _METRICA_TRACKING_SELECT_COLS, _METRICA_CONSUMED_COLS, set()),
        ("SkillCorner", _SKILLCORNER_TRACKING_SELECT_COLS, _SKILLCORNER_CONSUMED_COLS, sc_process_extra),
    ]:
        extra = set(proj) - consumed - process_extra
        assert not extra, (
            f"{name} projection has unexplained columns: {sorted(extra)}. "
            f"Remove from projection, add to consumed, or add to process_extra."
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_tracking_context_column_projection.py -v`
Expected: 5 PASS.

---

### Task 2: Apply Column Projection + Staged Lifecycle in `_process_*` Functions

**Files:**
- Modify: `src/ingestion/tracking_context.py:314-401` (`_process_idsse`)
- Modify: `src/ingestion/tracking_context.py:788-854` (`_process_metrica`)
- Modify: `src/ingestion/tracking_context.py:857-922` (`_process_skillcorner`)

Each `_process_*` currently calls `.toPandas()` with no column selection. After this task, each will `.select(*_<PROVIDER>_TRACKING_SELECT_COLS)` before `.toPandas()`, `del` intermediates after each stage, and `gc.collect()` between matches.

- [ ] **Step 1: Modify `_process_idsse` to use column projection + staged lifecycle**

Key changes to `_process_idsse`:
1. Add `import gc` at function top.
2. Change `.toPandas()` at line 338 to `.select(*_IDSSE_TRACKING_SELECT_COLS).toPandas()`.
3. Add `del trk_pdf` after `_bronze_idsse_to_sportec_input(trk_pdf)`.
4. Add `del sportec_input` after `convert_to_frames(sportec_input, ...)`.
5. Add `del frames, actions_pdf` after `_enrich_match(...)`.
6. Add `del result` after `spark.createDataFrame(result)`.
7. Add `gc.collect()` at end of match loop.

The full function body:

```python
def _process_idsse(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process IDSSE matches via sportec.convert_to_frames from bronze."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812
    from silly_kicks.tracking import PreprocessConfig
    from silly_kicks.tracking.sportec import convert_to_frames

    from ingestion.spadl_adapter import (
        adapt_idsse_events_for_silly_kicks,
        derive_idsse_home_team_start_left,
    )
    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing IDSSE match %s", match_id)

        # Load tracking frames — project only consumed columns
        trk_pdf = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_IDSSE_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for IDSSE match %s", match_id)
            continue

        # Load SPADL actions from bronze
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "idsse"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for IDSSE match %s", match_id)
            continue

        # Derive home team info from bronze events
        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
        del events_pdf, adapted_events

        # Map bronze columns to sportec EXPECTED_INPUT_COLUMNS schema
        sportec_input = _bronze_idsse_to_sportec_input(trk_pdf)
        del trk_pdf

        # Convert tracking to silly-kicks frames (105x68 LTR)
        frames, _report = convert_to_frames(
            sportec_input,
            home_team_id=home_team_id,
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        del sportec_input

        # Align game_id: sportec converter uses DFL string ID, but SPADL
        # actions carry a BIGINT hash.
        frames["game_id"] = int(actions_pdf["game_id"].iloc[0])

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="idsse",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total
```

Note: `events_pdf` is NOT projected (idsse_events is ~2000 rows/match — too few rows for projection to matter). This is a known low-priority optimization tracked as a TODO comment if desired, but not worth the added constant maintenance.

- [ ] **Step 2: Modify `_process_metrica` to use column projection + staged lifecycle**

Key changes:
1. Add `import gc`.
2. `.select(*_METRICA_TRACKING_SELECT_COLS).toPandas()`.
3. `del trk_pdf` after converter call.
4. `del frames, actions_pdf` after enrich.
5. `del result` after createDataFrame.
6. `gc.collect()` at end of loop.

```python
def _process_metrica(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process Metrica matches from bronze tables (NOT from internet)."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing Metrica match %s", match_id)

        trk_pdf = (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_METRICA_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for Metrica match %s", match_id)
            continue

        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "metrica"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for Metrica match %s", match_id)
            continue

        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_metrica_to_frames(trk_pdf, game_id=game_id)
        del trk_pdf

        home_team_id = "Home"

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="metrica",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total
```

- [ ] **Step 3: Modify `_process_skillcorner` to use column projection + staged lifecycle**

Key changes:
1. Add `import gc`.
2. `.select(*_SKILLCORNER_TRACKING_SELECT_COLS).toPandas()`.
3. Derive `home_team_id` from `trk_pdf` BEFORE calling the converter (line 900 order preserved).
4. `del trk_pdf` after converter call.
5. `del frames, actions_pdf` after enrich.
6. `del result` after createDataFrame.
7. `gc.collect()` at end of loop.

```python
def _process_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process SkillCorner matches from bronze tables (NOT from internet)."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing SkillCorner match %s", match_id)

        trk_pdf = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for SkillCorner match %s", match_id)
            continue

        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "skillcorner"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for SkillCorner match %s", match_id)
            continue

        # Derive home_team_id from projected bronze data BEFORE converter discards it
        home_team_id = str(trk_pdf["home_team_id"].dropna().iloc[0])

        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_skillcorner_to_frames(trk_pdf, game_id=game_id)
        del trk_pdf

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="skillcorner",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total
```

- [ ] **Step 4: Run existing tests**

Run: `uv run pytest src/tests/ -k "tracking_context" -v`
Expected: All existing tests PASS (no behavioral change, only memory optimisation).

---

### Task 3: Guard Chunking + `--match-ids` CLI Support

**Files:**
- Modify: `src/ingestion/tracking_context.py:928-974` (guard class + skip_guard)
- Modify: `src/ingestion/tracking_context.py` (add parse function after skip_guard)
- Create: `src/tests/test_tracking_context_preflight.py`

The guard currently returns `FilterResult` without `chunks`. This task adds chunking logic and `--match-ids` parsing so the `for_each_task` iteration can receive a subset of matches.

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_tracking_context_preflight.py`:

```python
"""Tests for tracking context preflight chunking and --match-ids parsing."""

from __future__ import annotations


def test_parse_tracking_match_ids_arg_none() -> None:
    """None input returns None (no filter)."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg(None) is None


def test_parse_tracking_match_ids_arg_empty() -> None:
    """Empty string returns None."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg("") is None


def test_parse_tracking_match_ids_arg_valid() -> None:
    """Comma-separated string returns parsed (provider, ids) tuple."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("idsse:J03WMX,J03WN1")
    assert result is not None
    assert result == ("idsse", ["J03WMX", "J03WN1"])


def test_parse_tracking_match_ids_arg_single() -> None:
    """Single match ID works."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("metrica:match_001")
    assert result is not None
    assert result == ("metrica", ["match_001"])


def test_parse_tracking_match_ids_arg_bad_format() -> None:
    """Missing provider prefix raises SystemExit."""
    import pytest

    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    with pytest.raises(SystemExit, match="must be 'provider:id1,id2'"):
        _parse_tracking_match_ids_arg("J03WMX,J03WN1")


def test_parse_tracking_match_ids_arg_unknown_provider() -> None:
    """Unknown provider raises SystemExit."""
    import pytest

    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    with pytest.raises(SystemExit, match="Unknown provider"):
        _parse_tracking_match_ids_arg("opta:12345")


def test_chunk_encoding_round_trip() -> None:
    """Chunk string 'provider:id1,id2' round-trips through parse."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    chunk_str = "idsse:J03WMX,J03WN1"
    provider, ids = _parse_tracking_match_ids_arg(chunk_str)
    assert provider == "idsse"
    assert ids == ["J03WMX", "J03WN1"]
    reconstructed = f"{provider}:{','.join(ids)}"
    assert reconstructed == chunk_str


def test_guard_chunk_sizes_are_set() -> None:
    """_TrackingContextGuard has provider-specific chunk sizes."""
    from ingestion.tracking_context import skip_guard

    assert hasattr(skip_guard, "chunk_sizes")
    assert skip_guard.chunk_sizes["idsse"] == 1
    assert skip_guard.chunk_sizes["metrica"] == 2
    assert skip_guard.chunk_sizes["skillcorner"] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_tracking_context_preflight.py -v`
Expected: FAIL with `ImportError` (functions/attributes don't exist yet).

- [ ] **Step 3: Add chunk_sizes, chunking logic, and parse function to tracking_context.py**

**3a.** Add `_VALID_PROVIDERS` frozenset and update `_TrackingContextGuard`:

Replace the `_TrackingContextGuard` class (lines 928-974) with:

```python
_VALID_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner"})


class _TrackingContextGuard:
    """SkipGuard adapter for tracking context pipeline.

    chunk_sizes: per-provider match count per for_each_task iteration.
    IDSSE = 1 match/iteration (~2.9 GB peak per match on 16 GB driver).
    Metrica/SkillCorner = 2 matches/iteration (lighter data).
    """

    workflow_id = "wf-tracking-context"
    chunk_sizes: dict[str, int] = {"idsse": 1, "metrica": 2, "skillcorner": 2}

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

        # Build chunks: each inner list has one element "provider:id1,id2".
        # main_preflight joins with "," -> same string (single element).
        # for_each_task spawns one iteration per chunk string.
        chunks: list[list[str]] = []
        for provider, ids in [("idsse", idsse_ids), ("metrica", metrica_ids), ("skillcorner", skillcorner_ids)]:
            chunk_size = self.chunk_sizes[provider]
            for i in range(0, len(ids), chunk_size):
                batch = ids[i : i + chunk_size]
                chunks.append([f"{provider}:{','.join(batch)}"])

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            chunks=chunks,
            metadata={
                "idsse_ids": idsse_ids,
                "metrica_ids": metrica_ids,
                "skillcorner_ids": skillcorner_ids,
            },
        )
```

**3b.** Add `_parse_tracking_match_ids_arg` function after `skip_guard = _TrackingContextGuard()`:

```python
def _parse_tracking_match_ids_arg(raw: str | None) -> tuple[str, list[str]] | None:
    """Parse ``--match-ids`` CLI value for tracking context iterations.

    Format: ``"provider:id1,id2"`` (e.g. ``"idsse:J03WMX,J03WN1"``).
    The provider prefix routes to the correct ``_process_*`` function.

    Returns:
        ``(provider, [id1, id2])`` tuple, or ``None`` when ``raw`` is empty.

    Raises:
        SystemExit: On missing provider prefix or unknown provider.
    """
    if raw is None or raw == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2' format, got {raw!r}. "
            f"Valid providers: {sorted(_VALID_PROVIDERS)}"
        )
    provider, ids_str = raw.split(":", 1)
    if provider not in _VALID_PROVIDERS:
        raise SystemExit(
            f"Unknown provider {provider!r} in --match-ids. "
            f"Valid providers: {sorted(_VALID_PROVIDERS)}"
        )
    ids = [mid.strip() for mid in ids_str.split(",") if mid.strip()]
    if not ids:
        return None
    return (provider, ids)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_tracking_context_preflight.py -v`
Expected: All 8 tests PASS.

---

### Task 4: Preflight + Iteration Entry Points

**Files:**
- Modify: `src/ingestion/tracking_context.py` (add `main_preflight`, `_write_tracking_chunks_task_value`; update `main`)

- [ ] **Step 1: Add `_write_tracking_chunks_task_value` helper**

Place after `_parse_tracking_match_ids_arg`, before `run_pipeline`:

```python
def _write_tracking_chunks_task_value(
    chunks_for_inputs: list[str],
    logger: logging.Logger,
) -> None:
    """Write discovered chunks as a Databricks task value.

    The downstream ``compute_tracking_context`` for_each_task reads this
    via ``"{{tasks.preflight_tracking_context.values.tracking_context_chunks}}"``.
    Empty list -> 0 iterations spawned.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            logger.warning("No active SparkSession -- task value not written")
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="tracking_context_chunks", value=chunks_for_inputs)
        logger.info(
            "Wrote task value 'tracking_context_chunks' (%d chunks)",
            len(chunks_for_inputs),
        )
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning("Task values not available (likely standalone mode) -- %s", exc)
```

- [ ] **Step 2: Add `main_preflight` entry point**

Place after `_write_tracking_chunks_task_value`:

```python
def main_preflight() -> None:
    """CLI entry point for the tracking context preflight task.

    Runs the skip guard, partitions discovered matches into fan-out chunks
    (``provider:id1,id2`` format), and writes them as a Databricks task value
    for the downstream ``compute_tracking_context`` ``for_each_task``.
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed tracking matches and emit chunks "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    logger = configure_logging("tracking_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    # Serialize each chunk as a single string (inner list is always length 1)
    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    logger.info(
        "Tracking context preflight: %d missing matches across %d chunks",
        fr.count,
        len(chunks_for_inputs),
    )

    _write_tracking_chunks_task_value(chunks_for_inputs, logger)
```

- [ ] **Step 3: Update `main()` to support `--match-ids` and fit xT per iteration**

Replace the existing `main()` function:

```python
def main() -> None:
    """CLI entry point for tracking context enrichment.

    Without ``--match-ids``: runs all providers (legacy / standalone mode).
    With ``--match-ids "provider:id1,id2"``: processes only the specified
    provider and match IDs (for_each_task iteration mode).
    """
    args = parse_ingestion_args(
        "Compute action-coupled tracking features",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2 from for_each_task"})],
    )
    logger = configure_logging("tracking_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids_parsed = _parse_tracking_match_ids_arg(getattr(args, "match_ids", None))

    if match_ids_parsed is not None:
        # for_each_task iteration mode: process one provider's chunk
        provider, ids = match_ids_parsed
        logger.info("Iteration mode: provider=%s, match_ids=%s", provider, ids)

        # Fit xT per iteration (deterministic, ~2s, no accuracy impact).
        # Value iteration on zone transition counts — row-order-independent.
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
            .select(
                "game_id", "action_id", "period_id", "time_seconds",
                "team_id", "player_id", "type_id", "result_id", "bodypart_id",
                "start_x", "start_y", "end_x", "end_y", "original_event_id",
            )
            .toPandas()
        )
        xt = ExpectedThreat().fit(spadl_pdf)
        del spadl_pdf
        logger.info("xT model fitted (grid shape %s)", xt.xT.shape)

        if provider == "idsse":
            total = _process_idsse(spark, args.catalog, args.schema, logger, xt, ids)
        elif provider == "metrica":
            total = _process_metrica(spark, args.catalog, args.schema, logger, xt, ids)
        elif provider == "skillcorner":
            total = _process_skillcorner(spark, args.catalog, args.schema, logger, xt, ids)
        else:
            raise SystemExit(f"Unknown provider: {provider}")

        logger.info("Iteration complete -- %d rows written for %s", total, provider)
    else:
        # Legacy / standalone mode: run full pipeline
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        logger.info("Starting tracking context pipeline into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
```

- [ ] **Step 4: Run all tracking_context tests**

Run: `uv run pytest src/tests/ -k "tracking_context" -v`
Expected: All tests PASS.

---

### Task 5: Terraform for_each_task + Seed CSV + pyproject.toml

**Files:**
- Modify: `terraform/modules/workflows/main.tf:502-543`
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`
- Modify: `pyproject.toml` (entry points)

- [ ] **Step 1: Replace the single `compute_tracking_context` task with for_each_task (at its alphabetical position)**

`test_workflows_tf_ordering.py` enforces alphabetical ordering of `task_key` blocks. The two new blocks go in SEPARATE alphabetical positions (same pattern as `ingest_idsse` at line 787 + `preflight_idsse` at line 932):

**1a.** Replace lines 502-543 (the existing `compute_tracking_context` block) IN PLACE with the restructured for_each_task version (still at its `c` alphabetical position):

```hcl
  # ── Task: Compute action-coupled tracking features (TC-1, for_each_task fan-out) ──
  # Each iteration processes one chunk (provider:id1,id2). IDSSE gets 1 match
  # per iteration (~2.9 GB peak); Metrica/SkillCorner get 2 matches per
  # iteration (lighter data). Each iteration fits its own xT model (~2s,
  # deterministic). concurrency=4 limits parallel serverless environments.
  task {
    task_key = "compute_tracking_context"

    depends_on {
      task_key = "preflight_tracking_context"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_tracking_context.values.tracking_context_chunks}}"
      concurrency = 4

      task {
        task_key        = "compute_tracking_context_iteration"
        timeout_seconds = 1800
        max_retries     = 1

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "compute_tracking_context"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-ids", "{{input}}",
          ]
        }

        environment_key = "analytics"
      }
    }
  }
```

**1b.** Insert `preflight_tracking_context` AFTER `preflight_idsse` (line ~947) and BEFORE `refresh_synced_tables` (line ~956) — its `p` alphabetical position:

```hcl
  # ── Task: Tracking context preflight — discover unprocessed matches + emit chunks ──
  # Runtime chunk discovery for the for_each_task fan-out. Anti-joins each
  # provider's tracking table against bronze.spadl_tracking_context, partitions
  # into provider:id1,id2 chunks (IDSSE chunk_size=1, Metrica/SkillCorner=2),
  # and writes the chunks as a Databricks task value `tracking_context_chunks`.
  task {
    task_key        = "preflight_tracking_context"
    timeout_seconds = 300
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_tracking_context"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
  }
```

- [ ] **Step 2: Add `preflight_tracking_context` to seed CSV**

In `dbt_project/seeds/task_workflow_mapping.csv`, add after line 14 (`compute_tracking_context,wf-tracking-context`):

```
preflight_tracking_context,wf-tracking-context
```

- [ ] **Step 3: Add `preflight_tracking_context` entry point to pyproject.toml**

In `pyproject.toml`, in the `[project.scripts]` section, add after the `compute_tracking_context` line:

```toml
preflight_tracking_context = "ingestion.tracking_context:main_preflight"
```

- [ ] **Step 4: Verify constraint triangle (seed ⊆ TF)**

Run: `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v -k "seed"`
Expected: PASS (or skip if env-gated). Manually verify both `preflight_tracking_context` and `compute_tracking_context` exist in seed AND TF file.

---

### Task 6: Wheel Bump + Full Test Suite + Lint

**Files:**
- Run: `scripts/bump_wheel.py` (updates `src/shared/wheel.py`, `pyproject.toml`, PEP 723 refs)

- [ ] **Step 1: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_column_projection.py src/tests/test_tracking_context_preflight.py`
Run: `uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_column_projection.py src/tests/test_tracking_context_preflight.py`
Run: `uv run pyright src/ingestion/tracking_context.py`
Expected: All clean.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=120`
Expected: All tests PASS. Key tests:
- `test_tracking_context_column_projection.py` — 5 PASS
- `test_tracking_context_preflight.py` — 8 PASS
- `test_sk3_mig_b_orchestrator_invariants.py` — seed/TF parity
- `test_wheel_constants.py` — wheel version parity

- [ ] **Step 4: Single commit on feature branch**

```bash
git add -A
git commit -m "feat(tc1): column projection + for_each_task fan-out for tracking context

Fix OOM on Databricks serverless (16 GB driver) by:
1. Projecting only consumed columns before .toPandas() (IDSSE: 16/31 cols,
   Metrica: 9/14, SkillCorner: 12/20)
2. Staged variable lifecycle with del + gc.collect() between matches
3. Converting single compute_tracking_context to preflight + for_each_task
   fan-out (IDSSE: 1 match/iter, Metrica/SkillCorner: 2 matches/iter,
   concurrency=4)

Non-tautological parity tests: consumed frozensets used by converters at
runtime, projection ⊇ consumed enforced by test.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: E2E Verification — Deploy + Run Daily Job

**Files:** None modified — verification only.

- [ ] **Step 1: Push and verify CI green**

USER APPROVAL REQUIRED for push + PR creation.

- [ ] **Step 2: Terraform apply**

The new `preflight_tracking_context` task + restructured `compute_tracking_context` for_each_task must be applied to the live Databricks job. USER APPROVAL REQUIRED for `terraform apply`.

- [ ] **Step 3: Trigger daily job and verify**

Run: `databricks jobs run-now 302697362345215 --no-wait`

Verify:
- `preflight_tracking_context` completes in <60s
- `compute_tracking_context` spawns N iterations (one per chunk)
- Each iteration completes without OOM within 1800s timeout
- All IDSSE/Metrica/SkillCorner matches produce rows in `bronze.spadl_tracking_context`

- [ ] **Step 4: Verify data quality**

```sql
SELECT data_source, COUNT(DISTINCT match_id), COUNT(*)
FROM soccer_analytics.bronze.spadl_tracking_context
GROUP BY data_source
ORDER BY data_source;
```

Expected: All three providers present with expected match counts and non-zero row counts.

---

## Self-Review Findings

1. **Spec coverage**: All approved design items covered — consumed/projection constants (Task 1), staged lifecycle (Task 2), guard chunking (Task 3), preflight entry point (Task 4), Terraform for_each_task (Task 5), wheel bump (Task 6), E2E verification (Task 7).

2. **Placeholder scan**: No TBD/TODO/placeholder language. All code blocks are complete.

3. **Type consistency**: `_parse_tracking_match_ids_arg` returns `tuple[str, list[str]] | None` — consistent across Task 3 (definition) and Task 4 (usage). `FilterResult.chunks` is `list[list[str]] | None` — matches the single-element inner list pattern. `chunk_sizes` dict keys match `_VALID_PROVIDERS` frozenset.

4. **Review findings addressed (v1 review)**:
   - Issue 1: Baseline note added — column mapping already on main.
   - Issue 2: Non-tautological tests via consumed frozensets used by converters at runtime.
   - Issue 3: xT per-iteration justified (20s/20MB overhead, deterministic, simplicity wins).
   - Issue 4: events_pdf not projected — acknowledged, low ROI (~4 MB), not blocking.
   - Issue 5: Removed wrong code / self-correction from SkillCorner.
   - Issue 6: Timeout reduced from 7200 → 1800.
   - Issue 7: Single commit convention clarified.

5. **Review finding addressed (v2 review)**:
   - Issue 8: Terraform task placement now respects alphabetical ordering — `compute_tracking_context` stays at its `c` position (lines 502+), `preflight_tracking_context` inserted at its `p` position (after `preflight_idsse`, before `refresh_synced_tables`). Matches `ingest_idsse` / `preflight_idsse` precedent.

6. **Projection constants verified against live code**:
   - Metrica: corrected from 7 → 9 cols (added home_players, away_players, gk_jersey_numbers, ball_x, ball_y; removed match_id, home_team_id).
   - SkillCorner: corrected `team_id` → `team`, removed `match_id` and `is_ball`, added `ball_x` and `ball_y`.
   - IDSSE: confirmed correct (16 cols).
   - At execution time, run `DESCRIBE bronze.<table>` to verify bronze schema hasn't drifted.
