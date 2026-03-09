# Ingestion Data Quality Fixes — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix two tech debt items — eliminate line-breaking duplicate risk at the ingestion layer (#8) and smooth tracking position data to reduce acceleration noise (#15).

**Architecture:** Item #8 replaces partition-level `replaceWhere` with a Delta MERGE (upsert) keyed on `event_id` in the line-breaking ingestion, making deduplication structural rather than relying on downstream dbt dedup. Item #15 adds Savitzky-Golay position smoothing to IDSSE and SkillCorner ingesters before Delta write, so all downstream derivatives (speed, acceleration) are naturally cleaner. Both fixes are at the bronze ingestion layer — no dbt or Streamlit changes needed.

**Tech Stack:** Python 3.10, PySpark (Delta MERGE), scipy (`savgol_filter`), pytest

---

### Task 1: Line-Breaking Dedup — Replace `replaceWhere` with Delta MERGE

**Files:**
- Modify: `src/ingestion/utils.py` (add `merge_delta_table` helper)
- Modify: `src/ingestion/line_breaking.py:207-214, 336-343` (call `merge_delta_table` instead of `write_delta_table`)
- Test: `src/tests/test_line_breaking_ingestion.py` (new)

**Context:**

The current `write_delta_table` with `replaceWhere` on `(data_source, match_id)` is idempotent at the match level. But the unique key is `event_id` — if a partial retry within a batch somehow writes overlapping events, Delta can't detect the conflict. A MERGE (upsert) on `event_id` makes this structurally impossible: matching rows are updated, new rows are inserted.

**Step 1: Write the failing test**

Create `src/tests/test_line_breaking_ingestion.py` with a test that verifies `merge_delta_table` upserts by `event_id` (mock-based since we can't create real Delta tables in unit tests). Also test that the ingestion module calls the merge function with the correct key column.

```python
"""Tests for line-breaking ingestion dedup via Delta MERGE."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ingestion.line_breaking import _TABLE_NAME


def test_statsbomb_360_uses_merge_key() -> None:
    """Path A write calls merge_delta_table with event_id as merge key."""
    # Verifies the ingestion code passes merge_key="event_id"
    # to the merge function instead of replaceWhere.
    ...


def test_metrica_tracking_uses_merge_key() -> None:
    """Path B write calls merge_delta_table with event_id as merge key."""
    ...
```

The actual test bodies will mock Spark and verify the merge call signature.

**Step 2: Add `merge_delta_table` to utils.py**

Add a new function to `src/ingestion/utils.py` that performs a Delta MERGE (upsert):

```python
def merge_delta_table(
    df: DataFrame,
    catalog: str,
    schema: str,
    table_name: str,
    merge_key: str,
    partition_filter: str | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """Upsert rows into a Delta table using MERGE on a unique key.

    Matching rows (by merge_key) are updated; non-matching are inserted.
    Optional partition_filter narrows the scan scope for performance.
    """
```

Uses `DeltaTable.forName(spark, full_table).alias("target").merge(...)`.

**Step 3: Update line_breaking.py to use merge**

Replace both `write_delta_table` calls (lines 207-214 and 336-343) with `merge_delta_table` calls using `merge_key="event_id"`.

**Step 4: Run tests**

```bash
uv run pytest src/tests/test_line_breaking_ingestion.py -v
```

**Step 5: Run full test suite to check for regressions**

```bash
uv run pytest src/tests/ -v
uv run ruff check src/
uv run pyright src/
```

**Step 6: Commit**

```bash
git commit -m "fix: use Delta MERGE for line-breaking dedup (tech debt #8)"
```

---

### Task 2: Tracking Smoothing — Add `smooth_positions` utility

**Files:**
- Create: `src/analytics/smoothing.py` (Savitzky-Golay position smoother)
- Test: `src/tests/test_smoothing.py` (new)

**Context:**

Tracking data has sensor noise (~5-10mm RMS) in x,y positions. Frame-to-frame differencing in `fct_tracking_frames.sql` amplifies this noise 2x for speed and 4x for acceleration. Smoothing the position data once in the ingestion layer fixes all downstream derivatives.

**Step 1: Write failing tests for the smoothing function**

```python
"""Tests for analytics.smoothing — Savitzky-Golay position smoother."""

import numpy as np
import pandas as pd

from analytics.smoothing import smooth_positions


def test_smooth_positions_reduces_noise() -> None:
    """Smoothed positions have lower frame-to-frame jitter than raw."""
    ...

def test_smooth_positions_preserves_trajectory() -> None:
    """Smoothed path stays close to original (no large drift)."""
    ...

def test_smooth_positions_short_sequence_passthrough() -> None:
    """Sequences shorter than window_length are returned unmodified."""
    ...

def test_smooth_positions_handles_single_frame() -> None:
    """Single-frame sequences are returned as-is."""
    ...

def test_smooth_positions_per_player_per_period() -> None:
    """Smoothing is applied independently per (player_id, period) group."""
    ...
```

**Step 2: Implement `smooth_positions`**

```python
"""Position smoothing for tracking data using Savitzky-Golay filtering."""

from __future__ import annotations

import pandas as pd
from scipy.signal import savgol_filter


def smooth_positions(
    df: pd.DataFrame,
    window_length: int = 7,
    polyorder: int = 2,
    group_cols: tuple[str, ...] = ("player_id", "period"),
    sort_col: str = "frame",
    x_col: str = "x",
    y_col: str = "y",
) -> pd.DataFrame:
    """Apply Savitzky-Golay smoothing to x,y positions per player per period.

    Parameters match standard human kinematics: window_length=7 at 25fps
    covers ~280ms, polyorder=2 preserves true acceleration features.

    Sequences shorter than window_length are returned unmodified.
    """
```

**Step 3: Run tests**

```bash
uv run pytest src/tests/test_smoothing.py -v
```

**Step 4: Commit**

```bash
git commit -m "feat: add Savitzky-Golay position smoother for tracking data"
```

---

### Task 3: Integrate smoothing into IDSSE ingestion

**Files:**
- Modify: `src/ingestion/idsse.py:265-267` (smooth after DataFrame creation)
- Modify: `src/tests/test_idsse.py` (add smoothing integration test)

**Step 1: Write failing test**

Add a test to `src/tests/test_idsse.py` that verifies the parsed positions have been smoothed (check that the output x,y values differ from raw parsed values when noise is present).

**Step 2: Add smoothing call**

In `ingest_idsse`, after `df = pd.DataFrame(rows)` (line 266), add:

```python
from analytics.smoothing import smooth_positions
df = smooth_positions(df)
```

**Step 3: Run tests**

```bash
uv run pytest src/tests/test_idsse.py -v
```

**Step 4: Commit**

```bash
git commit -m "fix: smooth IDSSE tracking positions to reduce acceleration noise"
```

---

### Task 4: Integrate smoothing into SkillCorner ingestion

**Files:**
- Modify: `src/ingestion/skillcorner.py:167-168` (smooth after DataFrame creation)
- Modify: `src/tests/test_skillcorner.py` (add smoothing integration test)

**Step 1: Write failing test**

Add a test to `src/tests/test_skillcorner.py` that verifies smoothing is applied.

**Step 2: Add smoothing call**

In `ingest_skillcorner`, after `df = pd.DataFrame(all_rows)` (line 168), add:

```python
from analytics.smoothing import smooth_positions
df = smooth_positions(df, group_cols=("player_id", "period", "match_id"))
```

Note: SkillCorner collects all matches into one DataFrame, so `match_id` must be added to the group columns to prevent cross-match smoothing.

**Step 3: Run tests**

```bash
uv run pytest src/tests/test_skillcorner.py -v
```

**Step 4: Commit**

```bash
git commit -m "fix: smooth SkillCorner tracking positions to reduce acceleration noise"
```

---

### Task 5: Update TODO.md and run final checks

**Files:**
- Modify: `TODO.md` (mark items #8 and #15 as resolved)

**Step 1: Move items #8 and #15 to Resolved section**

**Step 2: Run full quality checks**

```bash
uv run ruff check src/
uv run ruff format --check src/
uv run pyright src/
uv run pytest src/tests/ -v
```

**Step 3: Commit**

```bash
git commit -m "docs: mark tech debt #8 and #15 as resolved"
```

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| MERGE vs replaceWhere (#8) | Delta MERGE on `event_id` | Structural dedup at write time; removes reliance on dbt `ROW_NUMBER()` |
| Keep dbt dedup (#8) | Yes, keep `stg_line_breaking__results.sql` dedup | Defense in depth — MERGE prevents new dupes, dbt catches any historical ones |
| Smoothing layer (#15) | Python ingestion, not dbt | SavGol requires scipy; dbt SQL can't do window-based polynomial fitting |
| Smooth positions, not speed (#15) | Smooth x,y once | All derivatives (speed, acceleration) are automatically cleaner |
| Window length (#15) | 7 frames (~280ms at 25fps, ~700ms at 10fps) | Standard for human kinematics; preserves real movement features |
| Polyorder (#15) | 2 (quadratic) | Preserves true acceleration while removing noise |
| Skip Metrica smoothing (#15) | Not included | Academic dataset, likely already clean; only 3 matches |
| `smooth_positions` location (#15) | `analytics/smoothing.py` | Pure analytics module, no Spark dependency, testable in isolation |
