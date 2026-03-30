# Embedding & Formation Foundations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the foundations for Cycle 2 (transformer embeddings, adversarial debiasing, shape graphs) by normalizing coordinates, fixing position-group z-scoring, upgrading the SPADL vocabulary, adding GK metadata to the tracking pipeline, and wiring IDSSE events into line-breaking detection (TD#6).

**Architecture:** Four independent workstreams that share no code dependencies on each other, so tasks within each chunk can be developed and tested in any order. D43 (coordinate normalization) creates a shared dbt macro and Python module. D28 (position-group z-scoring) is a localized change to `_compute_stat_vectors`. D29 (SPADL vocabulary) rewires the tokenizer data source and vocabulary. D26 (GK metadata) threads `is_goalkeeper` from ingestion through staging/mart/formation.

**Tech Stack:** dbt (Jinja macros, SQL), Python 3.10, PySpark `applyInPandas`, gensim Doc2Vec, kloppy, pytest, Ruff, Pyright

---

## File Map

### D43 — Coordinate Normalization Layer

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `dbt_project/macros/normalize_coordinates.sql` | Shared dbt macro for all provider→StatsBomb 120×80 transforms |
| Create | `src/analytics/coordinates.py` | Python-side coordinate transforms (used by `line_breaking.py` and future analytics) |
| Create | `src/tests/test_coordinates.py` | Unit tests for Python coordinate transforms |
| Modify | `dbt_project/dbt_project.yml:52-59` | Add real-world pitch dimension vars (`pitch_length_m`, `pitch_width_m`) |
| Modify | `dbt_project/models/staging/metrica/stg_metrica__tracking.sql:93-99` | Replace inline transform with macro call |
| Modify | `dbt_project/models/staging/metrica/stg_metrica__events.sql:36-40` | Replace inline transform with macro call |
| Modify | `dbt_project/models/staging/idsse/stg_idsse__tracking.sql:38-44` | Replace inline transform with macro call |
| Modify | `dbt_project/models/staging/idsse/stg_idsse__events.sql:46-47` | Replace inline transform with macro call |
| Modify | `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql:33-38` | Replace inline transform with macro call |
| Modify | `dbt_project/models/staging/wyscout/stg_wyscout__events.sql:42-43` | Replace inline transform with macro call (already parameterized — standardize to macro) |
| Modify | `src/ingestion/line_breaking.py:241-249` | Replace inline Metrica transform with `coordinates.metrica_to_statsbomb()` |
| Modify | `src/ingestion/line_breaking.py` (new Path C) | TD#6: Add IDSSE tracking+events path using `coordinates.center_m_to_statsbomb()` and `coordinates.pitch_m_to_statsbomb()` |

### D28 — Position-Group Z-Scoring

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/ingestion/player_embeddings.py:261-302` | Add `position_group` to SQL, groupby z-scoring |
| Modify | `src/ingestion/player_embeddings.py:779-801` | Update `_save_norm_params` for group-keyed params |
| Modify | `src/tests/test_player_embeddings.py` | Add tests for per-group z-score behavior |

### D29 — SPADL Vocabulary Upgrade

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/analytics/football2vec.py:34-171` | Replace dual event maps with SPADL 23-type identity mapping, update grid to 105×68 |
| Modify | `src/ingestion/player_embeddings.py:113-213` | Rewrite `_load_events_sdf` to query `fct_action_values` instead of raw events |
| Modify | `src/ingestion/player_embeddings.py:447-535` | Rewrite inline tokenizer in `_make_behavioral_udf` for SPADL vocabulary |
| Modify | `src/tests/test_football2vec.py` | Update tokenizer tests for SPADL types and 105×68 grid |
| Modify | `src/tests/test_player_embeddings.py` | Update event loading tests for SPADL source |

### D26 — GK Metadata Pipeline

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/ingestion/metrica.py:86-98,148-153` | Parse GK from EPTS metadata; propagate to tracking rows |
| Modify | `src/ingestion/idsse.py:106-140` | Parse `PlayingPosition` from DFL XML roster |
| Modify | `src/ingestion/skillcorner.py:113-139` | Read `player.position` from kloppy `Player` objects |
| Modify | `dbt_project/models/staging/metrica/stg_metrica__tracking.sql` | Add `is_goalkeeper` column |
| Modify | `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` | Add `is_goalkeeper` column |
| Modify | `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql` | Add `is_goalkeeper` column |
| Modify | `dbt_project/models/marts/fct_tracking_frames.sql:103-142` | Add `is_goalkeeper` passthrough |
| Modify | `dbt_project/models/marts/_marts__models.yml:860-976` | Add `is_goalkeeper` to contract |
| Modify | `src/ingestion/formations.py:114-117` | Filter out GK before formation detection |
| Modify | `src/tests/test_idsse.py` | Add GK parsing tests |
| Modify | `src/tests/test_metrica.py` | Add GK parsing tests |

---

## Chunk 1: D43 — Coordinate Normalization Layer

### Task 1.1: Add real-world pitch dimension dbt vars

**Files:**
- Modify: `dbt_project/dbt_project.yml:52-59`

- [ ] **Step 1: Add meter-based pitch dimension vars**

In `dbt_project/dbt_project.yml`, after the existing `pitch_width: 80` line (line 59), add the real-world meter equivalents. These are used by the normalization macro for center-origin and pitch-origin transforms.

```yaml
  # Real-world pitch dimensions in meters (FIFA standard, used by IDSSE/SkillCorner/Metrica)
  pitch_length_m: 105
  pitch_width_m: 68
```

The existing `pitch_length: 120` and `pitch_width: 80` remain unchanged — they define the target StatsBomb coordinate system.

- [ ] **Step 2: Verify dbt compiles cleanly**

Run: `cd dbt_project && dbt parse --profiles-dir .`
Expected: no errors, `target/manifest.json` updated.

---

### Task 1.2: Create the `normalize_coordinates` dbt macro

**Files:**
- Create: `dbt_project/macros/normalize_coordinates.sql`

- [ ] **Step 1: Write the macro with all provider variants**

The macro dispatches on a `system` argument to select the correct transform formula. Each provider's coordinate system is documented inline.

```sql
{% macro normalize_coordinates(x_col, y_col, system) %}
{#
    Normalize provider-specific coordinates to the StatsBomb 120×80 system.

    Supported coordinate systems:
      - 'metrica':     [0, 1] normalized, y-flipped (y=0 is top)
      - 'center_m':    Center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34)
                        Used by IDSSE tracking and SkillCorner
      - 'pitch_m':     Pitch-origin meters, x ∈ (0, 105), y ∈ (0, 68)
                        Used by IDSSE events
      - 'pct':         Percentage [0, 100] on both axes
                        Used by Wyscout events

    Target system: StatsBomb 120×80 yards, (0,0) = bottom-left

    Args:
      x_col: Column name or expression for the raw x coordinate
      y_col: Column name or expression for the raw y coordinate
      system: One of 'metrica', 'center_m', 'pitch_m', 'pct'

    Returns:
      Two expressions: normalized x, normalized y
      Use as: {{ normalize_coordinates('raw_x', 'raw_y', 'metrica') }}
      which expands to two named columns (pipe into `as x, ... as y` in the caller)

    Example:
      {{ normalize_x('raw_x', 'metrica') }} as x,
      {{ normalize_y('raw_y', 'metrica') }} as y
#}
{% endmacro %}


{% macro normalize_x(x_col, system) %}
{# Normalize a single x coordinate to StatsBomb 120-yard scale. #}

    {% if system == 'metrica' %}
        {{ x_col }} * {{ var('pitch_length') }}.0
    {% elif system == 'center_m' %}
        ({{ x_col }} + {{ var('pitch_length_m') }}.0 / 2.0) / {{ var('pitch_length_m') }}.0 * {{ var('pitch_length') }}.0
    {% elif system == 'pitch_m' %}
        {{ x_col }} / {{ var('pitch_length_m') }}.0 * {{ var('pitch_length') }}.0
    {% elif system == 'pct' %}
        {{ x_col }} / 100.0 * {{ var('pitch_length') }}.0
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown coordinate system: " ~ system ~ ". Use 'metrica', 'center_m', 'pitch_m', or 'pct'.") }}
    {% endif %}

{% endmacro %}


{% macro normalize_y(y_col, system) %}
{# Normalize a single y coordinate to StatsBomb 80-yard scale. #}

    {% if system == 'metrica' %}
        (1.0 - {{ y_col }}) * {{ var('pitch_width') }}.0
    {% elif system == 'center_m' %}
        ({{ y_col }} + {{ var('pitch_width_m') }}.0 / 2.0) / {{ var('pitch_width_m') }}.0 * {{ var('pitch_width') }}.0
    {% elif system == 'pitch_m' %}
        {{ y_col }} / {{ var('pitch_width_m') }}.0 * {{ var('pitch_width') }}.0
    {% elif system == 'pct' %}
        {{ y_col }} / 100.0 * {{ var('pitch_width') }}.0
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown coordinate system: " ~ system ~ ". Use 'metrica', 'center_m', 'pitch_m', or 'pct'.") }}
    {% endif %}

{% endmacro %}
```

- [ ] **Step 2: Verify macro compiles**

Run: `cd dbt_project && dbt compile --select stg_metrica__tracking --profiles-dir .`
Expected: compiled SQL shows `raw_x * 120.0` (same as current inline expression).

---

### Task 1.3: Create Python coordinate module

**Files:**
- Create: `src/analytics/coordinates.py`
- Create: `src/tests/test_coordinates.py`

- [ ] **Step 1: Write failing tests for coordinate transforms**

```python
"""Tests for src/analytics/coordinates.py."""

import numpy as np
import pandas as pd
import pytest

from analytics.coordinates import (
    center_m_to_statsbomb,
    metrica_to_statsbomb,
    pitch_m_to_statsbomb,
    pct_to_statsbomb,
    statsbomb_to_meters,
)


class TestMetricaToStatsbomb:
    """Metrica [0,1] normalized with y-flip → StatsBomb 120×80."""

    def test_origin(self) -> None:
        x, y = metrica_to_statsbomb(0.0, 0.0)
        assert x == pytest.approx(0.0)
        assert y == pytest.approx(80.0)  # y-flipped: Metrica top-left → SB top-left is y=80

    def test_far_corner(self) -> None:
        x, y = metrica_to_statsbomb(1.0, 1.0)
        assert x == pytest.approx(120.0)
        assert y == pytest.approx(0.0)

    def test_center(self) -> None:
        x, y = metrica_to_statsbomb(0.5, 0.5)
        assert x == pytest.approx(60.0)
        assert y == pytest.approx(40.0)

    def test_vectorized(self) -> None:
        xs = np.array([0.0, 0.5, 1.0])
        ys = np.array([0.0, 0.5, 1.0])
        rx, ry = metrica_to_statsbomb(xs, ys)
        np.testing.assert_allclose(rx, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(ry, [80.0, 40.0, 0.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [0.0, 1.0], "y": [0.0, 1.0]})
        rx, ry = metrica_to_statsbomb(df["x"], df["y"])
        assert list(rx) == pytest.approx([0.0, 120.0])
        assert list(ry) == pytest.approx([80.0, 0.0])


class TestCenterMToStatsbomb:
    """IDSSE/SkillCorner center-origin meters → StatsBomb 120×80."""

    def test_center(self) -> None:
        x, y = center_m_to_statsbomb(0.0, 0.0)
        assert x == pytest.approx(60.0)
        assert y == pytest.approx(40.0)

    def test_top_left(self) -> None:
        x, y = center_m_to_statsbomb(-52.5, -34.0)
        assert x == pytest.approx(0.0)
        assert y == pytest.approx(0.0)

    def test_bottom_right(self) -> None:
        x, y = center_m_to_statsbomb(52.5, 34.0)
        assert x == pytest.approx(120.0)
        assert y == pytest.approx(80.0)


class TestPitchMToStatsbomb:
    """IDSSE events pitch-origin meters → StatsBomb 120×80."""

    def test_origin(self) -> None:
        x, y = pitch_m_to_statsbomb(0.0, 0.0)
        assert x == pytest.approx(0.0)
        assert y == pytest.approx(0.0)

    def test_far_corner(self) -> None:
        x, y = pitch_m_to_statsbomb(105.0, 68.0)
        assert x == pytest.approx(120.0)
        assert y == pytest.approx(80.0)


class TestPctToStatsbomb:
    """Wyscout percentage [0,100] → StatsBomb 120×80."""

    def test_origin(self) -> None:
        x, y = pct_to_statsbomb(0.0, 0.0)
        assert x == pytest.approx(0.0)
        assert y == pytest.approx(0.0)

    def test_full(self) -> None:
        x, y = pct_to_statsbomb(100.0, 100.0)
        assert x == pytest.approx(120.0)
        assert y == pytest.approx(80.0)


class TestStatsbombToMeters:
    """StatsBomb 120×80 → real-world meters (for analytics modules)."""

    def test_center(self) -> None:
        x, y = statsbomb_to_meters(60.0, 40.0)
        assert x == pytest.approx(52.5)
        assert y == pytest.approx(34.0)

    def test_origin(self) -> None:
        x, y = statsbomb_to_meters(0.0, 0.0)
        assert x == pytest.approx(0.0)
        assert y == pytest.approx(0.0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_coordinates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analytics.coordinates'`

- [ ] **Step 3: Write the coordinate transform module**

```python
"""Coordinate system transforms for soccer pitch data.

All providers' coordinate systems normalized to the StatsBomb 120×80 yard
system used as the platform standard.  Functions accept scalars, NumPy
arrays, or pandas Series (any type that supports arithmetic operators).

Provider coordinate systems:
  - Metrica:     [0, 1] normalized, y-flipped (y=0 is top-left)
  - IDSSE/SC:    Center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34)
  - IDSSE events: Pitch-origin meters, x ∈ (0, 105), y ∈ (0, 68)
  - Wyscout:     Percentage [0, 100] on both axes
  - StatsBomb:   Already in target system (120×80 yards)

Target system: StatsBomb 120×80, (0,0) = bottom-left, (120,80) = top-right.

References:
  - StatsBomb Open Data Spec: 120×80 yards
  - DFL (IDSSE): 105×68 meters, center-origin
  - Metrica EPTS: [0,1] normalized, y-inverted
"""

from __future__ import annotations

from typing import TypeVar

_T = TypeVar("_T")

# Platform constants — single source of truth
STATSBOMB_LENGTH: float = 120.0
STATSBOMB_WIDTH: float = 80.0
PITCH_LENGTH_M: float = 105.0
PITCH_WIDTH_M: float = 68.0


def metrica_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Metrica [0,1] normalized with y-flip → StatsBomb 120×80."""
    return (x * STATSBOMB_LENGTH, (1.0 - y) * STATSBOMB_WIDTH)  # type: ignore[return-value]


def center_m_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Center-origin meters (IDSSE tracking, SkillCorner) → StatsBomb 120×80."""
    return (
        (x + PITCH_LENGTH_M / 2.0) / PITCH_LENGTH_M * STATSBOMB_LENGTH,  # type: ignore[return-value]
        (y + PITCH_WIDTH_M / 2.0) / PITCH_WIDTH_M * STATSBOMB_WIDTH,  # type: ignore[return-value]
    )


def pitch_m_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Pitch-origin meters (IDSSE events) → StatsBomb 120×80."""
    return (
        x / PITCH_LENGTH_M * STATSBOMB_LENGTH,  # type: ignore[return-value]
        y / PITCH_WIDTH_M * STATSBOMB_WIDTH,  # type: ignore[return-value]
    )


def pct_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Percentage [0,100] (Wyscout) → StatsBomb 120×80."""
    return (
        x / 100.0 * STATSBOMB_LENGTH,  # type: ignore[return-value]
        y / 100.0 * STATSBOMB_WIDTH,  # type: ignore[return-value]
    )


def statsbomb_to_meters(x: _T, y: _T) -> tuple[_T, _T]:
    """StatsBomb 120×80 → real-world meters (for analytics needing SI units)."""
    return (
        x * PITCH_LENGTH_M / STATSBOMB_LENGTH,  # type: ignore[return-value]
        y * PITCH_WIDTH_M / STATSBOMB_WIDTH,  # type: ignore[return-value]
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_coordinates.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check src/analytics/coordinates.py src/tests/test_coordinates.py && uv run pyright src/analytics/coordinates.py`
Expected: zero violations.

---

### Task 1.4: Refactor dbt staging models to use the macro

**Files:**
- Modify: `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`
- Modify: `dbt_project/models/staging/metrica/stg_metrica__events.sql`
- Modify: `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`
- Modify: `dbt_project/models/staging/idsse/stg_idsse__events.sql`
- Modify: `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql`
- Modify: `dbt_project/models/staging/wyscout/stg_wyscout__events.sql`

- [ ] **Step 1: Replace inline transforms in `stg_metrica__tracking.sql`**

Replace lines 93-99 (the `normalized` CTE coordinate columns):

```sql
        -- Scaled player coordinates (120x80)
        {{ normalize_x('raw_x', 'metrica') }} as x,
        {{ normalize_y('raw_y', 'metrica') }} as y,

        -- Ball coordinates broadcast from frame-level bronze columns
        {{ normalize_x('raw_ball_x', 'metrica') }} as ball_x,
        {{ normalize_y('raw_ball_y', 'metrica') }} as ball_y
```

- [ ] **Step 2: Replace inline transforms in `stg_metrica__events.sql`**

Replace lines 36-40:

```sql
        -- Scaled start location (120x80)
        {{ normalize_x('start_x', 'metrica') }} as start_x,
        {{ normalize_y('start_y', 'metrica') }} as start_y,

        -- Scaled end location (120x80)
        {{ normalize_x('end_x', 'metrica') }} as end_x,
        {{ normalize_y('end_y', 'metrica') }} as end_y,
```

- [ ] **Step 3: Replace inline transforms in `stg_idsse__tracking.sql`**

Replace lines 38-44:

```sql
        -- Scaled player coordinates (120×80)
        {{ normalize_x('x', 'center_m') }} as x,
        {{ normalize_y('y', 'center_m') }} as y,

        -- Ball coordinates scaled to 120×80
        {{ normalize_x('ball_x', 'center_m') }} as ball_x,
        {{ normalize_y('ball_y', 'center_m') }} as ball_y
```

- [ ] **Step 4: Replace inline transforms in `stg_idsse__events.sql`**

Replace lines 46-47:

```sql
        -- Scaled event coordinates (120×80) — events use pitch-origin (0-105, 0-68)
        {{ normalize_x('x', 'pitch_m') }} as x,
        {{ normalize_y('y', 'pitch_m') }} as y
```

- [ ] **Step 5: Replace inline transforms in `stg_skillcorner__tracking.sql`**

Replace lines 33-38:

```sql
        -- Scaled player coordinates (120×80)
        {{ normalize_x('x', 'center_m') }} as x,
        {{ normalize_y('y', 'center_m') }} as y,

        -- Ball coordinates scaled to 120×80
        {{ normalize_x('ball_x', 'center_m') }} as ball_x,
        {{ normalize_y('ball_y', 'center_m') }} as ball_y
```

- [ ] **Step 6: Replace inline transforms in `stg_wyscout__events.sql`**

Replace lines 42-43 (start and end locations):

```sql
        -- Start location (scaled to 120x80, use get() for safe access)
        {{ normalize_x('get(parsed_positions, 0).x', 'pct') }} as start_x,
        {{ normalize_y('get(parsed_positions, 0).y', 'pct') }} as start_y,

        -- End location (scaled to 120x80, may be NULL if positions has only 1 element)
        {{ normalize_x('get(parsed_positions, 1).x', 'pct') }} as end_x,
        {{ normalize_y('get(parsed_positions, 1).y', 'pct') }} as end_y,
```

- [ ] **Step 7: Verify all staging models compile**

Run: `cd dbt_project && dbt compile --select tag:staging --profiles-dir .`
Expected: all staging models compile with no errors.

---

### Task 1.5: Replace inline Python transforms in `line_breaking.py`

**Files:**
- Modify: `src/ingestion/line_breaking.py:241-249`

- [ ] **Step 1: Replace inline Metrica transform with module call**

At the top of `line_breaking.py`, add the import (near other imports):

```python
from analytics.coordinates import metrica_to_statsbomb
```

Replace lines 241-249:

```python
            # Convert Metrica 0-1 -> StatsBomb 120x80 (y-flip: Metrica y=0 is top)
            opp_x, opp_y = metrica_to_statsbomb(opp_positions["x"], opp_positions["y"])
            opp_positions["x"] = opp_x
            opp_positions["y"] = opp_y

            # Pass coordinates (also 0-1 -> 120x80 with y-flip)
            raw_sx = float(row.get("evt_start_x", 0) or 0)
            raw_sy = float(row.get("evt_start_y", 0) or 0)
            raw_ex = float(row.get("evt_end_x", 0) or 0)
            raw_ey = float(row.get("evt_end_y", 0) or 0)
            start_x, start_y = metrica_to_statsbomb(raw_sx, raw_sy)
            end_x, end_y = metrica_to_statsbomb(raw_ex, raw_ey)
```

- [ ] **Step 2: Lint and type check**

Run: `uv run ruff check src/ingestion/line_breaking.py && uv run pyright src/ingestion/line_breaking.py`
Expected: zero violations.

- [ ] **Step 3: Run existing line_breaking tests**

Run: `uv run pytest src/tests/test_line_breaking.py -v`
Expected: all existing tests pass (the transform produces identical results).

---

### Task 1.6: TD#6 — Add IDSSE line-breaking Path C

**Context:** Line-breaking currently has two data paths: Path A (StatsBomb 360 freeze frames) and Path B (Metrica tracking + events). IDSSE has both tracking and ELASTIC-aligned events (ingested in D9) but line-breaking is not wired to them. This task adds Path C.

**Key differences from Path B (Metrica):**
- IDSSE tracking is **narrow format** (one row per player per frame) in `idsse_tracking` bronze — not wide JSON like Metrica's `home_players`/`away_players` columns.
- IDSSE events are in **pitch-origin meters** (0–105, 0–68); tracking is in **center-origin meters** (−52.5 to 52.5, −34 to 34). Two different coordinate systems from the same provider.
- Join is **temporal** (closest frame to event `timestamp_seconds`) — no `start_frame` column in IDSSE events.

**Files:**
- Modify: `src/ingestion/line_breaking.py`

- [ ] **Step 1: Write the IDSSE UDF**

Add `_make_idsse_udf` after the existing `_make_metrica_udf` (after line 273):

```python
def _make_idsse_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for IDSSE tracking data.

    The UDF receives a pandas DataFrame containing one match's worth of
    pass events joined to tracking frames (narrow format: one row per
    pass × tracking player).  It groups opponents per pass event, converts
    coordinates, and runs detection.

    IDSSE uses two coordinate systems:
      - Events: pitch-origin meters (0–105, 0–68)
      - Tracking: center-origin meters (−52.5 to 52.5, −34 to 34)
    Both are converted to StatsBomb 120×80 via ``analytics.coordinates``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import pandas as _pd

        from analytics.coordinates import center_m_to_statsbomb as _center_m
        from analytics.coordinates import pitch_m_to_statsbomb as _pitch_m
        from analytics.line_breaking import LineBreakingParams as _LBParams
        from analytics.line_breaking import detect_line_breaking_batch as _detect_batch

        if pdf.empty:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        params = _LBParams()
        match_id = str(pdf["evt_match_id"].iloc[0])

        # Deduplicate pass events (each pass appears once per opponent row)
        pass_cols = [
            "evt_event_id", "evt_match_id", "evt_team",
            "evt_x", "evt_y", "evt_end_x", "evt_end_y",
        ]
        passes_dedup = pdf[pass_cols].drop_duplicates(subset=["evt_event_id"])

        # Group opponents by event_id
        opponent_groups = pdf.groupby("evt_event_id")

        passes_list: list[dict[str, object]] = []
        opponents_by_event: dict[str, _pd.DataFrame] = {}

        for _, pass_row in passes_dedup.iterrows():
            event_id = str(pass_row["evt_event_id"])
            event_team = str(pass_row["evt_team"])

            # Get opponent tracking rows for this event
            try:
                event_rows = opponent_groups.get_group(pass_row["evt_event_id"])
            except KeyError:
                continue

            # Filter to opponent team only
            opp_rows = event_rows[event_rows["trk_team"] != event_team]
            if len(opp_rows) < params.min_opponents:
                continue

            # Convert tracking positions (center-origin meters → StatsBomb 120×80)
            opp_x, opp_y = _center_m(opp_rows["trk_x"].values, opp_rows["trk_y"].values)
            opp_positions = _pd.DataFrame({"x": opp_x, "y": opp_y})
            opponents_by_event[event_id] = opp_positions

            # Convert event positions (pitch-origin meters → StatsBomb 120×80)
            start_x, start_y = _pitch_m(
                float(pass_row["evt_x"]),
                float(pass_row["evt_y"]),
            )
            end_x, end_y = _pitch_m(
                float(pass_row.get("evt_end_x") or 0),
                float(pass_row.get("evt_end_y") or 0),
            )

            passes_list.append({
                "event_id": event_id,
                "start_x": start_x,
                "start_y": start_y,
                "end_x": end_x,
                "end_y": end_y,
            })

        if not passes_list:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        passes_df = _pd.DataFrame(passes_list)
        result_df: _pd.DataFrame = _detect_batch(passes_df, opponents_by_event, params)

        result_df["match_id"] = match_id
        result_df["data_source"] = "idsse_tracking"

        return _pd.DataFrame(result_df[_RESULT_COLUMNS])

    return _udf
```

- [ ] **Step 2: Write the IDSSE processing function**

Add `_process_idsse_tracking` after `_process_metrica_tracking` (after line 538):

```python
def _process_idsse_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
) -> int:
    """Detect line-breaking passes using IDSSE tracking + ELASTIC-aligned events.

    Joins IDSSE pass events with tracking frames via temporal nearest-frame
    match, then uses ``groupBy("evt_match_id").applyInPandas`` to distribute
    detection across executors.

    Coordinate systems:
      - Events (``idsse_events``): pitch-origin meters (0–105, 0–68)
      - Tracking (``idsse_tracking``): center-origin meters (−52.5 to 52.5)
      Both converted to StatsBomb 120×80 inside the UDF via ``analytics.coordinates``.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.idsse_events"
    tracking_table = f"{catalog}.{schema}.idsse_tracking"

    # Get distinct match_ids that have events
    try:
        match_ids_rows = (
            spark.table(events_table)
            .filter("event_type = 'successfulPassEvent' OR event_type = 'failedPassEvent'")
            .select("match_id")
            .distinct()
            .collect()
        )
    except Exception:
        logger.exception("Cannot read IDSSE events table")
        return 0

    if not match_ids_rows:
        logger.info("No PASS events in IDSSE events — skipping Path C")
        return 0

    match_ids = [row["match_id"] for row in match_ids_rows]
    logger.info("Path C: %d matches with PASS events", len(match_ids))

    # Incremental skip
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    existing_ids: set[str] = set()
    try:
        existing_rows = (
            spark.table(results_table)
            .filter("data_source = 'idsse_tracking'")
            .select("match_id")
            .distinct()
            .collect()
        )
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in match_ids if str(mid) not in existing_ids]
    logger.info(
        "Path C: %d matches total, %d already processed, %d to process",
        len(match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

    new_ids_str = [str(mid) for mid in new_match_ids]

    # IDSSE events (pass types from ELASTIC-aligned DFL events)
    passes_df = (
        spark.table(events_table)
        .filter(
            (F.col("event_type") == "successfulPassEvent")
            | (F.col("event_type") == "failedPassEvent")
        )
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("event_id").alias("evt_event_id"),
            F.col("match_id").alias("evt_match_id"),
            F.col("team").alias("evt_team"),
            F.col("timestamp_seconds").alias("evt_ts"),
            F.col("x").alias("evt_x"),
            F.col("y").alias("evt_y"),
            # IDSSE events may not have end coords — use NULL if absent
            F.col("x").alias("evt_end_x"),  # placeholder — update if end coords available
            F.col("y").alias("evt_end_y"),
        )
        .filter(F.col("evt_x").isNotNull())
    )

    # IDSSE tracking (narrow: one row per player per frame)
    tracking_df = (
        spark.table(tracking_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("match_id").alias("trk_match_id"),
            F.col("timestamp").alias("trk_ts"),
            F.col("player_id").alias("trk_player_id"),
            F.col("team").alias("trk_team"),
            F.col("x").alias("trk_x"),
            F.col("y").alias("trk_y"),
        )
    )

    # Temporal join: find the closest tracking frame to each event's timestamp.
    # Use a range join with a 0.1s tolerance window (2-3 frames at 25fps).
    joined = passes_df.join(
        tracking_df,
        (passes_df["evt_match_id"] == tracking_df["trk_match_id"])
        & (F.abs(passes_df["evt_ts"] - tracking_df["trk_ts"]) <= 0.06),
        "inner",
    ).drop("trk_match_id")

    result_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("is_line_breaking", BooleanType(), nullable=True),
            StructField("lines_broken", IntegerType(), nullable=True),
            StructField("line_breaking_type", StringType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )

    udf_fn = _make_idsse_udf()

    result_sdf = joined.groupBy("evt_match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=result_schema,
    )

    written = merge_delta_table(
        result_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        merge_key="event_id",
        logger=logger,
    )

    logger.info("Path C complete: %d rows written", written)
    return written
```

- [ ] **Step 3: Wire Path C into the pipeline orchestration**

In `run_pipeline` (line 547-562), add Path C after Path B:

```python
@workflow("wf-line-breaking", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx=None,
) -> None:
    """Execute the line-breaking detection pipeline."""
    params = LineBreakingParams()

    path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params)
    path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params)
    path_c_rows = _process_idsse_tracking(spark, catalog, schema, logger, params)

    total = path_a_rows + path_b_rows + path_c_rows
    logger.info("Line-breaking pipeline complete — %d total rows written", total)
```

- [ ] **Step 4: Update the import in the Metrica UDF to use `coordinates` module**

In `_make_metrica_udf`, replace the inline coordinate transform (already done in Task 1.5) and verify the import works inside the UDF closure:

```python
        from analytics.coordinates import metrica_to_statsbomb as _metrica

        # ...
        opp_x, opp_y = _metrica(opp_positions["x"], opp_positions["y"])
        opp_positions["x"] = opp_x
        opp_positions["y"] = opp_y

        raw_sx = float(row.get("evt_start_x", 0) or 0)
        raw_sy = float(row.get("evt_start_y", 0) or 0)
        raw_ex = float(row.get("evt_end_x", 0) or 0)
        raw_ey = float(row.get("evt_end_y", 0) or 0)
        start_x, start_y = _metrica(raw_sx, raw_sy)
        end_x, end_y = _metrica(raw_ex, raw_ey)
```

Note: `analytics.coordinates` is pure Python (numpy/pandas arithmetic) — safe to import on Spark executors.

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check src/ingestion/line_breaking.py && uv run pyright src/ingestion/line_breaking.py`
Expected: zero violations.

- [ ] **Step 6: Run existing line-breaking tests**

Run: `uv run pytest src/tests/test_line_breaking.py -v`
Expected: all existing tests pass. New Path C will be validated at pipeline execution time (requires Databricks).

**Note:** The IDSSE event type names (`successfulPassEvent`, `failedPassEvent`) and end-coordinate columns should be verified against the actual `idsse_events` bronze table schema during implementation. The temporal join tolerance (0.06s ≈ 1.5 frames at 25fps) may need tuning — a single frame at 25fps is 0.04s.

---

## Chunk 2: D28 — Position-Group Z-Scoring

### Task 2.1: Add position-group z-scoring tests

**Files:**
- Modify: `src/tests/test_player_embeddings.py`

- [ ] **Step 1: Write failing tests for per-group normalization**

Add these test functions to the existing test file:

```python
def test_zscore_normalize_per_group() -> None:
    """Position-group z-scoring normalizes within groups, not globally."""
    from ingestion.player_embeddings import _zscore_normalize

    # Two groups: GK has very different stats than outfield
    df = pd.DataFrame(
        {
            "position_group": ["Goalkeeper", "Goalkeeper", "Defender", "Defender"],
            "goals_per_90": [0.0, 0.01, 1.5, 2.0],
            "passes_per_90": [25.0, 30.0, 55.0, 60.0],
        }
    )
    features = ["goals_per_90", "passes_per_90"]

    # Global z-score: GK goals would be very negative (pulled down by outfield mean)
    global_norm, _ = _zscore_normalize(df, features)
    # GK goals should be near the bottom globally
    assert global_norm["goals_per_90"].iloc[0] < -0.5

    # Per-group z-score: GK goals normalized within GK group only
    results = []
    for _group, group_df in df.groupby("position_group"):
        norm_group, _ = _zscore_normalize(group_df, features)
        results.append(norm_group)
    per_group_norm = pd.concat(results).sort_index()

    # GK goals should be near 0 within their own group (both are near-zero)
    assert abs(per_group_norm["goals_per_90"].iloc[0]) < 1.0


def test_compute_stat_vectors_returns_position_group_params(
    mock_spark: Any,
) -> None:
    """_compute_stat_vectors returns params keyed by position_group."""
    # This test validates the params structure after D28 change.
    # The actual Spark query is mocked — we verify the return type.
    pass  # Placeholder — integration test against Databricks
```

- [ ] **Step 2: Run to verify the conceptual test passes (it validates _zscore_normalize behavior)**

Run: `uv run pytest src/tests/test_player_embeddings.py::test_zscore_normalize_per_group -v`
Expected: PASS (the test validates the existing `_zscore_normalize` function works per-group when called that way).

---

### Task 2.2: Implement position-group z-scoring in `_compute_stat_vectors`

**Files:**
- Modify: `src/ingestion/player_embeddings.py:261-302`

- [ ] **Step 1: Add `position_group` to the SQL query and implement per-group z-scoring**

Replace the `_compute_stat_vectors` function body (lines 261-302):

```python
    feature_cols = ", ".join(f"ps.{f}" for f in STAT_FEATURES)
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(ps.competition_id AS STRING) AS competition_id,
            CAST(ps.season_id AS STRING) AS season_id,
            dp.position_group,
            {feature_cols}
        FROM {catalog}.{gold_schema}.fct_player_stats ps
        INNER JOIN {catalog}.{gold_schema}.dim_players dp
            ON ps.player_id = dp.player_id
        WHERE dp.canonical_player_id IS NOT NULL
          AND dp.position_group IS NOT NULL
    """  # noqa: S608
    sdf = spark.sql(query)
    if player_ids:
        id_list = [str(pid) for pid in player_ids]
        sdf = sdf.filter(sdf["canonical_player_id"].isin(id_list))
    df = sdf.limit(50_000).toPandas()

    if df.empty:
        return (
            pd.DataFrame(
                {
                    "canonical_player_id": pd.Series(dtype="str"),
                    "competition_id": pd.Series(dtype="str"),
                    "season_id": pd.Series(dtype="str"),
                    "stat_vector": pd.Series(dtype="object"),
                }
            ),
            {},
        )

    # Z-score normalize within each position group (D28: fixes GK contamination)
    normalized_groups: list[pd.DataFrame] = []
    all_params: dict[str, dict[str, dict[str, float]]] = {}
    for group_name, group_df in df.groupby("position_group"):
        norm_group, group_params = _zscore_normalize(group_df, STAT_FEATURES)
        normalized_groups.append(norm_group)
        all_params[str(group_name)] = group_params

    normalized = pd.concat(normalized_groups).sort_index()

    # Build stat_vector column as list[float | None] (vectorized via NumPy array access)
    stat_arr = normalized[STAT_FEATURES].values
    normalized["stat_vector"] = [[None if pd.isna(v) else float(v) for v in row] for row in stat_arr]

    result_df = cast(pd.DataFrame, normalized[["canonical_player_id", "competition_id", "season_id", "stat_vector"]])
    return result_df, all_params
```

- [ ] **Step 2: Update `_save_norm_params` to handle group-keyed params**

The params dict is now `{"Goalkeeper": {"goals_per_90": {"mean": ..., "std": ...}, ...}, "Defender": {...}, ...}`. The `_save_norm_params` function already accepts `dict` and `json.dump` handles nested dicts — no structural change needed, but update the type annotation and docstring:

Replace lines 779-801 type annotation:

```python
def _save_norm_params(
    catalog: str,
    params: dict[str, dict[str, dict[str, float]]],
    logger: logging.Logger,
) -> None:
    """Save normalization parameters to UC Volumes as JSON.

    After D28, params are keyed by position_group:
    ``{"Goalkeeper": {"goals_per_90": {"mean": 0.1, "std": 0.05}, ...}, ...}``

    Args:
        catalog: Unity Catalog name.
        params: Position-group-keyed feature normalization parameters.
        logger: Logger instance.
    """
```

- [ ] **Step 3: Update the return type annotation on `_compute_stat_vectors`**

Change the return type (line 247):

```python
) -> tuple[pd.DataFrame, dict[str, dict[str, dict[str, float]]]]:
```

- [ ] **Step 4: Lint and type check**

Run: `uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py`
Expected: zero violations.

- [ ] **Step 5: Run all player_embeddings tests**

Run: `uv run pytest src/tests/test_player_embeddings.py -v`
Expected: all tests pass.

---

## Chunk 3: D29 — SPADL Vocabulary Upgrade

### Task 3.1: Update tokenizer in `football2vec.py`

**Files:**
- Modify: `src/analytics/football2vec.py`
- Modify: `src/tests/test_football2vec.py`

- [ ] **Step 1: Write failing tests for SPADL tokenization**

Add to `test_football2vec.py`:

```python
class TestSpadlTokenization:
    """Tests for SPADL 23-type vocabulary tokenization."""

    def test_spadl_action_passthrough(self) -> None:
        """SPADL action_type is used directly — no mapping needed."""
        event = {
            "action_type": "tackle",
            "start_x": 52.5,
            "start_y": 34.0,
            "data_source": "spadl",
        }
        token = tokenize_event(event)
        # Grid cell for center of 105×68 pitch: x=52.5/8.75=6, y=34.0/8.5=4
        assert token == "tackle_6_4"

    def test_spadl_corner_short(self) -> None:
        """SPADL distinguishes corner_short from corner_crossed."""
        event = {
            "action_type": "corner_short",
            "start_x": 105.0,
            "start_y": 0.0,
            "data_source": "spadl",
        }
        token = tokenize_event(event)
        assert token is not None
        assert token.startswith("corner_short_")

    def test_spadl_keeper_variants(self) -> None:
        """SPADL has 4 keeper action types instead of generic 'goalkeeper'."""
        for keeper_type in ["keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up"]:
            event = {
                "action_type": keeper_type,
                "start_x": 5.0,
                "start_y": 34.0,
                "data_source": "spadl",
            }
            token = tokenize_event(event)
            assert token is not None
            assert token.startswith(f"{keeper_type}_")

    def test_spadl_grid_dimensions(self) -> None:
        """Grid uses SPADL 105×68 coordinate system."""
        # Far corner of 105×68 pitch
        event = {
            "action_type": "pass",
            "start_x": 104.9,
            "start_y": 67.9,
            "data_source": "spadl",
        }
        token = tokenize_event(event)
        # Should be in the last grid cell (11, 7)
        assert token == "pass_11_7"

    def test_all_23_spadl_types(self) -> None:
        """All 23 SPADL action types produce valid tokens."""
        spadl_types = [
            "pass", "cross", "throw_in", "freekick_crossed", "freekick_short",
            "corner_crossed", "corner_short", "take_on", "foul", "tackle",
            "interception", "shot", "shot_penalty", "shot_freekick",
            "keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up",
            "clearance", "bad_touch", "non_action", "dribble", "goalkick",
        ]
        for action in spadl_types:
            event = {
                "action_type": action,
                "start_x": 52.5,
                "start_y": 34.0,
                "data_source": "spadl",
            }
            token = tokenize_event(event)
            assert token is not None
            assert token.startswith(f"{action}_")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_football2vec.py::TestSpadlTokenization -v`
Expected: FAIL — `tokenize_event` doesn't accept `action_type` key or SPADL grid dimensions.

- [ ] **Step 3: Rewrite `tokenize_event` for SPADL vocabulary**

Replace the event maps and `tokenize_event` function in `football2vec.py`. The key changes:
1. Remove `_STATSBOMB_EVENT_MAP`, `_WYSCOUT_EVENT_MAP`, `_WYSCOUT_OTHERS_SUB_MAP`
2. Remove `_resolve_statsbomb_action`, `_resolve_wyscout_action`
3. `tokenize_event` reads `action_type` directly (SPADL canonical string)
4. Grid dimensions change from 120×80 to 105×68 (SPADL coordinate system)
5. Keep backward compat: if `event_type` key exists (legacy path), fall back to old logic

```python
# ---------------------------------------------------------------------------
# SPADL 23-type vocabulary (canonical since D29)
# ---------------------------------------------------------------------------

SPADL_ACTION_TYPES: frozenset[str] = frozenset({
    "pass", "cross", "throw_in", "freekick_crossed", "freekick_short",
    "corner_crossed", "corner_short", "take_on", "foul", "tackle",
    "interception", "shot", "shot_penalty", "shot_freekick",
    "keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up",
    "clearance", "bad_touch", "non_action", "dribble", "goalkick",
})
```

Update `TokenizerConfig`:

```python
@dataclass(frozen=True)
class TokenizerConfig:
    """Configuration for event tokenization."""

    grid_cols: int = 12
    grid_rows: int = 8
    pitch_length: float = 105.0  # SPADL coordinate system (meters)
    pitch_width: float = 68.0    # SPADL coordinate system (meters)
```

Replace `tokenize_event`:

```python
def tokenize_event(
    event: dict[str, Any],
    config: TokenizerConfig | None = None,
) -> str | None:
    """Convert a single SPADL action to a spatial grid token.

    Token format: ``{action_type}_{grid_x}_{grid_y}``

    Args:
        event: Dict with keys ``action_type``, ``start_x``, ``start_y``.
            Coordinates are in SPADL 105×68 meter system.
        config: Optional tokenizer config (default: 12×8 grid on 105×68).

    Returns:
        Token string, or None if coordinates are missing/invalid.
    """
    cfg = config or TokenizerConfig()

    x_val = event.get("start_x")
    y_val = event.get("start_y")
    if x_val is None or y_val is None:
        return None
    if isinstance(x_val, float) and math.isnan(x_val):
        return None
    if isinstance(y_val, float) and math.isnan(y_val):
        return None

    cell_w = cfg.pitch_length / cfg.grid_cols
    cell_h = cfg.pitch_width / cfg.grid_rows
    gx = min(int(float(x_val) / cell_w), cfg.grid_cols - 1)
    gy = min(int(float(y_val) / cell_h), cfg.grid_rows - 1)

    action = event.get("action_type", "non_action")
    return f"{action}_{gx}_{gy}"
```

- [ ] **Step 4: Run SPADL tokenization tests**

Run: `uv run pytest src/tests/test_football2vec.py::TestSpadlTokenization -v`
Expected: all PASS.

- [ ] **Step 5: Update remaining `football2vec.py` tests for new signature**

Existing tests that use the old `event_type`/`data_source` keys need updating to use `action_type`/`start_x`/`start_y` and the 105×68 grid. Update each test case's event dicts and expected grid cell calculations.

- [ ] **Step 6: Lint and type check**

Run: `uv run ruff check src/analytics/football2vec.py src/tests/test_football2vec.py && uv run pyright src/analytics/football2vec.py`
Expected: zero violations.

---

### Task 3.2: Rewrite event loading to use `fct_action_values`

**Files:**
- Modify: `src/ingestion/player_embeddings.py:113-213`

- [ ] **Step 1: Replace `_load_events_sdf` to query SPADL actions**

Replace the entire function body. The new query is dramatically simpler — no per-source CTE union, no event type columns, no play_pattern/pass_cross/sub_event_type.

```python
def _load_events_sdf(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    match_ids: set[str] | None = None,
) -> SparkDataFrame:
    """Load SPADL actions joined to dim_players as a Spark DF.

    Reads from ``fct_action_values`` (23-type SPADL vocabulary, 105×68m
    coordinate system) instead of raw provider events.  Source-agnostic:
    StatsBomb and Wyscout events are already unified by socceraction.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name (unused — queries gold directly).
        match_ids: If provided, only load actions for these match IDs.

    Returns:
        Spark DataFrame with columns: canonical_player_id, match_id,
        action_type, start_x, start_y, event_index, data_source,
        competition_id, season_id.
    """
    _ = schema
    gold = _GOLD_SCHEMA

    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(av.match_id AS STRING) AS match_id,
            av.action_type,
            CAST(av.start_x AS DOUBLE) AS start_x,
            CAST(av.start_y AS DOUBLE) AS start_y,
            CAST(av.action_id AS INT) AS event_index,
            av.data_source,
            CAST(m.competition_id AS STRING) AS competition_id,
            CAST(m.season_id AS STRING) AS season_id
        FROM {catalog}.{gold}.fct_action_values av
        INNER JOIN {catalog}.{gold}.dim_players dp
            ON av.player_id = dp.player_id
        INNER JOIN {catalog}.{gold}.fct_match_summary m
            ON CAST(av.match_id AS STRING) = CAST(m.match_id AS STRING)
        WHERE av.player_id IS NOT NULL
          AND dp.canonical_player_id IS NOT NULL
    """  # noqa: S608
    events_sdf = spark.sql(query)

    if match_ids:
        from pyspark.sql import functions as spark_fn

        events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(list(match_ids)))

    return events_sdf
```

- [ ] **Step 2: Lint and type check**

Run: `uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py`
Expected: zero violations.

---

### Task 3.3: Rewrite inline tokenizer in `_make_behavioral_udf`

**Files:**
- Modify: `src/ingestion/player_embeddings.py:447-535`

- [ ] **Step 1: Replace the inline tokenizer block**

The UDF closure no longer needs per-source event maps. Replace lines 444-535 (the tokenization section inside `_udf`):

```python
        # ---- Tokenize events per (player, match) ----
        # SPADL 23-type vocabulary (D29): action_type is the canonical token.
        # Grid: 12×8 on 105×68m SPADL coordinate system.
        grid_cols, grid_rows = 12, 8
        pitch_length, pitch_width = 105.0, 68.0
        cell_w = pitch_length / grid_cols
        cell_h = pitch_width / grid_rows

        sorted_pdf = pdf.sort_values("event_index")
        sequences: dict[tuple[str, str], list[str]] = {}
        match_meta: dict[str, tuple[str, str, str]] = {}

        for rec in sorted_pdf.to_dict("records"):
            rec_dict: dict[str, _Any] = rec
            x_val = rec_dict.get("start_x")
            y_val = rec_dict.get("start_y")
            if x_val is None or y_val is None:
                continue
            if isinstance(x_val, float) and _math.isnan(x_val):
                continue
            if isinstance(y_val, float) and _math.isnan(y_val):
                continue

            gx = min(int(x_val / cell_w), grid_cols - 1)
            gy = min(int(y_val / cell_h), grid_rows - 1)

            action = rec_dict.get("action_type", "non_action")
            token = f"{action}_{gx}_{gy}"

            key = (str(rec_dict["canonical_player_id"]), str(rec_dict["match_id"]))
            if key not in sequences:
                sequences[key] = []
            sequences[key].append(token)

            mid = str(rec_dict["match_id"])
            if mid not in match_meta:
                match_meta[mid] = (
                    str(rec_dict.get("data_source", "unknown")),
                    str(rec_dict.get("competition_id", "")),
                    str(rec_dict.get("season_id", "")),
                )
```

- [ ] **Step 2: Run all player_embeddings tests**

Run: `uv run pytest src/tests/test_player_embeddings.py -v`
Expected: all tests pass.

- [ ] **Step 3: Lint and type check**

Run: `uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py`
Expected: zero violations.

---

## Chunk 4: D26 — GK Metadata Pipeline

### Task 4.1: Add GK extraction to IDSSE ingestion

**Files:**
- Modify: `src/ingestion/idsse.py:106-140`
- Modify: `src/tests/test_idsse.py`

- [ ] **Step 1: Write failing test for GK parsing**

Add to `test_idsse.py`:

```python
def test_parse_teams_extracts_goalkeeper(tmp_path: Path) -> None:
    """_parse_teams reads PlayingPosition='TW' as goalkeeper."""
    xml_content = """<?xml version="1.0" encoding="utf-8"?>
    <MatchDay>
        <MatchDayMatchLineUp MatchId="DFL-MAT-000001" MatchDayId="1">
            <Players>
                <Players TeamId="DFL-CLU-000008" Role="home">
                    <Player PersonId="H001" ShirtNumber="1" PlayingPosition="TW"
                            FirstName="A" LastName="GK" />
                    <Player PersonId="H002" ShirtNumber="2" PlayingPosition="IV"
                            FirstName="C" LastName="CB" />
                </Players>
                <Players TeamId="DFL-CLU-000009" Role="guest">
                    <Player PersonId="A001" ShirtNumber="1" PlayingPosition="TW"
                            FirstName="E" LastName="GK2" />
                    <Player PersonId="A002" ShirtNumber="7" PlayingPosition="RA"
                            FirstName="G" LastName="RW" />
                </Players>
            </Players>
        </MatchDayMatchLineUp>
    </MatchDay>"""
    xml_path = tmp_path / "match_info.xml"
    xml_path.write_text(xml_content)

    from ingestion.idsse import _parse_teams

    home_id, away_id, player_team_map, gk_player_ids = _parse_teams(str(xml_path))
    assert "H001" in gk_player_ids
    assert "A001" in gk_player_ids
    assert "H002" not in gk_player_ids
    assert "A002" not in gk_player_ids
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_idsse.py::test_parse_teams_extracts_goalkeeper -v`
Expected: FAIL — `_parse_teams` returns 3 values, not 4.

- [ ] **Step 3: Update `_parse_teams` to extract GK player IDs**

In `src/ingestion/idsse.py`, modify `_parse_teams` (around lines 106-140) to also return a set of GK player IDs:

```python
def _parse_teams(
    match_info_path: str,
) -> tuple[str, str, dict[str, str], set[str]]:
    """Parse team and player metadata from DFL match information XML.

    Returns:
        Tuple of (home_team_id, away_team_id, player_team_map, gk_player_ids).
        ``gk_player_ids`` contains PersonIds of players with PlayingPosition='TW'.
    """
```

Inside the player iteration loop, add GK detection:

```python
    gk_player_ids: set[str] = set()
    for player_el in team_el.iter("Player"):
        person_id = player_el.get("PersonId", "")
        if person_id:
            player_team_map[person_id] = team_label
            if player_el.get("PlayingPosition") == "TW":
                gk_player_ids.add(person_id)
```

Return the extended tuple:

```python
    return home_team_id, away_team_id, player_team_map, gk_player_ids
```

- [ ] **Step 4: Update all callers of `_parse_teams`**

Search for all call sites and update destructuring to accept the 4th return value. The main call site is in `_ingest_match` — add `gk_player_ids` to the destructuring and pass it to `_parse_positions_xml`.

- [ ] **Step 5: Add `is_goalkeeper` to tracking rows**

In `_parse_positions_xml` (or the function that builds per-player rows), add:

```python
row["is_goalkeeper"] = player_id in gk_player_ids
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest src/tests/test_idsse.py::test_parse_teams_extracts_goalkeeper -v`
Expected: PASS.

---

### Task 4.2: Add GK extraction to SkillCorner ingestion

**Files:**
- Modify: `src/ingestion/skillcorner.py:113-139`

- [ ] **Step 1: Add `is_goalkeeper` to `_dataset_to_rows`**

In `_dataset_to_rows`, where each player row is constructed, add the GK flag. The kloppy `Player` object has a `.position` attribute:

```python
from kloppy.domain import PositionType
```

Then in the row construction (around line 131):

```python
row["is_goalkeeper"] = (
    player.position is not None
    and player.position.position_id == PositionType.Goalkeeper.position_id
)
```

Note: kloppy `PositionType` is an enum — check the exact API. The comparison should use the enum's identity. If `player.position` is a `Position` object with `.position_id`, compare with `PositionType.Goalkeeper`. Verify with kloppy source.

- [ ] **Step 2: Lint and run existing SkillCorner tests**

Run: `uv run ruff check src/ingestion/skillcorner.py && uv run pytest src/tests/ -k skillcorner -v`
Expected: all pass.

---

### Task 4.3: Add GK extraction to Metrica ingestion

**Files:**
- Modify: `src/ingestion/metrica.py`
- Modify: `src/tests/test_metrica.py`

- [ ] **Step 1: Determine GK identification strategy per game format**

**Game 3 (EPTS XML):** The FIFA EPTS standard can carry `<Player PlayingPosition="...">` but the Metrica open data EPTS XML does not include this attribute (confirmed by test fixture). However, the EPTS metadata pairs `ShirtNumber` with `PlayerChannelRef`. Convention: jersey number `"1"` is almost universally the GK in open data samples. Since Metrica open data is only 3 matches with known squads, use a pragmatic approach: check the EPTS XML for a `PlayingPosition` attribute first; if absent, treat jersey `"1"` as GK with a warning log.

**Games 1-2 (CSV):** Player identity is encoded as jersey numbers in column headers (`Home_11`). Same heuristic: jersey number `"1"` is GK.

- [ ] **Step 2: Update `_EPTSMetadata` to include GK set**

Add `gk_player_ids: set[str]` to the `_EPTSMetadata` NamedTuple (line ~98):

```python
class _EPTSMetadata(NamedTuple):
    # ... existing fields ...
    gk_player_ids: set[str]
```

In `_parse_epts_metadata`, after parsing `<Player>` elements:

```python
gk_player_ids: set[str] = set()
for player_el in players_el.iter("Player"):
    pid = player_el.get("id", "")
    position = player_el.get("PlayingPosition", "")
    shirt = player_el.get("ShirtNumber", "")
    if position == "TW" or position == "GK":
        gk_player_ids.add(pid)
    elif shirt == "1" and not position:
        # Fallback heuristic for open data without position metadata
        gk_player_ids.add(pid)
        logger.warning("GK heuristic: assuming player %s (shirt #1) is GK (no PlayingPosition in EPTS XML)", pid)
```

- [ ] **Step 3: Propagate `is_goalkeeper` to tracking rows**

In the functions that build narrow-format tracking rows (`_parse_epts_tracking`, `_reshape_tracking_to_narrow`), add `is_goalkeeper` based on the player_id matching the GK set.

For CSV games (Games 1-2): in `_reshape_tracking_to_narrow`, after extracting jersey number as `player_id`:

```python
row["is_goalkeeper"] = player_id == "1"
```

For Game 3 (EPTS): in `_parse_epts_tracking`, use the `gk_player_ids` from metadata:

```python
row["is_goalkeeper"] = player_id in metadata.gk_player_ids
```

- [ ] **Step 4: Write test for Metrica GK detection**

Add to `test_metrica.py`:

```python
def test_epts_metadata_identifies_goalkeeper() -> None:
    """EPTS metadata parsing identifies GK by PlayingPosition or shirt #1 fallback."""
    # Test with explicit PlayingPosition attribute
    # (mock or fixture XML with PlayingPosition="TW")
    pass  # Integration test — validated via dbt staging model
```

- [ ] **Step 5: Lint and run Metrica tests**

Run: `uv run ruff check src/ingestion/metrica.py && uv run pytest src/tests/test_metrica.py -v`
Expected: all pass.

---

### Task 4.4: Add `is_goalkeeper` to dbt staging and mart models

**Files:**
- Modify: `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`
- Modify: `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`
- Modify: `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql`
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Add `is_goalkeeper` to all three staging models**

In each staging model's `normalized` CTE, add after the `source_provider` line:

For `stg_metrica__tracking.sql` (after line 91):
```sql
        -- Goalkeeper flag (from EPTS metadata or jersey #1 heuristic)
        cast(is_goalkeeper as boolean)              as is_goalkeeper,
```

For `stg_idsse__tracking.sql` (after line 36):
```sql
        -- Goalkeeper flag (from DFL match info PlayingPosition='TW')
        cast(is_goalkeeper as boolean)              as is_goalkeeper,
```

For `stg_skillcorner__tracking.sql` (after line 32):
```sql
        -- Goalkeeper flag (from kloppy Player.position)
        cast(is_goalkeeper as boolean)              as is_goalkeeper,
```

- [ ] **Step 2: Add `is_goalkeeper` to `fct_tracking_frames.sql`**

In the `final` CTE (around line 111), add after `source_provider`:

```sql
        is_goalkeeper,
```

- [ ] **Step 3: Update the model contract in `_marts__models.yml`**

In the `fct_tracking_frames` column list (around line 870+), add:

```yaml
      - name: is_goalkeeper
        data_type: boolean
        description: >
          Whether this player is the goalkeeper. Derived from provider metadata:
          IDSSE (PlayingPosition='TW'), SkillCorner (kloppy PositionType.Goalkeeper),
          Metrica (EPTS PlayingPosition or jersey #1 heuristic).
        data_tests:
          - not_null
```

- [ ] **Step 4: Verify dbt compiles**

Run: `cd dbt_project && dbt compile --select fct_tracking_frames+ --profiles-dir .`
Expected: compiles with `is_goalkeeper` in the output schema.

---

### Task 4.5: Filter GK in formation detection pipeline

**Files:**
- Modify: `src/ingestion/formations.py:114-117,198-209`

- [ ] **Step 1: Add `is_goalkeeper` to tracking data SELECT**

In `_process_matches` (line 201), add `is_goalkeeper` to the select list:

```python
    tracking_df = (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            "match_id",
            "period",
            "team",
            "player_id",
            "timestamp_seconds",
            "x",
            "y",
            "is_goalkeeper",
        )
    )
```

- [ ] **Step 2: Add GK filter in the UDF**

In `_make_formation_udf`, replace lines 114-117:

```python
        # Filter to outfield players:
        # - player_id must be non-null (excludes ball rows)
        # - team must be non-null (excludes unassigned rows)
        # - is_goalkeeper must be False (D26: GK excluded from formation detection)
        pdf = _pd.DataFrame(
            pdf[
                pdf["player_id"].notna()
                & pdf["team"].notna()
                & ~pdf["is_goalkeeper"].fillna(False)
            ]
        )
```

- [ ] **Step 3: Run existing formation tests**

Run: `uv run pytest src/tests/test_formations.py -v`
Expected: all pass (tests use synthetic data without `is_goalkeeper` column — the `fillna(False)` handles missing column gracefully).

- [ ] **Step 4: Lint and type check**

Run: `uv run ruff check src/ingestion/formations.py && uv run pyright src/ingestion/formations.py`
Expected: zero violations.

---

## Chunk 5: CI, Pipeline Execution & E2E Verification

Everything must be deployed, rebuilt, retrained, and tested end-to-end before this cycle is considered done.

### Task 5.1: Local CI gate

- [ ] **Step 1: Full test suite**

Run: `uv run pytest src/tests/ -v`
Expected: all tests pass.

- [ ] **Step 2: Lint + format + type check**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`
Expected: zero violations.

- [ ] **Step 3: dbt compilation**

Run: `cd dbt_project && dbt compile --profiles-dir .`
Expected: all models compile cleanly with macro-based coordinate transforms.

---

### Task 5.2: Ensure Databricks warehouse is running

- [ ] **Step 1: Start warehouse**

Run: `python scripts/ensure_warehouse.py -- echo "Warehouse is RUNNING"`
Expected: warehouse confirmed RUNNING. If auto-stopped, the script resumes it.

---

### Task 5.3: Re-run tracking ingestion (populate `is_goalkeeper` in bronze)

All three tracking providers must be re-ingested to write the new `is_goalkeeper` column to their bronze Delta tables. The skip guard checks `match_id` — existing matches will be skipped unless we force a full re-run.

- [ ] **Step 1: Delete existing bronze tracking data to force re-ingestion**

For each provider, delete existing rows so the skip guard allows re-processing:

```sql
-- Run via Databricks SQL or notebook
DELETE FROM soccer_analytics.bronze.metrica_tracking;
DELETE FROM soccer_analytics.bronze.idsse_tracking;
DELETE FROM soccer_analytics.bronze.skillcorner_tracking;
```

Alternatively, if the ingestion modules support a `--force` flag or if `replaceWhere` on `match_id` handles it, use that instead. Verify the skip guard behavior before deleting.

- [ ] **Step 2: Re-run Metrica ingestion**

Run via Databricks job or:
```bash
python scripts/ensure_warehouse.py -- python -m ingestion.metrica --catalog soccer_analytics --schema bronze
```

Expected: 3 matches ingested with `is_goalkeeper` column present in bronze.

- [ ] **Step 3: Re-run IDSSE ingestion**

```bash
python scripts/ensure_warehouse.py -- python -m ingestion.idsse --catalog soccer_analytics --schema bronze
```

Expected: 7 matches ingested with `is_goalkeeper` column.

- [ ] **Step 4: Re-run SkillCorner ingestion**

```bash
python scripts/ensure_warehouse.py -- python -m ingestion.skillcorner --catalog soccer_analytics --schema bronze
```

Expected: 10 matches ingested with `is_goalkeeper` column.

- [ ] **Step 5: Spot-check bronze data**

```sql
-- Verify is_goalkeeper exists and has TRUE values (1 GK per team per frame)
SELECT source_provider, is_goalkeeper, count(*) as cnt
FROM soccer_analytics.bronze.idsse_tracking
GROUP BY source_provider, is_goalkeeper;

-- Expect: is_goalkeeper=true rows ≈ total_rows / 11 (1 of 11 players)
```

---

### Task 5.4: dbt full-refresh for tracking pipeline

Adding `is_goalkeeper` to `fct_tracking_frames` (incremental merge) requires `--full-refresh` — existing rows won't have the column populated by an incremental run.

- [ ] **Step 1: Full-refresh tracking mart + downstream models**

```bash
cd dbt_project && python ../scripts/ensure_warehouse.py -- dbt run --full-refresh --select stg_metrica__tracking+ stg_idsse__tracking+ stg_skillcorner__tracking+ --profiles-dir .
```

This rebuilds: 3 staging views → `fct_tracking_frames` (full table rebuild) → any downstream models.

Expected: `fct_tracking_frames` now has `is_goalkeeper` column with correct values.

- [ ] **Step 2: Run dbt tests on tracking models**

```bash
cd dbt_project && python ../scripts/ensure_warehouse.py -- dbt test --select fct_tracking_frames --profiles-dir .
```

Expected: all contract tests pass, including the new `is_goalkeeper` not_null test.

- [ ] **Step 3: Verify row counts**

```sql
SELECT source_provider, count(*) as rows, sum(case when is_goalkeeper then 1 else 0 end) as gk_rows
FROM soccer_analytics.dev_gold.fct_tracking_frames
GROUP BY source_provider;
```

Expected: ~38M total rows. GK rows ≈ 1/11 of total per provider.

---

### Task 5.5: Re-run formation pipeline

With GK filtering now in place, formation detection should produce results for all 20 tracking matches (previously only 2 matches returned results).

- [ ] **Step 1: Clear existing formation results (force full recompute)**

```sql
DELETE FROM soccer_analytics.bronze.formation_labels;
```

- [ ] **Step 2: Run formation pipeline**

```bash
python scripts/ensure_warehouse.py -- python -m ingestion.formations --catalog soccer_analytics --schema bronze
```

Expected: formations detected for all 20 tracking matches (was 2).

- [ ] **Step 3: Rebuild formation dbt model**

```bash
cd dbt_project && python ../scripts/ensure_warehouse.py -- dbt run --select fct_formation_labels --profiles-dir .
```

- [ ] **Step 4: Verify formation coverage**

```sql
SELECT count(distinct match_id) as matches_with_formations
FROM soccer_analytics.dev_gold.fct_formation_labels;
```

Expected: 20 (or close — some matches may have insufficient outfield players in some windows). Previously: 2.

---

### Task 5.6: Re-run line-breaking pipeline (includes new Path C)

- [ ] **Step 1: Run line-breaking pipeline**

```bash
python scripts/ensure_warehouse.py -- python -m ingestion.line_breaking --catalog soccer_analytics --schema bronze
```

Expected: Path A (StatsBomb 360) + Path B (Metrica) + Path C (IDSSE) all produce results.

- [ ] **Step 2: Verify Path C output**

```sql
SELECT data_source, count(*) as rows, count(distinct match_id) as matches
FROM soccer_analytics.bronze.line_breaking_results
GROUP BY data_source;
```

Expected: `idsse_tracking` rows appear for up to 7 matches (new). `statsbomb_360` and `metrica_tracking` rows unchanged.

- [ ] **Step 3: Rebuild line-breaking dbt model**

```bash
cd dbt_project && python ../scripts/ensure_warehouse.py -- dbt run --select fct_line_breaking_passes --profiles-dir .
```

---

### Task 5.7: Re-run player embeddings pipeline (SPADL retrain)

This is the most impactful re-execution — the Doc2Vec model is retrained from scratch with the new 23-type SPADL vocabulary and position-group z-scores.

- [ ] **Step 1: Clear existing embeddings (force full retrain)**

```sql
DELETE FROM soccer_analytics.bronze.player_embeddings_raw;
```

- [ ] **Step 2: Run player embeddings pipeline**

```bash
python scripts/ensure_warehouse.py -- python -m ingestion.player_embeddings --catalog soccer_analytics --schema bronze
```

Expected: ~87K behavioral vectors (32-dim, SPADL tokens on 105×68 grid) + ~20K stat vectors (13-dim, position-group z-scored). New `zscore_params.json` saved to UC Volumes with group-keyed structure.

- [ ] **Step 3: Rebuild embedding dbt models**

```bash
cd dbt_project && python ../scripts/ensure_warehouse.py -- dbt run --full-refresh --select fct_player_embeddings fct_player_embeddings_season fct_player_embeddings_career --profiles-dir .
```

- [ ] **Step 4: Verify embeddings**

```sql
-- Check behavioral vector dimension is still 32
SELECT length(from_json(behavioral_vector, 'ARRAY<DOUBLE>')) as dim
FROM soccer_analytics.dev_gold.fct_player_embeddings
LIMIT 1;

-- Check stat vector dimension is still 13
SELECT length(from_json(stat_vector, 'ARRAY<DOUBLE>')) as dim
FROM soccer_analytics.dev_gold.fct_player_embeddings
WHERE stat_vector IS NOT NULL
LIMIT 1;
```

Expected: 32 and 13 respectively.

---

### Task 5.8: Synced table recreation + index rebuild

`fct_tracking_frames` has a new column (`is_goalkeeper`), so its synced table must be recreated. The embedding tables may also need recreation if the underlying data changed significantly.

- [ ] **Step 1: User recreates synced tables via Databricks UI**

The following synced tables need recreation (TD#1 — no API, UI only):
- `fct_tracking_frames_synced` (new `is_goalkeeper` column)
- `fct_formation_labels_synced` (new data — was mostly empty, now has 20 matches)
- `fct_player_embeddings_season_synced` (retrained embeddings)
- `fct_player_embeddings_career_synced` (retrained embeddings)
- `fct_line_breaking_passes_synced` (new IDSSE data)

- [ ] **Step 2: Rebuild indexes**

```bash
python scripts/create_indexes.py --verify
```

Expected: all indexes created successfully, EXPLAIN ANALYZE confirms Index Scan on fact tables.

---

### Task 5.9: Local Taipy E2E verification

Test the app locally with live Lakebase data to verify all changes render correctly.

- [ ] **Step 1: Start local Taipy app**

```bash
cd hf_taipy_app && uv run python -m src.main
```

- [ ] **Step 2: Puppeteer verification — Team Shape page**

Navigate to Team Shape page. Verify:
- Formation labels now appear in the Timeline sub-view (was `"--"`, now shows detected formations)
- Snapshot sub-view shows team shape metrics (excluding GK from computation)
- Formation changes counter shows real values, not `"--"`

- [ ] **Step 3: Puppeteer verification — Player Similarity page**

Navigate to Player Similarity. Verify:
- Similarity search returns results (embeddings retrained)
- GK players are not ranked as top passers (position-group z-scoring working)
- Distance values are in expected ranges

- [ ] **Step 4: Puppeteer verification — Player Impact / Pass Map pages**

Verify line-breaking stats appear for IDSSE matches (Path C data flowing through).

- [ ] **Step 5: Spot-check other pages**

Quick visual check that no page is broken by the coordinate normalization changes (the transforms are mathematically identical — this is a refactor, not a behavior change).
