# Analytics Quality Cycle Q1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PAUSA activity filtering (D25), calibration anchors via percentile ranks (U1), incremental `fct_match_summary` (O1), team shape analytics module (D19), and Numba pitch control benchmark (D24).

**Architecture:** dbt mart-layer additions (2 new models, 1 incremental conversion) feed new Lakebase synced tables consumed by the Taipy dashboard. A pure NumPy/scipy analytics module (D19) is standalone with unit tests. D24 is a benchmark-only evaluation with a go/no-go gate.

**Tech Stack:** dbt (Databricks SQL), Python 3.10, NumPy, scipy, Numba (dev-only), Taipy, Lakebase (PostgreSQL), pytest-benchmark.

**Spec:** `docs/superpowers/specs/2026-03-24-analytics-quality-cycle-q1-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `dbt_project/models/marts/fct_pausa_rankings.sql` | D25: Player-level PAUSA aggregate with activity filters |
| `dbt_project/models/marts/fct_player_percentiles.sql` | U1: Per-competition percentile ranks for all player metrics |
| `src/analytics/team_shape.py` | D19: Team shape spatial metrics (centroid, hull, stretch, lines) |
| `src/tests/test_team_shape.py` | D19: Unit tests for team shape module |
| `src/analytics/pitch_control_numba.py` | D24: Numba JIT evaluation kernels (may be removed) |
| `docs/decisions/d24-numba-evaluation.md` | D24: Benchmark findings and decision |

### Modified Files

| File | Change |
|------|--------|
| `dbt_project/models/marts/fct_match_summary.sql` | O1: table → incremental |
| `dbt_project/models/marts/_marts__models.yml` | D25+U1: Contracts for 2 new models |
| `hf_taipy_app/src/state/pass_timing.py` | D25: Aggregate rankings query + filter sliders |
| `hf_taipy_app/src/pages/pass_timing.py` | D25: Slider SidebarWidgets in PageConfig |
| `hf_taipy_app/src/state/player_radar.py` | U1: Percentile-based radar scaling |
| `hf_taipy_app/src/state/action_values.py` | U1: Percentile column in rankings |
| `hf_taipy_app/src/state/defensive_valuation.py` | U1: Percentile column in rankings |
| `hf_taipy_app/src/state/match_summary.py` | U1: League average reference text |
| `hf_taipy_app/src/template.py` | D25+U1: Glossary terms |
| `src/tests/test_benchmarks.py` | D24: Numba benchmark class |
| `pyproject.toml` | D24: `numba` in dev dependency group |
| `scripts/create_indexes.py` | D25+U1: Indexes for 2 new synced tables |

---

## Task 1: D19 — Team Shape Params and Result Dataclasses

**Files:**
- Create: `src/analytics/team_shape.py`
- Create: `src/tests/test_team_shape.py`

- [ ] **Step 1: Write failing test for TeamShapeParams defaults**

```python
# src/tests/test_team_shape.py
"""Tests for team shape spatial metrics module (D19)."""

from __future__ import annotations

import math

import numpy as np
import pytest

from analytics.team_shape import TeamShapeParams, TeamShapeResult


class TestTeamShapeParams:
    """Verify params dataclass defaults and overrides."""

    def test_defaults(self) -> None:
        p = TeamShapeParams()
        assert p.n_defensive_lines == 3
        assert p.min_players == 3
        assert p.pitch_length == 120.0
        assert p.pitch_width == 80.0

    def test_override(self) -> None:
        p = TeamShapeParams(n_defensive_lines=4, min_players=4)
        assert p.n_defensive_lines == 4
        assert p.min_players == 4

    def test_frozen(self) -> None:
        p = TeamShapeParams()
        with pytest.raises(AttributeError):
            p.n_defensive_lines = 5  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_team_shape.py::TestTeamShapeParams -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analytics.team_shape'`

- [ ] **Step 3: Write minimal implementation — dataclasses only**

```python
# src/analytics/team_shape.py
"""Team shape spatial metrics — centroid, convex hull, stretch index, defensive lines.

Computes per-team per-frame spatial metrics from player positions:
1. Team centroid (mean x, y)
2. Convex hull area (scipy.spatial.ConvexHull)
3. Team length and width (max spread along attacking/lateral axes)
4. Stretch index — mean distance from centroid (Clemente et al. 2013)
5. Defensive line height — mean position of deepest Ward cluster
6. Inter-line gaps — distances between cluster centroids along the y-axis

Reference: Clemente, F.M. et al. (2013). "Collective tactical behaviour
in association football: A systematic review."
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TeamShapeParams:
    """Tunable parameters for team shape computation."""

    n_defensive_lines: int = 3       # Ward clustering cluster count
    min_players: int = 3             # Minimum players for convex hull
    pitch_length: float = 120.0      # StatsBomb x-axis extent
    pitch_width: float = 80.0        # StatsBomb y-axis extent


@dataclass(frozen=True)
class TeamShapeResult:
    """Per-team per-frame spatial shape metrics."""

    centroid_x: float                # Team centroid x coordinate
    centroid_y: float                # Team centroid y coordinate
    convex_hull_area: float          # Convex hull area in coordinate units²
    team_length: float               # Max spread along attacking axis (x)
    team_width: float                # Max spread along lateral axis (y)
    stretch_index: float             # Mean distance from centroid
    defensive_line_height: float     # Mean x-position of deepest cluster
    inter_line_gaps: tuple[float, ...]  # Gaps between cluster centroids
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_team_shape.py::TestTeamShapeParams -v`
Expected: 3 PASSED

---

## Task 2: D19 — `compute_team_shape` Core Function

**Files:**
- Modify: `src/analytics/team_shape.py`
- Modify: `src/tests/test_team_shape.py`

- [ ] **Step 1: Write failing tests for `compute_team_shape`**

Append to `src/tests/test_team_shape.py`:

```python
from analytics.team_shape import compute_team_shape


def _make_rectangle() -> tuple[np.ndarray, np.ndarray]:
    """4 players in a 20x10 rectangle — known geometry.

    Positions: (50,35), (70,35), (50,45), (70,45)
    Centroid: (60, 40). Hull area: 20*10=200.
    Length (x-spread): 20. Width (y-spread): 10.
    Stretch index: mean distance from (60,40) = sqrt(10²+5²) ≈ 11.18.
    """
    x = np.array([50.0, 70.0, 50.0, 70.0])
    y = np.array([35.0, 35.0, 45.0, 45.0])
    return x, y


def _make_442() -> tuple[np.ndarray, np.ndarray]:
    """10 outfield players in a 4-4-2 formation (no GK).

    Three clear lines along x-axis for Ward clustering:
    - Back 4: x ≈ 30
    - Midfield 4: x ≈ 60
    - Forward 2: x ≈ 90
    """
    x = np.array([28.0, 30.0, 32.0, 30.0,   # back 4
                   58.0, 60.0, 62.0, 60.0,   # midfield 4
                   88.0, 92.0])               # forward 2
    y = np.array([20.0, 35.0, 50.0, 65.0,    # back 4 spread
                   20.0, 35.0, 50.0, 65.0,    # midfield 4 spread
                   35.0, 50.0])               # forward 2
    return x, y


class TestComputeTeamShape:
    """Core compute_team_shape function tests."""

    def test_rectangle_centroid(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        assert abs(result.centroid_x - 60.0) < 1e-10
        assert abs(result.centroid_y - 40.0) < 1e-10

    def test_rectangle_hull_area(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        np.testing.assert_allclose(result.convex_hull_area, 200.0, atol=1e-10)

    def test_rectangle_length_width(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        np.testing.assert_allclose(result.team_length, 20.0, atol=1e-10)
        np.testing.assert_allclose(result.team_width, 10.0, atol=1e-10)

    def test_rectangle_stretch_index(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        expected = np.sqrt(10.0**2 + 5.0**2)  # all 4 equidistant from centroid
        np.testing.assert_allclose(result.stretch_index, expected, atol=1e-10)

    def test_442_three_lines(self) -> None:
        """Ward clustering should find 3 clusters along the x-axis."""
        x, y = _make_442()
        result = compute_team_shape(x, y)
        assert len(result.inter_line_gaps) == 2  # 3 lines → 2 gaps
        # Gaps should be roughly 30 units each (30→60→90)
        for gap in result.inter_line_gaps:
            assert 25.0 < gap < 35.0

    def test_442_defensive_line(self) -> None:
        """Defensive line height should be near x=30 (the back 4)."""
        x, y = _make_442()
        result = compute_team_shape(x, y)
        assert 25.0 < result.defensive_line_height < 35.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_team_shape.py::TestComputeTeamShape -v`
Expected: FAIL — `ImportError: cannot import name 'compute_team_shape'`

- [ ] **Step 3: Implement `compute_team_shape`**

Add to `src/analytics/team_shape.py` after the dataclasses:

```python
import math

import numpy as np
from scipy.spatial import ConvexHull, QhullError  # type: ignore[import-untyped]
from scipy.cluster.hierarchy import fcluster, linkage  # type: ignore[import-untyped]


def _nan_result() -> TeamShapeResult:
    """Return a result with all NaN values for insufficient data."""
    return TeamShapeResult(
        centroid_x=math.nan,
        centroid_y=math.nan,
        convex_hull_area=math.nan,
        team_length=math.nan,
        team_width=math.nan,
        stretch_index=math.nan,
        defensive_line_height=math.nan,
        inter_line_gaps=(),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_team_shape(
    players_x: np.ndarray,
    players_y: np.ndarray,
    params: TeamShapeParams | None = None,
) -> TeamShapeResult:
    """Compute spatial shape metrics for a single team in a single frame.

    Parameters
    ----------
    players_x : 1-D array of player x-coordinates (StatsBomb 120×80).
    players_y : 1-D array of player y-coordinates.
    params : Optional tuning parameters. Defaults to ``TeamShapeParams()``.

    Returns
    -------
    TeamShapeResult with centroid, hull area, length, width, stretch index,
    defensive line height, and inter-line gaps.
    """
    if params is None:
        params = TeamShapeParams()

    n = len(players_x)
    if n < params.min_players:
        return _nan_result()

    px = np.asarray(players_x, dtype=np.float64)
    py = np.asarray(players_y, dtype=np.float64)

    # Centroid
    cx = float(np.mean(px))
    cy = float(np.mean(py))

    # Team length (x-spread) and width (y-spread)
    team_length = float(np.ptp(px))
    team_width = float(np.ptp(py))

    # Stretch index: mean Euclidean distance from centroid
    dists = np.sqrt((px - cx) ** 2 + (py - cy) ** 2)
    stretch = float(np.mean(dists))

    # Convex hull area
    try:
        points = np.column_stack((px, py))
        hull = ConvexHull(points)
        hull_area = float(hull.volume)  # 2-D: volume = area
    except QhullError:
        hull_area = 0.0

    # Ward clustering along the attacking axis (x) for defensive lines
    n_clusters = min(params.n_defensive_lines, n)
    if n_clusters < 2:
        defensive_line_height = float(np.min(px))
        gaps: tuple[float, ...] = ()
    else:
        x_col = px.reshape(-1, 1)
        Z = linkage(x_col, method="ward")
        labels = fcluster(Z, t=n_clusters, criterion="maxclust")

        # Compute cluster centroids along x, sorted ascending
        cluster_x_means = np.array(
            [float(np.mean(px[labels == c])) for c in range(1, n_clusters + 1)]
        )
        cluster_x_means.sort()

        defensive_line_height = float(cluster_x_means[0])
        gaps = tuple(float(cluster_x_means[i + 1] - cluster_x_means[i])
                     for i in range(len(cluster_x_means) - 1))

    return TeamShapeResult(
        centroid_x=cx,
        centroid_y=cy,
        convex_hull_area=hull_area,
        team_length=team_length,
        team_width=team_width,
        stretch_index=stretch,
        defensive_line_height=defensive_line_height,
        inter_line_gaps=gaps,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_team_shape.py::TestComputeTeamShape -v`
Expected: 6 PASSED

---

## Task 3: D19 — `compute_team_shape_frame` Wrapper + Edge Cases

**Files:**
- Modify: `src/analytics/team_shape.py`
- Modify: `src/tests/test_team_shape.py`

- [ ] **Step 1: Write failing tests**

Append to `src/tests/test_team_shape.py`:

```python
import pandas as pd

from analytics.team_shape import compute_team_shape_frame


class TestComputeTeamShapeFrame:
    """DataFrame wrapper tests."""

    def test_both_teams(self) -> None:
        df = pd.DataFrame({
            "x": [50.0, 70.0, 50.0, 70.0, 30.0, 40.0, 30.0, 40.0],
            "y": [35.0, 35.0, 45.0, 45.0, 20.0, 20.0, 30.0, 30.0],
            "team": ["home"] * 4 + ["away"] * 4,
        })
        result = compute_team_shape_frame(df)
        assert set(result.keys()) == {"home", "away"}
        np.testing.assert_allclose(result["home"].centroid_x, 60.0, atol=1e-10)
        np.testing.assert_allclose(result["away"].centroid_x, 35.0, atol=1e-10)

    def test_ignores_non_team_rows(self) -> None:
        """Rows with team values other than home/away are ignored."""
        df = pd.DataFrame({
            "x": [50.0, 70.0, 50.0, 99.0],
            "y": [35.0, 35.0, 45.0, 99.0],
            "team": ["home", "home", "home", "ball"],
        })
        result = compute_team_shape_frame(df)
        assert "ball" not in result
        assert "home" in result


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_arrays(self) -> None:
        result = compute_team_shape(np.array([]), np.array([]))
        assert math.isnan(result.centroid_x)
        assert result.inter_line_gaps == ()

    def test_single_player(self) -> None:
        result = compute_team_shape(np.array([50.0]), np.array([40.0]))
        assert math.isnan(result.centroid_x)  # below min_players

    def test_two_players(self) -> None:
        result = compute_team_shape(np.array([50.0, 70.0]), np.array([40.0, 40.0]))
        assert math.isnan(result.centroid_x)  # below min_players=3

    def test_three_collinear(self) -> None:
        """Three collinear players — ConvexHull can't form 2-D hull."""
        result = compute_team_shape(
            np.array([50.0, 60.0, 70.0]),
            np.array([40.0, 40.0, 40.0]),
        )
        assert result.convex_hull_area == 0.0
        assert result.team_width == 0.0
        assert result.team_length == 20.0

    def test_all_same_position(self) -> None:
        x = np.array([50.0, 50.0, 50.0])
        y = np.array([40.0, 40.0, 40.0])
        result = compute_team_shape(x, y)
        assert result.stretch_index == 0.0
        assert result.team_length == 0.0
        assert result.convex_hull_area == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_team_shape.py::TestComputeTeamShapeFrame src/tests/test_team_shape.py::TestEdgeCases -v`
Expected: FAIL — `cannot import name 'compute_team_shape_frame'`

- [ ] **Step 3: Implement `compute_team_shape_frame`**

Add to `src/analytics/team_shape.py` after `compute_team_shape`:

```python
def compute_team_shape_frame(
    players_df: pd.DataFrame,
    params: TeamShapeParams | None = None,
) -> dict[str, TeamShapeResult]:
    """Compute shape metrics for both teams in a single frame.

    Parameters
    ----------
    players_df : DataFrame with columns ``x``, ``y``, ``team``.
        Only rows with ``team in ("home", "away")`` are processed.
    params : Optional tuning parameters.

    Returns
    -------
    Dict mapping team name to TeamShapeResult.
    """
    if params is None:
        params = TeamShapeParams()

    results: dict[str, TeamShapeResult] = {}
    for team in ("home", "away"):
        mask = players_df["team"] == team
        team_df = players_df.loc[mask]
        if team_df.empty:
            continue
        results[team] = compute_team_shape(
            team_df["x"].to_numpy(),
            team_df["y"].to_numpy(),
            params,
        )
    return results
```

Also add the `pd` import near the top (after numpy):

```python
import pandas as pd
```

- [ ] **Step 4: Run full test suite for D19**

Run: `uv run pytest src/tests/test_team_shape.py -v`
Expected: All tests PASSED (params: 3, core: 6, frame: 2, edge: 5 = 16 total)

- [ ] **Step 5: Run linting and type check**

Run: `uv run ruff check src/analytics/team_shape.py src/tests/test_team_shape.py && uv run pyright src/analytics/team_shape.py`
Expected: 0 violations, 0 errors

---

## Task 4: O1 — `fct_match_summary` Incremental Conversion

**Files:**
- Modify: `dbt_project/models/marts/fct_match_summary.sql`

- [ ] **Step 1: Add config block and incremental guard**

Add config block at the very top of the file (before the comment on line 1):

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='match_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
```

Add `existing_matches` as the **FIRST CTE** in the `with` block (before `matches`). This avoids circular reference — `existing_matches` reads from `{{ this }}`, not from any other CTE:

```sql
with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

matches as (
    select * from {{ ref('stg_statsbomb__matches') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
),
```

- [ ] **Step 2: Add incremental filters to remaining source CTEs**

In `match_team_ids` (after `where team_id is not null`):
```sql
    where team_id is not null
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}
```

In `shots` and `passes` CTEs:
```sql
shots as (
    select * from {{ ref('fct_shots') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
),

passes as (
    select * from {{ ref('fct_passes') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
),
```

In `defensive_actions` (after `and location_x > ...`):
```sql
      and location_x > {{ var('pitch_length') }} * 0.4
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}
```

In `opponent_passes_in_def_zone` (after `and location_x < ...`):
```sql
      and location_x < {{ var('pitch_length') }} * 0.6
    {% if is_incremental() %}
      and match_id not in (select match_id from existing_matches)
    {% endif %}
```

- [ ] **Step 3: Validate SQL compiles**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/dbt_project && uv run dbt compile --select fct_match_summary`
Expected: Compiles without error. Check `target/compiled/` output for correct SQL.

---

## Task 5: D25 — `fct_pausa_rankings` dbt Model

**Files:**
- Create: `dbt_project/models/marts/fct_pausa_rankings.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (insert after line 1773)

- [ ] **Step 1: Create the dbt model**

```sql
-- dbt_project/models/marts/fct_pausa_rankings.sql
{{ config(
    materialized='table',
    liquid_clustered_by=['player_id']
) }}
-- fct_pausa_rankings.sql
-- Player-level PAUSA aggregate with activity quality filters.
--
-- Aggregates pass-level PAUSA scores to one row per player across all matches.
-- Uses actual_obso > 0 as a quality proxy for "successful" passes, since
-- IDSSE event data lacks a pass outcome attribute.
--
-- Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa."

{% if var('pausa_enabled', false) %}

with pass_quality as (

    select * from {{ ref('int_pausa__pass_quality') }}

),

physical_minutes as (

    select
        cast(player_id as string) as player_id,
        sum(minutes_played)       as total_minutes
    from {{ ref('fct_physical_stats') }}
    group by cast(player_id as string)

),

aggregated as (

    select
        pq.player_id,
        pq.player_display_name,
        count(distinct pq.match_id)                                  as total_matches,
        count(*)                                                     as total_passes,
        sum(case when pq.actual_obso > 0 then 1 else 0 end)         as passes_with_value,
        avg(pq.pausa_score)                                          as avg_pausa,
        avg(pq.temporal_judgment)                                    as avg_temporal_judgment,
        avg(pq.spatial_selection)                                    as avg_spatial_selection,
        percentile_approx(pq.pausa_score, 0.5)                      as median_pausa

    from pass_quality pq
    group by pq.player_id, pq.player_display_name

),

final as (

    select
        cast(a.player_id as string)                                  as player_id,
        cast(a.player_display_name as string)                        as player_display_name,
        cast(a.total_matches as int)                                 as total_matches,
        cast(a.total_passes as int)                                  as total_passes,
        cast(a.passes_with_value as int)                             as passes_with_value,
        cast(a.avg_pausa as double)                                  as avg_pausa,
        cast(a.avg_temporal_judgment as double)                      as avg_temporal_judgment,
        cast(a.avg_spatial_selection as double)                      as avg_spatial_selection,
        cast(a.median_pausa as double)                               as median_pausa,
        cast(pm.total_minutes as double)                             as total_minutes,
        current_timestamp()                                          as _loaded_at

    from aggregated a
    left join physical_minutes pm
        on a.player_id = pm.player_id

)

select * from final

{% else %}

-- PAUSA not enabled — produce empty table with correct schema
select
    cast(null as string)    as player_id,
    cast(null as string)    as player_display_name,
    cast(null as int)       as total_matches,
    cast(null as int)       as total_passes,
    cast(null as int)       as passes_with_value,
    cast(null as double)    as avg_pausa,
    cast(null as double)    as avg_temporal_judgment,
    cast(null as double)    as avg_spatial_selection,
    cast(null as double)    as median_pausa,
    cast(null as double)    as total_minutes,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
```

- [ ] **Step 2: Add contract to `_marts__models.yml`**

Insert after the `fct_pass_timing` contract (after the `_loaded_at` column definition, before `fct_workflow_costs`):

```yaml
  - name: fct_pausa_rankings
    description: >
      Player-level PAUSA aggregate with activity quality filters.
      One row per player, aggregated across all matches.
      Uses actual_obso > 0 as quality proxy for pass filtering.
    config:
      contract:
        enforced: true
    columns:
      - name: player_id
        data_type: string
        description: Player identifier.
        tests:
          - unique
          - not_null
      - name: player_display_name
        data_type: string
        description: Human-readable player name from dim_players.
      - name: total_matches
        data_type: int
        description: Number of distinct matches this player appears in.
      - name: total_passes
        data_type: int
        description: Total passes evaluated by PAUSA pipeline.
      - name: passes_with_value
        data_type: int
        description: >
          Passes with actual_obso > 0 (quality proxy for successful passes).
          Used as the primary activity filter threshold.
      - name: avg_pausa
        data_type: double
        description: Average PAUSA composite score across all passes (0-1, higher = better timing).
      - name: avg_temporal_judgment
        data_type: double
        description: Average temporal judgment component (0-1).
      - name: avg_spatial_selection
        data_type: double
        description: Average spatial selection component (0-1).
      - name: median_pausa
        data_type: double
        description: Median PAUSA score via percentile_approx.
      - name: total_minutes
        data_type: double
        description: >
          Sum of minutes_played from fct_physical_stats (tracking-derived).
          NULL if player has no tracking data. IDSSE-only for current dataset.
      - name: _loaded_at
        data_type: timestamp
        description: Audit timestamp.
```

- [ ] **Step 3: Validate model compiles**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/dbt_project && uv run dbt compile --select fct_pausa_rankings`
Expected: Compiles without error.

---

## Task 6: U1 — `fct_player_percentiles` dbt Model

**Files:**
- Create: `dbt_project/models/marts/fct_player_percentiles.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create the dbt model**

```sql
-- dbt_project/models/marts/fct_player_percentiles.sql
{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_player_percentiles.sql
-- Per-competition percentile ranks for all player metrics.
--
-- Provides calibration context for raw metric values (CHI-AUDIT-180).
-- Grain: one row per (player_id, competition_id, season_id).
-- Percentiles computed via PERCENT_RANK() within each competition/season.

with player_stats as (

    select
        cast(ps.player_id as string) as player_id,
        ps.competition_id,
        ps.season_id,
        dp.player_display_name,
        ps.minutes_played,
        ps.xg_per_90,
        ps.goals_per_90,
        ps.passes_per_90,
        ps.progressive_passes_per_90,
        ps.pass_completion_pct,
        ps.vaep_per_90,
        ps.offensive_vaep_per_90,
        ps.defensive_vaep_per_90,
        ps.line_breaking_per_90
        {% if var('defcon_enabled', false) %}
        , ps.defcon_per_90
        {% endif %}
    from {{ ref('fct_player_stats') }} ps
    left join {{ ref('dim_players') }} dp
        on cast(ps.player_id as string) = cast(dp.player_id as string)

),

physical_by_comp as (

    select
        cast(ps.player_id as string) as player_id,
        ms.competition_id,
        ms.season_id,
        avg(ps.distance_per_minute_m)  as avg_distance_per_minute,
        avg(ps.max_speed_ms)           as avg_max_speed
    from {{ ref('fct_physical_stats') }} ps
    inner join {{ ref('fct_match_summary') }} ms
        on cast(ps.match_id as string) = cast(ms.match_id as string)
    group by cast(ps.player_id as string), ms.competition_id, ms.season_id

),

{% if var('pausa_enabled', false) %}
pausa_agg as (

    select * from {{ ref('fct_pausa_rankings') }}

),
{% endif %}

enriched as (

    select
        s.player_id,
        s.competition_id,
        s.season_id,
        s.player_display_name,
        s.minutes_played,

        -- Core per-90 metrics
        s.xg_per_90,
        s.goals_per_90,
        s.passes_per_90,
        s.progressive_passes_per_90,
        s.pass_completion_pct,
        s.vaep_per_90,
        s.offensive_vaep_per_90,
        s.defensive_vaep_per_90,
        s.line_breaking_per_90,
        -- Always emit conditional columns (NULL when disabled) for static contract
        {% if var('defcon_enabled', false) %}
        s.defcon_per_90,
        {% else %}
        cast(null as double) as defcon_per_90,
        {% endif %}

        -- Physical stats (NULL for non-tracking)
        ph.avg_distance_per_minute,
        ph.avg_max_speed,

        {% if var('pausa_enabled', false) %}
        pa.avg_pausa
        {% else %}
        cast(null as double) as avg_pausa
        {% endif %}

    from player_stats s
    left join physical_by_comp ph
        on s.player_id = ph.player_id  -- both cast to string in their CTEs
        and s.competition_id = ph.competition_id
        and s.season_id = ph.season_id
    {% if var('pausa_enabled', false) %}
    left join pausa_agg pa
        on s.player_id = pa.player_id  -- both string type
    {% endif %}

),

percentiled as (

    select
        player_id,
        competition_id,
        season_id,
        player_display_name,
        minutes_played,

        percent_rank() over (partition by competition_id, season_id order by xg_per_90)                as xg_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by goals_per_90)             as goals_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by passes_per_90)            as passes_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by progressive_passes_per_90) as progressive_passes_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by pass_completion_pct)      as pass_completion_pct_pctile,
        percent_rank() over (partition by competition_id, season_id order by vaep_per_90)              as vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by offensive_vaep_per_90)    as offensive_vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by defensive_vaep_per_90)    as defensive_vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by line_breaking_per_90)     as line_breaking_per_90_pctile,
        -- Always emit conditional pctile columns (NULL when source is NULL)
        percent_rank() over (partition by competition_id, season_id order by defcon_per_90)            as defcon_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by avg_pausa)                as avg_pausa_pctile,

        percent_rank() over (partition by competition_id, season_id order by avg_distance_per_minute)  as distance_per_minute_pctile,
        percent_rank() over (partition by competition_id, season_id order by avg_max_speed)            as max_speed_pctile

    from enriched

)

select
    cast(player_id as string)          as player_id,
    cast(competition_id as int)        as competition_id,
    cast(season_id as int)             as season_id,
    cast(player_display_name as string) as player_display_name,
    cast(minutes_played as double)     as minutes_played,

    cast(xg_per_90_pctile as double)                as xg_per_90_pctile,
    cast(goals_per_90_pctile as double)             as goals_per_90_pctile,
    cast(passes_per_90_pctile as double)            as passes_per_90_pctile,
    cast(progressive_passes_per_90_pctile as double) as progressive_passes_per_90_pctile,
    cast(pass_completion_pct_pctile as double)      as pass_completion_pct_pctile,
    cast(vaep_per_90_pctile as double)              as vaep_per_90_pctile,
    cast(offensive_vaep_per_90_pctile as double)    as offensive_vaep_per_90_pctile,
    cast(defensive_vaep_per_90_pctile as double)    as defensive_vaep_per_90_pctile,
    cast(line_breaking_per_90_pctile as double)     as line_breaking_per_90_pctile,
    cast(defcon_per_90_pctile as double)            as defcon_per_90_pctile,
    cast(avg_pausa_pctile as double)                as avg_pausa_pctile,

    cast(distance_per_minute_pctile as double)      as distance_per_minute_pctile,
    cast(max_speed_pctile as double)                as max_speed_pctile,

    current_timestamp()                             as _loaded_at

from percentiled
```

- [ ] **Step 2: Add contract to `_marts__models.yml`**

Insert after the `fct_pausa_rankings` contract (added in Task 5):

```yaml
  - name: fct_player_percentiles
    description: >
      Per-competition percentile ranks for player metrics.
      Provides calibration context for raw values (CHI-AUDIT-180 U1).
      Grain: one row per (player_id, competition_id, season_id).
    config:
      contract:
        enforced: true
    columns:
      - name: player_id
        data_type: string
        description: Player identifier.
        tests:
          - not_null
      - name: competition_id
        data_type: int
        description: Competition identifier (partition key).
        tests:
          - not_null
      - name: season_id
        data_type: int
        description: Season identifier.
        tests:
          - not_null
      - name: player_display_name
        data_type: string
        description: Human-readable player name.
      - name: minutes_played
        data_type: double
        description: Total minutes played in this competition/season.
      - name: xg_per_90_pctile
        data_type: double
        description: Percentile rank of xG per 90 within competition/season (0-1).
      - name: goals_per_90_pctile
        data_type: double
        description: Percentile rank of goals per 90.
      - name: passes_per_90_pctile
        data_type: double
        description: Percentile rank of passes per 90.
      - name: progressive_passes_per_90_pctile
        data_type: double
        description: Percentile rank of progressive passes per 90.
      - name: pass_completion_pct_pctile
        data_type: double
        description: Percentile rank of pass completion percentage.
      - name: vaep_per_90_pctile
        data_type: double
        description: Percentile rank of VAEP per 90.
      - name: offensive_vaep_per_90_pctile
        data_type: double
        description: Percentile rank of offensive VAEP per 90.
      - name: defensive_vaep_per_90_pctile
        data_type: double
        description: Percentile rank of defensive VAEP per 90.
      - name: line_breaking_per_90_pctile
        data_type: double
        description: Percentile rank of line-breaking passes per 90.
      - name: defcon_per_90_pctile
        data_type: double
        description: Percentile rank of DEFCON per 90 (NULL when defcon_enabled=false).
      - name: avg_pausa_pctile
        data_type: double
        description: Percentile rank of avg PAUSA (NULL when pausa_enabled=false).
      - name: distance_per_minute_pctile
        data_type: double
        description: Percentile rank of distance per minute (NULL for non-tracking).
      - name: max_speed_pctile
        data_type: double
        description: Percentile rank of max speed (NULL for non-tracking).
      - name: _loaded_at
        data_type: timestamp
        description: Audit timestamp.
```

Note: `defcon_per_90_pctile` and `avg_pausa_pctile` are always emitted (as NULL when the feature flag is disabled). This keeps the contract static — no conditional columns. `PERCENT_RANK()` over a NULL column produces NULL, which is correct.

- [ ] **Step 3: Validate model compiles**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/dbt_project && uv run dbt compile --select fct_player_percentiles`
Expected: Compiles without error.

---

## Task 7: D25 — Taipy Pass Timing State + Page Updates

**Files:**
- Modify: `hf_taipy_app/src/state/pass_timing.py`
- Modify: `hf_taipy_app/src/pages/pass_timing.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Add aggregate rankings query to `pass_timing.py`**

Add new state variables (near existing declarations around line 60):

```python
pt_min_passes_with_value: int = 50
pt_min_minutes: int = 0
pt_per_match_min_passes: int = 5
pt_aggregate_rankings_data: pd.DataFrame = pd.DataFrame()
```

Add these to `__all__` list.

Add new fetch function after `_fetch_rankings()` (around line 197):

```python
@ttl_cache()
def _fetch_aggregate_rankings() -> pd.DataFrame:
    """Load fct_pausa_rankings (player-level aggregate, bounded)."""
    rankings_tbl = t("fct_pausa_rankings_synced")
    return execute_query(
        f"SELECT player_display_name, total_matches, total_passes, "
        f"  passes_with_value, avg_pausa, avg_temporal_judgment, "
        f"  avg_spatial_selection, median_pausa, total_minutes "
        f"FROM {rankings_tbl} "
        f"ORDER BY avg_pausa DESC "
        f"LIMIT 500",
    )
```

- [ ] **Step 2: Add filter logic in `_refresh_data()`**

In the `_refresh_data()` function, add aggregate rankings filtering. After the existing rankings fetch, add:

```python
# Aggregate rankings with activity filter
agg_df = _fetch_aggregate_rankings()
if not agg_df.empty:
    mask = agg_df["passes_with_value"] >= state.pt_min_passes_with_value
    if state.pt_min_minutes > 0:
        mask = mask & (agg_df["total_minutes"].fillna(0) >= state.pt_min_minutes)
    state.pt_aggregate_rankings_data = agg_df[mask].reset_index(drop=True)
else:
    state.pt_aggregate_rankings_data = agg_df
```

Also apply per-match filter to existing rankings:

```python
# Per-match rankings with min passes filter
rankings_df = _fetch_rankings()
if not rankings_df.empty:
    rankings_df = rankings_df[rankings_df["pass_count"] >= state.pt_per_match_min_passes].reset_index(drop=True)
state.pt_rankings_data = rankings_df
```

- [ ] **Step 3: Add slider callbacks**

```python
def pt_on_min_passes_change(state: State) -> None:
    """Refilter aggregate rankings when slider changes."""
    _refresh_data(state)


def pt_on_min_minutes_change(state: State) -> None:
    """Refilter aggregate rankings when minutes slider changes."""
    _refresh_data(state)


def pt_on_per_match_min_passes_change(state: State) -> None:
    """Refilter per-match rankings when slider changes."""
    _refresh_data(state)
```

Add callbacks to `__all__`.

- [ ] **Step 4: Update `pass_timing.py` page config**

Add `SidebarWidget` entries for the sliders in the `PageConfig`. Add the aggregate rankings table as a new `ContentBlock`.

- [ ] **Step 5: Add glossary terms to `template.py`**

Add to `GLOSSARY` dict:

```python
"Passes with Value": "Passes where the off-ball scoring opportunity (actual OBSO) was greater than zero. Used as a quality proxy for 'successful' passes when pass outcome data is unavailable.",
"Percentile Rank": "Where a player's metric sits relative to all other players in the same competition (0-1 scale, 1.0 = top of competition).",
```

Add to `PAGE_TERMS["Pass-Timing"]`:

```python
"Pass-Timing": ["PAUSA", "Temporal Judgment", "Spatial Selection", "OBSO", "Passes with Value"],
```

---

## Task 8: U1 — Taipy Percentile Integration (Radar, Rankings, Match Summary)

**Files:**
- Modify: `hf_taipy_app/src/state/player_radar.py`
- Modify: `hf_taipy_app/src/state/action_values.py`
- Modify: `hf_taipy_app/src/state/defensive_valuation.py`
- Modify: `hf_taipy_app/src/state/match_summary.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Update radar to use percentiles**

In `player_radar.py`, modify `_fetch_player_radar_stats()` to LEFT JOIN `fct_player_percentiles_synced` and replace hardcoded `(low, high)` ranges in `_DEFAULT_METRICS` with `(0.0, 1.0)` when percentile data is available. The radar spider becomes a percentile plot where 1.0 = top of competition.

Add a query for percentile data:

```python
@ttl_cache()
def _fetch_player_percentiles(player_id: str, comp_id: int) -> pd.DataFrame | None:
    """Fetch percentile ranks for a specific player in a competition."""
    pctile_tbl = t("fct_player_percentiles_synced")
    df = execute_query(
        f"SELECT * FROM {pctile_tbl} "
        f"WHERE player_id = %s AND competition_id = %s "
        f"LIMIT 1",
        (player_id, comp_id),
    )
    return df if not df.empty else None
```

When percentile data is available, use `_pctile` values for radar axes (0–1 scale). When not available (player not in percentile table), fall back to hardcoded ranges.

- [ ] **Step 2: Add percentile column to Player Impact rankings**

In `action_values.py`, modify `_fetch_rankings()` to LEFT JOIN `fct_player_percentiles_synced` on `(player_id, competition_id)` and select `vaep_per_90_pctile`. Add a "Pctile" column to the displayed rankings table. Format as percentage (e.g., "85th").

- [ ] **Step 3: Add percentile column to Defensive Impact rankings**

In `defensive_valuation.py`, the rankings query aggregates from `fct_defcon_pressure_synced` (not `fct_player_stats`). The percentile for DEFCON is in `fct_player_percentiles` keyed on `(player_id, competition_id, season_id)`. Add a post-fetch LEFT JOIN or a secondary query to annotate rankings with `defcon_per_90_pctile`.

- [ ] **Step 4: Add league average reference to Match Summary**

In `match_summary.py`, add a helper that queries league averages:

```python
@ttl_cache()
def _fetch_league_averages(comp_id: int) -> pd.DataFrame:
    """Fetch competition-wide averages for reference context."""
    tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT AVG(home_xg + away_xg) / 2 as avg_xg_per_team, "
        f"  AVG(home_possession_pct) as avg_possession, "
        f"  AVG((home_pass_completion_pct + away_pass_completion_pct) / 2) as avg_pass_completion "
        f"FROM {tbl} WHERE competition_id = %s",
        (comp_id,),
    )
```

Display as reference text below the match metrics (e.g., "League avg xG/team: 1.3").

- [ ] **Step 5: Update glossary**

Ensure `PAGE_TERMS` for Player-Comparison, Player-Impact, Defensive-Impact, and Match-Summary pages include "Percentile Rank".

---

## Task 9: D24 — Numba Pitch Control Benchmark

**Files:**
- Create: `src/analytics/pitch_control_numba.py`
- Modify: `src/tests/test_benchmarks.py`
- Modify: `pyproject.toml`
- Create: `docs/decisions/d24-numba-evaluation.md`

- [ ] **Step 1: Add `numba` to dev dependency group**

In `pyproject.toml`, add to the `[dependency-groups]` `dev` list (around line 143):

```toml
[dependency-groups]
dev = [
    "pytest-benchmark>=5.2.3",
    "numba>=0.60.0",
]
```

Run: `uv sync`

- [ ] **Step 2: Create Numba JIT kernels**

```python
# src/analytics/pitch_control_numba.py
"""Numba JIT evaluation kernels for pitch control (D24).

Mirrors _tti_numpy and _influence_numpy from pitch_control.py for
benchmarking. This file is an evaluation artifact — it may be removed
if Numba does not demonstrate sufficient speedup.
"""

from __future__ import annotations

import math

import numba  # type: ignore[import-untyped]
import numpy as np


@numba.njit(cache=True)
def tti_numba(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    reaction_time: float,
    max_acceleration: float,
) -> np.ndarray:
    """Compute time-to-intercept for all players to all targets.

    Parameters
    ----------
    player_pos_m : (n_players, 2) positions in metres.
    player_vel_m : (n_players, 2) velocities in m/s.
    target_m : (n_targets, 2) target positions in metres.
    reaction_time : Reaction time in seconds.
    max_acceleration : Maximum acceleration in m/s².

    Returns
    -------
    (n_players, n_targets) TTI array in seconds.
    """
    n_players = player_pos_m.shape[0]
    n_targets = target_m.shape[0]
    result = np.empty((n_players, n_targets), dtype=np.float64)

    for i in range(n_players):
        for j in range(n_targets):
            dx = target_m[j, 0] - player_pos_m[i, 0]
            dy = target_m[j, 1] - player_pos_m[i, 1]
            dist = math.sqrt(dx * dx + dy * dy)

            if dist < 1e-10:
                result[i, j] = reaction_time
                continue

            # Project velocity onto displacement direction
            v_proj = (player_vel_m[i, 0] * dx + player_vel_m[i, 1] * dy) / dist

            discriminant = v_proj * v_proj + 2.0 * max_acceleration * dist
            if discriminant < 0:
                result[i, j] = 1e6  # unreachable
            else:
                result[i, j] = reaction_time + (-v_proj + math.sqrt(discriminant)) / max_acceleration

    return result


@numba.njit(cache=True)
def influence_numba(
    team_tti: np.ndarray,
    opponent_min_tti: np.ndarray,
    sigma: float,
) -> np.ndarray:
    """Compute summed team influence via logistic sigmoid over TTI difference.

    Mirrors ``_influence_numpy`` — returns (n_targets,) summed influence,
    not per-player influence.

    Parameters
    ----------
    team_tti : (n_players, n_targets) TTI array.
    opponent_min_tti : (n_targets,) minimum opponent TTI per target.
    sigma : Sigmoid width parameter.

    Returns
    -------
    (n_targets,) array of summed team influence values in [0, n_players].
    """
    k = math.pi / math.sqrt(3.0) / sigma
    n_players = team_tti.shape[0]
    n_targets = team_tti.shape[1]
    result = np.zeros(n_targets, dtype=np.float64)

    for i in range(n_players):
        for j in range(n_targets):
            exponent = -k * (opponent_min_tti[j] - team_tti[i, j])
            if exponent > 50.0:
                exponent = 50.0
            elif exponent < -50.0:
                exponent = -50.0
            result[j] += 1.0 / (1.0 + math.exp(exponent))

    return result
```

- [ ] **Step 3: Write parity test**

Add to `src/tests/test_benchmarks.py`:

```python
try:
    from analytics.pitch_control_numba import influence_numba, tti_numba

    _USE_NUMBA = True
except ImportError:
    _USE_NUMBA = False


def _to_metres(players_df: pd.DataFrame, params: PitchControlParams) -> tuple[np.ndarray, np.ndarray]:
    """Convert StatsBomb DataFrame to metre-space arrays for benchmarking.

    Returns (positions_m, velocities_m) for the home team only.
    """
    from analytics.pitch_control import _col_f64, _sb_to_meters_x, _sb_to_meters_y

    home = pd.DataFrame(players_df[players_df["team"] == "home"])
    pos = np.column_stack([
        _sb_to_meters_x(_col_f64(home, "x"), params),
        _sb_to_meters_y(_col_f64(home, "y"), params),
    ])
    vel = np.column_stack([
        _sb_to_meters_x(_col_f64(home, "velocity_x"), params),
        _sb_to_meters_y(_col_f64(home, "velocity_y"), params),
    ])
    return pos, vel


@pytest.mark.skipif(not _USE_NUMBA, reason="Numba not installed")
class TestNumbaParity:
    """Verify Numba kernels produce identical results to NumPy."""

    def test_tti_parity(self, players_df: pd.DataFrame, target_points_22: np.ndarray,
                        pitch_control_params: PitchControlParams) -> None:
        from analytics.pitch_control import _sb_to_meters_x, _sb_to_meters_y, _tti_numpy

        pos_m, vel_m = _to_metres(players_df, pitch_control_params)
        targets_m = np.column_stack([
            _sb_to_meters_x(target_points_22[:, 0], pitch_control_params),
            _sb_to_meters_y(target_points_22[:, 1], pitch_control_params),
        ])

        numpy_result = _tti_numpy(pos_m, vel_m, targets_m,
                                  pitch_control_params.reaction_time,
                                  pitch_control_params.max_acceleration)
        numba_result = tti_numba(pos_m, vel_m, targets_m,
                                 pitch_control_params.reaction_time,
                                 pitch_control_params.max_acceleration)

        np.testing.assert_allclose(numba_result, numpy_result, atol=1e-10)

    def test_influence_parity(self) -> None:
        rng = np.random.default_rng(42)
        team_tti = rng.uniform(0.5, 3.0, size=(11, 22))
        opp_min_tti = rng.uniform(0.5, 2.0, size=(22,))
        sigma = 0.45

        from analytics.pitch_control import _influence_numpy

        numpy_result = _influence_numpy(team_tti, opp_min_tti, sigma)
        numba_result = influence_numba(team_tti, opp_min_tti, sigma)

        np.testing.assert_allclose(numba_result, numpy_result, atol=1e-10)
```

- [ ] **Step 4: Write benchmark tests**

Add to `src/tests/test_benchmarks.py`:

```python
@pytest.mark.skipif(not _USE_NUMBA, reason="Numba not installed")
class TestNumbaBenchmarks:
    """Benchmark Numba JIT vs NumPy for pitch control kernels."""

    def test_bench_numba_pitch_control_warm(
        self, benchmark: Any, players_df: pd.DataFrame,
        target_points_22: np.ndarray, pitch_control_params: PitchControlParams,
    ) -> None:
        """Numba warm benchmark — post-JIT-compile, against 5ms NumPy budget."""
        from analytics.pitch_control import _sb_to_meters_x, _sb_to_meters_y

        pos_m, vel_m = _to_metres(players_df, pitch_control_params)
        targets_m = np.column_stack([
            _sb_to_meters_x(target_points_22[:, 0], pitch_control_params),
            _sb_to_meters_y(target_points_22[:, 1], pitch_control_params),
        ])

        # Warmup: trigger JIT compilation
        tti_numba(pos_m, vel_m, targets_m,
                  pitch_control_params.reaction_time,
                  pitch_control_params.max_acceleration)

        def run() -> np.ndarray:
            return tti_numba(pos_m, vel_m, targets_m,
                             pitch_control_params.reaction_time,
                             pitch_control_params.max_acceleration)

        result = benchmark(run)
        assert result.shape == (pos_m.shape[0], targets_m.shape[0])

    def test_bench_numba_pitch_control_cold(
        self, benchmark: Any, players_df: pd.DataFrame,
        target_points_22: np.ndarray, pitch_control_params: PitchControlParams,
    ) -> None:
        """Numba cold benchmark — includes JIT compile time.

        Caveat: pytest-benchmark runs multiple iterations. Only the first
        iteration is truly cold. The median will reflect warm performance.
        For accurate cold-start measurement, see the single-invocation
        timing in docs/decisions/d24-numba-evaluation.md.
        """
        import numba as nb
        from analytics.pitch_control import _sb_to_meters_x, _sb_to_meters_y

        pos_m, vel_m = _to_metres(players_df, pitch_control_params)
        targets_m = np.column_stack([
            _sb_to_meters_x(target_points_22[:, 0], pitch_control_params),
            _sb_to_meters_y(target_points_22[:, 1], pitch_control_params),
        ])

        # Re-wrap without cache to measure compile overhead
        tti_numba_fresh = nb.njit(cache=False)(tti_numba.py_func)

        def run() -> np.ndarray:
            return tti_numba_fresh(pos_m, vel_m, targets_m,
                                   pitch_control_params.reaction_time,
                                   pitch_control_params.max_acceleration)

        result = benchmark(run)
        assert result.shape == (pos_m.shape[0], targets_m.shape[0])
```

- [ ] **Step 5: Run benchmarks and document findings**

Run: `uv run pytest src/tests/test_benchmarks.py::TestNumbaParity src/tests/test_benchmarks.py::TestNumbaBenchmarks -v --benchmark-enable`
Expected: Parity tests PASS, benchmark results printed.

Compare against existing NumPy benchmark:
Run: `uv run pytest src/tests/test_benchmarks.py::TestBenchmarks::test_bench_batched_pitch_control -v --benchmark-enable`

- [ ] **Step 6: Write decision document**

Create `docs/decisions/d24-numba-evaluation.md` with benchmark results and decision:
- If Numba warm ≥2x faster than NumPy: recommend adding as third dispatch tier.
- If not: recommend removing `pitch_control_numba.py` and closing D24 as "Resolved — No Numba."

---

## Task 10: Infrastructure — Synced Tables and PG Indexes

**Files:**
- Modify: `scripts/create_indexes.py`

Note: Synced tables must be created via Databricks UI (Terraform workaround, see Tech Debt #1). PG indexes are managed by `scripts/create_indexes.py`.

- [ ] **Step 1: Add index entries for new synced tables**

Add to `INDEXES` list in `scripts/create_indexes.py` (after existing PAUSA entries):

```python
# ── fct_pausa_rankings_synced — Player-level PAUSA aggregate ─────
# PR-1: activity filter on passes_with_value
("idx_pausa_rankings_passes_value", "fct_pausa_rankings_synced", "passes_with_value"),
# ── fct_player_percentiles_synced — Calibration anchors ──────────
# PP-1: competition + season + player lookup
("idx_player_pctile_comp_season_player", "fct_player_percentiles_synced", "competition_id, season_id, player_id"),
```

- [ ] **Step 2: Document synced table creation steps**

The following synced tables need manual creation via Databricks UI before the Taipy app can query them:
1. `fct_pausa_rankings_synced` → source: `soccer_analytics.dev_gold.fct_pausa_rankings`, PK: `player_id`, scheduling: SNAPSHOT
2. `fct_player_percentiles_synced` → source: `soccer_analytics.dev_gold.fct_player_percentiles`, PK: `(player_id, competition_id, season_id)`, scheduling: SNAPSHOT

After creation: `python scripts/create_indexes.py && python scripts/create_indexes.py --verify`

---

## Task 11: Linting, Type Check, Full Test Suite

**Files:** All modified/created files.

- [ ] **Step 1: Run Ruff lint on all changed Python files**

Run: `uv run ruff check src/analytics/team_shape.py src/analytics/pitch_control_numba.py src/tests/test_team_shape.py src/tests/test_benchmarks.py`
Expected: 0 violations

- [ ] **Step 2: Run Ruff format check**

Run: `uv run ruff format --check src/analytics/team_shape.py src/analytics/pitch_control_numba.py src/tests/test_team_shape.py`
Expected: All files formatted

- [ ] **Step 3: Run Pyright**

Run: `uv run pyright src/analytics/team_shape.py src/analytics/pitch_control_numba.py`
Expected: 0 errors in basic mode

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest src/tests/ -v --benchmark-disable`
Expected: All existing tests still pass + 16 new team shape tests + Numba parity tests pass

---

## Dependency Graph

```
Task 1 (D19 dataclasses) → Task 2 (D19 core) → Task 3 (D19 frame + edge cases)
Task 4 (O1 incremental) — independent
Task 5 (D25 dbt model) → Task 6 (U1 dbt model) → Task 8 (U1 Taipy)
Task 5 (D25 dbt model) → Task 7 (D25 Taipy)
Task 9 (D24 Numba) — independent
Task 10 (Infrastructure) — after Tasks 5+6
Task 11 (Linting) — after all other tasks
```

Parallelizable groups:
- **Group A:** Tasks 1→2→3 (D19)
- **Group B:** Task 4 (O1)
- **Group C:** Tasks 5→6→7→8 (D25+U1)
- **Group D:** Task 9 (D24)
- **Sequential:** Task 10 after C, Task 11 last
