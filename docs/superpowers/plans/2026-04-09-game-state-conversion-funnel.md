# PA1 + PA4: Game State Segmentation & Conversion Rate Funnel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-event game state to dbt mart models (PA1) and build a Conversion Rate Funnel dashboard page (PA4).

**Architecture:** New ephemeral intermediate model `int_running_score` computes cumulative scoreline from goal events. Three mart models (`fct_action_values`, `fct_shots`, `fct_passes`) gain a `game_state` column. `fct_action_values` also gains `possession_id` and `possession_team_id` from StatsBomb events. Taipy app gets a shared game state filter (conditionally shown) and a new dashboard page with horizontal mirror funnel bars.

**Tech Stack:** dbt (Spark SQL), Taipy, Plotly, Lakebase (PostgreSQL), Puppeteer MCP (E2E)

**Spec:** `docs/superpowers/specs/2026-04-09-game-state-conversion-funnel-design.md`

---

## Task 1: Create `int_running_score` Intermediate Model

**Files:**
- Create: `dbt_project/models/intermediate/int_running_score.sql`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`

- [ ] **Step 1: Create the intermediate model**

```sql
-- dbt_project/models/intermediate/int_running_score.sql
{{ config(materialized='ephemeral') }}
-- int_running_score.sql
-- Running scoreline per match — one kickoff row (0-0) plus one row per goal
-- with cumulative home/away scores. Ephemeral: inlined as CTE into consumers.
--
-- Used by fct_action_values, fct_shots, fct_passes to derive per-action
-- game_state (winning/losing/drawing from the acting team's perspective).
--
-- Known limitation: own goals are not tracked. An own goal does not appear
-- in int_unified_shots with shot_outcome = 'Goal', so the running score
-- may be inaccurate in matches with own goals (~3-5% of all goals).

with match_teams as (

    select
        match_id,
        cast(home_team_id as int) as home_team_id,
        cast(away_team_id as int) as away_team_id
    from {{ ref('stg_statsbomb__matches') }}

    union all

    select
        match_id,
        cast(home_team_id as int) as home_team_id,
        cast(away_team_id as int) as away_team_id
    from {{ ref('stg_wyscout__matches') }}

),

goals as (

    select
        s.match_id,
        s.team_id    as scoring_team_id,
        s.period,
        s.minute,
        s.second
    from {{ ref('int_unified_shots') }} s
    where s.shot_outcome = 'Goal'

),

goals_with_scores as (

    select
        g.match_id,
        mt.home_team_id,
        mt.away_team_id,
        g.period,
        g.minute,
        g.second,
        sum(case when g.scoring_team_id = mt.home_team_id then 1 else 0 end)
            over (partition by g.match_id
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as home_score_after,
        sum(case when g.scoring_team_id = mt.away_team_id then 1 else 0 end)
            over (partition by g.match_id
                  order by g.period, g.minute, g.second
                  rows between unbounded preceding and current row)
            as away_score_after
    from goals g
    inner join match_teams mt on g.match_id = mt.match_id

),

kickoffs as (

    select
        match_id,
        home_team_id,
        away_team_id,
        1    as period,
        0    as minute,
        0    as second,
        0    as home_score_after,
        0    as away_score_after
    from match_teams

)

select * from kickoffs
union all
select * from goals_with_scores
```

- [ ] **Step 2: Add YAML entry to `_intermediate__models.yml`**

Append after the `int_minutes_played` entry (after line 229):

```yaml

  - name: int_running_score
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Running scoreline per match. One kickoff row (0-0) plus one row per
      goal event with cumulative home/away scores. Ephemeral CTE consumed
      by mart models to derive per-action game state.
    columns:
      - name: match_id
        description: Match identifier
        data_tests:
          - not_null
      - name: home_team_id
        description: Home team identifier
        data_tests:
          - not_null
      - name: away_team_id
        description: Away team identifier
        data_tests:
          - not_null
      - name: period
        description: Match period of the score change
      - name: minute
        description: Match minute of the score change
      - name: second
        description: Match second of the score change
      - name: home_score_after
        description: Cumulative home team goals after this event
        data_tests:
          - not_null
      - name: away_score_after
        description: Cumulative away team goals after this event
        data_tests:
          - not_null
```

- [ ] **Step 3: Verify compilation**

Run: `cd dbt_project && uv run dbt compile -s int_running_score`
Expected: compiled SQL with no errors.

---

## Task 2: Add Game State + Possession to `fct_action_values`

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql`

- [ ] **Step 1: Replace the full model SQL**

Replace the entire content of `fct_action_values.sql` with:

```sql
{{ config(
    materialized='incremental',
    unique_key='action_value_id',
    liquid_clustered_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_action_values.sql
-- Gold-layer SPADL action values with VAEP scores, possession context,
-- and per-action game state.
--
-- Contains every on-ball action from all data sources converted to the
-- SPADL unified format, scored with offensive, defensive, and net VAEP
-- values. Enables player ranking by total contribution beyond goals/assists.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}

),

sb_events as (

    select
        event_id,
        possession,
        possession_team_id
    from {{ ref('stg_statsbomb__events') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

-- Join each action to its most recent score milestone.
-- The kickoff row (period=1, minute=0, second=0) ensures every action
-- in period >= 1 has at least one matching score row.
actions_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'av.match_id',
            'av.period',
            'av.time_seconds',
            'av.player_id',
            'av.type_id',
            'av.data_source'
        ]) }}                                       as action_value_id,

        av.match_id,
        av.player_id,
        av.team_id,
        av.competition_id,
        av.season_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,

        -- SPADL coordinates (105x68 meters)
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,

        -- Action classification
        av.action_type,
        av.action_result,
        av.bodypart,

        -- VAEP scores
        av.offensive_value,
        av.defensive_value,
        av.vaep_value,

        -- Possession context (StatsBomb only; NULL for Wyscout)
        sbe.possession                              as possession_id,
        sbe.possession_team_id,

        -- Running score for game state derivation
        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id                             as _rs_home_team_id,

        -- Rank to pick the most recent score milestone
        row_number() over (
            partition by
                av.match_id, av.period, av.time_seconds,
                av.player_id, av.type_id, av.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        )                                           as _score_rn,

        -- Provenance
        av.data_source,
        av.original_event_id

    from action_values av
    left join sb_events sbe
        on av.original_event_id = sbe.event_id
        and av.data_source = 'statsbomb'
    left join running_score rs
        on rs.match_id = av.match_id
        and (
            rs.period < av.period
            or (rs.period = av.period
                and (rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second))
        )

),

final as (

    select
        action_value_id,
        match_id,
        player_id,
        team_id,
        competition_id,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        action_type,
        action_result,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        possession_id,
        possession_team_id,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end                                         as game_state,
        data_source,
        original_event_id,
        current_timestamp()                         as _loaded_at

    from actions_with_score
    where _score_rn = 1

)

select * from final
```

- [ ] **Step 2: Verify compilation**

Run: `cd dbt_project && uv run dbt compile -s fct_action_values`
Expected: compiled SQL with no errors.

---

## Task 3: Add Game State to `fct_shots` and `fct_passes`

**Files:**
- Modify: `dbt_project/models/marts/fct_shots.sql`
- Modify: `dbt_project/models/marts/fct_passes.sql`

Both models follow the same pattern: add a `running_score` CTE, LEFT JOIN to it with the timestamp range condition, ROW_NUMBER to pick the most recent score, and compute `game_state` in the final SELECT.

- [ ] **Step 1: Add game state to `fct_shots.sql`**

Add after the existing `ws_matches` CTE (after line 50):

```sql
running_score as (

    select * from {{ ref('int_running_score') }}

),
```

In the `final` CTE, add the LEFT JOIN to `running_score` after the existing `ws_matches` LEFT JOIN:

```sql
    left join running_score rs
        on unified_shots.match_id = rs.match_id
        and (
            rs.period < unified_shots.period
            or (rs.period = unified_shots.period
                and (rs.minute * 60 + rs.second) <= (unified_shots.minute * 60 + unified_shots.second))
        )
```

Wrap the existing `final` CTE in a `shots_with_score` CTE that adds ROW_NUMBER, then create a new `final` CTE that filters and computes game_state:

```sql
shots_with_score as (

    select
        -- [all existing columns from current final CTE] --
        ...,

        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id                             as _rs_home_team_id,

        row_number() over (
            partition by unified_shots.event_id, unified_shots.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        )                                           as _score_rn

    from unified_shots
    left join sb_matches on unified_shots.match_id = sb_matches.match_id
    left join ws_matches on unified_shots.match_id = ws_matches.match_id
    left join running_score rs
        on unified_shots.match_id = rs.match_id
        and (
            rs.period < unified_shots.period
            or (rs.period = unified_shots.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_shots.minute * 60 + unified_shots.second))
        )

),

final as (

    select
        -- [all existing columns] --
        ...,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end                                         as game_state
    from shots_with_score
    where _score_rn = 1

)
```

Note: the `...` placeholders mean "copy all existing columns from the current `final` CTE." The implementer should preserve every existing column exactly and add the new ones.

- [ ] **Step 2: Add game state to `fct_passes.sql`**

Same pattern as `fct_shots.sql`:

1. Add `running_score` CTE after existing `line_breaking` CTE
2. Add `LEFT JOIN running_score rs ON ...` with the timestamp range condition
3. Rename existing `final` to `passes_with_score`, add `rs.home_score_after`, `rs.away_score_after`, `rs.home_team_id as _rs_home_team_id`, and `row_number() over (...) as _score_rn`
4. Create new `final` that filters `where _score_rn = 1` and computes `game_state`

The partition key for ROW_NUMBER in `fct_passes`: `partition by unified_passes.event_id, unified_passes.data_source`.

- [ ] **Step 3: Verify compilation of both models**

Run: `cd dbt_project && uv run dbt compile -s fct_shots fct_passes`
Expected: compiled SQL with no errors.

---

## Task 4: Update dbt Contracts and Tests

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Add new columns to `fct_action_values` contract**

Insert after `original_event_id` (line 682) and before `_loaded_at` (line 683):

```yaml
      - name: possession_id
        data_type: int
        description: >
          StatsBomb possession sequence number identifying the possession
          chain this action belongs to. NULL for Wyscout (no possession
          tracking in open data).
      - name: possession_team_id
        data_type: int
        description: >
          Team ID of the team in possession during this action.
          NULL for Wyscout.
      - name: game_state
        data_type: string
        description: >
          Game state from the acting team's perspective at the moment of
          this action: winning, losing, or drawing. Derived from
          int_running_score cumulative scoreline.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['winning', 'losing', 'drawing']
```

- [ ] **Step 2: Add `game_state` column to `fct_shots` contract**

Insert after the last column (before the next model definition), inside the `fct_shots` columns list (after `play_pattern`, around line 155):

```yaml
      - name: game_state
        data_type: string
        description: >
          Game state from the shooting team's perspective at the moment of
          the shot: winning, losing, or drawing.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['winning', 'losing', 'drawing']
```

- [ ] **Step 3: Add `game_state` column to `fct_passes` contract**

Insert after the last column in the `fct_passes` columns list (after `line_breaking_type`, around line 323):

```yaml
      - name: game_state
        data_type: string
        description: >
          Game state from the passing team's perspective at the moment of
          the pass: winning, losing, or drawing.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['winning', 'losing', 'drawing']
```

- [ ] **Step 4: Verify YAML syntax**

Run: `cd dbt_project && uv run dbt parse`
Expected: no parse errors.

---

## Task 5: Build and Verify dbt Changes

**Files:** None (infrastructure task)

- [ ] **Step 1: Full-refresh the three modified marts**

Schema changed (new columns) — `on_schema_change: fail` requires full refresh.

Run: `cd dbt_project && uv run dbt build --full-refresh -s int_running_score+ --exclude tag:embeddings tag:pausa`

This builds `int_running_score` (ephemeral) and all downstream models that depend on it, plus full-refreshes `fct_action_values`, `fct_shots`, `fct_passes`.

Expected: all models pass, all tests pass.

**Important:** Use `scripts/ensure_warehouse.py` wrapper if the warehouse may be stopped:
```bash
uv run python scripts/ensure_warehouse.py -- dbt build --full-refresh -s int_running_score+ --exclude tag:embeddings tag:pausa
```

- [ ] **Step 2: Verify game_state values**

Run a spot-check query via `databricks-sql-connector` or `dbt run-operation`:
```sql
SELECT game_state, count(*) FROM soccer_analytics.dev_gold.fct_action_values GROUP BY 1;
SELECT game_state, count(*) FROM soccer_analytics.dev_gold.fct_shots GROUP BY 1;
SELECT game_state, count(*) FROM soccer_analytics.dev_gold.fct_passes GROUP BY 1;
```
Expected: three rows per table (`winning`, `losing`, `drawing`), all non-zero.

- [ ] **Step 3: Verify possession columns**

```sql
SELECT
    data_source,
    count(*) as total,
    count(possession_id) as has_possession
FROM soccer_analytics.dev_gold.fct_action_values
GROUP BY 1;
```
Expected: `statsbomb` rows have `has_possession > 0`, `wyscout` rows have `has_possession = 0`.

- [ ] **Step 4: Recreate synced tables and indexes**

After schema change, synced tables need recreation:
1. Recreate synced tables via UI (or `scripts/refresh_synced_tables.py`)
2. Run `uv run python scripts/create_indexes.py` to restore custom indexes
3. Run `uv run python scripts/lakebase_grants.sql` for permissions

---

## Task 6: Add Game State Filter to Taipy Shared State

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Add state variables to `shared.py`**

After `selected_xg_model` / `xg_model_lov` (lines 48-49), add:

```python
selected_game_state: str | None = "All"
game_state_lov: list[str] = ["All", "Winning", "Losing", "Drawing"]
```

- [ ] **Step 2: Add callback to `shared.py`**

After `on_xg_model_change` (around line 376), add:

```python
def on_game_state_change(state: Any) -> None:
    """Re-render current page with the selected game state filter."""
    _refresh_current_page(state)
```

- [ ] **Step 3: Update `__all__` in `shared.py`**

Add to the `__all__` list (around lines 71-114):

```python
    "selected_game_state",
    "game_state_lov",
    "on_game_state_change",
```

- [ ] **Step 4: Add page group and widget to `template.py`**

Add the page group tuple after `_WF_PAGES` (line 305):

```python
_GAME_STATE_PAGES = ("Shot-Map", "Heat-Map", "Player-Impact", "Pass-Map",
                     "Pass-Timing", "Goalkeeper-Analytics", "Conversion-Funnel")
```

Add a `SidebarWidget` entry to `_FILTER_WIDGETS` list, after the `selected_xg_model` widget (after line 327):

```python
    SidebarWidget(
        "dropdown",
        "selected_game_state",
        "Game State",
        "on_game_state_change",
        condition=f"current_page in {_GAME_STATE_PAGES}",
        lov="game_state_lov",
        help="Filter by game state at the time of each action: winning, losing, or drawing. Based on the cumulative scoreline.",
    ),
```

- [ ] **Step 5: Add page group to `_FILTER_HEADER_PAGES`**

The Conversion Funnel page needs to appear in `_FILTER_HEADER_PAGES` (line 306) and the appropriate filter tuples. Add `"Conversion-Funnel"` to:

- `_COMP_PAGES` (line 287)
- `_TEAM_PAGES` (line 288)
- `_MATCH_PAGES` (line 289)
- `_FILTER_HEADER_PAGES` (line 306)

---

## Task 7: TDD — Funnel Query and Aggregation Logic

**Files:**
- Create: `src/tests/test_conversion_funnel.py`
- Create: `hf_taipy_app/src/queries/funnel.py`

- [ ] **Step 1: Write failing tests for funnel aggregation**

```python
# src/tests/test_conversion_funnel.py
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


class TestFunnelAggregation:
    """Verify funnel stage counts from raw action data."""

    @pytest.fixture
    def sample_actions(self) -> pd.DataFrame:
        return pd.DataFrame({
            "team_id": [1, 1, 1, 1, 1, 1, 2, 2, 2],
            "possession_id": [1, 1, 2, 2, 2, 3, 4, 4, 5],
            "possession_team_id": [1, 1, 1, 1, 1, 1, 2, 2, 2],
            "start_x": [60, 65, 50, 69.9, 72, 80, 60, 65, 50],
            "end_x": [65, 72, 69.9, 70.1, 80, 90, 72, 80, 60],
            "action_type": [
                "pass", "pass", "pass", "pass",
                "shot", "shot", "pass", "shot", "pass",
            ],
            "action_result": [
                "success", "success", "success", "success",
                "success", "fail", "success", "fail", "success",
            ],
        })

    def test_possession_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["possessions"] == 3  # possession_ids 1, 2, 3

    def test_a3_entry_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        # start_x <= 70 AND end_x > 70 AND success: rows at idx 1 (65->72) and 3 (69.9->70.1)
        assert result["a3_entries"] == 2

    def test_a3_entry_excludes_start_in_a3(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        # Row idx 4: start_x=72 (already in A3) -> not an entry
        # Row idx 5: start_x=80 (already in A3) -> not an entry
        assert result["a3_entries"] == 2

    def test_shot_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        # action_type == 'shot': idx 4, 5
        assert result["shots"] == 2

    def test_goal_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        # action_type == 'shot' AND action_result == 'success': idx 4
        assert result["goals"] == 1

    def test_away_team_independent(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=2)
        assert result["possessions"] == 2  # possession_ids 4, 5
        assert result["shots"] == 1
        assert result["goals"] == 0

    def test_a3_boundary_exact_70_is_entry(self) -> None:
        """start_x exactly 70 is at the boundary — counts as an entry."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame({
            "team_id": [1],
            "possession_id": [1],
            "possession_team_id": [1],
            "start_x": [70.0],
            "end_x": [75.0],
            "action_type": ["pass"],
            "action_result": ["success"],
        })
        result = compute_funnel_stages(df, team_id=1)
        assert result["a3_entries"] == 1  # start_x <= 70 AND end_x > 70

    def test_failed_actions_excluded_from_a3(self) -> None:
        """Only successful actions count as A3 entries."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame({
            "team_id": [1],
            "possession_id": [1],
            "possession_team_id": [1],
            "start_x": [65.0],
            "end_x": [75.0],
            "action_type": ["pass"],
            "action_result": ["fail"],
        })
        result = compute_funnel_stages(df, team_id=1)
        assert result["a3_entries"] == 0


class TestConversionRates:
    """Verify conversion rate computation."""

    def test_step_rates(self) -> None:
        from queries.funnel import compute_conversion_rates

        stages = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        rates = compute_conversion_rates(stages)
        assert rates["poss_to_a3"] == pytest.approx(25.0)
        assert rates["a3_to_shot"] == pytest.approx(20.0)
        assert rates["shot_to_goal"] == pytest.approx(20.0)
        assert rates["end_to_end"] == pytest.approx(1.0)

    def test_zero_division(self) -> None:
        from queries.funnel import compute_conversion_rates

        stages = {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}
        rates = compute_conversion_rates(stages)
        assert rates["poss_to_a3"] == 0.0
        assert rates["end_to_end"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /d/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_conversion_funnel.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'queries.funnel'`

- [ ] **Step 3: Implement `queries/funnel.py`**

```python
# hf_taipy_app/src/queries/funnel.py
"""Conversion rate funnel queries and aggregation logic."""
from __future__ import annotations

import pandas as pd

from queries.common import execute_query, t, ttl_cache

# ---------------------------------------------------------------------------
# A3 entry threshold: attacking third starts at x > 70 on SPADL 105m pitch
# ---------------------------------------------------------------------------
_A3_THRESHOLD = 70.0

# SPADL action types that count as shots
_SHOT_TYPES = ("shot", "shot_penalty", "shot_freekick")


def compute_funnel_stages(df: pd.DataFrame, team_id: int) -> dict[str, int]:
    """Compute funnel stage counts from a DataFrame of actions for one team.

    Args:
        df: DataFrame with columns: team_id, possession_id, possession_team_id,
            start_x, end_x, action_type, action_result.
        team_id: The team to compute the funnel for.

    Returns:
        Dict with keys: possessions, a3_entries, shots, goals.
    """
    team_df = df[df["team_id"] == team_id]

    possessions = int(
        team_df.loc[
            team_df["possession_team_id"] == team_id, "possession_id"
        ].nunique()
    )

    a3_mask = (
        (team_df["start_x"] <= _A3_THRESHOLD)
        & (team_df["end_x"] > _A3_THRESHOLD)
        & (team_df["action_result"] == "success")
    )
    a3_entries = int(a3_mask.sum())

    shot_mask = team_df["action_type"].isin(_SHOT_TYPES)
    shots = int(shot_mask.sum())

    goal_mask = shot_mask & (team_df["action_result"] == "success")
    goals = int(goal_mask.sum())

    return {
        "possessions": possessions,
        "a3_entries": a3_entries,
        "shots": shots,
        "goals": goals,
    }


def compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]:
    """Compute step-wise and end-to-end conversion rates.

    Returns:
        Dict with keys: poss_to_a3, a3_to_shot, shot_to_goal, end_to_end.
        All values are percentages (0-100). Zero when denominator is zero.
    """
    def _pct(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0

    return {
        "poss_to_a3": _pct(stages["a3_entries"], stages["possessions"]),
        "a3_to_shot": _pct(stages["shots"], stages["a3_entries"]),
        "shot_to_goal": _pct(stages["goals"], stages["shots"]),
        "end_to_end": _pct(stages["goals"], stages["possessions"]),
    }


@ttl_cache()
def fetch_funnel_actions(
    comp_id: int,
    team_id: int,
    match_id: int | None = None,
    game_state: str | None = None,
) -> pd.DataFrame:
    """Fetch action data for funnel computation from Lakebase.

    Returns DataFrame with columns needed by compute_funnel_stages.
    """
    tbl = t("fct_action_values_synced")
    ms_tbl = t("fct_match_summary_synced")

    where = ["av.competition_id = %s"]
    params: list[int | str] = [int(comp_id)]

    where.append(
        "(av.team_id = ms.home_team_id OR av.team_id = ms.away_team_id)"
    )

    if match_id is not None:
        where.append("av.match_id = %s")
        params.append(int(match_id))

    if game_state and game_state != "All":
        where.append("av.game_state = %s")
        params.append(game_state.lower())

    query = f"""
        SELECT
            av.match_id,
            av.team_id,
            av.possession_id,
            av.possession_team_id,
            av.start_x,
            av.end_x,
            av.action_type,
            av.action_result,
            ms.home_team_id,
            ms.away_team_id
        FROM {tbl} av
        INNER JOIN {ms_tbl} ms ON av.match_id = ms.match_id
        WHERE {' AND '.join(where)}
    """
    return execute_query(query, tuple(params))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /d/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_conversion_funnel.py -v`
Expected: all 9 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check hf_taipy_app/src/queries/funnel.py src/tests/test_conversion_funnel.py && uv run ruff format --check hf_taipy_app/src/queries/funnel.py src/tests/test_conversion_funnel.py`
Expected: no violations.

---

## Task 8: Funnel State Module and Chart Rendering

**Files:**
- Create: `hf_taipy_app/src/state/conversion_funnel.py`
- Modify: `src/tests/test_conversion_funnel.py` (add chart tests)

- [ ] **Step 1: Add chart rendering test**

Append to `src/tests/test_conversion_funnel.py`:

```python
plotly = pytest.importorskip("plotly")


class TestFunnelChart:
    """Verify mirror funnel chart rendering."""

    def test_chart_has_two_traces(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        assert len(fig.data) == 2

    def test_chart_home_positive_away_negative(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        # Home trace values should be positive
        assert all(v >= 0 for v in fig.data[0].x)
        # Away trace values should be negative (mirror)
        assert all(v <= 0 for v in fig.data[1].x)

    def test_chart_uses_canonical_colors(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        away = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        fig = _build_mirror_chart(home, away, "H", "A")
        assert fig.data[0].marker.color == "#e63946"   # HOME_COLOR
        assert fig.data[1].marker.color == "#457b9d"   # AWAY_COLOR
```

- [ ] **Step 2: Run tests to verify chart tests fail**

Run: `uv run pytest src/tests/test_conversion_funnel.py::TestFunnelChart -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'state.conversion_funnel'`

- [ ] **Step 3: Create state module**

```python
# hf_taipy_app/src/state/conversion_funnel.py
"""Conversion Rate Funnel — state module (prefix: cf_)."""
from __future__ import annotations

import logging
from typing import Any

import plotly.graph_objects as go

from queries.funnel import (
    compute_conversion_rates,
    compute_funnel_stages,
    fetch_funnel_actions,
)
from render import AWAY_COLOR, HOME_COLOR, PITCH_BG_COLOR, TEXT_COLOR
from state.shared import (
    get_comp_id,
    get_match_id,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State variables (cf_ prefix)
# ---------------------------------------------------------------------------
cf_possessions: str = ""
cf_possessions_detail: str = ""
cf_a3_entries: str = ""
cf_a3_detail: str = ""
cf_shots: str = ""
cf_shots_detail: str = ""
cf_goals: str = ""
cf_goals_detail: str = ""
cf_funnel_chart: go.Figure | None = None
cf_scope_label: str = ""
cf_data_freshness: str = ""
cf_warning_text: str = ""

__all__ = [
    "cf_possessions",
    "cf_possessions_detail",
    "cf_a3_entries",
    "cf_a3_detail",
    "cf_shots",
    "cf_shots_detail",
    "cf_goals",
    "cf_goals_detail",
    "cf_funnel_chart",
    "cf_scope_label",
    "cf_data_freshness",
    "cf_warning_text",
]

# ---------------------------------------------------------------------------
# Chart builder
# ---------------------------------------------------------------------------
_STAGE_LABELS = ["Possessions", "A3 Entries", "Shots", "Goals"]
_STAGE_KEYS = ["possessions", "a3_entries", "shots", "goals"]


def _build_mirror_chart(
    home: dict[str, int],
    away: dict[str, int],
    home_name: str,
    away_name: str,
) -> go.Figure:
    """Build horizontal mirror bar chart comparing home vs away funnel."""
    home_vals = [home[k] for k in _STAGE_KEYS]
    away_vals = [-away[k] for k in _STAGE_KEYS]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=_STAGE_LABELS,
            x=home_vals,
            orientation="h",
            name=home_name,
            marker_color=HOME_COLOR,
            text=[str(v) for v in home_vals],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=13),
        )
    )

    fig.add_trace(
        go.Bar(
            y=_STAGE_LABELS,
            x=away_vals,
            orientation="h",
            name=away_name,
            marker_color=AWAY_COLOR,
            text=[str(abs(v)) for v in away_vals],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=13),
        )
    )

    # Conversion rate annotations between stages
    home_rates = compute_conversion_rates(home)
    away_rates = compute_conversion_rates(away)
    rate_keys = ["poss_to_a3", "a3_to_shot", "shot_to_goal"]
    for i, rk in enumerate(rate_keys):
        fig.add_annotation(
            x=0,
            y=i + 0.5,
            text=f"{home_rates[rk]}% | {away_rates[rk]}%",
            showarrow=False,
            font=dict(size=11, color=TEXT_COLOR),
            xanchor="center",
        )

    fig.update_layout(
        barmode="overlay",
        plot_bgcolor=PITCH_BG_COLOR,
        paper_bgcolor=PITCH_BG_COLOR,
        font_color=TEXT_COLOR,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=100, r=40, t=40, b=40),
        xaxis=dict(
            title="",
            showticklabels=False,
            zeroline=True,
            zerolinecolor="#444",
            gridcolor="#333",
        ),
        yaxis=dict(title="", autorange="reversed"),
        height=350,
    )

    return fig


# ---------------------------------------------------------------------------
# Refresh callback
# ---------------------------------------------------------------------------
def _clear_state(state: Any) -> None:
    state.cf_possessions = ""
    state.cf_possessions_detail = ""
    state.cf_a3_entries = ""
    state.cf_a3_detail = ""
    state.cf_shots = ""
    state.cf_shots_detail = ""
    state.cf_goals = ""
    state.cf_goals_detail = ""
    state.cf_funnel_chart = None
    state.cf_scope_label = ""
    state.cf_warning_text = ""


def cf_refresh(state: Any) -> None:
    """Refresh conversion funnel data for the selected filters."""
    comp_id = get_comp_id(state.selected_competition)
    if not comp_id:
        _clear_state(state)
        return

    team_id = get_team_id(state.selected_team)
    match_id = get_match_id(state.selected_match)
    game_state = getattr(state, "selected_game_state", "All")

    gs_param = game_state if game_state and game_state != "All" else None
    df = fetch_funnel_actions(comp_id, team_id or 0, match_id, gs_param)

    if df.empty:
        _clear_state(state)
        state.cf_warning_text = (
            "No action data found for this filter combination. "
            "Try selecting a different competition or team."
        )
        return

    # Determine home/away team IDs from match data
    home_tid = int(df["home_team_id"].iloc[0])
    away_tid = int(df["away_team_id"].iloc[0])

    # If a specific team is selected, orient the funnel around that team
    if team_id:
        primary_tid = team_id
    else:
        primary_tid = home_tid

    home_stages = compute_funnel_stages(df, home_tid)
    away_stages = compute_funnel_stages(df, away_tid)
    home_rates = compute_conversion_rates(home_stages)
    away_rates = compute_conversion_rates(away_stages)

    # Populate stat cards (show selected team or home team)
    show_stages = home_stages if primary_tid == home_tid else away_stages
    show_rates = home_rates if primary_tid == home_tid else away_rates

    state.cf_possessions = f"{show_stages['possessions']:,}"
    state.cf_possessions_detail = "total team possessions"
    state.cf_a3_entries = f"{show_stages['a3_entries']:,}"
    state.cf_a3_detail = f"{show_rates['poss_to_a3']}% of possessions"
    state.cf_shots = f"{show_stages['shots']:,}"
    state.cf_shots_detail = f"{show_rates['a3_to_shot']}% of A3 entries"
    state.cf_goals = f"{show_stages['goals']:,}"
    state.cf_goals_detail = f"{show_rates['shot_to_goal']}% of shots"

    # Build mirror chart
    # Resolve team names from match summary data
    home_name = f"Home ({home_tid})"
    away_name = f"Away ({away_tid})"
    if "home_team_name" in df.columns:
        home_name = str(df["home_team_name"].iloc[0])
        away_name = str(df["away_team_name"].iloc[0])

    state.cf_funnel_chart = _build_mirror_chart(
        home_stages, away_stages, home_name, away_name
    )

    # Scope label
    scope_parts = [str(state.selected_competition)]
    if state.selected_team:
        scope_parts.append(str(state.selected_team))
    if state.selected_match:
        scope_parts.append(str(state.selected_match))
    if gs_param:
        scope_parts.append(f"Game State: {game_state}")
    state.cf_scope_label = " · ".join(scope_parts)
    state.cf_warning_text = ""

    logger.info(
        "Funnel refreshed: home=%s away=%s",
        home_stages,
        away_stages,
    )


register_page_refresher("Conversion-Funnel", cf_refresh, is_dashboard=True)
```

- [ ] **Step 4: Run all tests**

Run: `uv run pytest src/tests/test_conversion_funnel.py -v`
Expected: all 12 tests PASS (9 aggregation + 3 chart).

---

## Task 9: Funnel Page Config and Registration

**Files:**
- Create: `hf_taipy_app/src/pages/conversion_funnel.py`
- Modify: `hf_taipy_app/src/main.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Create page config**

```python
# hf_taipy_app/src/pages/conversion_funnel.py
"""Conversion Rate Funnel — page configuration."""
from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    StatCard,
    build_page,
)

page_config = PageConfig(
    title="Conversion Funnel",
    icon="filter_alt",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Possessions → Attacking Third Entries → Shots → Goals. "
        "Conversion rates at each stage reveal where a team's attack "
        "breaks down or excels. Mirror bars compare home vs away."
    ),
    citations=[
        Citation(
            "Donnelly (2024) — Systematic Approach to Performance Analysis",
        ),
    ],
    empty_message=(
        "Select a competition and team to see the conversion funnel. "
        "Optionally select a match for single-game analysis."
    ),
    empty_condition="len(cf_possessions) == 0",
    warning_var="cf_warning_text",
    scope_vars=["cf_scope_label"],
    stats=[
        StatCard(
            label="Possessions",
            var="cf_possessions",
            detail_var="cf_possessions_detail",
            help_text=(
                "Total team possessions — a continuous sequence of actions "
                "by one team. StatsBomb data only."
            ),
        ),
        StatCard(
            label="A3 Entries",
            var="cf_a3_entries",
            detail_var="cf_a3_detail",
            help_text=(
                "Successful actions crossing into the attacking third "
                "(final 35m of the pitch). Higher = better territorial "
                "penetration. Includes passes, dribbles, and carries."
            ),
        ),
        StatCard(
            label="Shots",
            var="cf_shots",
            detail_var="cf_shots_detail",
            help_text=(
                "Total shots attempted from attacking positions. "
                "Conversion from A3 entries measures chance creation rate."
            ),
        ),
        StatCard(
            label="Goals",
            var="cf_goals",
            detail_var="cf_goals_detail",
            help_text=(
                "Goals scored (excludes own goals). Conversion from shots "
                "measures finishing efficiency. 0–100%, higher = better."
            ),
        ),
    ],
    content=[
        ContentRow(
            blocks=[
                ContentBlock(
                    "chart",
                    "cf_funnel_chart",
                    header="Conversion Funnel — Home vs Away",
                    chart_height="350px",
                ),
            ],
        ),
    ],
)

page_md = build_page(page_config)
```

- [ ] **Step 2: Register in `main.py`**

Add imports (with the other page/state imports):

```python
from pages.conversion_funnel import page_config as funnel_config, page_md as funnel_page
from state.conversion_funnel import *  # noqa: F403
```

Add to `PAGE_REGISTRY` (after the `Defensive-Impact` entry):

```python
    PageEntry("Conversion-Funnel", funnel_config, funnel_page),
```

- [ ] **Step 3: Add glossary terms to `template.py`**

Add to `GLOSSARY` dict (alphabetically):

```python
    "A3 Entry": "A successful action (pass, dribble, or carry) that crosses from outside the attacking third into the final 35 meters of the pitch. Measures territorial penetration.",
    "Conversion Rate": "The percentage of events at one funnel stage that progress to the next stage. Higher = more efficient progression through the attack.",
    "Possession": "A continuous sequence of on-ball actions by one team, ending when the opposing team gains control. StatsBomb definition.",
```

Add to `PAGE_TERMS` dict:

```python
    "Conversion-Funnel": ["A3 Entry", "Conversion Rate", "Possession"],
```

- [ ] **Step 4: Verify the app starts locally**

Run: `cd hf_taipy_app && python src/main.py`
Expected: app starts on `localhost:7860`, Conversion Funnel appears in navigation.

---

## Task 10: Wire Game State Filter into Existing Page Queries

**Files:**
- Modify: Multiple state modules and query modules for pages in `_GAME_STATE_PAGES`

The pattern is identical for each page. Here is the pattern using Shot Map as the example, then the list of all files to modify.

- [ ] **Step 1: Pattern — add game state filter to query function**

For every query function that feeds a game-state-enabled page, add the conditional WHERE clause. Example for a shot map query:

```python
# Before:
def fetch_shots(comp_id: int, ...) -> pd.DataFrame:
    query = f"SELECT ... FROM {tbl} WHERE competition_id = %s ..."
    return execute_query(query, (comp_id, ...))

# After:
def fetch_shots(comp_id: int, ..., game_state: str | None = None) -> pd.DataFrame:
    where = ["competition_id = %s"]
    params: list[int | str] = [int(comp_id)]
    ...  # existing filters
    if game_state and game_state != "All":
        where.append("game_state = %s")
        params.append(game_state.lower())
    query = f"SELECT ... FROM {tbl} WHERE {' AND '.join(where)} ..."
    return execute_query(query, tuple(params))
```

- [ ] **Step 2: Pattern — pass game state from state module**

In each state module's refresh function, read the game state and pass it:

```python
def xx_refresh(state):
    ...
    game_state = getattr(state, "selected_game_state", "All")
    df = fetch_xxx(..., game_state=game_state)
    ...
```

- [ ] **Step 3: Apply pattern to all game-state-enabled pages**

Pages and their state/query files to modify:

| Page | State module | Query module/function |
|------|-------------|----------------------|
| Shot Map | `state/shot_map.py` | query function in state or `queries/` |
| Heat Map | `state/heat_map.py` | query function in state or `queries/` |
| Player Impact | `state/action_values.py` | query function in state or `queries/` |
| Pass Map | `state/pass_map.py` | query function in state or `queries/` |
| Pass Timing | `state/pass_timing.py` | query function in state or `queries/` |
| Goalkeeper | `state/goalkeeper.py` | query function in state or `queries/` |

For each file:
1. Find the query function(s) that query `fct_shots_synced`, `fct_passes_synced`, or `fct_action_values_synced`
2. Add `game_state` parameter with the conditional WHERE clause
3. In the refresh function, read `state.selected_game_state` and pass it

- [ ] **Step 4: Verify all pages still load**

Start the app locally and navigate to each modified page. Verify:
- Page loads with "All" game state (default)
- Selecting "Winning" / "Losing" / "Drawing" re-renders with filtered data
- Data changes when filter changes (may need a competition with goals)

---

## Task 11: Deploy and E2E Test with Puppeteer

**Files:** None (testing task)

- [ ] **Step 1: Deploy to staging**

Run: `uv run python scripts/manage_space.py deploy staging`
Expected: staging Space builds and runs successfully.

- [ ] **Step 2: E2E — Conversion Funnel page loads**

Using Puppeteer MCP:
1. Navigate to the staging URL
2. Click "Conversion Funnel" in navigation
3. Select a competition (e.g., "FIFA World Cup")
4. Select a team
5. Verify: four stat cards show non-zero values
6. Verify: mirror chart renders with two colored bar sets
7. Screenshot for verification

- [ ] **Step 3: E2E — Single match drill-down**

1. On Conversion Funnel page, select a specific match
2. Verify: stat cards update to single-match values
3. Verify: mirror chart updates

- [ ] **Step 4: E2E — Game state filter**

1. On Conversion Funnel page, select "Winning" from Game State dropdown
2. Verify: stat cards change (values should differ from "All")
3. Select "Drawing"
4. Verify: stat cards change again

- [ ] **Step 5: E2E — Game state dropdown visibility**

1. Navigate to Shot Map
2. Verify: "Game State" dropdown is visible in sidebar
3. Navigate to Player Similarity
4. Verify: "Game State" dropdown is NOT visible
5. Navigate to AI/ML Workflows
6. Verify: "Game State" dropdown is NOT visible

- [ ] **Step 6: E2E — Game state filter on existing page**

1. Navigate to Shot Map
2. Select competition, team, and a match with goals
3. Select "Winning" game state
4. Verify: shot map updates, shows fewer shots (only while winning)
5. Select "All" to reset

---

## Deployment Checklist

After all tasks complete:

- [ ] All dbt tests pass (`uv run dbt test`)
- [ ] All Python tests pass (`uv run pytest src/tests/test_conversion_funnel.py -v`)
- [ ] Ruff check passes (`uv run ruff check hf_taipy_app/src/ src/tests/test_conversion_funnel.py`)
- [ ] Pyright passes (`uv run pyright hf_taipy_app/src/state/conversion_funnel.py hf_taipy_app/src/pages/conversion_funnel.py hf_taipy_app/src/queries/funnel.py`)
- [ ] Synced tables recreated after dbt schema change
- [ ] Custom indexes re-applied (`scripts/create_indexes.py`)
- [ ] Staging deployment verified via Puppeteer E2E
- [ ] Production deployment pending user approval
