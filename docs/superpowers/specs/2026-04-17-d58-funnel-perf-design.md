# D58: Conversion Funnel — end-to-end performance + correctness fix

**Date:** 2026-04-17
**Branch:** `perf/conversion-funnel`
**TODO entry:** D58 — "Conversion Funnel — end-to-end performance optimization"
**Precedent:** [ADR-004 — Pre-aggregated `fct_*_agg` marts with dual-path Taipy queries](../adrs/ADR-004-pre-aggregated-marts-dual-path-queries.md)
**Prior feature spec:** [2026-04-09 — PA1 + PA4 game state segmentation & conversion funnel](2026-04-09-game-state-conversion-funnel-design.md)

---

## Summary

The Conversion Funnel page has been disabled in nav (`hf_taipy_app/src/main.py:106`) since 2026-04-10 because the season-mode query times out from the HF Space. Investigation revealed **two bugs, not one**: a perf problem, and a silent correctness problem (`LIMIT 500000` truncation hiding up to 57 % of a prolific team's actions, producing shots/goals counts less than half the real values). Re-enabling requires fixing both.

The fix follows ADR-004's established pattern: a new pre-aggregated mart (`fct_funnel_stages_agg`) at `(match_id, team_id, opponent_team_id, game_state)` grain with ~12,145 rows, a mart-only Taipy query rewrite, three PG composite indexes, a synced table (via UI-create + Terraform import), and page re-enable. All Phase 0 verifications have run live against Databricks + Lakebase; the amended mart design preserves parity with the current Python semantics on all non-truncated fixtures (zero delta) and closes the correctness gap on the truncated ones.

---

## Background

### Current state (disabled page)

- `PageEntry` commented out at `hf_taipy_app/src/main.py:106` with reason "query perf not validated from HF Space".
- State module still imported (`main.py:47`), so re-enable is a one-line uncomment.
- Sidebar widget for `cf_selected_game_state` already registered at `template.py:382-390`.
- Lakebase grants to the Taipy SP verified clean: `run_lakebase_grants.py --verify` returns "SELECT on all 37 synced tables" (2026-04-17 18:45 UTC).

### Two bugs discovered during investigation

**Bug 1 — silent data truncation in season mode.** `queries/funnel.py:fetch_funnel_actions` appends `LIMIT 500000` to the action query. For `(comp=11, team=217)` — the aggregate StatsBomb Barcelona collection, 526 matches — the true row count is **1,179,025**, meaning the current page silently drops 57 % of actions. For A3 entries, shots, and goals, the displayed values are **<50 %** of the real numbers. `_fetch_match_meta`'s `LIMIT 200` drops a similar fraction of match-name joins, leaving chart labels NaN for most of that team's matches.

**Bug 2 — Parallel Seq Scan on the 9.5 M-row raw fact table.** The season-mode query's `WHERE competition_id = %s AND match_id IN (...)` falls off the existing indexes (only composite `(competition_id, team_id, player_id)` and `(competition_id, team_id, game_state)` exist, and the query doesn't filter by `team_id`). Measured plans:

| query | rows returned | plan | execution |
|---|---|---|---|
| F-season-nogs | 500k (LIMIT hit) | Parallel Seq Scan on 9.5 M rows | **6,305 ms** |
| F-season-gs | 500k (LIMIT hit) | Nested Loop, 486 match bitmap scans | **37,800 ms** |

F-season-gs exceeds the app's pool-level `statement_timeout=30000` (`hf_taipy_app/src/db.py:182`). The large-team + game-state filter combination **cannot complete in production** under current code. This was empirically reproduced during Phase 0 (V10 oracle capture hit the timeout twice and succeeded on retry 3).

### Cache gap

`hf_taipy_app/src/queries/funnel.py` has **zero `@ttl_cache` decorators** — the only query module in the app without caching. Every filter change re-hits Lakebase. Fixed as part of the rewrite.

---

## Phase 0 verification results

All 14 verifications executed live on 2026-04-17. Raw outputs in `/tmp/d58_phase0.json`, `/tmp/d58_v08_parity.json`, `/tmp/d58_v10_season.json` (ephemeral; numbers below are the locked record).

| id | assertion | status |
|---|---|---|
| V01 | `possession_id` never spans >1 `game_state` within a match | **FAIL** — 168,298 straddler pairs; **design amended** (see § Mart schema) |
| V02 | `action_result` for shots is `{success, fail}` only | pass |
| V03 | Coordinate range 0–105 (normalized); A3 threshold 70 = 66.7 % of pitch | pass |
| V04 | `fct_action_values` rows all have a matching `fct_match_summary` row | pass-with-note — 2,465,557 orphan rows in 1,941 matches; current Python already drops them via meta-probe early return; mart INNER JOIN preserves behavior |
| V05 | NULL rates for `possession_id` / `possession_team_id` match docstring | pass (28.27 %, docstring's "~36 %" was stale) |
| V06 | `game_state` is non-null with 3 accepted values | pass |
| V07 | Proposed mart materializes with row count ≤ 20 k | pass — **12,145 total rows** across 3,463 matches, 21 competitions, 312 teams |
| V08 | Mart SQL reproduces current Python semantics exactly on 1 SB match + 1 Wyscout match | **pass** — 4 scenarios, zero delta on every stage for both teams |
| V09 | No row has `team_id = opponent_team_id`; no match with `home_team_id = away_team_id` | pass |
| V10 | Baseline numbers locked for 6 fixtures (4 expected zero-delta, 2 expected truncation-fix) | **pass** — 4 zero-delta, 2 confirm the 57 %-data-loss correctness bug |
| V13 | No caller of `fetch_funnel_actions` / `compute_funnel_stages` outside `hf_taipy_app/src/` | pass |
| V14 | `scripts/run_lakebase_grants.py` sources table list from `ingestion.refresh_synced_tables.SYNCED_TABLES` | pass |
| V15 | TF resource shape matches ADR-004 precedent | pass (`spec { primary_key_columns, scheduling_policy="SNAPSHOT" }` + `lifecycle { ignore_changes = all }`) |
| V16 | Current `SYNCED_TABLES` = 37, PG indexes = 63 | pass |
| V11 | Post-Lakebase-sync EXPLAIN on all 4 mart query shapes shows Index Scan and <100 ms | **deferred to Phase B end (gate)** |
| V12 | HF Space cold page-load measured before + after, after < 3 s | **deferred to Phase C end (gate)** |

### V10 locked parity fixtures (captured from current live Lakebase oracle, 2026-04-17 18:56 UTC)

These tuples are the oracle the mart must reproduce. `(possessions, a3_entries, shots, goals)`.

| fixture | primary (selected team) | opponent(s) | verdict |
|---|---|---|---|
| `comp=11 team=213 match=None gs=None` | `(6295, 2721, 742, 75)` | `(6510, 3341, 974, 115)` | mart **must match exactly** |
| `comp=11 team=213 match=None gs=winning` | `(1198, 589, 187, 37)` | `(2118, 1081, 320, 73)` | mart **must match exactly** |
| `comp=11 team=217 match=3888713 gs=None` | `(101, 7, 21, 5)` | `(109, 69, 7, 0)` | mart **must match exactly** |
| `comp=11 team=217 match=3888713 gs=drawing` | `(32, 4, 4, 0)` | `(33, 23, 3, 0)` | mart **must match exactly** |
| `comp=11 team=217 match=None gs=None` | `(47201, 13812, 3523, 570)` | `(38526, 8186, 1933, 178)` | mart **must be ≥ oracle per stage** — delta is the 57 % correctness fix |
| `comp=11 team=217 match=None gs=drawing` | `(23244, 13777, 3240, 251)` | `(19766, 8302, 1762, 102)` | mart **must be ≥ oracle per stage** — delta is the correctness fix |

### Correctness fix, quantified (large-team case)

`comp=11 team=217 season gs=All`, selected team:

| stage | old (broken) | mart | hidden by `LIMIT 500000` |
|---|---|---|---|
| possessions | 47,201 | **53,134** | +5,933 (+12.6 %) |
| a3 entries | 13,812 | **32,367** | +18,555 (**+134 %**) |
| shots | 3,523 | **8,247** | +4,724 (**+134 %**) |
| goals | 570 | **1,336** | +766 (**+134 %**) |

For A3 entries, shots, and goals the old page displays **less than half** the real values. Closing this is the correctness half of D58.

---

## Architecture

### Mart — `dbt_project/models/marts/fct_funnel_stages_agg.sql`

**Materialization:** `table`, `liquid_clustered_by=['competition_id']` (matches ADR-004 marts).

**Grain:** `(match_id, team_id, game_state)` — `opponent_team_id` is derivable. `dbt_utils.unique_combination_of_columns` asserts this.

**Columns (contract-enforced):**

| column | type | definition |
|---|---|---|
| `match_id` | `bigint` | FK to `fct_match_summary` (INNER JOIN at build time) |
| `competition_id` | `int` | denormalized for season-mode `WHERE` |
| `team_id` | `int` | acting team |
| `opponent_team_id` | `int` | the other team in the match (from `fct_match_summary` home/away) |
| `game_state` | `string` | `winning` \| `losing` \| `drawing` |
| `pos_in_gs` | `bigint` | `COUNT(DISTINCT possession_id)` among rows of this `(match, team, gs)` — straddlers counted once per gs they touch |
| `pos_in_match` | `bigint` | `COUNT(DISTINCT possession_id)` across the full `(match, team)` — replicated on every gs row for that match+team |
| `a3_entries` | `bigint` | `SUM(CASE WHEN start_x <= 70 AND end_x > 70 THEN 1 ELSE 0 END)` — matches the Python code at `queries/funnel.py:42` |
| `shots` | `bigint` | `SUM(CASE WHEN action_type IN ('shot','shot_penalty','shot_freekick') THEN 1 ELSE 0 END)` |
| `goals` | `bigint` | `SUM(CASE … AND action_result='success' THEN 1 ELSE 0 END)` |
| `wy_match_flag` | `smallint` | `1` if team had any NULL-`possession_id` row in this match — replicated on every gs row for that match+team |
| `_loaded_at` | `timestamp` | dbt convention |

**Why `pos_in_gs` AND `pos_in_match`:** V01 found 168,298 straddler possessions. At gs-filter query time the app wants "possessions that touched this gs" = `SUM(pos_in_gs)`. At gs=All the app wants "distinct possessions in the match" = `MAX(pos_in_match)` per `(match, team)` then `SUM`. Replicating `pos_in_match` across gs rows makes the driver-side dedup a single `.groupby(..).first().sum()`.

**Why `wy_match_flag` is match-level (not per-gs):** Current Python treats Wyscout (NULL-possession) matches as 1 synthetic possession per match at gs=All, 1 per `(match, gs)` at gs-filter. Replicating a match-level flag on every gs row and using `COUNT(DISTINCT CASE WHEN wy_match_flag=1 THEN match_id END)` at the driver reproduces both semantics from one column.

**SQL:**

```sql
{{ config(materialized='table', liquid_clustered_by=['competition_id']) }}

with base as (
  select av.match_id, av.competition_id, av.team_id, av.game_state,
         av.possession_id, av.possession_team_id,
         av.start_x, av.end_x, av.action_type, av.action_result,
         ms.home_team_id, ms.away_team_id
  from {{ ref('fct_action_values') }} av
  join {{ ref('fct_match_summary') }} ms using (match_id)
  where av.team_id is not null
    and av.game_state is not null
),
own_possession as (
  select *,
         case when team_id = home_team_id then away_team_id else home_team_id end as opponent_team_id
  from base
  where possession_team_id is null or possession_team_id = team_id
),
per_gs as (
  select match_id, competition_id, team_id, opponent_team_id, game_state,
         count(distinct case when possession_id is not null then possession_id end) as pos_in_gs,
         sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)               as a3_entries,
         sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
         sum(case when action_type in ('shot','shot_penalty','shot_freekick')
                   and action_result = 'success' then 1 else 0 end)                  as goals
  from own_possession
  group by match_id, competition_id, team_id, opponent_team_id, game_state
),
per_match as (
  select match_id, team_id,
         count(distinct case when possession_id is not null then possession_id end) as pos_in_match,
         max(case when possession_id is null then 1 else 0 end)                     as wy_match_flag
  from own_possession
  group by match_id, team_id
)
select
  cast(g.match_id as bigint)             as match_id,
  cast(g.competition_id as int)          as competition_id,
  cast(g.team_id as int)                 as team_id,
  cast(g.opponent_team_id as int)        as opponent_team_id,
  cast(g.game_state as string)           as game_state,
  cast(g.pos_in_gs as bigint)            as pos_in_gs,
  cast(m.pos_in_match as bigint)         as pos_in_match,
  cast(g.a3_entries as bigint)           as a3_entries,
  cast(g.shots as bigint)                as shots,
  cast(g.goals as bigint)                as goals,
  cast(m.wy_match_flag as smallint)      as wy_match_flag,
  current_timestamp()                    as _loaded_at
from per_gs g
join per_match m using (match_id, team_id)
```

### PG indexes — `scripts/create_indexes.py`

Three new composite B-tree indexes; count goes **63 → 66**:

| name | columns | purpose |
|---|---|---|
| `idx_funnel_agg_match` | `(match_id)` | single-match query |
| `idx_funnel_agg_comp_team_gs` | `(competition_id, team_id, game_state)` | season mode — selected-team side |
| `idx_funnel_agg_comp_opp_gs` | `(competition_id, opponent_team_id, game_state)` | season mode — opponent side (BitmapOr with above) |

Verify block appended to `scripts/create_indexes.py --verify` asserting Index Scan (not Seq Scan) on all three access paths — mirrors the existing assertion pattern at `create_indexes.py:240-246`.

### Synced table — `terraform/modules/synced_tables/main.tf`

UI-create-then-TF-import (Path A from ADR-004). Resource block mirrors `fct_heatmap_agg` at `main.tf:603-618`:

```hcl
resource "databricks_database_synced_database_table" "fct_funnel_stages_agg" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg"
    primary_key_columns    = ["match_id", "team_id", "game_state"]
    scheduling_policy      = "SNAPSHOT"
    timeseries_key         = null
  }

  lifecycle {
    ignore_changes = all
  }
}
```

### Taipy query — `hf_taipy_app/src/queries/funnel.py` (full rewrite)

```python
from __future__ import annotations

from typing import Any
import pandas as pd

from queries.common import execute_query, t, ttl_cache

_STAGE_KEYS = ("possessions", "a3_entries", "shots", "goals")


@ttl_cache()
def fetch_funnel_agg(
    comp_id: int,
    team_id: int,
    match_id: int | None = None,
    game_state: str | None = None,
) -> pd.DataFrame:
    """Pre-aggregated mart read — single path for single-match and season modes."""
    tbl = t("fct_funnel_stages_agg_synced")
    cols = ("match_id, competition_id, team_id, opponent_team_id, game_state,"
            " pos_in_gs, pos_in_match, a3_entries, shots, goals, wy_match_flag")
    if match_id is not None:
        where: list[str] = ["match_id = %s"]
        params: list[Any] = [int(match_id)]
    else:
        where = ["competition_id = %s", "(team_id = %s OR opponent_team_id = %s)"]
        params = [int(comp_id), int(team_id), int(team_id)]
    if game_state and game_state != "All":
        where.append("game_state = %s")
        params.append(game_state.lower())
    return execute_query(
        f"SELECT {cols} FROM {tbl} WHERE {' AND '.join(where)}",
        tuple(params),
    )


@ttl_cache()
def _fetch_match_meta(comp_id: int, team_id: int, match_id: int) -> pd.DataFrame:
    """Single-match home/away team name lookup — only called in single-match mode."""
    ms_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT match_id, home_team_id, away_team_id, home_team_name, away_team_name"  # noqa: S608
        f" FROM {ms_tbl}"
        f" WHERE competition_id = %s AND match_id = %s"
        f" LIMIT 1",
        (int(comp_id), int(match_id)),
    )


def rollup_stages(rows: pd.DataFrame, *, gs_filtered: bool) -> dict[str, int]:
    """Collapse mart rows into funnel totals, honoring V01 straddler semantics."""
    if rows.empty:
        return {k: 0 for k in _STAGE_KEYS}
    if gs_filtered:
        sb = int(rows["pos_in_gs"].sum())
    else:
        sb = int(rows.groupby(["match_id", "team_id"])["pos_in_match"].first().sum())
    wy = int(rows.loc[rows["wy_match_flag"] == 1, "match_id"].nunique())
    return {
        "possessions": sb + wy,
        "a3_entries":  int(rows["a3_entries"].sum()),
        "shots":       int(rows["shots"].sum()),
        "goals":       int(rows["goals"].sum()),
    }


def compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]:
    """Unchanged from previous implementation."""
    def _pct(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0
    return {
        "poss_to_a3":    _pct(stages["a3_entries"], stages["possessions"]),
        "a3_to_shot":    _pct(stages["shots"], stages["a3_entries"]),
        "shot_to_goal":  _pct(stages["goals"], stages["shots"]),
        "end_to_end":    _pct(stages["goals"], stages["possessions"]),
    }
```

**Deleted:** `fetch_funnel_actions`, `compute_funnel_stages`, old `_fetch_match_meta` with `LIMIT 200`.

### Taipy state — `hf_taipy_app/src/state/conversion_funnel.py`

`cf_refresh` rewritten to consume mart rows:

```python
def cf_refresh(state):
    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(state.selected_team)
    if not comp_id or not team_id:
        _clear_state(state); return

    match_id = get_match_id(state.selected_match)
    game_state = getattr(state, "cf_selected_game_state", "All")
    gs_param = game_state if game_state and game_state != "All" else None

    df = fetch_funnel_agg(comp_id, team_id, match_id, gs_param)
    if df.empty:
        _clear_state(state)
        state.cf_warning_text = "No action data found for this filter combination. "\
                                "Try selecting a different competition or team."
        return

    gs_filtered = gs_param is not None
    team_rows = df[df["team_id"] == team_id]
    opp_rows  = df[df["team_id"] != team_id]
    primary_stages = rollup_stages(team_rows, gs_filtered=gs_filtered)
    opp_stages     = rollup_stages(opp_rows,  gs_filtered=gs_filtered)

    if match_id is not None:
        meta = _fetch_match_meta(comp_id, team_id, match_id)
        home_name = str(meta["home_team_name"].iloc[0])
        away_name = str(meta["away_team_name"].iloc[0])
        home_tid  = int(meta["home_team_id"].iloc[0])
        home_stages, away_stages = (
            (primary_stages, opp_stages) if team_id == home_tid else (opp_stages, primary_stages)
        )
        state.cf_funnel_chart = _build_mirror_chart(home_stages, away_stages, home_name, away_name)
    else:
        state.cf_funnel_chart = _build_mirror_chart(
            primary_stages, opp_stages, str(state.selected_team), "Opponents"
        )

    # Stat cards + scope label unchanged
    show_stages = primary_stages
    show_rates  = compute_conversion_rates(show_stages)
    state.cf_possessions        = f"{show_stages['possessions']:,}"
    state.cf_possessions_detail = "total team possessions"
    state.cf_a3_entries         = f"{show_stages['a3_entries']:,}"
    state.cf_a3_detail          = f"{show_rates['poss_to_a3']}% of possessions"
    state.cf_shots              = f"{show_stages['shots']:,}"
    state.cf_shots_detail       = f"{show_rates['a3_to_shot']}% of A3 entries"
    state.cf_goals              = f"{show_stages['goals']:,}"
    state.cf_goals_detail       = f"{show_rates['shot_to_goal']}% of shots"

    scope_parts = [str(state.selected_competition), str(state.selected_team)]
    if state.selected_match: scope_parts.append(str(state.selected_match))
    if gs_param:             scope_parts.append(f"Game State: {game_state}")
    state.cf_scope_label = " · ".join(scope_parts)
    state.cf_warning_text = ""
```

`_build_mirror_chart` — **unchanged** (pure Plotly).

### Page re-enable — `hf_taipy_app/src/main.py`

Uncomment line 106 — `PageEntry("Conversion-Funnel", funnel_config, funnel_page)`.

---

## Testing strategy

### dbt tests (`dbt_project/models/marts/_marts__models.yml`)

New contract block for `fct_funnel_stages_agg` with `enforced: true`:

| column | tests |
|---|---|
| `match_id` | `not_null`, `relationships` → `fct_match_summary.match_id` |
| `competition_id`, `team_id`, `opponent_team_id` | `not_null` |
| `game_state` | `not_null`, `accepted_values: [winning, losing, drawing]` |
| `pos_in_gs`, `pos_in_match`, `a3_entries`, `shots`, `goals` | `not_null` |
| `wy_match_flag` | `not_null`, `accepted_values: [0, 1]` |

Model-level test: `dbt_utils.unique_combination_of_columns: [match_id, team_id, game_state]`.

### Python unit tests (`src/tests/test_conversion_funnel.py`)

**Delete:** class `TestFunnelAggregation` (10 tests — they tested `compute_funnel_stages` which is removed; semantics migrate to SQL + V10 integration fixtures).

**Keep:** `TestConversionRates` (2 tests), `TestFunnelChart` (3 tests).

**Add — `TestRollupStages`:**

| test | assertion |
|---|---|
| `test_empty_rows` | empty df → all four stages = 0 |
| `test_sb_only_gs_filtered` | `wy_match_flag=0` rows, `gs_filtered=True` → `possessions = sum(pos_in_gs)` |
| `test_sb_only_gs_all` | `wy_match_flag=0` rows, `gs_filtered=False` → `possessions = groupby((match,team)).first().sum()` (uses `pos_in_match`) |
| `test_straddler_gs_all_deduped` | **V01 regression guard** — same `match_id` appears on 3 gs rows each with `pos_in_gs=5` but `pos_in_match=12`. `gs_filtered=False` → `possessions=12` (not 15). |
| `test_straddler_gs_filtered_not_deduped` | Same rows, `gs_filtered=True` → `possessions = sum(pos_in_gs) = 15` |
| `test_wy_match_deduped_across_gs` | `wy_match_flag=1` replicated across 3 gs rows for same match → counted as 1 |
| `test_wy_mixed_sb` | 2 SB matches + 1 Wyscout match → correct combined count |
| `test_stage_sums_independent_of_wyscout` | `a3_entries`/`shots`/`goals` sum correctly regardless of `wy_match_flag` |

**Add — `TestFetchFunnelAggSQL`** (mocks `execute_query`, captures SQL + params):

| test | assertion |
|---|---|
| `test_single_match_no_gs` | `WHERE match_id = %s`, params `(match_id,)`, no `LIMIT` |
| `test_single_match_with_gs` | `WHERE match_id = %s AND game_state = %s`, params `(match_id, 'drawing')` (lowercased) |
| `test_season_no_gs` | `WHERE competition_id = %s AND (team_id = %s OR opponent_team_id = %s)`, 3-param tuple |
| `test_season_with_gs` | season clause + `AND game_state = %s`, 4-param tuple |
| `test_no_limit_clause` | none of the four emitted queries contain `LIMIT` (V10 correctness guard) |

**Add — `TestFetchMatchMetaSingle`:** LIMIT is 1, not 200. Assert via captured SQL.

### Parity integration test — `src/tests/integration/test_funnel_mart_parity.py`

Gated by env var `LAKEBASE_HOST` (skipped in default CI). Uses the 6 V10 fixtures above:

- 4 zero-delta fixtures: assert `mart_primary == oracle_primary` and `mart_opponent == oracle_opponent` on every stage.
- 2 truncation-fix fixtures: assert `mart_stage >= oracle_stage` on every stage; record the delta in the commit message.

Oracle numbers are hardcoded (not re-fetched) so the test is deterministic.

### Post-build gate — V11 (EXPLAIN on Lakebase)

After Phase B completes, run `scripts/_d58_v11_explain.py` (temp) against live Lakebase on all 4 mart query shapes. **Gate:** all 4 plans show Index Scan (not Seq Scan); execution ≤100 ms on the worst case (comp 11 team 217). If fail → no commit, re-open index design.

### Post-deploy gate — V12 (HF Space cold load)

After Phase C staging deploy, measure cold page-load via `performance.now()` + DevTools Network tab. **Gate:** after-number ≤3 s for `comp=11 team=217 season gs=None`. If fail → no commit, investigate pool / OAuth / network before retry.

### E2E Puppeteer (Chrome, staging)

| scenario | assertion |
|---|---|
| Navigate to Funnel (re-enabled) | stat cards non-empty within 3 s |
| Switch competition → team (season, no gs) | cards + chart re-render, `performance.now()` Δ <1500 ms |
| Select match (single-match mode) | home/away names in chart labels, Δ <800 ms |
| Select game state = Winning | cards update, Δ <800 ms |
| Clear match back to "All" | cards return to season values, no stale chart |
| Full-page screenshot | saved to `/tmp/d58_funnel_{scope}.png` for visual regression baseline |

---

## Sequencing

### Phase 0 — DONE

14 live verifications complete. Mart schema amended (V01 straddlers). Parity fixtures locked (V10). Two bugs quantified. Remaining V11/V12 require infrastructure and execute in Phase B/C.

### Phase A — code edits, no infrastructure

All in the working tree. Gate: ruff + ruff format + pyright + `pytest src/tests/test_conversion_funnel.py src/tests/test_refresh_synced_tables.py -v` all green.

| # | file | change |
|---|---|---|
| 1 | `dbt_project/models/marts/fct_funnel_stages_agg.sql` | new — mart SQL above |
| 2 | `dbt_project/models/marts/_marts__models.yml` | new contract block |
| 3 | `workflow-cards/wf-dbt-build.yaml` | add `outputs.tables` entry with `dbt_model: fct_funnel_stages_agg` |
| 4 | `scripts/create_indexes.py` | append 3 indexes + 3 verify assertions |
| 5 | `src/ingestion/refresh_synced_tables.py` | `SYNCED_TABLES` append `fct_funnel_stages_agg_synced` (37 → 38) |
| 6 | `src/tests/test_refresh_synced_tables.py` | expected count 37 → 38 |
| 7 | `src/tests/test_conversion_funnel.py` | delete `TestFunnelAggregation`, add `TestRollupStages` + `TestFetchFunnelAggSQL` + `TestFetchMatchMetaSingle` |
| 8 | `src/tests/integration/test_funnel_mart_parity.py` | new — 6 V10 fixtures as hardcoded oracles |
| 9 | `hf_taipy_app/src/queries/funnel.py` | full rewrite (see above) |
| 10 | `hf_taipy_app/src/state/conversion_funnel.py` | `cf_refresh` replaced |
| 11 | `hf_taipy_app/src/main.py` | uncomment line 106 |

### Phase B — infrastructure, sequential

| # | action | owner | gate |
|---|---|---|---|
| B1 | `scripts/ensure_warehouse.py -- dbt build --select fct_funnel_stages_agg+` | me | mart populates |
| B2 | `dbt test --select fct_funnel_stages_agg` | me | all contract + `accepted_values` + `relationships` + `unique_combination_of_columns` tests pass |
| B3 | **Create `dev_gold.fct_funnel_stages_agg_synced` via Databricks UI** | **you** | table shows "Active" in Lakebase UI |
| B4 | `terraform import databricks_database_synced_database_table.fct_funnel_stages_agg <pipeline-id>` + add resource block to TF module | me | `terraform plan` shows 0 drift |
| B5 | `scripts/grant_synced_table_permissions.py --grant` | me | workspace `CAN_USE` + `CAN_RUN` granted to ingestion SP + Taipy SP |
| B6 | `scripts/run_lakebase_grants.py` → `--verify` | me | prints `OK: SP … has SELECT on all 38 synced tables` |
| B7 | `scripts/create_indexes.py` → `--verify` | me | all 3 new paths show Index Scan |
| V11 | Temp `scripts/_d58_v11_explain.py` — EXPLAIN all 4 mart query shapes against live Lakebase | me | Index Scan + ≤100 ms on worst case |

### Phase C — validation

| # | action | gate |
|---|---|---|
| C1 | `LAKEBASE_HOST=… pytest src/tests/integration/test_funnel_mart_parity.py -v` | 6/6 pass |
| C2 | Capture fresh before/after EXPLAIN ANALYZE numbers for commit message | numbers recorded |
| C3 | `scripts/manage_space.py deploy staging --no-wait` then `--rebuild` | staging RUNNING |
| V12 | Cold page-load on staging (Chrome Puppeteer + `performance.now()` + DevTools screenshots) | after ≤3 s |
| C4 | E2E Puppeteer scenarios | all 5 pass, screenshots captured |

### Phase D — docs + cleanup, no side effects until explicit commit approval

| # | file | change |
|---|---|---|
| D1 | `README.md` | 37→38 synced, 63→66 indexes, 36→37 gold marts |
| D2 | `ARCHITECTURE.md` | same three counts |
| D3 | `docs/huggingface/org-card.md` | 37→38 synced |
| D4 | `docs/c4/architecture.dsl` + regenerate `.html` | only if counts referenced |
| D5 | `TODO.md` | remove D58 entry |
| D6 | delete `scripts/_d58_explain.py`, `_d58_extra.py`, `_d58_phase0.py`, `_d58_v08_parity.py`, `_d58_v10_baseline.py`, `_d58_v10_season.py` | tmp files |

### Approval checkpoints (per `feedback_no_commits_without_approval.md` + `feedback_one_commit_at_a_time.md`)

1. After Phase A (code diff review, before Phase B starts).
2. Before B3 (UI handoff prompt).
3. After Phase C green (evidence package + commit approval request).
4. Before `git push` (separate approval).
5. Before `gh pr create` (separate approval).
6. Before `gh pr merge` (separate approval).

### Commit message structure (single commit)

```
perf: D58 — fct_funnel_stages_agg mart, mart-only query, re-enable Conversion Funnel (ADR-004)

Close D58: Conversion Funnel was disabled since 2026-04-10 due to timeouts
from the HF Space. Investigation revealed two bugs — perf + silent data
truncation.

## Perf fix (mart + indexes)
<before/after EXPLAIN timings table, V11 measurements>

## Correctness fix (LIMIT 500000 removed)
<truncation-delta table for comp 11 / team 217>

## HF Space cold page-load
<V12 before/after numbers>

## Infrastructure
- 1 new dbt mart: fct_funnel_stages_agg (grain: match_id, team_id, game_state;
  ~12,145 rows; liquid_clustered_by competition_id)
- 3 new composite PG indexes (63 → 66)
- 1 new synced table (37 → 38)
- Contract enforced; dbt_utils.unique_combination_of_columns asserts grain

## Tests
- V08 single-match parity: 4/4 zero delta
- V10 integration parity: 6/6 pass (4 zero-delta, 2 correctness-fix)
- TestRollupStages: 8 tests (incl. V01 straddler regression guard)
- TestFetchFunnelAggSQL: 5 tests (incl. no-LIMIT guard)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Out of scope

- **Cross-cutting `_refresh_current_page` refactor** — every `on_*_change` callback in `state/shared.py` refreshes the current page regardless of whether the changed filter affects it. Useful but separate; new TODO to be filed.
- **Rolling out the shared game-state filter to the other 8 PA1 pages** — the 2026-04-09 spec's § "Shared State (PA1)" planned a shared `selected_game_state` dropdown on Shot Map / Heat Map / Action Values / Pass Map / Pass Timing / Match Summary / Defensive Impact / Goalkeeper in addition to Funnel. DB columns exist; UI rollout is a separate cycle.
- **HF Space → Lakebase network latency investigation** — V12 will surface whether the remaining load time is dominated by query (mart fixes) or network. If the latter, a follow-up cycle covers pool / OAuth / warehouse-wakeup tuning.
- **ADR** — ADR-004 already codifies this pattern. No new ADR required; mart SQL header comment + query module docstring document the funnel-specific nuances.

---

## References

- **Investigation evidence:** `/tmp/d58_explain.txt` (6 EXPLAIN plans), `/tmp/d58_extra.txt` (row counts), `/tmp/d58_phase0.json` (9 verification probes), `/tmp/d58_v08_parity.json` (single-match parity), `/tmp/d58_v10_season.json` (6-fixture season parity).
- **Precedent:** ADR-004, commit `0a736b0` (pre-aggregated marts landing pattern).
- **Original feature spec:** 2026-04-09 PA1+PA4 (game state + conversion funnel).
- **Indexes reference:** `scripts/create_indexes.py:69-94` (existing action/match indexes).
- **TF module reference:** `terraform/modules/synced_tables/main.tf:603-645` (three ADR-004 agg marts).
- **Grant script reference:** `scripts/run_lakebase_grants.py:126` (imports `SYNCED_TABLES` from `ingestion.refresh_synced_tables`).
