# D58 — Conversion Funnel perf + correctness fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Also respect `superpowers:test-driven-development` for tests-first flow and `superpowers:verification-before-completion` at the final gate.

**Goal:** Close D58 — restore the disabled Conversion Funnel page with correct counts (removing a silent `LIMIT 500000` that hid 57 % of prolific-team actions) and sub-3 s cold page-load from HF Space.

**Architecture:** Adds a new pre-aggregated mart `fct_funnel_stages_agg` at `(match_id, team_id, game_state)` grain (~12,145 rows, `liquid_clustered_by competition_id`), following the ADR-004 pattern established by `fct_heatmap_agg` / `fct_vaep_breakdown_agg` / `fct_gk_actions_detail`. Taipy replaces the raw-event scan of `fct_action_values` (9.5 M rows → Parallel Seq Scan, 6.3 s no-gs / 37.8 s with gs) with a mart-only read (~12k rows → composite-index lookup, <100 ms). The V01 Phase 0 verification found 168,298 `(match_id, possession_id)` straddlers crossing `game_state` boundaries; the mart stores two possession counts (`pos_in_gs`, `pos_in_match`) plus a match-level `wy_match_flag` so both game-state-filtered and "All" rollups reproduce Python semantics exactly.

**Tech Stack:** dbt 1.9 (Databricks adapter, contract-enforced), PySpark (materializer), PostgreSQL 16 (Lakebase synced tables), psycopg2 (index creation), Terraform (synced-table resource), Taipy 4.1 (state + query modules), pytest + pytest-mock (unit + parity tests), Puppeteer (E2E).

**Spec:** [`docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md`](../specs/2026-04-17-d58-funnel-perf-design.md).
**Precedent:** [`docs/superpowers/adrs/ADR-004-pre-aggregated-marts-dual-path-queries.md`](../adrs/ADR-004-pre-aggregated-marts-dual-path-queries.md).

---

## Commit policy (overrides the writing-plans skill default)

`CLAUDE.md` + `feedback_no_commits_without_approval.md` + `feedback_one_commit_at_a_time.md` forbid per-task commits. The spec mandates a **single commit** at the end of Phase D. **Do not run `git add` or `git commit` at any point during Phases A–C.** The plan has six explicit approval checkpoints — wait at each.

| # | Checkpoint | Wait for |
|---|---|---|
| 1 | End of Phase A (code-only, local gate green) | User approves Phase A diff before Phase B starts |
| 2 | Before Task B3 (UI handoff — user creates synced table in Databricks UI) | User confirms the synced table shows "Active" in Lakebase |
| 3 | End of Phase C (parity test green, V11 + V12 gates passed, E2E green) | User approves commit |
| 4 | After commit lands locally | User approves `git push` |
| 5 | After push succeeds | User approves `gh pr create` |
| 6 | After CI green on PR | User approves `gh pr merge` |

---

## File structure

### New files
- `dbt_project/models/marts/fct_funnel_stages_agg.sql` — mart SQL
- `src/tests/integration/test_funnel_mart_parity.py` — 6-fixture V10 parity test (env-gated)
- `scripts/_d58_v11_explain.py` — temp V11 EXPLAIN gate script (deleted at Phase D)

### Modified files
- `dbt_project/models/marts/_marts__models.yml` — new contract block for `fct_funnel_stages_agg`
- `workflow-cards/wf-dbt-build.yaml` — new `outputs.tables` entry (alphabetical)
- `scripts/create_indexes.py` — 3 new composite indexes + 3 verify queries
- `src/ingestion/refresh_synced_tables.py` — `SYNCED_TABLES` append (37 → 38)
- `src/tests/test_refresh_synced_tables.py` — expected count 37 → 38
- `src/tests/test_conversion_funnel.py` — delete `TestFunnelAggregation`; add `TestRollupStages` + `TestFetchFunnelAggSQL` + `TestFetchMatchMetaSingle`
- `terraform/modules/synced_tables/main.tf` — new `databricks_database_synced_database_table.fct_funnel_stages_agg` resource
- `hf_taipy_app/src/queries/funnel.py` — full rewrite
- `hf_taipy_app/src/state/conversion_funnel.py` — `cf_refresh` rewrite (other exports unchanged)
- `hf_taipy_app/src/main.py` — add two imports + uncomment line 106
- Phase D docs: `README.md`, `ARCHITECTURE.md`, `docs/huggingface/org-card.md`, `TODO.md`, optionally `docs/c4/architecture.dsl`

### Deleted files (Phase D)
- `scripts/_d58_explain.py`
- `scripts/_d58_extra.py`
- `scripts/_d58_phase0.py`
- `scripts/_d58_v08_parity.py`
- `scripts/_d58_v10_baseline.py`
- `scripts/_d58_v10_season.py`
- `scripts/_d58_v11_explain.py` (after B7/V11 gate passes)

---

# Phase A — Code only, no infrastructure side effects

Gate: `uv run ruff check src/ scripts/`, `uv run ruff format --check src/ scripts/`, `uv run pyright src/`, `uv run pytest src/tests/test_conversion_funnel.py src/tests/test_refresh_synced_tables.py -v` — all green.

---

## Task A1: dbt mart model — `fct_funnel_stages_agg.sql`

**Files:**
- Create: `dbt_project/models/marts/fct_funnel_stages_agg.sql`

- [ ] **Step 1: Create the model file**

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_funnel_stages_agg.sql
-- Pre-aggregated conversion funnel stages for Taipy Conversion Funnel page.
--
-- Motivation (2026-04-17 D58 perf + correctness audit):
-- The live funnel query scans fct_action_values (9.5 M rows). Season mode hits
-- a Parallel Seq Scan (6,305 ms no-gs / 37,800 ms with gs) — exceeding the
-- app's 30 s statement_timeout for prolific teams. Simultaneously, the old
-- Taipy query truncated results at LIMIT 500000, silently dropping 57 % of
-- actions for (comp=11, team=217) — under-reporting A3 entries, shots and
-- goals by >50 %. Pre-aggregating to (match_id, team_id, game_state) grain
-- yields ~12,145 rows total and closes both bugs in one change.
--
-- Grain: (match_id, team_id, game_state)
--   — dbt_utils.unique_combination_of_columns asserts this.
--   — opponent_team_id is derivable from match_summary home/away.
--
-- Straddler handling (V01 Phase 0 verification):
--   168,298 (match_id, possession_id) pairs span >1 game_state within a match.
--   pos_in_gs  — COUNT(DISTINCT possession_id) within this (match, team, gs);
--                a straddler is counted ONCE per game_state it touches.
--   pos_in_match — COUNT(DISTINCT possession_id) across the full (match, team);
--                  replicated on every gs row for that match+team so the app
--                  can dedup via groupby((match,team)).first().sum() at gs=All.
--
-- Wyscout handling:
--   Wyscout actions have possession_id = NULL. Current Python treats those
--   as 1 synthetic possession per match (at gs=All) or per (match, gs) at
--   gs-filter. wy_match_flag=1 if a team had any NULL-possession row in the
--   match; replicated across all gs rows for that (match, team). The app
--   dedups at the driver via COUNT(DISTINCT CASE WHEN wy_match_flag=1 THEN match_id END).

with base as (

    select
        av.match_id,
        av.competition_id,
        av.team_id,
        av.game_state,
        av.possession_id,
        av.possession_team_id,
        av.start_x,
        av.end_x,
        av.action_type,
        av.action_result,
        ms.home_team_id,
        ms.away_team_id
    from {{ ref('fct_action_values') }} av
    join {{ ref('fct_match_summary') }} ms using (match_id)
    where av.team_id is not null
      and av.game_state is not null

),

own_possession as (

    select
        *,
        case
            when team_id = home_team_id then away_team_id
            else home_team_id
        end as opponent_team_id
    from base
    where possession_team_id is null or possession_team_id = team_id

),

per_gs as (

    select
        match_id,
        competition_id,
        team_id,
        opponent_team_id,
        game_state,
        count(distinct case when possession_id is not null then possession_id end)      as pos_in_gs,
        sum(case when start_x <= 70 and end_x > 70 then 1 else 0 end)                    as a3_entries,
        sum(case when action_type in ('shot','shot_penalty','shot_freekick') then 1 else 0 end) as shots,
        sum(case
                when action_type in ('shot','shot_penalty','shot_freekick')
                 and action_result = 'success'
                then 1 else 0
            end)                                                                         as goals
    from own_possession
    group by match_id, competition_id, team_id, opponent_team_id, game_state

),

per_match as (

    select
        match_id,
        team_id,
        count(distinct case when possession_id is not null then possession_id end) as pos_in_match,
        max(case when possession_id is null then 1 else 0 end)                     as wy_match_flag
    from own_possession
    group by match_id, team_id

),

final as (

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

)

select * from final
```

- [ ] **Step 2: dbt parse locally (no warehouse connection needed)**

Run: `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project`
Expected: Exit 0. Model picked up. No Jinja / ref errors. If `ensure_warehouse.py` is triggered because of compile-time checks, the parse step alone should not require warehouse — `dbt parse` reads manifest locally.

Note: mart population runs in Phase B1 — Task A1 only authors the file.

---

## Task A2: dbt contract — `_marts__models.yml`

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml` (insert after the `fct_heatmap_agg` / `fct_vaep_breakdown_agg` / `fct_gk_actions_detail` block; alphabetical: after `fct_gk_actions_detail`, before `fct_heatmap_agg`)

- [ ] **Step 1: Insert the new contract block**

Locate the `fct_heatmap_agg` block (begins around line 2867) and insert the new block **before** it, so the agg marts remain alphabetically ordered: `fct_funnel_stages_agg` → `fct_gk_actions_detail` → `fct_heatmap_agg` → `fct_vaep_breakdown_agg`.

Since `fct_gk_actions_detail` is at line ~2974 and `fct_heatmap_agg` at line ~2867, the actual file ordering may not be strictly alphabetical. Insert the new block **immediately before** `fct_heatmap_agg` to group it with the other agg marts, and leave an explanatory lead comment.

```yaml
  - name: fct_funnel_stages_agg
    description: >
      Pre-aggregated conversion funnel stages for Taipy Conversion Funnel page.
      Grain: (match_id, team_id, game_state). Source: fct_action_values INNER
      JOIN fct_match_summary. Replaces a 37,800 ms Parallel Seq Scan + Nested
      Loop that exceeded the app's 30 s statement_timeout, AND closes a silent
      LIMIT 500000 truncation that under-reported A3/shots/goals by >50 % for
      prolific teams. See fct_funnel_stages_agg.sql header for straddler +
      Wyscout handling.
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    columns:
      - name: match_id
        data_type: bigint
        description: Foreign key to fct_match_summary.
        data_tests:
          - not_null
          - relationships:
              to: ref('fct_match_summary')
              field: match_id
      - name: competition_id
        data_type: int
        description: Denormalized for season-mode WHERE clauses (drives idx_funnel_agg_comp_*_gs).
        data_tests:
          - not_null
      - name: team_id
        data_type: int
        description: Acting team.
        data_tests:
          - not_null
      - name: opponent_team_id
        data_type: int
        description: The other team in the match (derived from fct_match_summary home/away).
        data_tests:
          - not_null
      - name: game_state
        data_type: string
        description: "Score-state bucket — winning, losing, or drawing — at the time of the action."
        data_tests:
          - not_null
          - accepted_values:
              values: ['winning', 'losing', 'drawing']
      - name: pos_in_gs
        data_type: bigint
        description: >
          COUNT(DISTINCT possession_id) within this (match, team, game_state).
          Straddlers that cross gs boundaries are counted once per gs they touch.
        data_tests:
          - not_null
      - name: pos_in_match
        data_type: bigint
        description: >
          COUNT(DISTINCT possession_id) across the full (match, team). Replicated on
          every gs row for that match+team so app-side dedup at gs=All is one
          groupby().first().sum() call.
        data_tests:
          - not_null
      - name: a3_entries
        data_type: bigint
        description: Actions with start_x ≤ 70 AND end_x > 70 (attacking-third entries).
        data_tests:
          - not_null
      - name: shots
        data_type: bigint
        description: Actions where action_type is shot, shot_penalty, or shot_freekick.
        data_tests:
          - not_null
      - name: goals
        data_type: bigint
        description: Shots where action_result = 'success'.
        data_tests:
          - not_null
      - name: wy_match_flag
        data_type: smallint
        description: >
          1 if the team had any NULL-possession_id row (Wyscout) in this match;
          replicated on every gs row for that match+team. Used at app-driver to
          dedup synthetic-per-match possessions across gs rows.
        data_tests:
          - not_null
          - accepted_values:
              values: [0, 1]
      - name: _loaded_at
        data_type: timestamp
        description: Audit timestamp.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - match_id
            - team_id
            - game_state
```

- [ ] **Step 2: dbt parse again to confirm YAML is valid**

Run: `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project`
Expected: Exit 0, no contract / YAML syntax errors.

---

## Task A3: Workflow card — `wf-dbt-build.yaml`

**Files:**
- Modify: `workflow-cards/wf-dbt-build.yaml` (insert between `fct_formation_labels` and `fct_gk_actions_detail` — alphabetical)

- [ ] **Step 1: Insert new `outputs.tables` entry**

Find the line `dbt_model: fct_formation_labels` (around line 57) and insert immediately after it, before `- id: "{catalog}.dev_gold.fct_gk_actions_detail"`:

```yaml
    - id: "{catalog}.dev_gold.fct_funnel_stages_agg"
      destination: delta-table
      dbt_model: fct_funnel_stages_agg
```

- [ ] **Step 2: Run workflow-card validator**

Run: `uv run pytest src/tests/test_card_dbt_model_field.py src/tests/test_card_cost_phase_parity.py -v`
Expected: PASS. If `test_card_dbt_model_field` fails because the new `.sql` file is not yet in the filesystem scan, it means the test parses `dbt_project/models/marts/*.sql` — Task A1 already created the file so it should match. If the test still fails, read its output carefully; it reports mismatches bidirectionally.

---

## Task A4: PG index definitions — `scripts/create_indexes.py`

**Files:**
- Modify: `scripts/create_indexes.py` — append to `INDEXES` list and `VERIFY_QUERIES` list

- [ ] **Step 1: Append three new index entries to `INDEXES`**

Add these three tuples at the end of the `INDEXES` list, immediately after the existing `fct_gk_actions_detail_*` entries (near line 179). Use the comment banner pattern used by previous agg marts:

```python
    # fct_funnel_stages_agg: ~12,145 rows. Filter combos: match (single-match
    # mode) / comp+team+gs OR comp+opp+gs (season mode BitmapOr union).  Three
    # composite B-tree indexes serve all four access paths identified in D58 spec.
    ("idx_funnel_agg_match", "fct_funnel_stages_agg_synced", "match_id"),
    ("idx_funnel_agg_comp_team_gs", "fct_funnel_stages_agg_synced", "competition_id, team_id, game_state"),
    ("idx_funnel_agg_comp_opp_gs", "fct_funnel_stages_agg_synced", "competition_id, opponent_team_id, game_state"),
```

- [ ] **Step 2: Append three new verify queries to `VERIFY_QUERIES`**

Add at the end of the `VERIFY_QUERIES` list (immediately after the existing `fct_player_embeddings_career` HNSW entry):

```python
    (
        "fct_funnel_stages_agg: single-match (idx_funnel_agg_match)",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced WHERE match_id = 3888713 LIMIT 1",  # noqa: S608
    ),
    (
        "fct_funnel_stages_agg: season selected-team (idx_funnel_agg_comp_team_gs)",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced"  # noqa: S608
        " WHERE competition_id = 11 AND team_id = 217 LIMIT 1",
    ),
    (
        "fct_funnel_stages_agg: season opponent-side (idx_funnel_agg_comp_opp_gs)",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced"  # noqa: S608
        " WHERE competition_id = 11 AND opponent_team_id = 217 LIMIT 1",
    ),
```

- [ ] **Step 3: Ruff on the changed file**

Run: `uv run ruff check scripts/create_indexes.py`
Expected: No violations.

---

## Task A5: SYNCED_TABLES registration — `refresh_synced_tables.py`

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py` — append to `SYNCED_TABLES`

- [ ] **Step 1: Append new tuple**

Locate the last agg mart entry in `SYNCED_TABLES`:

```python
    # Pre-aggregated marts added 2026-04-17 (perf/base-case-query-bottlenecks)
    ("fct_heatmap_agg_synced", None),
    ("fct_vaep_breakdown_agg_synced", None),
    ("fct_gk_actions_detail_synced", None),
```

Insert the new tuple immediately after `fct_gk_actions_detail_synced` to keep agg marts grouped:

```python
    ("fct_funnel_stages_agg_synced", None),
```

The final block should read:

```python
    # Pre-aggregated marts added 2026-04-17 (perf/base-case-query-bottlenecks)
    ("fct_heatmap_agg_synced", None),
    ("fct_vaep_breakdown_agg_synced", None),
    ("fct_gk_actions_detail_synced", None),
    # Pre-aggregated mart added 2026-04-18 (perf/conversion-funnel, D58)
    ("fct_funnel_stages_agg_synced", None),
```

---

## Task A6: Test update — `test_refresh_synced_tables.py` count

**Files:**
- Modify: `src/tests/test_refresh_synced_tables.py:82-92`

- [ ] **Step 1: Write the updated test (TDD: this is the failing assertion that drives the fix)**

Replace the `test_synced_tables_list_has_37_entries` function with:

```python
def test_synced_tables_list_has_38_entries() -> None:
    """SYNCED_TABLES drift guard — should match the 38 tables in Terraform.

    34 baseline + 3 pre-aggregated marts added 2026-04-17
    (fct_heatmap_agg, fct_vaep_breakdown_agg, fct_gk_actions_detail) + 1
    pre-aggregated mart added 2026-04-18 (fct_funnel_stages_agg, D58) to
    eliminate the season-mode Parallel Seq Scan + LIMIT 500000 truncation
    on the Conversion Funnel page.
    """
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    assert len(SYNCED_TABLES) == 38
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py::test_synced_tables_list_has_38_entries -v`
Expected: PASS (Task A5 already added the 38th entry).

- [ ] **Step 3: Run the full test_refresh_synced_tables module**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py -v`
Expected: all green (no regressions).

---

## Task A7: Rewrite Taipy query module — `hf_taipy_app/src/queries/funnel.py`

**TDD note:** The tests in Task A8 encode the new query contract. Task A7 provides the implementation. Running the tests **before** finishing Task A8 is acceptable (Task A8 has the test bodies). A strict TDD order would swap: write Task A8 first, run it (red), then write Task A7 (green). Below I present A7 before A8 because the test file imports from the query module by path, not by function — so either ordering produces identical failing-then-passing states.

**Files:**
- Modify (replace entirety): `hf_taipy_app/src/queries/funnel.py`

- [ ] **Step 1: Replace the file contents**

```python
"""Conversion rate funnel — mart-only Lakebase queries + app-side rollups.

Replaces the earlier raw-event scan of fct_action_values (9.5 M rows → Parallel
Seq Scan, 37.8 s with game-state filter, exceeding app statement_timeout) with
a read of the pre-aggregated fct_funnel_stages_agg mart (~12,145 rows → composite
index lookup, <100 ms).  Simultaneously closes a silent LIMIT 500000 truncation
that under-reported A3 entries / shots / goals by >50 % for prolific teams.

Straddler + Wyscout semantics are handled in rollup_stages() using the two
possession-count columns (pos_in_gs, pos_in_match) plus the wy_match_flag —
see fct_funnel_stages_agg.sql header for the mart-side derivation.
"""

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
    """Pre-aggregated mart read — single path for single-match and season modes.

    Single-match mode: WHERE match_id = %s (idx_funnel_agg_match).
    Season mode:       WHERE competition_id = %s AND (team_id = %s OR opponent_team_id = %s)
                       (BitmapOr of idx_funnel_agg_comp_team_gs + idx_funnel_agg_comp_opp_gs).
    Game-state filter appends AND game_state = %s (lowercased).

    No LIMIT clause — mart is bounded to ~12,145 rows total. Adding a LIMIT
    would reintroduce the silent-truncation bug the mart was built to fix.
    """
    tbl = t("fct_funnel_stages_agg_synced")
    cols = (
        "match_id, competition_id, team_id, opponent_team_id, game_state,"
        " pos_in_gs, pos_in_match, a3_entries, shots, goals, wy_match_flag"
    )
    where: list[str]
    params: list[Any]
    if match_id is not None:
        where = ["match_id = %s"]
        params = [int(match_id)]
    else:
        where = ["competition_id = %s", "(team_id = %s OR opponent_team_id = %s)"]
        params = [int(comp_id), int(team_id), int(team_id)]
    if game_state and game_state != "All":
        where.append("game_state = %s")
        params.append(game_state.lower())
    return execute_query(
        f"SELECT {cols} FROM {tbl} WHERE {' AND '.join(where)}",  # noqa: S608
        tuple(params),
    )


@ttl_cache()
def _fetch_match_meta(comp_id: int, team_id: int, match_id: int) -> pd.DataFrame:
    """Single-match home/away name lookup — used only in single-match mode.

    team_id is retained in the signature so the cache key is per-team; match_id
    uniquely identifies the summary row, so team_id does not change the SQL.
    """
    ms_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT match_id, home_team_id, away_team_id, home_team_name, away_team_name"  # noqa: S608
        f" FROM {ms_tbl}"
        f" WHERE competition_id = %s AND match_id = %s"
        f" LIMIT 1",
        (int(comp_id), int(match_id)),
    )


def rollup_stages(rows: pd.DataFrame, *, gs_filtered: bool) -> dict[str, int]:
    """Collapse mart rows into funnel totals, honoring V01 straddler semantics.

    rows must be pre-filtered to a single side (selected team OR opponent — the
    caller splits the mart df on team_id vs opponent_team_id).

    gs_filtered = True  → use pos_in_gs (straddlers count once per gs they touched)
    gs_filtered = False → dedup pos_in_match across (match_id, team_id) then sum
                          (handles the per-match replication across gs rows)

    wy_match_flag=1 matches are counted once per match and added as synthetic
    possessions (Wyscout data has possession_id = NULL for 28.27 % of rows per V05).
    """
    if rows.empty:
        return dict.fromkeys(_STAGE_KEYS, 0)
    if gs_filtered:
        sb_possessions = int(rows["pos_in_gs"].sum())
    else:
        sb_possessions = int(
            rows.groupby(["match_id", "team_id"])["pos_in_match"].first().sum()
        )
    wy_matches = int(rows.loc[rows["wy_match_flag"] == 1, "match_id"].nunique())
    return {
        "possessions": sb_possessions + wy_matches,
        "a3_entries": int(rows["a3_entries"].sum()),
        "shots": int(rows["shots"].sum()),
        "goals": int(rows["goals"].sum()),
    }


def compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]:
    """Compute step-wise and end-to-end conversion rates (percentages 0-100).

    Unchanged from the previous implementation — kept for state-module parity.
    """

    def _pct(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0

    return {
        "poss_to_a3": _pct(stages["a3_entries"], stages["possessions"]),
        "a3_to_shot": _pct(stages["shots"], stages["a3_entries"]),
        "shot_to_goal": _pct(stages["goals"], stages["shots"]),
        "end_to_end": _pct(stages["goals"], stages["possessions"]),
    }
```

- [ ] **Step 2: Ruff + Pyright on the query module**

Run: `uv run ruff check hf_taipy_app/src/queries/funnel.py`
Expected: No violations.

Run: `uv run pyright hf_taipy_app/src/queries/funnel.py`
Expected: 0 errors, 0 warnings.

---

## Task A8: Rewrite funnel tests — `src/tests/test_conversion_funnel.py`

**Files:**
- Modify (full rewrite of the file, preserving `TestConversionRates` + `TestFunnelChart`): `src/tests/test_conversion_funnel.py`

- [ ] **Step 1: Replace the file contents**

```python
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


# ---------------------------------------------------------------------------
# TestRollupStages — V01 straddler + Wyscout regression guard for the
# driver-side rollup that consumes fct_funnel_stages_agg_synced rows.
# ---------------------------------------------------------------------------


class TestRollupStages:
    """Verify rollup_stages() semantics against straddler + Wyscout inputs."""

    def test_empty_rows(self) -> None:
        from queries.funnel import rollup_stages

        result = rollup_stages(pd.DataFrame(), gs_filtered=True)
        assert result == {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}

        result_all = rollup_stages(pd.DataFrame(), gs_filtered=False)
        assert result_all == {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}

    def test_sb_only_gs_filtered(self) -> None:
        """gs_filtered=True → possessions is sum of pos_in_gs."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200],
                "team_id": [1, 1],
                "pos_in_gs": [10, 5],
                "pos_in_match": [15, 8],  # ignored at gs_filtered=True
                "a3_entries": [4, 2],
                "shots": [1, 1],
                "goals": [0, 1],
                "wy_match_flag": [0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["possessions"] == 15
        assert result["a3_entries"] == 6
        assert result["shots"] == 2
        assert result["goals"] == 1

    def test_sb_only_gs_all(self) -> None:
        """gs_filtered=False → possessions is groupby((match,team)).first(pos_in_match).sum()."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200],
                "team_id": [1, 1],
                "pos_in_gs": [10, 5],
                "pos_in_match": [15, 8],
                "a3_entries": [4, 2],
                "shots": [1, 1],
                "goals": [0, 1],
                "wy_match_flag": [0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 15 + 8

    def test_straddler_gs_all_deduped(self) -> None:
        """V01 regression guard — a straddler spans multiple gs rows.

        Same match appears on three gs rows, each with pos_in_gs=5 but
        pos_in_match=12. At gs_filtered=False, possessions must be 12
        (NOT 15 — that would double-count the straddler).
        """
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 100, 100],
                "team_id": [1, 1, 1],
                "pos_in_gs": [5, 5, 5],
                "pos_in_match": [12, 12, 12],
                "a3_entries": [2, 1, 2],
                "shots": [1, 0, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [0, 0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 12, "straddler must dedup to pos_in_match=12"

    def test_straddler_gs_filtered_not_deduped(self) -> None:
        """Same straddler rows — gs_filtered=True sums pos_in_gs = 15."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 100, 100],
                "team_id": [1, 1, 1],
                "pos_in_gs": [5, 5, 5],
                "pos_in_match": [12, 12, 12],
                "a3_entries": [2, 1, 2],
                "shots": [1, 0, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [0, 0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["possessions"] == 15

    def test_wy_match_deduped_across_gs(self) -> None:
        """A Wyscout match with wy_match_flag=1 on all 3 gs rows → counted ONCE."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [300, 300, 300],
                "team_id": [5, 5, 5],
                "pos_in_gs": [0, 0, 0],
                "pos_in_match": [0, 0, 0],
                "a3_entries": [4, 2, 3],
                "shots": [1, 1, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [1, 1, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        # 0 SB possessions + 1 Wyscout match = 1 synthetic possession
        assert result["possessions"] == 1
        assert result["a3_entries"] == 9

    def test_wy_mixed_sb(self) -> None:
        """2 SB matches (pos_in_match=15, 8) + 1 Wyscout match → 15+8+1 = 24."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200, 300],
                "team_id": [1, 1, 1],
                "pos_in_gs": [10, 5, 0],
                "pos_in_match": [15, 8, 0],
                "a3_entries": [4, 2, 3],
                "shots": [1, 1, 0],
                "goals": [0, 1, 0],
                "wy_match_flag": [0, 0, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 15 + 8 + 1

    def test_stage_sums_independent_of_wyscout(self) -> None:
        """a3/shots/goals are simple sums regardless of wy_match_flag."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 300],
                "team_id": [1, 1],
                "pos_in_gs": [10, 0],
                "pos_in_match": [15, 0],
                "a3_entries": [4, 3],
                "shots": [1, 2],
                "goals": [0, 1],
                "wy_match_flag": [0, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["a3_entries"] == 7
        assert result["shots"] == 3
        assert result["goals"] == 1


# ---------------------------------------------------------------------------
# TestFetchFunnelAggSQL — captures the SQL + params emitted by
# fetch_funnel_agg under each of the four filter combinations.
# Protects against LIMIT re-introduction (the original D58 correctness bug).
# ---------------------------------------------------------------------------


def _capture_execute_query() -> tuple[list[tuple[str, tuple]], MagicMock]:
    """Return (captured_calls, patched_execute_query).

    captured_calls is appended to inside the mock; each entry is
    (sql_string, params_tuple).
    """
    calls: list[tuple[str, tuple]] = []

    def _mock(sql: str, params: tuple) -> pd.DataFrame:
        calls.append((sql, params))
        return pd.DataFrame()

    return calls, MagicMock(side_effect=_mock)


class TestFetchFunnelAggSQL:
    """Capture SQL + params for every supported combination of filter args."""

    def test_single_match_no_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state=None)

        sql, params = calls[0]
        assert "match_id = %s" in sql
        assert "competition_id" not in sql
        assert "game_state" not in sql
        assert params == (3888713,)

    def test_single_match_with_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state="Drawing")

        sql, params = calls[0]
        assert "match_id = %s" in sql
        assert "game_state = %s" in sql
        # Lowercased at the query-module boundary
        assert params == (3888713, "drawing")

    def test_season_no_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state=None)

        sql, params = calls[0]
        assert "competition_id = %s" in sql
        assert "(team_id = %s OR opponent_team_id = %s)" in sql
        assert "game_state" not in sql
        assert params == (11, 217, 217)

    def test_season_with_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state="Winning")

        sql, params = calls[0]
        assert "competition_id = %s" in sql
        assert "(team_id = %s OR opponent_team_id = %s)" in sql
        assert "game_state = %s" in sql
        assert params == (11, 217, 217, "winning")

    def test_game_state_all_is_treated_as_no_filter(self) -> None:
        """game_state='All' is a sentinel for 'no filter' — must NOT emit a clause."""
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state="All")

        sql, _params = calls[0]
        assert "game_state" not in sql

    def test_no_limit_clause(self) -> None:
        """V10 correctness guard — no code path may emit LIMIT.

        The mart is bounded to ~12,145 rows total; a LIMIT would reintroduce
        the 2026-04-17 silent-truncation bug that under-reported A3/shots/goals
        by >50 % for prolific teams.
        """
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod.fetch_funnel_agg(11, 217)
            mod.fetch_funnel_agg(11, 217, match_id=3888713)
            mod.fetch_funnel_agg(11, 217, game_state="Drawing")
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state="Drawing")

        for sql, _ in calls:
            assert "LIMIT" not in sql.upper(), f"LIMIT found in emitted SQL: {sql}"


class TestFetchMatchMetaSingle:
    """_fetch_match_meta must use LIMIT 1 (single-match lookup only)."""

    def test_limit_is_1_not_200(self) -> None:
        """Old implementation had LIMIT 200 (season-mode artifact) — must be 1 now."""
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with patch.object(mod, "execute_query", mock_exec):
            mod._fetch_match_meta(11, 217, 3888713)

        sql, params = calls[0]
        assert "LIMIT 1" in sql
        assert "LIMIT 200" not in sql
        assert "LIMIT 500000" not in sql
        assert params == (11, 3888713)


# ---------------------------------------------------------------------------
# TestConversionRates (unchanged — pre-existing, kept for regression coverage)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestFunnelChart (unchanged — pre-existing, kept for regression coverage)
# ---------------------------------------------------------------------------


try:
    import plotly  # noqa: F401

    _has_plotly = True
except ImportError:
    _has_plotly = False


@pytest.mark.skipif(not _has_plotly, reason="plotly not installed")
class TestFunnelChart:
    """Verify mirror funnel chart rendering."""

    def test_chart_has_two_traces(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        assert len(fig.data) == 2  # pyright: ignore[reportArgumentType]

    def test_chart_home_positive_away_negative(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        assert all(v >= 0 for v in fig.data[0].x)  # pyright: ignore[reportAttributeAccessIssue]
        assert all(v <= 0 for v in fig.data[1].x)  # pyright: ignore[reportAttributeAccessIssue]

    def test_chart_uses_canonical_colors(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        away = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        fig = _build_mirror_chart(home, away, "H", "A")
        assert fig.data[0].marker.color == "#e63946"  # pyright: ignore[reportAttributeAccessIssue]
        assert fig.data[1].marker.color == "#457b9d"  # pyright: ignore[reportAttributeAccessIssue]
```

- [ ] **Step 2: Run the new + kept tests**

Run: `uv run pytest src/tests/test_conversion_funnel.py -v`
Expected: all green — 8 TestRollupStages + 6 TestFetchFunnelAggSQL + 1 TestFetchMatchMetaSingle + 2 TestConversionRates + 3 TestFunnelChart = 20 tests.

- [ ] **Step 3: Ruff + Pyright**

Run: `uv run ruff check src/tests/test_conversion_funnel.py`
Expected: No violations.

Run: `uv run pyright src/tests/test_conversion_funnel.py`
Expected: 0 errors.

---

## Task A9: Rewrite state module — `hf_taipy_app/src/state/conversion_funnel.py`

**Files:**
- Modify: `hf_taipy_app/src/state/conversion_funnel.py` — replace only `cf_refresh` and its imports; keep `_build_mirror_chart`, `_clear_state`, `on_cf_game_state_change`, `register_page_refresher`, and all module-level state vars intact.

- [ ] **Step 1: Rewrite the imports and cf_refresh**

Replace the imports and the `cf_refresh` function with:

```python
"""Conversion Rate Funnel — state module (prefix: cf_)."""

from __future__ import annotations

import logging
from typing import Any

import plotly.graph_objects as go
from queries.funnel import (
    _fetch_match_meta,
    compute_conversion_rates,
    fetch_funnel_agg,
    rollup_stages,
)
from render import AWAY_COLOR, HOME_COLOR, PITCH_BG_COLOR, TEXT_COLOR

from state.shared import (
    get_comp_id,
    get_match_id,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)
```

The module-level state vars (`cf_possessions` … `cf_game_state_lov`), `__all__`, `_STAGE_LABELS`/`_STAGE_KEYS`, `_build_mirror_chart`, `_clear_state`, `on_cf_game_state_change`, and `register_page_refresher` at the bottom all remain unchanged.

Replace the `cf_refresh` function (lines 152-233 of the current file) with:

```python
def cf_refresh(state: Any) -> None:
    """Refresh conversion funnel data for the selected filters.

    Two aggregation modes:
    - **Single match**: Home vs Away mirror chart with team names from JOIN.
    - **Season**: Selected Team vs Opponents — summed per side from the
      pre-aggregated fct_funnel_stages_agg mart. Straddler + Wyscout
      semantics are handled by rollup_stages() (see queries/funnel.py).
    """
    comp_id = get_comp_id(state.selected_competition)
    if not comp_id:
        _clear_state(state)
        return

    team_id = get_team_id(state.selected_team)
    if not team_id:
        _clear_state(state)
        return

    match_id = get_match_id(state.selected_match)
    game_state = getattr(state, "cf_selected_game_state", "All")
    gs_param = game_state if game_state and game_state != "All" else None
    gs_filtered = gs_param is not None

    df = fetch_funnel_agg(comp_id, team_id, match_id, gs_param)
    if df.empty:
        _clear_state(state)
        state.cf_warning_text = (
            "No action data found for this filter combination. "
            "Try selecting a different competition or team."
        )
        return

    team_rows = df[df["team_id"] == team_id]
    opp_rows = df[df["team_id"] != team_id]
    primary_stages = rollup_stages(team_rows, gs_filtered=gs_filtered)
    opp_stages = rollup_stages(opp_rows, gs_filtered=gs_filtered)

    if match_id is not None:
        meta = _fetch_match_meta(comp_id, team_id, match_id)
        if meta.empty:
            _clear_state(state)
            state.cf_warning_text = (
                "Match metadata not found. Try selecting a different match."
            )
            return
        home_tid = int(meta["home_team_id"].iloc[0])
        home_name = str(meta["home_team_name"].iloc[0])
        away_name = str(meta["away_team_name"].iloc[0])
        home_stages, away_stages = (
            (primary_stages, opp_stages)
            if team_id == home_tid
            else (opp_stages, primary_stages)
        )
        state.cf_funnel_chart = _build_mirror_chart(
            home_stages, away_stages, home_name, away_name
        )
        show_stages = primary_stages
    else:
        state.cf_funnel_chart = _build_mirror_chart(
            primary_stages, opp_stages, str(state.selected_team), "Opponents"
        )
        show_stages = primary_stages

    show_rates = compute_conversion_rates(show_stages)

    state.cf_possessions = f"{show_stages['possessions']:,}"
    state.cf_possessions_detail = "total team possessions"
    state.cf_a3_entries = f"{show_stages['a3_entries']:,}"
    state.cf_a3_detail = f"{show_rates['poss_to_a3']}% of possessions"
    state.cf_shots = f"{show_stages['shots']:,}"
    state.cf_shots_detail = f"{show_rates['a3_to_shot']}% of A3 entries"
    state.cf_goals = f"{show_stages['goals']:,}"
    state.cf_goals_detail = f"{show_rates['shot_to_goal']}% of shots"

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
        "Funnel refreshed: stages=%s rates=%s", show_stages, show_rates
    )
```

- [ ] **Step 2: Ruff + Pyright on the state module**

Run: `uv run ruff check hf_taipy_app/src/state/conversion_funnel.py`
Expected: No violations.

Run: `uv run pyright hf_taipy_app/src/state/conversion_funnel.py`
Expected: 0 errors.

---

## Task A10: Re-enable page in `main.py` (imports + uncomment)

**Files:**
- Modify: `hf_taipy_app/src/main.py` — add two page imports + uncomment line 106

Note: the spec said "uncomment line 106", but `funnel_config` and `funnel_page` are **not currently imported** in `main.py`. Both additions are required.

- [ ] **Step 1: Add page imports**

The `pages.*` imports currently span lines 12–44 in alphabetical order. Insert after `from pages.defensive_valuation import page_md as defensive_impact_page` (line 15, end of the `defensive_valuation` block, next is `from pages.goalkeeper ...`). The correct alphabetical slot is **before `goalkeeper`** and **after `defensive_valuation`**:

```python
from pages.conversion_funnel import page_config as funnel_config
from pages.conversion_funnel import page_md as funnel_page
```

Final ordering (lines 12–17 become):

```python
from pages.action_values import page_config as action_values_config
from pages.action_values import page_md as action_values_page
from pages.conversion_funnel import page_config as funnel_config
from pages.conversion_funnel import page_md as funnel_page
from pages.defensive_valuation import page_config as defensive_impact_config
from pages.defensive_valuation import page_md as defensive_impact_page
from pages.goalkeeper import page_config as goalkeeper_config
from pages.goalkeeper import page_md as goalkeeper_page
```

- [ ] **Step 2: Uncomment the PageEntry**

Change line 106 from:

```python
    # PageEntry("Conversion-Funnel", funnel_config, funnel_page),  # disabled — query perf not validated from HF Space
```

to:

```python
    PageEntry("Conversion-Funnel", funnel_config, funnel_page),
```

- [ ] **Step 3: Ruff on main.py**

Run: `uv run ruff check hf_taipy_app/src/main.py`
Expected: No violations.

---

## Task A11: Parity integration test — `src/tests/integration/test_funnel_mart_parity.py`

**Files:**
- Create: `src/tests/integration/test_funnel_mart_parity.py`
- Verify `src/tests/integration/` directory exists (create if missing; add empty `__init__.py` if the package requires it).

- [ ] **Step 1: Ensure the directory exists**

Check: `ls src/tests/integration/`
If the directory does not exist, create it:

```bash
mkdir -p src/tests/integration
```

Check for an `__init__.py`. If `src/tests/__init__.py` exists, add an empty `src/tests/integration/__init__.py` too. If not, skip (pytest auto-discovery handles it).

- [ ] **Step 2: Write the test file**

```python
"""D58 parity integration test — env-gated, runs against live Lakebase.

Skipped by default unless LAKEBASE_HOST is set. Uses the 6 V10 locked oracle
fixtures captured 2026-04-17 18:56 UTC from the current live Lakebase (see
docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md § V10).

Four fixtures must hit exactly — those are cases the old query did NOT
truncate, so the mart should reproduce them zero-delta.

Two fixtures (comp=11 team=217 season) are the correctness-fix cases:
the old query silently dropped 57 % of actions via LIMIT 500000, so the
mart MUST return values ≥ the oracle on every stage.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "hf_taipy_app" / "src"))

pytestmark = pytest.mark.skipif(
    not os.environ.get("LAKEBASE_HOST"),
    reason="Live Lakebase integration test — set LAKEBASE_HOST to run",
)


def _rollup(df, team_id: int, *, gs_filtered: bool) -> dict[str, int]:
    """Call rollup_stages after splitting the mart df by side."""
    from queries.funnel import rollup_stages

    team_rows = df[df["team_id"] == team_id]
    opp_rows = df[df["team_id"] != team_id]
    return {
        "primary": rollup_stages(team_rows, gs_filtered=gs_filtered),
        "opponent": rollup_stages(opp_rows, gs_filtered=gs_filtered),
    }


# Oracle values: (possessions, a3_entries, shots, goals)
# Captured 2026-04-17 18:56 UTC from the live Lakebase query path (V10).
_EXACT_FIXTURES: list[tuple[str, dict, dict, dict]] = [
    (
        "comp=11 team=213 match=None gs=None",
        {"comp_id": 11, "team_id": 213, "match_id": None, "game_state": None},
        {"primary": (6295, 2721, 742, 75), "opponent": (6510, 3341, 974, 115)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=213 match=None gs=winning",
        {"comp_id": 11, "team_id": 213, "match_id": None, "game_state": "winning"},
        {"primary": (1198, 589, 187, 37), "opponent": (2118, 1081, 320, 73)},
        {"gs_filtered": True},
    ),
    (
        "comp=11 team=217 match=3888713 gs=None",
        {"comp_id": 11, "team_id": 217, "match_id": 3888713, "game_state": None},
        {"primary": (101, 7, 21, 5), "opponent": (109, 69, 7, 0)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=217 match=3888713 gs=drawing",
        {"comp_id": 11, "team_id": 217, "match_id": 3888713, "game_state": "drawing"},
        {"primary": (32, 4, 4, 0), "opponent": (33, 23, 3, 0)},
        {"gs_filtered": True},
    ),
]

# Oracle is the TRUNCATED value; mart must be >= on every stage.
# Delta = correctness fix quantified.
_GREATER_OR_EQUAL_FIXTURES: list[tuple[str, dict, dict, dict]] = [
    (
        "comp=11 team=217 match=None gs=None",
        {"comp_id": 11, "team_id": 217, "match_id": None, "game_state": None},
        {"primary": (47201, 13812, 3523, 570), "opponent": (38526, 8186, 1933, 178)},
        {"gs_filtered": False},
    ),
    (
        "comp=11 team=217 match=None gs=drawing",
        {"comp_id": 11, "team_id": 217, "match_id": None, "game_state": "drawing"},
        {"primary": (23244, 13777, 3240, 251), "opponent": (19766, 8302, 1762, 102)},
        {"gs_filtered": True},
    ),
]


def _tuple_from_dict(stages: dict[str, int]) -> tuple[int, int, int, int]:
    return (
        stages["possessions"],
        stages["a3_entries"],
        stages["shots"],
        stages["goals"],
    )


@pytest.mark.parametrize(
    ("label", "params", "oracle", "meta"),
    _EXACT_FIXTURES,
    ids=[f[0] for f in _EXACT_FIXTURES],
)
def test_mart_parity_exact(label: str, params: dict, oracle: dict, meta: dict) -> None:
    """Mart must reproduce the live-oracle tuple with zero delta."""
    from queries.funnel import fetch_funnel_agg

    df = fetch_funnel_agg(**params)
    assert not df.empty, f"mart returned empty rows for {label}"

    rolled = _rollup(df, team_id=params["team_id"], gs_filtered=meta["gs_filtered"])
    assert _tuple_from_dict(rolled["primary"]) == oracle["primary"], f"primary {label}"
    assert _tuple_from_dict(rolled["opponent"]) == oracle["opponent"], f"opponent {label}"


@pytest.mark.parametrize(
    ("label", "params", "oracle", "meta"),
    _GREATER_OR_EQUAL_FIXTURES,
    ids=[f[0] for f in _GREATER_OR_EQUAL_FIXTURES],
)
def test_mart_parity_greater_or_equal(
    label: str, params: dict, oracle: dict, meta: dict
) -> None:
    """Mart closes the LIMIT-500000 correctness gap — must be ≥ oracle per stage."""
    from queries.funnel import fetch_funnel_agg

    df = fetch_funnel_agg(**params)
    assert not df.empty, f"mart returned empty rows for {label}"

    rolled = _rollup(df, team_id=params["team_id"], gs_filtered=meta["gs_filtered"])

    primary = _tuple_from_dict(rolled["primary"])
    opponent = _tuple_from_dict(rolled["opponent"])

    for i, stage in enumerate(("possessions", "a3_entries", "shots", "goals")):
        assert primary[i] >= oracle["primary"][i], (
            f"primary {stage} regressed: {primary[i]} < oracle {oracle['primary'][i]} ({label})"
        )
        assert opponent[i] >= oracle["opponent"][i], (
            f"opponent {stage} regressed: {opponent[i]} < oracle {oracle['opponent'][i]} ({label})"
        )
```

- [ ] **Step 3: Verify collection (skipped in CI without LAKEBASE_HOST)**

Run: `uv run pytest src/tests/integration/test_funnel_mart_parity.py -v --collect-only`
Expected: 6 tests collected, all marked SKIPPED (when `LAKEBASE_HOST` is not set).

Actual execution against live Lakebase runs in Phase C1.

---

## Task A12: Add TF resource block — `terraform/modules/synced_tables/main.tf`

**Files:**
- Modify: `terraform/modules/synced_tables/main.tf` — append after the `fct_gk_actions_detail` resource block (around line 650)

**Note:** This is declarative only. No `terraform apply`. The actual import happens in Phase B4 after the user creates the synced table via UI in Phase B3.

- [ ] **Step 1: Append the resource block**

At the end of the file (after the `fct_gk_actions_detail` resource, currently ending at line ~650), append:

```hcl
resource "databricks_database_synced_database_table" "fct_funnel_stages_agg" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg"
    primary_key_columns    = ["match_id", "team_id", "game_state"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}
```

- [ ] **Step 2: Terraform fmt**

Run: `terraform fmt -check terraform/modules/synced_tables/main.tf`
Expected: Exit 0 (formatting clean). If not, run without `-check` to apply.

- [ ] **Step 3: Terraform validate**

Run from `terraform/environments/dev/`: `terraform validate`
Expected: "Success! The configuration is valid."

Do **not** run `terraform plan` yet — the synced table does not exist on the Databricks side until Phase B3, so plan would try to create it and lose the UI-managed state (the ADR-004 import pattern requires UI-create first).

---

## Task A13: Phase A gate — full green

- [ ] **Step 1: Ruff across the project**

Run: `uv run ruff check src/ scripts/ hf_taipy_app/src/`
Expected: No violations.

- [ ] **Step 2: Ruff format check**

Run: `uv run ruff format --check src/ scripts/ hf_taipy_app/src/`
Expected: All files formatted.

- [ ] **Step 3: Pyright**

Run: `uv run pyright src/`
Expected: 0 errors, 0 warnings.

Run: `uv run pyright hf_taipy_app/src/queries/funnel.py hf_taipy_app/src/state/conversion_funnel.py hf_taipy_app/src/main.py`
Expected: 0 errors.

- [ ] **Step 4: Python tests**

Run: `uv run pytest src/tests/test_conversion_funnel.py src/tests/test_refresh_synced_tables.py src/tests/test_card_dbt_model_field.py src/tests/test_card_cost_phase_parity.py -v`
Expected: all green (no skips except plotly-gated chart tests if plotly absent).

- [ ] **Step 5: dbt parse**

Run: `uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project`
Expected: Exit 0.

- [ ] **Step 6: `git diff` summary for review**

Run: `git status` and `git diff --stat`

**⏸️ Approval Checkpoint 1 — end of Phase A.**

Present the Phase A diff to the user. **Wait for explicit approval before starting Phase B.** No commits.

---

# Phase B — Infrastructure (live warehouse + Lakebase + Terraform)

Sequential. User handoff at Task B3 — wait for confirmation before continuing.

---

## Task B1: Build the mart

- [ ] **Step 1: Ensure the warehouse is running**

Run: `uv run python scripts/ensure_warehouse.py -- echo "warehouse ready"`
Expected: prints "warehouse ready" after the warehouse transitions to RUNNING. If this takes >60 s, use `run_in_background: true` per CLAUDE.md rule.

- [ ] **Step 2: Build the new model + downstream**

Run: `uv run python scripts/ensure_warehouse.py -- dbt build --project-dir dbt_project --profiles-dir dbt_project --select fct_funnel_stages_agg+`
Expected: model compiles, runs, and passes all contract + data tests. The `+` selector includes any downstream models (none expected today — the mart is a new leaf).

- [ ] **Step 3: Verify row count**

Using the Databricks SQL connector or the `scripts/_d58_phase0.py` pattern, run:

```sql
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_funnel_stages_agg
```

Expected: 12,145 (±1 % tolerance for potential source-table updates since Phase 0 was captured). If the count differs by >10 %, pause and investigate before proceeding — the oracle fixtures in Task A11 may need re-capture.

---

## Task B2: dbt tests (explicit)

- [ ] **Step 1: Run the contract + data tests**

Run: `uv run python scripts/ensure_warehouse.py -- dbt test --project-dir dbt_project --profiles-dir dbt_project --select fct_funnel_stages_agg`
Expected: all pass — `not_null` on every required column, `accepted_values` on `game_state` + `wy_match_flag`, `relationships` on `match_id → fct_match_summary.match_id`, `dbt_utils.unique_combination_of_columns` on the grain.

---

## Task B3: User handoff — create synced table via Databricks UI

**⏸️ Approval Checkpoint 2 — UI handoff.**

Provide the user with this exact prompt (paste in chat, wait for confirmation):

> **Create the `fct_funnel_stages_agg_synced` synced table in Databricks UI**
>
> 1. Open Databricks workspace → Catalog Explorer.
> 2. Navigate to `soccer_analytics.dev_gold.fct_funnel_stages_agg` (the source Delta table we just built).
> 3. Click **Create → Online Table (synced)**.
> 4. Settings:
>    - Name: `fct_funnel_stages_agg_synced`
>    - Target database instance: (same Lakebase instance the other 37 synced tables use)
>    - Primary keys: `match_id, team_id, game_state` (composite, in that order)
>    - Scheduling policy: **Snapshot**
> 5. Wait for the table to show "Active" in the Lakebase UI (1–3 minutes for 12k rows).
> 6. Reply "synced table active" when ready; include the pipeline ID from the sync details panel.

Do not proceed to B4 until the user confirms and provides the pipeline ID.

---

## Task B4: Terraform import

- [ ] **Step 1: Import the resource into TF state**

Use the pipeline ID provided by the user in B3. Run from `terraform/environments/dev/`:

```bash
terraform import module.synced_tables.databricks_database_synced_database_table.fct_funnel_stages_agg \
  "soccer_analytics.dev_gold.fct_funnel_stages_agg_synced"
```

Expected: "Import successful!" The resource block added in Task A12 matches the imported state.

- [ ] **Step 2: Verify zero drift**

Run: `terraform plan`
Expected: "No changes. Your infrastructure matches the configuration." If there is drift, compare the imported state (via `terraform state show module.synced_tables.databricks_database_synced_database_table.fct_funnel_stages_agg`) against the block in Task A12 and reconcile.

---

## Task B5: Grant workspace permissions to SPs

- [ ] **Step 1: Grant CAN_USE on the database project + CAN_RUN on the new backing pipeline**

Run: `uv run python scripts/grant_synced_table_permissions.py --grant`
Expected: idempotent — prints "granted" for the new pipeline, "already granted" for the existing 37. Both Taipy SP (`hf_app_v2`) and ingestion SP are covered.

- [ ] **Step 2: Status verify**

Run: `uv run python scripts/grant_synced_table_permissions.py --status`
Expected: both SPs show `CAN_USE` on the database project and `CAN_RUN` on the new pipeline ID.

---

## Task B6: Grant PG SELECT to Taipy SP on the new synced table

- [ ] **Step 1: Apply grants**

Run: `uv run python scripts/run_lakebase_grants.py`
Expected: `SELECT` granted to the Taipy SP on `fct_funnel_stages_agg_synced` (and all other tables — idempotent).

- [ ] **Step 2: Verify**

Run: `uv run python scripts/run_lakebase_grants.py --verify`
Expected: `OK: SP <app_id> has SELECT on all 38 synced tables`.

---

## Task B7: Create PG indexes + verify

- [ ] **Step 1: Create the 3 new indexes**

Run: `uv run python scripts/create_indexes.py`
Expected: three lines for `idx_funnel_agg_*` print `OK (...s)` (likely <1 s each for 12k rows). No errors. Summary prints "N processed (IF NOT EXISTS), 0 errors".

- [ ] **Step 2: Verify Index Scan on all access paths**

Run: `uv run python scripts/create_indexes.py --verify`
Expected: the three new entries at the end of `VERIFY_QUERIES` print `PASS — Index Scan detected`.

---

## Task V11: Post-index EXPLAIN gate — `_d58_v11_explain.py`

**Files:**
- Create: `scripts/_d58_v11_explain.py` (temporary; deleted at Phase D)

- [ ] **Step 1: Write the script**

```python
#!/usr/bin/env python3
"""D58 V11 gate — EXPLAIN ANALYZE on all 4 mart query shapes against live Lakebase.

Pass criteria:
  - Every plan shows Index Scan (no Seq Scan or Parallel Seq Scan on fct_funnel_stages_agg_synced).
  - Execution time <= 100 ms on each shape.

Run after Phase B7 (indexes created + verified). Deleted in Phase D before commit.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import uuid

import psycopg2
import requests

SCHEMA = "dev_gold"
THRESHOLD_MS = 100.0

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
LAKEBASE_HOST = os.environ["LAKEBASE_HOST"]
ENDPOINT_NAME = os.environ.get(
    "LAKEBASE_ENDPOINT_NAME", "projects/soccer-analytics-dev/branches/production/endpoints/primary"
)

# Four mart query shapes — match the SQL emitted by fetch_funnel_agg().
_QUERIES: list[tuple[str, str, tuple]] = [
    (
        "single-match no-gs",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced WHERE match_id = %s",  # noqa: S608
        (3888713,),
    ),
    (
        "single-match gs=drawing",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced"  # noqa: S608
        f" WHERE match_id = %s AND game_state = %s",
        (3888713, "drawing"),
    ),
    (
        "season team=217 no-gs",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced"  # noqa: S608
        f" WHERE competition_id = %s AND (team_id = %s OR opponent_team_id = %s)",
        (11, 217, 217),
    ),
    (
        "season team=217 gs=drawing",
        f"SELECT * FROM {SCHEMA}.fct_funnel_stages_agg_synced"  # noqa: S608
        f" WHERE competition_id = %s AND (team_id = %s OR opponent_team_id = %s)"
        f" AND game_state = %s",
        (11, 217, 217, "drawing"),
    ),
]

_EXEC_RE = re.compile(r"Execution Time:\s+([0-9.]+)\s+ms")


def _get_pg_credential() -> tuple[str, str]:
    """Copy of create_indexes._get_pg_credential — keep behavior identical."""
    try:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
        host = (ws.config.host or "").rstrip("/")
        auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]
    except Exception:
        result = subprocess.run(
            ["databricks", "auth", "token", "--profile", "OAUTH"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
        token = json.loads(result.stdout)["access_token"]
        host = DATABRICKS_HOST.rstrip("/")
        auth_headers = {"Authorization": f"Bearer {token}"}

    resp = requests.post(
        f"{host}/api/2.0/postgres/credentials",
        headers={**auth_headers, "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    pg_token: str = resp.json()["token"]
    payload = pg_token.split(".")[1] + "=="
    username: str = json.loads(base64.b64decode(payload))["sub"]
    return pg_token, username


def main() -> int:
    pg_token, username = _get_pg_credential()
    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname="databricks_postgres",
        user=username,
        password=pg_token,
        sslmode="require",
    )
    failures: list[str] = []
    try:
        with conn.cursor() as cur:
            for label, sql, params in _QUERIES:
                cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}", params)
                plan = "\n".join(row[0] for row in cur.fetchall())
                print(f"\n=== {label} ===\n{plan}\n")

                uses_seq = "Seq Scan" in plan or "Parallel Seq Scan" in plan
                uses_index = "Index" in plan
                match = _EXEC_RE.search(plan)
                exec_ms = float(match.group(1)) if match else float("inf")

                if uses_seq:
                    failures.append(f"{label}: Seq Scan detected")
                if not uses_index:
                    failures.append(f"{label}: no Index Scan in plan")
                if exec_ms > THRESHOLD_MS:
                    failures.append(f"{label}: {exec_ms:.1f} ms > {THRESHOLD_MS} ms threshold")

                print(f"    — exec={exec_ms:.1f} ms — seq={uses_seq} index={uses_index}")
    finally:
        conn.close()

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nALL PASS — V11 gate green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the gate**

Run: `uv run python scripts/_d58_v11_explain.py`
Expected: "ALL PASS — V11 gate green." Exit 0.

If **any** failure: **stop Phase B**. Do not proceed to Phase C. Re-open index design and re-spec. Record failure evidence (the printed plan) and present to user.

Capture the full output — reuse in the commit message (Phase D5).

---

# Phase C — Validation

## Task C1: Parity test against live Lakebase

- [ ] **Step 1: Run with LAKEBASE_HOST set**

Run: `uv run pytest src/tests/integration/test_funnel_mart_parity.py -v`

With `LAKEBASE_HOST` exported (local dev environment already has it set via `hf_taipy_app/.env`, but pytest does not auto-load .env — export explicitly if needed).

Expected: 6 tests pass — 4 exact + 2 greater-or-equal. If ANY exact fixture fails, the mart SQL has drifted from Python semantics; do not proceed. Debug against the V08/V10 fixtures in `scripts/_d58_v08_parity.py` + `scripts/_d58_v10_season.py` (these scripts remain on disk until Phase D for exactly this purpose).

---

## Task C2: Capture before/after numbers for commit message

- [ ] **Step 1: Capture timings**

Save the V11 output from Task V11 Step 2 to `/tmp/d58_v11_after.txt`. That's the "after" for the performance section of the commit message.

Record the "before" from the spec § Phase 0 table (already on disk in `/tmp/d58_explain.txt` — no re-run needed):
- `F-season-nogs`: 6,305 ms (Parallel Seq Scan)
- `F-season-gs`: 37,800 ms (exceeds 30 s timeout — query fails)

Record the correctness delta from the spec table:
- Selected-team `comp=11 team=217 season gs=All`: possessions 47,201 → 53,134 (+12.6 %), a3 13,812 → 32,367 (+134 %), shots 3,523 → 8,247 (+134 %), goals 570 → 1,336 (+134 %).

Save as `/tmp/d58_commit_notes.md` for direct paste into the Phase D5 commit message.

---

## Task C3: Deploy to staging

- [ ] **Step 1: Deploy without wait**

Run: `uv run python scripts/manage_space.py deploy staging --no-wait`
Expected: files uploaded, space build triggered, returns quickly (does NOT block on startup — per `feedback_deploy_then_rebuild.md`).

- [ ] **Step 2: Rebuild + poll status**

Run: `uv run python scripts/manage_space.py rebuild staging`
Expected: space transitions APP_STARTING → RUNNING within ~2 min (per the `startup_duration_timeout: 2m` in README.md YAML).

- [ ] **Step 3: Confirm RUNNING**

Run: `uv run python scripts/manage_space.py status staging`
Expected: `RUNNING`.

---

## Task V12: HF Space cold page-load gate

- [ ] **Step 1: Puppeteer-measure cold load**

Use Chrome (never Chromium — `feedback_chrome_not_chromium.md`). Open the staging URL, pick `competition=La Liga` → `team=Barcelona` (resolves to `comp=11 team=217` — the worst case).

In DevTools → Network, clear cache and hard-reload the Conversion Funnel page. Capture:
- Time-to-first-byte
- Full-page-load
- `performance.now()` delta between competition selection and stat-card render

Write measurements to `/tmp/d58_v12_cold_load.txt`.

**Gate:** full-page-load ≤ 3,000 ms on the worst-case `comp=11 team=217 season gs=None`.

- [ ] **Step 2: If FAIL**

Do not proceed. Investigate whether the remaining budget is dominated by mart query (unlikely — V11 showed <100 ms), network (HF Space → Lakebase), OAuth token refresh, or Plotly chart build. Present findings to user before retry.

- [ ] **Step 3: If PASS**

Append the measurement to `/tmp/d58_commit_notes.md`.

---

## Task C4: E2E Puppeteer scenarios

Verify all five user-flow scenarios on staging. Capture screenshots to `/tmp/d58_funnel_<scope>.png`.

| # | Scenario | Assertion |
|---|---|---|
| 1 | Navigate to Funnel (newly re-enabled in nav) | Stat cards render non-empty within 3 s |
| 2 | Switch competition → team (season, no gs) | Cards + chart re-render; `performance.now()` Δ < 1,500 ms |
| 3 | Select match (single-match mode) | Home/away team names in chart labels; Δ < 800 ms |
| 4 | Select game state = Winning | Stat cards update; Δ < 800 ms |
| 5 | Clear match back to "All" | Cards return to season values; no stale chart |

- [ ] **Step 1: Execute all 5 scenarios**

Save each screenshot as `/tmp/d58_funnel_<scope>.png`. Save any regressions or Taipy errors observed.

- [ ] **Step 2: All pass — Phase C green**

If all 5 pass AND V11 + V12 gates passed, Phase C is complete.

**⏸️ Approval Checkpoint 3 — end of Phase C.**

Present to user: V11 output, V12 cold-load number, parity test result, E2E screenshots. **Wait for explicit "commit" approval.** No `git add` / `git commit` yet.

---

# Phase D — Docs, cleanup, single commit, push, PR, merge

## Task D1: Documentation count updates

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`
- Modify: `docs/huggingface/org-card.md`

- [ ] **Step 1: Update counts**

For each of the three files, update the following counts:

| Metric | From | To |
|---|---|---|
| Gold-layer mart models | 36 | 37 |
| Synced tables | 37 | 38 |
| PG indexes | 63 | 66 |

Use Grep to locate each reference first (counts may not all appear in every file; only change what exists):

```
Grep: "36 (gold|mart)|37 (gold|mart)|37 synced|38 synced|63 (index|PG)|66 (index|PG)"
```

For each match, verify context before editing — some may be unrelated counts.

- [ ] **Step 2: Ruff format ignores (none needed — .md)**

No lint required.

---

## Task D2: C4 DSL regenerate (only if counts referenced)

**Files:**
- Modify (if applicable): `docs/c4/architecture.dsl`
- Regenerate: `docs/c4/architecture.html`

- [ ] **Step 1: Check whether the count appears in DSL**

Run Grep: `"37 synced|38 synced|63 index|66 index"` in `docs/c4/architecture.dsl`. If no match, skip.

If match: update the number in the DSL and regenerate the HTML per the `mad-scientist-skills:c4` skill workflow.

---

## Task D3: Remove D58 from TODO.md

**Files:**
- Modify: `TODO.md` — remove line 21 (the D58 entry in its entirety).

- [ ] **Step 1: Delete the D58 row**

Current line 21 is the full `| D58 | Conversion Funnel — ...` row. Remove the entire line. Do not strike through — `feedback_no_strikethrough_todo.md` requires full removal.

Verify the table still has a valid border row above and the next row (`D40d`) flows correctly.

---

## Task D4: Delete temp probe scripts

**Files:**
- Delete: `scripts/_d58_explain.py`
- Delete: `scripts/_d58_extra.py`
- Delete: `scripts/_d58_phase0.py`
- Delete: `scripts/_d58_v08_parity.py`
- Delete: `scripts/_d58_v10_baseline.py`
- Delete: `scripts/_d58_v10_season.py`
- Delete: `scripts/_d58_v11_explain.py` (created in Task V11)

- [ ] **Step 1: Remove files**

Use `rm` or `git rm` — these are untracked (Phase 0) or newly created (V11) files:

```bash
rm scripts/_d58_explain.py scripts/_d58_extra.py scripts/_d58_phase0.py scripts/_d58_v08_parity.py scripts/_d58_v10_baseline.py scripts/_d58_v10_season.py scripts/_d58_v11_explain.py
```

- [ ] **Step 2: Verify absence**

Run: `ls scripts/_d58_*.py 2>/dev/null || echo "no d58 temp files remain"`
Expected: "no d58 temp files remain".

---

## Task D5: Single commit

**⏸️ Approval Checkpoint 4 — commit authorization.**

Only proceed after user explicitly says "commit" (or equivalent action verb — per `feedback_one_commit_at_a_time.md`, "approved", "ok", "looks good" do NOT authorize a commit). If unclear, ask.

- [ ] **Step 1: Stage explicit file list (not `git add -A`)**

Stage only the intended files:

```bash
git add \
  dbt_project/models/marts/fct_funnel_stages_agg.sql \
  dbt_project/models/marts/_marts__models.yml \
  workflow-cards/wf-dbt-build.yaml \
  scripts/create_indexes.py \
  src/ingestion/refresh_synced_tables.py \
  src/tests/test_refresh_synced_tables.py \
  src/tests/test_conversion_funnel.py \
  src/tests/integration/test_funnel_mart_parity.py \
  src/tests/integration/__init__.py \
  terraform/modules/synced_tables/main.tf \
  hf_taipy_app/src/queries/funnel.py \
  hf_taipy_app/src/state/conversion_funnel.py \
  hf_taipy_app/src/main.py \
  docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md \
  docs/superpowers/plans/2026-04-18-d58-conversion-funnel.md \
  README.md \
  ARCHITECTURE.md \
  docs/huggingface/org-card.md \
  TODO.md
```

Add any `docs/c4/architecture.dsl` + `.html` only if Task D2 modified them.

Remove the deleted files from the index:

```bash
git add -u scripts/
```

(This picks up the 7 deletions without adding anything new under `scripts/`.)

Run `git status` to sanity-check the staged changeset before committing.

- [ ] **Step 2: Create the single commit**

Use a HEREDOC per `CLAUDE.md` git rules. Insert the concrete numbers from `/tmp/d58_commit_notes.md` (Task C2 + V11 output + V12 cold-load number):

```bash
git commit -m "$(cat <<'EOF'
perf: D58 — fct_funnel_stages_agg mart, mart-only query, re-enable Conversion Funnel (ADR-004)

Close D58: Conversion Funnel was disabled since 2026-04-10 due to timeouts from
the HF Space. Investigation revealed two bugs — perf AND silent data truncation.

## Perf fix (mart + indexes)
| query shape | before (ms) | after (ms) | plan |
|---|---|---|---|
| season team=217 no-gs      | 6,305   | <V11_NOGS>   | Seq→Index Scan |
| season team=217 gs=drawing | 37,800  | <V11_GS>     | Seq→BitmapOr+Index |
| single-match no-gs         | (fast)  | <V11_SM>     | Index Scan |
| single-match gs=drawing    | (fast)  | <V11_SMGS>   | Index Scan |

## Correctness fix (LIMIT 500000 removed — +134 % on A3/shots/goals for comp 11 / team 217)
| stage | old (broken) | new (correct) | hidden by truncation |
|---|---|---|---|
| possessions | 47,201 | 53,134 | +5,933 (+12.6 %) |
| a3 entries  | 13,812 | 32,367 | +18,555 (+134 %) |
| shots       | 3,523  | 8,247  | +4,724 (+134 %) |
| goals       | 570    | 1,336  | +766 (+134 %) |

## HF Space cold page-load (V12)
before: timeout (30 s statement_timeout)
after:  <V12_COLD_MS> ms

## Infrastructure
- 1 new dbt mart: fct_funnel_stages_agg (grain match_id,team_id,game_state;
  ~12,145 rows; liquid_clustered_by competition_id)
- 3 new composite PG indexes (63 → 66):
  idx_funnel_agg_match, idx_funnel_agg_comp_team_gs, idx_funnel_agg_comp_opp_gs
- 1 new synced table (37 → 38): fct_funnel_stages_agg_synced
- Contract enforced; dbt_utils.unique_combination_of_columns asserts grain

## Tests
- V08 single-match parity: 4/4 zero delta (Phase 0)
- V10 integration parity: 6/6 pass (4 zero-delta, 2 correctness-fix)
- TestRollupStages: 8 tests (incl. V01 straddler regression guard)
- TestFetchFunnelAggSQL: 6 tests (incl. no-LIMIT guard)
- TestFetchMatchMetaSingle: 1 test (LIMIT 1, not 200)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Replace `<V11_NOGS>`, `<V11_GS>`, `<V11_SM>`, `<V11_SMGS>`, `<V12_COLD_MS>` with the actual numbers captured in C2 + V12.

- [ ] **Step 3: Verify commit created**

Run: `git log -1 --stat`
Expected: the commit shows roughly 19–20 files changed. Line count should match what was staged. If the pre-commit hook fails, fix the issue and create a **new** commit — never `--amend` per `CLAUDE.md` git rules.

---

## Task D6: Push

**⏸️ Approval Checkpoint 5 — push authorization.**

Only proceed on explicit user "push" (or equivalent action verb).

- [ ] **Step 1: Push the branch**

Run: `git push -u origin perf/conversion-funnel`
Expected: pushed to GitHub, returns the GitHub URL of the branch.

---

## Task D7: Create PR

**⏸️ Approval Checkpoint 6 — PR authorization.**

Only proceed on explicit user "create PR" (or equivalent).

- [ ] **Step 1: Open PR**

Run:

```bash
gh pr create --title "perf: D58 Conversion Funnel — mart + correctness fix" --body "$(cat <<'EOF'
## Summary
- Adds `fct_funnel_stages_agg` mart (ADR-004 pattern) at (match_id, team_id, game_state) grain — ~12,145 rows; mart-only Taipy query eliminates the 37,800 ms Parallel Seq Scan + Nested Loop on `fct_action_values` (9.5 M rows) that exceeded the app statement_timeout.
- Removes the silent `LIMIT 500000` truncation in the old query — A3 entries / shots / goals were under-reported by >50 % for prolific teams (comp 11 / team 217: +134 % on every shot-stage metric; +12.6 % on possessions).
- Re-enables the Conversion Funnel page in the HF Space nav (disabled since 2026-04-10 per TODO D58).

## Infrastructure
- 1 new dbt mart (gold marts: 36 → 37), contract-enforced, `dbt_utils.unique_combination_of_columns` grain
- 3 new composite PG indexes (63 → 66): `idx_funnel_agg_match`, `idx_funnel_agg_comp_team_gs`, `idx_funnel_agg_comp_opp_gs`
- 1 new Lakebase synced table (37 → 38): `fct_funnel_stages_agg_synced`

## Test plan
- [x] Phase 0: 14 live verifications (V01–V16) — spec § Phase 0
- [x] V08 single-match parity (4 scenarios, zero delta)
- [x] V10 integration parity (6 fixtures, hardcoded oracles)
- [x] V11 EXPLAIN gate (Index Scan on all 4 mart query shapes, <100 ms worst case)
- [x] V12 HF Space cold page-load on staging (<3 s on worst-case comp 11 team 217)
- [x] E2E Puppeteer (5 scenarios — nav, competition→team, match select, gs select, match clear)

Spec: [`docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md`](docs/superpowers/specs/2026-04-17-d58-funnel-perf-design.md)
Plan: [`docs/superpowers/plans/2026-04-18-d58-conversion-funnel.md`](docs/superpowers/plans/2026-04-18-d58-conversion-funnel.md)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL returned.

- [ ] **Step 2: Report URL to user**

---

## Task D8: Merge

**⏸️ Approval Checkpoint 7 — merge authorization.**

Wait for CI green on the PR AND user explicit "merge" (or equivalent).

- [ ] **Step 1: Squash-merge**

Run: `gh pr merge <PR_NUMBER> --squash --delete-branch`
Expected: merged to `main`, branch deleted locally and remotely.

- [ ] **Step 2: Verify main has the commit**

Run: `git checkout main && git pull && git log -1 --stat`
Expected: the D58 squash commit is at HEAD with the same 19–20 files changed.

- [ ] **Step 3: Post-merge infra sanity**

Run: `uv run python scripts/run_lakebase_grants.py --verify`
Expected: `OK: SP <app_id> has SELECT on all 38 synced tables`.

Run: `uv run python scripts/manage_space.py status production` (if the user deploys production after merge — that is a separate user action).

---

# Self-review

## Spec coverage

| Spec section | Task(s) that cover it |
|---|---|
| Mart SQL (`fct_funnel_stages_agg.sql`) | A1 |
| Mart contract block | A2 |
| Workflow card `outputs.tables` entry | A3 |
| PG index definitions + verify queries | A4 |
| `SYNCED_TABLES` registration | A5 |
| `test_refresh_synced_tables.py` count bump | A6 |
| Taipy query rewrite (`fetch_funnel_agg`, `rollup_stages`, `_fetch_match_meta`, `compute_conversion_rates`) | A7 |
| Unit test rewrite (`TestRollupStages`, `TestFetchFunnelAggSQL`, `TestFetchMatchMetaSingle`, kept `TestConversionRates` + `TestFunnelChart`) | A8 |
| Taipy state rewrite (`cf_refresh`) | A9 |
| Page re-enable (imports + uncomment) | A10 |
| Parity integration test (6 V10 oracles) | A11 |
| TF resource block | A12 |
| dbt build + test | B1, B2 |
| UI create synced table | B3 |
| TF import + zero drift | B4 |
| Workspace grants | B5 |
| Lakebase PG SELECT grants | B6 |
| PG index creation + verify | B7 |
| V11 EXPLAIN gate | V11 |
| Parity test live | C1 |
| Before/after metric capture | C2 |
| Staging deploy | C3 |
| V12 cold load | V12 |
| E2E Puppeteer | C4 |
| Docs counts | D1 |
| C4 DSL | D2 |
| TODO removal | D3 |
| Temp file cleanup | D4 |
| Single commit | D5 |
| Push | D6 |
| PR | D7 |
| Merge | D8 |

No spec section left uncovered.

## Placeholder scan

Red flags scanned: no "TBD", no "implement later", no "similar to Task N". Every SQL and Python block is concrete. The only templated text is the commit message numeric placeholders (`<V11_NOGS>`, etc.) which are **explicit** — they have named capture points in C2/V11/V12 and the instruction says "replace with the actual numbers captured".

## Type consistency

- `fetch_funnel_agg(comp_id, team_id, match_id=None, game_state=None) -> pd.DataFrame` — used identically in Task A7 (definition), Task A8 (SQL capture tests), Task A11 (parity test), Task A9 (state-module call site).
- `rollup_stages(rows, *, gs_filtered) -> dict[str, int]` — same signature across A7, A8, A9, A11.
- `_fetch_match_meta(comp_id, team_id, match_id) -> pd.DataFrame` — same in A7, A8, A9.
- `compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]` — unchanged contract kept.
- Mart column list in A1 SQL, A2 contract, A7 cols string, A11 fixture access — all 11 columns consistent.
- Index names match between A4 (definition), A12 spec mention, and V11 script's plan-text inspection.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-18-d58-conversion-funnel.md`. Two execution options:

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using `executing-plans`, with approval checkpoints at the four per-phase gates.

**Which approach?**
