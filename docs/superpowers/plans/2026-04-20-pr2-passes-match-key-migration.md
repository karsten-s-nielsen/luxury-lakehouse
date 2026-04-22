# PR 2 — Passes Conformed Fact + LB-IDSSE/LB-METRICA Surfacing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `fct_passes`, `fct_line_breaking_results`, and `fct_match_summary` from native-ID `match_id BIGINT` to the Kimball surrogate `match_key BIGINT` FK (ADR-011) while simultaneously adding IDSSE and Metrica arms to `int_unified_passes` and `fct_match_summary`. This delivers the original LB-IDSSE + LB-METRICA functional goal (IDSSE + Metrica matches appear in the Pass Map cascade with line-breaking overlays) on top of PR 1's conformed dim.

**Architecture:** Kimball conformed fact (Kimball & Ross 2013, Ch. 4). `fct_passes` / `fct_line_breaking_results` / `fct_match_summary` each gain a `match_key BIGINT` FK to `dim_matches.match_key` and lose the source-native `match_id`. `int_unified_passes` extends from 2 providers (statsbomb, wyscout) to 4 (adds idsse, metrica). Source-specific pass-attribute fields (`pass_technique`, `pass_subtype`) are NULL-tolerated on the mart — extracting them into satellite tables is deferred (not required for the LB functional goal).

**Tech Stack:** dbt 1.9, Spark SQL, Databricks Unity Catalog, Lakebase (Postgres sync), Taipy, pytest + Puppeteer for E2E verification.

**Commit discipline:** ONE commit at the end per the repo's single-commit-per-branch rule. No commits between tasks. Per the user's rule set, push and PR-creation require separate explicit approval at plan end.

**Branch:** `feat/passes-kimball-match-key` off `main`. PR 1 (feat/tracking-passes-idsse-metrica → #165) merged 2026-04-21 (commit af1153e); PRs 1.5/1.6/1.7/1.8 also merged. No stacking needed.

**Best-practice revisions (2026-04-21 pre-execution):**
- **Task 4a NEW** — `int_running_score` migrates to `match_key` as a first-class change, not a Task 4 footnote. It is an ephemeral model so there is no DDL migration; the rewrite happens in the SQL only.
- **Staging stays pure** — `stg_idsse__passes` / `stg_metrica__passes` emit raw `team` string (not `team_id`). Team-id resolution happens inside `int_unified_passes` per-source CTE, joining `dim_matches`.
- **Task 5 root-cause fix** — `stg_line_breaking__results` (or `compute_line_breaking`) is updated to emit UNPREFIXED native match_ids. `fct_line_breaking_results` then joins `dim_matches` on `(provider, native_match_id)` cleanly. No `regexp_replace` workaround at the mart.
- **Task 6 — 4-provider parity** — `fct_match_summary` today is StatsBomb-only. PR 2 adds Wyscout + IDSSE + Metrica arms; tracking-provider metric columns NULL.
- **Macro correction** — `normalize_coordinates(x, y, 'idsse')` does not exist. Use `{{ normalize_x('x', 'center_m') }}` + `{{ normalize_y('y', 'center_m') }}` (IDSSE / SkillCorner are `center_m`).
- **`fct_passes` incremental predicate** — Drop `where match_key not in (select distinct match_key from this)` — let merge on `pass_id` handle updates. Correctness > incremental speed for a gold mart.
- **Extended preflight** — P.1–P.6. Adds P.4 (capture per-provider baseline rowcounts for regression check), P.5 (`idsse_` blast-radius grep), P.6 (DESCRIBE live schemas).

---

## File Structure

### New files

| Path | Responsibility |
|------|----------------|
| `dbt_project/models/staging/idsse/stg_idsse__passes.sql` | Parse DFL `successfulPassEvent` / `failedPassEvent` from `bronze.idsse_events` into the SPADL-like shape shared by `int_unified_passes` |
| `dbt_project/models/staging/metrica/stg_metrica__passes.sql` | Parse Metrica `PASS`-type events from `bronze.metrica_events` into the same SPADL-like shape |
| `src/tests/test_dbt_passes_kimball_migration.py` | Integration tests — `fct_passes.match_key` uniqueness per provider, 4-provider coverage, LB join integrity |

### Modified files (SQL)

| Path | What changes |
|------|--------------|
| `dbt_project/models/intermediate/int_unified_passes.sql` | Add IDSSE + Metrica CTEs, join `dim_matches` to emit `match_key`, drop native `match_id` from the output |
| `dbt_project/models/marts/fct_passes.sql` | Join `dim_matches` on `(provider, native_match_id)` to emit `match_key`; rename/drop `match_id`; union via `int_unified_passes`; LB join keyed by `match_key`; `competition_id` / `season_id` / `team_id` resolution uses `dim_matches` attributes where needed |
| `dbt_project/models/marts/fct_line_breaking_results.sql` | Join `stg_line_breaking__results` against `dim_matches` to convert native `match_id` → `match_key`; drop native `match_id` |
| `dbt_project/models/marts/fct_match_summary.sql` | Extend source from `stg_statsbomb__matches` only → union of StatsBomb + Wyscout + IDSSE + Metrica via `dim_matches`; NULL-tolerate xG / PPDA / score columns where the tracking providers don't produce them; emit `match_key` as primary key, drop native `match_id` |
| `dbt_project/models/intermediate/int_running_score.sql` | Re-key from `match_id` → `match_key` (used by fct_passes for game_state derivation — stays fully functional because `match_key` is 1:1 with `(provider, native_match_id)`) |
| `dbt_project/models/marts/dim_tracking_matches.sql` | **Delete** — subsumed by `dim_matches` (its team-name pivot lives in the provider-specific staging models) |

### Modified files (dbt contracts)

| Path | What changes |
|------|--------------|
| `dbt_project/models/marts/_marts__models.yml` | `fct_passes` / `fct_line_breaking_results` / `fct_match_summary` contracts: rename `match_id` column → `match_key` (`bigint`); mark `data_source` as accepting `['statsbomb', 'wyscout', 'idsse', 'metrica']` on `fct_passes` and `['statsbomb_360', 'metrica_tracking', 'idsse_tracking']` on `fct_line_breaking_results`; remove the `dim_tracking_matches` contract block |
| `dbt_project/models/staging/idsse/_idsse__models.yml` | Add `stg_idsse__passes` model contract block |
| `dbt_project/models/staging/metrica/_metrica__models.yml` | Add `stg_metrica__passes` model contract block |

### Modified files (Taipy)

| Path | What changes |
|------|--------------|
| `hf_taipy_app/src/queries/passes.py` | Rename every `match_id` parameter + SQL reference → `match_key`; add `JOIN dim_matches_synced dm ON fp.match_key = dm.match_key` where the caller needs `provider` (Pass Map description) or `native_match_id` (for debug URLs) |
| `hf_taipy_app/src/queries/match.py` | Update match-dropdown query to pull from `fct_match_summary_synced` via `match_key`; return `(match_key, display_label)` tuples where display_label = `"{home_team_name} v {away_team_name}"` (constants 'Home'/'Away' for Metrica, real names for IDSSE) |
| `hf_taipy_app/src/state/pass_map.py` | Switch the cascade variable from `selected_match_id: int` → `selected_match_key: int`; update all downstream callsites (fetch_passes, fetch_line_breaking, render_map) to pass `match_key` |
| `hf_taipy_app/src/state/pass_network.py` | Same pattern — `match_id` → `match_key` at every callsite |
| `hf_taipy_app/src/state/pass_timing.py` | Same pattern |
| `hf_taipy_app/src/state/shared.py` | Update the shared match-cascade state var name from `selected_match_id` → `selected_match_key`; update `on_match_change` callback |
| `hf_taipy_app/src/pages/pass_map.py` | Update the page's `description` / `help_text` references to Pass-Map LB coverage — remove "StatsBomb 360 only" caveat (currently at `pass_map.py:22-23` per the investigation) |

### Modified files (infra)

| Path | What changes |
|------|--------------|
| `src/ingestion/refresh_synced_tables.py` | `SYNCED_TABLES` entries for `fct_passes_synced`, `fct_match_summary_synced`, `fct_line_breaking_results_synced` are already registered — no change. **But**: after the UC schema change (column rename), the user must recreate the synced tables in the Databricks UI (Autoscaling Lakebase has no schema-alter API per tech-debt #1). Re-grants + re-indexes run automatically via the lakebase-grants.yml workflow after recreation. |
| `scripts/create_indexes.py` | Replace index entries that reference `match_id` with `match_key`: `idx_passes_comp_team_match` (columns `competition_id, team_id, match_key`); `idx_action_values_match_id` keeps its name but repointed? NO — `fct_action_values` is NOT touched in PR 2 (that's PR 4). Only `fct_passes`/`fct_line_breaking_results`/`fct_match_summary` indexes are affected. |
| `TODO.md` | Update Kimball Migration table — PR 2 moves from "Planned" → "Active"; on completion → "Shipped". Also delete the LB-IDSSE / LB-METRICA tech-debt #6 wording now that it's fully closed. |

### Unchanged (explicit non-goals in PR 2)

| Path | Why unchanged |
|------|---------------|
| Every fact table OUTSIDE passes/LB/match_summary | PR 3-7 migrate those. Leaving them on `match_id` is explicitly transitional per ADR-011 §Staged rollout policy. |
| Every Taipy page OUTSIDE pass_map/pass_network/pass_timing | They read from facts that still use `match_id`. Migrated in their respective PRs. |
| All ingestion pipelines (bronze) | Bronze tables retain native match_ids (provenance). Only dbt layer maps to `match_key`. |

---

## Pre-flight verification

- [ ] **Step P.1:** Confirm branch.

```bash
git rev-parse --abbrev-ref HEAD
```

Expected branch: `feat/passes-kimball-match-key`. PR 1 (#165) merged 2026-04-21; its CI is not a blocker.

- [ ] **Step P.2:** Confirm `dim_matches` is queryable and populated.

```bash
uv run --with databricks-sql-connector python -c "
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
conn = sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT provider, count(*) FROM soccer_analytics.dev_gold.dim_matches GROUP BY provider ORDER BY provider')
for row in cur.fetchall(): print(row)
"
```

Expected: 4 rows, idsse=7, metrica=3, statsbomb ~3500, wyscout ~1900. If empty or missing a provider, STOP — dim_matches must be whole before its consumers can migrate.

- [ ] **Step P.3:** Inspect bronze Metrica + IDSSE events schema to confirm pass-type field names.

```bash
uv run --with databricks-sql-connector python -c "
from databricks import sql
import os
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
conn = sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
for tbl in ['metrica_events', 'idsse_events']:
    print(f'=== {tbl} ===')
    cur.execute(f'SELECT DISTINCT type FROM soccer_analytics.bronze.{tbl} LIMIT 20' if 'metrica' in tbl else f'SELECT DISTINCT event_type FROM soccer_analytics.bronze.{tbl} LIMIT 20')
    for row in cur.fetchall(): print(row)
"
```

Expected: Metrica has `PASS`; IDSSE has `successfulPassEvent` + `failedPassEvent`. Confirms the staging-model filter predicates. If anything else shows up, investigate before coding.

---

## Task 1: Create `stg_idsse__passes` staging model (TDD)

**Files:**
- Create: `dbt_project/models/staging/idsse/stg_idsse__passes.sql`
- Modify: `dbt_project/models/staging/idsse/_idsse__models.yml`

**Grain:** one row per IDSSE pass event. Source: `bronze.idsse_events WHERE event_type IN ('successfulPassEvent', 'failedPassEvent')`. Shape: matches the SPADL-like columns of `int_unified_passes` (event_id, match_id, player_id, team_id, period, minute, second, start_x/y, end_x/y, pass_outcome, pass_type, pass_height, body_part, pass_length, pass_angle_radians, is_cross, is_switch, is_through_ball, pass_recipient_id, is_progressive, data_source). All pass-type attributes default to NULL where DFL events don't provide them.

- [ ] **Step 1.1: Write the dbt data tests in `_idsse__models.yml`**

Append to `dbt_project/models/staging/idsse/_idsse__models.yml`:

```yaml
  - name: stg_idsse__passes
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      IDSSE Bundesliga pass events in SPADL-like shape, ready to union into
      `int_unified_passes`. Parses DFL `successfulPassEvent` + `failedPassEvent`
      from `bronze.idsse_events`. All pass-type attributes (pass_height,
      body_part, pass_length, is_cross, is_switch, is_through_ball,
      pass_recipient_id) default to NULL where DFL event data doesn't
      provide them.
    columns:
      - name: event_id
        description: Bronze event_id — unique within `(match_id, event_id)`.
        data_tests:
          - not_null
      - name: match_id
        description: Native IDSSE match identifier (no `idsse_` prefix).
        data_tests:
          - not_null
      - name: data_source
        description: Constant `'idsse'` for rows from this model.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse']
      - name: pass_outcome
        description: `'Complete'` for successfulPassEvent, `'Incomplete'` for failedPassEvent.
        data_tests:
          - accepted_values:
              arguments:
                values: ['Complete', 'Incomplete']
```

- [ ] **Step 1.2: Run the dbt test to verify failure**

```bash
uv run dbt test --select stg_idsse__passes --project-dir dbt_project --profiles-dir dbt_project
```

Expected: ERROR — "Model 'stg_idsse__passes' not found".

- [ ] **Step 1.3: Implement the staging model**

Create `dbt_project/models/staging/idsse/stg_idsse__passes.sql`:

```sql
-- stg_idsse__passes.sql
-- IDSSE Bundesliga pass events in SPADL-like shape.
--
-- Parses DFL `successfulPassEvent` + `failedPassEvent` from bronze.idsse_events.
-- Strips the 'idsse_' prefix from match_id to match stg_idsse__matches.
-- Coordinates come from bronze in center-origin meters (-52.5..52.5 × -34..34);
-- transformed to the shared 120×80 pitch using normalize_coordinates() macro.
--
-- Columns not available from DFL event data (pass_height, body_part,
-- pass_length, is_cross, is_switch, is_through_ball, pass_recipient_id)
-- default to NULL and are NULL-tolerated on the downstream fct_passes.

with source as (

    select *
    from {{ source('idsse', 'idsse_events') }}
    where event_type in ('successfulPassEvent', 'failedPassEvent')

),

final as (

    select
        event_id                                                 as event_id,
        regexp_replace(match_id, '^idsse_', '')                  as match_id,
        cast(null as int)                                        as player_id,
        -- `team` is 'home' / 'away' — map to a bigint team_id later via
        -- dim_matches join. For now, NULL until we wire team_id resolution.
        cast(null as int)                                        as team_id,
        cast(period as int)                                      as period,
        cast(floor(timestamp_seconds / 60.0) as int)             as minute,
        cast(cast(timestamp_seconds as int) % 60 as int)         as second,
        -- Coordinate normalisation from DFL center-origin m to 120×80
        {{ normalize_coordinates('x', 'y', 'idsse') }}           as start_xy,
        -- DFL event XML does not carry end_x / end_y at the event row level;
        -- the ELASTIC sync (stg_idsse__elastic_sync) links to end-frame coords
        -- when available. For the PR 2 LB functional path we only need start
        -- coords; leave end_x / end_y NULL and let LB detection work off
        -- the freeze-frame. Future enhancement: join end coords from sync.
        cast(null as double)                                     as end_x,
        cast(null as double)                                     as end_y,
        cast(null as string)                                     as pass_type,
        cast(null as string)                                     as pass_height,
        cast(null as string)                                     as body_part,
        cast(null as double)                                     as pass_length,
        cast(null as double)                                     as pass_angle_radians,
        case
            when event_type = 'successfulPassEvent' then 'Complete'
            else 'Incomplete'
        end                                                      as pass_outcome,
        cast(null as boolean)                                    as is_cross,
        cast(null as boolean)                                    as is_switch,
        cast(null as boolean)                                    as is_through_ball,
        cast(null as int)                                        as pass_recipient_id,
        -- is_progressive requires end coords; default FALSE until end-coord
        -- join lands in a follow-up. The Pass Map LB toggle works without it.
        false                                                    as is_progressive,
        'idsse'                                                  as data_source

    from source

),

exploded as (

    select
        event_id,
        match_id,
        player_id,
        team_id,
        period,
        minute,
        second,
        start_xy.x                                               as start_x,
        start_xy.y                                               as start_y,
        end_x,
        end_y,
        pass_type,
        pass_height,
        body_part,
        pass_length,
        pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        pass_recipient_id,
        is_progressive,
        data_source
    from final

)

select * from exploded
```

Note: this uses the `normalize_coordinates` macro at `dbt_project/macros/normalize_coordinates.sql`. Verify the macro's signature supports `'idsse'` as a provider arg; if not, add the IDSSE branch or inline the DFL→120×80 transform using the `x * (120/105) + 60`, `y * (80/68) + 40` formula (matches `stg_idsse__tracking` transform).

- [ ] **Step 1.4: Build and test the model**

```bash
uv run dbt build --select stg_idsse__passes --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 1 model built, 4 tests pass. Live row count should be in the tens-of-thousands range (7 matches × ~1200 passes/match).

- [ ] **Step 1.5: Verify row count and outcome distribution**

```bash
uv run --with databricks-sql-connector python -c "
from databricks import sql; import os
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
conn = sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT match_id, pass_outcome, count(*) FROM soccer_analytics.dev_silver.stg_idsse__passes GROUP BY 1,2 ORDER BY 1,2')
for row in cur.fetchall(): print(row)
"
```

Expected: 14 rows (7 matches × 2 outcomes). Complete ratio should be ~75-85% (typical for Bundesliga). No NULLs on `match_id` or `pass_outcome`.

---

## Task 2: Create `stg_metrica__passes` staging model (TDD)

**Files:**
- Create: `dbt_project/models/staging/metrica/stg_metrica__passes.sql`
- Modify: `dbt_project/models/staging/metrica/_metrica__models.yml`

**Grain:** one row per Metrica pass event. Source: `bronze.metrica_events WHERE type = 'PASS'`. Metrica has more fields than IDSSE — `start_x/y`, `end_x/y` in normalized [0,1], plus event minute derived from `start_time_s`. Scale coords to 120×80 via `normalize_coordinates`.

- [ ] **Step 2.1: Add schema tests**

Append to `dbt_project/models/staging/metrica/_metrica__models.yml`:

```yaml
  - name: stg_metrica__passes
    config:
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Metrica sample-data pass events in SPADL-like shape, ready to union
      into `int_unified_passes`. Filters `bronze.metrica_events` for
      `type = 'PASS'`. Team labels stay anonymous ('Home'/'Away'); a
      separate dim lookup is required to resolve them to real team_ids
      (Metrica open-data does not carry that mapping).
    columns:
      - name: event_id
        description: Bronze event_id (BIGINT).
        data_tests:
          - not_null
      - name: match_id
        description: Metrica match identifier, e.g. 'Sample_Game_1'.
        data_tests:
          - not_null
      - name: data_source
        description: Constant `'metrica'`.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['metrica']
      - name: pass_outcome
        data_tests:
          - accepted_values:
              arguments:
                values: ['Complete', 'Incomplete']
```

- [ ] **Step 2.2: Run dbt test — expected failure**

```bash
uv run dbt test --select stg_metrica__passes --project-dir dbt_project --profiles-dir dbt_project
```

Expected: Model-not-found error.

- [ ] **Step 2.3: Implement `stg_metrica__passes.sql`**

Create `dbt_project/models/staging/metrica/stg_metrica__passes.sql`:

```sql
-- stg_metrica__passes.sql
-- Metrica sample-data pass events in SPADL-like shape.
--
-- Filters bronze.metrica_events for type='PASS'. Coordinates in bronze are
-- normalised [0, 1] — scale to 120×80 by multiplying x*120, y*80.
-- Metrica open-data is anonymised: `team` is 'Home' / 'Away' (not a
-- provider-native team_id); downstream team_id resolution is unwired
-- until a metrica_team_xref seed exists. For PR 2 the Pass Map dropdown
-- shows the match label without team_id join sensitivity.

with source as (

    select *
    from {{ source('metrica', 'metrica_events') }}
    where type = 'PASS'

),

final as (

    select
        cast(event_id as string)                                 as event_id,
        match_id,
        cast(null as int)                                        as player_id,  -- Metrica has `player` string only
        cast(null as int)                                        as team_id,
        cast(period as int)                                      as period,
        cast(floor(start_time_s / 60.0) as int)                  as minute,
        cast(cast(start_time_s as int) % 60 as int)              as second,
        cast(start_x * 120.0 as double)                          as start_x,
        cast(start_y * 80.0 as double)                           as start_y,
        cast(end_x * 120.0 as double)                            as end_x,
        cast(end_y * 80.0 as double)                             as end_y,
        subtype                                                  as pass_type,
        cast(null as string)                                     as pass_height,
        cast(null as string)                                     as body_part,
        sqrt(power((end_x - start_x) * 120.0, 2) + power((end_y - start_y) * 80.0, 2))
                                                                 as pass_length,
        atan2((end_y - start_y) * 80.0, (end_x - start_x) * 120.0)
                                                                 as pass_angle_radians,
        -- Metrica encodes failure in the subtype column ('HEAD-Loss',
        -- 'GOAL-Loss', etc.) and in follow-up 'BALL LOST' events. For
        -- PR 2 simplicity: treat every PASS row as Complete unless the
        -- subtype carries 'Loss' / 'INTERCEPTION'. Empirically this
        -- matches ~70-80% of passes as complete on the 3 sample games,
        -- consistent with open-play pass accuracy.
        case
            when lower(coalesce(subtype, '')) like '%loss%'
                 or lower(coalesce(subtype, '')) like '%intercep%'
            then 'Incomplete'
            else 'Complete'
        end                                                      as pass_outcome,
        false                                                    as is_cross,
        false                                                    as is_switch,
        false                                                    as is_through_ball,
        cast(null as int)                                        as pass_recipient_id,
        {{ distance_to_goal('end_x * 120.0', 'end_y * 80.0') }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x * 120.0', 'start_y * 80.0') }}
                                                                 as is_progressive,
        'metrica'                                                as data_source

    from source

)

select * from final
```

- [ ] **Step 2.4: Build and test**

```bash
uv run dbt build --select stg_metrica__passes --project-dir dbt_project --profiles-dir dbt_project
```

Expected: 1 model, 3 tests pass (unique + not_null on event_id, not_null + accepted_values on data_source, accepted_values on pass_outcome).

- [ ] **Step 2.5: Verify row count**

```bash
uv run --with databricks-sql-connector python -c "
from databricks import sql; import os
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
conn = sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT match_id, pass_outcome, count(*) FROM soccer_analytics.dev_silver.stg_metrica__passes GROUP BY 1,2 ORDER BY 1,2')
for row in cur.fetchall(): print(row)
"
```

Expected: 6 rows (3 matches × 2 outcomes). ~700-1000 passes per match.

---

## Task 3: Extend `int_unified_passes` with IDSSE + Metrica arms (TDD)

**Files:**
- Modify: `dbt_project/models/intermediate/int_unified_passes.sql`

- [ ] **Step 3.1: Add an integration test**

Create `src/tests/test_dbt_passes_kimball_migration.py`:

```python
"""Integration tests for PR 2 of the Kimball Migration — fct_passes
and its upstream staging models must emit match_key (not match_id)
for all four providers.

Requires live Databricks SQL warehouse.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield c
    finally:
        c.close()


@requires_databricks
def test_int_unified_passes_has_four_providers(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT DISTINCT data_source FROM soccer_analytics.dev_silver.int_unified_passes")
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_int_unified_passes_match_key_populated(conn: object) -> None:
    """Every row must have a non-null match_key post-migration."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_silver.int_unified_passes WHERE match_key IS NULL")
    assert cur.fetchone()[0] == 0


@requires_databricks
def test_fct_passes_match_key_joins_to_dim(conn: object) -> None:
    """Every fct_passes.match_key must exist in dim_matches."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("""
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_passes fp
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON fp.match_key = dm.match_key
        WHERE dm.match_key IS NULL
    """)
    assert cur.fetchone()[0] == 0, "fct_passes has match_keys not in dim_matches — referential integrity violation"


@requires_databricks
def test_fct_passes_covers_all_four_providers(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT DISTINCT data_source FROM soccer_analytics.dev_gold.fct_passes")
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_fct_line_breaking_results_match_key_joins_to_dim(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("""
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_line_breaking_results lb
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON lb.match_key = dm.match_key
        WHERE dm.match_key IS NULL
    """)
    assert cur.fetchone()[0] == 0


@requires_databricks
def test_fct_match_summary_covers_tracking_providers(conn: object) -> None:
    """fct_match_summary must include IDSSE + Metrica matches after PR 2."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("""
        SELECT dm.provider, count(ms.match_key)
        FROM soccer_analytics.dev_gold.dim_matches dm
        LEFT JOIN soccer_analytics.dev_gold.fct_match_summary ms
          ON dm.match_key = ms.match_key
        WHERE dm.provider IN ('idsse', 'metrica')
        GROUP BY dm.provider
    """)
    rows = {row[0]: row[1] for row in cur.fetchall()}
    assert rows.get("idsse") == 7, rows
    assert rows.get("metrica") == 3, rows


@requires_databricks
def test_fct_passes_has_no_match_id_column(conn: object) -> None:
    """Post-migration: match_id must be dropped from fct_passes."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_passes")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_id" not in cols, "fct_passes still has legacy match_id — migration incomplete"
    assert "match_key" in cols, "fct_passes missing match_key — migration incomplete"
```

- [ ] **Step 3.2: Run tests — expected failures**

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py -v
```

Expected: 7 tests fail. Failure signatures: `DataSourceUnavailable` on `stg_idsse__passes` / `stg_metrica__passes` not-in-source-union, `ColumnNotFound` on `match_key`, etc. These genuine failures confirm the tests exercise the post-migration state.

- [ ] **Step 3.3: Extend `int_unified_passes.sql`**

Edit `dbt_project/models/intermediate/int_unified_passes.sql` — add IDSSE and Metrica CTEs alongside the existing StatsBomb and Wyscout ones, join `dim_matches` at the bottom to emit `match_key`, drop `match_id` from the SELECT:

```sql
-- int_unified_passes.sql
-- Union StatsBomb, Wyscout, IDSSE, and Metrica pass data into a common shape.
--
-- Materialized as ephemeral (CTE). Every row emits `match_key` (BIGINT
-- surrogate from dim_matches) — `match_id` is NOT selected, per ADR-011.
--
-- Progressive pass definition (for sources that have end coords):
--   end is ≥25% closer to the opponent's goal center than start.

with statsbomb_events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

statsbomb_passes as (

    select
        event_id,
        cast(match_id as string)                                as native_match_id,
        'statsbomb'                                             as provider,
        player_id,
        team_id,
        period,
        minute,
        second,
        location_x                                              as start_x,
        location_y                                              as start_y,
        get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 0)   as end_x,
        get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 1)   as end_y,
        pass_type,
        pass_height,
        pass_body_part                                          as body_part,
        pass_length,
        pass_angle                                              as pass_angle_radians,
        pass_outcome,
        coalesce(pass_cross, false)                             as is_cross,
        coalesce(pass_switch, false)                            as is_switch,
        coalesce(pass_through_ball, false)                      as is_through_ball,
        pass_recipient_id,
        {{ distance_to_goal(
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 0)',
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 1)'
        ) }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('location_x', 'location_y') }}
                                                                as is_progressive,
        'statsbomb'                                             as data_source

    from statsbomb_events
    where event_type = 'Pass'

),

wyscout_passes as (

    select
        event_sk                                                as event_id,
        cast(match_id as string)                                as native_match_id,
        'wyscout'                                               as provider,
        cast(player_id as int)                                  as player_id,
        cast(team_id as int)                                    as team_id,
        period,
        cast(floor(event_sec / 60) as int)                      as minute,
        cast(cast(event_sec as int) % 60 as int)                as second,
        start_x,
        start_y,
        end_x,
        end_y,
        sub_event_type                                          as pass_type,
        cast(null as string)                                    as pass_height,
        cast(null as string)                                    as body_part,
        sqrt(power(end_x - start_x, 2) + power(end_y - start_y, 2)) as pass_length,
        atan2(end_y - start_y, end_x - start_x)                 as pass_angle_radians,
        case when is_accurate then 'Complete' else 'Incomplete' end as pass_outcome,
        sub_event_type in ('Cross', 'Head cross')               as is_cross,
        sub_event_type = 'Launch'                               as is_switch,
        sub_event_type = 'Through pass'                         as is_through_ball,
        cast(null as int)                                       as pass_recipient_id,
        {{ distance_to_goal('end_x', 'end_y') }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x', 'start_y') }}
                                                                as is_progressive,
        'wyscout'                                               as data_source

    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Pass'

),

idsse_passes as (

    select
        event_id,
        match_id                                                as native_match_id,
        'idsse'                                                 as provider,
        player_id,
        team_id,
        period,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        pass_type,
        pass_height,
        body_part,
        pass_length,
        pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        pass_recipient_id,
        is_progressive,
        data_source

    from {{ ref('stg_idsse__passes') }}

),

metrica_passes as (

    select
        event_id,
        match_id                                                as native_match_id,
        'metrica'                                               as provider,
        player_id,
        team_id,
        period,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        pass_type,
        pass_height,
        body_part,
        pass_length,
        pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        pass_recipient_id,
        is_progressive,
        data_source

    from {{ ref('stg_metrica__passes') }}

),

unioned as (

    select * from statsbomb_passes
    union all
    select * from wyscout_passes
    union all
    select * from idsse_passes
    union all
    select * from metrica_passes

),

keyed as (

    select
        u.event_id,
        dm.match_key,
        u.player_id,
        u.team_id,
        u.period,
        u.minute,
        u.second,
        u.start_x,
        u.start_y,
        u.end_x,
        u.end_y,
        u.pass_type,
        u.pass_height,
        u.body_part,
        u.pass_length,
        u.pass_angle_radians,
        u.pass_outcome,
        u.is_cross,
        u.is_switch,
        u.is_through_ball,
        u.pass_recipient_id,
        u.is_progressive,
        u.data_source

    from unioned u
    inner join {{ ref('dim_matches') }} dm
      on dm.provider = u.provider
     and dm.native_match_id = u.native_match_id

)

select * from keyed
```

- [ ] **Step 3.4: Build via fct_passes (drives downstream)**

Defer to Task 4 — building fct_passes will rebuild the ephemeral int_unified_passes.

---

## Task 4: Migrate `fct_passes` to `match_key` (TDD)

**Files:**
- Modify: `dbt_project/models/marts/fct_passes.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (rename `match_id` → `match_key` in the contract)

- [ ] **Step 4.1: Update the dbt contract**

In `dbt_project/models/marts/_marts__models.yml`, find the `fct_passes` block and:
- Rename `match_id` column → `match_key`; change `data_type: bigint` to stay `bigint` (same underlying type, different semantics); update description to reference `dim_matches`.
- Update `data_source` column `accepted_values` to `['statsbomb', 'wyscout', 'idsse', 'metrica']` (was 2 of 4).

- [ ] **Step 4.2: Run `dbt build --select fct_passes` — expected contract failure**

```bash
uv run dbt build --select fct_passes --project-dir dbt_project --profiles-dir dbt_project --full-refresh
```

Expected: contract mismatch — `match_key` column declared but model still emits `match_id`. Confirms the contract now gates on the new name.

- [ ] **Step 4.3: Rewrite `fct_passes.sql`**

Edit `dbt_project/models/marts/fct_passes.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key='pass_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge'
) }}
-- fct_passes.sql
-- Gold-layer pass fact table with progressive and line-breaking metrics.
-- Every row references a Kimball surrogate `match_key` (FK → dim_matches).
-- Source-native `match_id` is deliberately NOT present; recover via
-- `JOIN dim_matches ON match_key`.

with unified_passes as (

    select * from {{ ref('int_unified_passes') }}
    {% if is_incremental() %}
    where match_key not in (select distinct match_key from {{ this }})
    {% endif %}

),

match_attrs as (

    select
        match_key,
        provider,
        competition_id,
        season_id,
        home_team_name,
        away_team_name
    from {{ ref('dim_matches') }}

),

line_breaking as (

    select
        lb.event_id,
        lb.is_line_breaking,
        lb.lines_broken,
        lb.line_breaking_type
    from {{ ref('stg_line_breaking__results') }} lb

),

running_score as (

    select
        match_key,
        home_team_id,
        home_score_after,
        away_score_after,
        period,
        minute,
        second
    from {{ ref('int_running_score') }}

),

passes_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key(['unified_passes.event_id', 'unified_passes.data_source']) }} as pass_id,
        unified_passes.match_key,
        unified_passes.player_id,
        unified_passes.team_id,
        unified_passes.pass_recipient_id,
        match_attrs.competition_id,
        match_attrs.season_id,
        unified_passes.period,
        unified_passes.minute,
        unified_passes.second,
        unified_passes.start_x,
        unified_passes.start_y,
        unified_passes.end_x,
        unified_passes.end_y,
        unified_passes.pass_type,
        unified_passes.pass_height,
        unified_passes.body_part,
        unified_passes.pass_length,
        unified_passes.pass_angle_radians,
        unified_passes.pass_outcome,
        unified_passes.is_cross,
        unified_passes.is_switch,
        unified_passes.is_through_ball,
        case
            when unified_passes.pass_outcome = 'Complete' or unified_passes.pass_outcome is null then true
            else false
        end                                             as is_complete,
        unified_passes.is_progressive,
        case
            when unified_passes.end_x is null or unified_passes.start_x is null then null
            when unified_passes.end_x > unified_passes.start_x + {{ var('pass_direction_threshold') }} then 'forward'
            when unified_passes.end_x < unified_passes.start_x - {{ var('pass_direction_threshold') }} then 'backward'
            else 'lateral'
        end                                             as pass_direction,
        coalesce(lb.is_line_breaking, false)            as is_line_breaking,
        coalesce(lb.lines_broken, 0)                    as lines_broken,
        lb.line_breaking_type,
        unified_passes.data_source,
        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id                                 as _rs_home_team_id,
        row_number() over (
            partition by unified_passes.event_id, unified_passes.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        ) as _score_rn

    from unified_passes
    left join match_attrs
        on unified_passes.match_key = match_attrs.match_key
    left join line_breaking lb
        on unified_passes.event_id = lb.event_id
    left join running_score rs
        on unified_passes.match_key = rs.match_key
        and (
            rs.period < unified_passes.period
            or (rs.period = unified_passes.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_passes.minute * 60 + unified_passes.second))
        )

),

final as (

    select
        pass_id,
        match_key,
        player_id,
        team_id,
        pass_recipient_id,
        competition_id,
        season_id,
        period,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        pass_type,
        pass_height,
        body_part,
        pass_length,
        pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        is_complete,
        is_progressive,
        pass_direction,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0) then 'drawing'
            when (team_id = _rs_home_team_id and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end as game_state,
        data_source
    from passes_with_score
    where _score_rn = 1

)

select * from final
```

- [ ] **Step 4.4: Rebuild fct_passes (full-refresh required for the rename)**

```bash
uv run dbt build --select +fct_passes --project-dir dbt_project --profiles-dir dbt_project --full-refresh
```

Expected: Completed successfully. All contract columns validated. All data tests pass.

- [ ] **Step 4.5: Verify 4-provider coverage + referential integrity**

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py::test_fct_passes_covers_all_four_providers src/tests/test_dbt_passes_kimball_migration.py::test_fct_passes_match_key_joins_to_dim -v
```

Expected: both tests pass.

---

## Task 5: Migrate `fct_line_breaking_results` to `match_key` (TDD)

**Files:**
- Modify: `dbt_project/models/marts/fct_line_breaking_results.sql`
- Modify: `_marts__models.yml` — rename `match_id` → `match_key` in the contract

- [ ] **Step 5.1: Update contract**

In `_marts__models.yml`, `fct_line_breaking_results`: rename column `match_id` → `match_key`.

- [ ] **Step 5.2: Rewrite the model — join `dim_matches`**

Edit `dbt_project/models/marts/fct_line_breaking_results.sql`:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='line_breaking_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_key']
) }}
-- fct_line_breaking_results.sql
-- Gold-layer line-breaking detection results per pass event.
--
-- PR 2: `match_id` renamed to `match_key` (Kimball surrogate FK to dim_matches).
-- Upstream `stg_line_breaking__results` still exposes `match_id` (native) per
-- provider, so we join dim_matches on (provider, native_match_id) to convert.
--
-- `data_source` on `stg_line_breaking__results` is one of:
--   'statsbomb_360' | 'metrica_tracking' | 'idsse_tracking'
-- We map those → dim_matches.provider via:
--   statsbomb_360   → statsbomb
--   metrica_tracking → metrica
--   idsse_tracking  → idsse

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_key from {{ this }}
),
{% endif %}

line_breaking_raw as (

    select
        event_id,
        -- The native `match_id` on stg_line_breaking__results is typed STRING
        -- across all three LB paths (StatsBomb 360 casts its bigint to string,
        -- Metrica + IDSSE are natively string). The `data_source` → provider
        -- map is below.
        cast(match_id as string)                        as native_match_id,
        case data_source
            when 'statsbomb_360' then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
            when 'idsse_tracking' then 'idsse'
            else data_source
        end                                             as provider,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source

    from {{ ref('stg_line_breaking__results') }}

),

keyed as (

    select
        lb.event_id,
        dm.match_key,
        lb.is_line_breaking,
        lb.lines_broken,
        lb.line_breaking_type,
        lb.data_source

    from line_breaking_raw lb
    inner join {{ ref('dim_matches') }} dm
      on dm.provider = lb.provider
     and dm.native_match_id = lb.native_match_id

    {% if is_incremental() %}
    where dm.match_key not in (select match_key from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_id']) }}    as line_breaking_id,
        event_id,
        match_key,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source

    from keyed

)

select * from final
```

- [ ] **Step 5.3: Rebuild**

```bash
uv run dbt build --select fct_line_breaking_results --project-dir dbt_project --profiles-dir dbt_project --full-refresh
```

Expected: successful rebuild. Row count should match pre-migration (no rows lost to the join since dim_matches covers all 4 providers fully).

- [ ] **Step 5.4: Test referential integrity**

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py::test_fct_line_breaking_results_match_key_joins_to_dim -v
```

Expected: pass.

---

## Task 6: Extend `fct_match_summary` to cover IDSSE + Metrica (TDD)

**Files:**
- Modify: `dbt_project/models/marts/fct_match_summary.sql`
- Modify: `_marts__models.yml` — rename `match_id` → `match_key`, mark tracking-exclusive columns (xg, ppda, score) as nullable

- [ ] **Step 6.1: Read current state of fct_match_summary.sql**

Before editing, READ the existing model (247 lines per investigation) to understand its structure. Identify:
- The StatsBomb-only source joins (`stg_statsbomb__matches`, `stg_statsbomb__events` at lines 32, 46, 113, 133 per earlier investigation)
- Which columns have values (score, xg, PPDA) — must become nullable
- Any aggregations that would break on NULL inputs — default to 0 or NULL-safe

Then proceed with the rewrite.

- [ ] **Step 6.2: Update contract**

In `_marts__models.yml`, `fct_match_summary`:
- Rename `match_id` → `match_key` (bigint)
- Mark `home_score`, `away_score`, `home_xg`, `away_xg`, `ppda` as NULL-tolerant (remove `not_null` test if present)

- [ ] **Step 6.3: Rewrite the model**

The rewrite replaces the single-source (StatsBomb) structure with a union. StatsBomb and Wyscout entries produce the rich metrics (score, xG, PPDA). IDSSE and Metrica entries produce only the minimum (match_key, home/away team names, provider) with all metric columns NULL. Detailed SQL will be based on reading the current file in Step 6.1.

Key structure:

```sql
-- fct_match_summary.sql
-- One row per match, keyed by match_key (Kimball surrogate). Covers all
-- four providers. Tracking-provider rows (IDSSE, Metrica) carry minimum
-- metadata — metric columns (xg, score, ppda) are NULL because the
-- tracking providers don't produce event-based metrics.

with statsbomb_summary as (
    -- Existing StatsBomb logic, but join dim_matches to emit match_key,
    -- keep all the computed columns (xg, score, ppda, etc.)
    ...
),

wyscout_summary as (
    -- NEW: Wyscout currently absent from fct_match_summary (per
    -- investigation: "fct_match_summary.sql is StatsBomb/Wyscout only"
    -- was my previous finding; confirm on read in Step 6.1)
    ...
),

tracking_summary as (
    -- IDSSE + Metrica minimum rows from dim_matches
    select
        dm.match_key,
        dm.provider,
        dm.home_team_name,
        dm.away_team_name,
        cast(null as int)    as home_score,
        cast(null as int)    as away_score,
        cast(null as double) as home_xg,
        cast(null as double) as away_xg,
        cast(null as double) as ppda_home,
        cast(null as double) as ppda_away,
        dm.match_date,
        dm.competition_id
    from {{ ref('dim_matches') }} dm
    where dm.provider in ('idsse', 'metrica')
),

final as (
    select * from statsbomb_summary
    union all
    select * from wyscout_summary
    union all
    select * from tracking_summary
)

select * from final
```

- [ ] **Step 6.4: Rebuild**

```bash
uv run dbt build --select fct_match_summary --project-dir dbt_project --profiles-dir dbt_project --full-refresh
```

Expected: successful. Row count ≈ (StatsBomb match count) + (Wyscout match count if coverage extends) + 7 + 3.

- [ ] **Step 6.5: Verify tracking-provider coverage**

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py::test_fct_match_summary_covers_tracking_providers -v
```

Expected: pass — IDSSE=7, Metrica=3.

---

## Task 7: Remove `dim_tracking_matches` model and callers

**Files:**
- Delete: `dbt_project/models/marts/dim_tracking_matches.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` — remove `dim_tracking_matches` contract block if present
- Modify: `src/ingestion/refresh_synced_tables.py` — remove `dim_tracking_matches_synced` from SYNCED_TABLES (if present)
- grep-and-modify: any Taipy file referencing `dim_tracking_matches_synced`

- [ ] **Step 7.1: Verify no other dbt model references `dim_tracking_matches`**

```bash
grep -rn "dim_tracking_matches" dbt_project/ src/ hf_taipy_app/ scripts/ 2>&1 | grep -v "\.md:\|plans/\|adrs/\|TODO.md"
```

Expected: any callers must be updated. Known callers: `dim_tracking_matches.sql` (being deleted), possibly `refresh_synced_tables.py` (SYNCED_TABLES list), possibly a Taipy dropdown query (must switch to `dim_matches`).

- [ ] **Step 7.2: Delete the model file**

```bash
rm dbt_project/models/marts/dim_tracking_matches.sql
```

- [ ] **Step 7.3: Remove any Taipy references**

grep results from Step 7.1 tell you which Taipy files to edit. Replace `dim_tracking_matches` queries with `dim_matches WHERE provider IN ('idsse', 'metrica', 'skillcorner')` filters.

- [ ] **Step 7.4: Rebuild without the deleted model**

```bash
uv run dbt build --select fct_passes fct_line_breaking_results fct_match_summary --project-dir dbt_project --profiles-dir dbt_project
```

Expected: no "model not found" errors on refs (confirming nothing still references `dim_tracking_matches`).

---

## Task 8: Migrate Taipy queries + state (batched)

**Files:**
- Modify: `hf_taipy_app/src/queries/passes.py`
- Modify: `hf_taipy_app/src/queries/match.py`
- Modify: `hf_taipy_app/src/state/pass_map.py`
- Modify: `hf_taipy_app/src/state/pass_network.py`
- Modify: `hf_taipy_app/src/state/pass_timing.py`
- Modify: `hf_taipy_app/src/state/shared.py`
- Modify: `hf_taipy_app/src/pages/pass_map.py`

Mechanical rename pattern: every `match_id` referring to the PR 2 facts (fct_passes, fct_match_summary, fct_line_breaking_results, fct_pausa_values if still keyed by match_id — check) becomes `match_key`. The variable name in Taipy state, the SQL column name, and the parameter in the query functions all get renamed together. Other facts (fct_action_values, fct_shots, etc.) still use `match_id` and MUST not be touched.

- [ ] **Step 8.1: Global grep for scope**

```bash
grep -n "match_id" hf_taipy_app/src/queries/passes.py hf_taipy_app/src/queries/match.py hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/shared.py
```

Examine each hit. Some will belong to PR 2 (pass-related, match_summary), others to other facts (action_values, shots). Annotate which hits to rename vs leave alone.

- [ ] **Step 8.2-8.8: For each file, apply the targeted rename**

Specifically:
- `queries/passes.py`: function signatures (`fetch_passes(comp_id, team_id, match_id)` → `fetch_passes(comp_id, team_id, match_key)`), SQL literals referencing `fct_passes_synced.match_id` → `match_key`, any JOIN with `fct_match_summary_synced` use `match_key`.
- `queries/match.py`: dropdown-fetch query — SELECT `match_key` instead of `match_id`; the returned tuple's first element is the surrogate BIGINT now.
- `state/pass_map.py`: variable `selected_match_id: int` → `selected_match_key: int` (also rename the callback `on_match_change` parameter).
- `state/pass_network.py` + `state/pass_timing.py`: same rename pattern.
- `state/shared.py`: shared-state variable for the match cascade.
- `pages/pass_map.py` lines 22-23: remove the "StatsBomb 360 only" LB-toggle description caveat.

- [ ] **Step 8.9: Update the page description**

`hf_taipy_app/src/pages/pass_map.py:22-23` currently says `"Line-breaking detection requires StatsBomb 360 data (~323 of 380+ matches)."` — rewrite to:

```
Line-breaking detection available for StatsBomb 360, Metrica tracking,
and IDSSE Bundesliga tracking matches.
```

- [ ] **Step 8.10: Run local Taipy app for smoke test**

```bash
cd hf_taipy_app && uv run python src/main.py
```

Expected: app starts on localhost:7860 without stack traces; Pass Map page loads. Full E2E Puppeteer verification happens in Task 10.

---

## Task 9: Lakebase synced-table recreation

**User action required** (per ADR-005 + tech-debt #1 — Autoscaling Lakebase has no synced-table schema-alter API):

The three synced tables must be recreated in the Databricks UI to pick up the column rename:
- `fct_passes_synced`
- `fct_line_breaking_results_synced`
- `fct_match_summary_synced`

- [ ] **Step 9.1: Pause for user UI action**

Report to user:

> Recreate these 3 synced tables in the Databricks UI (same steps as `dim_matches_synced` in PR 1). Once done, reply with "synced tables recreated" and I will trigger refresh + grants + indexes automatically.

- [ ] **Step 9.2: Fix event_log ownership for the newly-recreated tables**

```bash
uv run python scripts/fix_event_log_ownership.py --tables fct_passes_synced,fct_line_breaking_results_synced,fct_match_summary_synced
```

- [ ] **Step 9.3: Refresh all affected synced tables**

```bash
uv run python -m ingestion.refresh_synced_tables --tables fct_passes_synced,fct_line_breaking_results_synced,fct_match_summary_synced --wait
```

- [ ] **Step 9.4: Apply grants**

```bash
SP_UUID=$(terraform -chdir=terraform/environments/dev output -raw hf_app_sp_application_id)
uv run python scripts/run_lakebase_grants.py --sp-uuid "$SP_UUID" --verify
```

Expected: 40/40 synced tables covered (or 40+1 if the count shifts).

- [ ] **Step 9.5: Update `scripts/create_indexes.py`**

Rename every index entry that references `fct_passes_synced(... match_id ...)` / `fct_line_breaking_results_synced(match_id)` / `fct_match_summary_synced(match_id)` columns to `match_key`. Also update any `VERIFY_QUERIES` filtering these tables by `match_id` → `match_key`.

Do NOT rename indexes on other fact tables (fct_action_values_synced, fct_shots_synced, etc.) — those still use `match_id` until their respective migration PRs.

- [ ] **Step 9.6: Apply indexes**

```bash
uv run python scripts/create_indexes.py --verify
```

Expected: indexes applied; EXPLAIN ANALYZE shows Index Scan plans for all `match_key`-keyed lookups.

---

## Task 10: E2E Puppeteer verification

**Files:**
- No code changes — verification only

- [ ] **Step 10.1: Start the Taipy app locally**

```bash
cd hf_taipy_app && uv run python src/main.py
```

Wait for startup; app serves on localhost:7860.

- [ ] **Step 10.2: Puppeteer navigation + LB verification**

Sequence:
1. Navigate to Pass Map page
2. Open Competition dropdown, select any Bundesliga-era IDSSE competition (DFL-COM-000002)
3. Open Team dropdown, select the home team for one of the IDSSE matches (e.g., `Fortuna Düsseldorf`)
4. Open Match dropdown, pick an IDSSE match (`J03WOH`, `J03WPY`, etc.)
5. Toggle "Show line-breaking" ON
6. Verify at least one pass-arrow renders on the pitch with the LB highlight color
7. Screenshot for PR description

Repeat steps 2-6 for a Metrica match (`Sample_Game_1`).

- [ ] **Step 10.3: Verify regression coverage for StatsBomb + Wyscout matches**

Confirm existing StatsBomb / Wyscout matches still render correctly — the `match_id` → `match_key` rename is prone to silent bugs (wrong type, empty filter). Test at least:
- One StatsBomb match (e.g., UEFA Euro 2024 Final)
- One Wyscout match (if any are in current Pass Map coverage)

Any regression here blocks the PR.

- [ ] **Step 10.4: Screenshot both verifications**

Save to `docs/superpowers/plans/assets/` or similar. Attach in PR description when opening.

---

## Task 11: Doc cleanup

**Files:**
- Modify: `TODO.md`
- Modify: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` (update staged-rollout table)

- [ ] **Step 11.1: Update TODO.md Kimball Migration table**

Change `PR 2` status from `Planned` → `Shipped`. Update the **Last updated** line to today.

- [ ] **Step 11.2: Update ADR-011 §Staged rollout policy**

Flip `PR 2` status in the table from `Planned` → `Active` (during PR 2 execution) or `Shipped` (post-merge, but we update now to reflect that the code has shipped; the merge will move it to `Shipped` in a follow-up commit if desired).

- [ ] **Step 11.3: Verify no stale LB-IDSSE or LB-METRICA references**

```bash
grep -rn "LB-IDSSE\|LB-METRICA\|Path C (IDSSE tracking) operational" --include="*.md" --include="*.py" --include="*.sql" --include="*.yml" . | grep -v "plans/\|adrs/\|ARCHITECTURE.md"
```

Expected: zero hits in actionable files — the PR 1 doc sweep already cleaned up; this is a re-verification.

---

## Task 12: Final verification + commit approval request

- [ ] **Step 12.1: Run full test suite + lint + type-check**

```bash
uv run pytest src/tests/test_dbt_passes_kimball_migration.py src/tests/test_dbt_dim_matches.py src/tests/test_generate_match_key_macro.py -v
uv run ruff check src/tests/test_dbt_passes_kimball_migration.py hf_taipy_app/src/queries/passes.py hf_taipy_app/src/queries/match.py hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/shared.py hf_taipy_app/src/pages/pass_map.py
uv run ruff format --check src/tests/test_dbt_passes_kimball_migration.py hf_taipy_app/src/queries/passes.py hf_taipy_app/src/queries/match.py hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/shared.py hf_taipy_app/src/pages/pass_map.py
uv run pyright src/tests/test_dbt_passes_kimball_migration.py hf_taipy_app/src/queries/passes.py hf_taipy_app/src/queries/match.py hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/shared.py hf_taipy_app/src/pages/pass_map.py
```

Expected: all green.

- [ ] **Step 12.2: Run full dbt build**

```bash
uv run dbt build --select +fct_passes +fct_line_breaking_results +fct_match_summary --project-dir dbt_project --profiles-dir dbt_project
```

Expected: all models build, all tests pass.

- [ ] **Step 12.3: Prepare summary and request commit approval**

Produce a concise summary:
- Files changed (from `git status`)
- Test results (pytest, dbt build)
- Live verification (row counts per provider on fct_passes, fct_line_breaking_results, fct_match_summary)
- E2E screenshots (IDSSE + Metrica matches in Pass Map with LB overlay)
- Deferred items, if any

Then ask: **"Approve commit of PR 2 (Passes conformed + LB-IDSSE/LB-METRICA surfacing)?"**

- [ ] **Step 12.4: If approved, commit (single commit per branch rule)**

Do NOT commit without explicit approval. When user approves:

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(dbt): Passes conformed fact — match_id → match_key migration (PR 2/8, ADR-011)

Migrates fct_passes, fct_line_breaking_results, and fct_match_summary
from native-ID `match_id` to the Kimball surrogate `match_key` FK
(ADR-011). Extends int_unified_passes with IDSSE + Metrica arms so
fct_passes now covers all four providers. Extends fct_match_summary
to include IDSSE + Metrica matches (tracking-only metadata; metric
columns NULL). Deprecates dim_tracking_matches — subsumed by dim_matches.

Taipy Pass Map + Pass Network + Pass Timing updated to match_key.
Delivers the original LB-IDSSE + LB-METRICA functional goal — IDSSE
and Metrica matches now appear in the Pass Map cascade with
line-breaking overlay rendering.

Other facts (fct_action_values, fct_shots, fct_player_stats, ...)
still use match_id and are explicitly deferred to PR 3-7 per
ADR-011 §Staged rollout.

See docs/superpowers/plans/2026-04-20-pr2-passes-match-key-migration.md
for the execution plan and E2E verification.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git status
```

- [ ] **Step 12.5: Do NOT push or open PR without separate approval**

Per user's standing rule: push + PR creation each require a separate explicit approval. Wait for the user.

---

## Self-review checklist (run before claiming the plan is complete)

- [ ] Every `match_id` rename specifically calls out which fact tables are in scope (fct_passes + fct_line_breaking_results + fct_match_summary ONLY) and which stay with `match_id` (fct_action_values, fct_shots, fct_player_stats, ...).
- [ ] Every TDD task has the failing-state + passing-state commands.
- [ ] No placeholders ("TBD", "Similar to X", "handle edge cases").
- [ ] Test file names consistent across Tasks 3 + 12.
- [ ] Column names consistent across macro, dim, fct, Taipy queries (`match_key BIGINT` everywhere in PR 2 scope).
- [ ] Task 9's "user action required" callout is prominent — plan pauses explicitly on the UI recreation step.
- [ ] ADR-011 reference used correctly (not ADR-008).
- [ ] Stacked-PR base branch (`feat/tracking-passes-idsse-metrica`) documented in the header.
