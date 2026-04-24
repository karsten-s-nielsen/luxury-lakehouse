# Kimball PR 4b — Action Values migration + G1 + Finding D Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `fct_action_values` (9.53M rows; base-case mart from PR #134) off smart-keyed `match_id`/`competition_id` onto surrogate `match_key`/`competition_key` per ADR-011. Update the HF dataset publisher (`publish_spadl_vaep_hf.py`) with dual-column output (90-day deprecation window, sunset 2026-07-22). Update the Taipy Player Impact page's SQL queries. Add a forward-looking G1 `wait_until_online` helper in `refresh_synced_tables.py` for SDK-path synced-table recreation. Fix Finding D (plain `CAST` → `try_cast` in `publish_xg_shots_hf.py:99`). Land an On-Deck entry for G2 + G3.

**Architecture:** Kimball surrogate keys resolve once at the mart layer via `LEFT JOIN dim_matches ON (av.match_id = dm.native_match_id AND av.data_source = dm.data_source)` + `LEFT JOIN dim_competitions`. The mart emits both new (`match_key`, `competition_key`) and legacy (`match_id` via `try_cast(dm.native_match_id as bigint)`, `competition_id` via equivalent) columns for the 90-day window. Downstream publish script and Taipy queries read the pre-joined columns — no additional dim joins in consumer SQL. Incremental predicate flips from smart key to surrogate key. G1 helper polls `status.detailed_state` for `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`; unused in PR 4b (no SDK-path caller yet) but ships ready.

**Tech Stack:** dbt (Databricks adapter), Spark/Delta, PySpark, Taipy GUI, Lakebase PG synced tables, pytest, HuggingFace Hub.

**Source spec:** `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`.

**Depends on:** Kimball PR 4a (live dbt CI) must be merged first — PR 4b's safety net is the live CI introduced in 4a.

---

## Decisions required — resolve before/during execution

| # | Decision | Default (this plan assumes) | Alternative |
|---|---|---|---|
| **D1** | Lakebase synced-table schema evolution vs recreation | **Inspect during implementation** (Phase 0 Task 0.2). Two paths: **(a) auto-evolution** — `fct_action_values_synced` picks up the new columns on next refresh after dbt build completes; no manual work. **(b) manual recreation** — `maintain_synced_tables.py` run (existing PR 2 standard pattern) recreates the synced table + grants + indexes. The empirical answer drives Phase 6 Task 6.2. | Pre-committing to (b) preemptively — over-cautious; adds unnecessary deploy steps if auto-evolution works. |
| **D2** | Commit granularity within PR 4b | **Two commits:** one for the full Kimball migration (dbt + python publishers + Taipy consumer + Finding D), one for G1 helper + On-Deck entry. Squash on merge (user's "single/minimal commits" preference). | One big commit — less reviewable history mid-PR. Five granular commits — squashed-away anyway; extra bookkeeping. |
| **D3** | `dim_matches` join key shape | **`(av.match_id = dm.native_match_id AND av.data_source = dm.data_source)`** per ADR-011 + PR 2 precedent. stg_spadl__action_values emits `match_id` BIGINT and `data_source` STRING; `dim_matches.native_match_id` is STRING — implicit cast happens at the join. Alternative: explicit `cast(av.match_id as string)` at the join site for clarity. | Explicit cast adds SQL noise; Spark's implicit BIGINT→STRING cast is standard. Verify PR 2 shipped the same pattern in Phase 0 Task 0.4. |
| **D4** | `int_running_score` already on `match_key` | **Assumed true per PR 2 memory.** Verified in Phase 0 Task 0.5 via grep. If not yet migrated, PR 4b expands to include int_running_score — but memory `project_kimball_migration_cycle.md` states PR 2 "fct_passes + fct_line_breaking_results + fct_match_summary → match_key" which implies int_running_score was migrated as a prerequisite. | If PR 2 didn't migrate it: add `int_running_score.sql` edit to Phase 1 scope. |
| **D5** | Lakebase VAEP queries join to `dim_matches_synced` in this PR | **Yes if any current SQL surfaces native match_id or filters on it.** No if the queries only filter by `competition_id`/`team_id`/`player_id`. Phase 0 Task 0.3 reads `hf_taipy_app/src/queries/defensive.py` to decide. | Defer all dim_matches_synced joins to a future PR — but some queries probably already need it; decide empirically. |

---

## File structure map

### Created

| Path | Responsibility |
|---|---|
| `src/tests/test_fct_action_values_contract.py` | Parser-level contract test: dbt YAML for fct_action_values has match_key NOT NULL + competition_key nullable + legacy match_id/competition_id nullable + contract `enforced: true`. |
| `src/tests/test_action_values_match_key_coverage.py` (or extend existing `test_marts_live_schema.py`) | Live DESCRIBE test: `soccer_analytics.dev_gold.fct_action_values` schema matches the YAML contract column-for-column. Runs in Bronze Live Schema CI pattern established in PR 1.8. |

### Modified

| Path | Reason |
|---|---|
| `dbt_project/models/marts/fct_action_values.sql` | Add `match_key` + `competition_key` via dim joins; retain legacy match_id + competition_id via try_cast; flip `liquid_clustered_by` to `['match_key']`; update incremental predicate. |
| `dbt_project/models/marts/_marts__models.yml` (or wherever the fct_action_values contract lives) | Update columns block: add match_key BIGINT NOT NULL + competition_key BIGINT nullable; retain legacy columns with deprecation-notice descriptions. |
| `scripts/publish_spadl_vaep_hf.py` | SQL update: select new + legacy key columns directly from the mart (no new dim joins here — mart already joined); `normalize_dtypes` handles new Int64 columns; log line notes 2026-07-22 sunset. |
| `scripts/publish_xg_shots_hf.py` | Finding D: line 99 `CAST(dm.native_match_id AS BIGINT)` → `try_cast(dm.native_match_id as bigint)`. Update adjacent comment. |
| `src/tests/test_publish_xg_shots_hf.py` (new class or existing file — check during implementation) | Regression test: assert `try_cast` substring present in `_SHOTS_SQL` module constant. |
| `hf_taipy_app/src/queries/defensive.py` | `fetch_vaep_rankings`, `fetch_vaep_breakdown`, `fetch_vaep_timeline` — filter/join on `match_key` / `competition_key` instead of smart keys. Exact edits depend on Phase 0 Task 0.3 findings. |
| `hf_taipy_app/src/state/action_values.py` | Call-site updates: swap `get_match_id(...)` → `get_match_key(...)` (or equivalent) at the VAEP-query call sites. |
| `hf_taipy_app/src/state/shared.py` | **Conditional** — only if Phase 0 Task 0.3 reveals `get_match_key` / `get_competition_key` helpers don't already exist from PR 2 or PR 3. Add peers to the existing `get_match_id` / `get_comp_id` shape, with the same LOV lookup semantics. Verify during Phase 3. |
| `src/ingestion/refresh_synced_tables.py` | Add `wait_until_online(table_fqn, timeout_s=600, poll_interval_s=15)` helper + `SYNCED_TABLE_ONLINE_STATE` + `_SYNCED_TABLE_TERMINAL_FAILURE_STATES` module constants. Not called from anywhere in PR 4b — helper ships ready for the future SDK-path recreation PR. |
| `src/tests/test_refresh_synced_tables.py` | New `TestWaitUntilOnline` class: happy path state transitions, timeout, terminal failure states, HTTP 404. |
| `docs/todo/*.md` (exact file verified in Phase 0 Task 0.6) | On-Deck entry for G2 + G3 with blocking condition "any future PR that switches synced-table create to `w.postgres.synced_tables.*` SDK path". |

### Explicitly NOT modified (Chesterton's Fence)

- `dbt_project/models/staging/stg_spadl__action_values.sql` — silver level; stays on native `match_id` per ADR-011 (surrogate keys resolve at gold).
- `src/ingestion/` silly-kicks writer (bronze.action_values path) — silly-kicks continues emitting `match_id`; bronze is provenance.
- `src/shared/constants.py` — no changes; PR 2 already established dim_matches resolution conventions in macros.
- `fct_action_values_synced` resource in Terraform (`terraform/modules/synced_tables/main.tf`) — resource definition unchanged; schema is source-driven so underlying recreation (if needed per D1) happens without TF edits. Recreation via UI or `maintain_synced_tables.py`.
- `scripts/create_indexes.py` — fct_action_values_synced PG indexes use existing keys (player_id, competition_id). `competition_id` stays as a legacy column so the existing index remains functional through the 90-day window. Adding a `(competition_key, player_id)` index is forward-looking but NOT in PR 4b scope — defer to PR 8 cleanup when legacy columns drop.
- `docs/superpowers/adrs/` — no new ADR (spec §4 rationale).
- Wheel + consumers — no new wheel-shipped module; `refresh_synced_tables.py` already ships in the wheel, and adding a module-level function doesn't bump semver.

---

## Phase 0: Pre-flight verification (read-only)

All downstream phases depend on these. Do not skip.

### Task 0.1: Baseline green dbt-live-ci

**Files:** None.

- [ ] **Step 1:** Confirm PR 4a's live dbt CI merged and is running green on main's last PR:

```bash
gh run list --workflow=dbt-live-ci.yml --limit=5 --json conclusion,headBranch,displayTitle,url
```

Expected: recent runs show `"conclusion": "success"`. If any failure: stop and investigate before PR 4b.

### Task 0.2: Determine Lakebase synced-table schema evolution behavior (resolves D1)

**Files:** None — live check.

- [ ] **Step 1:** Describe `fct_action_values_synced` schema on Lakebase AND describe the underlying `fct_action_values` Delta table in UC. Compare column sets. Record whether they match.

```bash
uv run python - <<'PY'
import os
from databricks import sql as dbsql
with dbsql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
    http_path=os.environ["DATABRICKS_HTTP_PATH"].lstrip("/"),
    access_token=os.environ["DATABRICKS_TOKEN"],
) as conn, conn.cursor() as cur:
    cur.execute("DESCRIBE TABLE soccer_analytics.dev_gold.fct_action_values")
    print("=== UC fct_action_values (pre-migration) ===")
    for r in cur.fetchall(): print(r)
PY
```

Then on Lakebase (via psycopg connection — exact env vars and script depend on `scripts/create_indexes.py` convention; use `scripts/maintain_synced_tables.py --dry-run` if that helps):

```bash
uv run python - <<'PY'
import os
import psycopg
conn_str = os.environ["LAKEBASE_URL"]   # or per project's conn-string convention
with psycopg.connect(conn_str) as conn, conn.cursor() as cur:
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fct_action_values_synced'
        ORDER BY ordinal_position
    """)
    for r in cur.fetchall(): print(r)
PY
```

- [ ] **Step 2:** Record finding. PR 4b's Phase 6 deploy-sequence branches on this:
  - **(a)** Columns match → auto-evolution works; Phase 6 is just `dbt build` + single refresh.
  - **(b)** Columns differ → manual recreation required; Phase 6 adds `maintain_synced_tables.py` run.

This is a BEFORE-migration baseline — after PR 4b lands and dbt rebuilds `fct_action_values`, repeat the check to see whether Lakebase picked up the new columns.

### Task 0.3: Read `hf_taipy_app/src/queries/defensive.py` VAEP helpers (resolves D5)

**Files:** None — grep + read.

- [ ] **Step 1:** Grep for the function signatures:

```bash
grep -n "def fetch_vaep_" hf_taipy_app/src/queries/defensive.py
```

Expected: three entries (`fetch_vaep_rankings`, `fetch_vaep_breakdown`, `fetch_vaep_timeline`) with their current parameter names.

- [ ] **Step 2:** Open the file and record:
  - Each function's exact current signature.
  - Each function's current SQL: does it `SELECT fct_action_values_synced.*`? Filter by `match_id` or `competition_id`? Join any `dim_*_synced` tables?
  - Whether any function returns or surfaces `native_match_id` or a match-label column to the UI.

These findings drive Phase 3's exact edits. Without them, Phase 3 Tasks 3.1–3.3 cannot be written as bite-sized steps.

- [ ] **Step 3:** Grep for call sites from `hf_taipy_app/src/state/action_values.py`:

```bash
grep -n "fetch_vaep_" hf_taipy_app/src/state/action_values.py
```

Expected: three call sites in the state module. Record the argument shapes.

### Task 0.4: Verify `dim_matches` join key shape from PR 2 precedent (resolves D3)

**Files:** None.

- [ ] **Step 1:** Read how PR 2's `fct_passes` resolves match_key. The pattern is the reference:

```bash
grep -n "dim_matches" dbt_project/models/marts/fct_passes.sql
grep -n -A 5 "native_match_id" dbt_project/models/marts/fct_passes.sql
```

Expected: an explicit `LEFT JOIN {{ ref('dim_matches') }} dm ON <native_match_id-based predicate>`. Record the exact predicate. PR 4b's fct_action_values edit reuses this pattern byte-for-byte.

### Task 0.5: Verify `int_running_score` column set (resolves D4)

**Files:** None.

- [ ] **Step 1:** Grep for match_key / match_id in int_running_score:

```bash
grep -n "match_id\|match_key" dbt_project/models/intermediate/int_running_score.sql
```

Expected outcome A: `match_key` is the keyed column; `match_id` is absent or legacy-only. PR 4b's fct_action_values mart joins `running_score rs ON rs.match_key = dm.match_key` (unchanged shape).

Expected outcome B: `match_id` is still the keyed column (PR 2 didn't migrate it). PR 4b must expand scope to migrate int_running_score — stop and raise with the user before proceeding.

### Task 0.6: Locate the repo's TODO / On-Deck file for Phase 5 entry

**Files:** None.

- [ ] **Step 1:**

```bash
ls docs/ | head -30
find docs -type f -name "*.md" | xargs grep -l "On.Deck\|TODO" 2>/dev/null | head -10
```

Expected: a top-level `docs/TODO.md` or `docs/todo/ON_DECK.md` or similar. Record the path. Phase 5 Task 5.1 appends to this file.

### Task 0.7: Local Taipy baseline smoke

**Files:** None.

- [ ] **Step 1:** Start the Taipy app locally against staging Lakebase:

```bash
cd hf_taipy_app
python src/main.py &
LOCAL_TAIPY_PID=$!
```

Wait ~20s for startup. Open `http://localhost:7860` in browser. Visit:
- Shot Map (PR 3's page) — verify it still works after recent Kimball migrations.
- Match Summary — verify xG race chart.
- Player Impact (VAEP) — verify rankings load for a selected competition.

- [ ] **Step 2:** Kill the local server:

```bash
kill $LOCAL_TAIPY_PID
```

Expected: all three pages work. If Player Impact is already broken pre-PR-4b: investigate before starting Phase 3.

---

## Phase 1: dbt mart migration

### Task 1.1: Write parser-level contract test first (red)

**Files:**
- Create: `src/tests/test_fct_action_values_contract.py`.

- [ ] **Step 1:** Create the test file:

```python
"""Parser-level contract test for fct_action_values post-Kimball migration (PR 4b).

Asserts the dbt YAML contract declares:
  - match_key BIGINT NOT NULL (Kimball surrogate, primary)
  - competition_key BIGINT (nullable — some sources may not resolve)
  - match_id BIGINT (nullable — legacy; removed 2026-07-22)
  - competition_id BIGINT (nullable — legacy; removed 2026-07-22)
  - contract.enforced = true

Live-schema test in test_action_values_match_key_coverage.py covers the
runtime DESCRIBE.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml


_MARTS_YAML = Path("dbt_project/models/marts/_marts__models.yml")


def _load_fct_action_values_entry() -> dict:
    if not _MARTS_YAML.exists():
        pytest.skip(f"{_MARTS_YAML} not found at expected path; update test when YAML file moves")
    doc = yaml.safe_load(_MARTS_YAML.read_text())
    for model in doc.get("models", []):
        if model.get("name") == "fct_action_values":
            return model
    pytest.fail("fct_action_values not found in _marts__models.yml")


class TestFctActionValuesContract:
    def test_contract_enforced(self) -> None:
        entry = _load_fct_action_values_entry()
        assert entry.get("config", {}).get("contract", {}).get("enforced") is True

    def test_match_key_not_null(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "match_key" in cols
        mk = cols["match_key"]
        assert mk.get("data_type", "").lower() == "bigint"
        tests = mk.get("data_tests") or mk.get("tests") or []
        assert "not_null" in tests

    def test_competition_key_present_nullable(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "competition_key" in cols
        ck = cols["competition_key"]
        assert ck.get("data_type", "").lower() == "bigint"
        tests = ck.get("data_tests") or ck.get("tests") or []
        assert "not_null" not in tests

    def test_legacy_match_id_retained_nullable(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "match_id" in cols, "Legacy match_id must be retained for 90-day window"
        mid = cols["match_id"]
        assert mid.get("data_type", "").lower() == "bigint"
        desc = (mid.get("description") or "").lower()
        assert "legacy" in desc or "deprecat" in desc or "sunset" in desc

    def test_legacy_competition_id_retained_nullable(self) -> None:
        entry = _load_fct_action_values_entry()
        cols = {c["name"]: c for c in entry.get("columns", [])}
        assert "competition_id" in cols, "Legacy competition_id must be retained for 90-day window"
```

- [ ] **Step 2:** Run — expect failure (YAML not yet updated):

```bash
uv run pytest src/tests/test_fct_action_values_contract.py -v
```

Expected: fails because `match_key` is not yet in the YAML contract. Red.

### Task 1.2: Update fct_action_values.sql (green for mart, still red for YAML)

**Files:**
- Modify: `dbt_project/models/marts/fct_action_values.sql`.

- [ ] **Step 1:** Rewrite `fct_action_values.sql`. Full target content:

```sql
{{ config(
    materialized='incremental',
    unique_key='action_value_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge'
) }}
-- fct_action_values.sql
-- Gold-layer SPADL action values with VAEP scores, possession context,
-- and per-action game state.
--
-- PR 4 (2026-04-23): migrated to Kimball surrogate keys per ADR-011.
-- Emits both new (match_key, competition_key) and legacy (match_id,
-- competition_id) key columns during the 90-day dual-column window;
-- legacy columns removed 2026-07-22 per ADR-011 + dataset README.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (
        select distinct try_cast(match_id as bigint)
        from {{ this }}
        where match_id is not null
    )
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

-- Resolve Kimball surrogates. PR 2 precedent (fct_passes) defines the
-- join-predicate shape; match this pattern byte-for-byte for consistency.
matches_keyed as (

    select
        match_key,
        native_match_id,
        competition_key,
        data_source
    from {{ ref('dim_matches') }}

),

competitions_keyed as (

    select
        competition_key,
        native_competition_id
    from {{ ref('dim_competitions') }}

),

-- Join each action to its most recent score milestone.
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

        -- Kimball surrogates (new canonical)
        mk.match_key                                as match_key,
        ck.competition_key                          as competition_key,

        -- Legacy native IDs for 90-day dual-column window (sunset 2026-07-22)
        try_cast(mk.native_match_id as bigint)      as match_id,
        try_cast(ck.native_competition_id as bigint) as competition_id,

        av.player_id,
        av.team_id,
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
    left join matches_keyed mk
        on av.match_id = cast(mk.native_match_id as bigint)
        and av.data_source = mk.data_source
    left join competitions_keyed ck
        on ck.competition_key = mk.competition_key
    left join sb_events sbe
        on av.original_event_id = sbe.event_id
        and av.data_source = 'statsbomb'
    left join running_score rs
        on rs.match_key = mk.match_key
        and (
            rs.period < av.period
            or (rs.period = av.period
                and (rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second))
        )

),

final as (

    select
        action_value_id,
        match_key,
        competition_key,
        match_id,
        competition_id,
        player_id,
        team_id,
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

**Notes on the join predicate** (resolve during Phase 0 Task 0.4):
- If PR 2's fct_passes uses explicit `cast(native_match_id as bigint)` on the dim side, replicate that. The example above uses `cast(mk.native_match_id as bigint)`.
- If PR 2 uses a `dbt_utils.*` macro or a project-local macro for resolution (e.g., `{{ resolve_match_key('av.match_id', 'av.data_source') }}`), use that instead of inline SQL.

### Task 1.3: Update `_marts__models.yml` for fct_action_values contract

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml` (exact path — verify during implementation; may be a per-mart YAML or a single shared file).

- [ ] **Step 1:** Find the existing fct_action_values entry:

```bash
grep -n "fct_action_values" dbt_project/models/marts/_marts__models.yml
```

Find the model block. Update the `columns:` section.

- [ ] **Step 2:** Replace the columns block with the following (keep surrounding blocks — description, meta, etc. — as-is):

```yaml
      - name: match_key
        description: "Kimball surrogate FK to dim_matches (ADR-011). Primary match identifier."
        data_type: bigint
        data_tests:
          - not_null
      - name: competition_key
        description: "Kimball surrogate FK to dim_competitions (ADR-011)."
        data_type: bigint
      - name: match_id
        description: "LEGACY native match identifier; removed 2026-07-22 per ADR-011 dual-column policy. Use match_key. NULL when native_match_id is non-BIGINT-parseable (IDSSE/Metrica rows, if any enter bronze.action_values)."
        data_type: bigint
      - name: competition_id
        description: "LEGACY native competition identifier; removed 2026-07-22. Use competition_key."
        data_type: bigint
      - name: action_value_id
        description: "dbt_utils.generate_surrogate_key over (match_id, period, time_seconds, player_id, type_id, data_source)."
        data_type: string
        data_tests:
          - not_null
          - unique
      # ... keep existing entries for player_id, team_id, season_id, period, time_seconds, minute, second,
      # ... start_x/y, end_x/y, action_type, action_result, bodypart, offensive/defensive/vaep_value,
      # ... possession_id, possession_team_id, game_state, data_source, original_event_id, _loaded_at.
```

Keep the contract: `enforced: true` line in the model config block — it should already be present.

- [ ] **Step 3:** Run the parser-level contract test:

```bash
uv run pytest src/tests/test_fct_action_values_contract.py -v
```

Expected: all five tests pass. Green.

- [ ] **Step 4:** Run dbt compile to verify the SQL parses:

```bash
uv run --no-sync dbt parse --profiles-dir dbt_project/
```

Expected: no errors. If `int_running_score` or `dim_competitions` references fail: stop, fix per Phase 0 findings.

### Task 1.4: Write live-DESCRIBE test for fct_action_values

**Files:**
- Modify: `src/tests/test_marts_live_schema.py` (or wherever the Bronze/Gold Live Schema CI suite from PR 1.8 lives — verify during implementation via `grep -l "test_bronze_live_schema\|live_schema" src/tests/`).

- [ ] **Step 1:** Locate the live-schema test file pattern:

```bash
grep -l "DESCRIBE TABLE" src/tests/*.py
```

Expected: one or more files matching the PR 1.8 Bronze Live Schema pattern. Use the closest analog (or add a new file if no gold-mart live-schema suite exists yet).

- [ ] **Step 2:** Add a test class (adapt to the existing file's patterns):

```python
class TestFctActionValuesLiveSchema:
    """Live-DESCRIBE regression guard for fct_action_values (PR 4b, 2026-04-23)."""

    _EXPECTED_COLUMNS = {
        "action_value_id": "string",
        "match_key": "bigint",
        "competition_key": "bigint",
        "match_id": "bigint",
        "competition_id": "bigint",
        "player_id": "bigint",
        "team_id": "bigint",
        "season_id": "bigint",
        "period": "int",
        "time_seconds": "double",
        "minute": "int",
        "second": "int",
        "start_x": "double",
        "start_y": "double",
        "end_x": "double",
        "end_y": "double",
        "action_type": "string",
        "action_result": "string",
        "bodypart": "string",
        "offensive_value": "double",
        "defensive_value": "double",
        "vaep_value": "double",
        "possession_id": "bigint",
        "possession_team_id": "bigint",
        "game_state": "string",
        "data_source": "string",
        "original_event_id": "string",
        "_loaded_at": "timestamp",
    }

    def test_live_schema_matches_contract(self) -> None:
        """DESCRIBE TABLE returns exactly the expected column list + types."""
        # Uses the existing test-helper connection pattern from the file.
        # Adapt the exact connection fixture per file conventions.
        actual = _describe_table("soccer_analytics.dev_gold.fct_action_values")
        for col_name, col_type in self._EXPECTED_COLUMNS.items():
            assert col_name in actual, f"Column {col_name} missing from live table"
            assert actual[col_name].lower() == col_type, (
                f"Column {col_name} type mismatch: got {actual[col_name]!r}, expected {col_type!r}"
            )
        # Strict equality: no extra columns beyond the expected set.
        extras = set(actual) - set(self._EXPECTED_COLUMNS)
        assert not extras, f"Unexpected columns in live table: {extras}"
```

(`_describe_table` is the helper already present in the file per PR 1.8. If not, extract the connection boilerplate from the top of the file.)

- [ ] **Step 3:** This test will fail until the mart is rebuilt with the new schema. That's expected — it becomes green AFTER Phase 6 Task 6.2's `dbt build` completes. Skip it locally for now via `@pytest.mark.skipif` keyed on an env var, OR run it and accept the red state until the live table is rebuilt.

### Task 1.5: Commit Phase 1 work (requires user approval)

**Files:** All Phase 1 changes.

- [ ] **Step 1:**

```bash
git add dbt_project/models/marts/fct_action_values.sql \
        dbt_project/models/marts/_marts__models.yml \
        src/tests/test_fct_action_values_contract.py \
        src/tests/test_marts_live_schema.py   # or wherever the live test landed
git commit -m "feat(kimball): fct_action_values match_key/competition_key migration (PR 4b Phase 1)"
```

---

## Phase 2: Python publishers

### Task 2.1: Fix Finding D — write regression test first (red)

**Files:**
- Create (or modify existing): `src/tests/test_publish_xg_shots_hf.py`.

- [ ] **Step 1:** Check whether a test file for `publish_xg_shots_hf.py` already exists:

```bash
ls src/tests/test_publish_xg_shots_hf.py 2>/dev/null || echo "no existing file"
```

- [ ] **Step 2:** If no existing file, create `src/tests/test_publish_xg_shots_hf.py`. If existing, add the test class below to it:

```python
"""Unit tests for scripts.publish_xg_shots_hf (regression + SQL guards)."""

from __future__ import annotations

from pathlib import Path


_SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "publish_xg_shots_hf.py"


class TestTryCastGuard:
    """Finding D regression guard (PR 4b, 2026-04-23).

    The publish SQL joins dim_matches whose native_match_id is STRING (IDSSE/Metrica
    use alphanumeric native IDs). Spark pushes CAST(bigint) into the dim scan;
    try_cast is the cast-pushdown-safe form that returns NULL on unparseable
    values instead of aborting the query.

    Reference: memory reference_try_cast_spark_pushdown.md.
    """

    def test_shots_sql_uses_try_cast(self) -> None:
        content = _SCRIPT.read_text()
        # Find the _SHOTS_SQL assignment block.
        assert "_SHOTS_SQL" in content, "_SHOTS_SQL module constant must exist"
        # try_cast (case-insensitive) is present.
        lower = content.lower()
        assert "try_cast(dm.native_match_id" in lower, (
            "publish_xg_shots_hf.py must use try_cast(dm.native_match_id ...) "
            "per memory reference_try_cast_spark_pushdown.md"
        )

    def test_shots_sql_does_not_use_plain_cast_on_native_match_id(self) -> None:
        content = _SCRIPT.read_text().lower()
        # Guard against regression to plain CAST(dm.native_match_id AS BIGINT).
        assert "cast(dm.native_match_id as bigint)" not in content, (
            "publish_xg_shots_hf.py must NOT use plain CAST on dm.native_match_id — "
            "use try_cast (Finding D)."
        )
```

- [ ] **Step 3:** Run:

```bash
uv run pytest src/tests/test_publish_xg_shots_hf.py::TestTryCastGuard -v
```

Expected: `test_shots_sql_uses_try_cast` fails; `test_shots_sql_does_not_use_plain_cast_on_native_match_id` fails. Red on both.

### Task 2.2: Apply Finding D fix (green)

**Files:**
- Modify: `scripts/publish_xg_shots_hf.py` (line 99 + adjacent comment).

- [ ] **Step 1:** Update the `_SHOTS_SQL` block in `scripts/publish_xg_shots_hf.py`. The current content around line 99 is:

```python
# ... comment block ...
_SHOTS_SQL = """\
SELECT
    s.shot_id,
    s.match_key,
    CAST(dm.native_match_id AS BIGINT)                     AS match_id,
    s.competition_id,
    ...
```

Change to:

```python
# ... comment block — update to reference try_cast semantics ...
_SHOTS_SQL = """\
SELECT
    s.shot_id,
    s.match_key,
    try_cast(dm.native_match_id as bigint)                 as match_id,
    s.competition_id,
    ...
```

Also update the comment above the SQL constant to reference `reference_try_cast_spark_pushdown.md`:

```python
# fct_shots carries match_key (new) + competition_id (legacy).
# Recover native match_id for the dual-column HF dataset via dim_matches join.
# try_cast (not plain CAST) is required: Spark pushes the cast into the dim
# scan, and native_match_id is STRING with non-BIGINT-parseable values (IDSSE
# 'J03WOY', Metrica 'Sample_Game_1') even if fct_shots today is StatsBomb/
# Wyscout-only. try_cast returns NULL on unparseable; plain CAST aborts the
# whole query. See memory reference_try_cast_spark_pushdown.md.
```

- [ ] **Step 2:** Run the regression test:

```bash
uv run pytest src/tests/test_publish_xg_shots_hf.py::TestTryCastGuard -v
```

Expected: both tests pass. Green.

- [ ] **Step 3:** Lint the file:

```bash
uv run ruff check scripts/publish_xg_shots_hf.py
uv run pyright scripts/publish_xg_shots_hf.py
```

Expected: zero new violations. The file probably already has some accepted patterns; baseline doesn't regress.

### Task 2.3: Update `publish_spadl_vaep_hf.py` for dual-column output

**Files:**
- Modify: `scripts/publish_spadl_vaep_hf.py`.

- [ ] **Step 1:** Update the `_ACTION_VALUES_SQL` constant. The current content is:

```python
_ACTION_VALUES_SQL = """\
SELECT
    action_value_id,
    match_id,
    player_id,
    team_id,
    competition_id,
    season_id,
    period,
    ...
FROM soccer_analytics.dev_gold.fct_action_values
"""
```

Change to (the mart now provides both new and legacy columns directly — no additional joins needed):

```python
# SQL to extract action values from the gold-layer fact table.
# fct_action_values is Kimball-conformed post-PR 4b: emits match_key + competition_key
# (new canonical) and match_id + competition_id (legacy; removed 2026-07-22).
# Dual-column dataset: consumers migrate to match_key / competition_key at their
# own pace within the 90-day deprecation window announced in the HF README.
_ACTION_VALUES_SQL = """\
SELECT
    action_value_id,
    match_key,                   -- new: Kimball surrogate
    competition_key,             -- new: Kimball surrogate
    match_id,                    -- LEGACY: sunset 2026-07-22
    competition_id,              -- LEGACY: sunset 2026-07-22
    player_id,
    team_id,
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
    original_event_id,
    data_source
FROM soccer_analytics.dev_gold.fct_action_values
"""
```

- [ ] **Step 2:** Update `normalize_dtypes` to handle match_key + competition_key as `Int64`. The current block is:

```python
# Integer columns
for col in ("match_id", "player_id", "team_id", "competition_id", "season_id", "period", "minute", "second"):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
```

Change to:

```python
# Integer columns (Kimball keys + legacy + other IDs)
for col in (
    "match_key", "competition_key",                       # new canonical
    "match_id", "competition_id",                         # LEGACY: sunset 2026-07-22
    "player_id", "team_id", "season_id", "period", "minute", "second",
):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
```

- [ ] **Step 3:** Update the log summary to note the sunset. The current block near the end of `main()`:

```python
logger.info(
    "Final stats: %s actions, %s matches, sources: %s",
    f"{len(actions_df):,}", f"{n_matches:,}", source_counts,
)
```

Change to:

```python
logger.info(
    "Final stats: %s actions, %s matches, sources: %s. "
    "Dual-column schema (match_id/competition_id sunset 2026-07-22).",
    f"{len(actions_df):,}", f"{n_matches:,}", source_counts,
)
```

- [ ] **Step 4:** If tests for this file exist (`src/tests/test_publish_spadl_vaep_hf.py`), run them:

```bash
ls src/tests/test_publish_spadl_vaep_hf.py 2>/dev/null && uv run pytest src/tests/test_publish_spadl_vaep_hf.py -v
```

Expected: any existing tests still pass (dtype normalization tests may need updating to include new columns; fix if needed).

- [ ] **Step 5:** Lint:

```bash
uv run ruff check scripts/publish_spadl_vaep_hf.py
uv run pyright scripts/publish_spadl_vaep_hf.py
```

Expected: zero new violations.

### Task 2.4: Commit Phase 2 work (requires user approval)

- [ ] **Step 1:**

```bash
git add scripts/publish_xg_shots_hf.py \
        scripts/publish_spadl_vaep_hf.py \
        src/tests/test_publish_xg_shots_hf.py
git commit -m "feat(kimball): publish_spadl_vaep dual-column + Finding D try_cast (PR 4b Phase 2)"
```

---

## Phase 3: Taipy consumer migration

**Note:** exact edits in this phase depend on Phase 0 Task 0.3 findings. The steps below assume the most common shape (filters by `competition_id` or `match_id`, no existing match_key join). Adapt if the actual SQL differs.

### Task 3.1: Add `get_match_key` / `get_competition_key` to `state/shared.py` if absent

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py` — conditional.

- [ ] **Step 1:** Grep for existing helpers:

```bash
grep -n "def get_match_key\|def get_competition_key\|def get_match_id\|def get_comp_id" hf_taipy_app/src/state/shared.py
```

Expected outcomes:
- **(a)** `get_match_key` + `get_competition_key` already exist (PR 2 or PR 3 added them) → skip this task.
- **(b)** Only `get_match_id` / `get_comp_id` exist → add peers with the same LOV-lookup shape.

If (b), follow steps 2–4.

- [ ] **Step 2:** Open `hf_taipy_app/src/state/shared.py`. Find the existing `get_match_id` function. Study its LOV-lookup pattern (likely: takes `state`, looks up selected match label, translates to `match_id` via a dict or filter helper).

- [ ] **Step 3:** Add `get_match_key(state)` using the same LOV pattern but returning the Kimball surrogate. Depending on how PR 2 + PR 3 shipped this, there are two typical implementations:

```python
def get_match_key(state) -> int | None:
    """Resolve the selected match label to its match_key (Kimball surrogate).

    Returns None when no match is selected (all-matches view).
    """
    label = getattr(state, "selected_match", None)
    if not label or label == _ALL_LABEL:
        return None
    # If the LOV is backed by a dict-of-label-to-match_key:
    return _MATCH_LABEL_TO_KEY.get(label)
    # OR if filters.match_label_to_key is the project convention:
    # return filters.match_label_to_key(label)
```

Add `get_competition_key` analogously. Add both to the module's `__all__`.

- [ ] **Step 4:** Test locally that `get_match_key` returns a BIGINT when a match is selected (Phase 6 verifies end-to-end).

### Task 3.2: Update `hf_taipy_app/src/queries/defensive.py` VAEP queries

**Files:**
- Modify: `hf_taipy_app/src/queries/defensive.py`.

- [ ] **Step 1:** Based on Phase 0 Task 0.3 findings, update each VAEP query function.

**Illustrative example** — actual edits depend on current signatures. The most common patterns:

Before:
```python
def fetch_vaep_rankings(competition_id: int | None, team_id: int | None, player_id: int | None) -> pd.DataFrame:
    sql = """
        SELECT player_id, total_vaep, minutes_played, ...
        FROM public.fct_action_values_synced
        WHERE (%s::BIGINT IS NULL OR competition_id = %s::BIGINT)
          AND (%s::BIGINT IS NULL OR team_id = %s::BIGINT)
          AND (%s::BIGINT IS NULL OR player_id = %s::BIGINT)
        GROUP BY player_id
    """
    return _run_query(sql, (competition_id, competition_id, team_id, team_id, player_id, player_id))
```

After:
```python
def fetch_vaep_rankings(competition_key: int | None, team_id: int | None, player_id: int | None) -> pd.DataFrame:
    """Player VAEP rankings for a competition.

    PR 4b: migrated to competition_key (Kimball surrogate) per ADR-011.
    """
    sql = """
        SELECT player_id, total_vaep, minutes_played, ...
        FROM public.fct_action_values_synced
        WHERE (%s::BIGINT IS NULL OR competition_key = %s::BIGINT)
          AND (%s::BIGINT IS NULL OR team_id = %s::BIGINT)
          AND (%s::BIGINT IS NULL OR player_id = %s::BIGINT)
        GROUP BY player_id
    """
    return _run_query(sql, (competition_key, competition_key, team_id, team_id, player_id, player_id))
```

Apply the same rename to `fetch_vaep_breakdown` (match_id → match_key, competition_id → competition_key as parameters AND in WHERE clauses) and `fetch_vaep_timeline`.

- [ ] **Step 2:** If any VAEP query surfaces `native_match_id` or a human-readable match label to the UI, add a join to `dim_matches_synced`:

```sql
FROM public.fct_action_values_synced av
LEFT JOIN public.dim_matches_synced dm USING (match_key)
WHERE av.match_key = %s::BIGINT
```

- [ ] **Step 3:** Unit-test the queries against a mocked psycopg connection if the repo has that infrastructure — grep:

```bash
grep -l "_run_query\|fetch_vaep" src/tests/ 2>/dev/null
```

If tests exist, update them for the new parameter names. If not, skip — Phase 6's Taipy smoke is the verification.

- [ ] **Step 4:** Lint:

```bash
uv run ruff check hf_taipy_app/src/queries/defensive.py hf_taipy_app/src/state/shared.py
uv run pyright hf_taipy_app/src/queries/defensive.py hf_taipy_app/src/state/shared.py
```

Expected: zero new violations.

### Task 3.3: Update `hf_taipy_app/src/state/action_values.py` call sites

**Files:**
- Modify: `hf_taipy_app/src/state/action_values.py`.

- [ ] **Step 1:** Grep for VAEP query invocations:

```bash
grep -n "fetch_vaep_\|get_match_id\|get_comp_id" hf_taipy_app/src/state/action_values.py
```

- [ ] **Step 2:** For each call site, swap the ID helper:
- `get_match_id(state)` → `get_match_key(state)`
- `get_comp_id(state)` → `get_competition_key(state)`

Keep the call-site argument order intact. Update the refresh function(s) that invoke `fetch_vaep_rankings` / `fetch_vaep_breakdown` / `fetch_vaep_timeline` accordingly.

- [ ] **Step 3:** Lint:

```bash
uv run ruff check hf_taipy_app/src/state/action_values.py
uv run pyright hf_taipy_app/src/state/action_values.py
```

Expected: zero new violations.

### Task 3.4: Commit Phase 3 work (requires user approval)

- [ ] **Step 1:**

```bash
git add hf_taipy_app/src/queries/defensive.py \
        hf_taipy_app/src/state/action_values.py \
        hf_taipy_app/src/state/shared.py    # only if modified
git commit -m "feat(kimball): Taipy VAEP queries on match_key/competition_key (PR 4b Phase 3)"
```

---

## Phase 4: G1 `wait_until_online` helper

### Task 4.1: Write tests first (red)

**Files:**
- Modify: `src/tests/test_refresh_synced_tables.py`.

- [ ] **Step 1:** Open the existing test file. Grep for the existing test class structure:

```bash
grep -n "class Test" src/tests/test_refresh_synced_tables.py
```

Expected: existing test classes (e.g., `TestGetPipelineId`, `TestTriggerRefresh`, etc. established in earlier PRs). Add a new class after the last existing one.

- [ ] **Step 2:** Add `TestWaitUntilOnline`:

```python
class TestWaitUntilOnline:
    """Tests for refresh_synced_tables.wait_until_online (PR 4b G1 helper).

    The helper polls /api/2.0/database/synced_tables/<fqn> until
    status.detailed_state == "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE".
    """

    @patch("src.ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("src.ingestion.refresh_synced_tables.requests.get")
    @patch("src.ingestion.refresh_synced_tables._get_auth_headers")
    def test_returns_on_online_state(
        self, mock_auth: MagicMock, mock_get: MagicMock,
    ) -> None:
        mock_auth.return_value = {"Authorization": "Bearer t"}
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"status": {"detailed_state": "SYNCED_TABLE_PENDING"}},
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "status": {"detailed_state": "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"}
                },
            ),
        ]
        for r in mock_get.side_effect:
            r.raise_for_status = MagicMock()

        # Should return without raising.
        refresh.wait_until_online(
            "soccer_analytics.dev_gold.fct_action_values_synced",
            timeout_s=60,
            poll_interval_s=1,
        )
        assert mock_get.call_count == 2

    @patch("src.ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("src.ingestion.refresh_synced_tables.time.monotonic")
    @patch("src.ingestion.refresh_synced_tables.requests.get")
    @patch("src.ingestion.refresh_synced_tables._get_auth_headers")
    def test_timeout_raises_with_context(
        self,
        mock_auth: MagicMock,
        mock_get: MagicMock,
        mock_mono: MagicMock,
    ) -> None:
        mock_auth.return_value = {"Authorization": "Bearer t"}
        # Make time jump past the timeout threshold on the 3rd call.
        mock_mono.side_effect = [0, 10, 20, 700]
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "status": {"detailed_state": "SYNCED_TABLE_PENDING"}
        }
        mock_get.return_value.raise_for_status = MagicMock()

        with pytest.raises(TimeoutError) as exc_info:
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.fct_action_values_synced",
                timeout_s=600,
                poll_interval_s=1,
            )
        assert "fct_action_values_synced" in str(exc_info.value)
        assert "SYNCED_TABLE_PENDING" in str(exc_info.value)

    @patch("src.ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("src.ingestion.refresh_synced_tables.requests.get")
    @patch("src.ingestion.refresh_synced_tables._get_auth_headers")
    def test_terminal_failure_state_raises(
        self, mock_auth: MagicMock, mock_get: MagicMock,
    ) -> None:
        mock_auth.return_value = {"Authorization": "Bearer t"}
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "status": {"detailed_state": "SYNCED_TABLE_OFFLINE_FAILED"}
        }
        mock_get.return_value.raise_for_status = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.fct_action_values_synced",
                timeout_s=60, poll_interval_s=1,
            )
        assert "SYNCED_TABLE_OFFLINE_FAILED" in str(exc_info.value)

    @patch("src.ingestion.refresh_synced_tables.time.sleep", new=MagicMock())
    @patch("src.ingestion.refresh_synced_tables.requests.get")
    @patch("src.ingestion.refresh_synced_tables._get_auth_headers")
    def test_http_404_propagates(self, mock_auth: MagicMock, mock_get: MagicMock) -> None:
        mock_auth.return_value = {"Authorization": "Bearer t"}
        import requests as req
        err = req.HTTPError("404 Not Found")
        mock_get.return_value.status_code = 404
        mock_get.return_value.raise_for_status = MagicMock(side_effect=err)

        with pytest.raises(req.HTTPError):
            refresh.wait_until_online(
                "soccer_analytics.dev_gold.nonexistent_synced",
                timeout_s=60, poll_interval_s=1,
            )
```

(Adjust the `import ... as refresh` / module path per the existing test file's conventions.)

- [ ] **Step 3:** Run:

```bash
uv run pytest src/tests/test_refresh_synced_tables.py::TestWaitUntilOnline -v
```

Expected: fails with `AttributeError: module 'ingestion.refresh_synced_tables' has no attribute 'wait_until_online'`. Red.

### Task 4.2: Implement `wait_until_online` in refresh_synced_tables.py (green)

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py`.

- [ ] **Step 1:** Add the helper after the existing module-level functions (before `main()`). Also add the state constants at the top of the file, near the other module constants.

At the top of the file (with other constants around line 150):

```python
SYNCED_TABLE_ONLINE_STATE = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"

# Synced-table detailed_state values that mean "table failed to come online";
# distinguished from in-flight states by the fact that polling further is
# pointless — caller should fail loud.
_SYNCED_TABLE_TERMINAL_FAILURE_STATES: frozenset[str] = frozenset(
    {
        "SYNCED_TABLE_OFFLINE",
        "SYNCED_TABLE_OFFLINE_FAILED",
    }
)
```

Then add the function (e.g., after `_poll_pipeline` and before `main`):

```python
def wait_until_online(
    table_fqn: str,
    *,
    timeout_s: int = 600,
    poll_interval_s: int = 15,
) -> None:
    """Poll a Lakebase synced table until it reaches SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE.

    Not called by PR 4b directly — ships ready for future SDK-based synced-table
    recreation paths. See On-Deck entry for G2 + G3.

    Args:
        table_fqn: Fully-qualified Unity Catalog name of the synced table,
            e.g. ``"soccer_analytics.dev_gold.fct_action_values_synced"``.
        timeout_s: Maximum total wait time before raising ``TimeoutError``.
            Default 600s (10 min) — sufficient for most synced-table warm-ups.
        poll_interval_s: Seconds between status polls. Default 15s.

    Raises:
        TimeoutError: if the table does not reach the online state within
            ``timeout_s``. Message includes the table FQN, last-seen
            detailed_state, and elapsed time.
        RuntimeError: if the table hits a terminal failure state
            (``SYNCED_TABLE_OFFLINE``, ``SYNCED_TABLE_OFFLINE_FAILED``). Message
            includes the terminal state name.
        requests.HTTPError: propagated on 4xx/5xx from the status endpoint
            (including 404 when the table doesn't exist).
    """
    if not IDENTIFIER_RE.match(table_fqn.split(".")[-1]):
        # Guard against SQL-injection-shaped inputs in the interpolated URL.
        # Accept the full catalog.schema.table form but validate the last
        # segment for common sanity.
        raise ValueError(f"Invalid table_fqn last-segment: {table_fqn!r}")

    headers = _get_auth_headers()
    url = f"{_get_host()}/api/2.0/database/synced_tables/{table_fqn}"

    start = time.monotonic()
    last_state: str | None = None
    while True:
        resp = requests.get(url, headers=headers, verify=True, timeout=(10, 30))
        resp.raise_for_status()
        body = resp.json()
        detailed_state = body.get("status", {}).get("detailed_state")
        last_state = detailed_state

        if detailed_state == SYNCED_TABLE_ONLINE_STATE:
            return

        if detailed_state in _SYNCED_TABLE_TERMINAL_FAILURE_STATES:
            raise RuntimeError(
                f"Synced table {table_fqn} reached terminal failure state "
                f"{detailed_state!r}"
            )

        elapsed = time.monotonic() - start
        if elapsed > timeout_s:
            raise TimeoutError(
                f"Synced table {table_fqn} did not reach "
                f"{SYNCED_TABLE_ONLINE_STATE} within {timeout_s}s "
                f"(last detailed_state: {last_state!r}, elapsed: {elapsed:.1f}s)"
            )

        time.sleep(poll_interval_s)
```

- [ ] **Step 2:** Run tests:

```bash
uv run pytest src/tests/test_refresh_synced_tables.py::TestWaitUntilOnline -v
```

Expected: all four tests pass. Green.

- [ ] **Step 3:** Lint + type check:

```bash
uv run ruff check src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py
uv run pyright src/ingestion/refresh_synced_tables.py src/tests/test_refresh_synced_tables.py
```

Expected: zero new violations.

---

## Phase 5: On-Deck entry for G2 + G3

### Task 5.1: Append On-Deck entry

**Files:**
- Modify: the repo's TODO / On-Deck file (path resolved in Phase 0 Task 0.6).

- [ ] **Step 1:** Append the following entry to the file:

```markdown
### SDK synced-table path hardening (G2 + G3 from Kimball PR 4, 2026-04-23)

**Blocking condition:** any future PR that switches synced-table creation from
Terraform / UI to the `w.postgres.synced_tables.*` SDK path MUST close both
gaps before shipping.

**G2 (confirmed real during PR 4 brainstorming):** `src/ingestion/refresh_synced_tables.py:178`
hits `/api/2.0/database/synced_tables/` (legacy REST endpoint). SDK-created synced tables
live under `/api/2.0/postgres/synced_tables/`. An SDK-created table is not
addressable by the current refresh module.

**G3 (unverified):** `scripts/run_lakebase_grants.py` + `scripts/fix_event_log_ownership.py`
behavior post-SDK-create — unclear whether ADR-005 grants flow and event_log
ownership semantics hold on the new creation path. Needs empirical verification
via an SDK-create + grants run + event_log ownership check cycle.

**Preparatory work already landed:** G1 `wait_until_online(table_fqn, ...)` helper
in `src/ingestion/refresh_synced_tables.py` polls `status.detailed_state` for
`SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`. Reusable by whichever PR closes G2 + G3.

**References:** spec `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md` §2 "Explicitly out of scope" + §6.7 On-Deck entry detail.
```

- [ ] **Step 2:** If the file uses a table format (TODO table), adapt the entry to match that shape. The key fields are: title, blocking condition, two sub-items with their current verification state, references.

### Task 5.2: Commit Phase 4 + Phase 5 together (requires user approval)

Per D2, G1 helper + On-Deck entry share one commit (both are forward-looking / hygiene).

- [ ] **Step 1:**

```bash
git add src/ingestion/refresh_synced_tables.py \
        src/tests/test_refresh_synced_tables.py \
        docs/<ON_DECK_FILE_PATH_FROM_0_6>
git commit -m "feat(lakebase): G1 wait_until_online helper + G2/G3 On-Deck entry (PR 4b Phase 4+5)"
```

---

## Phase 6: Integration / smoke verification

### Task 6.1: Live dbt build against dev_gold

**Files:** None — live Databricks operation.

- [ ] **Step 1:** Build `fct_action_values` only (the migrated mart):

```bash
cd dbt_project
uv run python ../scripts/ensure_warehouse.py -- uv run dbt build --select fct_action_values+ --profiles-dir .
```

Expected: dbt build succeeds. Live CI on the PR branch (PR 4a) also runs full state:modified+ as a parallel safety check.

- [ ] **Step 2:** Re-run Phase 0 Task 0.2 DESCRIBE to see whether Lakebase auto-evolved the synced table schema:

```bash
# Repeat the Lakebase DESCRIBE from Task 0.2.
```

- [ ] **Step 3 (conditional — branches on D1 outcome):**
  - **(a) Lakebase auto-evolved** → continue to Task 6.2.
  - **(b) Lakebase did NOT auto-evolve** → run `uv run python scripts/maintain_synced_tables.py --catalog soccer_analytics --schema dev_gold` (includes Step 0.5 grants, refresh, indexes). Then re-check.

### Task 6.2: Verify live schema matches contract

**Files:** None — live test run.

- [ ] **Step 1:** Run the live-DESCRIBE test added in Phase 1 Task 1.4:

```bash
uv run pytest src/tests/test_marts_live_schema.py::TestFctActionValuesLiveSchema -v
```

Expected: green. If red: the Lakebase schema and UC schema are out of sync; investigate per Task 6.1 Step 3 (b).

### Task 6.3: Taipy local smoke on Player Impact page

**Files:** None — manual smoke.

- [ ] **Step 1:** Start Taipy locally:

```bash
cd hf_taipy_app
python src/main.py &
LOCAL_PID=$!
```

Open `http://localhost:7860`. Navigate to Player Impact page.

- [ ] **Step 2:** Verify each sub-view:
- **Rankings:** Select a competition; rankings table populates with non-zero rows; VAEP/90, Off per 90, Def per 90 columns all numeric.
- **Breakdown:** Select a competition; horizontal bar chart renders; total VAEP metric shows a non-dash value.
- **Timeline:** Select a specific match; scatter plot renders with positive + negative markers; halftime line visible.

- [ ] **Step 3:** Kill local Taipy:

```bash
kill $LOCAL_PID
```

Expected: all three sub-views render. If any fails: investigate Phase 3 before proceeding.

### Task 6.4: HF Jobs dry-run (gated on user approval — do NOT run without explicit approval)

**Files:** None.

- [ ] **Step 1 (gated):** If user approves, dry-run the publish in HF Jobs against dev_gold:

```bash
hf jobs uv run scripts/publish_spadl_vaep_hf.py \
    --flavor cpu-basic --timeout 30m \
    --secrets HF_TOKEN \
    --env DATABRICKS_HOST="$DATABRICKS_HOST" \
    --env DATABRICKS_TOKEN="$DATABRICKS_TOKEN" \
    --env DATABRICKS_SQL_WAREHOUSE_ID="$DATABRICKS_SQL_WAREHOUSE_ID"
```

Expected: HF job runs to completion; new spadl-vaep-action-values revision on HF Hub contains both new (match_key, competition_key) and legacy (match_id, competition_id) columns.

- [ ] **Step 2:** Verify on HF:

```bash
uv run --no-project --with requests python -c "
import requests
r = requests.get('https://huggingface.co/api/datasets/luxury-lakehouse/spadl-vaep-action-values', timeout=10)
print(r.json().get('lastModified'))
"
```

Expected: `lastModified` reflects the recent publish.

Manually download a sample Parquet partition and confirm the column set via `pyarrow`:

```bash
uv run python - <<'PY'
import pyarrow.parquet as pq
import urllib.request
# Download one partition — exact URL varies by data_source
url = "https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values/resolve/main/data/data_source=statsbomb/data.parquet"
out = "/tmp/sample.parquet"
urllib.request.urlretrieve(url, out)
tbl = pq.read_table(out)
cols = set(tbl.column_names)
assert "match_key" in cols and "competition_key" in cols
assert "match_id" in cols and "competition_id" in cols
print("Columns OK:", sorted(cols))
PY
```

Expected: both new and legacy columns present.

---

## Phase 7: Ship PR 4b

### Task 7.1: Open PR

- [ ] **Step 1:** Open the PR (requires user approval):

```bash
gh pr create \
  --base main \
  --title "feat(kimball): Action Values match_key migration + G1 + Finding D (PR 4b)" \
  --body "$(cat <<'EOF'
## Summary
- Migrates `fct_action_values` onto `match_key` / `competition_key` per ADR-011; retains legacy `match_id` / `competition_id` for the 90-day window (sunset 2026-07-22).
- Updates `publish_spadl_vaep_hf.py` for dual-column HF output.
- Fixes Finding D in `publish_xg_shots_hf.py:99` (`CAST` → `try_cast`).
- Updates Taipy Player Impact queries to filter on `match_key` / `competition_key`.
- Adds forward-looking G1 `wait_until_online` helper in `refresh_synced_tables.py`.
- Lands On-Deck entry for G2 + G3 (deferred SDK-path gaps).

## Test plan
- [x] `dbt parse` clean; `dbt build --select fct_action_values+` green on dev_gold.
- [x] Parser-level contract test (`test_fct_action_values_contract.py`) green.
- [x] Live-DESCRIBE test (`test_marts_live_schema.py::TestFctActionValuesLiveSchema`) green post-build.
- [x] Finding D regression test (`test_publish_xg_shots_hf.py::TestTryCastGuard`) green.
- [x] G1 helper tests (`test_refresh_synced_tables.py::TestWaitUntilOnline`) green.
- [x] Taipy local smoke on Player Impact page (Rankings + Breakdown + Timeline).
- [ ] HF Jobs dry-run of `publish_spadl_vaep_hf.py` against dev_gold — gated on explicit approval before running.

Part of Kimball PR 4 series. Depends on PR 4a (live dbt CI) being merged.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2:** After user approves merge:

```bash
# User merges via GH UI (or, with approval, gh pr merge --squash --delete-branch).
```

### Task 7.2: Post-merge operational steps

- [ ] **Step 1:** On next scheduled daily Databricks job run, verify `fct_action_values` builds clean with the new schema.

- [ ] **Step 2:** If Phase 6 Task 6.1 Step 3 path (b) was needed, confirm synced-table recreation + grants + indexes succeeded (`scripts/maintain_synced_tables.py` output captures this).

- [ ] **Step 3:** Deploy Taipy Space: staging first, E2E smoke (Player Impact), then production (requires user approval per `manage_space.py deploy` convention):

```bash
uv run python scripts/manage_space.py deploy staging
# Verify Player Impact page on staging URL
uv run python scripts/manage_space.py deploy production
```

### Task 7.3: Run publish cycle for spadl-vaep-action-values (gated)

- [ ] **Step 1:** With user approval, run the HF publish (pre-PR-4c, README auto-upload not yet in place — the README is stale until PR 4c lands):

```bash
hf jobs uv run scripts/publish_spadl_vaep_hf.py --flavor cpu-basic --timeout 30m ...
```

Note: the dual-column data lands on HF but the README warning isn't yet auto-uploaded. PR 4c closes this.

---

## Self-review findings (plan author notes)

**Spec coverage:** Spec §2 PR 4b scope maps to Phases 1–5. Spec §3 Decision 2 maps to D1 (live dbt CI — in PR 4a, consumed here). Spec §6.1 SQL shape is reproduced in Phase 1 Task 1.2. Spec §6.2 contract YAML maps to Phase 1 Task 1.3. Spec §6.3 publish script SQL maps to Phase 2 Task 2.3. Spec §6.4 Finding D maps to Phase 2 Tasks 2.1–2.2. Spec §6.5 Taipy queries maps to Phase 3. Spec §6.6 G1 helper maps to Phase 4. Spec §6.7 On-Deck entry maps to Phase 5. Spec §9 deploy sequence maps to Phase 7.

**Placeholders scan:** None. Phase 0 tasks are read-only verification that resolves D1–D5. D1 is a known unknown with a clear resolution path in Phase 6.

**Type consistency:** `RunResult` from PR 4a plan is not used here. `SYNCED_TABLE_ONLINE_STATE` + `_SYNCED_TABLE_TERMINAL_FAILURE_STATES` constants match test expectations. `wait_until_online` signature matches test mocks.

**Known ambiguities for execution time:**
- **Phase 0 Task 0.3 findings drive Phase 3 shape.** Before starting Phase 3, have a concrete reading of `hf_taipy_app/src/queries/defensive.py`. Without that, Phase 3 Tasks 3.1–3.3 can't be executed in order.
- **`_marts__models.yml` path.** The plan assumes `dbt_project/models/marts/_marts__models.yml` per PR 3 reference. If the project uses per-file YAMLs (`fct_action_values.yml`), Phase 1 Task 1.3 uses that file instead.
- **Lakebase auto-evolution behavior (D1).** Empirically verified in Phase 0 Task 0.2 + Phase 6 Task 6.1 — no guess in the plan.
- **On-Deck file path.** Resolved in Phase 0 Task 0.6 before Phase 5.
