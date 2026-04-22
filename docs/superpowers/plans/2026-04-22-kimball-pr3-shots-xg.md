# Kimball PR 3 — Shots + xG Migration + ADR-013 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `fct_shots`, `fct_xg_predictions`, and the Python-written `xg_predictions_v2` off smart-keyed `match_id` onto surrogate `match_key`/`competition_key` per ADR-011; promote xG v2 to a contract-enforced dbt mart (`fct_xg_predictions_v2`); codify the canonical Python → bronze → dbt staging → gold pattern for ML inference outputs as ADR-013.

**Architecture:** Surrogate keys resolve once at `fct_shots` (INNER JOIN to `dim_matches` via `provider`+`native_match_id`). All downstream prediction marts inherit `match_key`/`competition_key` via `INNER JOIN fct_shots ON shot_id` — never by recomputing the hash. Python writers emit only native identifiers + predictions; dbt owns surrogate resolution. `competition_id INT` is retained on migrated facts as a nullable legacy column until PR 8 sweep; `match_id` drops completely.

**Tech Stack:** dbt (Databricks adapter), Spark/Delta, PySpark (Python 3.10 serverless), Taipy GUI, Terraform (Databricks provider), pytest, HuggingFace Hub, PostgreSQL (Lakebase synced tables).

**Source spec:** `docs/superpowers/specs/2026-04-22-kimball-pr3-shots-xg-design.md` (uncommitted; commits with the plan as the first commit on the branch).

---

## Decisions required — resolve before/during execution

These three are load-bearing for the plan shape. Default positions below; override if you want.

| # | Decision | Default (this plan assumes) | Alternative |
|---|---|---|---|
| **D1** | Wheel version bump for PR 3 | **Accept 0.3.12 → 0.3.13** + run `scripts/bump_wheel.py` to sync 19 consumers. Spec §15 #11's "no bump" line was drafted before §6.1-§6.3 were fully enumerated — `src/ingestion/xg_model_v2.py` and `src/ingestion/xg_model.py` are both wheel-shipped so a bump is unavoidable if their bytes change. | Defer all of §6.1-§6.3 to a follow-up PR; PR 3 becomes dbt+Taipy+TF-only. This leaves `bronze.xg_predictions_v2` with the obsolete `match_id` column until that follow-up lands, and `fct_xg_predictions_v2` has to tolerate it at the staging layer. |
| **D2** | `bronze.xg_predictions_v2` 131,077-row migration path | **Enable `delta.columnMapping.mode = 'name'` if not already on, then `ALTER TABLE DROP COLUMN match_id`.** Preserves all 131,077 rows, zero downtime, sets SOP for all future Kimball migrations (PR 4-8). Phase 0.1 checks current mode; if already enabled, skip the enable step. See Phase 5.0. | DROP TABLE + rescore on next wf-xg-v2 (~$14 one-time, loses history, creates a data gap). Rejected — doesn't scale to tables in later PRs. |
| **D3** | HF dataset key-migration strategy (spec §6.4/§6.5) | **Dual-column with 90-day deprecation window, uniform across both HF datasets.** Every export emits BOTH `match_key` (new, primary) AND `match_id` (legacy via `LEFT JOIN dim_matches` → `native_match_id AS match_id`). README changelog declares `match_id` deprecated with removal-eligible date 2026-07-22 (≥90-day window). Follow-up PR in 2026Q3 drops `match_id`. Same pattern becomes SOP for PR 5 (`player_id`→`player_key`), PR 7, etc. Zero day-one consumer breakage (verified — `scripts/train_psxg_hf.py:55` keeps working; unknown external consumers keep working). | Uniform immediate rename (Hyrum's-Law-hostile); mixed per-dataset (inconsistent mental model — what we just rejected). |

---

## File structure map

### Created

| Path | Responsibility |
|---|---|
| `dbt_project/models/staging/stg_xg__predictions_v2.sql` | v2 staging view — dedup by shot_id, strip `_ingested_at`, expose `shot_id`+`competition_id`+v2 prediction columns. |
| `dbt_project/models/marts/fct_xg_predictions_v2.sql` | v2 gold mart. INNER JOIN `fct_shots` ON `shot_id` to inherit `match_key`+`competition_key`; contract-enforced; clustered by `match_key`; gated by `xg_v2_enabled=false` default. |
| `src/tests/test_dbt_shots_kimball_migration.py` | Mirrors `test_dbt_passes_kimball_migration.py`: fct_shots has `match_key` NOT `match_id`; per-provider row parity; `shot_id` uniqueness; `match_key` 100% non-null. |
| `src/tests/test_dbt_xg_v2_mart.py` | `fct_xg_predictions_v2` contract + `xg_set_encoder ∈ [0,1]` + CI bound ordering + INNER JOIN row preservation. |
| `src/tests/fixtures/xg_predictions_v2_bronze.json` | Bronze parser-level schema snapshot for G6 drop-safety pattern. |
| `docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md` | New ADR — consumer-side counterpart to ADR-012. |

### Modified

| Path | Reason |
|---|---|
| `dbt_project/models/intermediate/int_unified_shots.sql` | Ephemeral → view; per-source CTEs gain `provider`/`native_match_id`; final CTE joins `dim_matches` and drops `match_id`, emits `match_key`. |
| `dbt_project/models/intermediate/int_running_score.sql` | **Added during Phase 2 execution (cross-model dependency).** `goals` CTE originally read `s.match_id` from int_unified_shots; Phase 1 dropped that column. Rewire to `s.match_key`; recover `match_id` via the existing `match_teams_keyed` join. Output schema (match_id + match_key + team_ids + scores) preserved for fct_action_values (PR 4 will migrate that consumer). |
| `dbt_project/models/marts/fct_shots.sql` | Replace `match_id` with `match_key` NOT NULL; add `competition_key` nullable; retain `competition_id INT` legacy; swap per-provider match CTEs for single `match_attrs` CTE reading `dim_matches`; `liquid_clustered_by=['match_key']`. |
| `dbt_project/tests/assert_coordinates_in_bounds.sql` | **Added during Phase 2 execution.** Singular dbt test's debug SELECT emitted `match_id`; swapped to `match_key`. Assertion logic unchanged. |
| `dbt_project/tests/assert_xg_between_0_and_1.sql` | **Added during Phase 2 execution.** Same pattern — `match_id` → `match_key` in debug SELECT. |
| `dbt_project/models/marts/fct_xg_predictions.sql` | Final SELECT pulls keys from `fct_shots` via INNER JOIN; drop `match_id` and `competition_id` from staging-side SELECT. `liquid_clustered_by=['match_key']`. |
| `dbt_project/models/staging/stg_xg__predictions.sql` | Drop `match_id`+`competition_id` from SELECT; dedup on `shot_id` only. (Bronze schema untouched.) |
| `dbt_project/models/staging/_xg__sources.yml` | Add second table entry `xg_predictions_v2`. |
| `dbt_project/models/marts/_marts__models.yml` | fct_shots contract update; fct_xg_predictions contract update; NEW fct_xg_predictions_v2 contract entry. |
| `dbt_project/models/intermediate/_intermediate__models.yml` | Add `int_unified_shots` entry. |
| `src/ingestion/xg_model_v2.py` | Shrink `_RESULTS_SCHEMA` + UDF output + `_load_shots_with_context` SQL — drop `match_id`. Keep `competition_id` (still present on fct_shots as legacy INT). |
| `src/ingestion/xg_model.py` (v1) | Read-SQL: `s.match_id` → `s.match_key` + `LEFT JOIN dim_matches dm ON s.match_key = dm.match_key` + emit `cast(dm.native_match_id as bigint) AS match_id` for bronze back-compat. |
| `src/ingestion/export_shots_on_target.py` | **Dual-column (D3):** SELECT pulls `s.match_key` (new primary) + LEFT JOIN dim_matches; published parquet has BOTH `match_key` and legacy `match_id`. 90-day deprecation window on `match_id`. |
| `scripts/publish_xg_shots_hf.py` | **Dual-column (D3):** same pattern as `export_shots_on_target.py`. Published parquet has BOTH `match_key` and legacy `match_id`. Dataset README gets the canonical deprecation changelog (reused verbatim on future key-migration PRs). |
| `src/ingestion/refresh_synced_tables.py` | Add `("fct_xg_predictions_v2_synced", None)` to `SYNCED_TABLES`. |
| `terraform/modules/synced_tables/main.tf` | Add `databricks_database_synced_database_table.fct_xg_predictions_v2` resource. |
| `terraform/modules/synced_tables/outputs.tf` | One line in outputs map. |
| `scripts/create_indexes.py` | Drop obsolete `competition_id`/`match_id` composites on `fct_shots_synced` + `fct_xg_predictions_synced`; add `competition_key`/`match_key` composites; add `fct_xg_predictions_v2_synced` index. |
| `workflow-cards/wf-xg-v2.yaml` | `outputs.tables` gains gold-mart entry with `dbt_model: fct_xg_predictions_v2`. `outputs.models` UNTOUCHED. |
| `hf_taipy_app/src/queries/shots.py` | `fetch_shots(competition_key, ...)`; `fetch_xg_predictions(competition_key)`. |
| `hf_taipy_app/src/queries/match.py` | `fetch_shots_timeline(match_key)`; SELECT emits `match_key` not `match_id`. |
| `hf_taipy_app/src/state/shot_map.py` | `get_comp_id` → `get_competition_key`; `comp_id` → `comp_key`. |
| `hf_taipy_app/src/state/match_summary.py` | Call site for `fetch_shots_timeline` + any downstream `shots["match_id"]` references in `build_xg_race_figure`. |
| `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` | PR 7 row amendment: add pausa promotion note; footnote referencing ADR-013. |
| `CLAUDE.md` | One bullet under "Architecture Principles" linking ADR-013. |
| `notebooks/train_xg_model.py` | Originally planned (spec §6.6) but verified zero `match_id` references in the file — **task removed from plan as no-op**. |
| `src/shared/constants.py` | Spec §6.2 `DEFAULT_BRONZE_SCHEMA = "dev_bronze"` proposed addition — **removed from plan**. TF `--schema bronze` is already in place (`terraform/modules/workflows/main.tf:499`); xg_model_v2.py receives schema via CLI arg and doesn't need a constant default. |
| `pyproject.toml`, `src/shared/wheel.py`, Terraform wheel consumers, PEP 723 script headers, `deploy.sh` | Wheel bump 0.3.12 → 0.3.13 via `scripts/bump_wheel.py`. 19 consumers per memory `reference_wheel_consumers`. |
| Updated tests (fixture edits): `src/tests/test_xg_model_v2.py`, `src/tests/test_queries_match_extended.py`, `src/tests/test_bronze_live_schema.py`, `src/tests/test_staging_coverage.py`, `src/tests/test_card_dbt_model_field.py`, `src/tests/test_card_cost_phase_parity.py`, `src/tests/test_card_parity_with_terraform.py` | Scaffolding/parity updates driven by the schema + card changes. |

### Explicitly NOT modified (Chesterton's Fence)

- `dbt_project/models/staging/stg_statsbomb__shots.sql`
- `dbt_project/models/staging/stg_wyscout__events.sql` (shots filter)
- `bronze.xg_predictions` (v1 writer continues emitting `match_id`; bronze shape frozen for historical comparability)
- `bronze.psxg_predictions` (goalkeeper path; out of scope)
- `dbt_project/models/intermediate/int_running_score.sql` (PR 2 migrated already)
- `src/ingestion/artifact_deploy.py` (ADR-012 producer-side; untouched)
- `scripts/train_xg_v2_hf.py`, `scripts/train_xg_model_hf.py` (producer-side; ADR-012)
- `fct_pausa_values` (deferred to PR 7 per ADR-011 amendment)
- `scripts/train_psxg_hf.py` (D3 alternative would touch it; default D3 preserves its contract)

---

## Phase 0: Pre-flight verification (read-only; Databricks SQL)

Every downstream phase depends on these answers. Do not skip. Uses rotated-and-propagated tokens (GH Actions + local env both refreshed 2026-04-22).

### Task 0.1: Verify `bronze.xg_predictions_v2` Delta properties (resolves D2)

**Files:** None — `uv run python -c ...` against Databricks SQL.

- [ ] **Step 1:** Run, from repo root:

```bash
uv run python - <<'PY'
import os
from databricks import sql
with sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
    http_path=os.environ["DATABRICKS_HTTP_PATH"].lstrip("/"),
    access_token=os.environ["DATABRICKS_TOKEN"],
) as conn, conn.cursor() as cur:
    cur.execute("DESCRIBE TABLE soccer_analytics.bronze.xg_predictions_v2")
    print("=== SCHEMA ===")
    for r in cur.fetchall(): print(r)
    cur.execute("SHOW TBLPROPERTIES soccer_analytics.bronze.xg_predictions_v2")
    print("=== TBLPROPERTIES ===")
    for r in cur.fetchall(): print(r)
    cur.execute("SELECT count(*) FROM soccer_analytics.bronze.xg_predictions_v2")
    print("=== ROW COUNT ===", cur.fetchone())
PY
```

Expected output (schema section) — current row should include `match_id BIGINT`. Look for `delta.columnMapping.mode` in TBLPROPERTIES. Row count should be ~131,077 per spec §14 Risk #8.

- [ ] **Step 2:** Record the finding. Drives the size of Phase 5.0's schema-migration step (per D2, the answer is always ALTER — this just determines whether you need the enable step first):
  - **(a)** `delta.columnMapping.mode = 'name'` already set → Phase 5.0 runs just the `ALTER TABLE DROP COLUMN match_id`.
  - **(b)** Property absent (expected — the table predates this convention) → Phase 5.0 runs two statements: enable column-mapping + upgrade protocol versions, then drop the column. Both online.
  - **(c)** Table unexpectedly empty (0 rows) → (b) still applies; ALTER is cheap on empty tables too.

### Task 0.2: Review the `create_indexes.py` INDEXES declarative list

**Key finding (Phase 0.2, 2026-04-22):** `scripts/create_indexes.py` is declarative — `CREATE INDEX IF NOT EXISTS` over the `INDEXES` list at lines 48-207. No `--status`/read-only flag exists; the list IS the source of truth. Per CLAUDE.md's Lakebase-maintenance convention, UI synced-table recreation drops all custom indexes, and the daily `.github/workflows/lakebase-grants.yml` action re-applies them from this list. **No live-state capture is needed for planning** — PR 3 edits the list; recreation handles the drop; cron re-applies.

**Files:** None — code review only.

- [ ] **Step 1:** Pre-plan review identified the PR-3-relevant entries (captured 2026-04-22):
  - `fct_xg_predictions_synced`: `idx_xg_predictions_match` on `(match_id)` and `idx_xg_predictions_comp` on `(competition_id)` — both need column edits.
  - `fct_shots_synced`: `idx_shots_comp_team_player` on `(competition_id, team_id, player_id)` → migrate leading column to `competition_key`. `idx_shots_player_team` on `(player_id, team_id)` — unchanged (no migrating columns). No `match_id`-keyed index exists on `fct_shots_synced` today.
  - No entries for `fct_xg_predictions_v2_synced` — Task 8.3 adds one new entry.

### Task 0.3: Confirm `get_competition_key` helper exists in `state.shared`

**Files:** None — grep only.

- [ ] **Step 1:** Grep for prior-PR helper addition:

```bash
grep -n "get_competition_key\|get_comp_id" hf_taipy_app/src/state/shared.py
```

Expected: PR 2 ("competition surrogate") should have added `get_competition_key`. Two outcomes:
  - **(a)** Both `get_comp_id` and `get_competition_key` exist → just use `get_competition_key` in shot_map.py (Phase 7).
  - **(b)** Only `get_comp_id` exists → Phase 7 adds `get_competition_key` to `state.shared.py` with the same LOV-lookup shape, and updates its public `__all__`.

### Task 0.4: Locate `stg_xg__predictions.sql`

**Files:** None.

- [ ] **Step 1:**

```bash
find dbt_project/models -name "stg_xg__predictions.sql"
find dbt_project/models -name "_xg__sources.yml"
```

Expected: two paths returned (one each). Record them. They're likely under `dbt_project/models/staging/xg/` or similar. The `{{ ref('stg_xg__predictions') }}` in `fct_xg_predictions.sql` confirms the model exists; we just need the file path.

### Task 0.5: Smoke-check Taipy staging on `main`

**Files:** None.

- [ ] **Step 1:** Ensure we have a green baseline BEFORE starting edits so any regression is attributable to this PR's changes:

```bash
uv run python scripts/manage_space.py deploy staging --ref main
```

Then open the staging Space URL, visit Shot-Map + Match Summary + Action Values pages, confirm no errors. 5-minute manual smoke.

Expected: Taipy app loads all pages. Shot Map renders shots. Match Summary renders xG race chart. If any failure, stop and investigate before touching PR 3.

---

## Phase 1: dbt foundation — `int_unified_shots` upgrade

### Task 1.1: Migrate `int_unified_shots` to view + add `match_key`

**Files:**
- Modify: `dbt_project/models/intermediate/int_unified_shots.sql`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`

- [ ] **Step 1:** Rewrite `int_unified_shots.sql`. Full target file:

```sql
{{ config(materialized='view') }}
-- int_unified_shots.sql
-- Union StatsBomb and Wyscout shot data into a common schema, keyed on match_key.
--
-- Materialization: view (up from ephemeral — now required as a durable target
-- for the Kimball join to dim_matches). The row count is bounded by the
-- underlying StatsBomb + Wyscout shot event counts; view-refresh cost is
-- negligible.

with statsbomb_shots as (

    select
        event_id,
        'statsbomb'                                  as provider,
        cast(match_id as string)                     as native_match_id,
        match_id                                     as _legacy_match_id,
        player_id,
        team_id,
        period,
        minute,
        second,
        location_x,
        location_y,
        end_location_x,
        end_location_y,
        end_location_z,
        shot_outcome,
        shot_body_part,
        shot_technique,
        shot_type,
        statsbomb_xg,
        is_first_time,
        play_pattern,
        distance_to_goal,
        shot_angle,
        'statsbomb'                                  as data_source

    from {{ ref('stg_statsbomb__shots') }}

),

wyscout_shots as (

    select
        event_sk                                     as event_id,
        'wyscout'                                    as provider,
        cast(match_id as string)                     as native_match_id,
        cast(match_id as bigint)                     as _legacy_match_id,
        cast(player_id as int)                       as player_id,
        cast(team_id as int)                         as team_id,
        period,
        cast(floor(event_sec / 60) as int)           as minute,
        cast(cast(event_sec as int) % 60 as int)     as second,
        start_x                                      as location_x,
        start_y                                      as location_y,
        end_x                                        as end_location_x,
        end_y                                        as end_location_y,
        cast(null as double)                         as end_location_z,
        case when is_goal then 'Goal' else 'No Goal' end as shot_outcome,
        case
            when sub_event_type like '%Head%' then 'Head'
            when sub_event_type like '%Right%' then 'Right Foot'
            when sub_event_type like '%Left%' then 'Left Foot'
            else 'Unknown'
        end                                          as shot_body_part,
        cast(null as string)                         as shot_technique,
        sub_event_type                               as shot_type,
        cast(null as double)                         as statsbomb_xg,
        cast(null as boolean)                        as is_first_time,
        cast(null as string)                         as play_pattern,
        {{ distance_to_goal('start_x', 'start_y') }} as distance_to_goal,
        {{ shot_angle('start_x', 'start_y') }}       as shot_angle,
        'wyscout'                                    as data_source

    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Shot'

),

unified as (

    select * from statsbomb_shots
    union all
    select * from wyscout_shots

),

keyed as (

    select
        u.event_id,
        dm.match_key,
        u.provider,
        u.native_match_id,
        u.player_id,
        u.team_id,
        u.period,
        u.minute,
        u.second,
        u.location_x,
        u.location_y,
        u.end_location_x,
        u.end_location_y,
        u.end_location_z,
        u.shot_outcome,
        u.shot_body_part,
        u.shot_technique,
        u.shot_type,
        u.statsbomb_xg,
        u.is_first_time,
        u.play_pattern,
        u.distance_to_goal,
        u.shot_angle,
        u.data_source
    from unified u
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = u.provider
        and dm.native_match_id = u.native_match_id

)

select * from keyed
```

- [ ] **Step 2:** Add entry to `dbt_project/models/intermediate/_intermediate__models.yml`:

```yaml
  - name: int_unified_shots
    description: >
      Union of StatsBomb and Wyscout shot events, keyed on match_key via
      an INNER JOIN to dim_matches on (provider, native_match_id). Materialized
      as a view (upgraded from ephemeral in PR 3 of the ADR-011 migration).
      Downstream: fct_shots.
    columns:
      - name: event_id
        description: Source-native event surrogate (pre-Kimball).
      - name: match_key
        description: Kimball surrogate BIGINT FK to dim_matches.
        data_tests:
          - not_null
      - name: provider
        description: Source provider — 'statsbomb' or 'wyscout'.
        data_tests:
          - accepted_values:
              values: ['statsbomb', 'wyscout']
      - name: native_match_id
        description: Provider-native match id as string.
        data_tests:
          - not_null
```

- [ ] **Step 3:** Locally validate dbt parses. Parse is compile-time only — no warehouse needed, so skip `ensure_warehouse.py`. The repo's `profiles.yml` lives in `dbt_project/` and resolves env vars at runtime:

```bash
uv run dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: `Running with dbt=1.11.x` header + success; exit 0.

---

## Phase 2: `fct_shots` Kimball migration

### Task 2.1: Write failing test `test_dbt_shots_kimball_migration.py`

**Files:**
- Create: `src/tests/test_dbt_shots_kimball_migration.py`

- [ ] **Step 1:** Full test file:

```python
"""PR 3 Kimball migration regression tests for fct_shots.

Mirrors test_dbt_passes_kimball_migration.py (PR 2). Asserts that after the
migration:
  - match_key replaces match_id in the mart contract
  - competition_key is present; competition_id legacy INT stays nullable
  - shot_id uniqueness preserved
  - match_key is 100% non-null (INNER JOIN to dim_matches in
    int_unified_shots guarantees this; the test is a live DB check).
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _cursor():
    from databricks import sql
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"].lstrip("/"),
        access_token=os.environ["DATABRICKS_TOKEN"],
    ).cursor()


def test_fct_shots_contract_has_match_key_not_match_id() -> None:
    yml = Path("dbt_project/models/marts/_marts__models.yml").read_text(encoding="utf-8")
    block_start = yml.index("- name: fct_shots")
    block = yml[block_start:block_start + 4000]
    assert "match_key" in block, "match_key must appear in fct_shots contract"
    assert "- name: match_id" not in block, (
        "match_id must be removed from fct_shots contract (spec §5.2, §5.8)"
    )
    assert "competition_key" in block, "competition_key must be added to fct_shots contract"
    assert "competition_id" in block, "competition_id legacy INT stays nullable in the contract"


def test_fct_shots_sql_uses_match_key_not_match_id() -> None:
    sql_text = Path("dbt_project/models/marts/fct_shots.sql").read_text(encoding="utf-8")
    assert "match_key" in sql_text
    # The final SELECT must not emit match_id. We allow the string elsewhere only
    # in comments referencing legacy behavior.
    import re
    final_select_block = sql_text[sql_text.rindex("final as"):]
    assert not re.search(r"\bmatch_id\b", final_select_block), (
        "fct_shots.sql final SELECT must not emit match_id (spec §5.2)"
    )
    assert "liquid_clustered_by=['match_key']" in sql_text


def test_fct_shots_live_match_key_non_null() -> None:
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset — live check unavailable")
    cur = _cursor()
    cur.execute(
        "SELECT count(*) FROM soccer_analytics.dev_gold.fct_shots WHERE match_key IS NULL"
    )
    (nulls,) = cur.fetchone()
    assert nulls == 0, f"fct_shots has {nulls} rows with NULL match_key — int_unified_shots INNER JOIN should prevent this"


def test_fct_shots_live_shot_id_unique() -> None:
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset — live check unavailable")
    cur = _cursor()
    cur.execute(
        "SELECT shot_id, count(*) c FROM soccer_analytics.dev_gold.fct_shots "
        "GROUP BY shot_id HAVING count(*) > 1 LIMIT 5"
    )
    dupes = cur.fetchall()
    assert not dupes, f"fct_shots has duplicate shot_id: {dupes}"


def test_fct_shots_live_per_provider_parity_nonzero() -> None:
    """Both StatsBomb and Wyscout provider rows should survive the migration."""
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset — live check unavailable")
    cur = _cursor()
    cur.execute(
        "SELECT data_source, count(*) FROM soccer_analytics.dev_gold.fct_shots "
        "GROUP BY data_source"
    )
    counts = dict(cur.fetchall())
    assert counts.get("statsbomb", 0) > 0, "StatsBomb shots missing from fct_shots"
    assert counts.get("wyscout", 0) > 0, "Wyscout shots missing from fct_shots"
```

- [ ] **Step 2:** Run:

```bash
uv run pytest src/tests/test_dbt_shots_kimball_migration.py -v
```

Expected: **FAIL.** `test_fct_shots_contract_has_match_key_not_match_id` fails because the yml still has `match_id`. `test_fct_shots_sql_uses_match_key_not_match_id` fails for the same reason on the SQL. Live checks may also fail with "column match_key does not exist" if they resolve — that's also expected.

### Task 2.2: Migrate `fct_shots.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_shots.sql`

- [ ] **Step 1:** Full replacement. Preserve all business logic (running_score join, _score_rn row-pick, game_state derivation); swap `match_id` → `match_key` and replace per-provider match CTEs with a single `match_attrs` CTE reading `dim_matches`:

```sql
{{ config(
    materialized='incremental',
    unique_key='shot_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge'
) }}
-- fct_shots.sql
-- Gold-layer shot fact table with xG features for ML model training.
-- Keyed on match_key (Kimball surrogate; see ADR-011).

with unified_shots as (

    select * from {{ ref('int_unified_shots') }}

),

match_attrs as (

    select
        match_key,
        competition_key,
        competition_id,          -- legacy INT nullable; retained until PR 8
        season_id
    from {{ ref('dim_matches') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

shots_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key(['unified_shots.event_id', 'unified_shots.data_source']) }} as shot_id,

        unified_shots.match_key,
        unified_shots.player_id,
        unified_shots.team_id,

        match_attrs.competition_key,
        cast(match_attrs.competition_id as int) as competition_id,
        cast(match_attrs.season_id as int)      as season_id,

        unified_shots.period,
        unified_shots.minute,
        unified_shots.second,

        unified_shots.location_x,
        unified_shots.location_y,
        unified_shots.end_location_x,
        unified_shots.end_location_y,
        unified_shots.end_location_z,

        unified_shots.shot_outcome,
        unified_shots.shot_body_part,
        unified_shots.shot_technique,
        unified_shots.shot_type,

        case when unified_shots.shot_outcome = 'Goal' then 1 else 0 end as is_goal,

        unified_shots.distance_to_goal,
        unified_shots.shot_angle,
        unified_shots.is_first_time,
        unified_shots.play_pattern,
        unified_shots.statsbomb_xg,
        unified_shots.data_source,

        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id as _rs_home_team_id,

        row_number() over (
            partition by unified_shots.event_id, unified_shots.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        ) as _score_rn

    from unified_shots
    left join match_attrs
        on unified_shots.match_key = match_attrs.match_key
    left join running_score rs
        on unified_shots.match_key = rs.match_key
        and (
            rs.period < unified_shots.period
            or (rs.period = unified_shots.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_shots.minute * 60 + unified_shots.second))
        )

),

final as (

    select
        shot_id,
        match_key,
        player_id,
        team_id,
        competition_key,
        competition_id,
        season_id,
        period,
        minute,
        second,
        location_x,
        location_y,
        end_location_x,
        end_location_y,
        end_location_z,
        shot_outcome,
        shot_body_part,
        shot_technique,
        shot_type,
        is_goal,
        distance_to_goal,
        shot_angle,
        is_first_time,
        play_pattern,
        statsbomb_xg,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end as game_state,
        data_source
    from shots_with_score
    where _score_rn = 1

)

select * from final
```

Notes:
- The incremental skip-predicate `where match_id not in (select distinct match_id from {{ this }})` was removed — the MERGE strategy on `shot_id` already prevents duplicates, and the predicate referenced the old key.
- `int_running_score` was migrated to `match_key` in PR 2 (per the memory + commit `7dd5a9b` subject line "passes + line-breaking + match summary on match_key; competition surrogate (PR 2)").

- [ ] **Step 2:** Update `_marts__models.yml`. Find the `fct_shots` block and replace/augment its columns. Concrete edit guide — the block currently enumerates `match_id` as a column; change to:

```yaml
  - name: fct_shots
    description: >
      Gold-layer shot fact keyed on match_key (Kimball, ADR-011). Contains xG
      features, is_goal target, game_state context, and per-shot player/team/
      competition/season attributes. Migrated from smart-keyed match_id in
      PR 3 of the staged Kimball rollout. competition_id legacy INT retained
      nullable until PR 8 sweep.
    config:
      contract:
        enforced: true
    columns:
      - name: shot_id
        description: Surrogate key — md5(event_id || '-' || data_source).
        data_type: string
        data_tests: [not_null, unique]
      - name: match_key
        description: Kimball surrogate BIGINT FK to dim_matches.
        data_type: bigint
        data_tests: [not_null]
      - name: player_id
        description: StatsBomb/Wyscout native player id (int).
        data_type: int
      - name: team_id
        description: StatsBomb/Wyscout native team id (int).
        data_type: int
      - name: competition_key
        description: Kimball surrogate BIGINT FK to dim_competitions.
        data_type: bigint
      - name: competition_id
        description: Legacy INT competition id. Nullable. Retained for bronze back-compat; will be removed in PR 8 sweep.
        data_type: int
      - name: season_id
        description: Legacy INT season id.
        data_type: int
      # ... (preserve the remaining fct_shots columns below this line exactly
      # as currently specified: period, minute, second, location_x,
      # location_y, end_location_x, end_location_y, end_location_z,
      # shot_outcome, shot_body_part, shot_technique, shot_type, is_goal,
      # distance_to_goal, shot_angle, is_first_time, play_pattern,
      # statsbomb_xg, game_state, data_source, with their existing data_type
      # and any data_tests — unchanged from before this PR)
      - name: data_source
        description: Provenance — 'statsbomb' or 'wyscout'.
        data_type: string
        data_tests:
          - accepted_values:
              values: ['statsbomb', 'wyscout']
```

The executor must preserve the non-key columns verbatim from the existing contract. Only add `match_key`+`competition_key`, drop `match_id`, mark `competition_id` as legacy. Every other column keeps its existing `data_type` and `data_tests`.

- [ ] **Step 3:** Run the tests:

```bash
uv run pytest src/tests/test_dbt_shots_kimball_migration.py::test_fct_shots_contract_has_match_key_not_match_id src/tests/test_dbt_shots_kimball_migration.py::test_fct_shots_sql_uses_match_key_not_match_id -v
```

Expected: PASS on both static-file tests. Live tests still skip or fail until we rebuild.

- [ ] **Step 4:** Build fct_shots end-to-end against dev:

```bash
uv run python scripts/ensure_warehouse.py -- dbt build --project-dir dbt_project --profiles-dir dbt_project --select +fct_shots
```

Expected: `OK created sql table model dev_gold.fct_shots` + data tests PASS. Watch for contract-enforced schema diffs (will fail the build if yml doesn't match SQL output).

- [ ] **Step 5:** Re-run live tests:

```bash
uv run pytest src/tests/test_dbt_shots_kimball_migration.py -v
```

Expected: all 5 tests PASS.

---

## Phase 3: `fct_xg_predictions` (v1) restructure

### Task 3.1: Simplify staging

**Files:**
- Modify: `dbt_project/models/staging/stg_xg__predictions.sql` (path from Phase 0.4)

- [ ] **Step 1:** Current staging selects `shot_id, match_id, competition_id, xg_logistic, xg_gradient_boosted`. Rewrite to strip keys:

```sql
{{ config(materialized='view') }}

with latest as (

    select
        shot_id,
        xg_logistic,
        xg_gradient_boosted,
        _ingested_at,
        row_number() over (partition by shot_id order by _ingested_at desc) as _rn
    from {{ source('xg', 'xg_predictions') }}

)

select
    shot_id,
    xg_logistic,
    xg_gradient_boosted
from latest
where _rn = 1
```

(The exact existing staging will already have a dedup pattern; preserve whatever exact dedup convention it uses and only drop the `match_id`+`competition_id` columns. Read the file first in Phase 0.4, then do a minimal-diff edit.)

### Task 3.2: Restructure `fct_xg_predictions.sql`

**Files:**
- Modify: `dbt_project/models/marts/fct_xg_predictions.sql`

- [ ] **Step 1:** Full replacement:

```sql
-- fct_xg_predictions.sql
-- Custom v1 xG predictions (logistic + XGBoost). Keys inherited by INNER JOIN
-- to fct_shots on shot_id per ADR-013. One row per scored shot.

{{ config(
    materialized='table',
    enabled=var('xg_model_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,        -- legacy INT nullable (inherited)
    p.xg_logistic,
    p.xg_gradient_boosted

from {{ ref('stg_xg__predictions') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
```

- [ ] **Step 2:** Update `_marts__models.yml` `fct_xg_predictions` block. Replace `match_id` column entry with `match_key` + `competition_key`; retain `competition_id` legacy; preserve xG test expectations on both v1 columns.

- [ ] **Step 3:** Build:

```bash
uv run python scripts/ensure_warehouse.py -- dbt build --project-dir dbt_project --profiles-dir dbt_project --select +fct_xg_predictions --vars 'xg_model_enabled: true'
```

Expected: OK + data tests pass.

---

## Phase 4: `fct_xg_predictions_v2` new mart

### Task 4.1: Add bronze source entry

**Files:**
- Modify: `dbt_project/models/staging/_xg__sources.yml` (path from Phase 0.4)

- [ ] **Step 1:** Under the existing `sources: - name: xg` block, under `tables:`, append:

```yaml
      - name: xg_predictions_v2
        description: >
          Raw v2 xG predictions written by compute_xg_model_v2 (Deep Sets set
          encoder + MC dropout confidence intervals). Columns:
          shot_id, competition_id, xg_set_encoder, xg_ci_lower, xg_ci_upper,
          _ingested_at. match_id was dropped from the writer schema in PR 3
          of the ADR-011 migration (historical rows may have had match_id;
          see ADR-013 for the consumer-side inference-table pattern).
        loaded_at_field: _ingested_at
        freshness:
          warn_after: {count: 48, period: hour}
          error_after: {count: 7, period: day}
```

### Task 4.2: Create `stg_xg__predictions_v2.sql`

**Files:**
- Create: `dbt_project/models/staging/stg_xg__predictions_v2.sql` (same directory as the v1 staging; Phase 0.4 will have located it)

- [ ] **Step 1:** Full file:

```sql
{{ config(
    materialized='view',
    enabled=var('xg_v2_enabled', false)
) }}

with latest as (

    select
        shot_id,
        xg_set_encoder,
        xg_ci_lower,
        xg_ci_upper,
        _ingested_at,
        row_number() over (partition by shot_id order by _ingested_at desc) as _rn
    from {{ source('xg', 'xg_predictions_v2') }}

)

select
    shot_id,
    xg_set_encoder,
    xg_ci_lower,
    xg_ci_upper
from latest
where _rn = 1
```

### Task 4.3: Create `fct_xg_predictions_v2.sql`

**Files:**
- Create: `dbt_project/models/marts/fct_xg_predictions_v2.sql`

- [ ] **Step 1:** Full file:

```sql
-- fct_xg_predictions_v2.sql
-- Gold-layer v2 xG predictions (set encoder + MC dropout CIs). Keys inherited
-- via INNER JOIN to fct_shots on shot_id per ADR-013.
--
-- First mart applying ADR-013 ("ML inference outputs flow Python writer →
-- bronze → dbt staging → gold with contract: enforced: true"). See
-- docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md.

{{ config(
    materialized='table',
    enabled=var('xg_v2_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,
    p.xg_set_encoder,
    p.xg_ci_lower,
    p.xg_ci_upper

from {{ ref('stg_xg__predictions_v2') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
```

### Task 4.4: Add mart contract

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1:** Append a new `- name: fct_xg_predictions_v2` block with contract enforced and column types mirroring the SELECT. Include `data_tests` on `xg_set_encoder` (range 0–1 via `dbt_utils.expression_is_true` or `dbt_expectations.expect_column_values_to_be_between`), `xg_ci_lower`/`xg_ci_upper` (range 0–1), and `not_null` on `shot_id` + `match_key`.

Exact block:

```yaml
  - name: fct_xg_predictions_v2
    description: >
      Gold-layer v2 xG predictions (Deep Sets set encoder with MC dropout
      confidence intervals). Keys inherited from fct_shots via INNER JOIN
      on shot_id. First mart under ADR-013 (ML inference outputs pattern).
      Enabled via var 'xg_v2_enabled' (default false; flip in job config).
    config:
      contract:
        enforced: true
    columns:
      - name: shot_id
        description: md5(event_id || '-' || data_source). FK to fct_shots.shot_id.
        data_type: string
        data_tests: [not_null, unique]
      - name: match_key
        description: Kimball surrogate BIGINT (inherited from fct_shots).
        data_type: bigint
        data_tests: [not_null]
      - name: competition_key
        description: Kimball competition surrogate BIGINT (inherited).
        data_type: bigint
      - name: competition_id
        description: Legacy INT competition id. Nullable. Inherited.
        data_type: int
      - name: xg_set_encoder
        description: v2 Deep Sets + MC dropout mean xG prediction.
        data_type: double
        data_tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND xg_set_encoder <= 1"
              config:
                where: xg_set_encoder is not null
      - name: xg_ci_lower
        description: v2 xG 95% CI lower bound.
        data_type: double
        data_tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND xg_ci_lower <= 1"
              config:
                where: xg_ci_lower is not null
      - name: xg_ci_upper
        description: v2 xG 95% CI upper bound.
        data_type: double
        data_tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 AND xg_ci_upper <= 1"
              config:
                where: xg_ci_upper is not null
```

### Task 4.5: Write `test_dbt_xg_v2_mart.py`

**Files:**
- Create: `src/tests/test_dbt_xg_v2_mart.py`

- [ ] **Step 1:** Full file:

```python
"""Static + live regression tests for fct_xg_predictions_v2."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _cursor():
    from databricks import sql
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"].lstrip("/"),
        access_token=os.environ["DATABRICKS_TOKEN"],
    ).cursor()


def test_mart_file_exists() -> None:
    assert Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").is_file()


def test_mart_sql_inner_joins_fct_shots_on_shot_id() -> None:
    text = Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").read_text(encoding="utf-8")
    assert "inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id" in text.replace("\n", " ").replace("  ", " ")
    assert "s.match_key" in text
    assert "s.competition_key" in text
    assert "contract={'enforced': true}" in text


def test_mart_contract_has_feature_range_tests() -> None:
    yml = Path("dbt_project/models/marts/_marts__models.yml").read_text(encoding="utf-8")
    idx = yml.index("- name: fct_xg_predictions_v2")
    block = yml[idx:idx + 3000]
    for col in ("xg_set_encoder", "xg_ci_lower", "xg_ci_upper"):
        assert col in block, f"contract block missing column {col}"
    assert "expression_is_true" in block, "range checks missing"


def test_mart_live_ci_bound_ordering() -> None:
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset — live check unavailable")
    cur = _cursor()
    try:
        cur.execute(
            "SELECT count(*) FROM soccer_analytics.dev_gold.fct_xg_predictions_v2 "
            "WHERE xg_ci_lower > xg_set_encoder OR xg_set_encoder > xg_ci_upper"
        )
    except Exception as exc:
        pytest.skip(f"fct_xg_predictions_v2 not built yet (xg_v2_enabled=false?): {exc}")
        return
    (violations,) = cur.fetchone()
    assert violations == 0, f"{violations} rows violate xg_ci_lower <= xg_set_encoder <= xg_ci_upper"


def test_mart_live_join_preserves_rows() -> None:
    """INNER JOIN fct_shots must not drop rows — every v2 prediction has a fct_shots match."""
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset — live check unavailable")
    cur = _cursor()
    try:
        cur.execute(
            "SELECT "
            " (SELECT count(*) FROM soccer_analytics.dev_silver.stg_xg__predictions_v2) stg, "
            " (SELECT count(*) FROM soccer_analytics.dev_gold.fct_xg_predictions_v2) mart"
        )
    except Exception as exc:
        pytest.skip(f"v2 staging/mart unavailable: {exc}")
        return
    stg, mart = cur.fetchone()
    assert stg == mart, (
        f"staging={stg} vs mart={mart} — INNER JOIN to fct_shots dropped rows "
        f"(means some v2 predictions have shot_ids not in fct_shots; Risk #8 may apply)"
    )
```

- [ ] **Step 2:** Run:

```bash
uv run pytest src/tests/test_dbt_xg_v2_mart.py -v
```

Expected: static tests PASS; live tests skip (mart not built yet).

### Task 4.6: Create bronze live-schema fixture

**Files:**
- Create: `src/tests/fixtures/xg_predictions_v2_bronze.json`

- [ ] **Step 1:** Snapshot the current bronze schema captured in Phase 0.1. Example structure (adjust to match Phase 0.1 DESCRIBE output):

```json
{
  "columns": [
    {"name": "shot_id", "type": "string", "nullable": true},
    {"name": "competition_id", "type": "int", "nullable": true},
    {"name": "xg_set_encoder", "type": "double", "nullable": true},
    {"name": "xg_ci_lower", "type": "double", "nullable": true},
    {"name": "xg_ci_upper", "type": "double", "nullable": true},
    {"name": "_ingested_at", "type": "timestamp", "nullable": true}
  ],
  "post_migration_expected": true,
  "notes": "Snapshot after PR 3 drops match_id from the v2 writer _RESULTS_SCHEMA. If Phase 0.1 revealed match_id still present (unmigrated), this fixture represents the POST-migration target — the live-DESCRIBE test asserts the migration has landed."
}
```

### Task 4.7: Add parser-level + live-DESCRIBE tests

**Files:**
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1:** Follow the G6 pattern established in the drop-safety sweep (per memory `project_drop_safety_sweep_closed.md`). Locate the existing `_EXPECTED_COLS` registry block. Add:

```python
# Post-PR 3 bronze.xg_predictions_v2 schema after _RESULTS_SCHEMA shrink
_XG_V2_EXPECTED_COLS = [
    "shot_id",
    "competition_id",
    "xg_set_encoder",
    "xg_ci_lower",
    "xg_ci_upper",
    "_ingested_at",
]
```

And a test function (mirror the existing bronze live-DESCRIBE tests for `xg_predictions` v1):

```python
def test_xg_predictions_v2_live_matches_expected() -> None:
    if not os.environ.get("DATABRICKS_TOKEN"):
        pytest.skip("DATABRICKS_TOKEN unset")
    live = _describe_columns("soccer_analytics.bronze.xg_predictions_v2")
    assert sorted(live) == sorted(_XG_V2_EXPECTED_COLS), (
        f"bronze.xg_predictions_v2 schema drift: live={sorted(live)} expected={sorted(_XG_V2_EXPECTED_COLS)}"
    )
```

Also add the fixture-parser test (existing G6 pattern — replicate the same structure used by the v1 entry).

### Task 4.8: Extend staging-coverage test

**Files:**
- Modify: `src/tests/test_staging_coverage.py`

- [ ] **Step 1:** Locate the source→staging pair registry. Add:

```python
    ("xg.xg_predictions_v2", "stg_xg__predictions_v2", {"non_null": []}),
```

(Argument order matches the existing tuples — source_fqn, staging_model_name, optional_filter_kwargs.)

### Task 4.9: Build + run tests end-to-end

- [ ] **Step 1:** Build the new mart:

```bash
uv run python scripts/ensure_warehouse.py -- dbt build --project-dir dbt_project --profiles-dir dbt_project --select +fct_xg_predictions_v2 --vars 'xg_v2_enabled: true'
```

Expected: OK + contract tests pass.

- [ ] **Step 2:** Run all new dbt tests:

```bash
uv run pytest src/tests/test_dbt_xg_v2_mart.py src/tests/test_bronze_live_schema.py src/tests/test_staging_coverage.py -v
```

Expected: PASS.

---

## Phase 5: Bronze schema migration + Python writer refactor + wheel bump (D1 = accept, D2 = ALTER)

### Task 5.0: Enable column-mapping and drop `match_id` from `bronze.xg_predictions_v2`

Must run BEFORE the Python writer's `_RESULTS_SCHEMA` shrink lands in production. If the writer lands first, the next `compute_xg_model_v2` MERGE hits `DELTA_MERGE_UNRESOLVED_EXPRESSION` against the old `match_id` column. Task 5.0 is cheap, online, and idempotent — safe to run even during PR review while the currently-deployed writer still emits `match_id` (Delta happily writes into the now-gone column as a no-op; the new writer lands next and the column is cleanly tombstoned).

**Files:** None — Databricks SQL only.

- [ ] **Step 1:** Enable column-mapping name mode.

**Phase 0.1 verified (2026-04-22):** table has 7 columns incl. `match_id BIGINT`, all 131,077 rows have non-NULL `match_id`, `delta.columnMapping.mode` is NOT currently set, and protocol versions are already `minReaderVersion=3` + `minWriterVersion=7` — well above the `(2, 5)` thresholds needed for columnMapping. Therefore **no protocol-version bump is needed**; a single-property ALTER is sufficient.

```sql
ALTER TABLE soccer_analytics.bronze.xg_predictions_v2
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
```

Expected: `OK`. This is a one-way door (table cannot be read by DBR < 12.2 afterward); our Serverless runtime is far above that and we have no older-DBR consumers.

- [ ] **Step 2:** Drop the column:

```sql
ALTER TABLE soccer_analytics.bronze.xg_predictions_v2 DROP COLUMN match_id;
```

Expected: `OK`. Verify:

```sql
DESCRIBE TABLE soccer_analytics.bronze.xg_predictions_v2;
```

Expected: 6 columns (`shot_id`, `competition_id`, `xg_set_encoder`, `xg_ci_lower`, `xg_ci_upper`, `_ingested_at`). Row count unchanged at 131,077.

- [ ] **Step 3:** Run the bronze-live-schema test — it should now PASS (live matches the fixture from Task 4.6):

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_xg_predictions_v2_live_matches_expected -v
```

Expected: PASS. This is the sanity check that Phase 4.6 → Phase 5.0 ordering is correct.

### Task 5.1: Shrink `xg_model_v2.py` `_RESULTS_SCHEMA` + UDF output

**Files:**
- Modify: `src/ingestion/xg_model_v2.py` at lines 33-36, 171, 279-288, 381-384

- [ ] **Step 1:** Change `_RESULTS_SCHEMA` at `src/ingestion/xg_model_v2.py:33-36`:

```python
_RESULTS_SCHEMA = (
    "shot_id STRING, competition_id INT, "
    "xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE, _ingested_at TIMESTAMP"
)
```

- [ ] **Step 2:** Change `_load_shots_with_context` SQL SELECT at line 171. Remove `s.match_id`:

```python
query = f"""
    SELECT s.shot_id, s.competition_id, s.player_id, s.team_id,
           s.location_x, s.location_y, s.end_location_x, s.end_location_y,
           s.distance_to_goal, s.shot_angle, s.shot_body_part, s.shot_technique,
           s.shot_type, s.play_pattern, s.is_first_time, s.period, s.minute,
           s.is_goal, s.data_source,
           e.shot_freeze_frame
    FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
    LEFT JOIN {catalog}.dev_silver.stg_statsbomb__events e
        ON s.shot_id = md5(CAST(CONCAT(
               COALESCE(CAST(e.event_id AS STRING), '_dbt_utils_surrogate_key_null_'),
               '-',
               COALESCE(CAST('statsbomb' AS STRING), '_dbt_utils_surrogate_key_null_')
           ) AS STRING))
        AND e.event_type = 'Shot'
    WHERE s.competition_id IS NOT NULL
"""  # noqa: S608
```

- [ ] **Step 3:** UDF pandas return at lines 279-288. Drop the `match_id` entry:

```python
return _pd.DataFrame(
    {
        "shot_id": pdf["shot_id"],
        "competition_id": pdf["competition_id"],
        "xg_set_encoder": xg_set_encoder,
        "xg_ci_lower": xg_ci_lower,
        "xg_ci_upper": xg_ci_upper,
    }
)
```

- [ ] **Step 4:** `output_schema` string at lines 381-384:

```python
output_schema = (
    "shot_id STRING, competition_id INT,"
    " xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE"
)
```

### Task 5.2: Update test_xg_model_v2.py fixture assignments

**Files:**
- Modify: `src/tests/test_xg_model_v2.py`

- [ ] **Step 1:** Remove `shots["match_id"] = ...` assignments at lines 145, 172, 208, 229, 250, 493, 519 (7 places — all synthetic fixture setup; no business meaning).

- [ ] **Step 2:** Update `expected_columns` at line 156:

```python
expected_columns = {"shot_id", "competition_id", "xg_set_encoder", "xg_ci_lower", "xg_ci_upper"}
```

- [ ] **Step 3:** Run:

```bash
uv run pytest src/tests/test_xg_model_v2.py -v
```

Expected: all tests PASS, including the 4 PR #177 regression tests (`TestV2EnvelopeFeatureNames` × 2, `TestMlflowLookupsUseGoldSchema` × 2) which do not exercise `match_id` in their assertions.

### Task 5.3: Update v1 `xg_model.py` read-SQL

**Files:**
- Modify: `src/ingestion/xg_model.py` (read the file in-flight; spec §6.3 says SQL only, UDF output + write schema unchanged)

- [ ] **Step 1:** Locate the SQL that SELECTs from `fct_shots`. Change `s.match_id` → `s.match_key` and add a LEFT JOIN to `dim_matches` to translate back to native match_id for the bronze back-compat write:

```sql
SELECT <existing columns>,
       CAST(dm.native_match_id AS BIGINT) AS match_id  -- bronze back-compat
FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
LEFT JOIN {catalog}.{DEFAULT_GOLD_SCHEMA}.dim_matches dm ON s.match_key = dm.match_key
WHERE ...
```

Rationale: v1 bronze `xg_predictions` table is NOT migrated in PR 3 (per Chesterton's Fence). The writer continues emitting `match_id` from dim_matches' native_match_id; only StatsBomb+Wyscout v1 data has competition_id != NULL, and their native_match_ids are BIGINT, so the CAST is safe.

- [ ] **Step 2:** Run:

```bash
uv run pytest src/tests/test_xg_model.py -v
```

Expected: PASS. If the test mocks the SQL string, update mocks to match the new SELECT.

### Task 5.4: Bump wheel 0.3.12 → 0.3.13

**Files:**
- Modify (automated via script): `pyproject.toml`, `src/shared/wheel.py`, Terraform modules, PEP 723 script headers, `deploy.sh`, `hf_taipy_app/requirements.txt` chain

- [ ] **Step 1:** Run the bumper:

```bash
uv run python scripts/bump_wheel.py --version 0.3.13
```

Expected output: "Updated pyproject.toml", "Updated src/shared/wheel.py", then ~19 "Updated <path>" lines covering Terraform + PEP 723 scripts.

- [ ] **Step 2:** Verify:

```bash
uv run python scripts/bump_wheel.py --check
```

Expected: `All 19 consumers in sync` (or however `--check` phrases success). Zero exit.

- [ ] **Step 3:** Inspect the diff — the bumper touches many files, some of which are Terraform consumers that must stay hash-free (`_VERSION_ONLY_CONSUMER_GLOBS` per memory). The `--check` gate from step 2 enforces this.

### Task 5.5: Run the full Python writer test suite

- [ ] **Step 1:**

```bash
uv run pytest src/tests/test_xg_model_v2.py src/tests/test_xg_model.py src/tests/test_artifact_deploy.py -v
```

Expected: PASS across all three (the artifact_deploy tests should be untouched and remain green — sanity check).

---

## Phase 6: HF dataset exports (D3 = mixed)

### Task 6.1: `publish_xg_shots_hf.py` dual-column (D3)

**Files:**
- Modify: `scripts/publish_xg_shots_hf.py`

- [ ] **Step 1:** Locate the SQL building the published parquet. Emit BOTH `match_key` (new primary) AND `match_id` (legacy, join-preserved):

```sql
SELECT ...existing columns...,
       s.match_key,                                    -- new primary surrogate key
       CAST(dm.native_match_id AS BIGINT) AS match_id  -- legacy, deprecated 2026-04-22
FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
LEFT JOIN {catalog}.{DEFAULT_GOLD_SCHEMA}.dim_matches dm
    ON s.match_key = dm.match_key
WHERE ...
```

Exported parquet schema = prior schema + new `match_key BIGINT` column. Existing consumers keep working on `match_id`; new consumers should use `match_key`.

- [ ] **Step 2:** Update the script's module docstring / inline comment: mention the schema change and ADR-011.

- [ ] **Step 3:** Add a README changelog entry to the HF dataset (update path specified in the script — typically a `DATASET_README_PATH` constant or inline `update_dataset_readme()` call). Changelog text — the **canonical dual-column deprecation notice**. Reuse verbatim on every future HF dataset key migration (PR 5, PR 7, ...):

```
## 2026-04-22 — `match_key` added; `match_id` deprecated

Added `match_key` (BIGINT, Kimball surrogate — now the primary match identifier;
see ADR-011 in the luxury-lakehouse repo).

`match_id` (the legacy smart key) remains in every row for backward compatibility
BUT is **deprecated effective 2026-04-22** and **WILL be removed on or after
2026-07-22** (≥90-day migration window).

Migration:
  - Replace `match_id` with `match_key` in your joins and filters.
  - If you need provider / native-match-id metadata, join on `match_key` to the
    `dim_matches` table (published to HF Hub under the same org).

Questions / active-use notice: open a discussion on this dataset before
2026-07-22. Removal timing can extend if there is documented active use.
```

### Task 6.2: `export_shots_on_target.py` dual-column (D3)

**Files:**
- Modify: `src/ingestion/export_shots_on_target.py`

- [ ] **Step 1:** Emit BOTH `match_key` (new primary) AND `match_id` (legacy, join-preserved):

```sql
SELECT ...existing columns...,
       s.match_key,                                    -- new primary
       dm.native_match_id AS match_id                  -- legacy, deprecated 2026-04-22
FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
LEFT JOIN {catalog}.{DEFAULT_GOLD_SCHEMA}.dim_matches dm
    ON s.match_key = dm.match_key
WHERE ...
```

Cast `native_match_id` to STRING or BIGINT to match the published parquet's historical `match_id` type.

- [ ] **Step 2:** Add the same canonical 2026-04-22 deprecation changelog (from Task 6.1 Step 3) to the `statsbomb-shots-on-target` dataset README. Adapt only the dataset-name mention in the first line; the migration guidance text is identical.

### Task 6.3: Smoke-test exports locally

- [ ] **Step 1:** Dry-run the publish script with a `--dry-run` or single-competition limit if it supports one. Otherwise, invoke its unit tests:

```bash
uv run pytest src/tests/test_publish_xg_shots_hf.py src/tests/test_export_shots_on_target.py -v
```

Expected: PASS (updating any test fixtures that reference `match_id` column accordingly).

---

## Phase 7: Taipy consumer migration

### Task 7.1: `queries/shots.py` — `competition_id` → `competition_key`

**Files:**
- Modify: `hf_taipy_app/src/queries/shots.py`

- [ ] **Step 1:** Rewrite both functions:

```python
@ttl_cache()
def fetch_shots(
    competition_key: int,
    team_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch shots with player names, filtered by competition/team/player.

    Post-PR 3 (ADR-011): scopes on competition_key (Kimball surrogate).

    Expected columns: shot_id, location_x, location_y, statsbomb_xg, is_goal,
    shot_outcome, shot_body_part, distance_to_goal, shot_angle, minute,
    player_display_name.
    """
    conditions = ["s.competition_key = %s"]
    params: list[Any] = [int(competition_key)]

    if team_id is not None:
        conditions.append("s.team_id = %s")
        params.append(int(team_id))
    if player_id is not None:
        conditions.append("s.player_id = %s")
        params.append(int(player_id))

    where = " AND ".join(conditions)

    return execute_query(
        f"SELECT s.shot_id, s.location_x, s.location_y, s.statsbomb_xg, s.is_goal, "  # noqa: S608
        f"  s.shot_outcome, s.shot_body_part, s.distance_to_goal, s.shot_angle, "
        f"  s.minute, p.player_display_name "
        f"FROM {t('fct_shots_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON s.player_id = p.player_id "
        f"WHERE {where} "
        f"ORDER BY s.minute, s.second "
        f"LIMIT 10000",
        tuple(params),
    )


@ttl_cache()
def fetch_xg_predictions(competition_key: int) -> pd.DataFrame:
    """Fetch custom xG predictions. Returns empty DataFrame if table unavailable.

    Post-PR 3 (ADR-011): scopes on competition_key.

    Expected columns: shot_id, xg_logistic, xg_gradient_boosted.
    """
    try:
        return execute_query(
            f"SELECT shot_id, xg_logistic, xg_gradient_boosted "  # noqa: S608
            f"FROM {t('fct_xg_predictions_synced')} "
            f"WHERE competition_key = %s "
            f"LIMIT 100000",
            (competition_key,),
        )
    except RuntimeError:
        return pd.DataFrame()
```

### Task 7.2: `queries/match.py` — `fetch_shots_timeline(match_key)`

**Files:**
- Modify: `hf_taipy_app/src/queries/match.py`

- [ ] **Step 1:** Replace `fetch_shots_timeline`:

```python
@ttl_cache()
def fetch_shots_timeline(match_key: int) -> pd.DataFrame:
    """All shots for a match ordered chronologically.

    Post-PR 3 (ADR-011): keyed on match_key.

    Used by Match Summary Row 2 (xG race chart). Returns per-shot ``xg``
    (aliased from ``statsbomb_xg`` for the render module's convention),
    ``is_goal`` flag, and the team display name for per-trace coloring.

    Expected columns: match_key, minute, second, period, team_id, team_name,
    xg, is_goal, player_id, player_name.
    """
    if not isinstance(match_key, int):
        raise TypeError(f"match_key must be int, got {type(match_key).__name__}")

    fs = t("fct_shots_synced")
    dp = t("dim_players_synced")
    dt = t("dim_teams_synced")
    return execute_query(
        f"SELECT s.match_key, s.minute, s.second, s.period, "  # noqa: S608
        f"  s.team_id, s.player_id, "
        f"  s.statsbomb_xg AS xg, s.is_goal, "
        f"  p.player_display_name AS player_name, "
        f"  tm.team_name AS team_name "
        f"FROM {fs} s "
        f"LEFT JOIN {dp} p ON s.player_id = p.player_id "
        f"LEFT JOIN {dt} tm ON s.team_id = tm.team_id "
        f"WHERE s.match_key = %s "
        f"ORDER BY s.period, s.minute, s.second "
        f"LIMIT 200",
        (int(match_key),),
    )
```

Do NOT touch `fetch_vaep_decisive_actions` or `fetch_discipline_events` — those still use `match_id` (migrated in PR 4 and later).

### Task 7.3: `state/shot_map.py` — `get_comp_id` → `get_competition_key`

**Files:**
- Modify: `hf_taipy_app/src/state/shot_map.py`

- [ ] **Step 1 (only if Phase 0.3 = outcome (b)):** Add `get_competition_key` helper to `hf_taipy_app/src/state/shared.py` mirroring the existing `get_comp_id` shape but returning the surrogate.

- [ ] **Step 2:** In `shot_map.py` line 21, update import:

```python
from state.shared import _ALL_LABEL, get_competition_key, get_player_id, get_team_id, register_page_refresher
```

- [ ] **Step 3:** In `sm_refresh` at line 170:

```python
comp_key = get_competition_key(state.selected_competition)
if comp_key is None:
    ...
```

Rename all `comp_id` → `comp_key` local variables (grep the function body — 2-3 occurrences). Update the `fetch_shots(comp_key, team_id, player_id)` and `_join_xg_predictions(shots, comp_key)` call sites.

### Task 7.4: `state/match_summary.py` — call-site rename + downstream shots df check

**Files:**
- Modify: `hf_taipy_app/src/state/match_summary.py`

- [ ] **Step 1:** At line 189, if the match_id local is a native id, resolve to match_key via `get_match_key` (must exist per PR 2); else if it's already match_key, just rename:

Grep first:

```bash
grep -n "match_id\|match_key\|get_match_id\|get_match_key" hf_taipy_app/src/state/match_summary.py
```

- [ ] **Step 2:** The fetch call becomes `shots = fetch_shots_timeline(int(match_key))` where `match_key` is either a local var (already present per PR 2) or resolved via `get_match_key(...)`.

- [ ] **Step 3:** Grep for downstream `shots["match_id"]` in render modules:

```bash
grep -rn 'shots\["match_id"\]\|shots\.match_id' hf_taipy_app/src/
```

If any hits, rename to `match_key` (the column in the df now). If zero hits, proceed.

- [ ] **Step 4:** Check `build_xg_race_figure` (referenced at `match_summary.py:230`):

```bash
grep -rn "def build_xg_race_figure" hf_taipy_app/src/
```

Read the function. If it accesses `shots["match_id"]` internally, rename to `match_key`. (Per spec §15 #6, this is the concrete downstream concern.)

### Task 7.5: Update `test_queries_match_extended.py`

**Files:**
- Modify: `src/tests/test_queries_match_extended.py`

- [ ] **Step 1:** Grep for `fetch_shots_timeline`:

```bash
grep -n "fetch_shots_timeline" src/tests/test_queries_match_extended.py
```

Update the mock SQL expected text and the argument name (`match_id` → `match_key`) anywhere the test asserts against the query string.

### Task 7.6: Local Taipy smoke test

- [ ] **Step 1:** Per memory `reference_local_taipy_testing.md`:

```bash
cd hf_taipy_app && uv run python src/main.py
```

Open `http://localhost:7860` in a browser. Visit:
- **Shot Map** — pick a competition, verify shots load, pitch renders.
- **Match Summary** — pick a match, verify xG race chart renders.

Expected: no errors in the console; both pages render identically to pre-PR-3 staging (Phase 0.5 baseline).

If `DATABRICKS_HTTP_PATH` isn't in the local env, set it first (the startup would fail otherwise).

---

## Phase 8: Infrastructure

### Task 8.1: Add `fct_xg_predictions_v2` synced-table Terraform resource

**Files:**
- Modify: `terraform/modules/synced_tables/main.tf`
- Modify: `terraform/modules/synced_tables/outputs.tf`

- [ ] **Step 1:** Read the existing `databricks_database_synced_database_table.fct_xg_predictions` resource. Copy its shape. Insert a new resource named `fct_xg_predictions_v2`:

```hcl
resource "databricks_database_synced_database_table" "fct_xg_predictions_v2" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = var.logical_database_name
  spec {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2"
    primary_key_columns    = ["shot_id"]
    # Match the existing fct_xg_predictions resource fields exactly.
    # Executor: read the current fct_xg_predictions block in this file and
    # mirror it 1:1 except name + source_table_full_name.
  }
}
```

- [ ] **Step 2:** Add one line to `outputs.tf` in the outputs map:

```hcl
    fct_xg_predictions_v2 = databricks_database_synced_database_table.fct_xg_predictions_v2.name
```

- [ ] **Step 3:** `terraform validate` in the module directory:

```bash
cd terraform/modules/synced_tables && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4:** From the root terraform directory, `terraform plan` (requires TF_VAR_* env vars per `reference_databricks_account_id.md`):

```bash
cd terraform && terraform plan -target=module.synced_tables.databricks_database_synced_database_table.fct_xg_predictions_v2
```

Expected: `Plan: 1 to add, 0 to change, 0 to destroy.` Do NOT apply.

### Task 8.2: Register in `refresh_synced_tables.py::SYNCED_TABLES`

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py`

- [ ] **Step 1:** Locate the `SYNCED_TABLES` list. Add after the `fct_xg_predictions_synced` entry:

```python
    ("fct_xg_predictions_v2_synced", None),
```

The `None` second-tuple-element matches the established convention for synced tables that don't carry custom PG-side schema directives (per PR 2 pattern).

### Task 8.3: Update `scripts/create_indexes.py` INDEXES list

**Files:**
- Modify: `scripts/create_indexes.py` (the `INDEXES: list[tuple[str, str, str]]` declaration, lines 48-207)

- [ ] **Step 1:** Three in-place column edits + one new entry (no DROP DDL — recreation handles it):

```python
# fct_xg_predictions_synced — PR 3 Kimball migration
# Before:  ("idx_xg_predictions_match", "fct_xg_predictions_synced", "match_id"),
# After:
("idx_xg_predictions_match", "fct_xg_predictions_synced", "match_key"),
# Before:  ("idx_xg_predictions_comp", "fct_xg_predictions_synced", "competition_id"),
# After:
("idx_xg_predictions_comp", "fct_xg_predictions_synced", "competition_key"),

# fct_shots_synced — PR 3 Kimball migration (leading column only)
# Before:  ("idx_shots_comp_team_player", "fct_shots_synced", "competition_id, team_id, player_id"),
# After:
("idx_shots_comp_team_player", "fct_shots_synced", "competition_key, team_id, player_id"),

# fct_xg_predictions_v2_synced — NEW mart. Append in a new comment block:
("idx_xg_predictions_v2_comp", "fct_xg_predictions_v2_synced", "competition_key"),
```

Keep `idx_shots_player_team` on `(player_id, team_id)` — not migrated. PK-backed `shot_id` indexes are automatic on synced tables; no explicit entry needed.

- [ ] **Step 2:** No DROP DDL required. UI synced-table recreation (post-merge deploy step) drops all custom indexes on the recreated table; `create_indexes.py` then re-applies from the updated list. Confirmed by `scripts/create_indexes.py:401` using `CREATE INDEX IF NOT EXISTS` for idempotent re-apply.

- [ ] **Step 3:** Static import-check — confirms the list parses and the count increases by exactly one:

```bash
PYTHONPATH=scripts uv run python -c "import create_indexes; print(f'INDEXES: {len(create_indexes.INDEXES)} btree, {len(create_indexes.HNSW_INDEXES)} HNSW')"
```

Expected: btree count = (pre-PR count) + 1. HNSW unchanged at 6. Running the actual script against live Lakebase is a post-merge deploy operation, not a plan-execution step.

### Task 8.4: Update `workflow-cards/wf-xg-v2.yaml`

**Files:**
- Modify: `workflow-cards/wf-xg-v2.yaml`

- [ ] **Step 1:** Find the `outputs.tables` block. Currently one entry. Add a second with `dbt_model:`:

```yaml
outputs:
  tables:
    - id: "{catalog}.bronze.xg_predictions_v2"
      destination: delta-table
      description: "Raw v2 xG predictions written by the scoring pipeline."
    - id: "{catalog}.{gold_schema}.fct_xg_predictions_v2"
      destination: dbt-mart
      dbt_model: "fct_xg_predictions_v2"
      description: "Contract-enforced gold mart; Kimball keys resolved via JOIN fct_shots per ADR-013."
  models:
    # ... UNCHANGED from PR #177 — three entries (HF Hub + MLflow @Champion + UC Volume)
```

- [ ] **Step 2:** Run card parity tests:

```bash
uv run pytest src/tests/test_card_dbt_model_field.py src/tests/test_card_cost_phase_parity.py src/tests/test_card_parity_with_terraform.py -v
```

Expected: all PASS. `test_card_dbt_model_field` specifically asserts the `dbt_model` field for dbt-produced tables (`fct_xg_predictions_v2`). `test_card_parity_with_terraform` may flag TF wiring — but since the TF `--schema bronze` is unchanged (Phase 0 verified), this should stay green.

### Task 8.5: Confirm no Terraform workflow edits required

**Files:** `terraform/modules/workflows/main.tf` (verification only, no edits expected)

- [ ] **Step 1:** Re-grep to confirm the `compute_xg_model_v2` task still has `--schema "bronze"`:

```bash
grep -n -A 15 "compute_xg_model_v2" terraform/modules/workflows/main.tf
```

Expected: line 499 still `"--schema", "bronze",`. No edit required.

---

## Phase 9: ADRs + CLAUDE.md

### Task 9.1: Write ADR-013

**Files:**
- Create: `docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md`

- [ ] **Step 1:** Full content (based on spec §10):

```markdown
# ADR-013: ML Inference Output Tables Flow Python → Bronze → dbt Staging → Gold Fact With Contract

| Field | Value |
|---|---|
| **Date** | 2026-04-22 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

xG v2 was added to the warehouse as a standalone Python writer that targeted a gold-layer Delta table directly. This bypassed dbt in four concrete ways:

1. No contract enforcement (schema drift possible per run).
2. Kimball surrogate keys had to be hardcoded in the Python writer, coupling it to ADR-011's key schema.
3. Synced-table grants + recreation handled inconsistently vs dbt-built marts.
4. Slim-CI (state:modified+) couldn't see the mart because no `.sql` file existed for it.

The 2026-04-22 brainstorming for PR 3 of the ADR-011 migration confirmed one adjacent offender (`fct_pausa_values`); both must be corrected under this ADR's scope, with pausa scheduled into PR 7 of the ADR-011 rollout.

This ADR is the **consumer-side** counterpart to ADR-012. ADR-012 is producer-side *weight delivery* (MLflow @Champion + UC Volume + HF Hub, loudly-or-not-at-all). ADR-013 is producer-side *prediction-table delivery* (bronze raw → dbt staging → gold mart with contract).

## Decision

All ML inference outputs flow `Python writer → bronze raw table → dbt staging view → gold dbt mart with contract: enforced: true`. Surrogate keys from ADR-011 resolve in the mart layer via `INNER JOIN fct_shots ON shot_id` (or the equivalent identity fact for non-shot inference). Python writers emit ONLY native identifiers + predictions — never surrogate keys.

### Scope

- **In scope:** Inference outputs — Python workflows producing per-row predictions over an existing identity fact (xG v1, xG v2, PSxG, VAEP action values when re-architected, future models).
- **Out of scope:** Ingestion writers. Bronze IS their contractual output.

### Normative requirements for any new ML inference pipeline

1. Bronze table with a defined schema (documented in `_xg__sources.yml` or equivalent `_<domain>__sources.yml`) and `_ingested_at` audit column.
2. `stg_<domain>__predictions.sql` view — dedup + type-cast + no key resolution.
3. `fct_<domain>_predictions.sql` mart — contract-enforced, keys via INNER JOIN to identity fact, clustered on `match_key` (or the equivalent fact's surrogate).
4. Workflow card `outputs.tables` lists both bronze and gold entries; gold entry carries `dbt_model: <name>`.
5. Terraform synced-table resource + `SYNCED_TABLES` registry entry + `create_indexes.py` index set.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Python writes to gold + contract tests alongside | No dbt hop | Duplicates test infra; ownership ambiguous; slim-CI invisible | Doesn't solve problems 3+4 |
| B. ALL ML facts through dbt (CHOSEN) | Uniform; contract-enforced; slim-CI-visible; clean ownership | Extra hop per mart | — |
| C. Two writers to gold (Python bypass + dbt) | Flexible | Ownership anti-pattern; race conditions | Ownership ambiguity is the bug we're fixing |

## Consequences

### Positive

- Uniform contract enforcement across v1 + v2 + all future ML marts.
- Python writer never needs to know about ADR-011 key schema.
- Synced-table wiring follows one pattern.
- Slim-CI sees every ML mart.
- Bronze re-ingest never requires rebuilding downstream ML tables — only `dbt run`.

### Negative

- One extra dbt hop per ML mart (marginal build latency).
- Two "versions of truth" during migration: bronze raw + gold modeled. Eventual consistency on next dbt build.

### Neutral

- Materialization strategy (incremental vs table) remains per-mart.
- Does not apply to ingestion writers — bronze IS their purpose.

## Related

- ADR-011 (Kimball surrogate keys — this is the consumption pattern)
- ADR-012 (Training-to-production delivery — producer-side weight counterpart)
- ADR-005 (Lakebase synced-table grants — standard pattern applies)
- ADR-002 (Silent exception elimination — bronze writers follow)

## CLAUDE.md Amendment

Add one bullet under "Architecture Principles":
> **ML inference outputs follow ADR-013**: Python writer → bronze → dbt staging → gold mart with contract. Surrogate keys resolved in mart layer via INNER JOIN fct_shots ON shot_id; Python writers never reference match_key/competition_key.

## Notes

### First two applications

- **PR 3 (this PR):** xG v2 promotion — `fct_xg_predictions_v2.sql` is the first ADR-013 mart.
- **PR 7:** `fct_pausa_values` promotion (amended into ADR-011's rollout table).
```

### Task 9.2: Amend ADR-011

**Files:**
- Modify: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`

- [ ] **Step 1:** In the staged rollout policy table (around line 82), update the PR 7 row:

```markdown
| PR 7 | Tracking + formations + pausa + tail facts migration (**`fct_pausa_values` also promoted Python→dbt mart under ADR-013 as part of this PR**) | Planned |
```

- [ ] **Step 2:** Append a footnote under §Notes > "Staged rollout policy" (around line 88):

```markdown
PR 3 is the first application of ADR-013 (xG v2 promotion); PR 7 is the second (fct_pausa_values).
```

Also update the PR 2 row's Status from `Shipped (2026-04-22)` to `Shipped` with the existing date — unchanged, just checking.

### Task 9.3: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1:** Under the "Architecture Principles" section, append one bullet:

```markdown
- **ML inference outputs follow [ADR-013](docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)**: Python writer → bronze raw → dbt staging view → gold mart with `contract: enforced: true`. Surrogate keys resolve in the mart via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit only native identifiers + predictions. First applied in PR 3 (xG v2 promotion); PR 7 extends to `fct_pausa_values`.
```

---

## Phase 10: Full verification

### Task 10.1: Lint + format + type-check

- [ ] **Step 1:**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/
uv run ruff format --check src/ scripts/ hf_taipy_app/src/
uv run pyright src/ hf_taipy_app/src/
```

Expected: all clean. If ruff format fails, run `uv run ruff format src/ scripts/ hf_taipy_app/src/` then re-check.

### Task 10.2: Full pytest

- [ ] **Step 1:**

```bash
uv run pytest src/tests/ -v --timeout=300
```

Expected: all PASS. Integration tests marked with `@pytest.mark.integration` will run against live Databricks using the propagated token. If any fail on `column match_key does not exist` that means a downstream test fixture hasn't been updated — grep+fix.

### Task 10.3: dbt slim CI equivalent

- [ ] **Step 1:**

```bash
uv run python scripts/dbt_build_and_refresh.py --select "state:modified+" --vars "xg_model_enabled: true, xg_v2_enabled: true"
```

(This wraps `dbt build` with a synchronous Lakebase synced-table refresh per CLAUDE.md project conventions.)

Expected: all models build; all tests pass; synced tables refresh. If `fct_shots_synced` refresh fails with schema mismatch, it's because the synced table needs UI recreation — that's expected for the deploy sequence, not during CI. Skip the refresh step during dev if so; let the daily-job handle it post-merge.

### Task 10.4: Re-deploy Taipy to staging (smoke)

- [ ] **Step 1:** Deploy staging from the branch (not main — user's call whether this is safe before PR):

```bash
uv run python scripts/manage_space.py deploy staging
```

Expected: Space rebuilds, loads. Visit Shot Map + Match Summary + Action Values. Confirm no errors.

---

## Phase 11: Commit + PR

### Task 11.1: Create branch + bundle commit

Per memory `feedback_single_commit_squash.md` — one commit per branch. Spec + plan + all implementation in a single commit.

- [ ] **Step 1:** **PAUSE for user approval.** Per CLAUDE.md "Never commit without explicit user approval" and the Magic Paste's Step 5. The first commit creates the branch and bundles spec + plan + all code/yml/tf/doc changes.

When approved, run (in a single approval window):

```bash
git checkout -b feat/kimball-pr3-shots-xg
git add \
  docs/superpowers/specs/2026-04-22-kimball-pr3-shots-xg-design.md \
  docs/superpowers/plans/2026-04-22-kimball-pr3-shots-xg.md \
  docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md \
  docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md \
  CLAUDE.md \
  dbt_project/models/intermediate/int_unified_shots.sql \
  dbt_project/models/intermediate/_intermediate__models.yml \
  dbt_project/models/marts/fct_shots.sql \
  dbt_project/models/marts/fct_xg_predictions.sql \
  dbt_project/models/marts/fct_xg_predictions_v2.sql \
  dbt_project/models/marts/_marts__models.yml \
  dbt_project/models/staging/stg_xg__predictions.sql \
  dbt_project/models/staging/stg_xg__predictions_v2.sql \
  dbt_project/models/staging/_xg__sources.yml \
  src/ingestion/xg_model.py \
  src/ingestion/xg_model_v2.py \
  src/ingestion/export_shots_on_target.py \
  src/ingestion/refresh_synced_tables.py \
  scripts/publish_xg_shots_hf.py \
  scripts/create_indexes.py \
  scripts/bump_wheel.py \
  pyproject.toml \
  src/shared/wheel.py \
  terraform/modules/synced_tables/main.tf \
  terraform/modules/synced_tables/outputs.tf \
  workflow-cards/wf-xg-v2.yaml \
  hf_taipy_app/src/queries/shots.py \
  hf_taipy_app/src/queries/match.py \
  hf_taipy_app/src/state/shot_map.py \
  hf_taipy_app/src/state/match_summary.py \
  hf_taipy_app/src/state/shared.py \
  hf_taipy_app/requirements.txt \
  src/tests/test_dbt_shots_kimball_migration.py \
  src/tests/test_dbt_xg_v2_mart.py \
  src/tests/test_xg_model_v2.py \
  src/tests/test_xg_model.py \
  src/tests/test_bronze_live_schema.py \
  src/tests/test_staging_coverage.py \
  src/tests/test_card_dbt_model_field.py \
  src/tests/test_card_cost_phase_parity.py \
  src/tests/test_card_parity_with_terraform.py \
  src/tests/test_queries_match_extended.py \
  src/tests/fixtures/xg_predictions_v2_bronze.json
# Plus any additional files touched by scripts/bump_wheel.py (`bump_wheel.py --check` output lists them all).

git commit -m "$(cat <<'EOF'
feat(kimball): shots + xG migration to match_key + xG v2 dbt mart (PR 3)

Staged rollout PR 3 per ADR-011. Drops match_id from fct_shots +
fct_xg_predictions onto match_key/competition_key surrogates.
Promotes xG v2 from direct-to-gold Python writer to a contract-enforced
dbt mart (fct_xg_predictions_v2). Establishes ADR-013 as the canonical
pattern for ML inference output tables. Wheel 0.3.12 → 0.3.13.

Spec: docs/superpowers/specs/2026-04-22-kimball-pr3-shots-xg-design.md
Plan: docs/superpowers/plans/2026-04-22-kimball-pr3-shots-xg.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 2:** Verify nothing was missed:

```bash
git status
```

Expected: clean working tree, `On branch feat/kimball-pr3-shots-xg`, `nothing to commit`.

### Task 11.2: Open PR

- [ ] **Step 1:** **PAUSE for user approval** (each PR action is separate per CLAUDE.md). When approved, push + create:

```bash
git push -u origin feat/kimball-pr3-shots-xg
gh pr create --base main --title "feat(kimball): shots + xG migration + ADR-013 (PR 3)" --body "$(cat <<'EOF'
## Summary

- Migrates `fct_shots` + `fct_xg_predictions` from smart-keyed `match_id` to Kimball surrogate `match_key` + adds `competition_key` (ADR-011 PR 3).
- Promotes xG v2 to a contract-enforced dbt mart (`fct_xg_predictions_v2`); writer shrinks to native IDs only.
- Establishes ADR-013 as the canonical Python → bronze → dbt → gold pattern for ML inference outputs; amends ADR-011 to schedule `fct_pausa_values` promotion into PR 7.
- Bumps wheel 0.3.12 → 0.3.13.

## Test plan
- [x] Unit: `uv run pytest src/tests/ -v` (green)
- [x] Static: `ruff check` + `ruff format --check` + `pyright` (green)
- [x] dbt build: `dbt build --select "state:modified+" --vars "xg_model_enabled: true, xg_v2_enabled: true"` (green)
- [x] Live contract: `test_dbt_shots_kimball_migration` + `test_dbt_xg_v2_mart` pass against dev warehouse
- [x] Taipy staging smoke: Shot Map + Match Summary + Action Values render end-to-end
- [x] Bronze live schema: `test_bronze_live_schema.py::test_xg_predictions_v2_live_matches_expected` PASS

## Deploy sequence (post-merge, per spec §12)
1. Terraform Apply on main (new fct_xg_predictions_v2 synced table + workflow card updates)
2. Next daily Databricks job → dbt builds new fct_shots shape + fct_xg_predictions_v2 (with xg_v2_enabled flipped in job config)
3. UI-manual: recreate fct_shots_synced, fct_xg_predictions_synced, create fct_xg_predictions_v2_synced
4. Daily 07:00 UTC `lakebase-grants.yml` (or manual `scripts/maintain_synced_tables.py`) re-applies grants + indexes
5. `compute_xg_model_v2` workflow run → writes to bronze.xg_predictions_v2 (new shape)
6. Deploy Taipy: staging → smoke → production
7. Scheduled HF publish → `xg-shot-data` with `match_key` + README changelog; `statsbomb-shots-on-target` unchanged schema (join-preserve)

## Rollback (spec §13)
- Warehouse break: `git revert` + TF Apply; synced tables UI-recreate. ~30 min RTO.
- Taipy break: `manage_space.py deploy production --ref <prior-sha>`.

## Related
- ADR-011 staged rollout — PR 3 of 8
- ADR-012 (PR #177) — producer-side counterpart
- Spec + plan committed alongside code

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Phase 12: Post-merge deploy (documented; NOT executed by the plan agent)

The agent finishes at PR open. The user runs the deploy sequence above manually. Flagging here so no step is forgotten:

- [ ] Terraform Apply on main (user)
- [ ] Verify `fct_xg_predictions_v2_synced` resource shows up in `terraform state list` (user)
- [ ] UI-recreate the two migrated synced tables + create the new one (user, Databricks workspace UI)
- [ ] Trigger `.github/workflows/lakebase-grants.yml` manually, OR wait for 07:00 UTC cron, OR run `scripts/maintain_synced_tables.py` (user)
- [ ] Verify `create_indexes.py --verify` passes (user)
- [ ] Run `compute_xg_model_v2` Databricks workflow manually to populate bronze with new schema (user)
- [ ] Deploy Taipy: `scripts/manage_space.py deploy staging` → smoke → `scripts/manage_space.py deploy production` (user)
- [ ] Manually kick HF publish scheduled run OR wait for next tick (user)
- [ ] Close follow-up: backfill bronze.xg_predictions_v2 ALTER-vs-DROP resolution (per Phase 0.1 finding)

---

## Self-review

**Spec coverage (spec §2 "In this branch" enumerated 40+ items):**

- All dbt files: Phases 1-4, Task 8.4. ✅
- Python writer changes: Phase 5, Phase 6. ✅ (spec §6.2 constants.py addition dropped — justified above)
- Taipy consumer migration (4 files): Phase 7. ✅
- Infrastructure (3 TF files, refresh list, indexes, workflow card, bronze CI fixture): Phases 4.6-4.8, Phase 8. ✅
- ADRs (013 new, 011 amend): Phase 9. ✅
- Tests (all files enumerated in §2 + §9): covered per-phase + Phase 10. ✅
- CLAUDE.md: Phase 9.3. ✅

**Spec §15 plan-stage open items mapping:**

| Spec #15 item | Handled where |
|---|---|
| 1. DATASET_REPO constant | Verified pre-plan (`publish_xg_shots_hf.py:84`) |
| 2. PSxG trainer consumer | Verified pre-plan (`train_psxg_hf.py:55`); informs D3 default |
| 3. notebooks/train_xg_model.py | Verified pre-plan (zero `match_id` refs); removed from plan |
| 4. Lakebase index set | Phase 0.2 |
| 5. Dataset card changelog wording | Task 6.1 step 3 (wording included inline) |
| 6. `fetch_shots_timeline` caller | Phase 0.3 verify + Task 7.4 (downstream shots df grep) |
| 7. Terraform `--schema` | Verified pre-plan (`workflows/main.tf:499` already bronze); Task 8.5 is a verification pass |
| 8. bronze.xg_predictions_v2 ALTER-vs-DROP | Phase 0.1 → D2 decision |
| 9. Token propagation | Resolved mid-planning (GH Actions secrets propagated 2026-04-22) |
| 10. PR #177 4-test coexistence | Phase 5 Task 5.2 (regression tests retained; scaffolding updates only) |
| 11. Wheel baseline + bump | D1 decision; Phase 5 Task 5.4 |

**Placeholder scan:** No "TBD", no "similar to above", no "add appropriate error handling". Every SQL/Python block shows the target code or explicitly says "preserve existing structure with X edit". ✅

**Type consistency:**
- `match_key` is BIGINT everywhere (int_unified_shots, fct_shots, fct_xg_predictions*, Taipy queries). ✅
- `competition_key` is BIGINT in dbt, `int` at Taipy boundary (Python int that gets bound to PG bigint via `%s` — verified pattern matches existing `competition_id` code path). ✅
- `get_competition_key(...)` → `int | None` (mirrors `get_comp_id`). ✅
- `fetch_shots_timeline(match_key: int)` signature matches existing `fetch_match_summary(match_key: int)` style from PR 2. ✅

**Risk coverage:**
- Risk #1 (synced-table recreation window) — deploy sequence Phase 12 orders it correctly. ✅
- Risk #2 (xg_v2_enabled=false CI gap) — Task 4.9 + Task 10.3 override `xg_v2_enabled: true` during validation. ✅
- Risk #3 (one-time $14 rescoring) — **OBVIATED** by D2 ALTER path. Post-ALTER the guard sees every competition already scored, skips. No rescoring triggered. ✅
- Risk #4 (HF NULL for tracking matches) — `export_shots_on_target` already filters to SB+WS via existing logic; no new affected rows. ✅
- Risk #5 (int_unified_shots full-refresh) — acceptable (<1M rows). ✅
- Risk #6 (Hyrum's Law on HF datasets) — dual-column + 90-day deprecation window (D3). Canonical changelog in Task 6.1 Step 3. ✅
- Risk #7 (ADR-013 scope interpretation) — text explicitly scopes to *inference*. ✅
- Risk #8 (131k bronze rows) — Phase 0.1 + Phase 5.0 ALTER (D2 accepted). ✅
- Risk #9 (token rotation lag) — resolved. ✅

---

## Execution choice

Plan complete and saved to `docs/superpowers/plans/2026-04-22-kimball-pr3-shots-xg.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. REQUIRED SUB-SKILL: `superpowers:subagent-driven-development`.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints.

Which approach?
