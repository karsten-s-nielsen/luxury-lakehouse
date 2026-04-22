# Kimball PR 3 — Shots + xG migration + ML-mart ADR — design spec

| | |
|---|---|
| **Date** | 2026-04-22 |
| **Branch** | `feat/kimball-pr3-shots-xg` (to be created) |
| **Author** | Karsten Nielsen (with Claude Opus 4.7) |
| **Status** | Draft — awaiting user review |
| **Supersedes** | — |
| **Related** | ADR-011 (Kimball surrogate keys); ADR-012 (training-to-production delivery — producer-side counterpart to this ADR-013); ADR-005 (Lakebase synced-table grants); ADR-002 (silent-exception policy); `docs/superpowers/plans/2026-04-20-pr2-passes-match-key-migration.md` (PR 2 reference); `docs/superpowers/plans/2026-04-22-xg2-production-unblock.md` (PR #177 plan — merged just before this spec finalized) |
| **Merge reconciliation** | PR #177 (`ecf2551`) merged to main between spec-brainstorming and spec-finalization (2026-04-22 evening). It took ADR number 012 (for producer-side delivery hardening) and bumped the wheel to 0.3.12. This spec was renumbered to **ADR-013**; the two ADRs are complementary (producer-side vs consumer-side). Wheel references updated. Terraform `--schema bronze` change likely a no-op (verified by 131,077 rows already in `bronze.xg_predictions_v2` post-PR #177). See §16. |

## 1. Goal

Complete PR 3 of the ADR-011 staged Kimball migration: move `fct_shots`, `fct_xg_predictions`, and the Python-written `xg_predictions_v2` table off smart-keyed `match_id`/`competition_id` and onto the surrogate `match_key`/`competition_key` FKs. In the same cycle, promote the xG v2 pipeline output to a proper dbt-modelled gold mart (`fct_xg_predictions_v2`) so every ML inference output in the warehouse flows through the standard Python → bronze → dbt staging → gold-with-contract chain.

This PR also drafts **ADR-013** codifying that flow as the canonical pattern for future ML inference pipelines, and amends **ADR-011** to extend PR 7's scope so `fct_pausa_values` (the remaining direct-to-gold Python writer) is promoted atomically with its later Kimball migration.

ADR-013 is the **consumer-side** counterpart to the newly-merged ADR-012 (training-to-production delivery hardening). ADR-012 guarantees that training artifacts (weights) reach MLflow @Champion + UC Volume + HF Hub loudly-or-not-at-all; ADR-013 guarantees that inference OUTPUTS (prediction tables) flow through bronze → staging → gold with contract enforcement. Both are producer-side *delivery* contracts operating on different artifact classes.

## 2. Scope

### In this branch

- dbt:
  - `int_unified_shots.sql` upgrade (ephemeral → view; add `match_key`).
  - `fct_shots.sql` Kimball migration (drop `match_id`, add `match_key` NOT NULL, add `competition_key`, retain legacy `competition_id` INT nullable, clustering on `match_key`).
  - `fct_xg_predictions.sql` restructure (keys inherited via `INNER JOIN fct_shots ON shot_id`).
  - `stg_xg__predictions.sql` simplification (drop match_id/competition_id from SELECT).
  - `stg_xg__predictions_v2.sql` NEW.
  - `fct_xg_predictions_v2.sql` NEW with `contract: enforced: true`.
  - `_xg__sources.yml` source entry for `bronze.xg_predictions_v2`.
  - `_marts__models.yml` contract updates (fct_shots, fct_xg_predictions, fct_xg_predictions_v2 NEW).
  - `_intermediate__models.yml` entry for int_unified_shots.
- Python ingestion:
  - `src/ingestion/xg_model_v2.py` refactor (writer targets bronze; `_RESULTS_SCHEMA` shrinks; UDF output shrinks).
  - `src/ingestion/xg_model.py` read-SQL update (`s.match_id` → `s.match_key` + JOIN dim_matches for back-compat bronze emission).
  - `src/ingestion/export_shots_on_target.py` **(ii) join-preserve** pattern for HF dataset `statsbomb-shots-on-target`.
  - `scripts/publish_xg_shots_hf.py` **(i) uniform rename** for HF dataset `xg-shot-data` + README changelog note.
  - `src/shared/constants.py` add `DEFAULT_BRONZE_SCHEMA = "dev_bronze"` (for future use; v2 refactor relies on Terraform `--schema` arg, which PR #177 verification shows is already bronze).
  - `notebooks/train_xg_model.py` rename references to `match_key`.
- Taipy consumer migration:
  - `hf_taipy_app/src/queries/shots.py` (fetch_shots + fetch_xg_predictions).
  - `hf_taipy_app/src/queries/match.py` (fetch_shots_timeline).
  - `hf_taipy_app/src/state/shot_map.py` (comp_id → comp_key).
  - `hf_taipy_app/src/state/match_summary.py` (get_match_id → get_match_key at fetch_shots_timeline call site).
- Infrastructure:
  - Terraform `terraform/modules/synced_tables/{main,outputs}.tf` — new `fct_xg_predictions_v2` resource + output.
  - Terraform `terraform/modules/workflows/main.tf` — `compute_xg_model_v2` task `--schema` arg bronze.
  - `src/ingestion/refresh_synced_tables.py::SYNCED_TABLES` — add `fct_xg_predictions_v2_synced`.
  - `scripts/create_indexes.py` — swap/add PG indexes for new key columns.
  - `workflow-cards/wf-xg-v2.yaml` — `outputs.tables` two-entry spec (bronze raw + gold mart with `dbt_model:` field).
- ADRs:
  - **NEW** `docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md`.
  - **AMEND** `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` — PR 7 row + footnote.
- Tests:
  - **NEW** `src/tests/test_dbt_shots_kimball_migration.py`.
  - **NEW** `src/tests/test_dbt_xg_v2_mart.py`.
  - **NEW** `src/tests/fixtures/xg_predictions_v2_bronze.json`.
  - Updates to: `test_xg_model_v2.py`, `test_queries_match_extended.py`, `test_bronze_live_schema.py`, `test_staging_coverage.py`, `test_card_dbt_model_field.py`, `test_card_cost_phase_parity.py`, `test_card_parity_with_terraform.py`.
- CLAUDE.md:
  - Bullet under "Architecture Principles" referencing ADR-013.

### Explicitly out of scope

- **`fct_pausa_values` promotion to dbt mart.** Deferred to PR 7 (ADR-011 amendment codifies it moves atomically with pausa's Kimball migration). This is the "C-defer" decision from brainstorming.
- **`src/ingestion/artifact_deploy.py` and its three helpers** (from PR #177 / ADR-012). Pure producer-side delivery plumbing for model WEIGHTS; this PR's scope is consumer-side inference OUTPUT tables. No changes to the module or its callers (`scripts/train_xg_v2_hf.py`, `scripts/train_xg_model_hf.py`).
- **Wheel version bump.** PR #177 already took 0.3.11 → 0.3.12. This PR does NOT bump further unless a new wheel-shipped module is added during plan stage (unlikely; changes are dbt + Taipy-surface + Terraform).
- **Terraform `compute_xg_model_v2 --schema bronze`.** Production evidence (131,077 rows in `bronze.xg_predictions_v2` post-PR #177) confirms this is already wired. Plan stage verifies and removes from scope if already bronze.
- **Taipy surfacing of xG v2 predictions.** No new `fetch_xg_predictions_v2` query, no new xG model selector option for v2, no new page. v2 mart exists post-PR 3; UI consumption is a separate feature ticket.
- **Shot provider expansion.** IDSSE and Metrica have no shot staging models today. Adding shot coverage from tracking providers is a separate PR when/if those models are built. `accepted_values` on fct_shots.data_source stays `['statsbomb', 'wyscout']`.
- **`dim_teams` / `dim_players` Kimball migration.** team_id and player_id stay as INT across fct_shots + all Taipy consumers. Targeted for PR 5.
- **Other fct_* Kimball migrations.** fct_action_values (PR 4), player stats + embeddings (PR 5), defensive + goalkeeper (PR 6), tracking + formations (PR 7), cleanup (PR 8). All covered by ADR-011 rollout table; not touched here.
- **Bronze `xg_predictions` (v1) schema change.** match_id column stays (Chesterton's Fence). v1 Python writer continues emitting it via JOIN dim_matches translation.
- **xG v2 UC model-registry grants.** Out of scope; existing TODO in `terraform/modules/catalog/main.tf` line 269.

## 3. Approved design decisions

Resolved during brainstorming on 2026-04-22 (this session):

| # | Decision | Choice | Brainstorm Q |
|---|---|---|---|
| 1 | Legacy `match_id` retention on migrated facts | **(a)** Drop match_id entirely; retain legacy `competition_id INT` nullable alongside new `competition_key`; all consumers migrate atomically | Q1 |
| 2 | xG v2 migration mechanism | **(II)** Promote to dbt mart (`fct_xg_predictions_v2`) with contract-enforcement; Python writer redirected to bronze | Q2 |
| 3 | Audit-discovered second offender (`fct_pausa_values`) | **C-defer** — ADR-013 drafted now + ADR-011 PR 7 row amended to include pausa promotion; no scope creep on PR 3 | Q3 |
| 4 | HF dataset key-migration strategy | **Dual-column with 90-day deprecation window, uniform across both HF datasets.** Every export emits both `match_key` (primary) and `match_id` (legacy, via `LEFT JOIN dim_matches`). READMEs declare `match_id` deprecated (≥90-day removal window); follow-up PR in 2026Q3 drops `match_id`. Pattern becomes SOP for PR 5 (`player_id`→`player_key`), PR 7, etc. See §6.4, §6.5. | Q4 |

## 4. Architecture principle (ADR-013 summary)

**All ML inference outputs flow `Python writer → bronze raw table → dbt staging view → gold dbt mart with contract: enforced: true`.** Surrogate-key resolution (ADR-011 Kimball keys) lives in the mart layer via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit ONLY native identifiers + predictions — never surrogate keys.

Data-flow diagram:

```
Python writer (xg_model.py, xg_model_v2.py)
    │  writes native IDs only — match_id BIGINT or competition_id INT
    ▼
bronze.xg_predictions         bronze.xg_predictions_v2
    │                              │
    ▼                              ▼
stg_xg__predictions.sql       stg_xg__predictions_v2.sql
    │   (dedup, type-cast, no key resolution)
    ▼                              ▼
fct_xg_predictions.sql        fct_xg_predictions_v2.sql
    │   (contract enforced; match_key + competition_key pulled
    │    from fct_shots via INNER JOIN on shot_id)
    └──────── fct_shots ───────────┘
              (match_key, competition_key, competition_id legacy)
                │
                ▼
        dim_matches, dim_competitions
              (surrogate key source of truth per ADR-011)
```

Key design consequence: `fct_xg_predictions*` marts inherit `match_key` + `competition_key` **by join**, not by duplicate-column. Bronze xG tables never need rebuilding when Kimball keys change. Only `fct_shots` resolves provider+native_id → match_key; everyone else joins.

## 5. dbt layer changes (detail)

### 5.1 `int_unified_shots.sql` — upgrade

- Materialization: **ephemeral → view**.
- Per-source CTE schema gains `provider STRING` + `native_match_id STRING`; `match_id BIGINT` retained inside the CTE for resolution, then DROPPED at the final `keyed` CTE after `INNER JOIN dim_matches ON (provider, native_match_id)` → `match_key BIGINT`.
- Output column set = existing int_unified_shots columns MINUS `match_id` PLUS `match_key`. Source set stays 2-provider (StatsBomb + Wyscout).

### 5.2 `fct_shots.sql` — Kimball migration

- `unique_key='shot_id'` unchanged. Incremental-merge unchanged.
- `liquid_clustered_by=['match_id']` → **`liquid_clustered_by=['match_key']`**.
- Drop separate `sb_matches` + `ws_matches` CTEs. New single `match_attrs` CTE pulls `match_key, competition_key, competition_id (legacy INT), season_id` from `dim_matches`.
- Join pattern: `LEFT JOIN match_attrs ON unified_shots.match_key = match_attrs.match_key`; `LEFT JOIN running_score rs ON unified_shots.match_key = rs.match_key`.
- Drop incremental skip-predicate (merge already idempotent per shot_id).
- Final SELECT: DROP `match_id`; ADD `match_key` NOT NULL + `competition_key` nullable; RETAIN `competition_id` legacy INT nullable.

### 5.3 `fct_xg_predictions.sql` (v1) — restructure

Keys inherited via INNER JOIN to fct_shots:

```sql
select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,           -- legacy nullable INT
    p.xg_logistic,
    p.xg_gradient_boosted
from {{ ref('stg_xg__predictions') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
```

Contract-enforced (existing flag). `liquid_clustered_by` switches to `match_key`. `enabled=var('xg_model_enabled', false)` flag retained.

### 5.4 `stg_xg__predictions.sql` (v1) — simplify

Drop `match_id` + `competition_id` from the SELECT. Staging becomes `(shot_id, xg_logistic, xg_gradient_boosted)` + dedup rank. Bronze `xg_predictions` schema itself unchanged.

### 5.5 `fct_xg_predictions_v2.sql` — NEW

```sql
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

### 5.6 `stg_xg__predictions_v2.sql` — NEW

Mirror of stg_xg__predictions but for v2 columns. Reads from `source('xg', 'xg_predictions_v2')`. Dedups by shot_id latest-`_ingested_at`. Gated by `enabled=var('xg_v2_enabled', false)`.

### 5.7 `_xg__sources.yml` — extend

Add a second table entry under the existing `xg` source for `xg_predictions_v2`. Columns: `shot_id STRING, competition_id INT, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE, _ingested_at TIMESTAMP`.

### 5.8 `_marts__models.yml` — contract updates

- **`fct_shots`**: drop `match_id` column spec; add `match_key BIGINT not_null` + `competition_key BIGINT` nullable; update `competition_id INT` description to note legacy-retained-until-PR-8. `data_source` accepted_values stays `['statsbomb', 'wyscout']`.
- **`fct_xg_predictions`**: add `match_key` + `competition_key`; update clustering documentation.
- **`fct_xg_predictions_v2`**: NEW entry with full column spec and data_tests mirroring v1 (range 0-1 on xg_set_encoder, ci bounds, not_null shot_id + match_key).

### 5.9 `_intermediate__models.yml` — extend

Add entry for int_unified_shots documenting view materialization + match_key column.

### 5.10 Non-changes (Chesterton's Fence)

- `stg_statsbomb__shots.sql` — unchanged.
- `stg_wyscout__events.sql` filter for shots — unchanged.
- `bronze.xg_predictions` schema — unchanged (match_id stays; Python v1 writer continues emitting it).
- `bronze.psxg_predictions` — unchanged (goalkeeper model, not in scope).
- `int_running_score.sql` — unchanged (PR 2 already migrated it).

## 6. Python writer changes (detail)

### 6.1 `src/ingestion/xg_model_v2.py`

**Merge note:** PR #177 modified this file (fixed `DEFAULT_GOLD_SCHEMA` MLflow-lookup bug; added feature_names envelope fallback at lines 219-242). The `_RESULTS_SCHEMA` + UDF output + schema-related code below was **NOT touched** by PR #177 — the Kimball-migration edits in this spec still apply. The 4 new regression tests in `test_xg_model_v2.py` (`TestV2EnvelopeFeatureNames`, `TestMlflowLookupsUseGoldSchema`) must continue to pass after this PR's edits.

- **`_RESULTS_SCHEMA` (line 33-36)** — drop match_id:
  ```python
  _RESULTS_SCHEMA = (
      "shot_id STRING, competition_id INT, "
      "xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE, "
      "_ingested_at TIMESTAMP"
  )
  ```
- **UDF pandas return (line 263-272)** — drop `match_id` column.
- **UDF `output_schema` (line 358-361)** — match UDF return.
- **`_load_shots_with_context` SQL (line 170-187)** — drop `s.match_id`; keep `s.competition_id` (still on fct_shots as nullable legacy).
- **Guard (`_XgV2Guard.check`, line 45-65)** — no code change; competition_id still there.
- **`replace_where` (line 381)** — no code change; partition key unchanged.
- **Temp-table scratch (line 370)** — stays in `args.schema`.

### 6.2 `src/shared/constants.py`

Add alongside existing constants:
```python
DEFAULT_BRONZE_SCHEMA = "dev_bronze"
```

### 6.3 `src/ingestion/xg_model.py` (v1) — minimal update

Read-SQL only: `SELECT s.match_id` → `SELECT s.match_key` + `LEFT JOIN dim_matches dm ON s.match_key = dm.match_key`; emit `cast(dm.native_match_id as bigint) AS match_id` for bronze write-back compatibility. Bronze `xg_predictions` schema unchanged. UDF output and write schema unchanged.

### 6.4 `src/ingestion/export_shots_on_target.py` — dual-column

Export SELECT pulls `s.match_key` (new primary) + `LEFT JOIN dim_matches dm ON s.match_key = dm.match_key` + emits `dm.native_match_id AS match_id` (legacy, deprecated 2026-04-22) in the exported parquet. Published `statsbomb-shots-on-target` HF dataset schema = prior schema + new `match_key BIGINT` column. Existing consumers (incl. `scripts/train_psxg_hf.py`) keep working on `match_id`; new consumers use `match_key`. `match_id` remains for the 90-day deprecation window, then a follow-up PR in 2026Q3 drops it.

### 6.5 `scripts/publish_xg_shots_hf.py` — dual-column

SELECT pulls `s.match_key` (new primary) + `LEFT JOIN dim_matches dm ON s.match_key = dm.match_key` + emits `cast(dm.native_match_id as bigint) AS match_id` (legacy, deprecated). Published `xg-shot-data` HF dataset schema = prior schema + new `match_key BIGINT` column. Dataset README gets the canonical 2026-04-22 deprecation changelog (see plan Task 6.1 Step 3 — reused verbatim on future HF key-migration PRs).

### 6.6 `notebooks/train_xg_model.py` — (i) uniform rename

Grep-and-swap `match_id` → `match_key` in SQL + pandas column references.

### 6.7 `terraform/modules/workflows/main.tf`

`compute_xg_model_v2` task block (around line 486-496): **likely no-op — verify during plan stage.** Production evidence post-PR #177 (131,077 rows landed in `bronze.xg_predictions_v2` at 2026-04-22 16:06 UTC) confirms the task is already passing a bronze-targeted `--schema`. If Terraform already passes `--schema bronze` (or the Databricks CLI default is bronze in the current workflow config), this section collapses to a verification pass rather than an edit. If the current arg is still `--schema dev_gold` and production writes are reaching bronze via a different mechanism, that mechanism gets documented and the spec's scope here narrows accordingly.

## 7. Consumer migration (detail)

### 7.1 Taipy `hf_taipy_app/src/queries/shots.py`

- `fetch_shots(competition_key, team_id, player_id)` — rename arg; swap `WHERE s.competition_id = %s` → `WHERE s.competition_key = %s`.
- `fetch_xg_predictions(competition_key)` — rename arg; swap `WHERE competition_id` → `WHERE competition_key`.

### 7.2 Taipy `hf_taipy_app/src/queries/match.py`

- `fetch_shots_timeline(match_key)` — rename arg; swap `WHERE s.match_id` → `WHERE s.match_key`; `SELECT s.match_id` → `SELECT s.match_key`.

### 7.3 Taipy `hf_taipy_app/src/state/shot_map.py`

- Line 170: `get_comp_id(...)` → `get_competition_key(...)` (variable rename `comp_id` → `comp_key`).
- Pass `comp_key` to fetch_shots + fetch_xg_predictions calls.

### 7.4 Taipy `hf_taipy_app/src/state/match_summary.py`

- `get_match_id(...)` → `get_match_key(...)` at the fetch_shots_timeline call site.

### 7.5 Lakebase PG indexes (`scripts/create_indexes.py`)

- **`fct_shots_synced`**: drop obsolete `(competition_id, team_id, player_id)` composite; add `(competition_key, team_id, player_id)`. Drop obsolete `(match_id, ...)`; add `(match_key)` for timeline queries.
- **`fct_xg_predictions_synced`**: drop `(competition_id)`; add `(competition_key)`.
- **`fct_xg_predictions_v2_synced`**: add `(competition_key)` single-col. PK-backed shot_id index is auto.
- All indexes declared `WITHOUT ONLY` per CLAUDE.md Lakebase rules. Verified via `create_indexes.py --verify` EXPLAIN ANALYZE pass.

## 8. Infrastructure (detail)

### 8.1 `terraform/modules/synced_tables/main.tf`

Add:
```hcl
resource "databricks_database_synced_database_table" "fct_xg_predictions_v2" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = var.logical_database_name
  spec {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2"
    primary_key_columns    = ["shot_id"]
    # ... remaining fields to match fct_xg_predictions block
  }
}
```
Existing `fct_shots` + `fct_xg_predictions` resource definitions unchanged (PK unchanged; underlying Delta recreation handled at deploy time via UI).

### 8.2 `terraform/modules/synced_tables/outputs.tf`

Add one line to the outputs map: `fct_xg_predictions_v2 = databricks_database_synced_database_table.fct_xg_predictions_v2.name`.

### 8.3 `src/ingestion/refresh_synced_tables.py`

Add one entry to `SYNCED_TABLES` after line 102:
```python
("fct_xg_predictions_v2_synced", None),
```

### 8.4 `workflow-cards/wf-xg-v2.yaml`

**Merge note:** PR #177 expanded `outputs.models` to three entries (HF Hub + MLflow `@Champion` + UC Volume with SHA-256 sidecar). `outputs.tables` still has ONE entry (`{catalog}.bronze.xg_predictions_v2`) — this spec adds the gold-mart entry alongside it without touching `outputs.models`.

Post-PR 3 `outputs.tables`:
```yaml
outputs:
  tables:
    - id: "{catalog}.bronze.xg_predictions_v2"
      destination: delta-table
      description: "Raw v2 xG predictions written by the scoring pipeline."
    - id: "{catalog}.{gold_schema}.fct_xg_predictions_v2"
      destination: dbt-mart
      dbt_model: "fct_xg_predictions_v2"
      description: "Contract-enforced gold mart; Kimball keys resolved via JOIN fct_shots."
```

`outputs.models` unchanged.

### 8.5 Bronze live-schema CI

- **NEW** fixture `src/tests/fixtures/xg_predictions_v2_bronze.json` with snapshot of bronze.xg_predictions_v2 schema.
- **NEW** parser-level + live-DESCRIBE tests in `test_bronze_live_schema.py` following G6 pattern from drop-safety sweep.
- **UPDATE** `test_staging_coverage.py` to add v2 source→staging pair.

### 8.6 HF Space deploy

Post-merge deploy cadence unchanged:
```
uv run python scripts/manage_space.py deploy staging      # staging smoke test first
uv run python scripts/manage_space.py deploy production   # after staging E2E passes
```

## 9. Tests

### 9.1 New test files

| File | What it asserts |
|---|---|
| `src/tests/test_dbt_shots_kimball_migration.py` | Mirrors `test_dbt_passes_kimball_migration.py`: fct_shots has match_key NOT match_id; competition_key present; competition_id legacy nullable retained; per-provider row-count parity pre/post; shot_id uniqueness preserved; INNER JOIN dim_matches returns 100% match_key coverage. |
| `src/tests/test_dbt_xg_v2_mart.py` | fct_xg_predictions_v2 contract satisfied; 100% non-null match_key; `xg_set_encoder ∈ [0,1]`; `xg_ci_lower ≤ xg_set_encoder ≤ xg_ci_upper`; INNER JOIN fct_shots preserves row count. |
| `src/tests/fixtures/xg_predictions_v2_bronze.json` | Bronze schema snapshot fixture for G6 live-DESCRIBE test. |

### 9.2 Updated test files

- `test_xg_model_v2.py` — expected `_RESULTS_SCHEMA` strings.
- `test_queries_match_extended.py` — `fetch_shots_timeline` mock signature.
- `test_bronze_live_schema.py` — v2 live-DESCRIBE test + parser-level assertion.
- `test_staging_coverage.py` — v2 source→staging pair.
- `test_card_dbt_model_field.py` — validates `dbt_model: fct_xg_predictions_v2` appears on wf-xg-v2 card.
- `test_card_cost_phase_parity.py` — recheck v2 card phases unchanged.
- `test_card_parity_with_terraform.py` — recheck TF wiring unchanged.

### 9.3 dbt tests

- `_marts__models.yml` fct_xg_predictions_v2 entry with `dbt_expectations.expect_column_values_to_be_between` on xg_set_encoder/ci bounds; `not_null` on shot_id + match_key.
- `_intermediate__models.yml` adds int_unified_shots entry.
- `assert_xg_between_0_and_1.sql` scope extends to v2 columns if applicable.

## 10. ADR-013 (new)

**Title:** ML Inference Output Tables Flow Python → Bronze → dbt Staging → Gold Fact With Contract

**Relationship to ADR-012 (merged 2026-04-22 PR #177):** ADR-012 covers producer-side *weight* delivery (MLflow @Champion + UC Volume + HF Hub, loudly-or-not-at-all). ADR-013 covers producer-side *prediction-table* delivery (bronze raw → dbt staging → gold mart with contract). Both are "producer-side" — they sit on the same side of the Databricks inference path — but they govern different artifact classes. Together, they enforce: "every weight the consumer tries to load is verifiable; every prediction table the consumer queries is contract-enforced."

**Context:** xG v2 was added as a standalone Python→gold-direct pipeline; created four concrete problems:
1. No contract enforcement on the gold table (schema drift possible).
2. Kimball surrogate keys had to be hardcoded in Python, coupling writer to ADR-011 key schema.
3. Taipy synced-table grants + recreation handled inconsistently vs dbt-built marts.
4. Slim-CI invisibility — `fct_xg_predictions_v2.sql` not existing made the mart invisible to dbt's `state:modified+` selection.

Audit during 2026-04-22 brainstorming session (reported in spec §2 non-goals) confirmed one additional adjacent offender in the codebase (`fct_pausa_values`); both to be corrected under this ADR's scope.

**Decision:** All ML inference outputs MUST flow `Python writer → bronze raw table → dbt staging view → gold dbt mart with contract: enforced: true`. Surrogate keys from ADR-011 resolve in the mart via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit ONLY native identifiers + predictions — never surrogate keys.

**Consequences (positive):**
- Uniform contract enforcement; schema drift caught at build time.
- Clean Python/dbt ownership split (Python owns bronze raw; dbt owns staging + gold).
- Future ADR-011 key-schema changes don't ripple into Python writers.
- Slim-CI sees all ML marts via `state:modified+`.
- Synced-table wiring follows same pattern for all gold marts.

**Consequences (negative):**
- Extra dbt-layer hop per ML mart → marginal build latency.
- During migration, warehouse has two "versions of truth" (bronze raw + gold modeled) — eventual consistency on next dbt build.

**Consequences (neutral):**
- ADR scoped to *inference* outputs (models producing per-row predictions). Ingestion writers (statsbomb/wyscout/idsse/metrica/skillcorner) are explicitly out of scope — bronze IS their purpose.
- ADR does not dictate materialization strategy (incremental vs table); that stays per-mart.

**Alternatives considered:**

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep Python writing to gold; add contract tests alongside Python | No architectural change | Duplicates test infra; no slim-CI benefit; ownership ambiguous | Doesn't solve problems 3 + 4 |
| B. Mandate all ML facts through dbt (chosen) | Uniform; clean ownership; contract-enforced; slim-CI-visible | Extra hop | — |
| C. Python writes to gold bypass-branch, dbt models on top | Could work architecturally | Two writers to same table → ownership ambiguous; race conditions | Ownership anti-pattern |

**Related:** ADR-011 (Kimball surrogate keys — this is the consumption pattern); ADR-002 (silent-exception policy — bronze writers follow); ADR-005 (Lakebase synced-table grants — gold marts follow standard pattern).

**CLAUDE.md Amendment:** Add bullet under "Architecture Principles":
> **ML inference outputs follow ADR-013**: Python writer → bronze → dbt staging → gold mart with contract. Surrogate keys resolved in mart layer via INNER JOIN fct_shots ON shot_id; Python writers never reference match_key/competition_key.

## 11. ADR-011 amendment

Single-row update to the rollout table in `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`:

| PR | Scope | Status |
|---|---|---|
| PR 7 | Tracking + formations + pausa + tail facts migration (**`fct_pausa_values` also promoted Python→dbt mart under ADR-013 as part of this PR**) | Planned |

Plus a footnote line appended to §Notes > Staged rollout policy:
> PR 3 is the first application of ADR-013 (xG v2 promotion); PR 7 is the second (fct_pausa_values).

## 12. Deploy sequence (time-ordered, post-merge)

1. **Merge PR 3 to main** (user-approved, green CI).
2. **Terraform Apply on main** — new fct_xg_predictions_v2 synced-table resource; compute_xg_model_v2 workflow param updates.
3. **Next daily Databricks job run** — `dbt build` produces new fct_shots + fct_xg_predictions with new columns; fct_xg_predictions_v2 built if `xg_v2_enabled=true` in job config.
4. **Manual UI step**: recreate fct_shots_synced + fct_xg_predictions_synced (schema changed underneath); create fct_xg_predictions_v2_synced.
5. **Daily 07:00 UTC cron OR manual** `scripts/maintain_synced_tables.py` runs 3-pass self-heal (event_log ownership → grants → indexes + verify).
6. **Databricks workflow `compute_xg_model_v2` run** — writes to `bronze.xg_predictions_v2` (new location).
7. **Deploy Taipy Space**: staging → E2E smoke → production.
8. **Next HF-publish scheduled run** — statsbomb-shots-on-target preserves schema via (ii); xg-shot-data goes out with (i) renamed + README changelog note.

## 13. Rollback

- **Warehouse-layer break (dbt build fails on new schema):** `git revert <merge_commit>` + Terraform Apply reverts definitions. fct_xg_predictions_v2 synced-table state needs `terraform state rm` before revert. Synced tables recreated back via UI. ~30 min RTO.
- **Taipy-layer break (warehouse OK but Space errors):** redeploy previous Space commit via `manage_space.py deploy production --ref <prior-sha>`. No warehouse touch needed.
- **HF dataset consumer complaint on xg-shot-data rename:** dual-publish (match_id + match_key) in follow-up if needed.

## 14. Risk register

| # | Risk | Mitigation |
|---|---|---|
| 1 | Synced-table recreation is UI-manual; window where Lakebase serves stale schema + Taipy fails | Deploy Taipy AFTER synced tables recreated; staging deploy first |
| 2 | `xg_v2_enabled=false` default means PR-time CI doesn't validate the fct_xg_predictions_v2 contract end-to-end | Add `xg_v2_enabled=true` override to a dedicated "validate-v2-mart" CI job OR accept post-merge validation on first scheduled workflow run |
| 3 | ~~First post-deploy `compute_xg_model_v2` run re-scores all competitions~~ **OBVIATED by D2=ALTER.** The ALTER preserves all 131,077 rows, so the `find_new_ids` guard sees every competition already scored and skips. No rescoring triggered; no $14 cost. |
| 4 | HF `statsbomb-shots-on-target` join-at-export returns NULL for IDSSE/Metrica matches | xG/PSxG trainer only covers SB+WS (competition_id IS NOT NULL filter); no affected rows. Plan-stage verification |
| 5 | int_unified_shots view-upgrade (was ephemeral) may trigger fct_shots full-refresh on first post-deploy build instead of incremental merge | Acknowledged; fct_shots <1M rows; acceptable |
| 6 | Hyrum's Law on either HF dataset: unknown external consumers of the legacy `match_id` column | Dual-column (§6.4, §6.5) — `match_id` remains for 90 days; consumers migrate to `match_key` at their own pace. READMEs carry canonical deprecation changelog with removal-eligible date 2026-07-22. Follow-up PR in 2026Q3 drops `match_id` after the window closes; extend window if HF discussions tab shows active use. |
| 7 | ADR-013 scope could be interpreted too broadly ("all Python writers" vs just inference) | ADR text explicitly scopes to *inference*; ingestion writers explicitly excluded |
| 8 | **Existing 131,077 rows in `bronze.xg_predictions_v2`** carry the pre-migration schema. **Resolved (D2 accepted):** enable `delta.columnMapping.mode = 'name'` if not already set, then `ALTER TABLE DROP COLUMN match_id`. Preserves history, zero downtime. Plan Task 5.0 runs the two ALTERs atomically before the Python writer change lands. | SOP applies to all future Kimball migrations (PR 4-8) — ALTER, never DROP. Potential future ADR-014 to default new Delta tables to column-mapping-enabled. |
| 9 | **Token rotation lag.** Both Databricks DevOps Agent token and HF lakehouse-write token rotated 2026-04-22 evening (unrelated to this PR). Until new tokens propagate to CI / HF Jobs / Databricks workflows, any test that calls those services fails. | Plan stage verifies rotated tokens are in GitHub Actions secrets + Terraform variables + HF org secrets before first E2E run. Local development unaffected (uses personal PAT). |

## 15. Open items for plan stage

The following require investigation during plan-writing (not design-time decisions):

1. Exact `DATASET_REPO` constant value in `scripts/publish_xg_shots_hf.py` (line 348 — couldn't grep directly during brainstorming).
2. PSxG trainer consumer of `statsbomb-shots-on-target` HF dataset — locate `scripts/train_psxg*.py` or equivalent and confirm atomic update (if it exists in repo).
3. `notebooks/train_xg_model.py` fct_shots.match_id usage — confirm exact scope of grep-and-swap.
4. Current Lakebase index set on `fct_shots_synced` and `fct_xg_predictions_synced` — exact index definitions so the drop/recreate plan is precise.
5. `scripts/publish_xg_shots_hf.py` dataset-card changelog — exact markdown wording for the version bump notice.
6. Caller of `fetch_shots_timeline` in the Match Summary render module — verify the returned DataFrame's `match_id` column isn't used as a filter key downstream (should be a pass-through identifier only).
7. Terraform `compute_xg_model_v2` task `parameters` block — verify exact current `--schema` value and whether Terraform passes bronze (dev_bronze/bronze) via env-mapping or hardcoded. Production evidence post-PR #177 shows writes landing in `bronze.xg_predictions_v2`, so this is a verification pass (not an edit).
8. **bronze.xg_predictions_v2 existing-rows migration path** — ALTER TABLE DROP COLUMN (if column-mapping mode enabled) vs full DROP + repopulate on next run. Inspect Delta table properties; pick the less-invasive path.
9. **Rotated-token propagation state** — 2026-04-22 evening rotation of Databricks DevOps Agent + HF lakehouse-write tokens. Confirm new values populated in: GitHub Actions repo secrets (DATABRICKS_TOKEN, HF_TOKEN); Terraform variable stores; HF Jobs secrets; Databricks workflow-level secrets. Spec cannot assume tokens are valid anywhere until this check passes.
10. **`test_xg_model_v2.py` coexistence** — confirm the 4 PR #177 regression tests (`TestV2EnvelopeFeatureNames`, `TestMlflowLookupsUseGoldSchema`, and 2 others) continue to pass after this PR's `_RESULTS_SCHEMA` + UDF output edits. None of those tests touch match_id per their names, so expected outcome is clean coexistence — but verify.
11. **Wheel version reference** — spec and plan should reference wheel 0.3.12 (post-PR #177) as the baseline; any new wheel-shipped code in this PR would require a further bump to 0.3.13 and `scripts/bump_wheel.py` sync of 19 consumers. Expected outcome: no bump (changes are dbt + Taipy + Terraform; `DEFAULT_BRONZE_SCHEMA` addition is trivial and could piggyback on next organic bump).

## 16. Merge reconciliation — PR #177 (`ecf2551`, merged 2026-04-22 evening)

This spec was drafted across one brainstorming session on 2026-04-22. Between the design-section approvals and the spec self-review, **PR #177** landed on main. It partially overlapped this PR's scope. Reconciliation below.

### 16.1 What PR #177 changed

- **ADR-012** (NEW) — Training-to-Production Delivery Hardening. Producer-side *weight* delivery contract: require_mlflow_env + upload_weights_to_uc_volume + set_and_verify_mlflow_champion. Three mandatory destinations (HF Hub + MLflow + UC Volume).
- `src/ingestion/artifact_deploy.py` (NEW) — shared wheel module implementing ADR-012's three helpers. 15 unit tests in `test_artifact_deploy.py`.
- `scripts/train_xg_v2_hf.py` — re-inlined helpers; added feature_names envelope.
- `scripts/train_xg_model_hf.py` — first-time hardening with @Champion + UC Volume writes.
- `src/ingestion/xg_model_v2.py` — fixed DEFAULT_GOLD_SCHEMA MLflow-lookup bug; added feature_names envelope fallback at lines 219-242.
- `src/tests/test_xg_model_v2.py` — +4 regression tests (`TestV2EnvelopeFeatureNames`, `TestMlflowLookupsUseGoldSchema`).
- `workflow-cards/wf-xg-v2.yaml` — `outputs.models` expanded to 3 entries (HF Hub + MLflow @Champion + UC Volume).
- `docs/huggingface/model-cards/xg-v2-model-card.md` — documents UC Volume destination.
- CLAUDE.md — new "Training-to-production delivery contract" rule linking ADR-012.
- `docs/c4/architecture.{dsl,html}` — new `artifactDeploy` container.
- Wheel 0.3.11 → 0.3.12 (19 consumers synced via `scripts/bump_wheel.py`).
- Production verified: 131,077 rows in `bronze.xg_predictions_v2` at 2026-04-22 16:06 UTC; MLflow `xg_model_v2@Champion` → v3 with feature_names envelope; HF + MLflow + UC Volume bytes match.

### 16.2 What THIS spec does that PR #177 did NOT

- **Kimball migration** of `fct_shots`, `fct_xg_predictions`, and the `xg_predictions_v2` column set (match_id → match_key; add competition_key; retain competition_id legacy INT).
- **Promote xG v2 to a dbt mart** (`fct_xg_predictions_v2.sql` + `stg_xg__predictions_v2.sql` + source entry + contract). PR #177 did NOT make xG v2 a dbt mart; it still lives in bronze as raw Python output.
- **Taipy consumer migration** — 4 files (queries/shots.py, queries/match.py, state/shot_map.py, state/match_summary.py).
- **HF dataset export mixed-strategy** — (i) rename for xg-shot-data; (ii) join-preserve for statsbomb-shots-on-target.
- **ADR-013** codifying the consumer-side inference-output-table pattern.
- **ADR-011 amendment** scheduling `fct_pausa_values` promotion into PR 7.

### 16.3 What this spec dropped (now redundant)

- No Terraform `--schema bronze` task edit. PR #177's production evidence confirms v2 already writes to bronze.
- No wheel version bump in this PR (unless a new wheel-shipped module is introduced during plan stage).

### 16.4 What this spec gained (because of PR #177)

- **ADR number bumped from 012 to 013.** All cross-references updated throughout.
- **Risk #8** — existing 131,077 bronze rows carry pre-migration schema; Plan decides ALTER vs DROP+repopulate.
- **Risk #9** — token rotation lag. Services that previously ran with old DATABRICKS_TOKEN / HF_TOKEN cannot run until rotated values propagate to CI / HF Jobs / Databricks secrets.
- **Open item #10** — coexistence check with PR #177's 4 new test_xg_model_v2.py regression tests.
- **Open item #11** — wheel-version baseline confirmed as 0.3.12.

### 16.5 Non-overlap verification

- `xg_model_v2.py::_RESULTS_SCHEMA` still has match_id (NOT touched by PR #177) → this spec's planned edit applies cleanly.
- `wf-xg-v2.yaml::outputs.tables` still has single bronze entry (NOT touched by PR #177) → this spec's planned addition of gold-mart entry applies cleanly.
- `fct_shots.sql`, `fct_xg_predictions.sql`, `int_unified_shots.sql` — not touched by PR #177; all Kimball migration edits apply cleanly.
- Taipy shot/match queries not touched by PR #177.
- `dim_matches`, `dim_competitions`, `int_running_score` not touched by PR #177.

### 16.6 Session continuity

The user rotated the DevOps Agent Databricks token and the HF lakehouse-write token during the spec-finalization step of this session (unrelated HF log leak from a separate session). Old tokens in this session's env are now invalid; session must restart for fresh tokens. This spec file is the durable artifact. Next session's first action:

1. Read this spec end-to-end.
2. Verify rotated-token propagation (open item #9).
3. Verify the plan-stage open items in §15.
4. Invoke `superpowers:writing-plans` to produce the implementation plan.
