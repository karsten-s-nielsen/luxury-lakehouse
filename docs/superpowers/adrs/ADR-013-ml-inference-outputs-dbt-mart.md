# ADR-013: ML Inference Output Tables Flow Python → Bronze → dbt Staging → Gold Fact With Contract

| Field | Value |
|---|---|
| **Date** | 2026-04-22 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (human), Claude Opus 4.7 (AI) |

## Context

xG v2 was added to the warehouse as a standalone Python writer that targeted a gold-layer Delta table directly. This bypassed dbt in four concrete ways:

1. **No contract enforcement** — schema drift possible per run (the writer's `_RESULTS_SCHEMA` was the only check; no dbt `contract: enforced: true`).
2. **Kimball surrogate keys hardcoded in the Python writer** — the writer would have needed to resolve `match_key` from `dim_matches` itself, coupling it tightly to [ADR-011](ADR-011-unified-kimball-match-dimension.md)'s key schema.
3. **Synced-table wiring handled inconsistently** vs dbt-built marts (Terraform, grants, indexes, refresh registry).
4. **Slim-CI (`state:modified+`) couldn't see the mart** because no `.sql` file existed for it — dbt's graph didn't know the mart existed.

The 2026-04-22 brainstorming for PR 3 of the ADR-011 migration confirmed one adjacent offender in the codebase (`fct_pausa_values`); both must be corrected under this ADR's scope, with pausa scheduled into PR 7 of the ADR-011 rollout.

This ADR is the **consumer-side** counterpart to [ADR-012](ADR-012-training-to-production-delivery-hardening.md). ADR-012 is producer-side *weight delivery* (MLflow `@Champion` + UC Volume + HF Hub, loudly-or-not-at-all). ADR-013 is producer-side *prediction-table delivery* (bronze raw → dbt staging → gold mart with contract). Both sit on the same side of the Databricks inference path but govern different artifact classes.

## Decision

All ML inference outputs flow `Python writer → bronze raw table → dbt staging view → gold dbt mart with contract: enforced: true`. Surrogate keys from ADR-011 resolve in the mart layer via `INNER JOIN fct_shots ON shot_id` (or the equivalent identity fact for non-shot inference). Python writers emit ONLY native identifiers + predictions — never surrogate keys.

### Scope

- **In scope:** Inference outputs — Python workflows producing per-row predictions over an existing identity fact (xG v1, xG v2, PSxG, VAEP action values when re-architected, future models).
- **Out of scope:** Ingestion writers. Bronze IS their contractual output.

### Normative requirements for any new ML inference pipeline

1. **Bronze raw table** with a defined schema (documented in `_<domain>__sources.yml`) and `_ingested_at` audit column.
2. **`stg_<domain>__predictions.sql`** view — dedup + type-cast + **no key resolution**.
3. **`fct_<domain>_predictions.sql`** mart — `contract: enforced: true`, keys via `INNER JOIN` to the identity fact (usually `fct_shots`), `liquid_clustered_by=['match_key']` (or equivalent fact surrogate).
4. **Workflow card `outputs.tables`** lists both the bronze and the gold entries; the gold entry carries `dbt_model: <name>`.
5. **Terraform synced-table resource** + `SYNCED_TABLES` registry entry + `create_indexes.py` index set covering the mart's filter columns.
6. **Python-side writer parity constant** (e.g. `_XG_V2_BRONZE_COLS`) mirrored by `src/tests/test_bronze_live_schema.py` via the file's permissive `missing = expected - actual` contract.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Python writes to gold + contract tests alongside | No dbt hop | Duplicates test infra; ownership ambiguous; slim-CI invisible; does not solve problems 3+4 | Doesn't solve problems 3+4 |
| B. ALL ML facts through dbt (CHOSEN) | Uniform; contract-enforced; slim-CI-visible; clean ownership | Extra hop per mart (marginal build latency) | — |
| C. Two writers to gold (Python bypass branch + dbt layering) | Flexible | Ownership anti-pattern; race conditions; two schema truths | Ownership ambiguity IS the bug we're fixing |

## Consequences

### Positive

- Uniform contract enforcement across v1 + v2 + all future ML marts.
- Python writer never needs to know about ADR-011 key schema — mart layer handles it.
- Synced-table wiring follows one pattern (TF resource + refresh list + index set).
- Slim-CI sees every ML mart through `state:modified+`.
- Bronze re-ingest never requires rebuilding downstream ML tables — only `dbt run`.

### Negative

- One extra dbt hop per ML mart (marginal build latency on incremental runs).
- Two "versions of truth" during migration: bronze raw + gold modeled. Eventual consistency on next dbt build.

### Neutral

- Materialization strategy (`incremental` vs `table`) remains per-mart.
- Does not apply to ingestion writers — bronze IS their purpose.

## Related

- [ADR-011](ADR-011-unified-kimball-match-dimension.md) — Kimball surrogate keys; this is the consumption pattern
- [ADR-012](ADR-012-training-to-production-delivery-hardening.md) — producer-side weight-delivery counterpart (weights ⇄ prediction-tables)
- [ADR-005](ADR-005-lakebase-synced-table-grants.md) — Lakebase synced-table grants apply to ADR-013 marts identically
- [ADR-002](ADR-002-silent-exception-swallow-elimination.md) — bronze writers follow the silent-exception policy

## CLAUDE.md Amendment

Add one bullet under "Architecture Principles":

> **ML inference outputs follow [ADR-013](docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)**: Python writer → bronze raw → dbt staging view → gold mart with `contract: enforced: true`. Surrogate keys resolve in the mart via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit only native identifiers + predictions. First applied in PR 3 (xG v2 promotion); PR 7 extends to `fct_pausa_values`.

## Notes

### First two applications

- **PR 3 (this PR):** xG v2 promotion — `fct_xg_predictions_v2.sql` is the first ADR-013 mart. Paired with the xG v1 mart restructure (`fct_xg_predictions.sql` also INNER-JOINs fct_shots per this pattern).
- **PR 7 (planned):** `fct_pausa_values` promotion to dbt mart, atomically with its ADR-011 Kimball migration.

### Interaction with wheel bumps

ADR-013 marts are dbt-native (pure SQL + YAML), so adding or updating a mart does NOT require a wheel bump. The Python writer (bronze emitter) is wheel-shipped and follows normal wheel-bump cadence for its own changes.
