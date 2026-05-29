# ADR-028: Hexagonal architecture for compute pipelines (recommended, not mandated)

| Field | Value |
|---|---|
| **Date** | 2026-05-28 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

AC-1 (`bronze.spadl_action_context`) timed out at 1800 s per IDSSE half and had never produced a row, so its enrichment logic was effectively unverifiable: the only way to run it was a full Databricks `applyInPandas` job, and the only way to check correctness was to inspect an empty table. The enrichment math (`_enrich_tracking_match` and tiers) was already pure pandas/numpy/silly-kicks — the Spark dependency was purely in the I/O and dispatch around it.

We needed to run the *real* enrichment on *real* data locally to prove correctness and to profile the timeout, without deploying. More generally, ingestion may one day run outside Databricks, so coupling domain math to Spark is a standing liability.

The forcing function: a 30-min-per-half pipeline that writes zero rows is not debuggable in place.

## Decision

Compute pipelines SHOULD follow a hexagonal (ports-and-adapters) split: pure domain math lives in `src/analytics/<pipeline>/` (no pyspark, enforced by import-linter), behind small `Protocol` ports (sources + sink) that take a `WorkUnit` and return pandas; Spark/Delta adapters and the `applyInPandas` UDF live in `src/ingestion/`, which is the only composition root. The shared unit of compute is a single function (e.g. `enrich_batch`) called identically by the Spark UDF (once per group) and a local loop, so production and local runs execute the same code by construction. This is a **recommended convention for new and substantially-touched pipelines, not a retrofit mandate** for existing ones.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep Spark-coupled, debug via Databricks jobs | No refactor | Cannot run/verify/profile locally; every iteration is a cloud job on an empty table | The forcing function — undebuggable in place |
| B. Mandate hexagonal for ALL pipelines now | Uniform | Large speculative refactor of working code; violates YAGNI | Retrofit cost unjustified for pipelines that already work |
| C. Recommend hexagonal for new/touched pipelines (chosen) | Local runnability + testability where it pays; no forced churn | Two styles coexist during transition | — |

## Consequences

### Positive
- The real pipeline runs locally on a real game (IDSSE J03WMX) with zero Databricks dependency (a full real-pipeline regen is ghost_gk-dominated at tens of minutes; the CI differential reads the frozen golden in <1 s), enabling a differential vs the legacy oracles and a per-function profiler.
- Domain math is unit-testable and import-linter-isolated from pyspark.
- Prod/local equivalence is structural (same `enrich_batch`), not asserted — the batch loop replicates the Spark `groupBy(frame_batch_id)` dispatch exactly (H3).
- **Running the real pipeline locally + the differential surfaced a latent production correctness bug** the Spark-only path never could (it wrote an empty table): the legacy `analytics.elastic_sync` (`elastic_sync_results`) has an IDSSE frame-origin error — it aligns events to `frame ≈ 25·ts` (0-based) instead of the correct `10000 + 25·ts`, so it has no results for the first ~400 s and ~400 s-misaligned ones after. The new silly-kicks 3.25.0 path is correct. Because the legacy oracle is therefore invalid, AC-1 `elastic_*` is range-checked (INVARIANT_ONLY), not oracle-compared. See `memory/project_legacy_elastic_sync_frame_origin_bug.md`.

### Negative
- Two architectural styles coexist until older pipelines are migrated (if ever).
- The domain/adapter split adds one indirection layer (ports) for pipelines that adopt it.

### Neutral
- Local fixtures (committed Parquet) and an extract tool become part of the pipeline's test surface.

## Related
- **Specs:** `docs/superpowers/specs/2026-05-28-action-context-hexagonal-and-perf-design.md`
- **ADRs:** complements ADR-013 (ML inference output contract), ADR-027 (analytics-env cold-start)
- **Code:** `src/analytics/action_context/` (domain + ports + local adapters), `src/ingestion/action_context.py` (composition root)
