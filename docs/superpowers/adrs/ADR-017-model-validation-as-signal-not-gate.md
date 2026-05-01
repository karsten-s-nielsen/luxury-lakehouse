# ADR-017: Model validation is a quality signal, not a correctness gate

| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

The daily Databricks ingestion job (`soccer-analytics-ingestion-dev`)
contained a structural anti-pattern: `dbt_build` (the task that
materializes 36 gold mart tables) had `depends_on = run_model_validation`.
The original intent was "don't refresh marts if validation says drift",
but the topology was self-contradictory:

- `run_model_validation` reads from `fct_xg_predictions`,
  `fct_action_values`, `fct_passes`, `fct_physical_stats`,
  `fct_pausa_values` — all GOLD marts.
- `dbt_build` is what BUILDS those marts.
- Therefore `run_model_validation` runs BEFORE `dbt_build` and reads
  YESTERDAY's mart data, not today's.

The dependency was forcing today's mart refresh to wait on a quality
check of yesterday's data. A validation regression on yesterday's data
blocked today's data from reaching every downstream consumer
(Lakebase synced tables, Taipy app, HF datasets).

The forcing function: PR-LL2 close-out (2026-04-29). Two latent column
drifts in `model_validation.py` surfaced sequentially via the
`tolerate_missing_table()` narrowing landed in PR #122
(`xg_prediction` → `xg_gradient_boosted` fixed in PR #225;
`match_id` → `match_key` fixed in PR #226). After both name-drift fixes,
a third — type-drift on `is_line_breaking` (BOOLEAN, not numeric) —
surfaced. Each fix-PR-CI-merge-redeploy-repair cycle was ~20 minutes.
This ADR + topology change ensures the next such regression cannot
block PR-LL3 / PR-LL4 close-outs the same way.

## Decision

`run_model_validation` runs INDEPENDENTLY of `dbt_build` and
`refresh_synced_tables`. It remains in the daily DAG (still triggered
by upstream compute tasks via its own dependencies), but no downstream
mart-materialization or synced-table-refresh task waits on its result.
Validation is a signal — drift gets logged, alerted, and surfaced in
observability — but it does not gate data freshness for downstream
consumers.

Implementation: removed `depends_on { task_key = "run_model_validation" }`
from the `dbt_build` task in `terraform/modules/workflows/main.tf` and
removed `- wf-model-validation` from the `depends_on` list in
`workflow-cards/wf-dbt-build.yaml`. `refresh_synced_tables` already
depended only on `dbt_build`, so it inherits the change automatically.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep dependency, ensure validation always passes via better tests | Strong gate semantics if it actually worked | Validation reads YESTERDAY's data — gate is structurally meaningless. Three latent bugs in 4 hours of close-out work prove the gate is fragile, not durable. | Doesn't fix the root cause |
| B. Use Databricks `run_if = "ALL_DONE"` on `dbt_build` | Per-task downgrade of all dependencies to "soft" | Too aggressive — `dbt_build` SHOULD hard-block on `compute_*` tasks (which produce its actual inputs). One-bit-per-task semantics doesn't distinguish compute-input from quality-signal. | Loses correctness on compute-input deps |
| C. Remove validation→dbt_build edge entirely (chosen) | Topology matches data flow; validation drift never blocks data freshness; one semantic change, easy to reason about | Validation regressions can no longer "stop the bleed" by halting mart refresh. | — (chosen) |
| D. Move validation AFTER `dbt_build` | Validates today's data; gate is now meaningful | Doubles end-to-end latency; validation latency joins critical path; same fragility re: bug-in-validator blocking mart refresh | Defers the same anti-pattern |

## Consequences

### Positive

- Single validator regression no longer blocks every downstream
  consumer (Lakebase synced tables, Taipy app, HF dataset push).
- DAG topology now matches data flow direction.
- Repair-runs and partial re-runs are simpler — fewer cross-task
  dependencies to repair.
- `dbt_build` parallelizes with `run_model_validation` instead of
  serializing after it (modest end-to-end latency reduction).

### Negative

- A validation drift signal no longer "stops the bleed" by halting
  mart refresh. Drift detection must be paired with alerting (already
  the case via `analytics.model_validation.ValidationResult` →
  observability schema) so it surfaces in dashboards instead of
  failing the job.
- Operators reading the failed-job email now need to distinguish
  between "data flow broke" (compute task or `dbt_build` failed) and
  "quality signal regressed" (validation task failed). Previously
  both flagged as "job failed". Mitigation: validation alerts route
  to a separate channel from operational failures (existing pattern
  in `observability` schema).

### Neutral

- The validation task itself is unchanged. It still runs daily,
  reads gold marts, computes drift metrics, writes results to the
  observability schema.
- `dbt_build` still hard-depends on the leaf compute tasks that
  produce its actual inputs (StatsBomb, Wyscout, IDSSE, Metrica,
  SkillCorner ingests + the compute-* family that produces bronze
  fact data). PR-Cycle-B (2026-05-01) extended the leaf-fan-in from
  8 to 12 by adding `compute_pausa`, `compute_elastic_sync`,
  `backfill_statsbomb_360`, and `compute_embeddings_360` — those four
  tasks write bronze tables that dbt's `stg_*` views read, but the
  TF DAG had been silently allowing them to run in parallel with
  `dbt_build` for an unknown duration. Without these edges today's
  `dbt_build` builds today's gold marts from yesterday's bronze for
  the four affected sources (1-day lag class). The new edges are
  enforced by `src/tests/test_workflow_dag_bronze_reads.py` going
  forward.

## CLAUDE.md Amendment

None.

## Related

- **Commits:** to be filled in at merge
- **Issues / PRs:** PR #224 (PR-LL2), PR #225 (xg_prediction fix),
  PR #226 (match_id fix), PR #227 (this — type fix + decoupling)
- **ADRs:**
  - ADR-002 — silent-swallow elimination, the rule that surfaced the
    three latent bugs whose cascade triggered this ADR
  - ADR-011 — Kimball unified match dimension, source of the
    `match_id → match_key` rename that caused PR #226's drift
  - ADR-013 — ML inference outputs (xG v2 mart pattern), tangentially
    related: gold marts as the canonical inference output surface
- **External references:** Databricks Jobs `depends_on` semantics —
  https://docs.databricks.com/api/workspace/jobs/create#tasks-depends_on
