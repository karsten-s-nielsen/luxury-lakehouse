# ADR-073: Medallion layer schemas are named, and hf_sync fails its task

**Status:** Accepted
**Date:** 2026-08-07
**Deciders:** Karsten Nielsen
**Supersedes:** none
**Amends:** [ADR-067](ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) §1 (extends the "swallow ≠ pass" rule to `hf_sync`)

## Context

`hf_sync` ran nine sub-operations and reported SUCCESS on every run. On
2026-08-07, run `271010187183650` was inspected line-by-line for the first time:

- **five of nine** sub-workflows raised,
- **a sixth** swallowed a missing upstream at INFO and did nothing,
- the task reported **SUCCESS**,
- and one publisher — `publish_freeze_frame_hf` — published **1,122,820 rows
  correctly** and was then marked failed by post-publish bookkeeping.

Two independent defects, plus the reason both stayed invisible.

### Defect A — one `--schema` threaded to three medallion layers

Terraform passes `--schema bronze` to `hf_sync`
(`terraform/modules/workflows/main.tf:916`). That value is handed to every
sub-operation. It is correct for exactly one of them:

| Sub-operation | Layer it means | Got | Result |
|---|---|---|---|
| `import_psxg_predictions` | bronze | bronze | correct |
| `publish_freeze_frame_hf` | silver (hardcoded) | ignored | published fine |
| `export_shots_on_target` | gold | bronze | `TABLE_OR_VIEW_NOT_FOUND` |
| `publish_xg_shots_hf` | gold | bronze | `TABLE_OR_VIEW_NOT_FOUND` |
| `publish_spadl_vaep_hf` | gold | bronze | `TABLE_OR_VIEW_NOT_FOUND` |
| `export_scoutgpt_training_data` | gold | bronze | failed via `record_watermarks` |
| `prepare_360_training_data` | gold | bronze | **swallowed at INFO** |

`fct_shots` and `fct_action_values` live in `dev_gold` (verified against
`information_schema`); `bronze` has no such tables.

The `{schema}` template was a multi-environment abstraction. **There is exactly
one environment.** The codebase had already settled this the other way — 16
workflow cards hardcode `dev_gold`/`dev_silver` against 7 that template it;
`export_shots_on_target`'s own CLI default is `--schema dev_gold`;
`dbt_runner.py` passes the literal `"dev_gold"`; and
`export_scoutgpt_training_data` already contained the exact fix
(`gold = DEFAULT_GOLD_SCHEMA`, `_ = schema`) — its **module** was corrected and
its **card** was not, which is why it still failed.

This is the fourth instance of one failure class. A safety- or
correctness-relevant property that a caller *can* pass will eventually be
passed wrong:

| Property | Old shape | How it failed |
|---|---|---|
| repo privacy | `private: bool = False` | caller-passed, fail-open ([ADR-072](ADR-072-publish-seam-guarded-frame.md)) |
| sweep | `delete_patterns` optional | 4/4 callers inert |
| HF token | `token: str` | 3/3 wrong on serverless |
| **layer schema** | `--schema` threaded | **6/9 consumers wrong** |

With one environment the layer is not a variable at all — it is a **constant**,
and the correct move is to name it at the point of use, not derive it through
another indirection.

### Defect B — a view declared as a watermark upstream

`record_watermarks` → `_get_latest_data_version` runs `DESCRIBE HISTORY`
(`guards.py:437`). `wf-publish-freeze-frames.yaml` declared
`{catalog}.dev_silver.stg_statsbomb__events`, and every dbt `stg_*` model is
materialized as a **view** (`dbt_project.yml` → `staging: +materialized: view`).
Views have no Delta history, so this can never succeed.

It fires *after* the publish, so the dataset is correct and the task is marked
failed — the inverse of Defect A, and equally undiagnosable from the task state.

### Why none of this was noticed

`hf_sync._run_sub_workflow` catches per-op exceptions, logs at ERROR, continues
— and the task then exits 0. The per-op catch is *correct*: one bad publisher
must not stop the other eight. What was missing is the second half of the rule
this repo already wrote down for `drain.py` (ADR-067 §1):

> A drain worker that swallows a unit failure must still FAIL ITS TASK.

`hf_sync` has the identical shape and never got the identical rule. Every
defect in this chain — the fail-open privacy flag, the inert sweeps, the token
resolution, and now both defects above — was survivable *only* because the task
reported SUCCESS regardless.

## Decision

**1. `hf_sync` fails its task when any sub-workflow failed.**

The per-op `except Exception` stays. `_run_sub_workflow` now returns a bool, and
`run_pipeline` calls `raise_on_failed_sub_workflows(failed, attempted=...)`
after the loop — naming every failed operation, because a bare count sends the
operator back to the logs. `completed` now counts successes, not attempts.

**2. Medallion layer schemas are constants in `shared.constants`.**

`DEFAULT_BRONZE_SCHEMA` / `DEFAULT_SILVER_SCHEMA` / `DEFAULT_GOLD_SCHEMA` join
the existing `DEFAULT_GOLD_SCHEMA`. Consumers name the layer they mean:

```python
_ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
sql = _SHOTS_SQL.format(catalog=catalog, gold=DEFAULT_GOLD_SCHEMA)
```

SQL templates use a `{gold}` placeholder rather than `{schema}`, so the intent
is legible at the point of use and the static gate can mean something. Workflow
cards pin the layer (`{catalog}.dev_gold.fct_x`). Bronze readers — the large
majority of `src/ingestion/` — are untouched: they use the passed schema and it
is correct for them.

**3. Watermark upstreams must be Delta tables.**

Cards declare the bronze table a `stg_` view selects from, not the view.

**4. Enforcement — `src/tests/test_layer_schema_conformance.py`.**

- no card templates the schema of an `fct_`/`dim_` mart;
- no card declares a `stg_*` (view) as a `delta-table` upstream;
- no `src/ingestion/` module interpolates the passed schema into an
  `fct_`/`dim_` reference;
- the layer constants hold the real schema names;
- **and the premise itself is tested** — `test_staging_models_are_still_views`
  asserts dbt still materializes staging as views. If that changes, rule 3 is
  no longer justified and the test says so rather than leaving a mystery ban.

## Consequences

**Positive**

- Six publishers can reach their data for the first time from the job.
- A failing `hf_sync` is now visible in the run list instead of only in a
  250K-character log.
- The wrong-layer and view-upstream classes are both unrepresentable in CI.
- `prepare_360_training_data` stops silently exporting nothing.

**Negative / risks**

- **`dbt_build_output_marts` depends on `hf_sync`.** Making `hf_sync` fail can
  now block the daily dbt build and, transitively, `refresh_synced_tables` and
  `run_model_validation`. The dependency is real — `import_psxg_predictions`
  writes `bronze.psxg_predictions`, which `stg_psxg__predictions` reads — but it
  couples an *external* service (HF Hub) to the internal data pipeline: an HF
  outage would now stop dbt. The precedent for fixing this exists — PR-Cycle-B
  (2026-05-01) split `import_obso_results` into its own task for exactly this
  reason — and splitting `import_psxg_predictions` the same way would leave
  `hf_sync` pure-export and blocking nothing. **Deliberately not done in this
  change**; tracked as follow-up, and flagged to the operator at review time
  rather than decided here.
- `--schema` is now inert for gold-reading consumers. `export_shots_on_target`
  keeps its CLI flag (its default was already `dev_gold`, so standalone
  behaviour is unchanged) but no longer honours a non-gold value. With one
  environment there is no valid non-gold value.
- The ban on templated gold marts is scoped to `fct_`/`dim_` prefixes. A gold
  mart named outside that convention would not be caught.

## Alternatives considered

**Flip the Terraform parameter to `dev_gold`.** One line, and it fixes six
consumers — but it breaks `import_psxg_predictions`, the one genuine bronze
writer, and leaves the same trap for the next sub-operation added.

**Derive the schema per-table from the table's layer.** Rejected on review: it
invents a resolution layer to serve a multi-environment distinction that does
not exist here, and it contradicts the convention 16 cards already follow.

**Make `_get_latest_data_version` tolerate views.** Rejected: a view has no
change history, so any fallback would be inventing a watermark signal. The
honest fix is to watermark the table that actually changes.

**Leave `hf_sync` exiting 0 and add a separate fan-in gate task**
(the ADR-068 `verify_action_context_drain` shape). Rejected as more machinery
for the same outcome — once `hf_sync` is off the critical path, the task can
simply fail. Worth revisiting if the import/export split lands.
