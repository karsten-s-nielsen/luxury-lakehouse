# ADR-074: hf_sync process isolation, and driver memory at the LifecycleHook seam

**Status:** Accepted
**Date:** 2026-08-08
**Deciders:** Karsten Nielsen
**Amends:** [ADR-073](ADR-073-layer-schemas-named-and-hf-sync-fails-loud.md) (which made `hf_sync` fail its task — the change that turned an intermittent OOM into a dbt blocker)
**Closes:** TODO SEC7

## Context

`hf_sync` ran nine sub-operations **in one driver process**. On 2026-08-07, run
`49905842293930` attempt 1 was killed:

```
Execution ran out of memory.
The Python process exited with exit code 137 (SIGKILL: Killed).
```

Exit 137 is the OS OOM-killer terminating the **Python driver**, not Spark. The retry
succeeded, which made it look flaky rather than structural.

### What was measured, not assumed

A read-only diagnostic (run `939215830803445`) executed the exact memory-heavy path of
the publisher that died — query → `toPandas` → `prepare_public_upload` — alone in a
clean driver of the same size:

| Stage | Peak RSS | Δ |
|---|---|---|
| baseline (Spark session up) | 2.52 GB | — |
| `spark.sql` (lazy) | 2.52 GB | 0 |
| **`toPandas` (9,756,155 rows)** | **6.97 GB** | **+4.45 GB** |
| `prepare_public_upload` (guard+split+drop) | 6.97 GB | **0** |
| `groupby` + `drop_columns` × 5 partitions | 6.97 GB | **0** |

**6.97 GB of a ~16 GB driver — 44% utilisation.** The publisher is not too big. The
`hf` environment is `wheel + huggingface_hub` only, so the diagnostic environment was
representative.

### Three designs rejected on that evidence

- **Chunk the read** by `(data_source, competition_id)` — the read costs 4.45 GB of a
  ~9 GB surplus. It would have fixed nothing while appearing to.
- **Spark → UC Volume → upload** — needs the Spark-side guard that does not exist
  (TODO SEC6), for a problem that turned out not to exist.
- **Move to the existing HF Jobs twin** — `scripts/publish_spadl_vaep_hf.py` runs on
  `flavor: cpu-basic`, which is **also 16 GB**, and materialises via
  `pa.concat_tables(...).to_pandas()`. Identical ceiling.

### The known unknown

What consumed the remaining ~9 GB in the shared process is **unidentified**. The three
sub-operations preceding this publisher are Spark-native with no `.toPandas()`. Three
theories were advanced during diagnosis and all three were wrong. This ADR does not
offer a fourth.

### Two defects found while fixing it

**Staleness.** `hf_sync`'s `depends_on` is
`{backfill_statsbomb_360, compute_elastic_sync, compute_spadl_vaep, resolve_players}`
— **no dbt stage at all**. `fct_action_values` is tagged `intermediate_mart` and built
by `dbt_build_intermediate_marts`, a *sibling*. So `publish_spadl_vaep_hf` has been
publishing a mart it has no ordering against, bounded only by its own watermark gate
(which skips when the mart is unchanged, so the effect was a ~one-cycle lag rather
than arbitrary staleness). **That gate is exactly what a naive promotion would have
deleted** — the two defects are one problem seen from both ends.

**Standalone-capability is not free.** `publish_spadl_vaep_hf.main()` was four lines:
`configure_logging → parse_ingestion_args → get_spark_session → run_pipeline`. The
watermark cycle lived in `hf_sync`'s `_make_watermark_op` factory, and hook
registration in `hf_sync.main()`. Promoted as-is, the most expensive driver operation
in the platform would have run **unconditionally every day** with **no
`workflow_cost_live` row** — and no test would have gone red.

`import_psxg_predictions`, split in the same cycle, *was* built standalone-capable (its
own `skip_guard`, its own `bootstrap_hooks`). **That asymmetry is why this shipped as
two commits rather than one.**

## Decision

**1. Driver memory is a `LifecycleHook`, not a bespoke probe.**

`ingestion.memory_hook.MemoryHook` implements the port this repo already uses for
cross-cutting workflow observability, registered in `bootstrap_hooks` beside
`CostEstimateHook`. Every `@workflow` reports memory — including the tasks split out
here, which a loop-body probe inside `hf_sync` would have left as the platform's only
blind spot, precisely where a `.toPandas()`-shaped leak would next land.

`LifecycleHook` is a `Protocol`, so the implementation lives in `ingestion/` (it
imports `shared.memory`); `.importlinter` forbids `workflows → shared` and
`src/workflows/` is untouched.

**2. Two numbers, read differently.** `peak` is a high-water mark — a delta means "this
raised the ceiling". `resident` reveals **retention**: a workflow ending with a high
resident value left something behind. Only the second can identify a leak.

**3. Two sub-operations get their own tasks.** `import_psxg_predictions` (SEC7 — the
only leg dbt needs) and `publish_spadl_vaep` (measured at 6.97 GB). `hf_sync` is now
**export-only** and gates nothing.

**4. `publish_spadl_vaep_hf.main()` does what the factory used to** — `bootstrap_hooks`
plus the full watermark cycle, mirroring `ingestion.model_validation.main`.

**5. `publish_spadl_vaep` depends on `dbt_build_intermediate_marts`** (not
`output_marts`, which excludes `+tag:intermediate_mart`), registered in
`_GOLD_READ_REQUIREMENTS`.

**6. Gates, because every one of these was invisible to a hardcoded list.**

| Gate | Closes |
|---|---|
| `test_card_phase_environments_match_terraform` | card/TF `environment` drift, both direct-task and orchestrated legs |
| `test_card_environments_are_defined_in_terraform` | a card naming an environment that does not exist |
| `test_direct_task_modules_register_hooks_in_main` | `bootstrap_hooks` missing from a standalone `main()` |
| `test_registered_hooks_match_the_lifecycle_protocol` | hook signature drift (pyright cannot see it — structural typing) |
| `test_hf_sync_is_export_only` | an importer moving back into hf_sync |
| `_GOLD_READ_REQUIREMENTS` / `_BRONZE_READ_REQUIREMENTS` entries | either split being silently reverted |

## Consequences

**Positive**

- The publisher gets a fresh 16 GB driver where its workload is *measured* at 6.97 GB.
- An HF Hub outage can no longer block the daily dbt build.
- A ~one-cycle staleness bug in the published VAEP dataset is fixed.
- Every `@workflow` in the platform now reports driver memory.
- The environment gate found **25** drifted card declarations, including nine
  sub-operation cards advertising `analytics` for a task that runs on
  `wheel + huggingface_hub`.

**Negative / bounded**

- **The staleness fix covers ONE of five gold readers.** `hf_sync` retains four —
  `export_shots_on_target`, `publish_xg_shots_hf`, `export_scoutgpt_training_data`,
  `prepare_360_training_data` — and **still has no dbt edge at all**. They remain
  siblings of the stages that build their inputs, bounded only by their own watermark
  gates, and none is registered in `_GOLD_READ_REQUIREMENTS`. Tracked as **SEC9**;
  registering `hf_sync` there today goes red immediately, which is what makes SEC9
  actionable rather than speculative.
- **Isolation fixes this publisher, not the unknown.** If the ~9 GB consumer is a real
  leak, it resurfaces for whichever sub-operation now runs last. `MemoryHook` is the
  detector, not the cure — **SEC8**.
- **The `wf-hf-sync` memory line is an envelope, not a peer.** Its delta is the sum of
  its children's ceiling rises and will be the largest number every run, by
  construction. Compare sub-operation lines to each other.
- **Four pre-existing modules** behind their own task never call `bootstrap_hooks`
  (`staleness_monitor`, `dbt_runner`, `refresh_synced_tables`, `shot_freeze_frames`) —
  allowlisted in `_PRE_EXISTING_HOOK_GAPS`, deliberately not fixed. `dbt_runner` needs
  *why* before *fix*: three dbt tasks with no `CostEstimateHook` either means dbt
  builds have no cost rows, or they register by another path.
- `on_skip` emits nothing (a skipped workflow consumed nothing, and `hf_sync` already
  logs the skip). This differs from a loop-body probe, which would emit a line per skip.

## Notes for the next reader

`on_skip`'s **arity is load-bearing**: `runner.py` dispatches `on_skip(ctx, str(exc))`.
A one-argument version raises `TypeError` on every skip; `_dispatch` swallows it but
logs at ERROR **with a traceback**, burying the memory lines this ADR exists to
produce. Nothing declares `MemoryHook` as a `LifecycleHook`, so pyright cannot catch
it — `test_registered_hooks_match_the_lifecycle_protocol` is the price of the Protocol
seam.

The adapters are **executed** by a Linux-gated test on CI. They were `# pragma: no
cover` in an earlier draft: `ru_maxrss` is KiB on Linux but bytes on macOS, and a
1024× error would have made every number in the SEC8 investigation confidently wrong.
