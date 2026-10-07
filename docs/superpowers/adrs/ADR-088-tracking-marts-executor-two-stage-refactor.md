# ADR-088: Tracking-marts + shot-freeze executor two-stage refactor (build-spill-score)

| Field | Value |
|---|---|
| **Date** | 2026-10-06 |
| **Status** | Accepted |
| **Deciders** | Karsten |

## Context

`compute_tracking_marts` OOMed (exit 137) on gradientsports units. Root cause was architectural, not per-unit size: `tracking_marts_driver._read_unit` did `trk.toPandas()` on a whole `(match, period)` half onto the 16 GB driver, then all scorers ran driver-side (profiled 3.4 GB peak/unit; oriented frame alone 1.27 GB float64). `compute_action_context` processes the same frames without OOM because it is executor-distributed (ADR-045 `repartition + mapInPandas`). The fix had to move the frame build + scoring off the driver.

Two constraints shaped the design. (1) The six tracking-marts outputs are **different-cardinality** — off_ball_runs (per-run), defensive_credit agg (per-action) + long (per-action×defender), gkdv_observations (per-keeper-frame), gk_decision (per-decision), rest_defense (per-sample) — so they cannot share one `mapInPandas` output schema, and rebuilding the oriented frame once per mart would pay the dominant BUILD cost six times. (2) The unit-global action→frame link means the scorers cannot be chopped to AC's sub-unit `frame_batch_id` grouping; their grain is the whole `(match, period)`. This refactor rides the silly-kicks 4.128 adoption (float32 frames, native DAS), which also re-enables the gkdv arm (previously gated off for the per-frame DAS cost ADR-082 described).

## Decision

`compute_tracking_marts` and `compute_shot_freeze_frames` move from driver-bound `toPandas` to the ADR-045 `repartition + mapInPandas` executor transport. For tracking-marts (six outputs) this is a **two-stage** design: **Stage 1** builds each unit's oriented frames ONCE via `mapInPandas` and spills them to a UC-Volume Parquet keyed by `(provider, match_id, period)`; **Stage 2** runs six per-mart scoring passes that read the spill back and score one mart each. `process(unit)` stays the per-unit unit-of-work boundary `drain_worker` wraps, so the ADR-068 events / watchdog / circuit-breaker / rollforward are unchanged. shot_freeze (single output) uses the transport directly with no spill, which also onboards IDSSE (metrica stays excluded — legacy data).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep driver-mode, shrink per-unit | smallest diff | the 16 GB OOM is serverless-opaque; float32 alone (0.64 GB) + scorer working set still risks the driver | does not move work off the driver — the actual fix |
| B. Single whole-unit `mapInPandas` emitting all six marts (union-stack schema) | one pass | six different-cardinality outputs can't row-align into one schema; a wide nullable union with cross-mart column/type collisions is fragile | the multi-output grain mismatch is intrinsic |
| C. Six `mapInPandas` passes, each rebuilding frames | no spill | pays the dominant BUILD cost 6× | unacceptable throughput |
| D. **Build-once → spill → six per-mart passes (chosen)** | build cost paid once; each mart a clean single-output pass; per-mart neutrality testable; memory bounded (Stage-1 ~0.64 GB float32, no scorer working set on top) | a UC-Volume Parquet intermediate + 7 Spark jobs/unit | — |

## Consequences

### Positive
- The OOM is fixed: the build holds ~0.64 GB (float32) on an executor task, not 3.4 GB on the 16 GB driver; each Stage-2 mart reads ~0.64 GB back on an executor with sk `batch_size` bounding the scorer working set.
- gkdv is re-enabled (sixth Stage-2 mart) — native DAS (ADR-107) + the off-driver move resolve the watchdog-exceedance ADR-082 gated it on.
- shot_freeze onboards IDSSE (the per-period driver `toPandas` that deferred it is gone).
- Per-mart scoring is pure-testable on fixtures; the Spark dispatch is validated by `_make_streaming_group_mapper`'s carry/flush test + a pyspark Docker harness + live Part-B.

### Negative
- A per-unit UC-Volume Parquet spill intermediate (cleaned up in a `finally`; overwritten per run). Requires a provisioned spill volume: `databricks_volume.tracking_marts_build_spill` (bronze) + the ingestion-SP `READ_VOLUME`/`WRITE_VOLUME` grant in `terraform/modules/catalog/main.tf` — a new operator precondition. (Missed in the original PR, caught by the ADR-087 preflight canary with `[UC_VOLUME_NOT_FOUND]`; added in the `fix/tracking-marts-spill-volume` hotfix 2026-10-07.)
- 7 Spark jobs per unit (1 build + 6 score) vs the old 1 driver pass — a throughput/dispatch-overhead trade, measured in the fit profile (ADR-082 budget re-measure).
- Built-frame schema is per-provider (smoothing columns for idsse/gradientsports; `team_id` object for gradientsports vs category elsewhere; `visibility` BooleanType) — declared explicitly with a parity test (ADR-033).

### Neutral
- The spilled built frames round-trip through Parquet; `read_built_frames` restores the F1b dtypes (a Spark-written Parquet loses pandas category/nullable metadata).

## Related
- **Specs:** `docs/superpowers/specs/2026-10-06-tracking-marts-executor-refactor-sk4128-design.md`
- **Plans:** `docs/superpowers/plans/2026-10-06-tracking-marts-executor-refactor-sk4128.md`
- **ADRs:** builds on ADR-045 (repartition+mapInPandas dispatch); amends ADR-082 (gkdv un-gated); consumes silly-kicks 4.128 (ADR-106 float32 frames / ADR-107 native DAS).
