# ADR-031: Executor-side per-batch progress logging (Spark Connect compatible)

| Field | Value |
|---|---|
| **Date** | 2026-05-30 |
| **Status** | Superseded by ADR-032 (2026-05-31) |
| **Deciders** | Karsten Nielsen |

> **Superseded.** The Decision below — emit `AC1_BATCH` log lines from inside the
> UDF closure and read them from the driver log stream — does **not** work on
> Databricks serverless. Spark Connect routes executor stdout/stderr to the
> *executor* log, which never reaches the parent task log (`jobs.get_run_output`
> / Jobs UI "Task logs"); only *driver*-process stdout lands there. Verified
> empirically 2026-05-31 (iteration `961253300571334`): the `AC1_BATCH` lines
> were invisible. The working design — a driver-side poller plus executor-written
> UC-Volume rendezvous markers — is in
> [ADR-032](ADR-032-spark-connect-safe-executor-visibility.md). This ADR is
> retained for the record: it correctly killed the `sparkContext` accumulator
> design (PR #320), but was wrong about where executor logs surface.

## Context

PR #320 added a driver-side observability mechanism for AC-1's `compute_action_context` task: a Spark `LongAccumulator` (`batches_counter = spark.sparkContext.accumulator(0)`) incremented inside the `applyInPandas` UDF on each successful batch, paired with a `_BatchHeartbeat` driver-side thread polling `accumulator.value` every 30 seconds to emit progress logs. The intent was operator visibility during the long-running (10+ minute) `write_delta_table` action that otherwise emits no signal between "Processing match X" and "wrote N rows."

First production run of the post-PR-#320 code (run 887895768424884, 2026-05-30) crashed in `_process_tracking_match` with:

> `PySparkAttributeError: [JVM_ATTRIBUTE_NOT_SUPPORTED] Directly accessing the underlying Spark driver JVM using the attribute 'sparkContext' is not supported on serverless compute.`

Databricks serverless compute uses Spark Connect, which structurally forbids access to `sparkContext`, `_jsc`, `_jconf`, `_jvm`, `_jsparkSession`, and `newSession` on the driver. There is no Spark Connect equivalent for `LongAccumulator`; the protocol does not expose accumulator state. The driver-aggregated design was structurally incompatible with the only environment AC-1 actually runs in.

The bug shipped because PR #320's tests only exercised the `analytics/action_context/pipeline.py` hexagon domain (which has no Spark dependencies), never the production driver entry point in `ingestion/action_context.py:_process_tracking_match` where the `spark.sparkContext` call lives. Captured separately in [[feedback_test_production_driver_entry_point]].

## Decision

Replace the driver-side `LongAccumulator` + `_BatchHeartbeat` thread with one terse log line emitted from inside the UDF closure at the end of each successful batch:

```python
_logger.info(
    "AC1_BATCH provider=%s match_id=%s batch_id=%s elapsed_s=%.1f",
    provider, match_id_val, batch_id_val, _time.monotonic() - _batch_start,
)
```

The UDF closure already runs per-batch on the executor; the log line piggybacks on that existing execution context with zero driver-JVM access. The Databricks driver log stream captures executor stdout/stderr, so the operator sees these lines in real time from the same place they would have read the heartbeat output.

Delete: `_BatchHeartbeat` class, its 6 lifecycle unit tests, the `batches_counter` parameter threaded through `_make_action_context_udf`, the `spark.sparkContext.accumulator(0)` call site, the `with _BatchHeartbeat(...)` wrapper around `write_delta_table`, and the now-unused `threading` import. Keep: `_iteration_fingerprint` and `_iteration_summary` (both driver-side but Spark-Connect-compatible — they touch only stdlib + the `silly_kicks.__version__` import).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `try/except AttributeError` around `spark.sparkContext` — skip accumulator + heartbeat on serverless | Smallest possible diff; preserves the heartbeat on non-serverless | Treats serverless as a degraded environment when serverless IS production for AC-1; observability is exactly what we need there, not where to drop it | Rejected — solves the wrong problem |
| B. Remove the observability work entirely; revert to pre-PR-#320 state (no per-batch visibility) | Smallest behavioural change | Loses the operator-visibility win that motivated PR #320's §37 work in the first place | Rejected — discards the motivation |
| C. Spark Connect plugin / custom protocol extension | Maintains driver-side aggregation pattern | Requires JVM-side work + a custom Connect extension; massive scope for a logging concern | Rejected — disproportionate complexity |
| D. `DataFrame.observe()` API | Spark-Connect-compatible | Only emits metrics POST-action, not mid-action; doesn't solve the "is it stuck or progressing" question that drove the original design | Rejected — wrong semantics |
| E. Periodic in-UDF logging with operator-side log tailing (chosen) | Spark-Connect-compatible; per-batch context (match_id + batch_id, not just a count); zero driver-side lifecycle complexity; net-negative LOC vs the old design | Operator-side aggregation (grep/log search) instead of driver-side; per-batch granularity tied to per-batch duration (no sub-batch heartbeat) | — |

## Consequences

### Positive

- AC-1 production runs on Databricks serverless without `PySparkAttributeError`.
- Operator visibility per-batch: each log line names the match_id + batch_id + elapsed time, so the operator can see exactly which batch is currently in flight (richer than the old design which just gave a count).
- Net-negative LOC: ~70 lines removed (`_BatchHeartbeat` class + 6 tests + accumulator wiring + wrapper) vs ~10 lines added (one log line + `_batch_start` capture + 1 import). Smaller code surface.
- No driver-side thread to manage: no lifecycle bugs, no daemon/join concerns, no thread leaks on action raise.
- The fingerprint + summary helpers (`AC1_FINGERPRINT` at iteration start, `AC1_SUMMARY` at iteration end) remain — they're driver-side stdlib-only and work fine.

### Negative

- Operator can no longer aggregate progress on the driver programmatically. To answer "how many batches completed so far?" they grep the driver log stream rather than read an accumulator value. Acceptable: the Databricks UI already surfaces driver logs for in-flight runs.
- Per-batch log cadence depends on per-batch duration. Typical IDSSE/GS batches are 10–30s, so the operator sees a line every ~20s — same effective cadence as the old 30s heartbeat. Heavy batches (dead-ball windows with `_fill_possession_from_set_piece_actions` filling possession for restart actions, or set-piece restart actions specifically) can run 60–90s, meaning longer silent stretches within those batches. If sub-batch visibility ever becomes the binding need, the right fix is to add log emission inside `enrich.py`'s 20-step chain (e.g. before `add_das`, the slowest step), not to resurrect a driver-side aggregator.
- Removes 6 unit tests for `_BatchHeartbeat` lifecycle. The new code path has no Python state to test (one log call); test coverage on log content lives in production via `AC1_BATCH` log assertions, not unit tests.

### Neutral

- Pattern is identical to what the daily job's ingestion tasks already do — each `applyInPandas` UDF logs progress from inside the closure. PR #320's design was the outlier; this ADR restores consistency with the rest of the codebase.
- The `_iteration_fingerprint` + `_iteration_summary` helpers remain ADR-029-relevant (they pair with the silly-kicks 4.0 ET-direction sentinel by recording per-iteration context).

## CLAUDE.md Amendment

None required. The new pattern is the implicit norm; the ADR exists to document the rationale for removing the driver-aggregated experiment.

## Related

- **Commits:** TBD (single hot-fix commit on `fix/ac1-serverless-batch-heartbeat` branch)
- **Issues / PRs:** introduced in PR #320 (`08c09b8` squash → `899e8be`); hot-fix PR TBD
- **ADRs:** none extended or superseded — independent decision about post-PR-#320 cleanup
- **External references:** Databricks Spark Connect limitations page — https://docs.databricks.com/release-notes/serverless.html#limitations

## Notes

Failed production run that surfaced the bug: `https://dbc-48322be9-16be.cloud.databricks.com/?o=7474660814094441#job/302697362345215/run/887895768424884`. Stack trace shows `pyspark/sql/connect/session.py:881` raising `PySparkAttributeError` on `spark.sparkContext` — confirming the underlying Spark Connect session implementation forbids the attribute by design, not by configuration.

The PR #320 local validation gate (`AC1_E2E=1 pytest src/tests/action_context/`) ran 39 tests under silly-kicks 4.0.0 + golden e2e and passed. None of those tests touch `ingestion/action_context.py:_process_tracking_match`. The gap is captured in [[feedback_test_production_driver_entry_point]] as a memory rule to prevent the next regression of this class.
