# ADR-045: AC-1 single-pass Delta write + AQE-proof UDF dispatch + per-batch overhead gates

| Field | Value |
|---|---|
| **Date** | 2026-06-09 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

A measured ~2× slowdown investigation (per-half wall ~20 min → ~45 min) ended in a full
optimization audit of the AC-1 pipeline. Every software-stack suspect was exonerated by
controlled A/B (silly-kicks 4.20.1→4.21.1 = +8% local; numba 0.64→0.65.1 = +7%; the
June-5-era chain at 130s vs today's at 128s on the same fixture). The real causes were
structural, measured from production telemetry (per-batch rendezvous markers carry epoch
timestamps — reconstructing the exact concurrency profile of completed halves):

1. **Double DAG execution.** `write_delta_table` ran `df.count()` on the applyInPandas
   result before `saveAsTable()` — materializing the ENTIRE enrichment chain twice per
   half. Measured: Metrica half 2538s wall = 1230s marker span (one pass) × 2.06;
   second half 2621s = 1263s × 2.08. Every tracking half on every provider paid double
   since AC-1 shipped.
2. **AQE bytes-based shuffle coalescing serialized the UDF stage.** `groupBy().applyInPandas`
   shuffles are coalesced by AQE to ~64 MB advisory partitions — by BYTES, blind to
   Python-UDF cost. A Metrica half (~286 groups, ~60 MB) coalesced to ONE task:
   **measured concurrency 1.00 (strictly serial)**, while GS's bigger rows got 3–4
   partitions (concurrency 3.29). Per-provider parallelism that tracks data bytes, not
   compute, clinched the mechanism (capacity contention was refuted: runs with MORE
   concurrent workers showed MORE parallelism).
3. **Per-batch fixed overhead at ~300 batches/half:** ghost-GK model re-resolved + re-loaded
   from disk per batch (`model="default"` string; 10.3s/127s = 8% of local wall, plus one
   sklearn warning per batch), `executor_env_fingerprint` (docstring: "one-shot") executed
   per batch with no guard (env introspection + socket probe + UC-Volume FUSE write), and
   two unconditional `gc.collect()` calls per batch (9.5% of local wall) protecting a 1 GB
   cap that a ~1–3 MB batch cannot threaten.

## Decision

Five value-neutral changes (goldens MUST stay byte-identical):

1. **Single-pass write** (`ingestion/utils.py::write_delta_table`): when `row_count` is
   not supplied, count the **materialized Delta slice post-write** (the `replaceWhere`
   predicate delimits exactly the written rows — ADR-038 guarantees concurrent writers
   touch disjoint slices; a full overwrite IS the table) instead of pre-counting the
   source DataFrame. Only the bare-append path without a caller `row_count` retains the
   pre-write count (appended rows are not identifiable post-write).
2. **AQE-proof dispatch** (`ingestion/action_context.py`): replace
   `groupBy(keys).applyInPandas(udf, schema)` with
   `repartition(_UDF_SHUFFLE_PARTITIONS=64, *keys).sortWithinPartitions(*keys)
   .mapInPandas(_make_streaming_group_mapper(udf, keys), schema)`. A repartition with an
   EXPLICIT N is exempt from AQE coalescing → deterministic stage parallelism. The
   streaming mapper re-derives the applyInPandas group semantics (groups contiguous via
   the sort; the tail group of each Arrow chunk is carried into the next chunk and
   flushed once complete) so each group reaches the unchanged `udf_fn` exactly once,
   whole.
3. **Ghost-GK process-local model cache** (`analytics/action_context/enrich.py`): both
   `add_ghost_gk` call sites pass a cached `GhostGkModel` instance (silly-kicks accepts
   `GhostGkModel | "default" | "full" | None`) — the databricks-serverless.md
   "Model loading on executors" convention, previously violated here.
4. **`executor_env_fingerprint` once-per-process latch** (`ingestion/exec_visibility.py`)
   — enforcing what the docstring always promised.
5. **gc gate** (`analytics/action_context/pipeline.py`): the per-batch `gc.collect()`
   runs only when the input group exceeds `_GC_COLLECT_MIN_ROWS=100_000` rows — keeping
   the 1 GB-cap protection where it can matter.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `df.observe()` for the in-flight count (single pass, no post-read) | one materialization, no second scan | `Observation` is single-action — breaks under ADR-038's `_commit_with_retry` re-invocation; Connect-version sensitivity | fragile under retry |
| B. `spark.sql.shuffle.partitions` (serverless-allowlisted) to widen the UDF stage | conf-only | sets only the INITIAL partition count — AQE coalescing still merges small-byte partitions afterward; the AQE-coalescing confs are not serverless-allowlisted | doesn't survive AQE |
| C. Raise `_FRAME_BATCH_SIZE` so groups are bigger | amortizes per-batch overhead | does NOT fix parallelism (total bytes unchanged → AQE still coalesces); changes golden values (savgol velocity windows at batch edges) | orthogonal; deferred as a follow-up requiring golden regen |
| D. `repartition(N, keys)` + sorted `mapInPandas` streaming groups (chosen) | AQE-exempt (explicit N), deterministic parallelism, value-neutral, no conf dependencies | a ~40-line group-reassembly adapter to maintain (unit-locked) | — |

## Consequences

### Positive

- The double execution is gone: per-half wall ≈ halves immediately on every tracking provider.
- UDF stage parallelism is deterministic (64 explicit partitions) instead of an AQE
  byte-heuristic accident — projected Metrica half 2538s → ~450–700s, GS 1713s → ~300–500s
  (to be verified post-deploy with the marker-derived concurrency instrument).
- Per-batch fixed overhead (model reload + fingerprint + gc) drops to once-per-process /
  gated.
- All five changes are value-neutral — goldens byte-identical — so no re-derive of any
  mart is needed.

### Negative

- `_make_streaming_group_mapper` re-implements applyInPandas group semantics — a
  correctness-critical adapter we now own (locked by `test_adr045_perf.py`, including the
  group-split-across-chunks case).
- Post-write count is a second (cheap, stats-backed) scan of the written slice — a few
  seconds, vs the minutes of the removed DAG re-execution. Bare-append callers without
  `row_count` keep the old double-execution shape (none are on hot paths).
- `_UDF_SHUFFLE_PARTITIONS=64` is a fixed choice; if serverless autoscale width changes
  materially it may need retuning.

### Neutral

- The driver heartbeat phase label `write_delta_applyInPandas` is retained for log
  continuity even though dispatch is now mapInPandas.
- Tier-C (bigger `_FRAME_BATCH_SIZE`, sized today against a stale "~200 MB" comment that
  is ~100× off — actual 250-frame group ≈ 1–3 MB) is deliberately NOT in this change:
  it shifts golden values (savgol boundaries) and belongs in its own measured PR.

## Related

- **ADRs:** ADR-037 (worker-drain fan-out), ADR-038 (commit retry — why post-write
  replaceWhere count is safe), ADR-035 (ghost-GK backend), ADR-031 (rendezvous markers —
  the instrument that measured concurrency), ADR-044 (executor env-drift guard),
  ADR-047 (closes the deferred Tier C: `_FRAME_BATCH_SIZE` 250 → 2500)
- **Code:** `ingestion/utils.py`, `ingestion/action_context.py`,
  `ingestion/exec_visibility.py`, `analytics/action_context/{enrich,pipeline}.py`
- **Tests:** `src/tests/action_context/test_adr045_perf.py`,
  `src/tests/test_ingestion_utils.py::TestWriteDeltaTable`

## Notes

Measurement provenance: local profiler fixture A/B (`scripts/profile_ac1_local.py`,
idsse/J03WMX p1, 128s steady baseline); prod rendezvous-marker concurrency reconstruction
(858–1047 marker files per half; PEAK/MEAN concurrency 1/0.90 Metrica vs 4/3.29 GS);
marker-span-vs-wall ratio 2.06–2.08 proving the double execution.
