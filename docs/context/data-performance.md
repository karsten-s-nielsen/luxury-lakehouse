# Context — Database & Compute Performance

On-demand detail for the Database Performance + benchmark rules in `AGENTS.md`. Governing ADRs: [ADR-037](../superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md), [ADR-038](../superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md), [ADR-043](../superpowers/adrs/ADR-043-strand-safe-synced-rederive.md), [ADR-045](../superpowers/adrs/ADR-045-ac1-single-pass-write-and-aqe-proof-dispatch.md), [ADR-082](../superpowers/adrs/ADR-082-tracking-marts-drain-fanout.md). Full detail in `docs/engineering/databricks-serverless.md`.

## Benchmarks

- **Performance benchmarks**: Critical-path functions must have `pytest-benchmark` tests. Includes: batched pitch control, off-ball xT frame computation, DEFCON credit assignment, line-breaking detection, OBSO surface computation, position jitter augmentation, team shape computation, team shape frame (both teams), shape graph construction, shape graph position inference, Numba-accelerated pitch control, ScoutGPT/Football2Vec/360 throughput and forward pass. Regressions caught in CI.
- **Benchmark with production-scale data**: A benchmark that passes on 100 rows but OOMs on 3M rows is a false green. For pipeline code touching tracking data, include at least one benchmark at expected production volume.

## Lakebase (PostgreSQL) — Synced Tables

- **Index every filtered column on fact tables >100K rows**: Any column used in a `WHERE` clause on a fact table must have an index. Use composite indexes matching the most common multi-column filter patterns (leftmost = highest selectivity).
- **No `ON ONLY` indexes**: Lakebase synced tables are internally partitioned (`__db_system.partition_*`). Indexes MUST be created WITHOUT the `ONLY` keyword to cascade to child partitions. Parent-only indexes are invisible to the query planner.
- **Index recreation after synced table rebuild**: Custom PG indexes are dropped when a synced table is recreated. The daily `.github/workflows/lakebase-grants.yml` GitHub Action reapplies them automatically. For immediate manual post-recreation repair, use `uv run python scripts/maintain_synced_tables.py --skip-refresh`. For full migration (all 41 tables), use `uv run python scripts/migrate_synced_tables.py` which runs the maintenance pipeline in Phase 4.
- **Avoid `SELECT DISTINCT` on large tables**: Use recursive CTE "loose index scan" pattern instead. `SELECT DISTINCT` forces a full sequential scan; the recursive CTE performs O(k × log n) index lookups for k distinct values.
- **Dimension tables don't need custom indexes**: Tables under ~50K rows with PK lookups perform well with sequential scans. Only index fact tables.
- **Verify with EXPLAIN ANALYZE**: After creating indexes, confirm Index Scan (not Seq Scan) on all fact tables via `scripts/create_indexes.py --verify`.
- **Re-deriving a TRIGGERED-synced mart** ([ADR-043](../superpowers/adrs/ADR-043-strand-safe-synced-rederive.md)): never `dbt --full-refresh` it directly (strands the synced table; the `on-run-start` tripwire aborts any `--full-refresh` selecting a TRIGGERED mart). Use `uv run --extra sdk python scripts/rederive_synced_marts.py --select <sel> …`. The TRIGGERED set lives in BOTH `SYNCED_TABLES` (`src/ingestion/refresh_synced_tables.py`) and the `triggered_synced_marts` var in `dbt_project.yml` — parity enforced by `src/tests/test_strand_safe_rederive.py`.

## Databricks (PySpark / Delta Lake)

- **Avoid double `df.count()` before writes**: Do not call `df.count()` for validation if `write_delta_table()` will call it again. **Since [ADR-045](../superpowers/adrs/ADR-045-ac1-single-pass-write-and-aqe-proof-dispatch.md)**, `write_delta_table` without a caller `row_count` counts the materialized Delta slice POST-write when identifiable (replaceWhere / full overwrite) — only bare-append without `row_count` still pre-counts the source.
- **Always pass `row_count`**: When `validate_dataframe()` returns a row count, pass it to `write_delta_table(row_count=row_count)` and `merge_delta_table(row_count=row_count)` to avoid redundant `df.count()` DAG recomputation.
- **Prefer `replaceWhere` over bare `mode="append"`**: Append without partition guards risks duplicates on retry. Use `replaceWhere` keyed on the logical partition for idempotent writes.
- **`write_delta_table` retries the concurrent-commit conflict class** ([ADR-038](../superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md)): Delta `Concurrent*Exception` + the serverless S3-400-at-`_delta_log`-commit signature, with fully-jittered backoff (`_COMMIT_MAX_ATTEMPTS=10`). Multiple workers writing one Delta table concurrently are safe (disjoint `replaceWhere` partitions — idempotent under retry).
- **`.toPandas()` calls must be bounded**: enforced mechanically by `src/tests/test_topandas_boundedness.py` (CI gate, AST + allowlist `src/tests/_topandas_exemptions.yml`). Adding a `.toPandas()` call without a DataFrame-API bound requires an allowlist entry articulating WHY driver memory can hold the result.
- **Prefer Spark executors over driver-bound processing**: `applyInPandas` / `mapInPandas` → UC Volume Parquet → last-resort per-partition `.toPandas()`.
- **Liquid clustering + auto-compaction + Predictive Optimization + deletion vectors** are the mart-table defaults (`liquid_clustered_by`, `autoOptimize` tblproperties, catalog-level PO, DBR 14.1+).

## Serverless constraints (summary)

16 GB driver (fixed), 1 GB UDF group cap, no broadcast variables, no `df.cache()`/`persist()`, no internet in UDFs, no local filesystem writes from Spark (`file://` forbidden), lazy closure capture — use frozen dataclasses for `applyInPandas` config. Full list in `docs/engineering/databricks-serverless.md`.

## Batch compute (summary)

Factor loop-invariant computation out and broadcast; verify HF Jobs dataset size against container RAM before loading (`l40sx1`: 62 GB, `cpu-basic`: 16 GB); pre-build `dict(iter(df.groupby(key)))` indexes at both match and frame level.

## Performance budgets

- **Pipeline task timeout**: ingest tasks ≤15 min, compute tasks ≤2 hr. **Documented exception** ([ADR-037](../superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md)): `compute_action_context` is a worker-drain task (`timeout_seconds = 28800`), one-time cold start ~5.5 h; the **2700 s** budget there is a **per-game watchdog inside the worker**, now effectively per-half. [ADR-082](../superpowers/adrs/ADR-082-tracking-marts-drain-fanout.md) adds a SECOND worker-drain, `compute_tracking_marts` (same 28800 s / 2700 s watchdog), consolidating off_ball_runs / defensive_credit / gkdv.
- **App page load**: ≤3 seconds (first load), ≤500ms (cached interaction).
- **UDF group memory**: ≤800 MB peak (1 GB limit minus overhead).
- **Batched pitch control**: ≤5ms per frame for 22 targets. **Line-breaking detection**: ≤2ms per pass. **Team shape**: ≤1ms per frame (≤2ms both teams).
- **Before modifying any benchmarked or hot-path function, invoke `mad-scientist-skills:measure-before-optimize`** (peer skill to `mad-scientist-skills:optimization-audit`: pre-change vs retrospective). Do not optimise benchmarked code on vibes.
