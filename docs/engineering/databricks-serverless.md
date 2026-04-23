# Databricks Serverless — Performance and Architecture Reference

Full detail for the performance rules summarised in `CLAUDE.md` under `## Database Performance`. `CLAUDE.md` carries the short-form rules and budgets; the explanatory detail, decision hierarchies, and pattern rationale live here.

Related:
- `docs/performance-baselines.md` — concrete benchmark numbers for critical-path functions
- `docs/engineering/conventions.md` — operational conventions (dbt ownership, workflow cards, Lakebase ops)

## Enterprise Integration Patterns (EIP)

The platform's architecture maps to classic EIP patterns (Hohpe & Woolf 2003). Consider these patterns when designing new pipelines:

| EIP Pattern | Implementation | Example |
|-------------|---------------|---------|
| **Splitter** | `applyInPandas` grouped by natural key | Compute pipelines split by `match_id` |
| **Aggregator** | `replaceWhere` idempotent Delta writes | Per-partition overwrite accumulates into full table |
| **Content-Based Router** | Skip guards + feature toggles | `existing` set check routes to skip/process |
| **Claim Check** | `match_id` references, not full DataFrames | Driver passes IDs; executors load data from Delta |
| **Pipes and Filters** | Medallion architecture | Bronze → Silver → Gold via dbt/workflows |

## Databricks (PySpark / Delta Lake) — Extended

Short-form rules live in `CLAUDE.md`. Detailed mechanics below.

- **Prefer Spark executors over driver-bound processing**: Always exhaust executor-side options before resorting to `.toPandas()` chunk-and-release on the driver. Decision hierarchy: (1) `applyInPandas` / `mapInPandas` for per-group compute, (2) `df.write.parquet()` to UC Volume for file exports (Spark writes to cloud storage, driver reads for upload), (3) per-partition `.toPandas()` with `del` + `gc.collect()` only as last resort when Spark cannot write to the target. On serverless, Spark can write to UC Volumes and Delta tables but NOT to local filesystem (`file://` forbidden, DBFS disabled).
- **Prefer `applyInPandas` over driver-bound loops**: Never use `for match_id in ...: spark.sql(...).toPandas()` loops for compute pipelines. Use `spark.groupBy(key).applyInPandas(func, schema)` to distribute computation across executors. The driver should only handle metadata (match IDs, config), never raw data.
- **Group sizing for `applyInPandas`**: Each group materializes as one pandas DataFrame on an executor. Keep groups under 800 MB (1 GB UDF memory limit minus overhead). Use synthetic partition keys (e.g., `frame_batch_id = (frame / batch_size).cast("int")`) to subdivide large natural groups.
- **Multi-pass `applyInPandas`**: When a computation has independent phases (e.g., credit assignment is per-period but value estimation needs the full match), chain two `applyInPandas` calls with different group keys rather than pulling everything to the driver.
- **Model loading on executors**: Use module-level `_model_cache: dict[str, object]` for lazy-loading ML models from UC Volume inside UDFs. Spark reuses Python workers across groups, so the model loads once per executor, not once per group.
- **Use CTEs for repeated window functions in dbt**: Extract `LAG()` / `LEAD()` into a CTE rather than repeating the window expression in derived columns. Spark may not deduplicate window evaluations across column expressions.
- **Liquid clustering over Z-ordering**: Mart fact tables use `liquid_clustered_by` (not `cluster_by`) for incremental, automatic data layout (24 of 33 models). Embedding models are excluded (not query-filtered). Liquid clustering is required for all new fact tables.
- **Auto-compaction and optimizeWrite**: All mart tables have `delta.autoOptimize.autoCompact` and `delta.autoOptimize.optimizeWrite` enabled via dbt `tblproperties`. These are NOT on by default for serverless.
- **Predictive Optimization**: Enabled at catalog level (`enable_predictive_optimization = "ENABLE"` in Terraform). Auto-VACUUMs and auto-OPTIMIZEs Unity Catalog managed tables.
- **Deletion vectors**: Enabled by default on Serverless DBR 14.1+ for new tables. No action needed.

## Databricks Serverless Constraints

- **Driver memory**: 16 GB fixed. Cannot configure instance types.
- **UDF executor memory**: 1 GB hard cap per `applyInPandas` / `mapInPandas` group.
- **No broadcast variables**: Use frozen dataclass closures for small config (<1 KB). Load larger artifacts (ML models, lookup tables) from UC Volume inside the UDF body.
- **No `df.cache()` / `df.persist()`**: Write intermediate results to Delta temp tables if re-reads are needed.
- **No internet in UDFs**: All data must come from Delta tables or UC Volumes. No HTTP calls inside UDF function bodies.
- **Lazy closure capture**: Variables are captured at action time, not definition time. Use frozen dataclasses for all config passed to `applyInPandas`. Never mutate variables between function definition and the `.applyInPandas()` call.
- **No local filesystem writes from Spark**: DBFS root is disabled and `file://` scheme is forbidden. Spark can write to UC Volumes (`/Volumes/...`) and Delta tables only. For file exports (e.g., Parquet for HF Hub upload), write to a UC Volume staging path, then read from the Volume path on the driver for upload.

## Batch Compute Optimization

- **Factor out loop-invariant computation**: When computing `f(variant_i) × constant` for N variants in a loop, compute the constant factor ONCE outside the loop and broadcast. Never call the function N times with the same constant inputs. Example: OBSO surfaces where transition × EPV grids are constant across all player-removal variants — compute the combined multiplier once, then `all_obso = all_ppcf * multiplier[None, :, :]` in a single NumPy broadcast. This is Critical severity because it converts O(N × grid_size) sequential Python calls into O(1) vectorized operations.
- **Memory budget for HF Jobs**: Before loading a dataset on HF Jobs, verify its size against the container's RAM (`l40sx1`: 62 GB, `cpu-basic`: 16 GB). Use column-selective loading (`pd.read_parquet(path, columns=[...])`) and per-partition streaming when full materialization would exceed 50% of available RAM. Default GPU flavor is `l40sx1` (L40S 48 GB VRAM, best cost/candidate — benchmarked 2026-04-05).
- **Pre-build indexes before batch processing**: For tracking-scale batch compute (>100K frames), always `dict(iter(df.groupby(key)))` at both the match level AND the frame level before entering processing loops. Two-level indexing (`match_groups[match_id]` → `frame_groups[frame]`) eliminates all O(n) boolean mask filtering.
