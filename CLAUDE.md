# Luxury Lakehouse — Engineering Standards

These standards apply to ALL code in this repository. They are non-negotiable.

## Architecture Principles

- **SOLID**: Single responsibility per module/function. Depend on abstractions.
- **Clean Code**: Meaningful names, small functions, no dead code.
- **Separation of Concerns**: Ingestion, transformation (dbt), and presentation (Streamlit) are fully isolated layers.
- **Idempotent Operations**: Every ingestion task can be re-run safely. Use partition-level overwrites, not full table drops.
- **Structured Logging**: JSON-line logs to stdout. No print statements. Include source name, row counts, and timing.

## Security Hardening

- **No secrets in code**: All authentication via Databricks runtime or environment variables. Never commit credentials, tokens, or connection strings.
- **HTTPS only**: All HTTP requests must use `https://`. Reject `http://` at the function level.
- **SSL verification**: Explicit `verify=True` on all `requests` calls. Never disable certificate verification.
- **Input validation**: Regex-validate all user-supplied identifiers (catalog, schema names) to prevent SQL injection. Pattern: `^[a-zA-Z_][a-zA-Z0-9_]*$`
- **Timeouts**: Every HTTP call must have explicit `(connect, read)` timeouts. Default: `(10, 30)`.
- **Retry with backoff**: Exponential backoff on transient errors (429, 5xx). Max 3 retries.
- **No dangerous builtins**: No `eval()`, `exec()`, `pickle.loads()`, or `subprocess.call(shell=True)`.
- **Content validation**: Verify DataFrame schema and non-empty data before every Delta write.
- **Least privilege**: Scripts write only to the specified `{catalog}.{schema}.*` — never to arbitrary paths.

## Type Safety

- **Pyright basic mode**: All Python code must pass `pyright` in basic type checking mode.
- **Pydantic models**: Use where appropriate for configuration and data contracts.
- **Type annotations**: All public function signatures must have type annotations.

## Code Quality

All code must pass these checks with zero violations:

```bash
uv run ruff check src/        # Lint (E, W, F, I, N, UP, B, S, RUF)
uv run ruff format --check src/ # Format check (CI enforced)
uv run pyright src/            # Type check (basic mode)
uv run pytest src/tests/ -v    # Unit tests
```

- **Performance benchmarks**: Critical-path functions must have `pytest-benchmark` tests. Includes: batched pitch control, off-ball xT frame computation, DEFCON credit assignment, line-breaking detection. Regressions caught in CI.

### Ruff Rules Enforced

| Rule Set | Purpose |
|----------|---------|
| E, W     | pycodestyle errors and warnings |
| F        | pyflakes (unused imports, undefined names) |
| I        | isort (import ordering) |
| N        | PEP 8 naming conventions |
| UP       | pyupgrade (Python 3.10+ idioms) |
| B        | flake8-bugbear (common pitfalls) |
| S        | flake8-bandit (security) |
| RUF      | Ruff-specific rules |

## Database Performance

### Lakebase (PostgreSQL) — Synced Tables

- **Index every filtered column on fact tables >100K rows**: Any column used in a `WHERE` clause on a fact table must have an index. Use composite indexes matching the most common multi-column filter patterns (leftmost = highest selectivity).
- **No `ON ONLY` indexes**: Lakebase synced tables are internally partitioned (`__db_system.partition_*`). Indexes MUST be created WITHOUT the `ONLY` keyword to cascade to child partitions. Parent-only indexes are invisible to the query planner.
- **Index recreation after synced table rebuild**: Custom PG indexes are dropped when a synced table is recreated. Always re-run `scripts/create_indexes.py` alongside `scripts/lakebase_grants.sql` after any recreation.
- **Avoid `SELECT DISTINCT` on large tables**: Use recursive CTE "loose index scan" pattern instead. `SELECT DISTINCT` forces a full sequential scan; the recursive CTE performs O(k × log n) index lookups for k distinct values.
- **Dimension tables don't need custom indexes**: Tables under ~50K rows with PK lookups perform well with sequential scans. Only index fact tables.
- **Verify with EXPLAIN ANALYZE**: After creating indexes, confirm Index Scan (not Seq Scan) on all fact tables via `scripts/create_indexes.py --verify`.

### Databricks (PySpark / Delta Lake)

- **Avoid double `df.count()` before writes**: Do not call `df.count()` for validation if `write_delta_table()` will call it again. Each `count()` triggers full DAG recomputation.
- **Always pass `row_count`**: When `validate_dataframe()` returns a row count, pass it to `write_delta_table(row_count=row_count)` and `merge_delta_table(row_count=row_count)` to avoid redundant `df.count()` DAG recomputation.
- **Prefer `replaceWhere` over bare `mode="append"`**: Append without partition guards risks duplicates on retry. Use `replaceWhere` keyed on the logical partition (e.g., `match_id`, `competition_id`) for idempotent writes.
- **Avoid `.toPandas()` on unbounded tables**: Never pull an entire fact table to driver memory. Use Spark-native operations or filter to bounded subsets first. Budget: <5M rows for `.toPandas()`.
- **Prefer `applyInPandas` over driver-bound loops**: Never use `for match_id in ...: spark.sql(...).toPandas()` loops for compute pipelines. Use `spark.groupBy(key).applyInPandas(func, schema)` to distribute computation across executors. The driver should only handle metadata (match IDs, config), never raw data.
- **Group sizing for `applyInPandas`**: Each group materializes as one pandas DataFrame on an executor. Keep groups under 800 MB (1 GB UDF memory limit minus overhead). Use synthetic partition keys (e.g., `frame_batch_id = (frame / batch_size).cast("int")`) to subdivide large natural groups.
- **Multi-pass `applyInPandas`**: When a computation has independent phases (e.g., credit assignment is per-period but value estimation needs the full match), chain two `applyInPandas` calls with different group keys rather than pulling everything to the driver.
- **Model loading on executors**: Use module-level `_model_cache: dict[str, object]` for lazy-loading ML models from UC Volume inside UDFs. Spark reuses Python workers across groups, so the model loads once per executor, not once per group.
- **Use CTEs for repeated window functions in dbt**: Extract `LAG()` / `LEAD()` into a CTE rather than repeating the window expression in derived columns. Spark may not deduplicate window evaluations across column expressions.

### Databricks Serverless Constraints

- **Driver memory**: 16 GB fixed. Cannot configure instance types.
- **UDF executor memory**: 1 GB hard cap per `applyInPandas` / `mapInPandas` group.
- **No broadcast variables**: Use frozen dataclass closures for small config (<1 KB). Load larger artifacts (ML models, lookup tables) from UC Volume inside the UDF body.
- **No `df.cache()` / `df.persist()`**: Write intermediate results to Delta temp tables if re-reads are needed.
- **No internet in UDFs**: All data must come from Delta tables or UC Volumes. No HTTP calls inside UDF function bodies.
- **Lazy closure capture**: Variables are captured at action time, not definition time. Use frozen dataclasses for all config passed to `applyInPandas`. Never mutate variables between function definition and the `.applyInPandas()` call.

## Streamlit Performance

- **`@st.cache_data` functions must be at module level**: Never define a `@st.cache_data`-decorated function inside another function — the decorator is re-applied on every call, creating a new cache key each time. This silently defeats caching.
- **Bound all data queries**: Every Streamlit SQL query returning user-facing data must have a `LIMIT` clause. Use `LIMIT 500` for ranking/leaderboard queries, `LIMIT 2000` for timeline queries.
- **Use recursive CTE for distinct values on fact tables**: `SELECT DISTINCT` forces full sequential scans. Use the recursive CTE loose index scan pattern instead (see Lakebase section above).

## Project Conventions

- **Python 3.10 (locked)**: Pinned to `>=3.10,<3.11` in `pyproject.toml` and `.python-version`. Databricks serverless only supports Python 3.10 — locking locally ensures tests catch version-specific behavior (e.g., pandas API differences) before they reach production. Run `uv sync` to get a 3.10 venv automatically.
- **Line length**: 120 characters maximum.
- **Imports**: stdlib → third-party → first-party, enforced by isort.
- **Entry points**: Each ingestion module exposes a `main()` function registered in `pyproject.toml`.
- **Delta tables**: All bronze writes include `_ingested_at` audit column with UTC timestamp.
- **Partition overwrite**: Use `replaceWhere` for incremental loads, not full table overwrites.
- **Incremental skip guards**: Every compute pipeline must check for already-processed results before expensive work. Pattern: `existing = {str(row["match_id"]) for row in spark.table(results).select("match_id").distinct().collect()}`. The `str()` normalization is critical — Spark returns `int`, Delta stores `string`.
- **Pre-compile regex at module level**: Never use `re.compile()`, `re.sub()`, or `re.match()` with raw pattern strings inside function bodies or loops. Compile patterns as module-level constants.
- **Use `requests.Session` for repeated calls**: When making multiple HTTP requests to the same host, use a `requests.Session` for TCP connection reuse and keep-alive.
- **HuggingFace Hub**: Org is `luxury-lakehouse`. Model artifacts cached in UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/`. Set `HF_HOME` env var for local cache location. Use `huggingface_hub` for model publish/download (no torch dependency). See `docs/huggingface-setup.md`. Model card and org card source of truth: `docs/huggingface/model-card.md` and `docs/huggingface/org-card.md` (pushed to HF Hub 2026-03-09).
