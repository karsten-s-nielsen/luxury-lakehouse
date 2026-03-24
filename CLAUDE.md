# Luxury Lakehouse — Engineering Standards

These standards apply to ALL code in this repository. They are non-negotiable.

## Architecture Principles

- **SOLID**: Single responsibility per module/function. Depend on abstractions.
- **Clean Code**: Meaningful names, small functions, no dead code.
- **Separation of Concerns**: Ingestion, transformation (dbt), workflow orchestration (`src/workflows/`), and presentation (Taipy) are fully isolated layers. `src/workflows/` has zero Spark/Streamlit/Taipy imports — only stdlib + PyYAML + Pydantic.
- **Idempotent Operations**: Every ingestion task can be re-run safely. Use partition-level overwrites, not full table drops.
- **Structured Logging**: JSON-line logs to stdout. No print statements. Include source name, row counts, and timing.
- **Streamlit retained for reference**: `src/streamlit_app/` and `hf_streamlit_app_deprecated/` are preserved during the Taipy transition period (~1 week). No changes needed to this code.

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

- **Performance benchmarks**: Critical-path functions must have `pytest-benchmark` tests. Includes: batched pitch control, off-ball xT frame computation, DEFCON credit assignment, line-breaking detection, OBSO surface computation, position jitter augmentation. Regressions caught in CI.
- **No DataFrame boolean mask filtering inside loops**: Never use `df[df["col"] == val]` inside a `for` loop over tracking or event data. This is O(n×m) — a hidden nested loop that causes pipeline timeouts on production-scale data (3M+ rows). Pre-build indexed lookups: `dict(iter(df.groupby("key")))`, `df.set_index("key")`, or use a merge/join. On tracking-scale data, this is always Critical severity, never Minor.
- **Benchmark with production-scale data**: A benchmark that passes on 100 rows but OOMs on 3M rows is a false green. For pipeline code touching tracking data, include at least one benchmark at expected production volume.

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
- **Prefer Spark executors over driver-bound processing**: Always exhaust executor-side options before resorting to `.toPandas()` chunk-and-release on the driver. Decision hierarchy: (1) `applyInPandas` / `mapInPandas` for per-group compute, (2) `df.write.parquet()` to UC Volume for file exports (Spark writes to cloud storage, driver reads for upload), (3) per-partition `.toPandas()` with `del` + `gc.collect()` only as last resort when Spark cannot write to the target. On serverless, Spark can write to UC Volumes and Delta tables but NOT to local filesystem (`file://` forbidden, DBFS disabled).
- **Prefer `applyInPandas` over driver-bound loops**: Never use `for match_id in ...: spark.sql(...).toPandas()` loops for compute pipelines. Use `spark.groupBy(key).applyInPandas(func, schema)` to distribute computation across executors. The driver should only handle metadata (match IDs, config), never raw data.
- **Group sizing for `applyInPandas`**: Each group materializes as one pandas DataFrame on an executor. Keep groups under 800 MB (1 GB UDF memory limit minus overhead). Use synthetic partition keys (e.g., `frame_batch_id = (frame / batch_size).cast("int")`) to subdivide large natural groups.
- **Multi-pass `applyInPandas`**: When a computation has independent phases (e.g., credit assignment is per-period but value estimation needs the full match), chain two `applyInPandas` calls with different group keys rather than pulling everything to the driver.
- **Model loading on executors**: Use module-level `_model_cache: dict[str, object]` for lazy-loading ML models from UC Volume inside UDFs. Spark reuses Python workers across groups, so the model loads once per executor, not once per group.
- **Use CTEs for repeated window functions in dbt**: Extract `LAG()` / `LEAD()` into a CTE rather than repeating the window expression in derived columns. Spark may not deduplicate window evaluations across column expressions.
- **Liquid clustering over Z-ordering**: All mart tables use `liquid_clustered_by` (not `cluster_by`) for incremental, automatic data layout. Liquid clustering is preferred for all new tables.
- **Auto-compaction and optimizeWrite**: All mart tables have `delta.autoOptimize.autoCompact` and `delta.autoOptimize.optimizeWrite` enabled via dbt `tblproperties`. These are NOT on by default for serverless.
- **Predictive Optimization**: Enabled at catalog level (`enable_predictive_optimization = "ENABLE"` in Terraform). Auto-VACUUMs and auto-OPTIMIZEs Unity Catalog managed tables.
- **Deletion vectors**: Enabled by default on Serverless DBR 14.1+ for new tables. No action needed.

### Enterprise Integration Patterns (EIP)

The platform's architecture maps to classic EIP patterns (Hohpe & Woolf 2003). Consider these patterns when designing new pipelines:

| EIP Pattern | Implementation | Example |
|-------------|---------------|---------|
| **Splitter** | `applyInPandas` grouped by natural key | Compute pipelines split by `match_id` |
| **Aggregator** | `replaceWhere` idempotent Delta writes | Per-partition overwrite accumulates into full table |
| **Content-Based Router** | Skip guards + feature toggles | `existing` set check routes to skip/process |
| **Claim Check** | `match_id` references, not full DataFrames | Driver passes IDs; executors load data from Delta |
| **Pipes and Filters** | Medallion architecture | Bronze → Silver → Gold via dbt/workflows |

### Databricks Serverless Constraints

- **Driver memory**: 16 GB fixed. Cannot configure instance types.
- **UDF executor memory**: 1 GB hard cap per `applyInPandas` / `mapInPandas` group.
- **No broadcast variables**: Use frozen dataclass closures for small config (<1 KB). Load larger artifacts (ML models, lookup tables) from UC Volume inside the UDF body.
- **No `df.cache()` / `df.persist()`**: Write intermediate results to Delta temp tables if re-reads are needed.
- **No internet in UDFs**: All data must come from Delta tables or UC Volumes. No HTTP calls inside UDF function bodies.
- **Lazy closure capture**: Variables are captured at action time, not definition time. Use frozen dataclasses for all config passed to `applyInPandas`. Never mutate variables between function definition and the `.applyInPandas()` call.
- **No local filesystem writes from Spark**: DBFS root is disabled and `file://` scheme is forbidden. Spark can write to UC Volumes (`/Volumes/...`) and Delta tables only. For file exports (e.g., Parquet for HF Hub upload), write to a UC Volume staging path, then read from the Volume path on the driver for upload.

### Batch Compute Optimization

- **Factor out loop-invariant computation**: When computing `f(variant_i) × constant` for N variants in a loop, compute the constant factor ONCE outside the loop and broadcast. Never call the function N times with the same constant inputs. Example: OBSO surfaces where transition × EPV grids are constant across all player-removal variants — compute the combined multiplier once, then `all_obso = all_ppcf * multiplier[None, :, :]` in a single NumPy broadcast. This is Critical severity because it converts O(N × grid_size) sequential Python calls into O(1) vectorized operations.
- **Memory budget for HF Jobs**: Before loading a dataset on HF Jobs, verify its size against the container's RAM (`a10g-small`: 15 GB, `a10g-large`: 46 GB, `cpu-basic`: 16 GB). Use column-selective loading (`pd.read_parquet(path, columns=[...])`) and per-partition streaming when full materialization would exceed 50% of available RAM. A 14 GB dataset on a 15 GB container WILL OOM after accounting for Python runtime + JAX CUDA memory.
- **Pre-build indexes before batch processing**: For tracking-scale batch compute (>100K frames), always `dict(iter(df.groupby(key)))` at both the match level AND the frame level before entering processing loops. Two-level indexing (`match_groups[match_id]` → `frame_groups[frame]`) eliminates all O(n) boolean mask filtering.

### Performance Budgets

- **Pipeline task timeout**: ingest tasks ≤15 min, compute tasks ≤2 hr
- **App page load**: ≤3 seconds (first load), ≤500ms (cached interaction)
- **UDF group memory**: ≤800 MB peak (1 GB limit minus overhead)
- **Batched pitch control**: ≤5ms per frame for 22 targets (benchmark baseline)
- **Line-breaking detection**: ≤2ms per pass (benchmark baseline)

## App Performance

- **`@st.cache_data` functions must be at module level**: Never define a `@st.cache_data`-decorated function inside another function — the decorator is re-applied on every call, creating a new cache key each time. This silently defeats caching.
- **Bound all data queries**: Every Streamlit SQL query returning user-facing data must have a `LIMIT` clause. Use `LIMIT 500` for ranking/leaderboard queries, `LIMIT 2000` for timeline queries.
- **Use recursive CTE for distinct values on fact tables**: `SELECT DISTINCT` forces full sequential scans. Use the recursive CTE loose index scan pattern instead (see Lakebase section above).

## UI Architecture

The Taipy dashboard uses a template-driven architecture where pages are declarative data, not imperative layout code. All page rendering flows through `page_template.py` → `build_page(cfg: PageConfig)`. This section defines the rules for maintaining and extending it.

### Adding a New Page

A new page requires exactly 3 files and 2 edits (4 files for dashboard pages):

1. **`hf_taipy_app/src/state/<page_name>.py`** — State variables, callbacks, SQL queries, chart rendering. Must follow prefix naming (`<prefix>_variable`) to avoid Taipy namespace collisions.
2. **`hf_taipy_app/src/pages/<page_name>.py`** — A `page_config: PageConfig` and `page_md: str` (from `build_page(page_config)`). No hand-crafted Taipy Markdown — the page file is pure configuration.
3. **`hf_taipy_app/src/main.py`** — Import the page's `page_config` and `page_md`, add a `PageEntry` to `PAGE_REGISTRY`.
4. **`hf_taipy_app/src/template.py`** — Add page-specific glossary terms to `PAGE_TERMS`.

#### Dashboard Page Variant

For operations/dashboard pages (stats cards + full-width content instead of 3fr/1fr layout):

- Use `stats: list[StatCard]` in `PageConfig` instead of `metrics` — this triggers the dashboard layout (`_build_dashboard_page`), which wraps content in a viewport-contained scroll wrapper (`ll-dashboard-scroll`).
- Call `register_page_refresher("Page-Name", refresh_fn, is_dashboard=True)` — the `is_dashboard` flag ensures the site-wide footer is hidden (dashboard pages render the footer inside the scroll wrapper).
- `ContentRow` wraps content blocks. `StatCard` defines the stat cards in the top bar.

### Template Rules

- **All pages must use `build_page()`**: Zero hand-crafted layouts. A page is a `PageConfig` (title, icon, description, metrics, sidebar widgets, content blocks, citations), not a string of Taipy Markdown.
- **`Metric` requires `help_text`**: If the metric name is not universally understood, `help_text` is mandatory — the `PageConfig` dataclass enforces this. "What does this mean?" and "Is this good or bad?" must be answerable from the tooltip alone.
- **`SidebarWidget` requires `help`**: Every filter widget must have a `help` tooltip explaining what it controls. Help icons are positioned absolute-right of the widget via CSS (`.md-para:has(> .ll-help)`), keeping all widget widths identical regardless of help presence.
- **`Citation` for every methodology**: Any page implementing a published algorithm must include a `Citation(text, url)` in its `PageConfig`. No uncited methodologies.
- **`StatCard` for dashboard stat cards**: Dashboard pages use `stats: list[StatCard]` in `PageConfig`. Each card has `label`, `var`, optional `detail_var`, `help_text`, and `detail_html`. The presence of `stats` activates the dashboard layout branch. Set `detail_html=True` to render `detail_var` as raw HTML via a content provider iframe (supports inline `<span style="">` coloring); default `False` renders as plain text.
- **`ContentBlock` for all content**: Images use `ContentBlock("image", var)`, tables use `ContentBlock("table", var)`, Plotly charts use `ContentBlock("chart", var)`. Tables accept `table_cell_class_name={column: callback_name}` for per-cell CSS styling via Taipy's `cell_class_name` attribute (the callback returns a CSS class string). Never construct raw `<|{var}|chart|>` markup in page files.
- **Layout changes go through the template**: If a visual change requires editing more than one page file, it belongs in `page_template.py`. Individual page files contain only page-specific data.
- **`_FOOTER_CONTENT` for footer text**: The footer text ("Interactive Demo · Published Datasets") is a shared constant in `page_template.py`. Dashboard pages render it inside the scroll wrapper; other pages render it as the site-wide footer. Do not hardcode footer text in page files.
- **`is_dashboard=True` on `register_page_refresher`**: Required for dashboard pages. Controls `show_site_footer` state variable — omitting it causes footer duplication.
- **`ll-dashboard-scroll` for dashboard viewport**: Dashboard content is wrapped in a viewport-contained scroll area (`overflow: auto`, `max-height: calc(100vh - 245px)`). Both horizontal and vertical scrollbars live inside this container. The horizontal scrollbar stays at the viewport bottom.
- **State module isolation**: Each page's state module manages its own variables and callbacks. Shared state (competition/team/match filters) lives in `state/shared.py`. No cross-page state imports except from `shared`.
- **Glossary coverage**: Every domain-specific term used in metric names, chart labels, or descriptions must have an entry in `GLOSSARY` (in `template.py`) and be listed in the page's `PAGE_TERMS` entry.

### Why Template-First

Building pages as imperative layout code leads to inconsistency debt that compounds per-page. With 12+ pages, hand-crafting each one guarantees: missing tooltips on some pages, different metric formats, inconsistent empty-state handling, and layout drift. The template makes these structurally impossible — required fields are constructor parameters, not afterthoughts. A CHI audit against a template architecture produces template-level fixes (one change, all pages); without it, the same audit produces N per-page fixes.

## Streamlit UX Standards

These rules prevent cognitive interface debt from accumulating. Derived from CHI-AUDIT-180 and CHI-AUDIT-190 (cognitive-interface-audit v1.8.0+, 15 frameworks). Every Streamlit, Gradio, or Taipy code change must satisfy all of these. Taipy equivalents: `Metric(help_text=)` for tooltips, `warning_var=` (amber `ll-warning-box`) vs `empty_message` (blue `ll-info-box`) for empty state distinction, `scope_vars=` for data context, `SidebarWidget(help=)` for widget tooltips, `GLOSSARY`/`PAGE_TERMS` in `template.py` for per-page glossary filtering.

- **Every `st.metric` must have `help=`**: If the metric name is not universally understood (i.e., anything beyond "Goals", "Passes", "Score"), add a `help=` tooltip explaining what it means and what "good" looks like. Examples: xG, VAEP, PPDA, Brier Score, cosine distance, xT, DEFCON credits.
- **Every `show_spinner=False` must be justified**: Default to descriptive spinner text (e.g., `show_spinner="Loading rankings..."`). Only suppress spinners on queries that complete in <100ms (e.g., small dimension lookups that are always cached). When in doubt, show the spinner.
- **Never silently substitute data**: If a fallback, default, or NaN-fill changes what the user sees, surface it. Use `st.info`, `st.caption`, or a visual indicator. The user must be able to tell what data source produced what they're looking at.
- **Patterns applied to some pages must be applied to all**: When adding a cross-cutting pattern (captions, tooltips, help text, layout changes), apply it to ALL pages in the same commit. If a page is excluded, add a code comment explaining why.
- **Model selectors need comparison affordance**: When adding a selector that switches between models/algorithms/views, consider the comparison workflow: add `delta=` on metrics, a side-by-side layout, or at minimum persist the previous selection's values visually. Users should not need to remember numbers across radio button clicks.
- **Navigation labels must be goal-oriented**: Page titles in `st.Page(title=...)` should describe the user's goal, not the implementation. "Player Comparison" not "Player Radar". "Defensive Impact" not "Def. Pressure".
- **Distinguish "please select" from "no data"**: Use `st.info` for guidance prompts ("Select a competition to begin") and `st.warning` for empty results ("No data found for the selected filters"). Never use the same widget type for both — users cannot distinguish "take action" from "nothing exists."
- **Raw IDs must never reach the user**: Never display `player_id`, `match_id`, or `team_id` in selectboxes, tables, or chart labels. Always join to dimension tables for human-readable names. Use `format_func` on selectboxes.
- **Multi-surface UX parity**: When a Streamlit page has glossary terms, help tooltips, scale references, or academic citations, the corresponding HF Space tab must have equivalents (e.g., `gr.Accordion("Glossary")` with per-tab filtered terms, axis labels with range/direction, `gr.Markdown` citations). A feature on one surface without its UX scaffolding on the other is incomplete.
- **Computed metrics must show scale and direction**: Any displayed score on a 0–1 or non-obvious scale (PAUSA, OBSO, cosine distance, xT, VAEP) must include the range and direction in at least one of: axis label, chart title, tooltip, or adjacent caption. "0.347" alone is never acceptable — "0.347 (0–1, higher = better)" is. This applies to both Streamlit (`help=`) and Gradio (axis labels, plot titles).
- **HF artifact link completeness**: When publishing a new HF dataset or model, update ALL locations that reference the artifact list: HF Space header, HF Space footer, `docs/huggingface/org-card.md`, and `README.md`. A checklist in the PR description prevents drift.

## Project Conventions

- **Python 3.10 (locked)**: Pinned to `>=3.10,<3.11` in `pyproject.toml` and `.python-version`. Databricks serverless only supports Python 3.10 — locking locally ensures tests catch version-specific behavior (e.g., pandas API differences) before they reach production. Run `uv sync` to get a 3.10 venv automatically.
- **Line length**: 120 characters maximum.
- **Imports**: stdlib → third-party → first-party, enforced by isort.
- **Entry points**: Each ingestion module exposes a `main()` function registered in `pyproject.toml`.
- **`@workflow` decorator**: All compute pipelines in `src/ingestion/` are decorated with `@workflow("wf-xxx", phase="yyy")` from `src/workflows/`. The decorator wraps calls through the lifecycle runner, dispatching `on_start`/`on_complete`/`on_skip`/`on_error` hooks. `CostEstimateHook` (registered in each pipeline's `main()`) writes cost data to `{catalog}.observability.workflow_cost_live` via Delta MERGE. Adding `*, ctx=None` to `run_pipeline()` is optional (injected by the runner when present).
- **Observability schema**: `soccer_analytics.observability` hosts platform operational metadata (cost tracking, future run history, SLIs, alerts). Separate from `bronze`/`gold` data schemas. `CostEstimateHook` uses `cost_schema="observability"` (default) to target this schema regardless of the pipeline's `--schema` argument.
- **Workflow cards**: Each AI/ML workflow has a YAML+Markdown manifest in `workflow-cards/wf-*.yaml`. Cards describe inputs, outputs, dependencies, execution config, academic provenance, cost estimates, and monitoring thresholds. Validated in CI via `validate_workflow_cards` entry point.
- **Delta tables**: All bronze writes include `_ingested_at` audit column with UTC timestamp.
- **Partition overwrite**: Use `replaceWhere` for incremental loads, not full table overwrites.
- **Incremental skip guards**: Every compute pipeline must check for already-processed results before expensive work. Pattern: `existing = {str(row["match_id"]) for row in spark.table(results).select("match_id").distinct().collect()}`. The `str()` normalization is critical — Spark returns `int`, Delta stores `string`.
- **dbt model contracts**: All gold-layer models have `contract: {enforced: true}` with explicit `data_type` on every column. When adding or changing columns in a mart model, update the `_marts__models.yml` contract to match. `on_schema_change: fail` ensures mismatches are caught at build time.
- **dbt slim CI**: PR builds use `state:modified+` to only build/test changed models. The `--empty` flag validates schema contracts with zero-cost DDL (no data movement).
- **Pre-compile regex at module level**: Never use `re.compile()`, `re.sub()`, or `re.match()` with raw pattern strings inside function bodies or loops. Compile patterns as module-level constants.
- **HTTP caching via `requests-cache`**: The shared `fetch_url()` session uses `requests_cache.CachedSession` with SQLite backend. Static open-data sources (StatsBomb GitHub) are cached indefinitely; other sources use a 24-hour TTL. Set `LUXURY_LAKEHOUSE_HTTP_CACHE=0` to disable. Bronze Delta tables remain the durable cache; HTTP cache avoids redundant network round-trips during development and retry.
- **HF Jobs wheel convergence**: HF Jobs PEP 723 scripts import domain logic from the `luxury-lakehouse` wheel hosted at `luxury-lakehouse/build-artifacts` on HF Hub. CI uploads the wheel on main merges. Scripts keep HF Hub I/O, MLflow logging, and script-specific helpers inlined — only shared analytics functions are imported from the wheel. Each script has the wheel URL as its first PEP 723 dependency.
- **HuggingFace Hub**: Org is `luxury-lakehouse`. Model artifacts cached in UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/`. Set `HF_HOME` env var for local cache location. Use `huggingface_hub` for model publish/download (no torch dependency). See `docs/huggingface-setup.md`. Model card and org card source of truth: `docs/huggingface/model-card.md` and `docs/huggingface/org-card.md` (pushed to HF Hub 2026-03-09).
