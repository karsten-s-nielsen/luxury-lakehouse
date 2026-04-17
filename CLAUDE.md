# Luxury Lakehouse — Engineering Standards

These standards apply to ALL code in this repository. They are non-negotiable.

## Git Workflow

- **Never commit without explicit user approval**: `git commit`, `git push`, `gh pr create`, `gh pr merge`, and branch deletion are all user-controlled actions. Claude proposes changes; the user decides when to commit, push, create PRs, merge, or delete branches. "approved, proceed" on an implementation plan does NOT grant commit authority. Each commit, PR, and destructive git operation requires separate, explicit approval.

## Architecture Principles

- **SOLID**: Single responsibility per module/function. Depend on abstractions.
- **Clean Code**: Meaningful names, small functions, no dead code.
- **Separation of Concerns**: Ingestion, transformation (dbt), workflow orchestration (`src/workflows/`), shared constants (`src/shared/`), and presentation (Taipy) are fully isolated layers. `src/workflows/` has zero Spark/Taipy imports — only stdlib + PyYAML + Pydantic. `src/shared/` has zero external dependencies — stdlib only. Dependency direction enforced by `import-linter` in CI.
- **Idempotent Operations**: Every ingestion task can be re-run safely. Use partition-level overwrites, not full table drops.
- **Structured Logging**: JSON-line logs to stdout. No print statements. Include source name, row counts, and timing.

## Failure Investigation Protocol

- **Three-strikes rule**: Once might be a coincidence, twice is suspicious, three times is a pattern. After the FIRST unexpected failure or hang, investigate the root cause — do not retry the same operation.
- **Investigate before retrying**: When an infrastructure operation fails (warehouse timeout, deploy hang, API error), check service state and logs FIRST. A 2-minute REST API call beats a 14-minute blind retry.
- **Never disappear into long-running commands**: Any command that may take >30 seconds MUST use `run_in_background: true` so the user sees responses while it runs. Poll the output file every 15-30 seconds and report progress. A spinning timer with no text is not feedback — the user must see what is happening.
- **Report findings before fixes**: Present the diagnosis (with evidence) to the user before proposing or implementing a fix. The user decides the approach.
- **Proactively flag patterns**: When the same symptom appears twice, explicitly tell the user "this is a pattern that needs investigation, not another attempt."

## Investigation Discipline

- **Answer the specific questions first**: When given specific investigation questions, answer THOSE questions directly before exploring anything else. Do not go on tangents.
- **"I don't know yet" is acceptable — speculation is not**: If the evidence is insufficient, say so and describe what you need to check next. Never fill gaps with theories presented as findings.
- **Reproduce at the exact conditions**: If you cannot reproduce a reported bug, fixing your reproduction setup is the priority — not theorizing about why it might happen. Wrong viewport, missing data, or wrong interaction sequence means the investigation is incomplete, not that the bug is a mystery.
- **Never declare a root cause without evidence**: Saying "this is a framework bug" or "this is a CSS issue" requires concrete evidence showing the exact mechanism. Without it, say "I haven't found the root cause yet."

## Security Hardening

- **No secrets in code**: All authentication via Databricks runtime or environment variables. Never commit credentials, tokens, or connection strings.
- **HTTPS only**: All HTTP requests must use `https://`. Reject `http://` at the function level.
- **SSL verification**: Explicit `verify=True` on all `requests` calls. Never disable certificate verification.
- **Input validation**: Regex-validate all user-supplied identifiers (catalog, schema names) to prevent SQL injection. Pattern: `^[a-zA-Z_][a-zA-Z0-9_]*$`
- **Timeouts**: Every HTTP call must have explicit `(connect, read)` timeouts. Default: `(10, 30)`.
- **Retry with backoff**: Exponential backoff on transient errors (429, 5xx). Max 3 retries.
- **No dangerous builtins**: No `eval()`, `exec()`, `pickle.loads()`, or `subprocess.call(shell=True)`.
- **Scoped exception — `src/evolve/`**: `exec()` is permitted in `src/evolve/targets/*/evaluator.py` and `src/evolve/remote_worker.py` under the defense-in-depth policy documented in [ADR-001](docs/superpowers/adrs/ADR-001-evolve-code-execution.md): AST allowlist (parse-time) + restricted globals with `__builtins__: {}` (runtime) + subprocess isolation (backends). Gated by `code_evolution=True`. All other code must continue to avoid `exec()`/`eval()`.
- **Content validation**: Verify DataFrame schema and non-empty data before every Delta write.
- **Least privilege**: Scripts write only to the specified `{catalog}.{schema}.*` — never to arbitrary paths.

## AI Governance

- **`AI_GOVERNANCE.md` is the living record of EU AI Act posture**: When adding, modifying, renaming, or removing a per-player evaluative ML system (any workflow card listed in `PER_PLAYER_EVALUATIVE_CARDS` in `src/tests/test_ai_governance_md.py`), update `AI_GOVERNANCE.md` §5 (Scope), create or update the matching HuggingFace model card under `docs/huggingface/model-cards/`, add the `governance:` YAML block to the workflow card, and re-run `uv run pytest src/tests/test_ai_governance_md.py -v` before merging. The test enforces: required sections present, workflow-card inventory parity, model-card inventory parity, `governance:` YAML block presence, `EU AI Act — Intended Use and Non-Use` stanza presence on every model card, `SEC-AUDIT-v1.12.0 REG-01` provenance tag, and a 30-day grace-period check on the **Next review** date. Non-negotiable.
- **`ARCHITECTURE.md` Appendix D is the living record of academic references**: When introducing a new published methodology — new `Citation(...)` in a `PageConfig`, new `references:` entry in a workflow card, new methodology cited in `NOTICE` — add the author to `ARCHITECTURE.md` § 8 "D. Academic References" and extend the `expected_authors` list in `src/tests/test_architecture_md_appendix.py`. That test is the reason the appendix exists; it ran the D56 cycle and it still runs today. This rule was forgotten between March and April 2026 and caused the D56 academic-reference audit; the rule exists so that gap does not reopen.

## Architectural Decision Records (ADRs)

Significant architectural decisions — ones future maintainers will reasonably ask "why?" about — are documented in `docs/superpowers/adrs/` using the Michael Nygard format captured in `docs/superpowers/adrs/ADR-TEMPLATE.md`. The `mad-scientist-skills:final-review` skill Phase 2.5 scans for decisions that warrant an ADR and prompts for one before commit.

**When to write an ADR** — any of these patterns:

- Introduces, removes, or replaces a cross-cutting dependency (e.g., swapping a library for another, dropping a framework)
- Changes a schema ownership or grants model (e.g., `dbt-owners-{env}` group ownership; definer's-rights views for system-table access)
- Hard-codes a workaround for a platform constraint (e.g., `DATABRICKS_HTTP_PATH` double-slash for Git Bash MSYS; Python 3.10 lock for Databricks serverless)
- Introduces a naming, identifier, or path convention with downstream consumers (e.g., `frame_batch_id` synthetic keys for `applyInPandas` group sizing)
- Reimplements an algorithm to avoid a dependency (e.g., EFPI algorithm reimplementation to avoid `unravelsports` Python 3.11+ requirement)
- Introduces a defense-in-depth control or security boundary (e.g., evolve exec sandbox AST allowlist — ADR-001; SEC2 artifact hash verification)
- Makes a structural trade-off in the pipeline (e.g., guard injection as a mandatory no-default parameter in `run_pipeline()`, enforced by `test_guard_conformance.py`)

**When NOT to write an ADR:**

- Routine feature work that follows established patterns
- Bug fixes that do not change an architectural contract
- Documentation-only changes
- Refactoring that preserves behaviour and contracts

**Existing ADRs:** `docs/superpowers/adrs/ADR-*.md`. **Template:** `docs/superpowers/adrs/ADR-TEMPLATE.md`.

## Type Safety

- **Pyright basic mode**: All Python code must pass `pyright` in basic type checking mode.
- **Pydantic models**: Use where appropriate for configuration and data contracts.
- **Type annotations**: All public function signatures must have type annotations.

## Code Quality

All code must pass these checks with zero violations:

```bash
uv run ruff check src/ scripts/        # Lint (E, W, F, I, N, UP, B, S, RUF)
uv run ruff format --check src/ scripts/ # Format check (CI enforced)
uv run pyright src/            # Type check (basic mode)
uv run pytest src/tests/ -v    # Unit tests
```

- **Performance benchmarks**: Critical-path functions must have `pytest-benchmark` tests. Includes: batched pitch control, off-ball xT frame computation, DEFCON credit assignment, line-breaking detection, OBSO surface computation, position jitter augmentation, team shape computation, team shape frame (both teams), shape graph construction, shape graph position inference, Numba-accelerated pitch control, ScoutGPT/Football2Vec/360 `__getitem__` throughput, ScoutGPT/Football2Vec/360 forward pass. Regressions caught in CI.
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
| BLE      | flake8-blind-except (forbid `except Exception:` without justification) |
| RUF      | Ruff-specific rules |

- **No silent exception swallows** ([ADR-002](docs/superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md)): `BLE001` is enforced. New broad catches (`except Exception:`) require either (a) narrowing to a specific exception class, (b) a line-level `# noqa: BLE001 — <reason>` comment with an explicit architectural justification, or (c) a per-file-ignores entry in `pyproject.toml` with a one-line explanation. Silent-swallow telemetry code (`except Exception: logger.warning(...)`) is specifically forbidden — warning-level logs are invisible in error-log queries, which hid the 2026-04-12 warm-tier cost-hook blocker for 62+ hours. Default telemetry exception handling must be one of: raise, typed error return, or **ERROR-level** log.
- **Table-missing helper** ([ADR-002 §3](docs/superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md)): Use `ingestion.utils.tolerate_missing_table(logger, msg)` context manager for bootstrap code that queries a results table which may not exist on first run. The helper suppresses ONLY Spark errors matching specific table-missing markers (`TABLE_OR_VIEW_NOT_FOUND`, `Table or view not found`, `Path does not exist`, `DELTA_MISSING_DELTA_TABLE`, `DELTA_TABLE_NOT_FOUND`, `TableNotFoundException`). Every other exception propagates — including the `DELTA_MERGE_UNRESOLVED_EXPRESSION` schema-drift errors that bare `except Exception:` patterns previously hid. Never reinvent this pattern — import the helper.
- **Writer/target schema drift guard** ([ADR-002 §4](docs/superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md)): Any operational telemetry writer that MERGEs into a Delta table via `whenMatchedUpdateAll()` must (a) define its schema as a module-level constant (e.g. `_COST_LIVE_COLUMNS` in `src/ingestion/cost_hook.py`), (b) provide a lazy factory function that converts the constant to a Spark `StructType`, and (c) have a pytest that parses the canonical `CREATE TABLE` DDL and asserts column-list equality. Without these, schema drift between code and live table silently fails every MERGE with `DELTA_MERGE_UNRESOLVED_EXPRESSION`.
- **Hard-fail-first UDF semantics** ([ADR-002 §5](docs/superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md)): Inside any closure passed to a distributed executor (`applyInPandas`, `mapInPandas`, `@ray.remote`, etc.), exceptions must propagate with the group key in the error message: `raise RuntimeError(f"... failed for <key>={value}") from exc`. No `except Exception: return empty_df` patterns — those silently drop per-group data.

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
- **Liquid clustering over Z-ordering**: Mart fact tables use `liquid_clustered_by` (not `cluster_by`) for incremental, automatic data layout (24 of 33 models). Embedding models are excluded (not query-filtered). Liquid clustering is required for all new fact tables.
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
- **Memory budget for HF Jobs**: Before loading a dataset on HF Jobs, verify its size against the container's RAM (`l40sx1`: 62 GB, `cpu-basic`: 16 GB). Use column-selective loading (`pd.read_parquet(path, columns=[...])`) and per-partition streaming when full materialization would exceed 50% of available RAM. Default GPU flavor is `l40sx1` (L40S 48 GB VRAM, best cost/candidate — benchmarked 2026-04-05).
- **Pre-build indexes before batch processing**: For tracking-scale batch compute (>100K frames), always `dict(iter(df.groupby(key)))` at both the match level AND the frame level before entering processing loops. Two-level indexing (`match_groups[match_id]` → `frame_groups[frame]`) eliminates all O(n) boolean mask filtering.

### Performance Budgets

- **Pipeline task timeout**: ingest tasks ≤15 min, compute tasks ≤2 hr
- **App page load**: ≤3 seconds (first load), ≤500ms (cached interaction)
- **UDF group memory**: ≤800 MB peak (1 GB limit minus overhead)
- **Batched pitch control**: ≤5ms per frame for 22 targets (benchmark baseline)
- **Line-breaking detection**: ≤2ms per pass (benchmark baseline)
- **Team shape computation**: ≤1ms per frame for 10 outfield players (benchmark baseline)
- **Team shape frame (both teams)**: ≤2ms per frame for 22 players (benchmark baseline)

**Before modifying any function listed above, any function with a `pytest-benchmark` wrapper, or any function flagged as a hot path in this document, invoke `mad-scientist-skills:measure-before-optimize`.** The skill captures a baseline, waits for the change, re-measures, and reports the delta against the budget and a configurable regression threshold (default 10%). Peer skill to `mad-scientist-skills:optimization-audit`: this one is pre-change, that one is retrospective. Do not optimise benchmarked code on vibes.

## App Performance

- **Bound all data queries**: Every SQL query returning user-facing data must have a `LIMIT` clause. Use `LIMIT 500` for ranking/leaderboard queries, `LIMIT 2000` for timeline queries.

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
- **`Citation` for every methodology**: Any page implementing a published algorithm must include a `Citation(text, url)` in its `PageConfig`. No uncited methodologies. Practitioner methodologies (course materials, coaching frameworks) use `Citation(text)` without a URL but must include "(course materials)" in the label.
- **`NOTICE` file maintenance**: When adding a new analytics module, page, or algorithm, add a corresponding entry to the `NOTICE` file in the project root. The NOTICE file is the authoritative record of all third-party data attributions, library credits, and mathematical/academic references. Every `Citation` in a `PageConfig` and every `references:` entry in a workflow card must have a corresponding NOTICE entry. Update NOTICE in the same change — not as a follow-up.
- **`StatCard` for dashboard stat cards**: Dashboard pages use `stats: list[StatCard]` in `PageConfig`. Each card has `label`, `var`, optional `detail_var`, `help_text`, and `detail_html`. The presence of `stats` activates the dashboard layout branch. Set `detail_html=True` to render `detail_var` as raw HTML via a content provider iframe (supports inline `<span style="">` coloring); default `False` renders as plain text. Convention: every `StatCard` should have `help_text` (same rationale as `Metric`).
- **`ContentBlock` for all content**: Images use `ContentBlock("image", var)`, tables use `ContentBlock("table", var)`, Plotly charts use `ContentBlock("chart", var)`. Tables accept `table_cell_class_name={column: callback_name}` for per-cell CSS styling via Taipy's `cell_class_name` attribute (the callback returns a CSS class string). Never construct raw `<|{var}|chart|>` markup in page files.
- **WCAG color-independence on table columns**: Table columns that use color for categorization (e.g., Type, Freshness) must include a `::before` shape marker as a WCAG 1.4.1 secondary visual cue. Each category gets a distinct CSS-drawn shape (circle, diamond, triangle, square, ring) via `currentColor` so shapes inherit the text color. If the page includes a legend (e.g., DAG legend), shapes and colored text in the legend must match the table column markers.
- **Layout changes go through the template**: If a visual change requires editing more than one page file, it belongs in `page_template.py`. Individual page files contain only page-specific data.
- **`_FOOTER_CONTENT` for footer text**: The footer text ("Interactive Demo · Published Datasets") is a shared constant in `page_template.py`. Dashboard pages render it inside the scroll wrapper; other pages render it as the site-wide footer. Do not hardcode footer text in page files.
- **`is_dashboard=True` on `register_page_refresher`**: Required for dashboard pages. Controls `show_site_footer` state variable — omitting it causes footer duplication.
- **`ll-dashboard-scroll` for dashboard viewport**: Dashboard content is wrapped in a viewport-contained scroll area (`overflow: auto`, `max-height: calc(100vh - 245px)`). Both horizontal and vertical scrollbars live inside this container. The horizontal scrollbar stays at the viewport bottom.
- **State module isolation**: Each page's state module manages its own variables and callbacks. Shared state (competition/team/match filters) lives in `state/shared.py`. No cross-page state imports except from `shared`.
- **Never use `tp_` as a state variable prefix**: Taipy reserves `tp_` internally for its expression evaluator (`TpExPr_`). Variables starting with `tp_` will have state updates silently dropped — no error, no warning. Also avoid `tpec_` (Taipy edge-case prefix). Safe prefixes: `tac_`, `gk_`, `ts_`, `pt_`, `dv_`, etc.
- **Glossary coverage**: Every domain-specific term used in metric names, chart labels, or descriptions must have an entry in `GLOSSARY` (in `template.py`) and be listed in the page's `PAGE_TERMS` entry.

### Why Template-First

Building pages as imperative layout code leads to inconsistency debt that compounds per-page. With 12+ pages, hand-crafting each one guarantees: missing tooltips on some pages, different metric formats, inconsistent empty-state handling, and layout drift. The template makes these structurally impossible — required fields are constructor parameters, not afterthoughts. A CHI audit against a template architecture produces template-level fixes (one change, all pages); without it, the same audit produces N per-page fixes.

## UX Standards

These rules prevent cognitive interface debt from accumulating. Derived from CHI-AUDIT-180 and CHI-AUDIT-190 (cognitive-interface-audit v1.8.0+, 15 frameworks). Every Taipy or Gradio code change must satisfy all of these.

- **Never silently substitute data**: If a fallback, default, or NaN-fill changes what the user sees, surface it with a visual indicator. The user must be able to tell what data source produced what they're looking at.
- **Patterns applied to some pages must be applied to all**: When adding a cross-cutting pattern (captions, tooltips, help text, layout changes), apply it to ALL pages in the same commit. If a page is excluded, add a code comment explaining why.
- **Model selectors need comparison affordance**: When adding a selector that switches between models/algorithms/views, consider the comparison workflow: side-by-side layout, delta indicators, or at minimum persist the previous selection's values visually. Users should not need to remember numbers across selector clicks.
- **Navigation labels must be goal-oriented**: Page titles should describe the user's goal, not the implementation. "Player Comparison" not "Player Radar". "Defensive Impact" not "Def. Pressure".
- **Raw IDs must never reach the user**: Never display `player_id`, `match_id`, or `team_id` in selectboxes, tables, or chart labels. Always join to dimension tables for human-readable names.
- **Multi-surface UX parity**: When a Taipy page has glossary terms, help tooltips, scale references, or academic citations, the corresponding Gradio demo tab must have equivalents (e.g., `gr.Accordion("Glossary")` with per-tab filtered terms, axis labels with range/direction, `gr.Markdown` citations). A feature on one surface without its UX scaffolding on the other is incomplete.
- **Computed metrics must show scale and direction**: Any displayed score on a 0–1 or non-obvious scale (PAUSA, OBSO, cosine distance, xT, VAEP) must include the range and direction in at least one of: axis label, chart title, tooltip, or adjacent caption. "0.347" alone is never acceptable — "0.347 (0–1, higher = better)" is.
- **HF artifact link completeness**: When publishing a new HF dataset or model, update ALL locations that reference the artifact list: HF Space header, HF Space footer, `docs/huggingface/org-card.md`, and `README.md`. A checklist in the PR description prevents drift.

## Project Conventions

- **`DATABRICKS_HTTP_PATH` must use double-slash prefix**: Set as `//sql/1.0/warehouses/<id>` (not `/sql/...`). Git Bash (MSYS) converts single-slash paths to Windows paths (`C:/Program Files/Git/sql/...`), silently breaking the `databricks-sql-connector` Thrift client. Double-slash is treated as a UNC prefix by MSYS (left alone) and as equivalent to single-slash on all other platforms.
- **Use `scripts/ensure_warehouse.py`** before any `dbt build`: The SQL warehouse auto-stops after 10 minutes of inactivity. The `databricks-sql-connector` auto-resume retry is unreliable (known sleep-floor bug). Always run `python scripts/ensure_warehouse.py -- <command>` to verify the warehouse is RUNNING first.
- **Use `scripts/dbt_build_and_refresh.py`** as the canonical dev flow for "rebuild gold tables and propagate". Wraps `dbt build` (any args forwarded) with a synchronous `refresh_synced_tables.py --wait` on success. Fails fast if dbt errors — refresh only runs after a clean dbt build. This eliminates the manual two-step that was the original cause of stale Lakebase data after gold rebuilds. Direct `dbt build` invocation is allowed but will leave Lakebase synced tables stale until the next manual refresh.
- **Run `scripts/grant_synced_table_permissions.py` after any synced table creation/recreation**: Lakebase synced table refresh requires two grants per service principal: `CAN_USE` on the database project + `CAN_RUN` on each backing pipeline. Both the staging Taipy admin endpoint (running as `hf_app_v2` SP) and the daily Databricks job's refresh task (running as `ingestion` SP) depend on these grants. The script is idempotent and looks up SP application_ids by display name pattern (no hardcoding) and the database project ID via synced table metadata (no hardcoding). Modes: `--status`, `--dry-run`, `--grant` (default), `--revoke`. Integrated into `scripts/maintain_synced_tables.py` as Step 0. Pipeline IDs may change after synced table recreation (undocumented Databricks behavior) — re-run after any UC schema change or synced table recreation.
- **Python 3.10 (locked)**: Pinned to `>=3.10,<3.11` in `pyproject.toml` and `.python-version`. Databricks serverless only supports Python 3.10 — locking locally ensures tests catch version-specific behavior (e.g., pandas API differences) before they reach production. Run `uv sync` to get a 3.10 venv automatically.
- **Line length**: 120 characters maximum.
- **Imports**: stdlib → third-party → first-party, enforced by isort.
- **Entry points**: Each ingestion module exposes a `main()` function registered in `pyproject.toml`.
- **`@workflow` decorator**: All compute pipelines in `src/ingestion/` are decorated with `@workflow("wf-xxx", phase="yyy")` from `src/workflows/`. The decorator wraps calls through the lifecycle runner, dispatching `on_start`/`on_complete`/`on_skip`/`on_error` hooks. `CostEstimateHook` (registered in each pipeline's `main()`) writes cost data to `{catalog}.observability.workflow_cost_live` via Delta MERGE. Adding `*, ctx=None` to `run_pipeline()` is optional (injected by the runner when present). All `run_pipeline()` functions must return `int` (row count) — enforced by `test_pipeline_row_count.py`.
- **Observability schema**: `soccer_analytics.observability` hosts platform operational metadata (cost tracking, future run history, SLIs, alerts). Separate from `bronze`/`gold` data schemas. `CostEstimateHook` uses `cost_schema="observability"` (default) to target this schema regardless of the pipeline's `--schema` argument.
- **Workflow cards**: Each AI/ML workflow has a YAML+Markdown manifest in `workflow-cards/wf-*.yaml`. Cards describe inputs, outputs, dependencies, execution config, academic provenance, cost estimates, and monitoring thresholds. Validated in CI via `validate_workflow_cards` entry point.
- **Workflow card phase-parity**: Every `cost.<phase>` key on a card MUST match an `execution.<phase>` key on the same card — `test_card_cost_phase_parity` enforces this. Prior failure mode: authors put `cost.inference` on ingestion/export/import/sync/orchestration cards as a workaround for a hardcoded Taipy render loop, which silently mislabelled the cost panel and hid the actual execution phase. Use the phase key that matches your `execution:` block: `cost.ingestion` for ingestion tasks, `cost.export` for export tasks, `cost.orchestration` for super-tasks, etc.
- **dbt-derived outputs in workflow cards**: Any `outputs.tables` entry whose table is produced by a dbt model (not by a Python execution phase on the same card) MUST declare `dbt_model: <model_name>` on its `TableRef`, where `<model_name>` matches a `.sql` file under `dbt_project/models/**/`. Enforced by `test_card_dbt_model_field`. `wf-dbt-build.yaml` enumerates all 33 gold-layer mart models with `dbt_model:` set on each; `wf-goalkeeper.yaml` declares `dbt_model: fct_goalkeeper_stats` because the table is produced by `dbt build`, not by a standalone Python entry point on that card.
- **HF Jobs publish scripts (`scripts/publish_*_hf.py`)**: Tracked alongside TF wheel tasks by `test_card_parity_with_terraform._HF_JOBS_SCRIPT_TO_CARD`. Every `publish_*_hf.py` script on disk must be listed in that mapping, either pointing at an owning workflow card (trigger=`manual`, runtime=`hf-jobs`, `script: "scripts/<name>.py"`) or `None` with an inline justification comment explaining why a card is not warranted.
- **Databricks job block ordering** (generalised 2026-04-16): `test_workflows_tf_ordering` walks every `databricks_job` resource under `terraform/**/*.tf` — not just `data_ingestion` — and asserts that `task` / `environment` / nested `depends_on` blocks are sorted alphabetically by their primary key. Empirically verified via targeted `terraform plan`: the same positional-matching drift class does NOT affect `access_control` blocks inside `databricks_permissions` (the provider matches those by principal identity, not position), so a parallel repo-wide ACL-ordering test is intentionally omitted.
- **Delta tables**: All bronze writes include `_ingested_at` audit column with UTC timestamp.
- **Partition overwrite**: Use `replaceWhere` for incremental loads, not full table overwrites.
- **Mandatory guard injection**: Every pipeline's `run_pipeline()` receives `FilterResult` as a **required** parameter (no default). Each pipeline's `main()` calls its guard via `timed_check(skip_guard, spark, catalog, schema)` from `ingestion/guards.py` — NOT `skip_guard.check()` directly. `timed_check` records guard wall-clock duration via `time.monotonic()` and returns a `FilterResult` with `guard_duration_seconds` populated. This enables three-way decomposition of total task time: environment setup, guard check, pipeline work. `find_new_ids()` is prohibited outside guard classes, enforced by `TestNoInlineGuardInPipeline`. 33 guards registered in `_GUARD_MODULES`. Guard conformance enforced by test classes in `test_guard_conformance.py`: import isolation (AST + runtime transitive), mandatory params, no inline guards, direct guard call (no gate indirection), early exit structure, early exit behavior, exception propagation (AST + behavioral), count/ID consistency, cost/time capture, workflow ID consistency.
- **Lakebase synced-table grants must be re-applied on every table recreation** ([ADR-005](docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md)): there is no PostgreSQL auto-inherit path available for synced tables. They are owned by the internal role `databricks_writer_<instance_id>`, not `databricks_superuser`, and any `ALTER DEFAULT PRIVILEGES FOR ROLE <x>` rule targeting a role we can reach will never fire. When a synced table is recreated (dbt `table` materialization drop-and-recreate, schema change, UI recreate), the new PG table has zero SP grants. The canonical repair is `uv run python scripts/run_lakebase_grants.py`; the script reads the Taipy SP UUID from `terraform output -raw hf_app_sp_application_id` (single source of truth), then issues schema-level + per-table explicit `GRANT SELECT` covering the full `refresh_synced_tables.SYNCED_TABLES` inventory. Two automated gates prevent this class of incident from recurring: (a) `scripts/manage_space.py deploy` runs `run_lakebase_grants.py --verify` in `_preflight` and aborts with a per-table drift diff if any `(SP, table)` pair is missing (escape hatch: `--skip-grants-check`); (b) `.github/workflows/lakebase-grants.yml` — a scheduled GitHub Actions workflow — runs `apply` + `--verify` daily at 07:00 UTC (post-daily-job) and after every `Terraform Apply` on main, auth via `secrets.DATABRICKS_TOKEN` admin PAT. This self-heals any drift within ≤24 h, or within minutes for TF-driven recreations. Grants are deliberately NOT run from the Databricks job runtime — the ingestion SP is not a Lakebase PG role and cannot execute `GRANT`. Do NOT re-introduce a `FOR ROLE databricks_superuser` clause — it was verified non-functional during the 2026-04-17 investigation.
- **No `#sha256=` fragment on UC Volume wheel paths**: Serverless pip rejects `#sha256=` on local file paths (`/Volumes/.../...whl#sha256=...`) with `ERROR_INVALID_REQUIREMENT`. The fragment is only valid for HTTP URLs. `bump_wheel.py` splits consumers into `_HASH_CONSUMER_GLOBS` (PEP 723 scripts, `deploy.sh` — receive hash) and `_VERSION_ONLY_CONSUMER_GLOBS` (Terraform — version only, NEVER hash). The `--check` mode forbids `#sha256=` in Terraform consumers.
- **dbt model contracts**: Gold-layer models have `contract: {enforced: true}` with explicit `data_type` on every column (30 of 33 models). When adding or changing columns in a mart model, update the `_marts__models.yml` contract to match. `on_schema_change: fail` ensures mismatches are caught at build time. New models must have contracts enforced.
- **dbt slim CI**: PR builds use `state:modified+` to only build/test changed models. The `--empty` flag validates schema contracts with zero-cost DDL (no data movement).
- **dbt ownership model — `dbt-owners-{env}` group**: Both the developer's account user and the ingestion service principal need to be able to REPLACE objects in `dev_silver` + `dev_gold` so that local `dbt build` (developer identity) and the daily-job `dbt_build` task (ingestion SP identity) can coexist. Unity Catalog requires the caller to be the object owner OR the schema owner; without group-based ownership, whichever identity built last would lock out the other. Solution: a Terraform-managed `dbt-owners-{env}` group (`terraform/modules/service_principals/main.tf`) contains both members. The `dev_silver` and `dev_gold` schemas are owned by this group via a one-time `ALTER SCHEMA ... OWNER TO` (these schemas are dbt-created at runtime, not Terraform-managed, so they cannot be owned via Terraform). A `+post-hook` in `dbt_project.yml` transfers per-object ownership of every newly-built model back to the group on each run, keeping ownership stable and preventing per-object owner drift. Adding a new identity that needs dbt write access: add a `databricks_group_member` resource referencing `databricks_group.dbt_owners` and `terraform apply`. New objects created outside dbt (via `CREATE TABLE` from a notebook, etc.) need a manual `ALTER ... OWNER TO 'dbt-owners-{env}'` to keep the schema consistent.
- **System-table access via definer's-rights views**: dbt models that need `system.billing` or `system.lakeflow` data MUST reference them via filtered views in `soccer_analytics.observability` (e.g., `system_billing_usage`, `system_billing_list_prices`, `system_lakeflow_job_task_run_timeline`). The `system` catalog is metastore-managed and CANNOT be granted to service principals or groups via the standard UC grant API (returns 403 even for account admins). UC views default to definer's-rights semantics: the view OWNER must have SELECT on the underlying tables, but CONSUMERS only need SELECT on the view itself. The views are created by `scripts/setup_system_billing_views.sql`, owned by an account-admin user (currently `karstenskyt@gmail.com` who inherits `system.*` access via account-users membership), and grant `SELECT` to `dbt-owners-{env}`. The ingestion SP reads through the group → view → account-admin identity → underlying table chain. Each view applies the filters needed by `fct_workflow_costs` (JOBS billing origin, 90-day window, non-NULL result_state) so downstream models don't repeat them. Maintenance: re-run the SQL script as an account admin if the view owner leaves the workspace; `CREATE OR REPLACE VIEW` is idempotent.
- **Pre-compile regex at module level**: Never use `re.compile()`, `re.sub()`, or `re.match()` with raw pattern strings inside function bodies or loops. Compile patterns as module-level constants.
- **HTTP caching via `requests-cache`**: The shared `fetch_url()` session uses `requests_cache.CachedSession` with SQLite backend. Static open-data sources (StatsBomb GitHub) are cached indefinitely; other sources use a 24-hour TTL. Set `LUXURY_LAKEHOUSE_HTTP_CACHE=0` to disable. Bronze Delta tables remain the durable cache; HTTP cache avoids redundant network round-trips during development and retry.
- **HF Jobs wheel convergence**: HF Jobs PEP 723 scripts import domain logic from the `luxury-lakehouse` wheel hosted at `luxury-lakehouse/build-artifacts` on HF Hub. CI uploads the wheel on main merges and publishes a `sha256sums.json` sidecar. The canonical wheel version lives in `src/shared/wheel.py` (`WHEEL_VERSION`, `WHEEL_FILENAME`, `WHEEL_BASE_URL`). Dynamic consumers (`evolve/backends/hf_jobs.py`, `benchmark_hf_jobs.py`, `manage_space.py`, `deploy_wheel.py`) import from `shared.wheel` directly. Static consumers (PEP 723 headers in `scripts/*_hf.py`, `deploy.sh`, Terraform) are kept in sync via `uv run python scripts/bump_wheel.py`. To bump the wheel version: update `pyproject.toml`, then run `bump_wheel.py` (optionally with `--pin-hash <SHA256>` after CI uploads). CI enforces consistency via `bump_wheel.py --check`.
- **HuggingFace Hub**: Org is `luxury-lakehouse`. Model artifacts cached in UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/`. Set `HF_HOME` env var for local cache location. Use `huggingface_hub` for model publish/download (no torch dependency). See `docs/huggingface-setup.md`. Model card and org card source of truth: `docs/huggingface/model-card.md` and `docs/huggingface/org-card.md` (pushed to HF Hub 2026-03-09).
