# Luxury Lakehouse — Engineering Standards

These standards apply to ALL code in this repository. They are non-negotiable.

## Git Workflow

- **Never commit without explicit user approval**: `git commit`, `git push`, `gh pr create`, and `gh pr merge` are all user-controlled actions. Claude proposes changes; the user decides when to commit, push, create PRs, or merge. "approved, proceed" on an implementation plan does NOT grant commit authority. Each commit, PR, and destructive git operation requires separate, explicit approval.

## Architecture Principles

- **SOLID**: Single responsibility per module/function. Depend on abstractions.
- **Clean Code**: Meaningful names, small functions, no dead code.
- **Separation of Concerns**: Ingestion, transformation (dbt), workflow orchestration (`src/workflows/`), shared constants (`src/shared/`), and presentation (Taipy) are fully isolated layers. `src/workflows/` has zero Spark/Taipy imports — only stdlib + PyYAML + Pydantic. `src/shared/` has zero external dependencies — stdlib only. Dependency direction enforced by `import-linter` in CI.
- **Idempotent Operations**: Every ingestion task can be re-run safely. Use partition-level overwrites, not full table drops.
- **Structured Logging**: JSON-line logs to stdout. No print statements. Include source name, row counts, and timing.
- **ML inference outputs follow [ADR-013](docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)**: Python writer → bronze raw → dbt staging view → gold mart with `contract: enforced: true`. Surrogate keys resolve in the mart via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit only native identifiers + predictions. First applied in PR 3 (xG v2 promotion); PR 7 extends to `fct_pausa_values`.

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
- **Content validation**: Verify DataFrame schema and non-empty data before every Delta write. `write_delta_table`/`merge_delta_table` auto-drop top-level `void` (NullType) columns with a loud log — an all-NULL inferred column schema-evolves into an unscannable Delta void column that bricks `SELECT *` on the whole table (2026-06-10 `gradientsports_events` incident; guard + tests in `ingestion/utils.py::_strip_void_columns`).
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
- Introduces a cross-table value-format contract or referential-integrity invariant (e.g., `bronze.X.native_id` ⊆ `dim.Y.native_id` per provider). See ADR-018 + the per-(source, entity) singular tests under `dbt_project/tests/`.

**When NOT to write an ADR:**

- Routine feature work that follows established patterns
- Bug fixes that do not change an architectural contract
- Documentation-only changes
- Refactoring that preserves behaviour and contracts

**Existing ADRs:** `docs/superpowers/adrs/ADR-*.md`. **Template:** `docs/superpowers/adrs/ADR-TEMPLATE.md`.

## External Research Tracking

- **`docs/research/external-research-tracking.md` is the durable record of external research sources being monitored** — specific pre-publication papers, academic labs (DTAI Sports Analytics Lab / KU Leuven), conferences (MLSA), and actively-developed libraries (UnravelSports) whose output could change a lakehouse decision. Add a tracker entry (What / Why / Mechanism / Last reviewed / Next review) when a LinkedIn post, paper, or release surfaces an *ongoing* research stream — not when a one-shot conversion to TODO suffices (e.g. D60-D64 from single LinkedIn posts went directly to TODO without a tracker entry).
- **Quarterly review cadence**: first week of Jan / Apr / Jul / Oct. Check each active tracker's mechanism, update "Last reviewed," promote anything ready to TODO/ROADMAP/ADR via the Promotion log, archive anything that went stale. Next scheduled review: 2026-07-24.

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
- **Training-to-production delivery contract** ([ADR-012](docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md)): Every training script that targets the Databricks inference path MUST import from `ingestion.artifact_deploy` and call all three helpers — `require_mlflow_env()` at the top of `main()` (no silent `if tracking_uri:` gate), `set_and_verify_mlflow_champion(...)` after `mlflow.pyfunc.log_model` (zombie-alias guard), and `upload_weights_to_uc_volume(...)` after the HF Hub publish (the second leg of the delivery chain — the consumer's UC Volume fallback is NOT optional). Training invocations via `hf jobs uv run` MUST pass secret-valued env vars via `--secrets` (encrypted), never `--env` (plain metadata visible via `hf jobs inspect`). Any PEP 723 script is single-file — sibling imports do NOT work on HF Jobs; helpers that need to travel with the script must be inlined.
- **Model weights envelope feature_names convention** ([ADR-012 §2](docs/superpowers/adrs/ADR-012-training-to-production-delivery-hardening.md)): Model weight serialization formats that do not natively embed feature names (e.g., the NumPy-dump envelope used by `analytics.set_encoder.serialize_set_encoder_weights`) MUST inject a top-level `feature_names: list[str]` field into the JSON envelope. Inference reads it and reindexes tabular input. XGBoost is exempt because the booster binary already carries feature_names via `get_booster().feature_names`. **Grace-period closure (2026-05-02, SK3-MIG):** legacy envelopes without `feature_names` now raise `RuntimeError` at inference time via `xg_model_v2._parse_v2_envelope_features` (the v2→v1 fallback was removed). Trainer (`scripts/train_xg_v2_hf.py`) hardened to inject `tabular_dim` alongside `feature_names` as defense-in-depth. See [ADR-022](docs/superpowers/adrs/ADR-022-direction-of-play-migration.md).
- **Cross-table format contracts** ([ADR-018](docs/superpowers/adrs/ADR-018-cross-table-format-contract-testing.md)): Every native ID format used as a JOIN key has its canonical generator in `src/shared/identifiers.py`. Bronze writers + applyInPandas UDFs import from this module; dbt singular tests (`assert_<source>_<entity>_native_join_resolves.sql`) assert JOIN-coverage from `bronze.spadl_actions` to `dim_*`. Adding a new bronze writer / dim staging touchpoint REQUIRES adding the corresponding format-contract test in the same PR. silly-kicks API drift caught at OUR boundary via `src/tests/test_silly_kicks_boundary.py`.
- **silly-kicks 3.0.1+ direction-of-play kwargs** ([ADR-022](docs/superpowers/adrs/ADR-022-direction-of-play-migration.md)): Sportec + Metrica converters require explicit `home_team_start_left` (or `home_attacks_right_per_period` mapping) per silly-kicks 3.0.1's `convert_to_actions(...)` API. Lakehouse derives the bool via two helpers in `src/ingestion/spadl_adapter.py`: `derive_idsse_home_team_start_left` reads `kickoff_team_left` from the IDSSE bronze KickOff event row (authoritative — DFL XML); `derive_metrica_home_team_start_left` infers from period-1 SHOT positions (empirical — Metrica bronze does not capture a kickoff-side flag). Both raise loud on insufficient signal. Canonical SPADL LTR invariant (both teams' shots cluster at high-x post-conversion) is regression-gated by `src/tests/test_sk3_coord_correctness.py`. Strict-mode env var `SILLY_KICKS_ASSERT_INVARIANTS=1` is set process-wide via `src/ingestion/bootstrap.py:os.environ.setdefault(...)` (Databricks serverless `compute.Environment` does not support per-job env vars natively); same env var also set at job level in CI workflows.
- **silly-kicks 4.0+ extra-time symmetric guard** ([ADR-029](docs/superpowers/adrs/ADR-029-silly-kicks-4-et-direction-adoption.md)): silly-kicks 4.0.0 adds `require_et_direction` across all 5 per-period-absolute converters (Sportec tracking + Sportec/Metrica/GS events + GS tracking) — raises on `period_id in {3, 4}` without `home_team_start_left_extratime`. Lakehouse-side ET derivers: `derive_idsse_home_team_start_left_extratime` reads DFL XML `extraTimeFirstHalf` KickOff; `derive_metrica_home_team_start_left_extratime` empirically infers from period-3 SHOT positions; GS carries the flag in bronze metadata. `MatchMeta.home_team_start_left_extratime` plumbs through to every `convert_to_frames` / `convert_to_actions` call site. `None` is safe when match has no ET periods. Set-piece restart actions get finite DAS via `src/analytics/action_context/enrich.py:_fill_possession_from_set_piece_actions` (lakehouse owns this modeling decision per the PR-S67 boundary; silly-kicks stays pure). Pre-flight sentinel `src/tests/action_context/test_et_direction_sentinel.py` asserts the pipeline path actually reaches the guard.
- **Gradient Sports bronze writer dedup** ([ADR-030](docs/superpowers/adrs/ADR-030-gradient-sports-bronze-frame-dedup.md)): The GS provider ships content-divergent duplicate `(period, frameNum)` records (up to 16 copies per frame in match 10502). `src/ingestion/gradientsports_tracking.py:_iter_unique_frames` does keep-first dedup at the bronze writer — the lakehouse's data-quality boundary. NO adapter-level dedup; NO per-feature dedup. silly-kicks 4.0.1's bekkers defense-in-depth is complementary, not a replacement. Synthetic 16-copy regression at `src/tests/test_gradientsports_ingestion.py::TestFrameDedup`. After provider-side bronze changes, existing GS bronze must be re-ingested for the dedup to apply to already-stored data.

## Database Performance

### Lakebase (PostgreSQL) — Synced Tables

- **Index every filtered column on fact tables >100K rows**: Any column used in a `WHERE` clause on a fact table must have an index. Use composite indexes matching the most common multi-column filter patterns (leftmost = highest selectivity).
- **No `ON ONLY` indexes**: Lakebase synced tables are internally partitioned (`__db_system.partition_*`). Indexes MUST be created WITHOUT the `ONLY` keyword to cascade to child partitions. Parent-only indexes are invisible to the query planner.
- **Index recreation after synced table rebuild**: Custom PG indexes are dropped when a synced table is recreated. The daily `.github/workflows/lakebase-grants.yml` GitHub Action reapplies them automatically. For immediate manual post-recreation repair, use `uv run python scripts/maintain_synced_tables.py --skip-refresh`. For full migration (all 41 tables), use `uv run python scripts/migrate_synced_tables.py` which runs the maintenance pipeline in Phase 4. See `docs/engineering/conventions.md` → "Lakebase Ops" for the full standard pattern.
- **Avoid `SELECT DISTINCT` on large tables**: Use recursive CTE "loose index scan" pattern instead. `SELECT DISTINCT` forces a full sequential scan; the recursive CTE performs O(k × log n) index lookups for k distinct values.
- **Dimension tables don't need custom indexes**: Tables under ~50K rows with PK lookups perform well with sequential scans. Only index fact tables.
- **Verify with EXPLAIN ANALYZE**: After creating indexes, confirm Index Scan (not Seq Scan) on all fact tables via `scripts/create_indexes.py --verify`.
- **Re-deriving a TRIGGERED-synced mart ([ADR-043](docs/superpowers/adrs/ADR-043-strand-safe-synced-rederive.md))**: never `dbt --full-refresh` it directly (strands the synced table; the `on-run-start` tripwire now aborts any `--full-refresh` selecting a TRIGGERED mart — including the mega-job `dbt_full_refresh=true` parameter on stages 2/3). Use `uv run --extra sdk python scripts/rederive_synced_marts.py --select <sel> [--provider P | --match-ids …]` — D marts MERGE-reprocess (no downtime), `table` marts plain-rebuild (**strand-and-heal since the 2026-06-10 platform change** — the rebuild strands the synced table and the ADR-041 heal recreates it, brief re-snapshot downtime; the tool exits loud via `refresh_synced_tables --fail-on-strand`, see ADR-043 amendment 2), other TRIGGERED marts delete→full-refresh→recreate; `--rebuild` full-rebuilds a D mart for a schema/contract change. The TRIGGERED set lives in BOTH `SYNCED_TABLES` (`src/ingestion/refresh_synced_tables.py`) and the `triggered_synced_marts` var in `dbt_project.yml` — parity enforced by `src/tests/test_strand_safe_rederive.py`.

### Databricks (PySpark / Delta Lake)

Short-form rules below. Full decision hierarchies, `applyInPandas` mechanics, serverless constraints, EIP pattern table, and batch-compute optimisation detail in `docs/engineering/databricks-serverless.md`.

- **Avoid double `df.count()` before writes**: Do not call `df.count()` for validation if `write_delta_table()` will call it again. Each `count()` triggers full DAG recomputation. **Since [ADR-045](docs/superpowers/adrs/ADR-045-ac1-single-pass-write-and-aqe-proof-dispatch.md)**, `write_delta_table` without a caller `row_count` counts the materialized Delta slice POST-write when the slice is identifiable (replaceWhere / full overwrite) — only bare-append without `row_count` still pre-counts the source (the AC-1 applyInPandas chain silently paid 2× per half under the old pre-count).
- **Always pass `row_count`**: When `validate_dataframe()` returns a row count, pass it to `write_delta_table(row_count=row_count)` and `merge_delta_table(row_count=row_count)` to avoid redundant `df.count()` DAG recomputation.
- **Prefer `replaceWhere` over bare `mode="append"`**: Append without partition guards risks duplicates on retry. Use `replaceWhere` keyed on the logical partition (e.g., `match_id`, `competition_id`) for idempotent writes.
- **`write_delta_table` retries the concurrent-commit conflict class** ([ADR-038](docs/superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md)): Delta `Concurrent*Exception` + the serverless S3-400-at-`_delta_log`-commit signature, with fully-jittered backoff (`_COMMIT_MAX_ATTEMPTS=10`, sized to the 8-way for-each fan-out). Multiple workers writing one Delta table concurrently are safe (writes must be disjoint `replaceWhere` partitions — idempotent under retry). Single-writer callers are unaffected (they never raise the conflict); the only change for them is the rare added latency if they ever do contend.
- **`.toPandas()` calls must be bounded**: enforced mechanically by `src/tests/test_topandas_boundedness.py` (CI gate, AST + allowlist `src/tests/_topandas_exemptions.yml`). Adding a `.toPandas()` call without a DataFrame-API bound (`.filter`/`.where`/`.limit`/`.distinct`/`.groupBy(...).agg|count|sum|mean`) requires an allowlist entry articulating WHY driver memory can hold the result. Origin: OPT-1 audit (2026-05-02) — discovery that the prior `<5M rows` guidance had drifted past one full-fact violator (`expected_threat.py` global rebuild) without anyone noticing. The test is the source of truth; this bullet exists only to point readers at it.
- **Prefer Spark executors over driver-bound processing**: `applyInPandas` / `mapInPandas` → UC Volume Parquet → last-resort per-partition `.toPandas()`. Full decision hierarchy + group sizing + multi-pass + executor model caching in databricks-serverless.md.
- **Liquid clustering + auto-compaction + Predictive Optimization + deletion vectors** are the mart-table defaults (liquid_clustered_by, autoOptimize tblproperties, catalog-level PO, DBR 14.1+). Rationale + coverage numbers in databricks-serverless.md.

### Databricks Serverless Constraints (summary)

16 GB driver (fixed), 1 GB UDF group cap, no broadcast variables, no `df.cache()`/`persist()`, no internet in UDFs, no local filesystem writes from Spark (`file://` forbidden, DBFS disabled), lazy closure capture — use frozen dataclasses for `applyInPandas` config. Full list with rationale in `docs/engineering/databricks-serverless.md` → "Databricks Serverless Constraints".

### Batch Compute Optimization (summary)

Factor loop-invariant computation out and broadcast; verify HF Jobs dataset size against container RAM before loading (`l40sx1`: 62 GB, `cpu-basic`: 16 GB); pre-build `dict(iter(df.groupby(key)))` indexes at both match and frame level for tracking-scale batch compute. Full detail + OBSO case study in `docs/engineering/databricks-serverless.md`.

### Performance Budgets

- **Pipeline task timeout**: ingest tasks ≤15 min, compute tasks ≤2 hr. **Documented exception ([ADR-037](docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md)):** `compute_action_context` is a worker-drain task (`timeout_seconds = 28800`); it drains the whole work-queue to completion (one-time cold start ~5.5 h). The **2700 s** budget there (raised from 1800 s 2026-06-03; overridable per run via the drain worker's `--watchdog-budget-s`) is a **per-game watchdog inside the worker** — now effectively **per-half**, since all tracking providers enqueue per-`(match, period)` units — not the iteration timeout, and there is no `chunk_size` (the per-game watchdog + persistent worker removed the per-iteration budget it packed against).
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

Taipy-specific rules (PageConfig template, StatCard, state-prefix conventions including the `tp_` ban, WCAG shape markers, dashboard scroll wrapper) live in `hf_taipy_app/CLAUDE.md`. Claude Code auto-loads it when working inside that directory tree.

## UX Standards

These rules prevent cognitive interface debt from accumulating. Derived from CHI-AUDIT-180 and CHI-AUDIT-190 (cognitive-interface-audit v1.8.0+, 15 frameworks). Every Taipy code change must satisfy all of these.

- **Never silently substitute data**: If a fallback, default, or NaN-fill changes what the user sees, surface it with a visual indicator. The user must be able to tell what data source produced what they're looking at.
- **Patterns applied to some pages must be applied to all**: When adding a cross-cutting pattern (captions, tooltips, help text, layout changes), apply it to ALL pages in the same commit. If a page is excluded, add a code comment explaining why.
- **Model selectors need comparison affordance**: When adding a selector that switches between models/algorithms/views, consider the comparison workflow: side-by-side layout, delta indicators, or at minimum persist the previous selection's values visually. Users should not need to remember numbers across selector clicks.
- **Navigation labels must be goal-oriented**: Page titles should describe the user's goal, not the implementation. "Player Comparison" not "Player Radar". "Defensive Impact" not "Def. Pressure".
- **Raw IDs must never reach the user**: Never display `player_id`, `match_id`, or `team_id` in selectboxes, tables, or chart labels. Always join to dimension tables for human-readable names.
- **Computed metrics must show scale and direction**: Any displayed score on a 0–1 or non-obvious scale (PAUSA, OBSO, cosine distance, xT, VAEP) must include the range and direction in at least one of: axis label, chart title, tooltip, or adjacent caption. "0.347" alone is never acceptable — "0.347 (0–1, higher = better)" is.
- **HF artifact link completeness**: When publishing a new HF dataset or model, update ALL locations that reference the artifact list: HF Space header, HF Space footer, `docs/huggingface/org-card.md`, and `README.md`. A checklist in the PR description prevents drift.

## Orchestration Discipline

Short-form rules for multi-backend training orchestration (`scripts/evaluate_*`, `src/evolve/backends/`). Full rationale, failure-mode catalog, and smoke-test reference implementation in `docs/engineering/orchestration.md`. Partially enforced by `src/tests/test_evolve_football2vec_l2.py`.

- **HF tokens via `huggingface_hub.get_token()`**, never `os.environ.get("HF_TOKEN", "")` — non-interactive SSH often has HF_TOKEN unset; empty default triggers `httpx.LocalProtocolError` on `Bearer ` header.
- **Smoke tests must exercise `HfApi().whoami()` auth**, not just imports — unauthenticated remotes pass import checks then silently burn every dispatched variant.
- **Post-deploy entrypoint verify is mandatory** — re-run the exact import chain the worker will execute (`from evolve.evaluator import EvolveEvaluator; from evolve.remote_worker import main`) after `_deploy_to_remote`.
- **Per-backend timeout = measured per-epoch × max epochs × 2** — global 900s default kills slow backends (GB10 at ~0.5× RTX 5070 Ti) mid-Epoch-1 with elapsed ≈ 904s.
- **Evaluator `except Exception`**, not a narrow tuple — narrow tuples miss `httpx.HTTPError`, `HfHubHTTPError`, `OSError`, and any class added by library bumps.
- **Remote shell probes: double quotes inside, ASCII only** — single quotes close the outer `python -c '<probe>'` wrapper; em-dashes get mangled by mismatched remote locale.
- **Silent-inf metrics are always a bug**, never "variant failed" — investigate via `_error_text` in uploaded `metrics.json` before re-firing.

## Project Conventions

Full catalogue with enforcement-test references and script interfaces in `docs/engineering/conventions.md`. Short-form rules below are the ones most likely to trip day-to-day work.

- **Python 3.10 (locked)**: Pinned to `>=3.10,<3.11` in `pyproject.toml` and `.python-version`. Databricks serverless only supports Python 3.10. Run `uv sync` to get a 3.10 venv automatically.
- **Line length**: 120 characters maximum.
- **Imports**: stdlib → third-party → first-party, enforced by isort.
- **Entry points**: Each ingestion module exposes a `main()` function registered in `pyproject.toml`.
- **Delta tables**: All bronze writes include `_ingested_at` audit column with UTC timestamp.
- **Partition overwrite**: Use `replaceWhere` for incremental loads, not full table overwrites.
- **Pre-compile regex at module level**: Never use `re.compile()`, `re.sub()`, or `re.match()` with raw pattern strings inside function bodies or loops. Compile patterns as module-level constants.
- **Serverless env deps are EXACT pins synced to uv.lock** ([ADR-046](docs/superpowers/adrs/ADR-046-serverless-env-exact-pins.md)): every PyPI dep in `terraform/modules/workflows/main.tf` `environment` blocks uses `==` mirroring `uv.lock` — floors let env rebuilds (every wheel bump) install untested versions (prod ran silly-kicks 4.21.0/4.21.1/4.21.2 in one day, none lock-tested). Bumping a library = pyproject + `uv lock` + terraform together; `src/tests/test_terraform_env_dep_parity.py` sentinels enforce `==`-only + lock-parity. Do NOT drop pins that look redundant with the serverless base image — env-version-1 ships numpy 1.23.5/scipy 1.10.0 (a drop silently DOWNGRADES prod).
- **uv silent-downgrade footgun in PEP 723 deps**: uv does NOT fail-fast on conflicting top-level vs wheel-transitive dep pins. The top-level pin wins silently. If a PEP 723 script declares `silly-kicks>=1.0.0,<2.0` and the wheel pulls `silly-kicks>=3.7.0`, uv silently installs silly-kicks 1.0.2 (verified empirically 2026-05-04 — the cancelled SK3-MIG-B Phase 9 cycle 1 ran on poisoned silly-kicks 1.0.2 for 4323 games before OOM, which would have produced silently-broken Champion artifacts had it not been cancelled). This makes explicit pins in PEP 723 scripts an active footgun: prefer letting the wheel's `[spadl]` extra be the single source of truth (trainers install `luxury-lakehouse[spadl] @ ...wheel` which transitively resolves `silly-kicks>=3.7.0,<4`). If a PEP 723 script must pin a project-owned library, its `main()` MUST add a runtime version assertion (see `scripts/train_*_hf.py`'s `_REQUIRED_SK_MIN` check). Sentinel: `src/tests/test_sk3_mig_b_orchestrator_invariants.py` enforces both no-pin and runtime-assertion-constant invariants.
- **HuggingFace Hub**: Org is `luxury-lakehouse`. Model artifacts cached in UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/`. Set `HF_HOME` env var for local cache location. Use `huggingface_hub` for model publish/download (no torch dependency).
- **HF READMEs ride with payload via `ingestion.hf_publish`** ([ADR-014](docs/superpowers/adrs/ADR-014-hf-card-inventory-parity.md)): every publisher that creates or refreshes a HF dataset / model / Space repo MUST call `ingestion.hf_publish.upload_hf_readme(...)` after the data/weight upload, passing a card resolved via `get_hf_card_path(name, kind=...)`. In-repo card filename must equal the HF repo basename (filename == repo basename invariant). Missing cards + orphan cards are caught by `src/tests/test_hf_publish_parity.py`. Orphan-only push paths (org Space, method-model cards) go through `scripts/publish_hf_cards.py --org` / `--orphans` / `--name --kind`. Never re-implement the README push inline in a publisher — the helper is the only path.
- **Restricted-data HF publishing via permanent private companion repos** ([ADR-049](docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md)): providers whose license forbids public redistribution are listed in `ingestion.hf_publish.RESTRICTED_HF_PROVIDERS` (single source of truth). Dataset publishers carrying provider data split rows via `split_restricted(df)` and publish BOTH repos on every run — public `<repo>` and private `restricted_repo_id(repo)` (`<repo>-restricted`, org-members only) — including a sweep-only publish of the restricted repo when the set is empty (the repos are permanent infrastructure; granting a provider permission = one edit to the set, the next publish migrates the partition). Trainers consuming such datasets import the SAME constant: set non-empty → restricted partitions REQUIRED (fail-loud — Champions v10-and-earlier silently trained without GS by inheriting a SQL-side publish filter), set empty → skip with a log line; record both repos' commit hashes in MLflow (`hf_dataset_commit` + `hf_restricted_dataset_commit`). `upload_folder` delete patterns must be `["**"]` — hf_hub matches them RELATIVE to `path_in_repo`, so any `"data/"`-prefixed pattern silently no-ops (left stale Spark part-files in partition dirs for months). Migrated: `publish_spadl_vaep_hf.py` + `publish_action_context_hf.py`; `publish_tracking_context_hf.py` stays legacy-SQL-gated pending deprecation (two-mode guard: `test_gradientsports_hf_exclusion.py`). Lockstep + helper behavior enforced by `src/tests/test_hf_publish.py` (TestRestrictedPublishing) and `test_hf_publish_parity.py`.
- **SPADL post-conversion enrichments live in `src/ingestion/spadl_enrichments.py`** ([ADR-016](docs/superpowers/adrs/ADR-016-spadl-enrichment-stage-canonical-naming.md)): new helpers added to `apply_spadl_enrichments` + a column added to `_SPADL_SCHEMA` + `_VAEP_SCHEMA` + applyInPandas StructTypes (parity-tested via `test_spadl_vaep_writer_parity.py` — closes the LL1 silent-drop class). Provider-native passthroughs use `<provider>_<field>` (e.g. `statsbomb_play_pattern`); computed enrichments use plain canonical names (e.g. `possession_id` for the heuristic from `add_possessions`). Native string identifiers paired with Kimball surrogates use `<entity>_native` (e.g. `team_id_native` for the actual DFL CLU id, `team_key` for the BIGINT surrogate). For IDSSE/Metrica/SkillCorner string identifiers, the legacy BIGINTs `match_id` and `team_id` carry `hash_native_id_to_bigint(native_id)` (deterministic SHA-256[:15]) — `match_id` so `applyInPandas(groupBy match_id)` continues to dispatch per-match groups, `team_id` so VAEP's `fs.team()` and `sameteam` equality comparisons produce correct features. NULL `team_id_native` edge cases (e.g. IDSSE freekick_short) get sentinel hash `UNKNOWN_TEAM_SENTINEL` with a warning log.
- **Bronze migrations under `scripts/migrations/*.sql` are operator-applied — there is NO CI auto-apply.** (The former `dbt-live-ci.yml` "Apply pending bronze migrations" step was removed; verified 2026-06-08 — no workflow references `scripts/migrations/_runner.py`.) Apply each new migration manually **with** the merge via `uv run --extra sdk python scripts/migrations/_runner.py scripts/migrations/<file>.sql` — `dbt-live-ci.yml` is a **daily scheduled** live build, so a merge whose migration is unapplied breaks the next daily build (staging casts a column that doesn't exist yet). `_runner.main()` executes ANY statement, but only single-leading-column `ALTER TABLE ADD COLUMNS` is made idempotent (DESCRIBE skip-if-exists); **every migration MUST still be idempotent by construction** (`UPDATE ... WHERE col IS NULL`, `SET TBLPROPERTIES`, `CREATE TABLE IF NOT EXISTS`). A non-idempotent op (e.g. `RENAME COLUMN`) is a documented run-once. Destructive ops (`DROP`, `DELETE`, `TRUNCATE`) remain operator-driven. Always verify with a live `DESCRIBE`/`SELECT` post-apply.

**Consult `docs/engineering/conventions.md` before touching these areas:**

| Area | Section |
|------|---------|
| `DATABRICKS_HTTP_PATH` double-slash quirk, `ensure_warehouse.py`, `dbt_build_and_refresh.py`, `patch_job_retries.py` ([ADR-025](docs/superpowers/adrs/ADR-025-post-apply-job-retry-patch.md)) | § Databricks Dev Flow |
| `@workflow` decorator, observability schema, mandatory guard injection via `timed_check` | § Workflow Framework |
| Workflow card YAML contracts — phase-parity, `dbt_model:` field, HF Jobs publish mapping, TF block ordering | § Workflow Cards |
| Lakebase synced-table grants + maintenance standard pattern ([ADR-005](docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md)) | § Lakebase Ops |
| dbt model contracts, slim CI, `dbt-owners-{env}` ownership model, system-table definer's-rights views | § dbt Conventions |
| HF Jobs wheel convergence (`bump_wheel.py`, `#sha256=` fragment rule, `src/shared/wheel.py` source of truth) | § HF Jobs Wheel Convergence |
| HTTP caching via `requests-cache` | § HTTP / Networking |
