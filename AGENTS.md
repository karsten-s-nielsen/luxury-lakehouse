# AGENTS.md — Luxury Lakehouse Engineering Standards

Canonical, always-loaded engineering rules for this repo. **Class-1 only:** terse, enforceable invariants a change must not break. The WHY / history / measurement lives on-demand in `docs/context/<domain>.md` and in the ADRs under `docs/superpowers/adrs/`. Every bullet points to its detail. Nested Taipy rules live in `hf_taipy_app/AGENTS.md` (auto-loaded in that subtree).

Anti-bloat gate: `src/tests/test_agents_md_budget.py` (byte budget + per-bullet pointer + per-invariant completeness oracle). Do not inline history back into this file.

## Architecture map

- `src/ingestion/` — provider ingest + bronze writers; entry point: a `main()` per module registered in `pyproject.toml`. → docs/context/conventions.md
- `dbt_project/` — silver/gold transforms + model contracts. → docs/context/architecture.md
- `src/workflows/` — workflow orchestration; stdlib + PyYAML + Pydantic only. → docs/context/architecture.md
- `src/shared/` — stdlib-only constants + `identifiers.py`. → docs/context/architecture.md
- `src/analytics/action_context/` — AC-1 tracking pipeline, drain worker, canary. → docs/context/action-context.md
- `src/evolve/` — code-evolution engine (scoped `exec()`). → docs/context/security.md
- `hf_taipy_app/` — Taipy presentation layer. → hf_taipy_app/AGENTS.md + docs/context/ui-ux.md

## Git & Scope

- **Never commit without explicit user approval.** `git commit` / `git push` / `gh pr create` / `gh pr merge` each require separate, explicit approval — never implied by "tests pass", a plan, or a prior commit. (CONTRIBUTING.md)

## Architecture Principles

- **SOLID / Clean Code / Separation of Concerns:** `src/workflows/` has zero Spark/Taipy imports; `src/shared/` is stdlib-only; direction enforced by `import-linter`. → docs/context/architecture.md
- **Idempotent operations:** Every ingestion task can be re-run safely — partition-level overwrites, not full table drops. → docs/context/architecture.md
- **Structured Logging: JSON-line logs to stdout**, no print; include source name, row counts, timing. → docs/context/architecture.md
- **ML inference outputs follow ADR-013**: Python writer → bronze → dbt staging view → gold mart with `contract: enforced`; surrogate keys resolve in the mart. → docs/context/architecture.md
- **Significant architectural decisions are documented in docs/superpowers/adrs** (Nygard format); see the ADR-when triggers. → docs/context/architecture.md

## Failure Investigation

- **Three-strikes:** once might be a coincidence, twice is suspicious, three times is a pattern — after the first unexpected failure, investigate the root cause. → docs/context/failure-investigation.md
- **Investigate before retrying:** check service state + logs first (a 2-minute REST call beats a 14-minute blind retry). → docs/context/failure-investigation.md
- **Never disappear into long-running commands:** any command over ~30s runs in background, polled every 15-30s with visible progress. → docs/context/failure-investigation.md
- **Report findings before fixes:** present the diagnosis with evidence first — the user decides the approach. → docs/context/failure-investigation.md
- **Answer the specific questions first** — answer THOSE questions directly before exploring anything else. → docs/context/failure-investigation.md
- **"I don't know yet" is acceptable — speculation is not acceptable.** State what you need to check next. → docs/context/failure-investigation.md
- **Reproduce at the exact conditions** — wrong viewport/data/sequence means the investigation is incomplete. → docs/context/failure-investigation.md
- **Never declare a root cause without evidence** of the exact mechanism; otherwise say you have not found it yet. → docs/context/failure-investigation.md

## Security Hardening

- **No secrets in code** — authentication via Databricks runtime or environment variables only. → docs/context/security.md
- **HTTPS only: All HTTP requests must use https**; reject `http://` at the function level. → docs/context/security.md
- **SSL verification: explicit verify=True** on all `requests` calls. → docs/context/security.md
- **Input validation: Regex-validate all user-supplied identifiers** (`^[a-zA-Z_][a-zA-Z0-9_]*$`) to prevent SQL injection. → docs/context/security.md
- **Timeouts: Every HTTP call must have explicit timeouts** `(connect, read)`, default `(10, 30)`. → docs/context/security.md
- **Retry with backoff: Exponential backoff on transient errors** (429, 5xx), max 3 retries. → docs/context/security.md
- **No dangerous builtins** — no `eval()`, `exec()`, `pickle.loads()`, `subprocess` with `shell=True`. → docs/context/security.md
- **Scoped exception:** `exec()` is permitted in `src/evolve/targets/*/evaluator.py` + `remote_worker.py` under ADR-001, gated by `code_evolution=True`. → docs/context/security.md
- **Content validation:** verify schema + non-empty before every Delta write; the writers auto-drop top-level void (NullType) columns. → docs/context/security.md
- **Least privilege: scripts write only to the specified** `{catalog}.{schema}.*`, never arbitrary paths. → docs/context/security.md

## AI Governance

- **AI_GOVERNANCE.md is the living record of EU AI Act posture** — a per-player evaluative ML card change updates §5 + model card + `governance:` YAML + re-runs the test. → docs/context/ai-governance.md
- **ARCHITECTURE.md Appendix D is the living record of academic references** — a new methodology adds its author + extends `expected_authors`. → docs/context/ai-governance.md
- **external-research-tracking.md is the durable record of external research** monitored; quarterly review Jan/Apr/Jul/Oct. → docs/context/ai-governance.md

## Type Safety

- **All Python must pass pyright in basic type checking mode.** → docs/context/conventions.md
- All public function signatures must have type annotations.

## Code Quality

- **Run all seven local checks — all seven, not the first two** (ruff check, ruff format --check, lint-imports, bump_wheel --check, pip_audit_ignores --check, pyright, pytest). → docs/context/code-quality.md
- **NOT pyright src/ — that is NARROWER than CI**; the target includes `hf_taipy_app/src/` + the `sync_tf_env_pins` modules. → docs/context/code-quality.md
- **Ruff Rules Enforced:** E, W, F, I, N, UP, B, S, BLE, RUF. → docs/context/code-quality.md
- **Critical-path functions must have pytest-benchmark tests**; regressions caught in CI. → docs/context/data-performance.md
- **No DataFrame boolean mask filtering inside loops** over tracking/event data (O(n×m)); pre-build indexed lookups. On tracking scale this is Critical. → docs/context/code-quality.md
- **Benchmark with production-scale data** — 100 rows passing but 3M OOM is a false green. → docs/context/data-performance.md
- **No silent exception swallows** (ADR-002): `BLE001` enforced; broad catches need narrowing / `# noqa` with reason / per-file-ignore; telemetry warning-swallow forbidden. → docs/context/code-quality.md
- **Use `tolerate_missing_table` context manager** for bootstrap table-missing; every other exception propagates. → docs/context/code-quality.md
- Any **operational telemetry writer that MERGEs must define its schema as a module-level constant** + lazy StructType factory + a DDL parity test (ADR-002 §4). → docs/context/code-quality.md
- **Hard-fail-first UDF semantics** (ADR-002 §5): closures in distributed executors propagate with the group key; no except-return-empty. → docs/context/code-quality.md
- **Cross-table format contracts** (ADR-018): every JOIN-key ID format has its canonical generator in `src/shared/identifiers.py`; new bronze writer needs a format-contract test. → docs/context/code-quality.md

### Training → Production & HF

- **Every training script that targets the Databricks inference path** imports `ingestion.artifact_deploy` + calls `require_mlflow_env` / `set_and_verify_mlflow_champion` / `upload_weights_to_uc_volume`; secrets via `--secrets` (ADR-012/079). → docs/context/training-and-hf.md
- Training-SP write **grants codified in Terraform because `databricks_permissions` is AUTHORITATIVE** and reverts ad-hoc grants; new model needs experiment resource + ACL + `TRAINING_MODELS` + conformance test (ADR-080/081). → docs/context/training-and-hf.md
- **Any PEP 723 script is single-file** — sibling imports do not work on HF Jobs; inline helpers. → docs/context/training-and-hf.md
- Weight envelopes without native feature names **must inject a top-level feature_names field** (ADR-012 §2); `xg_model_v3` is canonical pre-shot xG. → docs/context/training-and-hf.md
- **HuggingFace org is luxury-lakehouse**; model artifacts cached in UC Volume; `HF_HOME` for local cache. → docs/context/training-and-hf.md
- Every publisher **MUST call `upload_hf_readme` after the data/weight upload** (ADR-014); card filename == repo basename. → docs/context/training-and-hf.md
- Multi-provider HF datasets **publish FLAT per-provider files** + inject the `configs:` block at publish time (ADR-054). → docs/context/training-and-hf.md
- **HF redistribution restriction is PER-MATCH via `access_tier`** (ADR-064/072); publishers use the `prepare_public_upload` / `upload_guarded` seam only; `upload_folder` delete patterns must be `["**"]`. → docs/context/training-and-hf.md

### Action-Context / silly-kicks

- silly-kicks 3.0.1+ **converters require explicit `home_team_start_left`** (ADR-022); derivers in `spadl_adapter.py`; `SILLY_KICKS_ASSERT_INVARIANTS=1`. → docs/context/action-context.md
- **silly-kicks 4.0.0 adds `require_et_direction`** across the 5 per-period-absolute converters (ADR-029); ET derivers + set-piece DAS enrich. → docs/context/action-context.md
- Frame orientation is owned upstream (geometric): the in-repo **`correct_frames_to_home_ltr` net is DELETED** — never reintroduce a lakehouse net or pass `home_start_left` to the builders (ADR-053/034/035). → docs/context/action-context.md
- Legacy **`tracking_context.py` / `fct_tracking_context` pipeline is RETIRED** (PR-1); consumers re-homed onto AC-1. → docs/context/action-context.md
- Gradient Sports **keep-first dedup at the bronze writer** (`_iter_unique_frames`, ADR-030); no adapter/per-feature dedup. → docs/context/action-context.md
- **`_derive_velocities_savgol` is DELETED** — velocity via the silly-kicks preprocess seam; always pass `PreprocessConfig.default()` (ADR-067). → docs/context/action-context.md
- A drain worker swallows a per-unit failure but **`main_drain_worker` MUST call `raise_on_failed_units`** (ADR-067). → docs/context/action-context.md
- **The drain and the sb360 task append per-unit lifecycle events** read back by the completeness gate (ADR-068): `mode="append"`, `run_id={{job.run_id}}`, per-worker tables + UNION, fail-open on events / fail-loud on `slice_completed`. → docs/context/action-context.md
- Drain fast-fails a systemic storm: a **hybrid circuit-breaker trips when `consecutive_failures`** or the failure rate exceeds threshold; preflight runs a dry-run canary before the fan-out (ADR-087). → docs/context/action-context.md
- **The AC planners are a WELDED PAIR** — `_find_tracking_new_period_pairs` + `_find_idsse_new_period_pairs`, both period-grain (ADR-068). → docs/context/action-context.md

## Database Performance

### Lakebase (PostgreSQL) synced tables
- **Index every filtered column on fact tables over 100K rows**; composite leftmost = highest selectivity. → docs/context/data-performance.md
- **Indexes MUST be created WITHOUT the ONLY keyword** (synced tables are internally partitioned). → docs/context/data-performance.md
- **Custom PG indexes are dropped when a synced table is recreated** — `lakebase-grants.yml` reapplies daily; `maintain_synced_tables.py` for manual repair. → docs/context/data-performance.md
- **Avoid SELECT DISTINCT on large tables** — use the recursive-CTE loose index scan. → docs/context/data-performance.md
- Dimension tables under ~50K don't need custom indexes; **Verify with EXPLAIN ANALYZE** after creating indexes. → docs/context/data-performance.md
- **Never dbt --full-refresh a TRIGGERED-synced mart directly** — use `rederive_synced_marts.py` (ADR-043). → docs/context/data-performance.md

### Databricks (PySpark / Delta)
- **Avoid double df.count() before writes** — `write_delta_table` counts the materialized slice post-write when identifiable (ADR-045). → docs/context/data-performance.md
- **Always pass `row_count` to `write_delta_table`** / `merge_delta_table` from `validate_dataframe`. → docs/context/data-performance.md
- **Prefer replaceWhere over bare append** for idempotent writes. → docs/context/data-performance.md
- **`write_delta_table` retries the concurrent-commit conflict class** with jittered backoff (ADR-038); disjoint `replaceWhere` partitions are safe. → docs/context/data-performance.md
- **`.toPandas()` calls must be bounded** (DataFrame-API bound or allowlisted); enforced by `test_topandas_boundedness.py`. → docs/context/data-performance.md
- **Prefer Spark executors over driver-bound processing** — applyInPandas → UC Volume Parquet → last-resort toPandas. → docs/context/data-performance.md
- Liquid clustering + auto-compaction + Predictive Optimization + deletion vectors are **the mart-table defaults**. → docs/context/data-performance.md
- **Databricks Serverless Constraints:** 16 GB driver, 1 GB UDF group cap, no broadcast, no cache/persist, no internet in UDFs, no local FS writes, frozen dataclasses for config. → docs/context/data-performance.md
- Batch compute: factor loop-invariants + broadcast; **verify HF Jobs dataset size against container RAM**; pre-build `dict(iter(df.groupby))`. → docs/context/data-performance.md

### Budgets
- **Ingest tasks 15 min, compute tasks 2 hr**; AC/tracking-marts drains are documented 28800s worker-drain exceptions with a 2700s watchdog (ADR-037/082). → docs/context/data-performance.md
- **UDF group memory 800 MB peak**; batched pitch control ≤5ms/frame; line-breaking ≤2ms/pass; team shape ≤1ms/frame (≤2ms both teams). → docs/context/data-performance.md
- **Before modifying any benchmarked / hot-path function invoke `mad-scientist-skills:measure-before-optimize`.** → docs/context/data-performance.md

## App & UX

- **Every SQL query returning user-facing data must have a LIMIT** — 500 ranking, 2000 timeline. → docs/context/ui-ux.md
- **App page load 3 seconds first load**, 500ms cached interaction. → docs/context/ui-ux.md
- **Never silently substitute data** — surface any fallback/default/NaN-fill with a visual indicator. → docs/context/ui-ux.md
- **Patterns applied to some pages must be applied to all** in the same commit (or a code comment explains the exclusion). → docs/context/ui-ux.md
- **Model selectors need comparison affordance** (side-by-side / delta / persisted previous values). → docs/context/ui-ux.md
- **Navigation labels must be goal-oriented**, not implementation ("Defensive Impact" not "Def. Pressure"). → docs/context/ui-ux.md
- **Raw IDs must never reach the user** — join `player_id`/`match_id`/`team_id` to dimension tables. → docs/context/ui-ux.md
- **Computed metrics must show scale and direction** (range + direction in axis/title/tooltip/caption). → docs/context/ui-ux.md
- **HF artifact link completeness** — publishing a new HF artifact updates every reference location. → docs/context/ui-ux.md

## Orchestration

- **`resolve_hf_token()` is the ONLY sanctioned resolver** for HF tokens under `src/` (ADR-072); AST-gated. → docs/context/orchestration.md
- **Smoke tests must exercise whoami auth** (`HfApi().whoami()`), not just imports. → docs/context/orchestration.md
- **Post-deploy entrypoint verify is mandatory** — re-run the exact worker import chain. → docs/context/orchestration.md
- **Per-backend timeout equals per-epoch times max epochs times 2** — the global 900s default kills slow backends. → docs/context/orchestration.md
- **Evaluator except Exception, not a narrow tuple** (narrow tuples miss httpx/HfHubHTTPError/OSError). → docs/context/orchestration.md
- **Remote shell probes: double quotes inside, ASCII only.** → docs/context/orchestration.md
- **Silent-inf metrics are always a bug**, never "variant failed" — investigate `_error_text` first. → docs/context/orchestration.md

## Project Conventions

- **Python 3.10 locked** (`>=3.10,<3.11`); Databricks serverless only supports 3.10. → docs/context/conventions.md
- Line length: 120 characters maximum.
- Imports: **stdlib, third-party, first-party, enforced by isort**. → docs/context/conventions.md
- **Each ingestion module exposes a `main()` function** registered in `pyproject.toml`. → docs/context/conventions.md
- All **bronze writes include `_ingested_at` audit column** with a UTC timestamp; use `replaceWhere` for incremental loads. → docs/context/conventions.md
- **Pre-compile regex at module level** — never `re.compile`/`sub`/`match` with raw strings in function bodies or loops. → docs/context/conventions.md
- **Serverless env deps are EXACT pins synced to uv.lock** (ADR-046) via `sync_tf_env_pins.py`; CI dbt pins ride the same lockstep. → docs/context/conventions.md
- **uv does NOT fail-fast on conflicting top-level vs wheel-transitive dep pins** — the top-level pin wins silently; PEP 723 scripts add a runtime version assertion. → docs/context/conventions.md
- **SPADL post-conversion enrichments live in `spadl_enrichments.py`** (ADR-016); `_SPADL_SCHEMA`/`_VAEP_SCHEMA` parity-tested; `<provider>_<field>` / `<entity>_native` naming. → docs/context/conventions.md
- **A driver-bound sub-operation gets its OWN task** (ADR-074); `hf_sync` is export-only; `MemoryHook` reports driver memory for every workflow. → docs/context/conventions.md
- **Medallion layer schemas are NAMED** (`DEFAULT_{BRONZE,SILVER,GOLD}_SCHEMA`, ADR-073); watermark upstreams must be tables; `hf_sync` calls `raise_on_failed_sub_workflows`. → docs/context/conventions.md
- **Bronze migrations are operator-applied; there is NO CI auto-apply** — run `scripts/migrations/_runner.py`; every migration idempotent by construction. → docs/context/conventions.md
- **Consult `docs/engineering/conventions.md` before touching these areas** (Databricks dev flow, workflow framework, cards, Lakebase ops, dbt, HF Jobs wheel, HTTP caching). → docs/context/conventions.md
