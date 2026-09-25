# Context — Architecture & ADRs

On-demand detail for the Architecture rules in `AGENTS.md`. Governing ADRs: [ADR-013](../superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md), the ADR template under `docs/superpowers/adrs/`.

## Architecture Principles

- **SOLID**: Single responsibility per module/function. Depend on abstractions.
- **Clean Code**: Meaningful names, small functions, no dead code.
- **Separation of Concerns**: Ingestion, transformation (dbt), workflow orchestration (`src/workflows/`), shared constants (`src/shared/`), and presentation (Taipy) are fully isolated layers. `src/workflows/` has zero Spark/Taipy imports — only stdlib + PyYAML + Pydantic. `src/shared/` has zero external dependencies — stdlib only. Dependency direction enforced by `import-linter` in CI.
- **Idempotent Operations**: Every ingestion task can be re-run safely. Use partition-level overwrites, not full table drops.
- **Structured Logging**: JSON-line logs to stdout. No print statements. Include source name, row counts, and timing.
- **ML inference outputs follow [ADR-013](../superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)**: Python writer → bronze raw → dbt staging view → gold mart with `contract: enforced: true`. Surrogate keys resolve in the mart via `INNER JOIN fct_shots ON shot_id` (or equivalent identity fact). Python writers emit only native identifiers + predictions. First applied in PR 3 (xG v2 promotion); PR 7 extends to `fct_pausa_values`.

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
