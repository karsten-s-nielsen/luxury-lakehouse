# Context — Code Quality

On-demand detail for the Code Quality rules in `AGENTS.md`. Governing ADRs: [ADR-002](../superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md) (exception discipline), [ADR-018](../superpowers/adrs/ADR-018-cross-table-format-contract-testing.md) (cross-table contracts).

## The seven checks

All code must pass these checks with zero violations:

```bash
uv run ruff check src/ scripts/                    # Lint (E, W, F, I, N, UP, B, S, BLE, RUF)
uv run ruff format --check src/ scripts/           # Format check (CI enforced)
uv run lint-imports                                # Layer isolation (see Separation of Concerns)
uv run python scripts/bump_wheel.py --check        # Wheel version consistency across ~30 consumers
uv run python scripts/pip_audit_ignores.py --check # .pip-audit-ignores.yml schema completeness
uv run pytest src/tests/ -v                        # Unit tests

# Type check. NOT `pyright src/` — that is NARROWER than CI and passes on code CI rejects.
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
```

All seven, not the first two. `lint-imports`, `bump_wheel --check` and `pip_audit_ignores --check` are enforced in CI (`python-ci.yml:95`, `:117`, `:181`) and were absent from this list until 2026-08-11 — a local run passing the four documented checks could still fail the pipeline on the three that were not. The pyright target was wrong in the same way, and worse: `pyright src/` omits `hf_taipy_app/src/` and the two `sync_tf_env_pins` modules that `python-ci.yml:92` checks, so a type error in the Taipy app or the ADR-046 pin tooling was invisible locally by construction.

## No DataFrame boolean mask filtering inside loops

Never use `df[df["col"] == val]` inside a `for` loop over tracking or event data. This is O(n×m) — a hidden nested loop that causes pipeline timeouts on production-scale data (3M+ rows). Pre-build indexed lookups: `dict(iter(df.groupby("key")))`, `df.set_index("key")`, or use a merge/join. On tracking-scale data, this is always Critical severity, never Minor.

## Ruff Rules Enforced

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

## Exception discipline (ADR-002)

- **No silent exception swallows** ([ADR-002](../superpowers/adrs/ADR-002-silent-exception-swallow-elimination.md)): `BLE001` is enforced. New broad catches (`except Exception:`) require either (a) narrowing to a specific exception class, (b) a line-level `# noqa: BLE001 — <reason>` comment with an explicit architectural justification, or (c) a per-file-ignores entry in `pyproject.toml` with a one-line explanation. Silent-swallow telemetry code (`except Exception: logger.warning(...)`) is specifically forbidden — warning-level logs are invisible in error-log queries, which hid the 2026-04-12 warm-tier cost-hook blocker for 62+ hours. Default telemetry exception handling must be one of: raise, typed error return, or **ERROR-level** log.
- **Table-missing helper** (ADR-002 §3): Use `ingestion.utils.tolerate_missing_table(logger, msg)` context manager for bootstrap code that queries a results table which may not exist on first run. The helper suppresses ONLY Spark errors matching specific table-missing markers (`TABLE_OR_VIEW_NOT_FOUND`, `Table or view not found`, `Path does not exist`, `DELTA_MISSING_DELTA_TABLE`, `DELTA_TABLE_NOT_FOUND`, `TableNotFoundException`). Every other exception propagates — including the `DELTA_MERGE_UNRESOLVED_EXPRESSION` schema-drift errors that bare `except Exception:` patterns previously hid. Never reinvent this pattern — import the helper.
- **Writer/target schema drift guard** (ADR-002 §4): Any operational telemetry writer that MERGEs into a Delta table via `whenMatchedUpdateAll()` must (a) define its schema as a module-level constant (e.g. `_COST_LIVE_COLUMNS` in `src/ingestion/cost_hook.py`), (b) provide a lazy factory function that converts the constant to a Spark `StructType`, and (c) have a pytest that parses the canonical `CREATE TABLE` DDL and asserts column-list equality. Without these, schema drift between code and live table silently fails every MERGE with `DELTA_MERGE_UNRESOLVED_EXPRESSION`.
- **Hard-fail-first UDF semantics** (ADR-002 §5): Inside any closure passed to a distributed executor (`applyInPandas`, `mapInPandas`, `@ray.remote`, etc.), exceptions must propagate with the group key in the error message: `raise RuntimeError(f"... failed for <key>={value}") from exc`. No `except Exception: return empty_df` patterns — those silently drop per-group data.

## Cross-table format contracts (ADR-018)

Every native ID format used as a JOIN key has its canonical generator in `src/shared/identifiers.py`. Bronze writers + applyInPandas UDFs import from this module; dbt singular tests (`assert_<source>_<entity>_native_join_resolves.sql`) assert JOIN-coverage from `bronze.spadl_actions` to `dim_*`. Adding a new bronze writer / dim staging touchpoint REQUIRES adding the corresponding format-contract test in the same PR. silly-kicks API drift caught at OUR boundary via `src/tests/test_silly_kicks_boundary.py`.
