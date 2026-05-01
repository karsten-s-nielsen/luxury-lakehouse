# ADR-002: Silent Exception Swallow Elimination

| Field | Value |
|---|---|
| **Date** | 2026-04-15 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (human), Claude Opus 4.6 (AI) |

## Context

In session 40 (2026-04-14), three instances of the same anti-pattern — silent `except Exception:` swallows — were each found to have masked a real data-integrity defect for an extended period:

1. **`_make_scoring_udf` (VAEP scoring, `src/ingestion/spadl_vaep.py`)** — `except Exception: pass  # noqa: S110` silently dropped failed VAEP-scoring games inside a Spark `applyInPandas` closure. The daily job reported SUCCEEDED with zero rows written to `bronze.vaep_action_values`. Initially misdiagnosed as a `_flatten_extra()` bug because the per-group failures were invisible at aggregate level.
2. **`CostEstimateHook._merge` (`src/ingestion/cost_hook.py`)** — `except Exception: logger.warning(...)` in four hook methods silently hid a `DELTA_MERGE_UNRESOLVED_EXPRESSION` error for **62+ hours**. The error fired on every hook call because a live Delta table had an orphaned `task_key` column that PR #115 had removed from the canonical schema but never dropped from the live table. `whenMatchedUpdateAll()` validates target columns at parse time; the source DataFrame was missing `task_key`; every MERGE raised. The `assert_warm_tier_not_empty` dbt test only started firing when the D59 cycle wired `dbt_build` into the daily job, three days after the breakage started.
3. **`_make_sb_spadl_udf` + `_make_ws_spadl_udf` (`src/ingestion/spadl_conversion.py`)** — `except Exception: return _pd.DataFrame(columns=_spadl_cols)` inside Spark `applyInPandas` closures for both providers. Probably hiding per-match silly-kicks conversion failures that were the real cause of the D57 goal-encoding symptom the cycle started with; status of the underlying silly_kicks calls is unknown because failures have been invisible for an unknown duration.

Each swallow looked defensive and reasonable in isolation. **Together they created a compounding data-integrity failure where the ground truth about what the pipelines were doing was invisible.** A repo-wide audit found 55+ instances of `except Exception:` in `src/` alone, 40+ in `scripts/`, and 24 in `hf_taipy_app/`.

CLAUDE.md previously had no rule prohibiting silent swallows. The ruff `S110` rule (try-except-pass) catches bare `pass` but not `logger.warning(...)`, `return empty_df`, or `return {}` patterns — the majority of dangerous cases. Nothing forced justification of broad catches.

## Decision

Eliminate silent exception swallows as a class of defect by combining four conventions — a lint rule, a cross-cutting helper, a schema-drift guard pattern, and an error-propagation semantic — into the durable project policy. Default telemetry exception handling must be **raise, typed error return, or ERROR-level observable log** — never silent, never at WARNING.

### 1. Default exception-handling policy (CLAUDE.md rule)

Telemetry code, callback/hook code, UDF closures, and defensive fallback code must use one of three exception-handling patterns:

1. **Propagate** — re-raise with context. For per-group/per-game UDF failures, always include the group key in the error message: `raise RuntimeError(f"... failed for game_id={game_id}") from exc`.
2. **Typed error return** — return a structured value (dataclass, enum, sentinel) that the caller must destructure.
3. **Observable log + typed error return** — log at **ERROR** level (not warning, not debug) AND return a typed error value. Only legitimate for fire-and-forget telemetry that must not crash the calling pipeline.

Forbidden patterns:

- `except Exception: pass`
- `except Exception: logger.warning(...)` / `logger.debug(...)` in fire-and-forget paths
- `except Exception: return empty_df` inside a UDF or distributed-executor closure
- `except Exception: use_fallback_value()` without a metric/log/flag making the fallback visible

Warning-level logs are structurally equivalent to no logging at all because error-log queries filter by level and on-call alerting fires on ERROR not WARNING.

### 2. `ruff BLE001` enforcement + justification convention

`BLE001` (flake8-blind-except) is enabled in `pyproject.toml`. New `except Exception:` sites require ONE of:

- **Narrowing** to a specific exception class (preferred)
- **Line-level `# noqa: BLE001 — <reason>`** with an explicit architectural reason
- **Per-file-ignores entry** in `pyproject.toml` with a one-line justification

Line-level `noqa` is preferred over file-level ignores because it forces per-site justification and is visible in diffs. File-level ignores are only acceptable when every site in the file shares the same architectural reason (e.g. `hf_taipy_app/src/state/**` — every catch in Taipy state modules is a UI event handler that must not crash the page).

### 3. `tolerate_missing_table` cross-cutting helper

Bootstrap code that queries a results table which may not exist on first run must use the `ingestion.utils.tolerate_missing_table(logger, msg)` context manager:

```python
from ingestion.utils import tolerate_missing_table

existing: set[int] = set()
with tolerate_missing_table(logger, f"Table {full_table} not found — starting fresh"):
    existing = {row[0] for row in spark.read.table(full_table).collect()}
return existing
```

The helper suppresses ONLY Spark errors whose message matches one of six specific table-missing markers:

- `TABLE_OR_VIEW_NOT_FOUND` (Spark 3.4+ error class)
- `Table or view not found` (older Spark message)
- `Path does not exist` (Delta table path not found)
- `DELTA_MISSING_DELTA_TABLE`
- `DELTA_TABLE_NOT_FOUND`
- `TableNotFoundException` (Unity Catalog)

Every other exception propagates — including the exact `DELTA_MERGE_UNRESOLVED_EXPRESSION` schema-drift error that started this ADR. Test coverage (`src/tests/test_utils.py::TestTolerateMissingTable`) includes an explicit regression guard asserting that the warm-tier blocker exception propagates and is NOT suppressed.

**Narrowing by error-message markers, not by exception class**, is the deliberate decision. The concrete exception class varies across classic PySpark, Spark Connect, Delta Lake, and Unity Catalog. An error-message check is portable across all four runtimes.

### 4. Writer/target schema drift guard pattern

For every operational telemetry writer that MERGEs into a Delta table via `whenMatchedUpdateAll()`, the writer's schema must be a module-level constant with a factory function, and a regression test must parse the canonical DDL and assert column-list equality.

**Reference implementation**: `src/ingestion/cost_hook.py`.

- `_COST_LIVE_COLUMNS: list[tuple[str, str, bool]]` is the single source of truth for the 16 canonical columns.
- `_build_cost_live_schema() -> StructType` is a factory that lazy-imports pyspark and converts the tuple list to a `StructType`.
- `CostEstimateHook._merge` uses `_build_cost_live_schema()` instead of an inline literal.
- `src/tests/test_cost_hook.py::TestCostHookSchemaDriftGuard` parses `scripts/create_cost_table.sql` and asserts set equality, order equality, and that orphan columns (`task_key`, `job_run_id`) stay out.
- `src/tests/test_cost_hook_integration.py` provides an end-to-end Spark MERGE round-trip against a real temp Delta table, auto-skipping when local Spark is unavailable.

**Confirming application — DEFCON `valued_schema` (2026-04-27, PR-6-followup):** the same pattern is applied to `ingestion.defcon_lite_360._RESULTS_SCHEMA` (the bronze DDL string) vs the in-function `valued_schema` `StructType` used by `applyInPandas`. `src/tests/test_defcon_schema_parity.py` parses `_RESULTS_SCHEMA` and asserts column-name + Spark-type equality with the StructType for both `defcon_lite_360` and `defcon_lite_tracking`, and asserts the two modules' DDL strings agree. Four production failures during the PR-6 cycle (StringType vs int64 `action_player_id`, DoubleType vs FLOAT `defcon_value`, INT casts overflowing on 64-bit synthetic IDs) all trace to drift this guard now catches at CI time.

Future writers to new telemetry tables must follow the same pattern: canonical schema constant, factory function, drift guard test, optional Spark integration test.

### 6. Full-overwrite writer schema parity (added 2026-05-01)

PR-Cycle-B (2026-05-01) discovered the same writer/target schema drift class
on a writer that uses **full overwrite** instead of MERGE: `extract_tracking_metadata`
overwrote `bronze.tracking_player_metadata` without including the `is_anonymized`
column added by the 2026-04-24 migration. Every daily-job run silently dropped
the migration-added column; scheduled `Bronze Live Schema` CI checks failed
nightly with `is_anonymized absent from bronze.tracking_player_metadata`.

§4 above scopes to MERGE writers. The same drift class affects writers that
use:

- `mode="overwrite"` (Delta full-replace)
- `replaceWhere` (partition-level overwrite)
- Any pattern where the writer's row-dict / DataFrame schema fully replaces
  the live table's schema instead of being merged into it

For these writers, the rule is: **the writer's schema constant MUST list
every column that exists in the live table, including columns added by
migrations**. When a migration adds a column, the writer's schema constant
must be updated in the same PR. A pytest parity test asserts:

1. The schema constant (DDL string or column list) declares the column.
2. Every emitted row dict includes the column as a key.

**Reference implementation**: `src/ingestion/tracking_metadata.py` —
`_RESULTS_SCHEMA` declares `is_anonymized BOOLEAN`; both `_extract_idsse_metadata`
and `_extract_skillcorner_metadata` emit `"is_anonymized": False` on every
row. `src/tests/test_tracking_metadata_schema.py` asserts both at PR-CI time.

**Why an explicit §6 instead of expanding §4**: MERGE-writer drift fails LOUDLY
(`DELTA_MERGE_UNRESOLVED_EXPRESSION` on every call), while overwrite-writer
drift fails SILENTLY (column quietly dropped, downstream `INNER JOIN` returns
NULL or empty). The two failure modes have different telemetry and need
different framings — both require the same parity test, but operators
diagnose them differently.

### 5. Hard-fail-first UDF semantics

Inside any closure passed to a distributed executor API (`applyInPandas`, `mapInPandas`, `map_partitions`, `@ray.remote`, `dask.delayed`, etc.), exceptions must **propagate with the group key in the error message**. No `except Exception: return pd.DataFrame(columns=...)` or `return []` patterns.

```python
def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
    match_id = int(pdf["match_id"].iloc[0])
    try:
        actions, _report = _spadl_sb.convert_to_actions(pdf, home_team_id)
    except Exception as exc:
        msg = f"StatsBomb SPADL conversion failed for match_id={match_id}"
        raise RuntimeError(msg) from exc
    return actions
```

The group key is critical — without it, driver-side error messages don't tell the operator which match/game/partition failed, and the failure is effectively invisible. Test coverage for the UDF factories must assert both (a) a specific exception propagates and (b) the error message contains the group-key context.

### Scope

This ADR applies to `src/`, `scripts/`, and `hf_taipy_app/` Python code under the `feat/gold-data-repair` branch and forward. `src/evolve/` retains its own architectural exception (ADR-001) — evolve backends use typed `fail_metrics()` returns on broad catches, which is an acceptable "typed error return" pattern under rule (2) above. `dbt_project/` Python hooks, Terraform, and CI workflow code are out of scope for this ADR but should be audited in a future cycle if the same pattern is found there.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **A.** Narrow to specific Spark exception classes (e.g., `except pyspark.errors.AnalysisException`) | Type-safe; IDE autocomplete; idiomatic Python | Concrete class varies across classic PySpark (`pyspark.errors.AnalysisException`), Spark Connect (`pyspark.errors.exceptions.connect.AnalysisException`), Py4J-wrapped (`py4j.protocol.Py4JJavaError`), and Delta-specific subclasses; bugs hit classes that local dev never sees | Class-based narrowing breaks across runtimes. Error-message markers work uniformly across all four. |
| **B.** Per-site `try/except` with explicit class tuples (e.g., `except (AnalysisException, Py4JJavaError):`) | Simple to read inline; no shared helper | Duplicates exception-list across 25+ sites; future runtime change requires 25+ edits; no single-test-regression-guard benefit; sites drift to suppress other errors | Fragile under maintenance. A single helper with one test suite is easier to keep aligned. |
| **C.** File-level `# noqa: BLE001` on every silent-swallow site | Minimal edits; keeps existing error handling intact | Leaves the landmines in place; next schema migration or SDK change still masked; defeats the purpose of the cleanup | Cosmetic fix, not a real remediation. |
| **D.** Require `raise` in all catches, no observable-log option | Strongest visibility guarantee; zero fire-and-forget code | Some paths legitimately must not crash (cost telemetry, background daemons, Taipy UI event handlers, health checks) | Over-broad. "Log at ERROR + typed error return" preserves fire-and-forget while keeping failures visible. |
| **E. CHOSEN.** Four-part policy: BLE001 lint + `tolerate_missing_table` helper + schema-drift guard + hard-fail UDF semantics | Runtime-portable narrowing; single source of truth; regression-guarded at CI time; per-site justification via line-level noqa; clear ADR-backed policy | One-time remediation cost (25+ sites); future contributors must learn the conventions; error-message markers have a small theoretical false-positive risk | — |

## Consequences

### Positive

- **Invisible data-integrity bugs become visible.** The class of defect that caused the 2026-04-12 warm-tier blocker (62+ hours of silent failure) and the D57 goal-encoding symptom cannot recur without being flagged.
- **Single source of truth for telemetry table schemas.** The schema-drift guard test prevents the next version of the warm-tier blocker at CI time.
- **Shared helper with single test suite.** 25+ sites that previously had independent defensive catches now share one well-tested context manager. Future runtime changes require one edit to the helper + one test update.
- **Line-level noqa forces per-site justification.** New broad catches are visible in diffs and require architectural reasoning, not silence.
- **Hard-fail UDFs surface per-group errors with context.** Operators can identify which match/game/partition failed from the driver-side error message, not by querying aggregate row counts later.
- **Production signal verified.** Daily job run `641288498990290` (triggered during destructive-ops phase 1) shipped 28/28 tasks SUCCESS, zero failures.

### Negative

- **25+ sites required narrowing during the remediation cycle.** Significant one-time engineering cost. Mitigated by mechanical rewrites — most sites followed identical patterns.
- **Future contributors must learn and follow the `tolerate_missing_table` convention.** Mitigated by CLAUDE.md rule + ruff BLE001 enforcement — contributors cannot accidentally introduce a bare `except Exception:` in bootstrap code without at least a noqa justification.
- **Schema-drift guard test requires maintenance when the canonical DDL changes.** Each schema edit now requires updating `_COST_LIVE_COLUMNS` AND the DDL file in lockstep. This is the intended behavior — the test forces the two sources of truth to stay aligned.
- **Error-message marker matching in `tolerate_missing_table` is susceptible to false positives** if a non-Spark exception happens to contain one of the six markers in its message. Judged implausible in practice (markers are Delta/Spark-specific strings, not English phrases) and acceptable in exchange for runtime-portable narrowing. Regression test coverage asserts the precise `DELTA_MERGE_UNRESOLVED_EXPRESSION` string propagates correctly.

### Neutral

- ADR-002 is a policy ADR, not a code-change ADR — the actual remediation ships in 5 code commits (`e0c4360` through `0c132ca`) and this ADR documents the durable position those commits codify.
- The silent-swallow policy applies forward, not retroactively: existing per-file-ignores in `pyproject.toml` document the architectural exceptions that were accepted during the cleanup cycle. Future exceptions require justification.

## CLAUDE.md Amendment

CLAUDE.md's "Code Quality" section gains new rules referencing this ADR. The amendment was applied during the same cycle's final-review phase:

- The Ruff Rules Enforced table gains a `BLE` row.
- A new bullet "No silent exception swallows" documents the forbidden patterns and references this ADR.
- A new bullet "Table-missing helper" documents the `tolerate_missing_table` convention and references §3 of this ADR.
- A new bullet "Writer/target schema drift guard" documents the `_COST_LIVE_COLUMNS` pattern and references §4 of this ADR.
- A new bullet "Hard-fail-first UDF semantics" documents the group-key error-propagation requirement and references §5 of this ADR.

## Related

- **Commits:** `e0c4360` (SPADL integrity), `52a5cf8` (warm-tier hook), `9ad7e4f` (systemic src/ remediation + BLE001), `3b02e1c` (scripts/ + hf_taipy_app/), `0c132ca` (docs)
- **Specs:** [`docs/superpowers/specs/2026-04-14-gold-data-repair-design.md`](../specs/2026-04-14-gold-data-repair-design.md)
- **Plans:** [`docs/superpowers/plans/2026-04-14-gold-data-repair.md`](../plans/2026-04-14-gold-data-repair.md)
- **Memory entries:** `~/.claude/projects/.../memory/feedback_no_silent_swallows.md` (the durable rule, with worked examples and enforcement); `memory/project_spadl_vaep_chain.md`, `memory/project_warm_tier_blocker.md` (investigation notes)
- **ADRs:** builds on [ADR-001](ADR-001-evolve-code-execution.md) (evolve code execution — defines the "typed error return" pattern that §1 rule 2 references)
- **External references:**
  - [Delta Lake `MERGE` documentation](https://docs.databricks.com/aws/en/delta/merge) — documents `whenMatchedUpdateAll()` parse-time validation
  - [ruff BLE001 rule](https://docs.astral.sh/ruff/rules/blind-except/)
  - `mad-scientist-skills` v1.17.0 — `architecture-audit` + `observability-audit` Phase 0 anti-pattern additions derived from this cycle (sibling repo, uncommitted)

## Notes

- **Production signal from destructive-ops phase 1**: The `soccer-analytics-ingestion-dev` daily job run `641288498990290` triggered after the `ALTER TABLE DROP COLUMN task_key` shipped 28/28 tasks SUCCESS with the pre-Commit-1 wheel and the post-ALTER table. This confirms the schema-drift fix works end-to-end in production with the existing deployed wheel, before the new code even ships.
- **Why this ADR is "Accepted" not "Proposed"**: the remediation has already shipped across 5 commits on `feat/gold-data-repair`, and all quality gates pass. The ADR codifies a decision that has already been made and verified — not one that is pending review.
