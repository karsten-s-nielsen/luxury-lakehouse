# Bronze Drop-Safety Sweep (G1–G6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Execution amendment (user decision, pre-execution):** originally scoped as two PRs (A = safety teeth, B = tail cleanup); combined to a **single branch `feat/bronze-drop-safety-sweep` → single squash-worthy commit → single PR** covering all six gaps. Task bodies below are unchanged; the two commit-and-push sections (A4 + B9) collapse into one final PAUSE gate. Wheel bump moves from PR B's tail into the unified E2E gauntlet. See also `project_drop_safety_sweep_in_flight.md` memory.

**Goal:** Close the six audit gaps (G1–G6) from the PR #173 post-merge bronze-safety audit, so that bronze and staging are guaranteed not to silently drop source values across the five known failure modes (writer drop, staging drop, unguarded cast, filter drop, silent sentinel). Ship before Kimball PR 3 (Shots + xG).

**Architecture:** The sweep maps six gaps onto the five failure-mode taxonomy established in the audit. Three High-severity gaps actually catch live drops (Modes 1/3/4); three Medium-severity gaps close out writer-level guard extension, nullable boolean discipline, and documentation drift. Each gap gets a focused test that is the success criterion; fixes follow the test. TDD where the change is code, schema-inspection-first where the change is SQL/YAML.

**Tech Stack:** Python 3.10, pandas, PySpark, `finalize_bronze_df` helper (`src/ingestion/utils.py:503-558`), dbt (staging models + `_models.yml`), pytest + `pytest.importorskip("databricks.sql")` + `@requires_databricks` gating, `src/tests/coverage_utils.py` helpers, GitHub Actions (new nightly + post-merge-to-main workflow for live-DB tests).

---

## Context

### Verified pre-conditions (2026-04-22)

- Main branch head: `7dd5a9b` (Kimball PR 2 merged).
- Working tree: clean.
- `finalize_bronze_df(df, expected_cols, dtype_overrides=None)` lives at `src/ingestion/utils.py:503-558` and is already used by: `idsse.py` (2 spots), `metrica_events.py` (2), `metrica_tracking.py` (2), `skillcorner.py` (1). **Not yet used by:** `statsbomb.py` (3 spots: lines 140, 291, 435) and `wyscout.py` (3 spots: lines 255, 298, 513). That's 7/13 bronze-source writers = ~54% — matches the audit's "~60%" estimate.
- `PROVIDER_COVERAGE` in `src/tests/test_staging_coverage.py:52-76` covers 11 (provider, bronze_table) pairs. `INITIAL_BRONZE_STAGING_GAPS` (`test_staging_coverage.py:108-383`) has a non-empty entry for all 11 pairs (memory's "9 pairs" was stale; plan uses 11).
- `databricks-sql-connector` is **NOT** installed in CI — `pyproject.toml` has no match for `databricks-sql|databricks\.sql`. `test_bronze_live_schema.py:55` (`pytest.importorskip("databricks.sql")`) therefore skips the module in CI for TWO reasons (import skip + env gate), not one. G6 fix needs both pip install + env injection.
- `test_bronze_live_schema.py:224` cited for the Wyscout skip decorator: decorator is actually lines 225-232 (224 is `@requires_databricks`). Trivial.
- Five filter-drop sites (G3) all confirmed exact: `stg_idsse__events.sql:346`, `stg_idsse__tracking.sql:54`, `stg_metrica__tracking.sql:134`, `stg_skillcorner__tracking.sql:62`, `stg_wyscout__players.sql:57`.
- G4 site confirmed exact: `stg_idsse__passes.sql:88` `coalesce(play_flat_cross, 'false') = 'true'`. Line 91 `false as is_progressive` is out of G4 scope (tracked as TODO #33).
- G2 site confirmed exact: `wyscout.py:131` `pd.to_numeric(df[col], errors="coerce")` inside `_normalize_mixed_types` — trips only when first non-null element is `int | float`.

### Constraints (from user memory + CLAUDE.md)

- **No commits / PRs / pushes without explicit approval.** Each of those is a separate approval gate. `git push` and `gh pr create` are gated.
- **Single-commit-per-branch convention.** Accumulate work locally, then squash to one commit per PR at the end. Do NOT create multiple intermediate commits.
- **TDD where code changes.** Test first, implementation second, re-run to confirm green.
- **Bash >30s must be `run_in_background: true`** (enforced by PreToolUse hook). Local dbt + pytest runs should either use explicit short timeout or run in background with polling.
- **BLE / ADR-002:** No silent exception swallows. Any coerce/filter audit test uses typed exceptions or ERROR-level logs, never warnings.
- **Bronze is the truth layer** — fixes surface at staging/mart, not by trimming bronze.
- **No `#sha256=` on UC Volume paths.** Wheel bump via `scripts/bump_wheel.py` only; Terraform consumers get version-only, never hash.
- **Wheel bump rule:** PR B touches `src/ingestion/*` (statsbomb.py, wyscout.py) so requires wheel bump to 0.3.10 + `bump_wheel.py --check` green. PR A touches only tests + workflows + SQL + YAML — no bump needed. (Confirm current wheel version at `src/shared/wheel.py:WHEEL_VERSION` during PR B execution.)

### Out of scope

- TODO #32 (Metrica pseudo-competition) and TODO #33 (IDSSE `is_progressive` classifier) — surfacing/enrichment gaps, not drop-safety gaps. Tracked separately.
- Bronze re-ingestion of StatsBomb / Wyscout. G1 adds the guard for *future* ingestion cycles; existing live bronze is already wide enough per the audit. No bronze schema change is forced by this sweep.

---

## File Structure

### PR A — Safety teeth

**Create:**
- `.github/workflows/bronze-live-schema.yml` — new nightly + post-merge-to-main workflow that installs `databricks-sql-connector` + injects real Databricks secrets, then runs `test_bronze_live_schema.py` and `test_staging_rowcount_vs_bronze.py`.
- `src/tests/test_wyscout_coerce_audit.py` — synthetic-fixture test asserting `pd.to_numeric(..., errors="coerce")` does not silently mutate non-null string values to NaN in Wyscout ingestion.
- `src/tests/test_staging_rowcount_vs_bronze.py` — live-DB parity test for the five `WHERE col IS NOT NULL` filter sites, asserts `count(staging) + count(bronze where filter-cond NULL) == count(bronze)` per pair.

**Modify:**
- `src/ingestion/wyscout.py` — change `_normalize_mixed_types(df)` (lines 115-136) to accept an optional `logger` parameter and emit an ERROR-level log when `pd.to_numeric(errors="coerce")` silently converts any non-null string to NaN. **No behavior change at default call site** — logging only.

### PR B — Tail cleanup

**Modify:**
- `src/ingestion/statsbomb.py` — apply `finalize_bronze_df(...)` at lines 140 / 291 / 435; define module-level `_STATSBOMB_COMPETITIONS_EXPECTED_COLS`, `_STATSBOMB_MATCHES_EXPECTED_COLS`, and loader for the per-table snapshot at `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json`.
- `src/ingestion/wyscout.py` — apply `finalize_bronze_df(...)` at lines 255 / 298 / 513; define module-level expected-col constants; load from `src/tests/fixtures/wyscout_bronze_schema_snapshot.json`.
- `src/tests/test_bronze_live_schema.py` — remove Wyscout `@pytest.mark.skip` (lines 225-232); remove StatsBomb exclusion (line 37 comment); add `test_wyscout_events_live_schema_covers_parser` (unskipped) + `test_statsbomb_events_live_schema_covers_parser` + `test_statsbomb_matches_live_schema_covers_parser`.
- `dbt_project/models/staging/idsse/stg_idsse__passes.sql` — replace line 88 `coalesce(...)` with nullable `case` expression for `is_cross`.
- `dbt_project/models/staging/idsse/_idsse__models.yml` — update `is_cross` data_type to nullable boolean (if contracted).
- `dbt_project/models/staging/{idsse,skillcorner,metrica,statsbomb,wyscout}/_<provider>__models.yml` — expand `columns:` entries on each staging model to match actual SELECT output, for every pair in `INITIAL_BRONZE_STAGING_GAPS`.
- `src/tests/test_staging_coverage.py` — shrink `INITIAL_BRONZE_STAGING_GAPS` to `{}` (or drop the constant entirely and inline `{}` per pair).
- `pyproject.toml` — bump `version` to `0.3.10`.
- `src/shared/wheel.py` — bump `WHEEL_VERSION`; run `scripts/bump_wheel.py` to propagate to static consumers.

**Create:**
- `src/tests/test_nullable_boolean_coalesce_audit.py` — scans every `stg_*.sql` file for `coalesce(X, 'false')` / `ifnull(X, false)` / `nvl(X, 0)` patterns on columns typed as nullable boolean/int, and FAILs when any is found. Exemption list for intentional uses, with reason string.

---

# PR A — Safety Teeth (G6 + G2 + G3)

Ships first. Tests that actually catch live drops. No `src/ingestion/*` changes; no wheel bump.

## Task A0: Prepare worktree + branch

- [ ] **Step 1: Verify clean main head**

```bash
git status && git log --oneline -1
```
Expected: `nothing to commit, working tree clean` + `7dd5a9b feat(kimball): passes + line-breaking + match summary on match_key...`

- [ ] **Step 2: Create the single feature branch**

```bash
git checkout -b feat/bronze-drop-safety-sweep
```
Expected: `Switched to a new branch 'feat/bronze-drop-safety-sweep'` (single-branch execution per the amendment at the top of this plan).

- [ ] **Step 3: Confirm baseline pytest collection**

```bash
uv run pytest --collect-only src/tests/test_bronze_live_schema.py src/tests/test_staging_coverage.py -q 2>&1 | tail -15
```
Expected: collection succeeds; existing tests found; no NEW tests (we haven't written them yet).

---

## Task A1 — G3: Rowcount-vs-bronze parity tests (Mode 4: filter drop)

**Files:**
- Create: `src/tests/test_staging_rowcount_vs_bronze.py`

G3 has zero parity tests today — Mode 4 coverage is 0%. We write the test first even though it will be skipped in local CI (live-DB only); the test runs in the new G6 workflow.

- [ ] **Step 1: Write the parity test**

Write `src/tests/test_staging_rowcount_vs_bronze.py`:

```python
"""Bronze↔staging rowcount parity for `WHERE col IS NOT NULL` filters.

For every staging model that filters bronze rows via a non-null guard,
assert that the filter is conservative — no bronze row is silently dropped
by an unintended side-effect.

Invariant per (bronze_table, staging_model, filter_col) triple:
  count(staging) + count(bronze WHERE filter_col IS NULL) == count(bronze)

Any deviation means either (a) the staging model joined away rows, (b) an
additional implicit filter narrowed the set, or (c) the stated filter-col
is not the only predicate. All three are bugs the author must triage.

Requires live Databricks. Skipped when DATABRICKS_{HOST,HTTP_PATH,TOKEN}
env vars are unset or `databricks-sql-connector` is not installed.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


# (bronze_table, staging_model, filter_column_sql_expr)
# filter_column_sql_expr is the SQL predicate identifying rows the staging
# model drops. For multi-column filters (x IS NOT NULL AND y IS NOT NULL)
# the predicate is the conjunction that matches the dropped rows:
# `(x IS NULL OR y IS NULL)`.
_FILTER_SITES: list[tuple[str, str, str]] = [
    ("idsse_events",       "stg_idsse__events",       "(x IS NULL OR y IS NULL)"),
    ("idsse_tracking",     "stg_idsse__tracking",     "(x IS NULL OR y IS NULL)"),
    ("metrica_tracking",   "stg_metrica__tracking",   "(raw_x IS NULL OR raw_y IS NULL)"),
    ("skillcorner_tracking", "stg_skillcorner__tracking", "(x IS NULL OR y IS NULL)"),
    ("wyscout_players",    "stg_wyscout__players",    "wyId IS NULL"),
]

# Catalog/schema override for CI. `soccer_analytics.dev_bronze` / `dev_staging`
# in dev; CI uses the same dev workspace because prod schemas are behind
# stricter grants and the filter invariants don't change per environment.
_BRONZE_SCHEMA = os.environ.get("BRONZE_SCHEMA", "soccer_analytics.dev_bronze")
_STAGING_SCHEMA = os.environ.get("STAGING_SCHEMA", "soccer_analytics.dev_staging")


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield c
    finally:
        c.close()


def _scalar(conn: object, sql: str) -> int:
    cur = conn.cursor()  # type: ignore[attr-defined]
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0])
    finally:
        cur.close()


@requires_databricks
@pytest.mark.parametrize(("bronze_table", "staging_model", "filter_predicate"), _FILTER_SITES)
def test_staging_preserves_bronze_rows(
    conn: object,
    bronze_table: str,
    staging_model: str,
    filter_predicate: str,
) -> None:
    bronze_total = _scalar(conn, f"SELECT count(*) FROM {_BRONZE_SCHEMA}.{bronze_table}")
    bronze_dropped = _scalar(
        conn,
        f"SELECT count(*) FROM {_BRONZE_SCHEMA}.{bronze_table} WHERE {filter_predicate}",
    )
    staging_total = _scalar(conn, f"SELECT count(*) FROM {_STAGING_SCHEMA}.{staging_model}")

    expected = bronze_total - bronze_dropped
    assert staging_total == expected, (
        f"[{bronze_table} → {staging_model}] parity broken:\n"
        f"  bronze total              = {bronze_total:,}\n"
        f"  bronze matching filter    = {bronze_dropped:,}\n"
        f"  staging total             = {staging_total:,}\n"
        f"  expected (bronze - dropped) = {expected:,}\n"
        f"  delta                     = {staging_total - expected:+,}\n"
        "Mode 4 (filter drop) violation: staging is dropping rows that the\n"
        "documented `WHERE ... IS NOT NULL` filter does not account for.\n"
        f"Filter predicate: {filter_predicate}\n"
        "Fix: either expand the predicate, remove the hidden filter, or\n"
        "document the additional exclusion in the staging SQL comment."
    )
```

- [ ] **Step 2: Run locally — confirm it skips (no Databricks SDK)**

```bash
uv run pytest src/tests/test_staging_rowcount_vs_bronze.py -v --no-header
```
Expected: `5 skipped` (5 parametrized cases, all gated by `pytest.importorskip("databricks.sql")` — databricks-sql-connector not installed locally either since it's not in pyproject.toml).

**If locally installed:** 5 tests run; 5 pass if DATABRICKS_* env vars are set. Move on in either case.

- [ ] **Step 3: Run lint + type check**

```bash
uv run ruff check src/tests/test_staging_rowcount_vs_bronze.py && uv run ruff format --check src/tests/test_staging_rowcount_vs_bronze.py && uv run pyright src/tests/test_staging_rowcount_vs_bronze.py
```
Expected: all clean.

---

## Task A2 — G2: Wyscout coerce-audit test (Mode 3: unguarded cast)

**Files:**
- Create: `src/tests/test_wyscout_coerce_audit.py`
- Modify: `src/ingestion/wyscout.py` (extend `_normalize_mixed_types` with audit logger)

TDD: write the failing test first, then the behavior.

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_wyscout_coerce_audit.py`:

```python
"""Assert `_normalize_mixed_types` does not silently coerce string values to NaN.

Wyscout ingestion's mixed-type normalisation (`_normalize_mixed_types`) runs
`pd.to_numeric(df[col], errors="coerce")` on any column whose first non-null
element is `int | float`. If a LATER element in the same column is a string
that cannot parse as numeric, it becomes NaN silently — Mode 3 (unguarded
cast) failure.

Fix: `_normalize_mixed_types` accepts a `logger` parameter; it emits an
ERROR-level log for every (column, bad_value_count) pair where a non-null
string value was converted to NaN. Callers treat ERROR logs as blocking.
"""

from __future__ import annotations

import logging

import pandas as pd

from ingestion.wyscout import _normalize_mixed_types


def test_clean_numeric_column_passes(caplog: pytest.LogCaptureFixture) -> None:
    """All-numeric object column: coerce succeeds, no ERROR logs."""
    df = pd.DataFrame({"x": [1, 2.5, 3, None, 4]}, dtype=object)
    logger = logging.getLogger("wyscout.audit")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records, f"unexpected ERROR logs: {[r.message for r in caplog.records]}"


def test_mixed_numeric_and_bad_string_emits_error(caplog: pytest.LogCaptureFixture) -> None:
    """Numeric-first column with a later bad-string row: ERROR log fires."""
    df = pd.DataFrame({"y": [1, 2, "not-a-number", 4, None]}, dtype=object)
    logger = logging.getLogger("wyscout.audit")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit"):
        _normalize_mixed_types(df, logger=logger)
    matching = [r for r in caplog.records if r.levelno == logging.ERROR and "coerce" in r.message.lower()]
    assert matching, f"expected ERROR log about coerce loss; got records: {[r.message for r in caplog.records]}"
    assert "y" in matching[0].message
    assert "1" in matching[0].message  # exactly 1 value lost


def test_all_null_column_no_error(caplog: pytest.LogCaptureFixture) -> None:
    """Pure-null object column: no coerce, no ERROR."""
    df = pd.DataFrame({"z": [None, None, None]}, dtype=object)
    logger = logging.getLogger("wyscout.audit")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records


def test_string_only_column_no_coerce(caplog: pytest.LogCaptureFixture) -> None:
    """First element is string → astype(str) branch; no numeric coerce."""
    df = pd.DataFrame({"s": ["alpha", None, "gamma"]}, dtype=object)
    logger = logging.getLogger("wyscout.audit")
    with caplog.at_level(logging.ERROR, logger="wyscout.audit"):
        _normalize_mixed_types(df, logger=logger)
    assert not caplog.records
```

Add `import pytest` at the top (omitted in the snippet).

- [ ] **Step 2: Run test — expect it to fail (signature mismatch)**

```bash
uv run pytest src/tests/test_wyscout_coerce_audit.py -v
```
Expected: FAIL. `_normalize_mixed_types()` current signature is `(df)` — test calls `(df, logger=logger)`. TypeError.

- [ ] **Step 3: Extend `_normalize_mixed_types` to accept `logger` and emit ERROR on coerce loss**

Edit `src/ingestion/wyscout.py` lines 115-136. Current:
```python
def _normalize_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize mixed-type object columns so PyArrow/Spark can convert them.

    After pd.concat across competitions, some columns end up as ``object``
    dtype with a mix of int/float/NaN or date/string values.  PyArrow cannot
    infer a single Arrow type from these heterogeneous Series.  We coerce
    numeric-looking columns to numeric and cast the rest to strings.
    """
    for col in df.columns:
        if df[col].dtype != object:
            continue
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if isinstance(first, int | float):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif isinstance(first, dict | list):
            pass  # already handled by serialize_json_columns
        else:
            df[col] = df[col].astype(str)
    return df
```

Replace with:
```python
def _normalize_mixed_types(
    df: pd.DataFrame,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Normalize mixed-type object columns so PyArrow/Spark can convert them.

    After pd.concat across competitions, some columns end up as ``object``
    dtype with a mix of int/float/NaN or date/string values. PyArrow cannot
    infer a single Arrow type from these heterogeneous Series. We coerce
    numeric-looking columns to numeric and cast the rest to strings.

    When ``logger`` is provided, every column where
    ``pd.to_numeric(..., errors="coerce")`` silently converts a non-null
    string value to NaN is reported at ERROR level. This is Mode 3
    (unguarded cast) instrumentation: callers treat ERROR logs as blocking.
    When ``logger`` is None the audit is silent (legacy behaviour).
    """
    for col in df.columns:
        if df[col].dtype != object:
            continue
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if isinstance(first, int | float):
            if logger is not None:
                non_null_before = df[col].notna().sum()
                coerced = pd.to_numeric(df[col], errors="coerce")
                non_null_after = coerced.notna().sum()
                lost = int(non_null_before - non_null_after)
                if lost > 0:
                    logger.error(
                        "coerce-loss: column %r lost %d non-null value(s) to NaN "
                        "during pd.to_numeric(errors='coerce'). Mode 3 (unguarded "
                        "cast) violation — inspect source for non-numeric strings.",
                        col,
                        lost,
                    )
                df[col] = coerced
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        elif isinstance(first, dict | list):
            pass  # already handled by serialize_json_columns
        else:
            df[col] = df[col].astype(str)
    return df
```

- [ ] **Step 4: Re-run the test — expect it to pass**

```bash
uv run pytest src/tests/test_wyscout_coerce_audit.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Run the rest of the Wyscout test suite to confirm no regression**

```bash
uv run pytest src/tests/test_wyscout_bronze_coverage.py src/tests/test_staging_coverage.py -v --no-header 2>&1 | tail -30
```
Expected: all tests pass (logger default is None → legacy behaviour preserved).

- [ ] **Step 6: Run lint + type check**

```bash
uv run ruff check src/tests/test_wyscout_coerce_audit.py src/ingestion/wyscout.py && uv run ruff format --check src/tests/test_wyscout_coerce_audit.py src/ingestion/wyscout.py && uv run pyright src/ingestion/wyscout.py
```
Expected: all clean.

---

## Task A3 — G6: CI live-schema + live-parity workflow

**Files:**
- Create: `.github/workflows/bronze-live-schema.yml`

This is not TDD — it's infrastructure. Validate by inspection + a later `workflow_dispatch` run once merged.

- [ ] **Step 1: Check required repo variables already exist**

We need `vars.DATABRICKS_HOST` (already exists per `.github/workflows/python-ci.yml:144`), `vars.DATABRICKS_HTTP_PATH` (NEW — probably not set), `secrets.DATABRICKS_TOKEN` (already exists per python-ci.yml:145). Confirm via `gh` if the token permission allows:

```bash
gh variable list 2>&1 | head -10
```
Expected: at minimum `DATABRICKS_HOST`. If `DATABRICKS_HTTP_PATH` is absent, the plan includes a repo-var creation step (Task A3 Step 5).

- [ ] **Step 2: Write the workflow file**

Create `.github/workflows/bronze-live-schema.yml`:

```yaml
name: Bronze Live Schema + Parity

# Runs the live-DB tests (`test_bronze_live_schema.py` + `test_staging_rowcount_vs_bronze.py`)
# that the main python-ci.yml skips because:
#   (a) `databricks-sql-connector` is not in any pyproject extra, so
#       `pytest.importorskip("databricks.sql")` skips the modules; and
#   (b) DATABRICKS_{HOST,HTTP_PATH,TOKEN} are scoped to the `dbt deps` step
#       in main CI, not the `Run tests` step.
#
# This workflow installs the SQL connector ad-hoc and injects real secrets
# so the tests actually run. It is the only gate that catches Mode 1
# (writer-side drop → NullType column drop) and Mode 4 (filter drop
# → row-parity violation) on live bronze.

on:
  push:
    branches: [main]
    paths:
      - 'src/ingestion/**'
      - 'dbt_project/models/staging/**'
      - 'dbt_project/models/sources/**'
      - 'src/tests/test_bronze_live_schema.py'
      - 'src/tests/test_staging_rowcount_vs_bronze.py'
      - 'src/tests/coverage_utils.py'
      - '.github/workflows/bronze-live-schema.yml'
  schedule:
    # Daily at 08:00 UTC — after lakebase-grants.yml runs at 07:00 UTC so
    # any index / grant drift is self-healed before the parity check runs.
    - cron: '0 8 * * *'
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: bronze-live-schema
  cancel-in-progress: false

jobs:
  live-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v5

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Set up Python 3.10
        run: uv python install 3.10

      - name: Install dependencies
        run: uv sync --frozen --extra analytics

      - name: Install databricks-sql-connector (ad-hoc, not in pyproject)
        run: uv pip install databricks-sql-connector

      - name: Run bronze live-schema + parity tests
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          DATABRICKS_HTTP_PATH: ${{ vars.DATABRICKS_HTTP_PATH }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          BRONZE_SCHEMA: soccer_analytics.dev_bronze
          STAGING_SCHEMA: soccer_analytics.dev_staging
        run: |
          uv run pytest \
            src/tests/test_bronze_live_schema.py \
            src/tests/test_staging_rowcount_vs_bronze.py \
            -v --tb=short --no-header

      - name: Report
        if: always()
        run: echo "::notice::bronze live-schema + parity workflow completed. Check pytest output above for mode-1/mode-4 violations."
```

Notes on the choices embedded above:
- `on.push.paths:` restricts the workflow to relevant file changes so it doesn't run on unrelated main pushes.
- `concurrency:` serialises runs — no concurrent warehouse load.
- Installing `databricks-sql-connector` via `uv pip install` inside the workflow step (not a new extra in pyproject.toml) is deliberate: avoids bloating the dev extras + keeps pyproject.toml provider-unopinionated. `-- extra analytics` covers everything else the tests need (pyspark / pandas / pyyaml).

- [ ] **Step 3: Validate workflow YAML syntax**

```bash
uv run python -c "import yaml; d = yaml.safe_load(open('.github/workflows/bronze-live-schema.yml', encoding='utf-8')); print(list(d.keys()))"
```
Expected: `['name', True, 'permissions', 'concurrency', 'jobs']` (the `True` is PyYAML rendering `on:` — this is cosmetic; GitHub parses it correctly).

- [ ] **Step 4: Ensure `DATABRICKS_HTTP_PATH` is set as a repo variable**

This step needs user intervention if the variable is missing. From the check in Step 1, if `DATABRICKS_HTTP_PATH` was absent:

**[PAUSE — user confirms or sets the variable]**

```bash
gh variable set DATABRICKS_HTTP_PATH --body "/sql/1.0/warehouses/<warehouse-id>"
```

Do **not** use double-slash prefix — that's Git Bash (MSYS) specific; GitHub Actions runs on Linux where single-slash is correct.

- [ ] **Step 5: Dry-run lint of workflow (actionlint optional)**

```bash
uv run python -c "
import yaml
from pathlib import Path
d = yaml.safe_load(Path('.github/workflows/bronze-live-schema.yml').read_text(encoding='utf-8'))
jobs = d['jobs']
assert 'live-tests' in jobs
assert jobs['live-tests']['runs-on'] == 'ubuntu-latest'
steps = jobs['live-tests']['steps']
assert any(s.get('name') == 'Install databricks-sql-connector (ad-hoc, not in pyproject)' for s in steps)
assert any(s.get('name') == 'Run bronze live-schema + parity tests' for s in steps)
print('workflow OK')
"
```
Expected: `workflow OK`.

---

## Task A4 — Full suite regression + self-review for PR A

- [ ] **Step 1: Run the full Python test suite locally**

```bash
uv run pytest -v --no-header 2>&1 | tail -50
```

Run in background given the suite size:

```
Bash(command=..., run_in_background=true, timeout=600000)
```

Expected: all pre-existing tests still pass; the 3 new modules either skip (live-DB) or pass (coerce audit).

- [ ] **Step 2: Run `lint-imports` import-boundary check**

```bash
uv run lint-imports
```
Expected: 0 violations.

- [ ] **Step 3: Ruff + pyright over the touched files**

```bash
uv run ruff check src/tests/ src/ingestion/wyscout.py && uv run ruff format --check src/tests/ src/ingestion/wyscout.py && uv run pyright src/ingestion/wyscout.py src/tests/test_wyscout_coerce_audit.py src/tests/test_staging_rowcount_vs_bronze.py
```
Expected: all clean.

- [ ] **Step 4: Validate workflow cards still pass**

```bash
uv run validate_workflow_cards workflow-cards/
```
Expected: all cards validate (no card change expected, sanity check).

- [ ] **Step 5: Diff summary**

```bash
git diff --stat main
```
Expected: 4 files changed — the 3 new files + modifications to `src/ingestion/wyscout.py`.

- [ ] **Step 6: **[PAUSE — user approval]** Commit PR A as a single squash-worthy commit**

Once the user approves:
```bash
git add .github/workflows/bronze-live-schema.yml \
        src/ingestion/wyscout.py \
        src/tests/test_staging_rowcount_vs_bronze.py \
        src/tests/test_wyscout_coerce_audit.py

git commit -m "$(cat <<'EOF'
test(bronze): drop-safety teeth — G6 live-schema wiring + G2 coerce audit + G3 parity

Closes three gaps from the PR #173 bronze drop-safety audit:

- G6 (Mode 1): new bronze-live-schema.yml workflow installs
  databricks-sql-connector + injects DATABRICKS_HOST/HTTP_PATH/TOKEN so
  test_bronze_live_schema.py actually runs. Runs nightly + on push-to-main
  for src/ingestion/** and staging SQL changes.
- G2 (Mode 3): _normalize_mixed_types() in wyscout.py now accepts an
  optional logger; emits ERROR-level log on silent pd.to_numeric coerce
  losses. test_wyscout_coerce_audit.py enforces on synthetic fixtures.
- G3 (Mode 4): new test_staging_rowcount_vs_bronze.py asserts
  count(staging) + count(bronze WHERE filter-is-null) == count(bronze)
  across the five documented `WHERE ... IS NOT NULL` filter sites.

No behavior change on default call sites; logger=None preserves legacy
_normalize_mixed_types behavior.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7: **[PAUSE — user approval]** Push + open PR A**

```bash
git push -u origin safety/bronze-drop-safety-teeth
gh pr create --title "test(bronze): drop-safety teeth — G6 live-schema wiring + G2 coerce audit + G3 parity" --body "$(cat <<'EOF'
## Summary

Closes three gaps from the PR #173 bronze drop-safety audit — the three Mode-{1,3,4} High-severity items:

- **G6 (Mode 1 writer drop):** `.github/workflows/bronze-live-schema.yml` runs `test_bronze_live_schema.py` with real Databricks secrets + ad-hoc `databricks-sql-connector` install. Nightly + post-merge-to-main for ingestion + staging changes.
- **G2 (Mode 3 unguarded cast):** `_normalize_mixed_types` in `wyscout.py` now logs at ERROR when `pd.to_numeric(errors="coerce")` silently converts a non-null string to NaN. New `test_wyscout_coerce_audit.py` enforces on synthetic fixtures.
- **G3 (Mode 4 filter drop):** new `test_staging_rowcount_vs_bronze.py` asserts `count(staging) + count(bronze WHERE filter-col IS NULL) == count(bronze)` across the five documented `WHERE ... IS NOT NULL` sites (idsse_events, idsse_tracking, metrica_tracking, skillcorner_tracking, wyscout_players).

No src/ingestion/ behavior change on default call sites (logger=None preserves legacy behavior). No wheel bump needed.

Part of the drop-safety sweep that precedes Kimball PR 3. PR B (G1+G4+G5 cleanup) follows.

## Test plan

- [x] `uv run pytest src/tests/test_wyscout_coerce_audit.py -v` — 4/4 pass locally
- [x] `uv run pytest src/tests/test_staging_rowcount_vs_bronze.py -v` — 5/5 skip locally (databricks-sql-connector not in pyproject)
- [ ] New workflow `bronze-live-schema.yml` runs successfully via workflow_dispatch after merge
- [ ] Workflow's first scheduled run at 08:00 UTC reports 0 Mode-1 / Mode-4 violations
- [ ] Python CI still green on this branch
- [ ] Semgrep SAST still green on this branch

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.

- [ ] **Step 8: **[PAUSE — wait for CI + merge]**

Once CI is green and user merges the PR:

```bash
git checkout main && git pull && git branch -d safety/bronze-drop-safety-teeth
```

---

# PR B — Tail Cleanup (G1 + G4 + G5)

Ships after PR A is on main. Extends `finalize_bronze_df` coverage, fixes IDSSE `is_cross` nullable discipline, and shrinks `INITIAL_BRONZE_STAGING_GAPS` to `{}`.

## Task B0: Branch from updated main

- [ ] **Step 1: Verify PR A merged**

```bash
git checkout main && git pull && git log --oneline -3
```
Expected: top commit is the squash-merge of `safety/bronze-drop-safety-teeth`.

- [ ] **Step 2: Create PR B branch**

```bash
git checkout -b cleanup/bronze-drop-safety-tail
```

---

## Task B1 — G1a: Build StatsBomb expected-col constants from snapshot

**Files:**
- Modify: `src/ingestion/statsbomb.py`
- Reference: `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json`

Before wiring `finalize_bronze_df`, we need canonical `expected_cols` for each of the 3 StatsBomb bronze tables (competitions, matches, events — 435 is called from the `_write_batch` helper that writes events/lineups/360).

- [ ] **Step 1: Inspect the snapshot JSON structure**

```bash
uv run python -c "
import json
d = json.load(open('src/tests/fixtures/statsbomb_bronze_schema_snapshot.json', encoding='utf-8'))
print(sorted(d.keys()) if isinstance(d, dict) else type(d).__name__)
for k, v in d.items() if isinstance(d, dict) else []:
    print(f'  {k}: {len(v) if hasattr(v, \"__len__\") else v} entries')
"
```
Expected: keys for `statsbomb_competitions`, `statsbomb_matches`, `statsbomb_events`, `statsbomb_lineups`, `statsbomb_360` (or equivalent). Adjust names in Step 2 to match actual keys.

- [ ] **Step 2: Write the failing test first**

Add `src/tests/test_statsbomb_bronze_expected_cols.py`:

```python
"""Assert statsbomb.py exposes module-level expected-col constants that
match the bronze schema snapshot — so finalize_bronze_df has a stable,
machine-checked source of truth for which columns to protect from
NullType drop."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_FIXTURE = Path(__file__).parent / "fixtures" / "statsbomb_bronze_schema_snapshot.json"


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_competitions_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_COMPETITIONS_EXPECTED_COLS
    assert set(_STATSBOMB_COMPETITIONS_EXPECTED_COLS) == set(snapshot["statsbomb_competitions"])


def test_matches_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_MATCHES_EXPECTED_COLS
    assert set(_STATSBOMB_MATCHES_EXPECTED_COLS) == set(snapshot["statsbomb_matches"])


def test_events_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_EVENTS_EXPECTED_COLS
    assert set(_STATSBOMB_EVENTS_EXPECTED_COLS) == set(snapshot["statsbomb_events"])


def test_lineups_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_LINEUPS_EXPECTED_COLS
    assert set(_STATSBOMB_LINEUPS_EXPECTED_COLS) == set(snapshot["statsbomb_lineups"])


def test_360_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_360_EXPECTED_COLS
    assert set(_STATSBOMB_360_EXPECTED_COLS) == set(snapshot["statsbomb_360"])
```

If the snapshot keys differ, adjust the test keys accordingly.

- [ ] **Step 3: Run test — expect ImportError / AttributeError**

```bash
uv run pytest src/tests/test_statsbomb_bronze_expected_cols.py -v
```
Expected: 5 tests, all FAIL with ImportError (constants not defined).

- [ ] **Step 4: Add the constants to `src/ingestion/statsbomb.py`**

After the module-level imports, before `class _StatsbombGuard`, add:

```python
# Expected-col constants for finalize_bronze_df (G1a / PR #XXX).
# Loaded from the bronze schema snapshot fixture so future parser additions
# surface as a test failure — keeping the wheel's expected-cols lockstep
# with live bronze.
def _load_statsbomb_snapshot() -> dict[str, list[str]]:
    """Load the StatsBomb bronze schema snapshot from the package fixture.

    The snapshot ships with the source tree (not the wheel) — resolved at
    import time via the repo layout. When the wheel is installed on
    Databricks, the fixture isn't needed because the constants below are
    baked into the module at import.
    """
    import json
    from pathlib import Path

    fixture = (
        Path(__file__).resolve().parent.parent
        / "tests" / "fixtures" / "statsbomb_bronze_schema_snapshot.json"
    )
    return json.loads(fixture.read_text(encoding="utf-8"))


try:
    _sb_snapshot = _load_statsbomb_snapshot()
    _STATSBOMB_COMPETITIONS_EXPECTED_COLS: tuple[str, ...] = tuple(_sb_snapshot["statsbomb_competitions"])
    _STATSBOMB_MATCHES_EXPECTED_COLS: tuple[str, ...] = tuple(_sb_snapshot["statsbomb_matches"])
    _STATSBOMB_EVENTS_EXPECTED_COLS: tuple[str, ...] = tuple(_sb_snapshot["statsbomb_events"])
    _STATSBOMB_LINEUPS_EXPECTED_COLS: tuple[str, ...] = tuple(_sb_snapshot["statsbomb_lineups"])
    _STATSBOMB_360_EXPECTED_COLS: tuple[str, ...] = tuple(_sb_snapshot["statsbomb_360"])
    del _sb_snapshot
except FileNotFoundError:
    # Wheel runtime: fixture is not shipped. Fall back to a parser-derived
    # lower bound. Downstream callers always pass `expected_cols=` to
    # finalize_bronze_df so an empty tuple here degrades gracefully to the
    # "protect all-None cols only" mode.
    _STATSBOMB_COMPETITIONS_EXPECTED_COLS = ()
    _STATSBOMB_MATCHES_EXPECTED_COLS = ()
    _STATSBOMB_EVENTS_EXPECTED_COLS = ()
    _STATSBOMB_LINEUPS_EXPECTED_COLS = ()
    _STATSBOMB_360_EXPECTED_COLS = ()
```

**If the fixture's top-level keys don't match those names:** open `src/tests/fixtures/statsbomb_bronze_schema_snapshot.json`, copy the actual key names into the constants, and update the test to use those names. Do **not** commit a divergent test + impl.

- [ ] **Step 5: Re-run tests — expect all pass**

```bash
uv run pytest src/tests/test_statsbomb_bronze_expected_cols.py -v
```
Expected: 5 passed.

- [ ] **Step 6: Confirm no runtime regression in existing StatsBomb tests**

```bash
uv run pytest src/tests/test_statsbomb_bronze_coverage.py -v --no-header
```
Expected: all pre-existing tests still pass.

---

## Task B2 — G1b: Wire `finalize_bronze_df` into StatsBomb writers

**Files:**
- Modify: `src/ingestion/statsbomb.py` at lines 140 / 291 / 435

- [ ] **Step 1: Import `finalize_bronze_df` at the top of statsbomb.py**

Check current imports. The module already imports from `ingestion.utils`. Add `finalize_bronze_df` to that import block.

Search for the existing utils import:
```bash
grep -n "from ingestion.utils" src/ingestion/statsbomb.py | head -3
```

Add `finalize_bronze_df` to the existing tuple/list.

- [ ] **Step 2: Apply to competitions writer (line 138-140)**

Current:
```python
    competitions_pdf = serialize_json_columns(competitions_pdf)

    sdf = spark.createDataFrame(competitions_pdf)
```

Replace with:
```python
    competitions_pdf = serialize_json_columns(competitions_pdf)
    competitions_pdf = finalize_bronze_df(
        competitions_pdf,
        expected_cols=_STATSBOMB_COMPETITIONS_EXPECTED_COLS,
    )

    sdf = spark.createDataFrame(competitions_pdf)
```

- [ ] **Step 3: Apply to matches writer (line 290-291)**

Current:
```python
        matches_pdf = serialize_json_columns(matches_pdf)
        matches_sdf = spark.createDataFrame(matches_pdf)
```

Replace with:
```python
        matches_pdf = serialize_json_columns(matches_pdf)
        matches_pdf = finalize_bronze_df(
            matches_pdf,
            expected_cols=_STATSBOMB_MATCHES_EXPECTED_COLS,
        )
        matches_sdf = spark.createDataFrame(matches_pdf)
```

- [ ] **Step 4: Apply to the `_write_batch` helper (lines 425-437)**

Current helper signature + body:
```python
def _write_batch(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
    replace_where: str,
    batch: list[pd.DataFrame],
    logger: logging.Logger,
    required_columns: list[str],
) -> None:
    """Concatenate a list of pandas DataFrames, serialize JSON columns, and write to Delta."""
    if not batch:
        logger.info("No data for %s in this partition", table_name)
        return

    combined = pd.concat(batch, ignore_index=True)
    combined = serialize_json_columns(combined)
    sdf = spark.createDataFrame(combined)
    row_count = validate_dataframe(sdf, required_columns, table_name, logger)
    write_delta_table(sdf, catalog, schema, table_name, replace_where=replace_where, logger=logger, row_count=row_count)
```

`_write_batch` is called for `statsbomb_events`, `statsbomb_lineups`, `statsbomb_360`. The helper doesn't know which table. Add a new parameter:

Replace with:
```python
def _write_batch(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
    replace_where: str,
    batch: list[pd.DataFrame],
    logger: logging.Logger,
    required_columns: list[str],
    expected_cols: tuple[str, ...] = (),
) -> None:
    """Concatenate a list of pandas DataFrames, serialize JSON columns, finalize
    against the expected bronze schema (guards against NullType drops), and
    write to Delta."""
    if not batch:
        logger.info("No data for %s in this partition", table_name)
        return

    combined = pd.concat(batch, ignore_index=True)
    combined = serialize_json_columns(combined)
    if expected_cols:
        combined = finalize_bronze_df(combined, expected_cols=expected_cols)
    sdf = spark.createDataFrame(combined)
    row_count = validate_dataframe(sdf, required_columns, table_name, logger)
    write_delta_table(sdf, catalog, schema, table_name, replace_where=replace_where, logger=logger, row_count=row_count)
```

- [ ] **Step 5: Pass `expected_cols` at every `_write_batch` call site**

```bash
grep -n "_write_batch(" src/ingestion/statsbomb.py | head -10
```

For each call site, wire the appropriate constant. Example for the events call:
```python
_write_batch(
    spark, catalog, schema, "statsbomb_events",
    replace_where=..., batch=..., logger=..., required_columns=...,
    expected_cols=_STATSBOMB_EVENTS_EXPECTED_COLS,
)
```

Similarly for `statsbomb_lineups` → `_STATSBOMB_LINEUPS_EXPECTED_COLS`, `statsbomb_360` → `_STATSBOMB_360_EXPECTED_COLS`.

- [ ] **Step 6: Run ruff + pyright**

```bash
uv run ruff check src/ingestion/statsbomb.py && uv run ruff format --check src/ingestion/statsbomb.py && uv run pyright src/ingestion/statsbomb.py
```
Expected: all clean.

- [ ] **Step 7: Run StatsBomb test suite**

```bash
uv run pytest src/tests/test_statsbomb_bronze_coverage.py src/tests/test_statsbomb_bronze_expected_cols.py -v --no-header
```
Expected: all pass.

---

## Task B3 — G1c: Same treatment for Wyscout writers

**Files:**
- Modify: `src/ingestion/wyscout.py` at lines 255 / 298 / 513
- Reference: `src/tests/fixtures/wyscout_bronze_schema_snapshot.json`

- [ ] **Step 1: Inspect snapshot structure**

```bash
uv run python -c "
import json
d = json.load(open('src/tests/fixtures/wyscout_bronze_schema_snapshot.json', encoding='utf-8'))
print(sorted(d.keys()) if isinstance(d, dict) else type(d).__name__)
for k, v in d.items() if isinstance(d, dict) else []:
    print(f'  {k}: {len(v) if hasattr(v, \"__len__\") else v} entries')
"
```
Expected: keys for `wyscout_events`, `wyscout_matches`, `wyscout_players`.

- [ ] **Step 2: Write the failing test first**

Add `src/tests/test_wyscout_bronze_expected_cols.py`:

```python
"""Assert wyscout.py exposes module-level expected-col constants that
match the bronze schema snapshot."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_FIXTURE = Path(__file__).parent / "fixtures" / "wyscout_bronze_schema_snapshot.json"


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_events_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_EVENTS_EXPECTED_COLS
    assert set(_WYSCOUT_EVENTS_EXPECTED_COLS) == set(snapshot["wyscout_events"])


def test_matches_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_MATCHES_EXPECTED_COLS
    assert set(_WYSCOUT_MATCHES_EXPECTED_COLS) == set(snapshot["wyscout_matches"])


def test_players_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_PLAYERS_EXPECTED_COLS
    assert set(_WYSCOUT_PLAYERS_EXPECTED_COLS) == set(snapshot["wyscout_players"])
```

- [ ] **Step 3: Run test — expect fail (ImportError)**

```bash
uv run pytest src/tests/test_wyscout_bronze_expected_cols.py -v
```
Expected: 3 FAILs, ImportError.

- [ ] **Step 4: Add constants + import to `src/ingestion/wyscout.py`**

Mirror the StatsBomb pattern. Add the same `_load_wyscout_snapshot` / `try/except FileNotFoundError` scaffolding. Import `finalize_bronze_df` into the existing `from ingestion.utils import (...)` tuple.

- [ ] **Step 5: Apply `finalize_bronze_df` at lines 255, 298, 513**

Line 255 (events writer), current:
```python
    df = serialize_json_columns(df, ["positions", "tags"])
    df = _normalize_mixed_types(df)
    sdf = spark.createDataFrame(df)
```

Replace with:
```python
    df = serialize_json_columns(df, ["positions", "tags"])
    df = _normalize_mixed_types(df, logger=logger)  # G2 audit
    df = finalize_bronze_df(df, expected_cols=_WYSCOUT_EVENTS_EXPECTED_COLS)  # G1
    sdf = spark.createDataFrame(df)
```

Note: this is the first call site that *opts into* the G2 audit by passing `logger=logger`. This is intentional — the audit is instrumental (not behavior-changing at default) and events is the largest Wyscout table.

Line 298 (matches writer), current:
```python
    for c in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[c] = df[c].astype(str)
    sdf = spark.createDataFrame(df)
```

Replace with:
```python
    for c in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[c] = df[c].astype(str)
    df = finalize_bronze_df(df, expected_cols=_WYSCOUT_MATCHES_EXPECTED_COLS)  # G1
    sdf = spark.createDataFrame(df)
```

Line 513 (players writer), current:
```python
    pdf = _normalize_mixed_types(pdf)

    sdf = spark.createDataFrame(pdf)
```

Replace with:
```python
    pdf = _normalize_mixed_types(pdf, logger=logger)  # G2 audit
    pdf = finalize_bronze_df(pdf, expected_cols=_WYSCOUT_PLAYERS_EXPECTED_COLS)  # G1

    sdf = spark.createDataFrame(pdf)
```

(If the `logger` local is not in scope at line 513, check the enclosing function and pass the module logger or drop the `logger=` kwarg.)

- [ ] **Step 6: Run lint + type check + Wyscout suite**

```bash
uv run ruff check src/ingestion/wyscout.py && uv run ruff format --check src/ingestion/wyscout.py && uv run pyright src/ingestion/wyscout.py
uv run pytest src/tests/test_wyscout_bronze_coverage.py src/tests/test_wyscout_bronze_expected_cols.py src/tests/test_wyscout_coerce_audit.py -v --no-header
```
Expected: all clean; all pass.

---

## Task B4 — G1d: Stop skipping StatsBomb + Wyscout in live-schema tests

**Files:**
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1: Remove the Wyscout `@pytest.mark.skip`**

Current lines 224-234:
```python
@requires_databricks
@pytest.mark.skip(
    reason=(
        "Wyscout events bronze is already source-complete per the existing "
        "test_wyscout_bronze_coverage snapshot-vs-sources.yml test. "
        "Per-competition writes don't exhibit the NullType drop pattern "
        "because every competition exercises every event field at least once."
    )
)
def test_wyscout_events_live_schema_covers_parser(conn: object) -> None:
    pass
```

Replace with an implemented test mirroring the metrica/skillcorner pattern:
```python
@requires_databricks
def test_wyscout_events_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.wyscout import _WYSCOUT_EVENTS_EXPECTED_COLS

    expected = set(_WYSCOUT_EVENTS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "wyscout_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[Wyscout events] Live bronze.wyscout_events is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)
```

- [ ] **Step 2: Add StatsBomb tests (competitions + matches + events; skip lineups/360 if snapshot-vs-live divergence is expected)**

After the Wyscout test, add:

```python
@requires_databricks
def test_statsbomb_competitions_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.statsbomb import _STATSBOMB_COMPETITIONS_EXPECTED_COLS

    expected = set(_STATSBOMB_COMPETITIONS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_competitions") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb competitions] Live bronze.statsbomb_competitions is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_statsbomb_matches_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.statsbomb import _STATSBOMB_MATCHES_EXPECTED_COLS

    expected = set(_STATSBOMB_MATCHES_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_matches") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb matches] Live bronze.statsbomb_matches is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)


@requires_databricks
def test_statsbomb_events_live_schema_covers_parser(conn: object) -> None:
    sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
    from ingestion.statsbomb import _STATSBOMB_EVENTS_EXPECTED_COLS

    expected = set(_STATSBOMB_EVENTS_EXPECTED_COLS)
    actual = _live_bronze_cols(conn, "statsbomb_events") - _AUDIT_COLS
    missing = expected - actual
    if missing:
        msg = (
            f"\n[StatsBomb events] Live bronze.statsbomb_events is missing "
            f"{len(missing)} column(s): {sorted(missing)}\n"
            f"Fix: re-ingest with wheel containing finalize_bronze_df guard."
        )
        raise AssertionError(msg)
```

- [ ] **Step 3: Update the module docstring**

The docstring currently says (line 36-38):
```
  StatsBomb is deliberately excluded — its bronze is already wide (126 cols)
  because its bulk-per-competition ingestion avoids the per-match all-None
  drop pattern.
```

Replace with:
```
  StatsBomb competitions/matches/events are now covered via the
  _STATSBOMB_*_EXPECTED_COLS constants (G1). Bulk-per-competition
  ingestion avoids the per-match NullType drop, but finalize_bronze_df
  still guards against future single-competition regressions.
```

- [ ] **Step 4: Run tests locally — expect skip (no databricks-sql-connector)**

```bash
uv run pytest src/tests/test_bronze_live_schema.py -v --no-header 2>&1 | tail -30
```
Expected: 7+ tests all skip with "databricks-sql-connector" or env-var reason.

- [ ] **Step 5: Lint + format**

```bash
uv run ruff check src/tests/test_bronze_live_schema.py && uv run ruff format --check src/tests/test_bronze_live_schema.py
```

---

## Task B5 — G4: IDSSE `is_cross` nullable boolean

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__passes.sql` line 88

- [ ] **Step 1: Check downstream consumers of `is_cross`**

```bash
grep -rn "is_cross" dbt_project/models/ | head -20
```

Typical consumers: `int_unified_passes`, `fct_passes`, mart-level aggregations. Confirm they handle NULL (via `coalesce(is_cross, false)` or by filtering). If any consumer does `where is_cross = true`, that's now safe (returns only bronze-recorded true). If any does `where is_cross = false`, that silently excludes NULLs post-fix — flag during execution.

- [ ] **Step 2: Edit `stg_idsse__passes.sql` line 88**

Current:
```sql
        coalesce(play_flat_cross, 'false') = 'true'             as is_cross,
```

Replace with:
```sql
        case
            when play_flat_cross is null then null
            when play_flat_cross = 'true' then true
            else false
        end                                                     as is_cross,
```

- [ ] **Step 3: Parse + compile the dbt project to catch syntax errors**

```bash
uv run python scripts/ensure_warehouse.py -- dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

(May be a long-running command; run with `run_in_background=true` and poll.)

Expected: parse succeeds.

- [ ] **Step 4: Build the model + downstream int_ / fct_ models in dev**

```bash
uv run python scripts/ensure_warehouse.py -- dbt build --project-dir dbt_project --profiles-dir dbt_project --select stg_idsse__passes+
```

Run with `run_in_background=true` and poll — this may take 5-15 minutes depending on what `+` pulls in.

Expected: all downstream models build; no schema contract violation. If a downstream model contract declares `is_cross` as `boolean NOT NULL`, this step will fail — in that case, extend the fix to relax the contract (the user must approve the contract change before execution).

- [ ] **Step 5: Verify post-build row counts**

```bash
uv run python -c "
import os
import databricks.sql
conn = databricks.sql.connect(
    server_hostname=os.environ['DATABRICKS_HOST'].replace('https://', '').rstrip('/'),
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
)
cur = conn.cursor()
cur.execute(\"SELECT count(*) FILTER (WHERE is_cross IS NULL), count(*) FILTER (WHERE is_cross = true), count(*) FILTER (WHERE is_cross = false) FROM soccer_analytics.dev_staging.stg_idsse__passes\")
null_count, true_count, false_count = cur.fetchone()
print(f'is_cross: NULL={null_count:,} true={true_count:,} false={false_count:,}')
assert null_count > 0, 'Expected non-zero NULL count after nullable fix; got 0'
cur.close(); conn.close()
"
```
Expected: non-zero NULL count + prior true/false counts preserved.

---

## Task B6 — G4b: Nullable-boolean coalesce audit test

**Files:**
- Create: `src/tests/test_nullable_boolean_coalesce_audit.py`

- [ ] **Step 1: Write the audit test**

```python
"""Scan staging SQL for silent null-collapse patterns on nullable boolean/int columns.

Pattern detected: `coalesce(X, 'false')`, `coalesce(X, false)`, `ifnull(X, false)`,
`nvl(X, 0)` on bronze columns typed as nullable — these collapse NULL → False
(or 0), destroying the "unknown vs recorded-as-false" distinction. Mode 5
(silent sentinel substitution) failure.

The test walks every `stg_*.sql` file under dbt_project/models/staging/,
extracts matching patterns, and FAILs for any pattern not listed in
_INTENTIONAL_COALESCES (which requires a reason string).
"""

from __future__ import annotations

import re
from pathlib import Path

_STAGING_DIR = Path(__file__).parent.parent.parent / "dbt_project" / "models" / "staging"

# Patterns that collapse NULL → False/0/''.
# We intentionally DO NOT flag `coalesce(x, 'Unknown')` or similar string
# defaults — those preserve a distinguishable sentinel. Only boolean / zero
# collapses are Mode 5 violations.
_COLLAPSE_RES = [
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*'false'\s*\)", re.IGNORECASE),
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*false\s*\)", re.IGNORECASE),
    re.compile(r"ifnull\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*false\s*\)", re.IGNORECASE),
    re.compile(r"nvl\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*0\s*\)", re.IGNORECASE),
    re.compile(r"coalesce\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*0\s*\)", re.IGNORECASE),
]

# (staging_filename_relative, bronze_col): reason. Reason must be non-empty.
# Entries here are Mode-5 patterns that are INTENTIONAL — the staging author
# declares "the distinction between NULL and False is not semantically
# meaningful for this column" and justifies it in writing.
_INTENTIONAL_COALESCES: dict[tuple[str, str], str] = {
    # No intentional Mode 5 collapses after G4 landed. Add entries as
    # staging authors justify each one.
}


def test_no_silent_null_collapse_on_staging_booleans() -> None:
    violations: list[tuple[Path, str, int, str]] = []
    for sql in sorted(_STAGING_DIR.rglob("stg_*.sql")):
        rel = sql.relative_to(_STAGING_DIR.parent.parent)
        text = sql.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            # Ignore commented lines
            stripped = line.lstrip()
            if stripped.startswith("--"):
                continue
            for regex in _COLLAPSE_RES:
                for m in regex.finditer(line):
                    col = m.group(1)
                    key = (str(rel).replace("\\", "/"), col)
                    if key in _INTENTIONAL_COALESCES:
                        continue
                    violations.append((rel, col, line_no, line.rstrip()))

    assert not violations, (
        "Mode 5 (silent sentinel substitution) violations — these coalesce/ifnull/nvl\n"
        "patterns collapse NULL → False/0 on staging columns:\n\n"
        + "\n".join(
            f"  {rel}:{lno}  [col={col}]  {line.strip()}"
            for rel, col, lno, line in violations
        )
        + "\n\nFix: use a nullable case expression:\n"
        "  case when X is null then null when X = 'true' then true else false end\n"
        "OR add (staging_path, col_name): 'reason' to _INTENTIONAL_COALESCES with a non-empty reason."
    )
```

- [ ] **Step 2: Run — expect pass after B5's is_cross fix**

```bash
uv run pytest src/tests/test_nullable_boolean_coalesce_audit.py -v
```

Expected: test passes. If it FAILs, the failure output lists other Mode-5 patterns that existed pre-G4 — triage each: either fix them (extend the PR scope) or add them to `_INTENTIONAL_COALESCES` with a non-empty reason. **Surface every finding to the user; do not silently add entries.**

- [ ] **Step 3: Lint + format**

```bash
uv run ruff check src/tests/test_nullable_boolean_coalesce_audit.py && uv run ruff format --check src/tests/test_nullable_boolean_coalesce_audit.py
```

---

## Task B7 — G5: Shrink `INITIAL_BRONZE_STAGING_GAPS` to `{}`

**Files:**
- Modify: `dbt_project/models/staging/idsse/_idsse__models.yml` (2 pairs)
- Modify: `dbt_project/models/staging/skillcorner/_skillcorner__models.yml` (1 pair)
- Modify: `dbt_project/models/staging/metrica/_metrica__models.yml` (2 pairs)
- Modify: `dbt_project/models/staging/statsbomb/_statsbomb__models.yml` (4 pairs)
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml` (2 pairs)
- Modify: `src/tests/test_staging_coverage.py` — shrink `INITIAL_BRONZE_STAGING_GAPS` incrementally; final state = `{}`

This is mechanical but voluminous. Break into one task per provider to keep each diff reviewable.

The strategy per pair:
1. Read the staging `.sql` file + extract the actual SELECT output columns (all `col as renamed_col` + bare `col` references in the final SELECT).
2. Read the `_<provider>__models.yml` entry for that staging model.
3. Identify cols in gap set that ARE already in the SQL SELECT but MISSING from yml `columns:` → add a `columns:` entry with `name:` + `description:` (use placeholder description referencing the bronze description).
4. Identify cols in gap set that are NOT in the SQL SELECT → either add them to the SQL or keep them in `INITIAL_BRONZE_STAGING_GAPS` with a reason (defer to that pair's per-provider task).
5. Remove the pair from `INITIAL_BRONZE_STAGING_GAPS` once the yml reflects all SQL outputs AND all bronze sources (modulo RENAMES + deliberate drops).
6. Run `uv run pytest src/tests/test_staging_coverage.py -v` to confirm the specific pair passes.

### B7.1: IDSSE (2 pairs: idsse_events, idsse_tracking)

- [ ] **Step 1: Extract staging SELECT output for stg_idsse__events**

```bash
grep -cE "^\s+[a-z_][a-z0-9_]*\s*(,|as\b)" dbt_project/models/staging/idsse/stg_idsse__events.sql
```

Then full column list — read the file + parse the last `select` block.

- [ ] **Step 2: Compare to bronze `_idsse__sources.yml`**

Compute: `bronze_cols - renames.keys() - intentional_drops - staging_cols` = what to add to yml.

- [ ] **Step 3: Edit `_idsse__models.yml`**

Add missing `columns:` entries for `stg_idsse__events` model. Same for `stg_idsse__tracking` (at minimum, add `timestamp`).

- [ ] **Step 4: Remove IDSSE pairs from `INITIAL_BRONZE_STAGING_GAPS`**

In `src/tests/test_staging_coverage.py` lines 113-194, delete the `("idsse", "idsse_events")` and `("idsse", "idsse_tracking")` entries.

- [ ] **Step 5: Run test — expect pass on IDSSE pairs**

```bash
uv run pytest src/tests/test_staging_coverage.py::TestStagingCoverage -v -k "idsse"
```

### B7.2: SkillCorner (1 pair)

Same pattern. Remove `("skillcorner", "skillcorner_tracking")` from the gap set after `_skillcorner__models.yml` is expanded.

### B7.3: Metrica (2 pairs: metrica_events, metrica_tracking)

Same pattern.

### B7.4: StatsBomb (4 pairs: events, matches, lineups, 360)

Largest — StatsBomb events gap has 82 cols. Allocate extra time. Many of the gap-set cols (e.g., `bad_behaviour_card`, `50_50`, `ball_recovery_offensive`) are raw StatsBomb event-type prefixed cols that staging does surface (as-is) but yml doesn't document. Confirm by reading `stg_statsbomb__events.sql`.

### B7.5: Wyscout (2 pairs)

Same pattern.

### B7.6: Drop the entire constant OR confirm it's `{}`

- [ ] **Step 1: Confirm `INITIAL_BRONZE_STAGING_GAPS` is empty**

```bash
grep -A 2 "INITIAL_BRONZE_STAGING_GAPS" src/tests/test_staging_coverage.py | head -5
```

If the dict is `{}`, optionally simplify:
```python
INITIAL_BRONZE_STAGING_GAPS: dict[tuple[str, str], set[str]] = {}
```

- [ ] **Step 2: Add a test asserting the dict is empty**

Add to `TestCoverageInvariants` class:
```python
def test_gaps_snapshot_is_empty(self) -> None:
    """Post-G5: every pair in PROVIDER_COVERAGE has its bronze cols
    either carried through, renamed, or intentionally dropped (via
    sources.yml). Leaving a gap entry here is documentation drift."""
    assert INITIAL_BRONZE_STAGING_GAPS == {}, (
        "Reopening a bronze→staging gap is allowed but must ship with a "
        "reason: add a per-col comment + either document the col in "
        "models.yml or add it to RENAMES."
    )
```

- [ ] **Step 3: Run full coverage test**

```bash
uv run pytest src/tests/test_staging_coverage.py -v --no-header
```

Expected: all 11 `test_bronze_col_coverage[...]` parametrize cases pass; the new `test_gaps_snapshot_is_empty` passes; all 3 `TestCoverageInvariants` tests pass.

---

## Task B8 — Wheel bump + final regression

PR B touches `src/ingestion/*` → wheel bump required.

- [ ] **Step 1: Confirm current wheel version**

```bash
grep -E "^version\s*=" pyproject.toml
grep "WHEEL_VERSION" src/shared/wheel.py
```
Expected: `0.3.9` on both.

- [ ] **Step 2: Bump pyproject.toml**

Edit `pyproject.toml`:
```toml
version = "0.3.10"
```

- [ ] **Step 3: Run bump_wheel.py to propagate**

```bash
uv run python scripts/bump_wheel.py
```

Expected: `src/shared/wheel.py` + static consumers (deploy.sh, Terraform, `scripts/*_hf.py` PEP 723 headers) all updated to `0.3.10`.

- [ ] **Step 4: `--check` confirms lockstep**

```bash
uv run python scripts/bump_wheel.py --check
```
Expected: no drift.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest --no-header 2>&1 | tail -40
```

(Use `run_in_background=true` + poll; full suite is long.)

Expected: all pre-existing tests green; all new tests in PR A + PR B green.

- [ ] **Step 6: Import boundary + workflow card validation**

```bash
uv run lint-imports && uv run validate_workflow_cards workflow-cards/
```
Expected: 0 violations; all cards validate.

- [ ] **Step 7: Ruff + pyright over entire src/**

```bash
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ hf_taipy_app/src/
```
Expected: all clean.

- [ ] **Step 8: Diff summary**

```bash
git diff --stat main
```

Expected files (rough count):
- 2 ingestion files (statsbomb.py, wyscout.py)
- 3 new test files (test_{statsbomb,wyscout}_bronze_expected_cols.py, test_nullable_boolean_coalesce_audit.py)
- 1 existing test file (test_bronze_live_schema.py)
- 1 SQL file (stg_idsse__passes.sql)
- 5 yml files (one per provider) under dbt_project/models/staging/
- 1 test file (test_staging_coverage.py)
- pyproject.toml + src/shared/wheel.py + any static consumers bump_wheel.py touches

---

## Task B9 — Commit + PR B

- [ ] **Step 1: **[PAUSE — user approval]** Commit PR B as a single commit**

```bash
git add -A  # Review first with git status
# Or specific files per the Step 8 diff
```

Actually use specific file adds per the single-commit convention; avoid `-A` per CLAUDE.md staging-file rule. Confirm via `git status` before committing.

```bash
git commit -m "$(cat <<'EOF'
feat(bronze): drop-safety tail — G1 StatsBomb/Wyscout finalize_bronze_df + G4 is_cross nullable + G5 gaps → {}

Closes the remaining three gaps from the PR #173 bronze drop-safety audit
(Medium severity, Modes 1/2/5):

- G1 (Mode 1): finalize_bronze_df wired into all 6 StatsBomb + Wyscout
  bronze writers. Expected-col constants loaded from the existing
  snapshot fixtures. test_{statsbomb,wyscout}_bronze_expected_cols.py
  enforces snapshot-vs-constant lockstep. Live-schema tests in
  test_bronze_live_schema.py now cover statsbomb_{competitions,matches,
  events} + wyscout_events.
- G4 (Mode 5): stg_idsse__passes.is_cross uses a nullable case
  expression; NULL preserved for DFL matches where play_flat_cross
  was not recorded. test_nullable_boolean_coalesce_audit.py scans
  every stg_*.sql for analogous Mode-5 patterns and fails on any
  not in _INTENTIONAL_COALESCES.
- G5 (Mode 2): every _<provider>__models.yml now documents the full
  staging SELECT output across all 11 (provider, bronze_table) pairs.
  INITIAL_BRONZE_STAGING_GAPS shrunk to {}; new invariant test
  asserts the dict stays empty.

Wheel bumped 0.3.9 → 0.3.10 (src/ingestion/* changes).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 2: **[PAUSE — user approval]** Push + open PR B**

```bash
git push -u origin cleanup/bronze-drop-safety-tail
gh pr create --title "feat(bronze): drop-safety tail — G1 finalize_bronze_df + G4 is_cross nullable + G5 gaps → {}" --body "$(cat <<'EOF'
## Summary

Closes the remaining three gaps (G1 + G4 + G5, Medium severity) from the PR #173 bronze drop-safety audit. Ships after PR A (`safety/bronze-drop-safety-teeth`) lands on main.

- **G1 (Mode 1 writer drop):** `finalize_bronze_df` wired into StatsBomb (3 spots) + Wyscout (3 spots) bronze writers. Live-schema tests in `test_bronze_live_schema.py` now cover all 4 previously-excluded tables.
- **G4 (Mode 5 silent sentinel):** `stg_idsse__passes.is_cross` uses a nullable `case` expression. New `test_nullable_boolean_coalesce_audit.py` scans all staging SQL for analogous Mode-5 patterns.
- **G5 (Mode 2 documentation drift):** every `_<provider>__models.yml` now documents the full staging SELECT output. `INITIAL_BRONZE_STAGING_GAPS` shrunk to `{}` across all 11 pairs.

Wheel bumped 0.3.9 → 0.3.10 per src/ingestion/* changes.

## Test plan

- [x] All pre-existing `pytest` tests green locally
- [x] `test_{statsbomb,wyscout}_bronze_expected_cols.py` — 5+3 pass
- [x] `test_nullable_boolean_coalesce_audit.py` — pass (no Mode-5 patterns outside `_INTENTIONAL_COALESCES`)
- [x] `test_staging_coverage.py` — all 11 parametrize cases + `test_gaps_snapshot_is_empty` pass
- [x] `uv run lint-imports` clean
- [x] `uv run python scripts/bump_wheel.py --check` clean
- [ ] `bronze-live-schema.yml` post-merge workflow runs green with the new statsbomb + wyscout live-schema tests
- [ ] IDSSE `is_cross` NULL count non-zero in dev staging after dbt build

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: **[PAUSE — wait for CI + merge]**

Once CI is green + user merges:

```bash
git checkout main && git pull && git branch -d cleanup/bronze-drop-safety-tail
```

- [ ] **Step 4: Verify post-merge live-schema workflow run**

After merge, trigger the workflow manually or wait for the next nightly:

```bash
gh workflow run bronze-live-schema.yml
# Wait ~2 minutes
gh run list --workflow=bronze-live-schema.yml --limit 3
```

Expected: all tests pass or skip for documented reason.

---

# Self-Review

Before execution starts, the author (me) ran the following checks:

**1. Spec coverage**
- G1 → Tasks B1, B2, B3, B4 ✓
- G2 → Task A2 ✓
- G3 → Task A1 ✓
- G4 → Tasks B5, B6 ✓
- G5 → Task B7 ✓
- G6 → Task A3 ✓
- Wheel bump (required by PR B per CLAUDE.md wheel rule) → Task B8 ✓
- Single-commit-per-branch convention → Tasks A4/B9 commit at end ✓
- Approval gates for commit + push + PR → marked `**[PAUSE — user approval]**` ✓

**2. Placeholder scan**
- No "TBD", "implement later", or "fill in details" markers left.
- Every step with a code change has the exact code block.
- Every shell command has an explicit expected output.
- Task B1/B3 include a data-probe step to resolve the fixture's actual JSON key names before wiring the constants — this is deliberate (we must not hardcode a key that might not exist).

**3. Type consistency**
- `finalize_bronze_df(df, expected_cols, dtype_overrides)` signature matches `src/ingestion/utils.py:503-558`.
- `_normalize_mixed_types(df, logger: logging.Logger | None = None)` is a new backward-compatible signature.
- `_write_batch(..., expected_cols: tuple[str, ...] = ())` default preserves existing call-site behaviour until updated.
- Expected-col constant names are consistent between definition (Task B1/B3), tests (Task B1/B3), and use sites (Task B2/B3).
- `INITIAL_BRONZE_STAGING_GAPS: dict[tuple[str, str], set[str]]` type preserved; final value `{}` satisfies type.

**4. Assumptions flagged for confirmation at execution**
- Snapshot JSON top-level keys (Task B1 Step 1, B3 Step 1). If they differ from `statsbomb_{competitions,matches,events,lineups,360}` / `wyscout_{events,matches,players}`, adjust code + tests to match — plan assumes canonical names; actual inspection supersedes.
- `DATABRICKS_HTTP_PATH` repo variable (Task A3 Step 4). If missing, requires user to set it. Plan flags this as a PAUSE.
- Schema contract on `is_cross` (Task B5 Step 4). If contracted as `NOT NULL`, extend PR scope to relax the contract — surface to user, don't silently relax.

---

## Handoff

Plan saved to `docs/superpowers/plans/2026-04-22-bronze-drop-safety-sweep.md`.

**Execution options:**

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration. Best for the 15-ish discrete tasks here.
2. **Inline Execution** — execute in this session using `superpowers:executing-plans`, batch execution with checkpoints.

Which approach? (Default: Inline; PR A first, pause before push, then PR B after PR A merges.)
