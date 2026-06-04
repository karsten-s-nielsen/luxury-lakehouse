# AC-1 Ghost-GK Backend Selection + Period Work-Units Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.
>
> **Commit discipline (PROJECT HARD RULE):** one commit per PR, created only with explicit user approval (sentinel-gated). Tasks end at **verification**, NOT a per-task `git commit`. The single commit is the final task, gated on the user. Do **not** `git commit` between tasks.
>
> **Review incorporated (2026-06-03):** behavioral tests replace source-string asserts; resolver raises `ValueError` (caught at CLI); pure-helper extraction for the Spark-bound invariants; HF card + AI-gov added; silly-kicks **4.11.0** floor bump + golden re-baseline **bundled** (decision A). See spec: `docs/superpowers/specs/2026-06-03-ac1-ghost-gk-backend-and-period-units-design.md`.

**Goal:** Make the AC-1 ghost-GK `kde_backend` selectable (per-installation default → per-run override → per-row provenance), default all tracking providers to per-period work-units, add watchdog/timeout headroom, and bump silly-kicks 4.9.1 → 4.11.0 (absorbing 4.10.0's ghost-GK re-baseline; **no** 4.11.0 xCrossAttempt column).

**Architecture:** `kde_backend` is domain policy carried on the `WorkUnit` (queue = single source of truth); a pure resolver owns precedence + allowlist; `watchdog_budget_s` is infra policy on the drain worker. Default `fft-cic` everywhere ⇒ no *backend-driven* value change (the only value shift is the 4.10.0 library bump).

**Tech Stack:** Python 3.10, PySpark/Delta, dbt, Terraform, pytest. silly-kicks 4.11.0.

**Task order rationale:** 4.11.0 bump first (env). Resolver/WorkUnit/schema/queue before threading. Threading before the CLI arg-parse (T9) which resolves the backend, which precedes the enqueue call-site (T10) that consumes it. Goldens re-baselined late (after all schema/value changes). No forward references.

---

## Task 1: silly-kicks 4.9.1 → 4.11.0 floor bump + adoption

**Files:** `pyproject.toml:27`; `scripts/submit_ac1_oneshot.py:49` (PEP 723 dep); `scripts/train_football2vec.py`, `train_football2vec_360.py`, `train_football2vec_v2.py`, `train_scoutgpt_hf.py`, `train_vaep_model_hf.py`, `train_xg_v2_hf.py` (`_REQUIRED_SK_MIN`); `src/tests/test_sk3_mig_b_orchestrator_invariants.py` (the §2.10.5 hardcoded `(4, 9, 1)` literal). **Decision (#3): bump all — keep every silly-kicks floor in sync.**

- [ ] **Step 1: Evidence gate — enumerate the changelog (review v2 #2)**

Read `D:\Development\karstenskyt__silly-kicks_part-deux\CHANGELOG.md` entries for 4.10.0 + 4.11.0. Confirm the AC-1-relevant numeric footprint is **`ghost_gk_x/y/spread` only** (4.10.0 serve-carrier fix + default re-fit); xS bit-identical; DAS/pitch-control/OBSO/PAUSA/shape-graph/line-breaking/team-shape unchanged; 4.11.0 `xCrossAttempt` not wired into any xfn list. Record this enumeration — it is the acceptance criterion for the Task 16 golden diff. If the changelog contradicts this, STOP and re-scope the re-baseline.

- [ ] **Step 2: Bump the AC-1 pins**
  - `pyproject.toml:27`: `"silly-kicks[das,ghost-gk]>=4.9.1,<5"` → `>=4.11.0,<5`.
  - `submit_ac1_oneshot.py:49`: `"silly-kicks>=4.9.1,<5"` → `>=4.11.0,<5`.

- [ ] **Step 3: trainer + sentinel pins (decision #3 = bump all)**
  - In the 6 trainers: `_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 9, 1)` → `(4, 11, 0)`.
  - `src/tests/test_sk3_mig_b_orchestrator_invariants.py` §2.10.5 (L257/292/311-315): change the hardcoded expected `(4, 9, 1)` → `(4, 11, 0)` in the docstring/message text AND the assertion literal (the test pins trainers to a LITERAL, not to the pyproject floor — without this edit it fails). Run `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py -v` after — expect PASS.

- [ ] **Step 4: Adopt (NEVER pip --force-reinstall)**

Run: `uv lock --refresh-package silly-kicks` then `uv sync --inexact`

- [ ] **Step 5: Verify installed version + allowlist parity**

Run: `uv run python -c "import importlib.metadata as m; print(m.version('silly-kicks'))"`
Expected: `4.11.0`

Run: `uv run python -c "from silly_kicks.tracking.features import add_ghost_gk; import inspect; print('kde_backend' in inspect.signature(add_ghost_gk).parameters)"`
Expected: `True`. Confirm the docstring's `kde_backend` set is exactly `{vectorized, scipy, cpu-numba, fft, fft-cic}` (matches `GHOST_GK_KDE_BACKENDS` added in Task 2). If it differs, STOP and reconcile the allowlist.

> Note: the mini-golden test will now FAIL until Task 16 re-baselines it (4.10.0 shifts `ghost_gk_x/y`). Expected; do not regen yet (schema also changes in Task 4/6).

---

## Task 2: Ghost-GK backend resolver (pure domain function, raises ValueError)

**Files:** Create `src/analytics/action_context/ghost_gk_backend.py`; Test `src/tests/action_context/test_ghost_gk_backend_resolver.py`.

- [ ] **Step 1: Write the failing test**

```python
# src/tests/action_context/test_ghost_gk_backend_resolver.py
import pytest

from analytics.action_context.ghost_gk_backend import (
    DEFAULT_GHOST_GK_BACKEND,
    GHOST_GK_KDE_BACKENDS,
    resolve_ghost_gk_backend,
)


def test_default_when_all_unset():
    assert resolve_ghost_gk_backend(None, None) == "fft-cic"
    assert resolve_ghost_gk_backend("", "") == "fft-cic"
    assert DEFAULT_GHOST_GK_BACKEND == "fft-cic"


def test_explicit_wins_over_installation_default():
    assert resolve_ghost_gk_backend("cpu-numba", "vectorized") == "cpu-numba"


def test_installation_default_when_no_explicit():
    assert resolve_ghost_gk_backend(None, "scipy") == "scipy"
    assert resolve_ghost_gk_backend("  ", "scipy") == "scipy"  # whitespace == unset


def test_all_five_backends_accepted():
    assert GHOST_GK_KDE_BACKENDS == {"scipy", "vectorized", "cpu-numba", "fft", "fft-cic"}
    for b in GHOST_GK_KDE_BACKENDS:
        assert resolve_ghost_gk_backend(b, None) == b


def test_unknown_backend_raises_valueerror_not_systemexit():
    # ValueError, NOT SystemExit — the domain layer must not raise a process-control exception.
    with pytest.raises(ValueError, match="Unknown ghost-GK backend"):
        resolve_ghost_gk_backend("gpu-magic", None)
    with pytest.raises(ValueError, match="Unknown ghost-GK backend"):
        resolve_ghost_gk_backend(None, "bogus")
```

- [ ] **Step 2: Run — expect FAIL** (`ModuleNotFoundError`)

Run: `uv run pytest src/tests/action_context/test_ghost_gk_backend_resolver.py -v`

- [ ] **Step 3: Implement**

```python
# src/analytics/action_context/ghost_gk_backend.py
"""Ghost-GK KDE backend selection policy (domain layer — stdlib only).

Resolved ONCE at the adapter boundary (preflight/oneshot), stamped onto each WorkUnit; the processor
consumes the resolved value. Precedence: explicit per-run flag > per-installation default > fallback.
Raises ValueError on an unknown value — the CLI boundary translates that to SystemExit. See
docs/superpowers/specs/2026-06-03-ac1-ghost-gk-backend-and-period-units-design.md.
"""

from __future__ import annotations

GHOST_GK_KDE_BACKENDS: frozenset[str] = frozenset({"scipy", "vectorized", "cpu-numba", "fft", "fft-cic"})
DEFAULT_GHOST_GK_BACKEND = "fft-cic"


def resolve_ghost_gk_backend(explicit: str | None, installation_default: str | None) -> str:
    """Resolve by precedence: explicit > installation default > fallback. Empty/whitespace == unset."""
    for candidate in (explicit, installation_default, DEFAULT_GHOST_GK_BACKEND):
        val = candidate.strip() if candidate and candidate.strip() else None
        if val is None:
            continue
        if val not in GHOST_GK_KDE_BACKENDS:
            raise ValueError(f"Unknown ghost-GK backend {val!r}. Valid: {sorted(GHOST_GK_KDE_BACKENDS)}")
        return val
    return DEFAULT_GHOST_GK_BACKEND
```

- [ ] **Step 4: Run — expect PASS** (5 tests)

---

## Task 3: `WorkUnit.kde_backend` field + `__post_init__` validation

**Files:** Modify `src/analytics/action_context/work_unit.py`; Test `src/tests/action_context/test_work_unit_kde_backend.py`.

- [ ] **Step 1: Write the failing test**

```python
# src/tests/action_context/test_work_unit_kde_backend.py
import pytest

from analytics.action_context.work_unit import WorkUnit


def test_kde_backend_defaults_to_fft_cic():
    assert WorkUnit(provider="skillcorner", match_id="1899585").kde_backend == "fft-cic"


def test_kde_backend_explicit_valid():
    u = WorkUnit(provider="metrica", match_id="X", period=1, kde_backend="cpu-numba")
    assert u.kde_backend == "cpu-numba" and u.period == 1


def test_invalid_kde_backend_rejected_before_queue():
    with pytest.raises(ValueError, match="Unknown ghost-GK backend|kde_backend"):
        WorkUnit(provider="metrica", match_id="X", kde_backend="typo")
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add the field + validation**

In `work_unit.py`, add after `frame_range`:

```python
    kde_backend: str = "fft-cic"

    def __post_init__(self) -> None:
        from analytics.action_context.ghost_gk_backend import GHOST_GK_KDE_BACKENDS

        if self.kde_backend not in GHOST_GK_KDE_BACKENDS:
            raise ValueError(f"Unknown ghost-GK backend {self.kde_backend!r}. Valid: {sorted(GHOST_GK_KDE_BACKENDS)}")
```

(`__post_init__` only reads `self` — valid on a frozen dataclass.)

- [ ] **Step 4: Run — expect PASS**

---

## Task 4: `ghost_gk_method` column in schema constants (110 → 111)

**Files:** Modify `src/analytics/action_context/schema.py`; extend `src/tests/test_action_context_createdataframe_schema.py`.

- [ ] **Step 1: Extend the failing test** — add to the existing column-presence test:

```python
    assert "ghost_gk_method" in RESULT_COLUMNS
    assert "ghost_gk_method STRING" in ACTION_CONTEXT_DDL
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add the column**
  - `RESULT_COLUMNS`: after `"pitch_control_method",` and before `"_ingested_at"`, add `"ghost_gk_method",` with a `# Ghost-GK backend provenance (1)` comment.
  - `ACTION_CONTEXT_DDL`: change `"xshot_occurrence DOUBLE, pitch_control_method STRING, "` → `"xshot_occurrence DOUBLE, pitch_control_method STRING, ghost_gk_method STRING, "`.
  - Count comment (~L17): `provenance (1)` → `provenance (2)`, `= 110` → `= 111`.

- [ ] **Step 4: Run — expect PASS** (incl. the StructType auto-derives from the DDL, count = 111)

---

## Task 5: Queue carries `kde_backend` (column + enqueue + pure `_row_to_work_unit` + migrations)

**Files:** Modify `src/ingestion/action_context_queue.py`; create `scripts/migrations/2026-06-03-add-kde-backend-to-action-context-work-queue.sql`; modify `scripts/migrations/2026-06-02-create-action-context-work-queue.sql`; Test `src/tests/action_context/test_queue_row_to_work_unit.py`.

- [ ] **Step 1: Write the failing behavioral test** (pure — no Spark)

```python
# src/tests/action_context/test_queue_row_to_work_unit.py
from ingestion.action_context_queue import _QUEUE_COLUMNS, _row_to_work_unit


def test_queue_has_kde_backend_column():
    assert "kde_backend" in [name for name, _, _ in _QUEUE_COLUMNS]


def test_null_kde_backend_reads_back_as_default():
    row = {"provider": "metrica", "match_id": "X", "period": 1,
           "frame_range_lo": None, "frame_range_hi": None, "kde_backend": None}
    assert _row_to_work_unit(row).kde_backend == "fft-cic"


def test_explicit_kde_backend_roundtrips():
    row = {"provider": "skillcorner", "match_id": "Y", "period": 2,
           "frame_range_lo": None, "frame_range_hi": None, "kde_backend": "cpu-numba"}
    u = _row_to_work_unit(row)
    assert u.kde_backend == "cpu-numba" and u.period == 2
```

- [ ] **Step 2: Run — expect FAIL** (no `_row_to_work_unit`, no column)

- [ ] **Step 3: Add column, enqueue write, and the pure reconstruction helper**

In `action_context_queue.py`:
- `_QUEUE_COLUMNS`: append `("kde_backend", "string", True)`.
- `enqueue()`: append `a.unit.kde_backend` to the row tuple (same position as the new column).
- Extract the row→WorkUnit reconstruction into a pure module-level helper (accepts a mapping/Row; defaults NULL → `"fft-cic"`):

```python
def _row_to_work_unit(row) -> WorkUnit:
    lo = row["frame_range_lo"]
    fr = (lo, row["frame_range_hi"]) if lo is not None else None
    kde = row["kde_backend"] if row["kde_backend"] else "fft-cic"
    return WorkUnit(provider=row["provider"], match_id=row["match_id"], period=row["period"],
                    frame_range=fr, kde_backend=kde)
```

`units_for_worker()` builds its list via `[_row_to_work_unit(r) for r in df.collect()]` (Spark `Row` supports `r["col"]`).

- [ ] **Step 4: Migrations** — create `2026-06-03-add-kde-backend-to-action-context-work-queue.sql`:

```sql
-- Adds the ghost-GK kde_backend policy/provenance column to the AC-1 work queue (domain policy on the
-- work spec; drain reads it per-unit). See ADR-035 amendment. Idempotent: ALTER ... ADD COLUMNS is
-- skip-if-exists handled by scripts/migrations/_runner.py.
ALTER TABLE soccer_analytics.observability.action_context_work_queue ADD COLUMNS (kde_backend STRING);
```

Then add `kde_backend STRING` to the column list in the canonical CREATE TABLE in
`2026-06-02-create-action-context-work-queue.sql` (it is `CREATE TABLE IF NOT EXISTS` → no-op on the live
table; keeps `test_work_queue_schema_parity.py` matching `_QUEUE_COLUMNS`).

- [ ] **Step 5: Run — expect PASS**

Run: `uv run pytest src/tests/action_context/test_queue_row_to_work_unit.py "src/tests" -k work_queue_schema_parity -v`

---

## Task 6: Thread `kde_backend` through `enrich.py` + set `ghost_gk_method` (HEADLINE behavioral test)

**Files:** Modify `src/analytics/action_context/enrich.py`; Test `src/tests/action_context/test_ghost_gk_method_provenance.py`.

> This is the crown-jewel test (review #2). It runs the **real** enrich and proves the chosen backend reaches the **computation** (spy on `add_ghost_gk`), not just the label.

- [ ] **Step 1: Write the failing behavioral test**

```python
# src/tests/action_context/test_ghost_gk_method_provenance.py
"""The selected kde_backend reaches add_ghost_gk (computation), and ghost_gk_method == that backend."""
from __future__ import annotations

import pandas as pd

_ROOT = "src/tests/fixtures/action_context"


def _recompute(kde_backend: str) -> pd.DataFrame:
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource, ParquetFrameSource, ParquetMatchMetadataSource, ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu, result_df):
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1, kde_backend=kde_backend),
        frames=ParquetFrameSource(_ROOT), actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT), meta=ParquetMatchMetadataSource(_ROOT), sink=sink,
    )
    assert sink.df is not None
    return sink.df


def test_backend_reaches_computation_and_label(monkeypatch):
    # We patch the SOURCE module (silly_kicks.tracking.features), not `enrich.add_ghost_gk`, because
    # enrich.py imports add_ghost_gk FUNCTION-LOCALLY (a per-call `from ... import add_ghost_gk` inside
    # _enrich_tracking_match / _enrich_sb360_match) — there is no module-level enrich.add_ghost_gk to patch.
    # If a future refactor hoists that import to module scope, this patch silently misses (`seen` stays
    # empty → "got [] expected all 'fft'"); patch `analytics.action_context.enrich.add_ghost_gk` instead.
    import silly_kicks.tracking.features as skf

    seen: list[str] = []
    real = skf.add_ghost_gk

    def spy(*args, **kwargs):
        seen.append(kwargs.get("kde_backend"))
        return real(*args, **kwargs)

    monkeypatch.setattr(skf, "add_ghost_gk", spy)
    result = _recompute("fft")  # non-default (default is fft-cic) → proves selection, fast-approx → quick

    assert seen and all(b == "fft" for b in seen), f"add_ghost_gk got {seen}, expected all 'fft'"
    assert (result["ghost_gk_method"] == "fft").all(), "ghost_gk_method label != selected backend"
```

- [ ] **Step 2: Run — expect FAIL** (no `ghost_gk_method`; backend not threaded → spy sees `fft-cic`)

- [ ] **Step 3: Thread the param + set provenance**

In `enrich.py`:
- Add `kde_backend: str = "fft-cic"` to `_enrich_tracking_match` and `_enrich_sb360_match`.
- Replace `kde_backend="fft-cic"` in BOTH `add_ghost_gk(...)` calls (~L304 tracking, ~L433 SB360) with `kde_backend=kde_backend`.
- After each `add_ghost_gk(...)` assignment, add `out["ghost_gk_method"] = kde_backend`.
- In `_enrich_event_only_match`, add `out["ghost_gk_method"] = None` (NULL provenance, mirrors `pitch_control_method`).

- [ ] **Step 4: Run — expect PASS**

---

## Task 7: Thread `kde_backend` through `pipeline.py`

**Files:** Modify **`src/analytics/action_context/pipeline.py`** (`enrich_batch` @ L177, `run_work_unit` @ L260).

> **Correct module (review v2 #1):** these functions live in `analytics/action_context/pipeline.py`, NOT `ingestion/pipeline.py` (which does not exist). The chain crosses layers: `ingestion.action_context._make_action_context_udf` (Spark UDF factory) → `analytics.action_context.pipeline.enrich_batch`. The headline test (T6) already imports the correct path.

- [ ] **Step 1: Thread the param**
  - `enrich_batch`: add `kde_backend: str = "fft-cic"`; pass into `_enrich_tracking_match(...)` / `_enrich_sb360_match(...)`.
  - `run_work_unit`: read `unit.kde_backend` and pass `kde_backend=unit.kde_backend` into its `enrich_batch(...)` call.

- [ ] **Step 2: Verify via the headline test** (already exercises `run_work_unit → enrich_batch → _enrich_tracking_match` with `kde_backend="fft"`)

Run: `uv run pytest src/tests/action_context/test_ghost_gk_method_provenance.py -v`
Expected: PASS (this is the behavioral proof the pipeline threading works end-to-end).

---

## Task 8: Thread `kde_backend` through the core + extract `_period_replace_where` (testable disjoint-write)

**Files:** Modify `src/ingestion/action_context.py` (`_make_action_context_udf`, `_process_tracking_match`, `_process_statsbomb_match` + SB360 helper, extract `_period_replace_where`); `src/ingestion/action_context_queue.py` (`SparkGameProcessor.process`); Test `src/tests/action_context/test_period_replace_where.py`.

- [ ] **Step 1: Write the failing pure test** (review #4 disjoint-write guard)

```python
# src/tests/action_context/test_period_replace_where.py
from ingestion.action_context import _period_replace_where


def test_period_scoped_predicate_includes_period_id():
    pred = _period_replace_where("J03WMX", 2)
    assert "match_id = 'J03WMX'" in pred and "period_id = 2" in pred


def test_whole_match_predicate_omits_period_id():
    pred = _period_replace_where("J03WMX", None)
    assert pred == "match_id = 'J03WMX'"
    assert "period_id" not in pred
```

- [ ] **Step 2: Run — expect FAIL** (no `_period_replace_where`)

- [ ] **Step 3: Extract the helper + thread the param**
  - Extract the existing replaceWhere predicate logic (action_context.py:1372-1375) into:

```python
def _period_replace_where(match_id: str, period_filter: int | None) -> str:
    if period_filter is not None:
        return f"match_id = '{match_id}' AND period_id = {period_filter}"
    return f"match_id = '{match_id}'"
```

  Use it at the write site (replace the inline branch).
  - Add `kde_backend: str = "fft-cic"` to `_make_action_context_udf` (capture in closure → pass to `enrich_batch`), `_process_tracking_match` (→ `_make_action_context_udf`), and `_process_statsbomb_match`/SB360 helper (→ SB360 enrich).
  - `SparkGameProcessor.process` (action_context_queue.py): pass `kde_backend=unit.kde_backend` to `_process_tracking_match(...)` and `_process_statsbomb_match(...)`.

- [ ] **Step 4: Run — expect PASS**, then **repo-wide caller grep** for `_process_tracking_match(`, `_make_action_context_udf(`, `_process_statsbomb_match(`, `enrich_batch(` (driver lives in BOTH `main` and `main_drain_worker`); confirm every call compiles (new params are defaulted → existing callers safe).

---

## Task 9: Parse `--ghost-gk-backend` in `main_preflight` + for-each `main`; resolve; catch ValueError→SystemExit

**Files:** Modify `src/ingestion/action_context.py` (new pure `_resolve_backend_or_exit`); Test `src/tests/action_context/test_resolve_backend_or_exit.py`.

> **Pure-helper extraction (review v2 #4):** the CLI's `ValueError → SystemExit` translation is extracted into `_resolve_backend_or_exit(explicit, env_default) -> str` so it is unit-testable WITHOUT Spark (the prior draft's test was mislabeled — it re-tested the resolver, not the translation). `main_preflight`/`main` call this helper.

- [ ] **Step 1: Write the failing test** (genuinely tests the translation)

```python
# src/tests/action_context/test_resolve_backend_or_exit.py
import pytest

from ingestion.action_context import _resolve_backend_or_exit


def test_invalid_backend_becomes_systemexit():
    # The CLI boundary translates the domain ValueError into SystemExit (operator fail-loud).
    with pytest.raises(SystemExit, match="Unknown ghost-GK backend"):
        _resolve_backend_or_exit("nope", None)


def test_valid_backend_returns_resolved():
    assert _resolve_backend_or_exit("cpu-numba", None) == "cpu-numba"
    assert _resolve_backend_or_exit(None, "scipy") == "scipy"
    assert _resolve_backend_or_exit(None, None) == "fft-cic"
```

- [ ] **Step 2: Run — expect FAIL** (`_resolve_backend_or_exit` does not exist)

- [ ] **Step 3: Add the pure helper + wire the arg**

Add the import + helper at module level in `action_context.py`:

```python
from analytics.action_context.ghost_gk_backend import resolve_ghost_gk_backend


def _resolve_backend_or_exit(explicit: str | None, env_default: str | None) -> str:
    """Resolve the ghost-GK backend, translating the domain ValueError into operator fail-loud SystemExit."""
    try:
        return resolve_ghost_gk_backend(explicit, env_default)
    except ValueError as e:
        raise SystemExit(str(e)) from e
```

In `main_preflight`, add to `extra_args` (after `--run-id`):

```python
        ("--ghost-gk-backend",
         {"type": str, "default": None,
          "help": "ghost-GK KDE backend; empty resolves to AC1_GHOST_GK_BACKEND env then fft-cic. "
                  "One of {scipy,vectorized,cpu-numba,fft,fft-cic}."}),
```

After parsing, resolve once and stamp units:

```python
    import os
    kde_backend = _resolve_backend_or_exit(args.ghost_gk_backend, os.environ.get("AC1_GHOST_GK_BACKEND"))
```

Pass `kde_backend` into every `WorkUnit(...)` built in this method (the IDSSE call site at ~L601 and the tracking call site changed in Task 10). In the for-each `main`, add the same arg + the same `_resolve_backend_or_exit(...)` call, and pass `kde_backend=kde_backend` into the `_process_tracking_match(...)` / `_process_statsbomb_match(...)` loop calls.

- [ ] **Step 4: Run — expect PASS**

---

## Task 10: Period work-units for all tracking providers

**Files:** Modify `src/ingestion/action_context.py` (new `_find_tracking_new_period_pairs`, replace `_find_tracking_new_ids` usage, update call site to emit per-period units with `kde_backend` resolved in Task 9); Test `src/tests/action_context/test_find_tracking_new_period_pairs.py`.

> `_find_tracking_new_period_pairs` is Spark-bound (`spark.table`); not pure-pandas-testable here. The test asserts its shape/structure; correctness is **live-validated** in Task 18 (a scoped tracking run produces per-`(match,period)` rows). Flagged honestly per the spec.

- [ ] **Step 1: Write the (shape) failing test**

```python
# src/tests/action_context/test_find_tracking_new_period_pairs.py
import inspect

from ingestion import action_context


def test_function_exists_with_provider_param_and_pair_return():
    fn = action_context._find_tracking_new_period_pairs
    params = inspect.signature(fn).parameters
    assert {"tracking_table", "spadl_table", "results_table", "provider"} <= set(params)
    assert fn.__annotations__.get("return") in ("list[tuple[str, int]]", list)
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add the function (mirror `_find_idsse_new_period_pairs`, parameterised by provider) + update the call site**

```python
def _find_tracking_new_period_pairs(
    spark: SparkSession, tracking_table: str, spadl_table: str, results_table: str, provider: str,
) -> list[tuple[str, int]]:
    from pyspark.sql import functions as F  # noqa: N812

    tracking_df = (
        spark.table(tracking_table)
        .select(F.col("match_id").cast("string").alias("_mid"), F.col("period").cast("bigint").alias("_period"))
        .distinct()
    )
    spadl_df = (
        spark.table(spadl_table).filter(F.col("data_source") == provider)
        .select(F.col("match_id_native").cast("string").alias("_mid")).distinct()
    )
    results_df = (
        spark.table(results_table).filter(F.col("data_source") == provider)
        .select(F.col("match_id").cast("string").alias("_mid"), F.col("period_id").cast("bigint").alias("_period"))
        .distinct()
    )
    new_df = tracking_df.join(spadl_df, "_mid", "inner").join(results_df, ["_mid", "_period"], "left_anti")
    return [(str(r["_mid"]), int(r["_period"])) for r in new_df.collect()]
```

Update the non-IDSSE tracking call site (~L603-612):

```python
    for prov, table in (("metrica", "metrica_tracking"), ("skillcorner", "skillcorner_tracking"),
                        ("gradientsports", "gradientsports_tracking")):
        if self._selected(prov):
            pairs = self._cap(
                _find_tracking_new_period_pairs(spark, f"{catalog}.bronze.{table}", spadl_table, results_table, prov)
            )
            units += [WorkUnit(provider=prov, match_id=mid, period=period, kde_backend=kde_backend)
                      for mid, period in pairs]
```

Add `kde_backend=kde_backend` to the IDSSE `WorkUnit(...)` at ~L601 too.

- [ ] **Step 4: Run — expect PASS**. Then grep for remaining `_find_tracking_new_ids` callers (Chesterton's fence); remove the dead function + any whole-match-enqueue test only if no caller remains.

---

## Task 11: Drain `--watchdog-budget-s` (guarded parse) + bump `WATCHDOG_BUDGET_S`

**Files:** Modify `src/analytics/action_context/drain.py:16`; `src/ingestion/action_context.py` (`main_drain_worker`); Test `src/tests/action_context/test_watchdog_budget.py`.

- [ ] **Step 1: Write the failing test**

```python
# src/tests/action_context/test_watchdog_budget.py
from analytics.action_context import drain


def test_watchdog_budget_default_2700():
    assert drain.WATCHDOG_BUDGET_S == 2700
```

- [ ] **Step 2: Run — expect FAIL** (still 1800)

- [ ] **Step 3: Bump + add guarded override arg**
  - `drain.py:16`: `WATCHDOG_BUDGET_S = 2700`.
  - `main_drain_worker` `extra_args`: add `--watchdog-budget-s` (str, default `None`). Parse with a guard (review #9):

```python
    raw = (args.watchdog_budget_s or "").strip()
    if raw:
        try:
            budget_s = int(raw)
        except ValueError as e:
            raise SystemExit(f"--watchdog-budget-s must be an integer, got {raw!r}") from e
        if budget_s <= 0:
            raise SystemExit(f"--watchdog-budget-s must be > 0, got {budget_s}")
    else:
        budget_s = WATCHDOG_BUDGET_S
```

  Pass `budget_s=budget_s` into `drain_worker(...)`.

- [ ] **Step 4: Run — expect PASS**

---

## Task 12: `submit_ac1_oneshot.py` — `--ghost-gk-backend` + `--timeout-seconds`

**Files:** Modify `scripts/submit_ac1_oneshot.py`; Test `src/tests/test_submit_ac1_oneshot_args.py`.

- [ ] **Step 1: Failing test**

```python
# src/tests/test_submit_ac1_oneshot_args.py
from pathlib import Path


def test_oneshot_exposes_backend_and_timeout():
    src = Path("scripts/submit_ac1_oneshot.py").read_text(encoding="utf-8")
    assert "--ghost-gk-backend" in src and "--timeout-seconds" in src and "timeout_seconds" in src
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add args**
  - `--ghost-gk-backend` (default `None`) → appended to `wheel_params` when set (reaches the for-each `main` from Task 9).
  - `--timeout-seconds` (int, default `7200`) → `jobs.SubmitTask(timeout_seconds=args.timeout_seconds)` (the oneshot escape hatch — the for-each path has no in-process watchdog).

- [ ] **Step 4: Run — expect PASS**

---

## Task 13: dbt — staging cast + mart column + contract

**Files:** `dbt_project/models/staging/action_context/stg_action_context__values.sql`; `dbt_project/models/marts/fct_action_context.sql`; `dbt_project/models/marts/_marts__models.yml`.

- [ ] **Step 1:** staging — after the `pitch_control_method` cast: `cast(ghost_gk_method as string) as ghost_gk_method,`.
- [ ] **Step 2:** mart — add `ghost_gk_method` to the `action_raw` CTE column list AND the `final` SELECT (mirror `pitch_control_method`).
- [ ] **Step 3:** contract — after `pitch_control_method`:

```yaml
      - name: ghost_gk_method
        data_type: string
        description: "Ghost-GK KDE backend that produced this row's ghost_gk_* values (scopes to ghost_gk_* only; orthogonal to pitch_control_method). NULL on event-only rows."
```

- [ ] **Step 4:** Run `uv run dbt parse` (from `dbt_project/`) — expect clean. **Note (review #6):** `contract: enforced` validates the type only on a **full-refresh**; an incremental absorbs it via `on_schema_change='append_new_columns'` (accepted pattern, same as PR #337). Confirm the post-merge `dbt-live-ci` does a full-refresh (or first-incremental is acceptable).

---

## Task 14: Terraform — variables, job params, task args, timeouts, env

**Files:** `terraform/modules/workflows/main.tf`.

- [ ] **Step 1: Variables**

```hcl
variable "ghost_gk_backend_default" {
  type        = string
  default     = "fft-cic"
  description = "Per-installation default ghost-GK KDE backend. Override (e.g. \"cpu-numba\") for always-accurate ghost-GK."
}
variable "watchdog_budget_s" {
  type        = string
  default     = ""
  description = "Optional per-game watchdog override seconds for the AC-1 drain worker (empty → in-code 2700)."
}
```

- [ ] **Step 2: Job parameters** (alongside `provider`/`max_units`):

```hcl
  parameter { name = "ghost_gk_backend"  default = var.ghost_gk_backend_default }
  parameter { name = "watchdog_budget_s" default = var.watchdog_budget_s }
```

- [ ] **Step 3: Task args**
  - `preflight_action_context` parameters: append `"--ghost-gk-backend", "{{job.parameters.ghost_gk_backend}}"`.
  - `compute_action_context` (drain) parameters: append `"--watchdog-budget-s", "{{job.parameters.watchdog_budget_s}}"`.
  - Set env `AC1_GHOST_GK_BACKEND = var.ghost_gk_backend_default` on the preflight/compute environment (follow the existing env-block pattern) so non-job entry points see the installation default.

- [ ] **Step 4: Timeouts** — `timeout_seconds = 300` → `600` on the 5 preflight tasks (L1054/1055, L1100/1101, L1128/1129, L1149/1150, L1196/1197).

- [ ] **Step 5: Validate** — `terraform -chdir=terraform validate`; `uv run pytest src/tests/ -k workflow_card -v`. Expect PASS.

---

## Task 15: oracle_map + `.toPandas()` line-key recheck

**Files:** `src/tests/action_context/oracle_map.py`; verify `src/tests/_topandas_exemptions.yml`.

- [ ] **Step 1:** add `"ghost_gk_method": ("categorical", None, None)` to `INVARIANT_ONLY` (after `pitch_control_method`).
- [ ] **Step 2:** re-grep the exempted `.toPandas()` line in `action_context.py` (Tasks 8/9 insert above it), update `_topandas_exemptions.yml`'s `line:`, then run `uv run pytest src/tests/test_topandas_boundedness.py -v` — expect PASS.

---

## Task 16: Re-baseline BOTH goldens (4.10.0 shift + new column)

**Files:** `src/tests/fixtures/action_context/idsse/J03WMXmini_p1/golden.parquet`, `.../J03WMX_p1/golden.parquet` (+ any oracle parquets the full regen touches).

> **Evidence-first (review v2 #2):** the changelog enumeration done in Task 1 establishes that the ONLY AC-1-relevant numeric change across 4.9.1→4.11.0 is `ghost_gk_x/y/spread` (4.10.0 serve-carrier fix + default re-fit); xS is bit-identical; DAS/pitch-control/OBSO/PAUSA/shape-graph/line-breaking/team-shape are unchanged. Use that as the acceptance criterion for the golden diff below — do NOT rubber-stamp.

- [ ] **Step 1: Regenerate the mini-golden** — `uv run python scripts/build_ac1_mini_golden.py`. **Inspect the diff against the Task-1 enumeration:** the ONLY expected changes are (a) the new `ghost_gk_method` column (`fft-cic`) and (b) a possible `ghost_gk_x/y/spread` shift. **TRIPWIRE — if any OTHER column's values changed (DAS, pitch_control, OBSO, PAUSA, shape_graph, line-breaking, team_shape, xshot_occurrence, …), STOP and reconcile against the silly-kicks 4.10.0/4.11.0 changelog before absorbing.** An unexplained shift = a regression, not a re-baseline.
- [ ] **Step 2: Regenerate the full `J03WMX_p1` golden** per its documented regen path (the HARD RULE requires BOTH after a tracking-enrichment value change); apply the same tripwire to its diff.
- [ ] **Step 3:** Run `uv run pytest src/tests/action_context/test_mini_golden.py -v` — expect PASS (recompute == regenerated golden). Confirm `ghost_gk_method` is non-NaN/`fft-cic` on the slice.

---

## Task 17: Docs — ADR amendments + CLAUDE.md + HF card + AI-gov + C4

**Files:** `docs/superpowers/adrs/ADR-035-silly-kicks-4-2-vectorized-ghost-gk-adoption.md`; `docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md`; `CLAUDE.md`; `docs/huggingface/dataset-cards/spadl-action-context.md`; `docs/c4/architecture.dsl`.

- [ ] **Step 1: ADR-035 amendment** — selectable backend (resolution hierarchy, per-unit on WorkUnit), `ghost_gk_method` provenance, exact-backend-overwrites-fft-cic consequence, and the bundled silly-kicks 4.11.0 floor (4.10.0 carrier re-baseline; 4.11.0 xCrossAttempt NOT consumed).
- [ ] **Step 2: ADR-037 amendment** — all tracking providers now per-`(match, period)` units (was IDSSE-only); per-game watchdog therefore per-half; 1800 → **2700** s + `--watchdog-budget-s` override; preflight timeout 300 → 600 s.
- [ ] **Step 3: CLAUDE.md "Performance Budgets"** — update the `compute_action_context` line: per-game watchdog **2700 s** (was 1800), overridable via `--watchdog-budget-s`.
- [ ] **Step 4: HF dataset card** (review #10) — `docs/huggingface/dataset-cards/spadl-action-context.md`: document `ghost_gk_method` (scope, values, NULL on event-only) and note any 4.10.0 `ghost_gk_*` value change. Run `uv run pytest src/tests/test_hf_publish_parity.py -v` — expect PASS.
- [ ] **Step 5: AI-gov determination** (review #11) — explicit: ghost-GK is a derived spatial metric, NOT a `PER_PLAYER_EVALUATIVE_CARDS` entry → `AI_GOVERNANCE.md` N/A. Run `uv run pytest src/tests/test_ai_governance_md.py -v` — expect PASS (no inventory change).
- [ ] **Step 6: C4** — only if the actionContext element description changes; EDIT concisely (≤200 chars), don't append. Regen per the C4 skill if edited.

---

## Task 18: Wheel bump, full verification, scoped live validation, single commit (USER-GATED)

- [ ] **Step 1: Wheel** — `uv run python scripts/bump_wheel.py` (0.5.15 → 0.5.16; never edit version manually).
- [ ] **Step 2: Lint/format** — `uv run ruff check src/ scripts/` + `uv run ruff format --check src/ scripts/` → zero violations.
- [ ] **Step 3: Type check (grep, NOT `| tail`)** — `uv run pyright src/ 2>&1 | grep -iE " - error:|errors?, [0-9]+ warning|0 errors"` → `0 errors`. Grep changed signatures repo-wide (both `main` + `main_drain_worker` driver paths).
- [ ] **Step 4: FULL suite** — `uv run pytest src/tests/` → PASS (incl. line-keyed guards: `test_topandas_boundedness`, `test_work_queue_schema_parity`, `validate_workflow_cards`, `test_action_context_createdataframe_schema`, `test_mini_golden`).
- [ ] **Step 5: Scoped live validation** (the Spark-bound paths — period-units, queue, preflight resolve — have no local test). After merge/deploy, run a small targeted AC-1 run and confirm: per-`(match,period)` queue rows, `ghost_gk_method` populated, a non-default backend run records its backend. (Operator step; present plan to user — do not self-trigger orchestrator before post-merge CI finishes per the wheel-deploy rule.)
- [ ] **Step 6: STOP — request explicit commit approval.** Present verification evidence. Do NOT `git commit` until the user gives explicit, in-the-moment approval (sentinel: user runs `!touch ~/.claude-git-approval`). Then a single commit:

```bash
git add -A
git commit -m "feat(ac-1): selectable ghost-GK backend + per-row provenance + period work-units; sk 4.11.0 (ADR-035/037 amendments)"
```

---

## Self-Review checklist

1. **Spec coverage** — every spec §10 file maps to a task: sk bump (T1), resolver (T2), WorkUnit+validation (T3), schema (T4), queue+helper+migration (T5), enrich+headline test (T6), pipeline (T7), core+`_period_replace_where` (T8), CLI args+resolve+catch (T9), period units (T10), drain override (T11), oneshot (T12), dbt (T13), terraform (T14), oracle_map/topandas (T15), goldens (T16), docs+HF+AI-gov (T17), wheel/verify/commit (T18). ✓
2. **Review v1 items addressed** — #2 headline behavioral test (T6); #3 structural→behavioral throughout; #4 `_period_replace_where` pure test (T8); #5 ValueError+CLI catch (T2/T9); #6 dbt full-refresh note (T13); #7 ordering fixed (T9 resolve before T10 call-site); #8 `__post_init__` (T3); #9 watchdog parse guard (T11); #10 HF card (T17); #11 AI-gov (T17). ✓
2b. **Review v2 items addressed** — #1 corrected `pipeline.py` path to `analytics/action_context/` (T7); #2 changelog evidence gate + Task-16 tripwire (T1/T16); #3 trainer pins made CONDITIONAL + sentinel-test bump noted (T1, pending user); #4 extracted pure `_resolve_backend_or_exit` + genuine translation test (T9); #5 spy hidden-dependency comment (T6). Quick-confirms: port class names verified against `test_mini_golden.py`; `ghost_gk_method` set on whole frame. ✓
3. **No forward references** — resolution (T9) precedes the enqueue call-site (T10); schema (T4) precedes enrich (T6); queue helper (T5) before processor (T8). ✓
4. **Type/name consistency** — `kde_backend: str` (param/field), `ghost_gk_method` (column), `resolve_ghost_gk_backend`/`GHOST_GK_KDE_BACKENDS`/`_period_replace_where`/`_row_to_work_unit` used consistently across tasks. ✓
