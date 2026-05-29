# AC-1 Action Context — Hexagonal Architecture + Performance Foundation: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make AC-1's action-context enrichment runnable and verifiable locally on one game (any provider, any chunk) so we can prove correctness against the legacy pipelines and profile the 30-min-per-IDSSE-half timeout — without deploying to Databricks.

**Architecture:** Extract the already-pure enrichment domain into `src/analytics/action_context/` behind five ports (`FrameSource`, `ActionsSource`, `XtSource`, `MatchMetadataSource`, `ResultSink`); keep Spark/Delta adapters + the `applyInPandas` UDF + CLI in `src/ingestion/action_context.py` as the composition root. A provider-parameterized extract tool writes committed Parquet fixtures; a tolerance-based differential harness compares against legacy oracles; a per-step + parallelism profiler localizes the timeout.

**Tech Stack:** Python 3.10, pandas/numpy, silly_kicks, PySpark (adapters only), Databricks SQL (extract), pytest, import-linter, pyarrow/parquet.

**Spec:** `docs/superpowers/specs/2026-05-28-action-context-hexagonal-and-perf-design.md` (rev 2, approved). Read §9.1 — the verified column-coverage matrix — before Phase C.

**Standing constraints (from project memory):**
- Wheel version: only via `uv run python scripts/bump_wheel.py` after editing `pyproject.toml` — never hand-edit derived files.
- No commits without explicit user approval + the git sentinel (`~/.claude-git-approval`). The "Commit" steps below are where to PAUSE and request approval, not auto-run.
- This plan does NOT change AC-1 outputs (Phases A0–A are behavior-preserving) and does NOT refactor other pipelines (converters are COPIED, legacy left untouched — M4).
- Targeted single-task / single-chunk Databricks runs + read-only pulls are fine; never trigger the full daily job or a full for_each fan-out.

---

## File Structure

**New — domain (pure; no pyspark):**
- `src/analytics/action_context/__init__.py` — public exports
- `src/analytics/action_context/work_unit.py` — `WorkUnit`, `MatchMeta`, `FrameBundle`, provider tiering
- `src/analytics/action_context/ports.py` — Protocols: `FrameSource`, `ActionsSource`, `XtSource`, `MatchMetadataSource`, `ResultSink`
- `src/analytics/action_context/convert.py` — provider bronze→frames conversion (COPIED from `ingestion.tracking_context` + GS converter)
- `src/analytics/action_context/enrich.py` — `enrich_tracking`, `enrich_sb360`, `enrich_event_only` (moved from `ingestion.action_context`)
- `src/analytics/action_context/schema.py` — `RESULT_COLUMNS`, `ACTION_CONTEXT_DDL`, `build_output`
- `src/analytics/action_context/pipeline.py` — `run_work_unit(wu, *, frames, actions, xt, meta, sink, profile=False)` + tier dispatch
- `src/analytics/action_context/profiling.py` — per-step timing wrapper
- `src/analytics/action_context/local/__init__.py`
- `src/analytics/action_context/local/parquet_sources.py` — Parquet adapters + `ParquetResultSink`

**New — adapters/tools/tests:**
- `scripts/extract_action_context_fixture.py` — `WorkUnit` → committed Parquet fixture (+ legacy oracle pull)
- `src/tests/action_context/__init__.py`
- `src/tests/action_context/test_*.py` — unit + differential + golden tests
- `src/tests/fixtures/action_context/<provider>/<match>[_p<period>]/*.parquet` — committed frame-slice fixtures

**Modified:**
- `src/ingestion/action_context.py` — becomes adapters + composition root: Spark source/sink adapters, thin UDF delegating to `analytics.action_context`, `main`/`main_preflight` wiring. Domain logic removed (now imported).

---

## PHASE A0 — Test-coverage inventory + pure unit nets FIRST (TDD; spec M1)

### Task A0.1: Inventory existing coverage of every function being relocated

**Files:**
- Create: `docs/superpowers/plans/notes/ac1-coverage-inventory.md` (working note, not committed to wheel)

- [ ] **Step 1: Find current tests touching the domain functions**

Run (Grep tool or):
```bash
grep -rln "_enrich_tracking_match\|_enrich_sb360_match\|_enrich_event_only_match\|_build_output\|_bronze_idsse_to_sportec_input\|_bronze_metrica_to_frames\|_bronze_skillcorner_to_frames\|_bronze_gradientsports_to_converter_input" src/tests/
```
Expected: list of test files. Record which functions have a DIRECT pure-pandas test vs only Spark-integration coverage.

- [ ] **Step 2: Write the inventory note**

For each of the 9 functions (3 enrich tiers, 4 converters, `_build_output`, schema builders) record: `direct-unit | spark-only | none`. Any `spark-only`/`none` gets a pure unit test in A0.2 BEFORE relocation.

- [ ] **Step 3: Commit** (PAUSE for approval)
```bash
git add docs/superpowers/plans/notes/ac1-coverage-inventory.md
git commit -m "docs(ac-1): test-coverage inventory for hexagon extraction"
```

### Task A0.2: Add pure unit tests for any un-covered moved function (against CURRENT code)

These run against the functions in their CURRENT location (`ingestion.action_context` / `ingestion.tracking_context`) so the net is green BEFORE the move. Use the smallest synthetic frames/actions DataFrames that exercise each tier.

**Files:**
- Create: `src/tests/action_context/__init__.py` (empty)
- Create: `src/tests/action_context/test_enrich_event_only_current.py`

- [ ] **Step 1: Write a failing test for the event-only tier (smallest tier first)**

```python
# src/tests/action_context/test_enrich_event_only_current.py
import pandas as pd
from ingestion.action_context import _enrich_event_only_match


def _minimal_actions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "game_id": [1, 1],
            "action_id": [0, 1],
            "period_id": [1, 1],
            "time_seconds": [1.0, 2.0],
            "team_id": ["A", "A"],
            "player_id": ["p1", "p2"],
            "type_id": [0, 21],          # pass, shot (silly_kicks ids)
            "result_id": [1, 1],
            "bodypart_id": [0, 0],
            "start_x": [50.0, 80.0],
            "start_y": [34.0, 34.0],
            "end_x": [80.0, 105.0],
            "end_y": [34.0, 34.0],
            "team_id_native": ["A", "A"],
            "player_id_native": ["p1", "p2"],
        }
    )


def test_event_only_adds_game_state_and_gk_context():
    out = _enrich_event_only_match(_minimal_actions())
    assert "game_state" in out.columns
    assert len(out) == 2
```

- [ ] **Step 2: Run to verify it passes against current code** (this is a characterization test, not red-first; the function already exists)

Run: `uv run pytest src/tests/action_context/test_enrich_event_only_current.py -v`
Expected: PASS. If it FAILS, the synthetic input is wrong — fix the fixture until it characterizes real behavior.

- [ ] **Step 3: Repeat for any tier/converter marked `spark-only`/`none` in A0.1**

For each, write a minimal pure test asserting output columns + row count + one known value. (Tracking tiers need a small synthetic frames DataFrame matching the converter's expected input columns; copy the column set from `_IDSSE_TRACKING_SELECT_COLS` etc.) Keep each test < 40 lines.

- [ ] **Step 4: Capture the pre-refactor behavior BASELINE (M10) — before any relocation**

Run the CURRENT `ingestion.action_context._enrich_event_only_match` and (with a small
synthetic frames set) `_enrich_tracking_match` on the synthetic inputs and commit their
**full output** as `src/tests/fixtures/action_context/_baseline/{event_only,tracking}_baseline.parquet`.
This is the only point the original code exists to snapshot against; A.7 Step 3 compares the
post-refactor path to these files. Example:
```python
# scripts/_capture_ac1_baseline.py  (throwaway; run once, then delete)
import pandas as pd
from pathlib import Path
from ingestion.action_context import _enrich_event_only_match
from src.tests.action_context.test_enrich_event_only_current import _minimal_actions
out = _enrich_event_only_match(_minimal_actions())
d = Path("src/tests/fixtures/action_context/_baseline"); d.mkdir(parents=True, exist_ok=True)
out.to_parquet(d / "event_only_baseline.parquet", index=False)
```
(Do the same for the tracking tier with a synthetic 2-batch / 500-frame frames set so the
baseline also exercises the batch boundary — see H3 below.) **Note:** Phase A preservation is
synthetic-input only; real-data preservation first appears in Phase C.

- [ ] **Step 5: Run the whole new net + commit** (PAUSE for approval)
```bash
uv run pytest src/tests/action_context/ -v   # all PASS (characterizes current behavior)
git add src/tests/action_context/ src/tests/fixtures/action_context/_baseline/
git commit -m "test(ac-1): pure unit net + committed pre-refactor behavior baseline (M10)"
```

---

## PHASE A — Hexagon extraction (behavior-preserving)

### Task A.1: Create domain package + WorkUnit/FrameBundle/MatchMeta

**Files:**
- Create: `src/analytics/action_context/__init__.py`
- Create: `src/analytics/action_context/work_unit.py`
- Test: `src/tests/action_context/test_work_unit.py`

- [ ] **Step 1: Write failing test**
```python
# src/tests/action_context/test_work_unit.py
from analytics.action_context.work_unit import WorkUnit, provider_tier


def test_provider_tiering():
    assert provider_tier(WorkUnit("idsse", "M", period=1)) == "tracking"
    assert provider_tier(WorkUnit("wyscout", "M")) == "event_only"
    # statsbomb tier depends on 360 availability, decided by the FrameSource,
    # so provider_tier returns "statsbomb" (resolved later by the bundle).
    assert provider_tier(WorkUnit("statsbomb", "M")) == "statsbomb"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/action_context/test_work_unit.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**
```python
# src/analytics/action_context/work_unit.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

_TRACKING_PROVIDERS = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
_EVENT_ONLY_PROVIDERS = frozenset({"wyscout"})  # statsbomb resolved via 360 presence

Tier = Literal["tracking", "sb360", "event_only", "statsbomb"]


@dataclass(frozen=True)
class WorkUnit:
    provider: str
    match_id: str
    period: int | None = None
    frame_range: tuple[int, int] | None = None


@dataclass(frozen=True)
class MatchMeta:
    home_team_id: str
    home_start_left: bool
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None


@dataclass(frozen=True)
class FrameBundle:
    tier: Tier
    frames: pd.DataFrame                 # tracking or synthetic freeze-frames; empty for event_only
    extra: dict[str, Any] = field(default_factory=dict)


def provider_tier(wu: WorkUnit) -> str:
    if wu.provider in _TRACKING_PROVIDERS:
        return "tracking"
    if wu.provider in _EVENT_ONLY_PROVIDERS:
        return "event_only"
    return "statsbomb"  # tier (sb360 vs event_only) resolved by FrameSource at runtime
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest src/tests/action_context/test_work_unit.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/__init__.py src/analytics/action_context/work_unit.py src/tests/action_context/test_work_unit.py
git commit -m "feat(ac-1): WorkUnit/FrameBundle/MatchMeta domain types"
```

### Task A.2: Define ports (Protocols)

**Files:**
- Create: `src/analytics/action_context/ports.py`
- Test: `src/tests/action_context/test_ports.py`

- [ ] **Step 1: Write failing test** (a fake implementing each Protocol type-checks + is usable)
```python
# src/tests/action_context/test_ports.py
import pandas as pd
from analytics.action_context.ports import (
    ActionsSource, FrameSource, MatchMetadataSource, ResultSink, XtSource,
)
from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


class _FakeFrames(FrameSource):
    def frames(self, wu: WorkUnit) -> FrameBundle:
        return FrameBundle(tier="event_only", frames=pd.DataFrame())


def test_fake_framesource_satisfies_protocol():
    fs: FrameSource = _FakeFrames()
    assert fs.frames(WorkUnit("wyscout", "M")).tier == "event_only"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/action_context/test_ports.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement**
```python
# src/analytics/action_context/ports.py
from __future__ import annotations

from typing import Protocol

import pandas as pd

from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


class FrameSource(Protocol):
    def frames(self, wu: WorkUnit) -> FrameBundle: ...


class ActionsSource(Protocol):
    def actions(self, wu: WorkUnit) -> pd.DataFrame: ...


class XtSource(Protocol):
    def grid(self) -> tuple[list[list[float]], int, int]: ...


class MatchMetadataSource(Protocol):
    def metadata(self, wu: WorkUnit) -> MatchMeta: ...


class ResultSink(Protocol):
    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int: ...
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest src/tests/action_context/test_ports.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/ports.py src/tests/action_context/test_ports.py
git commit -m "feat(ac-1): domain ports (FrameSource/ActionsSource/XtSource/MatchMetadataSource/ResultSink)"
```

### Task A.3: Move schema (RESULT_COLUMNS, DDL, build_output) into domain

**Files:**
- Create: `src/analytics/action_context/schema.py`
- Modify: `src/ingestion/action_context.py` (re-export from domain to avoid breaking imports)
- Test: `src/tests/action_context/test_schema.py`

- [ ] **Step 1: Write failing test** (DDL column list == RESULT_COLUMNS minus audit; build_output fills missing cols)
```python
# src/tests/action_context/test_schema.py
import pandas as pd
from analytics.action_context.schema import RESULT_COLUMNS, ACTION_CONTEXT_DDL, build_output


def test_ddl_matches_result_columns():
    ddl_cols = [tok.strip().split()[0] for tok in ACTION_CONTEXT_DDL.split(",")]
    assert ddl_cols == RESULT_COLUMNS  # DDL includes _ingested_at, same order


def test_build_output_fills_missing_and_selects():
    raw = pd.DataFrame({"action_id": [1], "start_x": [50.0]})
    out = build_output(raw, match_id_native="M", data_source="wyscout")
    expected = [c for c in RESULT_COLUMNS if c != "_ingested_at"]
    assert list(out.columns) == expected
    assert out["match_id"].iloc[0] == "M"
    assert out["data_source"].iloc[0] == "wyscout"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/action_context/test_schema.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement** — move `_RESULT_COLUMNS` → `RESULT_COLUMNS`, `_ACTION_CONTEXT_DDL` → `ACTION_CONTEXT_DDL`, `_build_output` → `build_output` verbatim from `ingestion/action_context.py` into `schema.py` (drop leading underscores; keep the exact list/DDL/body). Then in `ingestion/action_context.py` replace the definitions with:
```python
from analytics.action_context.schema import (  # noqa: F401
    ACTION_CONTEXT_DDL as _ACTION_CONTEXT_DDL,
    RESULT_COLUMNS as _RESULT_COLUMNS,
    build_output as _build_output,
)
```

- [ ] **Step 4: Run schema test + the A0 characterization net**

Run: `uv run pytest src/tests/action_context/ -v`
Expected: all PASS (build_output behavior unchanged).

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/schema.py src/ingestion/action_context.py src/tests/action_context/test_schema.py
git commit -m "refactor(ac-1): move result schema + build_output to analytics domain"
```

### Task A.4: COPY converters into domain (M4 — legacy untouched) + drift guard (L4)

**Files:**
- Create: `src/analytics/action_context/convert.py`
- Test: `src/tests/action_context/test_convert_drift.py`

- [ ] **Step 1: Implement convert.py — COPY (do NOT move) the pure converters**

Copy verbatim into `convert.py` (rename without leading underscore where public):
`_bronze_idsse_to_sportec_input`, `_bronze_metrica_to_frames`, `_bronze_skillcorner_to_frames`
from `ingestion/tracking_context.py`, and `_bronze_gradientsports_to_converter_input`
from `ingestion/action_context.py`. **Leave the originals in `tracking_context.py`
untouched** (it remains the differential oracle). Keep module-level constants they need
(`_GS_FRAME_RATE`, `_JERSEY_RE`).

- [ ] **Step 2: Write the drift-guard test (L4)** — both copies identical on synthetic input
```python
# src/tests/action_context/test_convert_drift.py
import pandas as pd
from analytics.action_context import convert as new
from ingestion import tracking_context as legacy


def _idsse_bronze_sample() -> pd.DataFrame:
    # minimal columns _bronze_idsse_to_sportec_input reads; copy from _IDSSE_TRACKING_SELECT_COLS
    return pd.DataFrame({
        "match_id": ["M"], "period": [1], "frame": [0], "game_clock": [0.0],
        "team_id": ["CLU1"], "player_id": ["OBJ1"], "x": [0.0], "y": [0.0],
        "is_ball": [False],
    })  # extend to the exact column set the converter requires


def test_idsse_converter_copies_agree():
    sample = _idsse_bronze_sample()
    pd.testing.assert_frame_equal(
        new.bronze_idsse_to_sportec_input(sample.copy()),
        legacy._bronze_idsse_to_sportec_input(sample.copy()),
    )
```

- [ ] **Step 3: Run to verify it passes**

Run: `uv run pytest src/tests/action_context/test_convert_drift.py -v`
Expected: PASS (identical copies). If the synthetic sample is insufficient for the converter, extend it to the converter's required columns until both run and agree.

- [ ] **Step 4: Add drift-guard tests for metrica/skillcorner/gradientsports converters** (same pattern, one per converter).

Run: `uv run pytest src/tests/action_context/test_convert_drift.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/convert.py src/tests/action_context/test_convert_drift.py
git commit -m "feat(ac-1): copy bronze->frames converters into domain + drift guard (M4/L4)"
```

### Task A.5: Move enrich tiers into domain

**Files:**
- Create: `src/analytics/action_context/enrich.py`
- Modify: `src/ingestion/action_context.py` (re-export)
- Test: `src/tests/action_context/test_enrich_event_only_current.py` (retarget import)

- [ ] **Step 1: Implement enrich.py** — move `_enrich_tracking_match`, `_enrich_sb360_match`, `_enrich_event_only_match` verbatim from `ingestion/action_context.py` into `enrich.py` as `enrich_tracking`, `enrich_sb360`, `enrich_event_only`. They already import silly_kicks at function scope (pure). Keep signatures identical.

- [ ] **Step 2: Re-export in ingestion** to avoid breaking the UDF/tests during transition:
```python
from analytics.action_context.enrich import (  # noqa: F401
    enrich_event_only as _enrich_event_only_match,
    enrich_sb360 as _enrich_sb360_match,
    enrich_tracking as _enrich_tracking_match,
)
```

- [ ] **Step 3: Retarget the A0 characterization test import** from `ingestion.action_context` to `analytics.action_context.enrich` and run the full net.

Run: `uv run pytest src/tests/action_context/ -v`
Expected: all PASS (same code, new home).

- [ ] **Step 4: import-linter green** (domain must not import pyspark/ingestion)

Run: `uv run lint-imports`
Expected: Contracts kept. If `enrich.py`/`convert.py`/`schema.py` accidentally import from `ingestion`, fix by moving the needed pure helper into the domain.

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/enrich.py src/ingestion/action_context.py src/tests/action_context/
git commit -m "refactor(ac-1): move enrich tiers to analytics domain; import-linter green"
```

### Task A.6: pipeline — `enrich_batch` contract + `run_work_unit` batch loop (M6 + H3)

**Files:**
- Create: `src/analytics/action_context/pipeline.py`
- Test: `src/tests/action_context/test_pipeline_dispatch.py`, `src/tests/action_context/test_batch_equivalence.py`

**H3 — the load-bearing contract:** production enriches **per 250-frame batch** (Spark
`groupBy(match_id,period,frame_batch_id)`, ~283/IDSSE half), and the differential oracle
`fct_tracking_context` was built the same way. Window-dependent features (`add_elastic_sync`,
OBSO peak/optimal, sync_score) differ between a 250-frame batch and a whole slice. So the
shared unit MUST be `enrich_batch` over ONE batch, called identically by prod (per Spark
group) and local (in a loop). Enriching the whole work unit at once is FORBIDDEN — it would
invalidate the differential (C) and profiler (D).

- [ ] **Step 1: Write failing test** — dispatch routes by FrameBundle.tier, calls correct enrich, writes via sink
```python
# src/tests/action_context/test_pipeline_dispatch.py
import pandas as pd
from analytics.action_context.pipeline import run_work_unit
from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


class _Frames:
    def frames(self, wu): return FrameBundle(tier="event_only", frames=pd.DataFrame())
class _Actions:
    def actions(self, wu):
        return pd.DataFrame({"action_id":[0],"period_id":[1],"time_seconds":[1.0],
                             "team_id":["A"],"player_id":["p"],"type_id":[0],"result_id":[1],
                             "bodypart_id":[0],"start_x":[50.0],"start_y":[34.0],
                             "end_x":[60.0],"end_y":[34.0],"game_id":[1],
                             "team_id_native":["A"],"player_id_native":["p"]})
class _Xt:
    def grid(self): return ([[0.0]], 1, 1)
class _Meta:
    def metadata(self, wu): return MatchMeta(home_team_id="A", home_start_left=True)
class _Sink:
    def __init__(self): self.rows = None
    def write(self, wu, df): self.rows = len(df); return len(df)


def test_event_only_dispatch_writes_rows():
    sink = _Sink()
    n = run_work_unit(WorkUnit("wyscout","M"), frames=_Frames(), actions=_Actions(),
                      xt=_Xt(), meta=_Meta(), sink=sink)
    assert n == 1 and sink.rows == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/action_context/test_pipeline_dispatch.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement `enrich_batch` (the shared contract) + `run_work_unit` (batch loop)**

`enrich_batch(provider, tier, batch_frames, actions_records, period, xt, meta, *, profile=None) -> pd.DataFrame`
does, for ONE 250-frame batch, EXACTLY what the current UDF body does per Spark group:
filter `actions_records` to this batch's time window (`[t_min - _ACTION_TIME_BUFFER_SECONDS,
t_max + _ACTION_TIME_BUFFER_SECONDS]`), provider-convert `batch_frames`→silly_kicks frames,
run the tier enrich, `build_output`. **M11: tier dispatch is by the `tier` arg, NOT
`provider`** — `provider_tier("statsbomb")` returns `"statsbomb"` (sb360 vs event_only is
resolved by the `FrameBundle`, not derivable from provider), so `enrich_batch` must be told
the resolved `tier` (`tracking`→`enrich_tracking`, `sb360`→`enrich_sb360`,
`event_only`→`enrich_event_only`). `run_work_unit` already holds `bundle.tier` and passes it
in. `_ACTION_TIME_BUFFER_SECONDS` + `_FRAME_BATCH_SIZE=250` move into the domain as constants.

`run_work_unit(wu, *, frames, actions, xt, meta, sink, profile=False)`:
- gets `FrameBundle` + tier; gets actions/xt/meta;
- **tracking tier:** compute `frame_batch_id = floor(<frame_col>/250)` on the bundle frames and
  **loop** `for bid in sorted(batches): enrich_batch(tier="tracking", ...batch_frames...)`;
  `pd.concat` results. (This replicates production's `groupBy(frame_batch_id).applyInPandas`
  exactly — H3.) **L10: per-provider column choices must match production** — GradientSports
  keys on `frame_num` (not `frame`) and renames `period_elapsed_time`→`timestamp`
  (action_context.py:1449,1456-1457); the `FrameSource`/`FrameBundle` must normalize the
  frame-key + `timestamp` columns so the batch loop and the ±0.5 s window key on the right
  columns for every provider.
- **sb360 / event_only tiers:** single `enrich_batch` call (no frame batching — they have no
  large tracking frame set).
- `sink.write(wu, result)`.

- [ ] **Step 4: Add the H3 invariant test — batched MUST DIFFER from whole-slice (M12)**

Do NOT test `run_work_unit == concat([enrich_batch...])` — that's tautological (run_work_unit
IS that concat), so it can't fail when the bug is reintroduced. Prod==local is true **by
construction** (both call the identical `enrich_batch`); the real cross-path check is C.2's
differential. Instead, lock the invariant by proving batching **changes** a window-dependent
result, so a future "simplify to one whole-slice enrich" regresses RED:

```python
# src/tests/action_context/test_batch_invariant.py
import pandas as pd
from analytics.action_context.pipeline import enrich_batch, run_work_unit
# Synthetic tracking fixture: 500 frames (2 batches) + an action whose value on a
# window-dependent feature (e.g. elastic_frame_id or an OBSO peak/sync_score column)
# depends on which frames are in scope.
def test_batching_changes_window_dependent_feature(idsse_2batch_fixture):
    batched = run_work_unit(idsse_2batch_fixture.wu, **idsse_2batch_fixture.deps)
    whole = enrich_batch(  # deliberately enrich ALL 500 frames as one "batch"
        provider="idsse", tier="tracking",
        **idsse_2batch_fixture.whole_slice_args(),
    )
    col = "elastic_frame_id"  # window-dependent; pick one the fixture exercises
    # The per-250-batch result must NOT equal the whole-slice result for this column,
    # which is the entire reason run_work_unit batches (H3).
    assert not batched[col].reset_index(drop=True).equals(
        whole[col].reset_index(drop=True)
    )
```
If this ever passes-as-equal, batching has stopped mattering (or been removed) — which would
silently invalidate the differential + profiler. This test is the guard.

- [ ] **Step 5: Run dispatch + batch-invariant**

Run: `uv run pytest src/tests/action_context/test_pipeline_dispatch.py src/tests/action_context/test_batch_invariant.py -v`
Expected: PASS (dispatch routes by tier; batched differs from whole-slice on the window-dependent column).

- [ ] **Step 6: Commit** (PAUSE for approval)
```bash
git add src/analytics/action_context/pipeline.py src/tests/action_context/test_pipeline_dispatch.py src/tests/action_context/test_batch_invariant.py
git commit -m "feat(ac-1): enrich_batch(tier) contract + run_work_unit 250-frame batch loop (M6/H3/M11/M12)"
```

### Task A.7: Make the Spark UDF + composition root delegate to the domain

**Files:**
- Modify: `src/ingestion/action_context.py` (`_make_action_context_udf`, `_process_*`, `main`, `main_preflight`)

- [ ] **Step 1: Rewrite the UDF body to call `enrich_batch` (the SAME loop body run_work_unit uses)** — Spark still does `groupBy("match_id","period","frame_batch_id").applyInPandas`, so each UDF invocation receives exactly ONE 250-frame batch. The UDF body becomes a thin shell that calls `analytics.action_context.pipeline.enrich_batch(provider, batch_frames=pdf, actions_records=..., period=..., xt=..., meta=...)` and returns its result — i.e. one iteration of `run_work_unit`'s loop. Remove the duplicated inline enrichment. The driver-side `_process_*` become the Spark `FrameSource`/`ActionsSource`/`MatchMetadataSource`/`ResultSink` adapters. Keep `groupBy(...frame_batch_id).applyInPandas` and the now-domain `_FRAME_BATCH_SIZE=250` UNCHANGED. This is what makes prod and local **provably identical** (Task A.6 Step 4 test).

- [ ] **Step 2: Run the existing ingestion unit tests** (whatever exercises action_context import/guard/schema)

Run: `uv run pytest src/tests/ -k action_context -v`
Expected: PASS.

- [ ] **Step 3: Behavior-preservation check (H2/M10) — compare to the committed A0 baseline**

Create `src/tests/action_context/test_behavior_preserved.py`: run the post-refactor path
(`run_work_unit` for event_only on the synthetic actions; and the tracking tier on the
synthetic 2-batch/500-frame set) and assert it equals the **committed baselines from A0 Step 4**
(`src/tests/fixtures/action_context/_baseline/{event_only,tracking}_baseline.parquet`) via
`pd.testing.assert_frame_equal` (exact — verbatim move + identical batch loop). This is why A0
captured the baseline BEFORE relocation (M10): the original code no longer exists to snapshot
now. Do NOT reference `fct_action_context` (never built; bronze 0 rows). Phase A proves
behavior-preservation on synthetic input only; real-data correctness is Phase C.

Run: `uv run pytest src/tests/action_context/test_behavior_preserved.py -v`
Expected: PASS.

- [ ] **Step 4: Full gate**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run lint-imports && uv run pytest src/tests/action_context/ -v`
Expected: all green.

- [ ] **Step 5: Commit** (PAUSE for approval)
```bash
git add src/ingestion/action_context.py src/tests/action_context/test_behavior_preserved.py
git commit -m "refactor(ac-1): UDF + composition root delegate to analytics domain (behavior-preserving)"
```

---

## PHASE B — Extract tool + committed fixtures

### Task B.1: Local Parquet adapters

**Files:**
- Create: `src/analytics/action_context/local/__init__.py`, `src/analytics/action_context/local/parquet_sources.py`
- Test: `src/tests/action_context/test_parquet_sources.py`

- [ ] **Step 1: Write failing test** — round-trip a tiny fixture dir through the Parquet adapters and assert the bundle/actions/xt/meta load.
```python
# src/tests/action_context/test_parquet_sources.py
import pandas as pd
from pathlib import Path
from analytics.action_context.local.parquet_sources import (
    ParquetFrameSource, ParquetActionsSource, ParquetXtSource, ParquetMatchMetadataSource,
)
from analytics.action_context.work_unit import WorkUnit


def test_roundtrip(tmp_path: Path):
    d = tmp_path / "wyscout" / "M"; d.mkdir(parents=True)
    pd.DataFrame({"action_id":[0]}).to_parquet(d / "actions.parquet")
    pd.DataFrame({"zone_x":[0],"zone_y":[0],"xt_value":[0.1]}).to_parquet(d / "xt_grid.parquet")
    pd.DataFrame({"home_team_id":["A"],"home_start_left":[True]}).to_parquet(d / "meta.parquet")
    wu = WorkUnit("wyscout","M")
    assert ParquetActionsSource(tmp_path).actions(wu).shape[0] == 1
    g, l, w = ParquetXtSource(tmp_path).grid(); assert (l, w) == (1, 1)
    assert ParquetMatchMetadataSource(tmp_path).metadata(wu).home_team_id == "A"
```

- [ ] **Step 2-4:** Run (FAIL → implement adapters reading `<root>/<provider>/<match>[_p<period>]/{frames,actions,xt_grid,meta}.parquet`; `ParquetFrameSource` returns `FrameBundle` with tier from `provider_tier` + empty frames for event-only → PASS).

- [ ] **Step 5: Commit** (PAUSE for approval) — `feat(ac-1): local Parquet source/sink adapters`

### Task B.2: Extract tool (read-only Databricks SQL → fixture + legacy oracles)

**Files:**
- Create: `scripts/extract_action_context_fixture.py`

**Provider strategy (per d32 oracle-sufficiency review — coverage is provider-dependent):**
- **IDSSE J03WMX is the ANCHOR differential fixture** — the ONLY provider where the 5 heavy
  operators (OBSO/PAUSA/elastic) have legacy oracles (`fct_pausa_values` + `elastic_sync_results`
  are IDSSE-only). It is the sole full first-time correctness check; it must exist.
- SkillCorner / Metrica fixtures: tracking-only differential (`fct_tracking_context`), the rest
  invariant-only.
- **GradientSports: NO differential** (`fct_tracking_context` has 0 GS rows). A GS fixture is for
  **behavior-preservation + profiling only** — do NOT wire it into the differential. **L9: a GS
  golden is REGRESSION-ONLY, not a correctness signal** — with zero oracle, every GS feature
  column is frozen at first-capture with no independent validation; a green GS golden proves only
  "unchanged since capture," never "correct." This is the L5 risk at its maximum (all columns,
  not just 8). State this wherever the GS golden is produced so no reader mistakes green for correct.

- [ ] **Step 1: Implement** the CLI `--provider --match-id [--period] [--frame-start --frame-end]`. Pull (read-only, via `WorkspaceClient.statement_execution`): bronze tracking (projected cols per provider), `bronze.spadl_actions` for the match, xT grid (`bronze.expected_threat_grids` global), provider metadata; **and the legacy oracle slices per the §9.1 matrix, each with its OWN match-key convention (M8 — verified):**
  - `dev_gold.fct_tracking_context`: has **no native match_id, only surrogate `match_key`** → first resolve `J03WMX → match_key` via `dev_gold.dim_matches` with **`WHERE provider='idsse' AND native_match_id='J03WMX'`** (L8 — `native_match_id` is unique only *with* provider; `match_key` = deterministic hash of (provider, native_match_id), so provider is load-bearing), then pull `WHERE match_key = <resolved>`.
  - `dev_gold.fct_pausa_values`: uses **prefixed** native `idsse_J03WMX` → pull `WHERE match_id = 'idsse_' || '<match>'`.
  - `bronze.elastic_sync_results`: stores BOTH `J03WMX` and `idsse_J03WMX` with different counts → pull both, then **keep the authoritative set** = rows with max `_ingested_at` per `(match_id-normalized, event_id)`; record which prefix won.
  - `int_running_score`/`fct_action_values` for game_state.
  Write each oracle slice to `…/<match>[_p<period>]/oracle_<table>.parquet` with a normalized `native_match_id` + `action_id` column already joined where possible. Default to a `--frame-range` slice (L3). `(10,30)` timeouts, `verify=True`, HTTPS.

- [ ] **Step 2: Dry-run against a tiny event-only match first** (cheap): `uv run python scripts/extract_action_context_fixture.py --provider wyscout --match-id <id>`; confirm files written.

- [ ] **Step 3: Extract the IDSSE ANCHOR fixture** (frame-slice): `--provider idsse --match-id J03WMX --period 1 --frame-start 0 --frame-end 7500` (≈30 batches). Confirm tracking + actions + the 3 oracle slices (tracking_context via match_key, pausa via prefix, elastic deduped) land. **L11: `--frame-range` MUST be batch-aligned — `frame-start` a multiple of 250 and the span a multiple of 250** (0–7500 = 30 whole batches ✔). A non-aligned end leaves a PARTIAL boundary batch whose enrichment ≠ production's full 250-frame batch, producing spurious differential mismatches on the edge. The extract tool should reject (or round) a non-multiple-of-250 range and log the adjustment.

- [ ] **Step 4: Confirm compressed fixture size** (`ls -la` the dir); if a full half is ever needed, use git-LFS (L3).

- [ ] **Step 5: Commit** (PAUSE for approval) — `feat(ac-1): fixture extract tool (per-oracle match-key resolution) + IDSSE anchor fixture`

---

## PHASE C — Differential harness + golden capture

### Task C.1: Column-name map module (from verified §9.1 matrix)

**Files:**
- Create: `src/tests/action_context/oracle_map.py`

- [ ] **Step 1: Implement** the explicit AC-1-column → (oracle table, oracle column, **match-join strategy**, action-join, tolerance, providers-with-oracle) map using the VERIFIED names + per-provider coverage from §9.1. **Match-scope FIRST, then join on action_id** (action_id is per-match, ~6× collision across matches — M8). Three distinct match-join strategies:
  - **tracking (66 cols)** → `fct_tracking_context.<same name>`; match-join = resolve native→`match_key` via `dim_matches`; providers = idsse/skillcorner/metrica (NOT gradientsports — 0 rows).
  - **OBSO (3)** → `fct_pausa_values.actual_obso/peak_obso/optimal_obso`; match-join = `idsse_`-prefix normalize; action-join = pass_id→action; **providers = IDSSE only**.
  - **PAUSA (3)** → `fct_pausa_values.temporal_judgment/spatial_selection/pausa_score`; same as OBSO; **IDSSE only**.
  - **elastic (3)** → `elastic_sync_results.frame_id/alignment_confidence/alignment_error_seconds`; match-join = deduped authoritative set (max `_ingested_at`); action-join = event_id→action; **IDSSE only**.
  - **game_state (1)** → `int_running_score`/`fct_action_values.game_state`.
  - **shape_graph_* (6) + space_created_* (2): INVARIANT_ONLY for ALL providers** (no oracle).
  - Encode a `providers_with_oracle` field per entry so C.2 can skip columns with no oracle for the fixture's provider instead of asserting against absent data.

- [ ] **Step 2: Commit** (PAUSE) — `test(ac-1): verified oracle map (3 match-join strategies + per-provider coverage)`

### Task C.2: Differential test (tolerance-based, determinism-pinned — M3)

**Files:**
- Create: `src/tests/action_context/test_differential.py`

- [ ] **Step 1: Boundary-action duplication assertion FIRST (M13) — the latent bug this harness exists to find**

Before any oracle join, assert the result has **unique `(match_id, action_id, period_id)`**:
```python
def test_no_boundary_action_duplication(idsse_anchor_fixture):
    result = run_work_unit(idsse_anchor_fixture.wu, **idsse_anchor_fixture.deps)
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert (dupes == 1).all(), f"duplicate action rows: {dupes[dupes > 1].to_dict()}"
```
**Why this matters:** `enrich_batch`'s action filter is an *overlapping* window
`[t_min-0.5, t_max+0.5]` (`_ACTION_TIME_BUFFER_SECONDS`). At 25 fps, batch boundaries are ~0.04 s
apart, so an action within 0.5 s of a boundary matches BOTH adjacent batches → enriched and
emitted **twice** → duplicate action rows. This is unverifiable in prod today (table is 0 rows)
but is exactly the latent production bug the local harness is built to expose. If this assertion
FAILS: it is a **real production bug** — fix in `run_work_unit`/`build_output` by assigning each
action to exactly ONE batch (the batch whose frame window contains the action's *linked* frame;
the ±0.5 s buffer stays only for frame *lookup*, not for output emission), and flag it in the PR.
Do NOT let the differential absorb the fan-out as "tolerance noise" — the join on action_id would
double-count.

- [ ] **Step 2: Implement the differential** — set `OMP_NUM_THREADS=MKL_NUM_THREADS=1` at module top; load the **IDSSE anchor** fixture; `run_work_unit` → result (now known dup-free from Step 1). For each column, look up `oracle_map`: if the fixture's provider is in that column's `providers_with_oracle`, match-scope the oracle (per its strategy) then join on action_id and assert within per-column `abs/rel` epsilon (bool/int exact); otherwise INVARIANT_ONLY → assert ranges only (shape_graph density ∈ [0,1]; space_created ≥ 0). Emit a divergence table (column, oracle|invariant, n_mismatch, max_delta). **Provider-parameterize** so IDSSE runs the full ~84-col differential, SkillCorner/Metrica tracking-only (~66), GS golden+invariant only.

- [ ] **Step 3: Run**

Run: `uv run pytest src/tests/action_context/test_differential.py -v`
Expected: PASS within tolerances (and dup-free). Investigate any column over-tolerance as a real finding (record max_delta) before loosening epsilon.

- [ ] **Step 4: Commit** (PAUSE) — `test(ac-1): boundary-dup assertion (M13) + tolerance differential vs legacy oracles`

### Task C.3: Freeze golden snapshot WHILE legacy still exists (M2)

**Files:**
- Create: `src/tests/action_context/test_golden.py`, `src/tests/fixtures/action_context/<...>/golden.parquet`

- [ ] **Step 1:** After C.2 passes, write the validated result to `golden.parquet` (committed). Implement `test_golden.py` asserting `run_work_unit` output equals the golden within the same tolerances.

- [ ] **Step 2: Run** — `uv run pytest src/tests/action_context/test_golden.py -v` → PASS.

- [ ] **Step 3: Commit** (PAUSE) — `test(ac-1): freeze validated golden snapshot (M2 — before legacy retires)`

---

## PHASE D — Profiling

### Task D.1: Per-step compute profiler (attributing actions-reconstruction — L1)

**Files:**
- Create: `src/analytics/action_context/profiling.py`
- Test: `src/tests/action_context/test_profiling.py`

- [ ] **Step 1: Write failing test** — `profile_steps` wraps a chain and returns `{step: seconds}` including an `actions_reconstruction` line.
- [ ] **Step 2-4:** FAIL → implement a monotonic-timer context manager + a `StepTimings` accumulator; wire `run_work_unit(..., profile=True)` to populate it; ensure the per-group `pd.DataFrame(actions_records)` rebuild is timed under its own key (L1) → PASS.
- [ ] **Step 5: Commit** (PAUSE) — `feat(ac-1): per-step profiler attributing actions reconstruction`

### Task D.2: Run both profiles + publish breakdown

**Files:**
- Create: `docs/superpowers/plans/notes/ac1-profile-results.md`

- [ ] **Step 1: Local per-step profile** — run `run_work_unit(profile=True)` on the full IDSSE half fixture (regenerate full half via extract tool if needed, local); record per-step seconds table.
- [ ] **Step 2: Parallelism/scheduling profile (M7)** — run the **`--frame-range` slice** as a single `compute_action_context` task on Databricks (one targeted task, NOT the fan-out), read the Spark UI executor timeline / event log for slot occupancy + per-group wall-time; extrapolate to 283 groups.
- [ ] **Step 3: Write the breakdown** attributing wall-time across the three §3 causes (per-group compute / executor starvation / per-group overhead). State which dominates.
- [ ] **Step 4: Decision gate (§8)** — only if (b) starvation and (c) overhead are ruled out, recommend the silly_kicks surface-sharing optimization (separate spec). Otherwise recommend the cheaper lever (cluster/concurrency/batch sizing).
- [ ] **Step 5: Commit** (PAUSE) — `docs(ac-1): profiling breakdown + optimization decision gate`

---

## Self-Review

**Spec coverage (L12 — refreshed):**
- A0 — test net (M1) + **pre-refactor behavior baseline captured before relocation (M10)** ✔
- A — hexagon: WorkUnit/ports, schema move, **converters COPIED + drift guard (M4/L4)**,
  enrich move, **`enrich_batch(tier)` shared contract + `run_work_unit` 250-frame batch loop
  (H3/M6/M11)**, **batch-invariant test proving batching changes results (M12)**,
  UDF→`enrich_batch` per Spark group, **behavior-preservation vs committed baseline (H2/M10)** ✔
- B — Parquet adapters; extract tool with **3 match-join conventions (M8), `provider=` filter
  (L8), batch-aligned `--frame-range` (L11)**; IDSSE anchor + GS-no-differential (L9); L3 slice ✔
- C — **boundary-action duplication assertion (M13)**; tolerance+determinism differential (M3);
  verified per-provider oracle matrix (M5); golden-before-retire (M2); invariant-only listed (L5) ✔
- D — per-step profiler attributing actions-reconstruction (L1) + parallelism profile via
  completing slice (M7) + decision gate (H1) ✔
- Going-forward ADR-028 (L2) — authored when Phase A merges.

**Placeholder scan:** converter drift-guard synthetic samples (A.4, B steps) say "extend to the converter's required columns" — concrete instruction (copy the documented `_*_TRACKING_SELECT_COLS`), not a TODO. No "TBD"/"implement later" remain.

**Type consistency:** `WorkUnit`, `FrameBundle(tier, frames, extra)`, `MatchMeta`, ports (`frames/actions/grid/metadata/write`), `enrich_batch(provider, tier, batch_frames, actions_records, period, xt, meta, *, profile)`, `run_work_unit(wu, *, frames, actions, xt, meta, sink, profile)`, `build_output(raw, match_id_native, data_source)`, `RESULT_COLUMNS`/`ACTION_CONTEXT_DDL` — consistent across A.1–D.2. (`enrich_batch` takes both `provider` AND `tier` — M11.)

**Added task (L2):** ADR-028 (hexagon = recommended for new/touched pipelines, NOT a retrofit mandate) authored when Phase A lands, bundled in that PR.

**Bundling:** the spec + this plan commit bundled with the implementation PR (project rule: specs/plans are not standalone).

## Pre-Commit E2E Gate (local, no Databricks compute) — MANDATORY before the single bundled commit

User directive (2026-05-28): **full e2e testing before we commit — test everything locally.**
Nothing is committed until ALL of the following are green on the local machine:
1. Full unit + drift + batch-invariant suite: `uv run pytest src/tests/action_context/ -v`.
2. Quality gate: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run lint-imports`.
3. **Real-game e2e through the actual hexagon:** the IDSSE anchor fixture (J03WMX, real
   data pulled read-only) run through `run_work_unit` → the real `enrich_batch` 250-frame
   loop, producing the full action-context output **locally** (no Spark, no Databricks job).
4. Correctness on that real run: **boundary-dup assertion (M13) passes**, the **differential
   vs legacy oracles passes within tolerance**, and the **golden snapshot is frozen**.
5. Behavior-preservation: post-refactor path == committed pre-refactor baseline (A0/M10).
Only after 1–5 are green do we present the bundled diff for approval + commit + PR.

## Post-execution deltas (2026-05-29 — silly-kicks 3.27.0 adoption)

After Phase A–D landed locally, the silly-kicks performance/correctness asks (handoff note
`silly-kicks-handoff-ac1-updates.md`) shipped and were adopted. Deltas vs the design above:

- **silly-kicks pinned `>=3.27.0`** (`pyproject.toml` [spadl]). 3.25.0 = ELASTIC frame-origin fix
  + native DAS/shape_graph linked-frame restriction + `PitchControlCache` (TF-7); 3.25.1 =
  cover_shadows leave-one-out vectorization; 3.26.0 = ghost_gk `link_frame_ids` (~100×);
  3.27.0 = GS player-id helper (additive; GS adoption is a separate gated follow-on).
- **`_restrict_to_linked_frames` workaround removed** — DAS/shape_graph now restrict natively.
- **`PitchControlCache` wired** through obso/cover_shadows/gk_influence/space_creation/
  pitch_control_at_action in `enrich.py`.
- **ghost_gk added** as Step 12b (`add_ghost_gk`, bundled "default" model — no-internet-UDF-safe);
  3 new columns `ghost_gk_x/y/spread` in `RESULT_COLUMNS` + DDL + a bronze ALTER migration (pending).
- **cover_shadows `detailed=True`** wired at all call sites (A/B: only
  `max_single_defender_blocking_score` differs; ~1.5× cover_shadows cost; off the critical path).
- **elastic → INVARIANT_ONLY (not oracle-validated).** Investigation found the legacy
  `elastic_sync_results` oracle is itself IDSSE-frame-origin-buggy (`frame≈25·ts`, intercept≈0 vs
  correct +10000), so it is not a valid target; AC-1 elastic (3.25.0-correct) is range-checked.
  Root cause: `memory/project_legacy_elastic_sync_frame_origin_bug.md`; to be captured in ADR-028.
- **Golden regenerated** on 3.27.0: rows=97, cols=103, 0 boundary dups; differential 2/2 +
  ruff/pyright green. ghost_gk_x/y/spread and elastic_* added to `oracle_map.INVARIANT_ONLY`.
