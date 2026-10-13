# Tracking-marts Stage-2 scoring window as a runtime job parameter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline, this session) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Implement INLINE — no dispatched implementation subagents (repo rule).

**Goal:** Make the tracking-marts Stage-2 scoring window (`_TM_SCORE_WINDOW_FRAMES`) tunable at runtime via a
new mega-job parameter, so the densest period-2 units can be fit without a wheel deploy per attempt.

**Architecture:** A new `tracking_marts_score_window` job parameter (default `""`) is plumbed to the
drain-worker task only, parsed (`"" → None`, else `int > 0`), threaded through `_run_worker` into
`TrackingMartsProcessor(score_window_frames=...)`, and resolved inside `process` (empty → the baked
`_TM_SCORE_WINDOW_FRAMES = 8000`) so BOTH `precompute_stage2a` and `build_stage2_mart_sdf` receive the same
value. The windowing algorithm is untouched; neutrality is already proven, so the window is a pure memory knob.

**Tech Stack:** Python 3.10, PySpark, Databricks mega-job, Terraform (databricks provider), pytest, Ruff,
pyright.

**Spec:** `docs/superpowers/specs/2026-10-13-tracking-marts-score-window-param-design.md`

## Global Constraints

- Python 3.10 (`>=3.10,<3.11`); line length 120.
- No commit / push / PR / merge / job-run without Karsten's explicit per-action approval. Stop at the commit
  gate.
- Wheel version bump via `pyproject.toml` edit → `uv run python scripts/bump_wheel.py` (NEVER hand-edit the
  ~30 consumers); `bump_wheel.py --check` is the CI gate. Current version `0.5.123` → bump to `0.5.124`.
- The default `_TM_SCORE_WINDOW_FRAMES = 8000` MUST NOT change (lowering the default is a separate approval).
- `precompute_stage2a` and `build_stage2_mart_sdf` MUST receive the SAME resolved window (cross-engine
  window-id agreement).
- Seven local checks + `validate_workflow_cards` (hidden eighth) before the commit gate. pyright over the CI
  target (`src/ hf_taipy_app/src/` + `sync_tf_env_pins`), NOT `pyright src/`. Full `uv run pytest`, not
  collect-only, via the CI extras (`--extra analytics --extra embeddings --extra mlflow --extra jax`).
- Implement INLINE. Read-only research subagents OK; no implementation subagents.

---

### Task 1: Parse helper `_parse_score_window`

**Files:**
- Modify: `src/ingestion/tracking_marts_drain.py` (add helper next to `_parse_budget`)
- Test: `src/tests/test_tracking_marts_drain_worker.py` (or the existing drain-worker test module — match where
  `_parse_budget` is tested)

**Interfaces:**
- Produces: `_parse_score_window(raw: object) -> int | None` — empty/whitespace/None → `None`; parseable
  `int > 0` → that int; non-int → `SystemExit`; `<= 0` → `SystemExit`.

- [ ] **Step 1: Write the failing tests**

```python
import pytest
from ingestion.tracking_marts_drain import _parse_score_window


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_parse_score_window_empty_is_none(raw):
    assert _parse_score_window(raw) is None


def test_parse_score_window_valid_int():
    assert _parse_score_window("4000") == 4000


@pytest.mark.parametrize("raw", ["0", "-5"])
def test_parse_score_window_non_positive_raises(raw):
    with pytest.raises(SystemExit):
        _parse_score_window(raw)


def test_parse_score_window_garbage_raises():
    with pytest.raises(SystemExit):
        _parse_score_window("abc")
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest src/tests/test_tracking_marts_drain_worker.py -k parse_score_window -q`
Expected: FAIL — `_parse_score_window` not defined.

- [ ] **Step 3: Implement the helper**

```python
def _parse_score_window(raw: object) -> int | None:
    """Resolve the Stage-2 scoring-window override (empty -> None = in-code _TM_SCORE_WINDOW_FRAMES).

    Mirrors ``_parse_budget``: empty/whitespace -> None (fall back to the baked default); a positive int
    overrides; anything else fails loud (a silently-ignored bad window would re-OOM with no signal).
    """
    val = str(raw or "").strip()
    if not val:
        return None
    try:
        frames = int(val)
    except ValueError as exc:
        raise SystemExit(f"--score-window-frames must be an integer, got {val!r}") from exc
    if frames <= 0:
        raise SystemExit(f"--score-window-frames must be > 0, got {frames}")
    return frames
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest src/tests/test_tracking_marts_drain_worker.py -k parse_score_window -q`
Expected: PASS.

---

### Task 2: `TrackingMartsProcessor` accepts and uses `score_window_frames`

**Files:**
- Modify: `src/ingestion/tracking_marts_processor.py` — `__init__` + `process`
- Test: `src/tests/test_tracking_marts_processor.py` (match the existing processor test module)

**Interfaces:**
- Consumes: `_TM_SCORE_WINDOW_FRAMES` (already lazy-imported in `process`).
- Produces: `TrackingMartsProcessor(spark, catalog, schema, *, gkdv_enabled=..., score_window_frames: int |
  None = None)`; `process` resolves `window = self._score_window_frames if not None else
  _TM_SCORE_WINDOW_FRAMES` and passes `window` to BOTH `precompute_stage2a(..., target_window_frames=window)`
  and `build_stage2_mart_sdf(..., target_window_frames=window)`.

- [ ] **Step 1: Write the failing wiring test**

Spy on the two dispatch calls (patch `ingestion.tracking_marts_dispatch.precompute_stage2a` and
`build_stage2_mart_sdf` at their import site in `process`) and assert the `target_window_frames` they receive.
Build the processor with the heavy constructor loads patched out (reuse whatever the existing processor tests
use to fake `ac_xt_grid` / `_build_comp_season_lookup` / the sk asserts / `_prepare_stage1`). The test must
prove:

```python
def test_process_passes_injected_score_window_to_both_dispatch_calls(...):
    # processor built with score_window_frames=4000, _prepare_stage1 faked to a non-None prep,
    # precompute_stage2a + build_stage2_mart_sdf + run_stage1_build_and_spill + cleanup_spill +
    # write_delta_table faked.
    proc = _make_processor(score_window_frames=4000)
    proc.process(unit)
    assert precompute_spy.call_args.kwargs["target_window_frames"] == 4000
    for call in build_stage2_spy.call_args_list:
        assert call.kwargs["target_window_frames"] == 4000


def test_process_defaults_to_baked_window_when_none(...):
    proc = _make_processor(score_window_frames=None)
    proc.process(unit)
    from ingestion.tracking_marts_dispatch import _TM_SCORE_WINDOW_FRAMES
    assert precompute_spy.call_args.kwargs["target_window_frames"] == _TM_SCORE_WINDOW_FRAMES
    for call in build_stage2_spy.call_args_list:
        assert call.kwargs["target_window_frames"] == _TM_SCORE_WINDOW_FRAMES
```

(If the existing processor tests already stub `process`'s collaborators, extend that fixture rather than
inventing a new one — match the module's established faking pattern.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest src/tests/test_tracking_marts_processor.py -k score_window -q`
Expected: FAIL — `__init__` rejects `score_window_frames` (unexpected kwarg) / dispatch receives the baked
value regardless of the injected one.

- [ ] **Step 3: Implement**

Add `score_window_frames` to the `__init__` signature (keyword-only, after `gkdv_enabled`):

```python
def __init__(
    self,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    gkdv_enabled: bool = GKDV_ENABLED,
    score_window_frames: int | None = None,
) -> None:
```

and store it:

```python
        # Stage-2 scoring-window override (ADR-090). None => resolve to the baked _TM_SCORE_WINDOW_FRAMES
        # inside process (where that constant is already lazy-imported). A runtime knob for the densest p2
        # units; the window is memory-only (neutrality proven), so any value is byte-identical output.
        self._score_window_frames = score_window_frames
```

In `process`, after the existing lazy import of `_TM_SCORE_WINDOW_FRAMES`, resolve once and use it in both
calls (replace the two `target_window_frames=_TM_SCORE_WINDOW_FRAMES` occurrences):

```python
        window = self._score_window_frames if self._score_window_frames is not None else _TM_SCORE_WINDOW_FRAMES
```

```python
            stage2a = precompute_stage2a(
                self._spark,
                sdir,
                resolved_actions,
                unit.provider,
                gkdv_enabled=self._gkdv_enabled,
                target_window_frames=window,
            )
```

```python
                    scored_sdf = build_stage2_mart_sdf(
                        self._spark,
                        unit,
                        sdir,
                        spec,
                        action_window=stage2a.action_window,
                        target_window_frames=window,
                    )
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest src/tests/test_tracking_marts_processor.py -k score_window -q`
Expected: PASS.

- [ ] **Step 5: Default-unchanged guard** (spec §5 test 4 — SWP-PLAN-01)

The Global Constraint "default `8000` MUST NOT change" needs a test, or a future accidental edit ships
unguarded. Add a pin in the processor test module (it already imports from `tracking_marts_dispatch`):

```python
def test_score_window_default_is_unchanged():
    # Lowering the DEFAULT is a separate approval (§4.6 fit-profile); this change only adds the override.
    # If this fails, someone moved the baked default — that needs its own sign-off, not this cycle.
    from ingestion.tracking_marts_dispatch import _TM_SCORE_WINDOW_FRAMES

    assert _TM_SCORE_WINDOW_FRAMES == 8000
```

Run: `uv run pytest src/tests/test_tracking_marts_processor.py -k score_window_default_is_unchanged -q`
Expected: PASS (the constant is unchanged by this cycle). This is a change-detector, not a behavioural test —
it fails loud only if the default moves.

---

### Task 3: Thread the override through the drain worker

**Files:**
- Modify: `src/ingestion/tracking_marts_drain.py` — `_run_worker` signature + processor construction;
  `main_tracking_marts_drain_worker` extra-arg + parse + pass-through
- Test: `src/tests/test_tracking_marts_drain_worker.py`

**Interfaces:**
- Consumes: `_parse_score_window` (Task 1), `TrackingMartsProcessor(score_window_frames=...)` (Task 2).
- Produces: `_run_worker(..., score_window_frames: int | None = None)` constructs
  `TrackingMartsProcessor(spark, catalog, schema, score_window_frames=score_window_frames)`;
  `main_tracking_marts_drain_worker` reads `--score-window-frames`, parses it, passes it to `_run_worker`.

- [ ] **Step 1: Write the failing test**

Assert `_run_worker` builds the processor with the given `score_window_frames` (patch
`ingestion.tracking_marts_processor.TrackingMartsProcessor` at its import site in `_run_worker`; stub the
queue/sink/`drain_worker`/`raise_on_failed_units` as the existing `_run_worker` tests do; give it one fake
unit so it does not early-return):

```python
def test_run_worker_passes_score_window_to_processor(...):
    _run_worker(spark, "cat", "bronze", 0, "run1", 60, logger, score_window_frames=4000)
    assert processor_ctor_spy.call_args.kwargs["score_window_frames"] == 4000
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_tracking_marts_drain_worker.py -k run_worker_passes_score_window -q`
Expected: FAIL — `_run_worker` has no such kwarg / processor built without it.

- [ ] **Step 3: Implement**

`_run_worker`: add `score_window_frames: int | None = None` to the keyword-only block, and build the
processor with it:

```python
    processor = TrackingMartsProcessor(spark, catalog, schema, score_window_frames=score_window_frames)
```

`main_tracking_marts_drain_worker`: add one entry to the `extra_args` list (next to `--watchdog-budget-s`):

```python
extra_args = [
    (
        "--score-window-frames",
        {
            "type": str,
            "default": None,
            "help": "Stage-2 scoring-window override in frames (ADR-090). Empty/absent -> in-code "
            "_TM_SCORE_WINDOW_FRAMES. Lower it for the densest p2 units (memory-only; output "
            "byte-identical at any window).",
        },
    ),
]
```

parse it before the `_run_worker` call:

```python
    score_window = _parse_score_window(getattr(args, "score_window_frames", None))
```

and pass it through:

```python
    _run_worker(
        spark,
        args.catalog,
        args.schema,
        worker_id,
        run_id,
        budget_s,
        task_logger,
        max_consecutive_failures=max_consec,
        systemic_failure_rate=sys_rate,
        systemic_min_sample=sys_min,
        score_window_frames=score_window,
    )
```

(Confirm the exact existing `_run_worker(...)` call tail — `sys_min` is the last positional/keyword today;
add `score_window_frames=score_window` after it.)

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest src/tests/test_tracking_marts_drain_worker.py -k "run_worker_passes_score_window or parse_score_window" -q`
Expected: PASS.

---

### Task 4: Terraform — the job parameter + the task argument

**Files:**
- Modify: `terraform/modules/workflows/main.tf`
- Test: terraform validate/plan + any workflow-param test the repo has (check
  `src/tests/test_*workflow*`/`test_*terraform*` for a golden over the drain-worker parameter list).

**Interfaces:**
- Produces: `job.parameters.tracking_marts_score_window` (default `""`); the `compute_tracking_marts_iteration`
  wheel task gains `"--score-window-frames", "{{job.parameters.tracking_marts_score_window}}"`.

- [ ] **Step 1: Add the job parameter** (after the `tracking_marts_providers` block, ~line 125):

```hcl
  # tracking_marts drain: Stage-2 scoring-window override in frames (ADR-090). Empty => the in-code
  # _TM_SCORE_WINDOW_FRAMES default. A runtime memory knob (output byte-identical at any window — neutrality)
  # for the densest period-2 units that OOM Stage-2 scoring at the default; lower it without a wheel deploy.
  parameter {
    name    = "tracking_marts_score_window"
    default = ""
  }
```

- [ ] **Step 2: Add the task argument** to `compute_tracking_marts_iteration`'s `parameters` list (after the
  `--watchdog-budget-s` line, ~811):

```hcl
            # Stage-2 scoring-window override (ADR-090); empty => in-code _TM_SCORE_WINDOW_FRAMES.
            "--score-window-frames", "{{job.parameters.tracking_marts_score_window}}",
```

- [ ] **Step 3: Format + validate**

Run: `terraform -chdir=terraform fmt` then the repo's terraform-plan equivalent (or
`terraform -chdir=terraform validate` if plan needs cloud creds). Expected: additive diff, no resource
replacement; fmt clean.

- [ ] **Step 4: Run any workflow-param golden test**

Run: `uv run pytest -k "workflow or terraform or cards" -q` (discover the actual module first).
Expected: PASS — if a golden pins the drain-worker arg list, update it to include `--score-window-frames` in
the SAME change (Hyrum's Law: the arg list is an observed contract).

---

### Task 5: ADR-090 amendment + wheel bump + TODO + full gate

**Files:**
- Modify: `docs/superpowers/adrs/ADR-090-tracking-marts-stage2-window-scoring.md` (amendment note)
- Modify: `pyproject.toml` (version) → run `scripts/bump_wheel.py`
- Modify: `TODO.md`

- [ ] **Step 1: ADR-090 amendment note** — add a dated amendment: the Stage-2 scoring window is now a runtime
  job-parameter `tracking_marts_score_window` plumbed to the drain worker; `_TM_SCORE_WINDOW_FRAMES` (8000)
  is the fallback default; the preflight canary does NOT validate the tuned window (Probe M is Stage-1-build,
  Probe C is fixture) — correctness is window-invariant (neutrality), memory-fit confirmed empirically by the
  re-run. Record the known limitation explicitly.

- [ ] **Step 2: Bump the wheel**

```bash
# edit pyproject.toml: version = "0.5.124"
uv run python scripts/bump_wheel.py
uv run python scripts/bump_wheel.py --check
```

Expected: `--check` clean; `git grep -nF "0.5.123"` returns nothing outside history/changelog.

- [ ] **Step 3: Update TODO.md** in this change (standing rule — every shipping cycle updates TODO.md in its
  commit). Reflect: score-window parameter shipped; the 4-unit re-run is the next operator step.

- [ ] **Step 4: Run all seven checks + the hidden eighth**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run lint-imports
uv run python scripts/bump_wheel.py --check
uv run python scripts/pip_audit_ignores.py --check   # confirm the exact invocation name
uv run pyright            # CI target, NOT pyright src/
uv run pytest -q          # full suite via the CI extras
uv run python scripts/validate_workflow_cards.py     # confirm the exact invocation name
```

Capture each exit code (never `| tail`). All must be green before the commit gate.

- [ ] **Step 5: STOP at the commit gate.** Present the diff / file list and wait for explicit approval. Do NOT
  commit.

---

## Self-review (run before handing to reviewers)

1. **Spec coverage:** every §4 file has a task; the §5 tests map to Tasks 1–3 (test 1 → Task 1; tests 2–3 →
   Task 2 Step 1 + Task 3; test 4 default-guard → Task 2 Step 5); §3.2 plumbing chain is
   realized end-to-end (param → task arg → parse → `_run_worker` → processor → both dispatch calls).
2. **Placeholder scan:** no TBD/TODO-in-code; every code step shows the actual edit.
3. **Type consistency:** `score_window_frames: int | None` is the single name across `_parse_score_window`
   return, `_run_worker`, and `TrackingMartsProcessor.__init__`; `target_window_frames` is the dispatch kwarg
   name (unchanged); default `_TM_SCORE_WINDOW_FRAMES` unchanged at 8000.
4. **Non-goal guard:** no change to the windowing algorithm, the Stage-1 window, the canary, or the baked
   default.
