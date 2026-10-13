# Tracking-marts Stage-2 scoring window as a runtime job parameter — Design

**Status:** Draft (awaiting two independent reviews)
**Date:** 2026-10-13
**Author:** Karsten (with Claude)
**Branch:** `feat/tracking-marts-score-window-param` (off `main` @ `210d354f`)
**Relates to:** ADR-088 (two-stage drain), ADR-089 (haloed frame-window build seam), ADR-090 (Stage-2 window-bounded scoring)

## 1. Context

The tracking-marts drain scores six per-mart surfaces per unit in two stages (ADR-088): Stage 1 builds the
unit's oriented frames on executors and spills them to a UC-Volume Parquet; Stage 2 reads the spill and runs
each mart's scorer. For the seam providers (skillcorner / idsse / gradientsports) Stage 2 is **windowed**
(ADR-090, `windowed_score_sdf`): the spilled frames are tagged into haloed scoring windows of
`_TM_SCORE_WINDOW_FRAMES` frames, each window is scored independently, and only the window's core outputs are
kept. The union is byte-identical to scoring the whole unit — the window is a **pure memory knob**, proven by
the shipped neutrality + halo-sufficiency tests (`src/tests/tracking_marts/test_stage2_window_scoring.py`, incl.
the per-mart windowed-union-equals-whole-unit and `precompute_stage2a` equals-whole-unit cases). Lowering the window only trades more compute / wall-time (more
windows, more halo recompute) for lower peak executor memory; it never changes the output.

`_TM_SCORE_WINDOW_FRAMES` is a **module-level constant** in `src/ingestion/tracking_marts_dispatch.py`,
currently `8000` (PROVISIONAL per its own doc comment, pending the §4.6 fit-profile lock). The drain
worker's `TrackingMartsProcessor.process` reads it in two places — `precompute_stage2a(...,
target_window_frames=_TM_SCORE_WINDOW_FRAMES)` and `build_stage2_mart_sdf(..., target_window_frames=
_TM_SCORE_WINDOW_FRAMES)` — and the SAME value feeds both so their window ids agree cross-engine.

### 1.1 The concrete problem

GATE-0's targeted 8-unit re-run (drain run `111048495154883`, wheel 0.5.123 live) left **4 of 8 succeeded
and 4 OOM'd** — all four were the densest period-2 halves: `1204769p2`, `1374493p2`, `1562415p2`,
`2015213p2`. Root cause is CONFIRMED (Stage-1 build fits — Probe-M passed and the four units' Stage-1 spill
completes; the failure is in the Stage-2 executor `mapInPandas` → write): **`_TM_SCORE_WINDOW_FRAMES=8000` is
too large for the densest p2 units' doubled-DAS scorer transients**, so the resident built slice + scorer
working set exceeds the 1 GB UDF group cap on those units.

Because 8000 is a **baked constant**, the only way to try a smaller window today is to edit the constant,
bump the wheel, run `bump_wheel.py`, re-deploy the wheel to the UC Volume, and re-run — a full wheel cycle
**per tuning attempt**. Finding the value that fits the densest unit may take several attempts. That is the
cost this change removes.

## 2. Goal

Make the Stage-2 scoring window **tunable at runtime** via a mega-job parameter, so the operator can lower it
on a `run_now` without a wheel deploy, converge on the value that fits the densest p2 units, and then
re-materialize the four OOM'd units — all against the already-deployed wheel.

### 2.1 Non-goals (explicit — no session decides these)

- **NOT lowering the baked default.** `_TM_SCORE_WINDOW_FRAMES` stays `8000`; it remains the fallback when the
  job parameter is empty. Locking a lower *default* is the separate §4.6 fit-profile item (plan Task D.1 of
  the ADR-090 plan) and needs its own approval. This change only adds the *override*.
- **NOT changing the windowing algorithm.** `windowed_score_sdf`, `assign_score_windows`, the per-mart halos,
  and the Stage-2a invariant injection are untouched. Neutrality is already proven; we rely on it, not re-open
  it.
- **NOT touching the Stage-1 build window** (`_TM_WINDOW_FRAMES`) or the Probe-M build-fit canary. Stage-1
  fits; only Stage-2 OOMs.
- **NOT auto-tuning / adaptive sizing.** The operator sets the value explicitly. An adaptive per-unit sizer is
  out of scope and not requested.
- **NOT re-running the 4 units in this change.** The re-run is a separate, operator-fired step after the wheel
  ships and the fitting value is found (and is itself a separate approval gate, per §7).

## 3. Design

### 3.1 The parameter

A new mega-job parameter `tracking_marts_score_window` (default `""`), alongside the existing
`tracking_marts_full` / `tracking_marts_providers`. Empty = use the in-code default
(`_TM_SCORE_WINDOW_FRAMES`); a positive integer = override the Stage-2 scoring window for that run.

It is plumbed to the **drain-worker** task only (`compute_tracking_marts_iteration`), because the drain worker
is the ONLY consumer of the scoring window:

- The **preflight** canary does NOT use it. Probe M (`build_fit_probe`) exercises the Stage-1 build window
  (`_TM_WINDOW_FRAMES`); Probe C (`run_fixture_probe_c`) scores a bundled fixture through the pure scorer
  cores with no windowing. Neither reads `_TM_SCORE_WINDOW_FRAMES`. (See §3.4 for the canary-coverage
  limitation this implies.)

### 3.2 Plumbing chain

```
job.parameters.tracking_marts_score_window               (terraform, default "")
  └─ compute_tracking_marts_iteration task
       parameters += ["--score-window-frames", "{{job.parameters.tracking_marts_score_window}}"]
          └─ main_tracking_marts_drain_worker()           (parse "" -> None, else int > 0)
               └─ _run_worker(..., score_window_frames=<None|int>)
                    └─ TrackingMartsProcessor(..., score_window_frames=<None|int>)
                         └─ process():
                              window = self._score_window_frames
                                       if self._score_window_frames is not None
                                       else _TM_SCORE_WINDOW_FRAMES
                              precompute_stage2a(...,  target_window_frames=window)
                              build_stage2_mart_sdf(..., target_window_frames=window)
```

`None` (empty parameter) flows end-to-end and the default is resolved **inside `process`**, where
`_TM_SCORE_WINDOW_FRAMES` is already imported lazily (avoiding the module's deliberate
tracking_marts_dispatch import-at-call-time pattern). Both `precompute_stage2a` and `build_stage2_mart_sdf`
receive the SAME resolved `window`, preserving the cross-engine window-id agreement the current code relies
on.

### 3.3 Parse contract

`--score-window-frames`:
- empty / whitespace / absent → `None` (processor falls back to `_TM_SCORE_WINDOW_FRAMES`).
- a parseable integer `> 0` → that int.
- non-integer → `SystemExit("--score-window-frames must be an integer, got ...")`.
- `<= 0` → `SystemExit("--score-window-frames must be > 0, got ...")`.

This mirrors the existing `_parse_budget` contract for `--watchdog-budget-s` (empty → in-code default,
garbage → fail-loud). A dedicated `_parse_score_window(raw) -> int | None` helper carries it, unit-tested
red-green.

### 3.4 Canary-coverage limitation (surfaced, not hidden)

The preflight two-probe canary does NOT validate the tuned Stage-2 scoring window: Probe M is Stage-1-build
only, Probe C is a fixture with no windowing. Therefore a too-large `tracking_marts_score_window` is NOT
caught pre-fan-out — it surfaces as the same per-unit OOM the drain rolls forward, exactly as today with the
baked constant. This is acceptable and in line with the current posture because:
- **Correctness is window-invariant** (neutrality, already proven) — a wrong window value can only cost memory
  / wall-time, never corrupt output.
- Memory-fit at a given window is inherently empirical per unit; the operator confirms it by the re-run
  succeeding (verified via the OUTPUT mart tables, N3), which is the same evidence standard GATE-0 used.

Extending Probe C/M to also exercise a Stage-2 window at the tuned value is a possible future hardening but is
**not** in scope here (it would re-introduce a real-unit-scale score into the 1200 s preflight budget — the
exact thing ADR-087 amendment 3 removed). Recorded as a known limitation, not a silent gap.

## 4. Files touched

- `terraform/modules/workflows/main.tf` — add the `tracking_marts_score_window` job parameter; add the
  `--score-window-frames` argument to the `compute_tracking_marts_iteration` wheel task.
- `src/ingestion/tracking_marts_processor.py` — `TrackingMartsProcessor.__init__` gains
  `score_window_frames: int | None = None`; `process` resolves and uses it.
- `src/ingestion/tracking_marts_drain.py` — `main_tracking_marts_drain_worker` adds the `--score-window-frames`
  extra arg + parses it; `_run_worker` gains `score_window_frames` and passes it to the processor; a
  `_parse_score_window` helper.
- `src/tests/` — wiring + parse tests (see §5).
- `docs/superpowers/adrs/ADR-090-*.md` — amendment note: the Stage-2 scoring window is now a runtime
  job-parameter (`tracking_marts_score_window`); `_TM_SCORE_WINDOW_FRAMES` is the fallback default.
- `pyproject.toml` + `scripts/bump_wheel.py` propagation — wheel version bump (new wheel must deploy to be
  usable on Databricks).
- `TODO.md` — updated in the shipping commit (standing rule).

## 5. Test plan (red-green, non-vacuous)

All new tests must be watched to FAIL first against the un-plumbed code, then PASS — a wiring test that passes
before the wiring exists proves nothing.

1. **Parse helper** (`_parse_score_window`): empty/whitespace/`None` → `None`; `"4000"` → `4000`;
   `"0"`/negative → `SystemExit`; `"abc"` → `SystemExit`. Pure, no Spark.
2. **Drain-worker → processor wiring**: patch `TrackingMartsProcessor` at its import site in `_run_worker` (or
   assert on the constructor kwarg) and prove that `--score-window-frames 4000` reaches the processor as
   `score_window_frames=4000`, and that an empty value reaches it as `None`. Mirrors the existing
   drain-worker arg tests.
3. **Processor → dispatch wiring**: with a fake `precompute_stage2a` / `build_stage2_mart_sdf` (spy), prove
   `process` passes the injected `score_window_frames` as `target_window_frames` to BOTH calls, and that
   `score_window_frames=None` passes the baked `_TM_SCORE_WINDOW_FRAMES` to both. This is the test that proves
   the SAME value feeds routing and dispatch (the cross-engine agreement invariant).
4. **Default-unchanged guard**: assert `_TM_SCORE_WINDOW_FRAMES == 8000` is still the fallback (a change to the
   default is a separate approval; this pins that this change did not move it).

Neutrality itself is NOT re-tested here — it is already proven window-size-independent by the shipped ADR-090
tests, and this change only chooses *which* size is passed.

## 6. The seven local checks + the hidden eighth

Before the commit gate, run all seven (ruff check, ruff format --check, lint-imports, bump_wheel --check,
pip_audit_ignores --check, pyright over the CI target, full `uv run pytest`) AND `validate_workflow_cards`
(the sk4118-lesson hidden gate). pyright target is the CI scope (`src/ hf_taipy_app/src/` + the
`sync_tf_env_pins` modules), NOT `pyright src/`. Full pytest via the CI extras, not collect-only.

Terraform: `terraform fmt` + the repo's terraform-plan equivalent so the plan CI stays green; the new
parameter + task arg is an additive, in-place change (no resource replacement).

## 7. Rollout (each step a SEPARATE explicit approval — nothing bundled)

1. Implement on `feat/tracking-marts-score-window-param`; stop at the commit gate.
2. **[approval]** commit → push → PR → watch CI green.
3. **[approval]** admin-merge (`gh pr merge <#> --squash --admin`) → watch post-merge CI (incl. terraform-apply
   + `patch_job_retries`) green.
4. **[approval]** confirm the new wheel deployed to the UC Volume (post-merge CI).
5. **[approval]** tuning run(s): `run_now(job, only=[preflight, compute], job_parameters={
   "tracking_marts_providers": "skillcorner", "tracking_marts_score_window": "<N>"})` against ONE failed p2
   unit (the stale events for the 4 are already cleared) — lower `N` until the densest unit fits; confirm via
   the OUTPUT mart tables.
6. **[approval]** re-run the four OOM'd units (`1204769p2`, `1374493p2`, `1562415p2`, `2015213p2`) at the
   fitting `N`; verify each wrote real rows across all six marts.

No `run_now` fires without CI green (no wheel-consuming job before post-merge CI). No commit / push / PR /
merge / job-run is implied by this spec — each is its own approval.

## 8. Risks

- **R1 — wrong value still OOMs.** A too-large `N` fails the same way as today (per-unit OOM, rolled forward).
  Mitigated by §3.4 (correctness-safe) + the tuning run in §7.5 converging before the full re-run.
- **R2 — routing/dispatch window-id drift.** If `precompute_stage2a` and `build_stage2_mart_sdf` ever got
  different window values the window ids would disagree and outputs would be wrong. Mitigated by resolving
  `window` ONCE in `process` and test §5.3 pinning that both calls receive it.
- **R3 — partial wheel bump.** A hand-edited consumer left stale. Mitigated by `bump_wheel.py` (not hand
  edits) + `bump_wheel --check` in CI.
- **R4 — card validator.** A new CLI arg could trip `validate_workflow_cards`. Mitigated by running it locally
  (§6) before the commit gate; no new task/entry-point is added (only an arg + a job param), so no new card is
  expected.

## 9. Approval gate

This spec and its plan require two independent reviews (saved to `D:\Development\_reviews\`) and the operator's
explicit approval before ANY implementation. No code is written until both reviewers approve and Karsten says
go.
