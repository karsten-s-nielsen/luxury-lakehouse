# Plan — Tracking-Marts Preflight Canary Bounded + Scope-Aware

Implements `docs/superpowers/specs/2026-10-09-tracking-marts-canary-bounded-design.md`. Branch `fix/tracking-marts-canary-bounded` off `origin/main` (`7e3af32b`). One coherent commit. No code runs on Databricks until post-merge CI green.

## Global constraints
- Implement INLINE (no subagents). Pure cores in `analytics` (no Spark); Spark dispatch in `ingestion` (import-linter).
- **TEST-FIRST (red-green), every guard — watch it fail before implementing** ([guards-that-cannot-fail], spec §5). Every task below leads with its failing test, then the impl that greens it. No test is deferred to a later task.
- 7 local gates run at T7; additionally each task's own test must be green before moving on.

## Task 1 — confirm the two unknowns before editing (research, no edit)
- **T1.1** Read `main_tracking_marts_preflight` arg block + how the job `provider` param reaches `preflight_tracking_marts` (TF `terraform/modules/workflows/main.tf` `python_wheel_task.parameters`). DECISION: reuse the existing `provider` job param for scope if it already scopes discovery; else add `--provider`. Record in the ADR. (Avoid a duplicate lever.)
- **T1.2** Confirm the R1/R2 frame-density proxies are FAITHFUL (load-bearing, CANARY-SPEC-01 / SPEC-04): is `spadl_action_context` ACTION-row-count a monotone proxy for unit smallness (R1, weaker bar)? For R2 the per-window count MUST reflect the BUILT rows the UDF group receives (the memory-peak driver) — confirm against `read_raw_trk_sdf` + `make_build_udf`/`windowed_build_sdf` (does the build expand/filter frames?). Pick the faithful source before T2.

## Task 2 — size signals from RAW TRACKING (R1, R2) — T1.2 branch
**T1.2 resolved:** `spadl_action_context` holds ACTIONS, not frames → NOT a faithful frame-density proxy. R1/R2 counts come from RAW TRACKING (the build-UDF input grain that drives the memory peak). `discover_tracking_units`/`discover_open_units` stay UNCHANGED (the gate caller is unaffected); a SEPARATE size-signal function reads raw tracking. `WorkUnit.n_frames` (done, additive, not serialized) carries the Probe-C size, attached in the preflight.
- **T2.a (red)** `test_compute_tracking_size_signals` (Docker Spark): on a two-provider fixture, assert per-unit raw-row counts (R1) + per-`(provider,match,period,_window_id)` counts (R2, via `assign_frame_windows`), scoped to `providers`. Fails — no function.
- **T2.b (green)** `compute_tracking_size_signals(spark, catalog, providers) -> (dict[UnitKey,int], list[WindowRef])` in `tracking_marts_driver.py`: per in-scope provider read provider-wide raw tracking (`read_provider_raw_trk_sdf` — the per-unit projection of `read_raw_trk_sdf` WITHOUT the match/period filter), `assign_frame_windows(...)`, `GROUP BY (match_id, period, _window_id) COUNT(*)` (core+halo rows = the UDF-group input) for R2, and `GROUP BY (match_id, period) COUNT(*)` for R1. No build, no driver collect beyond the small grouped result.
- **T2.c (red)** `test_global_densest_window` (pure, in `test_canary.py`): `select_canary_probes` picks the GLOBAL densest window even when it lives in a NON-densest unit (CANARY-SPEC-01 regression). **DONE** — `test_probe_m_picks_global_densest_window_even_in_non_densest_unit`.
- **T2.d (green)** Global-densest selection is pure in `select_canary_probes` (`_window_sort_key`); **DONE** (canary.py).

## Task 3 — probe selection (R5, 3.3) + docstring fix — `analytics/action_context/canary.py`
- **Test home (MIGRATE, not delete — spec §6, TMC-PLAN-05):** T3.a/T3.c land by MIGRATING the existing `src/tests/action_context/test_canary.py` IN PLACE to the two-probe API — `test_select_canary_units_single_first_unit` (asserts `units[:1]`, imports `select_canary_units`) → `test_select_canary_probes` (T3.a); `test_run_canary_passes_and_uses_dry_run` + `test_run_canary_raises_on_processor_failure` → the two-probe `run_canary` cases (T3.c), **preserving** the raise-on-failure ADR-087 regression coverage. The file is updated, never deleted; green pytest must NOT be achieved by removing canary coverage.
- **T3.a (red)** `test_select_canary_probes` (in the migrated `test_canary.py`): global-densest-window (even in a non-densest unit) + smallest-unit-PER-in-scope-provider + scope-exclusion of carved-out providers + empty-scope no-op + deterministic tie-break. Fails — still `units[:1]`.
- **T3.b (green)** Replace `select_canary_units` with `select_canary_probes(units, in_scope_providers) -> CanaryProbes(memory_window, correctness_units)`; `correctness_units` = {smallest in-scope unit per provider}; `memory_window` = global densest in-scope `(unit, _window_id)`. Empty in-scope → `None`/empty. **Rewrite the stale docstrings** (drop "600 s"/"one unit per provider"; state the two-probe bounded design).
- **T3.c (red)** `test_run_canary_two_probe`: a seeded scorer-defect fake makes Probe C raise; a seeded build-defect fake makes Probe M raise; raise-on-first-failure preserved. Fails — `run_canary` still single-probe.
- **T3.d (green)** `run_canary(processor, probes, logger)` (signature change; BOTH callers migrate — T5): runs Probe C (`process(u, dry_run=True)` per `correctness_unit`) always; runs Probe M (`build_fit_probe`) ONLY when `probes.memory_window is not None`. `drain_canary_ok/_failed` per probe; raise-on-first-failure preserved. **`build_fit_probe` on a NEW `BuildFitProbePort` Protocol** (NOT added to the shared `GameProcessorPort` — would break the AC `SparkGameProcessor`); when `memory_window` set, `run_canary` `cast`s processor to `BuildFitProbePort` behind a `hasattr` guard that raises a clear error if absent (defensive). `select_canary_units` is removed — both callers migrate to `select_canary_probes` (so no stale shared symbol).

## Task 4 — Probe-M build-fit path (R3) — `ingestion/tracking_marts_processor.py` + `tracking_marts_dispatch.py`
- **T4.a (red)** `test_build_fit_probe` (fixture + Docker Spark parity, mirror `test_ac_windowed_dispatch_spark.py`): one dense fixture window builds+spills non-empty; a seeded build-UDF defect raises; an empty build fails the non-empty assertion. Fails — no `build_fit_probe`.
- **T4.b (green)** `TrackingMartsProcessor.build_fit_probe(unit, window_id)`: read `raw_trk_sdf`, `assign_frame_windows` then `.where(_window_id == window_id)`, run `windowed_build_sdf`/`run_stage1_build_and_spill` on that slice, assert built output non-empty (force the UDF), `cleanup_spill`, return 0. NO Stage-2. Reuse existing build helpers. metrica path N/A (R8 — never selected).

## Task 5 — scope-aware preflights, BOTH drains (R6, 3.3, 3.4) — `tracking_marts_drain.py` + `action_context.py`
- **T5.a (red)** `test_preflight_scope` (tracking-marts): with `--providers idsse,skillcorner`, `discover_open_units` excludes gradientsports from BOTH canary selection AND enqueue. Fails — no scope threading.
- **T5.b (green, tracking-marts)** Add `--providers` arg (comma inclusion, empty=all) → new job param `tracking_marts_providers` in TF (`terraform/modules/workflows/main.tf`, the `preflight_tracking_marts` task). Thread to `discover_open_units` → `discover_tracking_units(providers=...)`. `main_tracking_marts_preflight` builds `in_scope_providers`, computes window_counts (T2.d) for Probe M, calls `select_canary_probes(units, window_counts, in_scope)` + `run_canary(TrackingMartsProcessor, probes, …)`. Enqueue filtered by scope (GS-tracking carve-out lever, D1).
- **T5.c (red)** `test_ac_preflight_scope_and_probe_c` (AC): AC preflight scope-filters + Probe C selects smallest-unit-per-provider; NO Probe M (`memory_window is None`). Fails — AC still `run_canary(proc, units, …)`.
- **T5.d (green, AC fold)** `ingestion/action_context.py` `main_preflight`: AC discovery returns sized units (per-unit frame COUNT — AC's own discover path) + a `--providers` arg (new job param `action_context_providers` in the AC preflight TF task); `select_canary_probes(units, None, in_scope)` (window_counts=None → no Probe M) + `run_canary(SparkGameProcessor, probes, …)`. AC scorers/fan-out otherwise untouched.

## Task 6 — ADR-087 amendment (spec §8)
- Append the two-probe bounded + scope-aware section to `ADR-087`: WHY `units[:1]` was worst-case under `--full` + the ADR-088 two-stage path; the GLOBAL-densest-window per-window memory-fit standing gate; per-provider Probe C; cross-link ADR-088/089. Record the T1.1 scope-lever decision + the T1.2 proxy decision.

## Task 7 — gates + review + ship (approval-gated)
- **T7.1** 7 gates: `ruff check src/ scripts/`, `ruff format --check src/ scripts/`, `lint-imports`, `bump_wheel.py --check`, `pip_audit_ignores.py --check`, `uv run pyright` (CI target), full `uv run pytest` (0-failed). `validate_workflow_cards` if a card changed (none expected). `test_topandas_boundedness` if an exempted file changed.
- **T7.2** Wheel bump? Pure code + tests + docs, no new entry point / TF task / public wheel-consumer version requirement → **confirm NO bump** via evidence at T7.1 (`bump_wheel.py --check` clean on the unchanged version). Do not bump without a reason.
- **T7.3** TODO.md updated IN the commit ([todo-on-cycle-close]); verify `git status TODO.md` + `bump_wheel --check`.
- **T7.4** STOP at commit — show diff/file list, await explicit approval. Then push (approval) → PR (approval; body bundles spec+plan) → CI → merge `--admin` (approval) → post-merge main CI green.

## Then (separate, out of this PR)
Resume GATE 0: `preflight_tracking_marts --full` (now bounded) → two-probe canary verdict → T3 destructive gate of the combined recompute runbook.

## Self-review
- Spec §3.1 global densest window → T2.c/d + T3.a/b + T4; §3.2 per-provider Probe C → T3; §3.3 scope → T5; R1/R2 proxies → T1.2 + T2; non-vacuity §5 → the (red) step leading every task; ADR §8 → T6; discipline §7 → T7. ✓
- TEST-FIRST: every impl step (`.b/.d`) is preceded by its failing test (`.a/.c`). No batched-at-end tests. ✓ (CANARY-PLAN-01 / TMC-PLAN-02)
- CANARY-SPEC-01 resolved: global densest window across ALL in-scope units, with the non-densest-unit regression test (T2.c). ✓
- PLAN-03: WorkUnit-with-size pre-decided; all callers updated, no two-arity shim. ✓
- No Stage-2 semantic change; metrica out of both probes (R8); no deferrals; wheel-bump evidence-decided. ✓
