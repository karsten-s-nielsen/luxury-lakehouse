# Plan — Preflight Size-Signals Single-Pass

Implements `docs/superpowers/specs/2026-10-09-preflight-size-signals-single-pass-design.md`. Branch `perf/preflight-size-signals-single-pass` off `origin/main` (`b15dc549`). One coherent commit. No Databricks job until post-merge CI green.

## Global constraints
- Implement INLINE. Pure/dispatch split (import-linter) unchanged — all edits in `ingestion`.
- **TEST-FIRST (red-green):** the single-source parity test is written + watched to fail before the refactor.
- No `.cache`/`.persist`/`.toPandas` (serverless). 7 gates + `validate_workflow_cards` + Docker leg.

## Task 1 — extract the single-sourced core window-id (R1) — `ingestion/frame_window_dispatch.py`
- **T1.a (red)** Docker test `test_assign_core_window_ids_matches_assign_frame_windows_core`: `assign_core_window_ids(raw, idsse, target=100)` `_window_id` per `(match,period,frame)` == the CORE (`_is_halo==False`) rows of `assign_frame_windows(...)`. Fails — no `assign_core_window_ids` yet.
- **T1.b (green)** Add `assign_core_window_ids(raw_sdf, provider, *, target_window_frames)`: the `dense_rank(frame) over (match_id,period)` − 1 → `_window_id = ord // target`, NO `_max_ord`/`_pos`/`_last_window`/`_is_halo`/halo union. Refactor `assign_frame_windows` to call it (then compute the halo `_pos`/`_last_window` + the left/right union on top) so the core `_window_id` is single-sourced. `assign_frame_windows`' existing Docker tests (`test_ac_windowed_dispatch_spark`, `test_tracking_marts_canary_spark`) stay green (core `_window_id` == `searchsorted//target` unchanged).

## Task 2 — single-pass size signals (R2, R3) — `ingestion/tracking_marts_driver.py`
- **T2.a (red)** Docker test `test_compute_tracking_size_signals_single_pass`: on the mini fixture, per-window CORE counts + per-unit R1 = sum of its window counts; the global-densest `WindowRef` is the true argmax; NO dependence on `assign_frame_windows` (halo) counts. Fails — current impl uses `assign_frame_windows` + a separate scan.
- **T2.b (green)** Rewrite `compute_tracking_size_signals`: per provider, ONE `raw` read → `assign_core_window_ids(...)` → `groupBy(match_id, period, _window_id).count()` → `WindowRef(n_rows = CORE count)`; build `unit_counts` by SUMMING each unit's window counts (drop the separate `raw.groupBy(match,period).count()` scan). Drop the `assign_frame_windows` import; keep the `read_provider_raw_trk_sdf` semi-join to the OPEN units. Update the docstring: `n_rows` = CORE rows; the halo is a bounded `<0.3%` additive constant irrelevant to densest-window SELECTION (Probe M then BUILDS the window — the real memory check).

## Task 3 — tests (spec §5)
- **T3.a** Update the existing Docker test `test_window_count_aggregate_feeds_global_densest` (`test_tracking_marts_canary_spark.py`) to count via `assign_core_window_ids` (CORE) instead of `assign_frame_windows` (core+halo); the global-densest assertion is unchanged.
- **T3.b** The CANARY-SPEC-01 non-densest-unit regression (pure, `test_canary.py`) is agnostic to production (constructs `WindowRef`s directly) → unchanged-green.
- **T3.c** Red-green: revert `assign_frame_windows` to an independent core window-id → T1.a fails (proves the single-source invariant is load-bearing).

## Task 4 — ADR-087 amendment-follow-up note (spec §7)
Append: the corpus-wide `compute_tracking_size_signals` (a redundant R1 read + a corpus-wide `dense_rank` + the `assign_frame_windows` halo union) timed out the `--full` preflight; fix = `assign_core_window_ids` single-sourced + count-once, dropping the redundant read + the (negligible, `<0.3%`) halo union — the `dense_rank` is RETAINED (the residual bottleneck, gated by R5/R6). `WindowRef.n_rows` = CORE rows. Cross-ref ADR-089. (Do NOT write "3× halo union" — the halo is `≤ 2H` frames/window, `<0.3%`.)

## Task 5 — wheel bump + gates + ship (approval-gated)
- **T5.1** Wheel **0.5.119 → 0.5.120**: edit `pyproject.toml` then `uv run python scripts/bump_wheel.py`; update the `uv.lock` root-package version (the `version` under `[[package]]` `name = "luxury-lakehouse"`, not a pinned line number); `bump_wheel.py --check` consistent.
- **T5.2** 7 gates (ruff check+format, lint-imports, `bump_wheel --check`, `pip_audit_ignores --check`, `uv run pyright` CI target, full `uv run pytest`) + `validate_workflow_cards` + the Docker leg (`docker run … test_tracking_marts_canary_spark.py` + the new core-window-id test).
- **T5.3** TODO.md updated IN the commit ([todo-on-cycle-close]); `git status TODO.md` + `bump_wheel --check`.
- **T5.4** STOP at commit — show diff/file list, await approval. Then push → PR (spec+plan bundled) → CI → independent `/review-impl` (2 reviewers) → merge `--admin` → post-merge main CI green.

## Task 6 — POST-MERGE measurement gate (R5, SEPARATE approval, operator)
Re-run `preflight_tracking_marts --full --providers idsse`; read the preflight task wall-time against the spec-R5 threshold: **≤ 600 s → PASS**; **600–1000 s → PASS + apply R6** (raise `timeout_seconds` ≥ 2× observed); **> 1000 s / TIMEDOUT → R6 MANDATORY** + Spark-UI check whether the retained `dense_rank` or the real-unit Probe C dominates. The fix removed the redundant read + the <0.3% halo union but KEPT the `dense_rank`, so an R5-fail means the `dense_rank`/Probe-C work legitimately exceeds the budget → R6 (timeout) is the fix (the ordinal needs the sort; no cheaper exact count). Only on R5 PASS resume the combined re-materialize GATE 0.

## Self-review
- Spec §3.1→T1, §3.2→T2, §3.3→T2.b docstring + T3.a, §5→T1.a/T2.a/T3.c, §7→T4, §6/R5→T6. ✓
- Single-source invariant (Probe M window == drain-built window) is the load-bearing risk → T1.a Docker parity + T3.c red-green. ✓
- No change to probe logic/scope/enqueue/AC; wheel bump 0.5.120; measurement gate before the re-materialize. ✓
