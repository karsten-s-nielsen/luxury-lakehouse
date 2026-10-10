# Preflight Size-Signals Single-Pass — Design (ADR-087 amendment follow-up)

## 1. Problem

The two-probe canary (ADR-087 amendment, merged `b15dc549`, wheel 0.5.119) fixed the dense-unit canary cost, but the `--full` preflight **still times out at the 1200 s task budget**. Isolated live 2026-10-09:

- `preflight_tracking_marts --full --providers idsse,skillcorner` → TIMEDOUT (run `67424733764183`).
- `--providers idsse` ALONE (~7 matches / 14 units) → TIMEDOUT at **~1258 s** (run `404089556508578`).

idsse-only timing out rules out combined-volume. By elimination the bottleneck is **`compute_tracking_size_signals`** (the new Probe-M/Probe-C size aggregate), NOT the probes: Probe C (`process(dry_run)` on ONE unit) and Probe M (ONE window build) are executor-distributed under the ADR-088 two-stage path (minutes). The aggregate runs BEFORE the canary and eats the budget. (Driver logs are unretrievable for a timed-out serverless task; the isolation is the evidence.)

## 2. Root cause

`compute_tracking_size_signals` (`tracking_marts_driver.py`), per in-scope provider, over the semi-joined OPEN units (for `--full` = the whole in-scope corpus, idsse ≈ 1.73 M/half × 14 ≈ 24 M rows):

1. `raw.groupBy("match_id","period").count()` — **read 1** (R1 per-unit count).
2. `assign_frame_windows(raw, …)` then `.groupBy(match,period,_window_id).count()` — **read 2** PLUS a `dense_rank` window-fn (**sort + shuffle** over the full corpus) + the halo `unionByName`, then the groupBy.

**Cost model (corrected per PSS-SPEC-01):** the dominant costs are **the two full reads + the corpus-wide `dense_rank` sort/shuffle**. The halo `unionByName` is NOT a 3× blow-up — `left`/`right` add only the first/last `halo_frames` (≤ 25) of each 20 000-frame window = **< 0.3 % extra rows**, negligible. So the levers are: (a) the redundant SECOND read (R1 is recomputed independently of R2), and (b) the `dense_rank`, which is INHERENT to the window-id (frames are non-contiguous, so an ordinal rank is required) and is NOT removed by this fix. The fix halves the reads + single-sources the window id; whether that alone clears 1200 s is **UNPROVEN pre-merge** — only R5 (post-merge wall-time measurement) proves sufficiency, with R6 (timeout bump) the data-driven fallback if the `dense_rank` + the real-unit Probe C still dominate.

## 3. Design — single-pass, halo-free counting, single-sourced window id

### 3.1 Extract the core window-id assignment (single source, no drift)
Factor the CORE `_window_id` computation out of `assign_frame_windows` into `assign_core_window_ids(raw_sdf, provider, *, target_window_frames) -> SparkDataFrame` (adds `_window_id = (dense_rank(frame) over (match,period) ordered by frame − 1) // target`, no `_is_halo`, no halo union). `assign_frame_windows` is refactored to call it and add the left/right halo union on top — so the core `_window_id` is **single-sourced**; the counting path and the real build path cannot drift (the load-bearing invariant: Probe M must select the window the drain actually builds).

### 3.2 Count once, derive R1 from R2
`compute_tracking_size_signals` per provider:
- ONE `raw` read → `assign_core_window_ids(...)` → `groupBy(match_id, period, _window_id).count()` → the CORE row count per window → `WindowRef`.
- Derive the per-unit R1 count by **summing** a unit's window counts (drop the separate `raw.groupBy().count()` scan).

Net: **1 read + 1 dense_rank + 1 groupBy per provider** (was 2 reads + a dense_rank + the <0.3% halo union + a second groupBy). No `.cache()`/`.persist()` (serverless-forbidden); the win is dropping the redundant read + the union, NOT removing the `dense_rank` — **the `dense_rank` is retained and is the residual bottleneck** (PERF-SPEC-01 / PERF-PLAN-01). Whether that clears 1200 s is proven only by R5.

### 3.3 `WindowRef.n_rows` becomes CORE rows (was core+halo)
The halo adds at most `2·H` frames to a window — `H = halo_frames(provider)` ≈ 22 at 25 Hz, ≈ 26 at 30 Hz (the `< 0.3 %` bound holds to ~29) — vs `target = 20000` → **< 0.3 %**. The bound (not "uniform"): an INTERIOR window gets `+2H` (left + right neighbours), an EDGE window `+H`; the **`≤ 2H`** bound is the argument — it cannot flip the densest-window ranking at the 20000-frame scale. The densest-CORE window is therefore the densest build window, and Probe M still **BUILDS** the selected window (`build_fit_probe`) — the real memory check; `n_rows` is only the selector. CANARY-SPEC-01 (GLOBAL densest across in-scope units, not densest-unit) is PRESERVED. Docstrings + the ADR note corrected (was "equals exactly the UDF core+halo input").

### 3.4 No behavioural change to the canary, scope, or enqueue
Only `compute_tracking_size_signals` + the `assign_frame_windows` refactor change. Probe selection, `--providers`, the enqueue, Probe C/M execution are untouched.

## 4. Requirements

- **R1** `assign_core_window_ids` is the single source of the CORE `_window_id`; `assign_frame_windows` calls it (its core `_window_id` + the Docker-pinned `== searchsorted//target` invariant unchanged).
- **R2** `compute_tracking_size_signals` does ONE scan/provider: `assign_core_window_ids` → one `groupBy` count; R1 per-unit = sum of the unit's window counts.
- **R3** `WindowRef.n_rows` = CORE rows per window; selection (global densest) unchanged. The halo-constant approximation documented.
- **R4** No `.cache`/`.persist`; no `.toPandas` (grouped `.collect()` of the small aggregate result only — unchanged).
- **R5** POST-MERGE measurement gate (operator, separate approval) — the BINDING sufficiency proof (the retained `dense_rank` makes pre-merge sufficiency unknowable; Databricks wall-time is unmeasurable in CI, the R7 that bit twice). Re-run `preflight_tracking_marts --full --providers idsse` and read the preflight task wall-time, with a **quantified threshold**:
  - **≤ 600 s** (half the 1200 s budget) → PASS, proceed to the full re-materialize.
  - **600–1000 s** → PASS but apply **R6** (raise `timeout_seconds` to ≥ 2× the observed wall-time) before the full-corpus run.
  - **> 1000 s or TIMEDOUT** → R6 MANDATORY; confirm via the run's Spark UI whether the `dense_rank` or the real-unit Probe C dominates, and size `timeout_seconds` to the measured work.
- **R6 (data-gated, per R5):** raise the preflight `timeout_seconds` — the `--full` preflight does legitimately heavy planning (the corpus `dense_rank` + a real-unit Probe C), unlike the retired `units[:1]` waste, so a larger budget is correct, not a band-aid. Decided WITH the R5 number, never pre-emptively. (If R5 shows `dense_rank` dominates, R6 is the fix — the ordinal needs the sort; there is no cheaper EXACT per-window count.)

## 5. Non-vacuity / tests

- **Docker real-Spark parity:** `assign_core_window_ids(raw).select(match,period,frame,_window_id)` equals the CORE rows of `assign_frame_windows(raw)` (same `_window_id` per frame) — the single-source invariant. Discriminating (a window-id formula change fails it).
- **Docker aggregate:** `compute_tracking_size_signals`-shaped count on the mini fixture → the per-window CORE counts + the summed per-unit counts; the global-densest `WindowRef` is the true argmax (CANARY-SPEC-01 non-densest-unit case still holds).
- **Red-green:** watch the parity test fail if `assign_frame_windows` is reverted to an independent window-id.
- Full `uv run pytest` + the existing canary/preflight suites unchanged-green (the pure selection tests are agnostic to how `WindowRef`s are produced).

## 6. Scope / commit / approval

- One feature branch `perf/preflight-size-signals-single-pass` off `origin/main` (`b15dc549`); one coherent commit; no worktrees/micro-commits.
- Spec + plan bundled with the PR. `git commit`/`push`/`gh pr create`/`gh pr merge` each a SEPARATE explicit approval. No job run until post-merge CI green (R5 is then its own approval).
- Wheel bump required (the preflight code runs on Databricks) → 0.5.119 → **0.5.120** via `bump_wheel.py`.

## 7. ADR

Append to **ADR-087** (2026-10-09 amendment follow-up): the size aggregate (`compute_tracking_size_signals`) timed out the `--full` preflight — its cost was a redundant R1 read + a corpus-wide `dense_rank` + the `assign_frame_windows` halo union (the halo is `≤ 2H` frames/window, `<0.3%` — NOT a 3× blow-up). The fix single-sources the CORE `_window_id` (`assign_core_window_ids`) and counts once, dropping the redundant read + the halo union; the `dense_rank` is RETAINED (the residual bottleneck, gated by the R5 post-merge wall-time measurement + R6 timeout fallback). `WindowRef.n_rows` = CORE rows. Cross-reference ADR-089 (`assign_frame_windows`).

## 8. Out of scope

- Probe C/M logic, `--providers`, the enqueue, the drain, AC `_compute_ac_unit_sizes` (its SPADL-action aggregate is events-grain, cheap, already semi-joined — not implicated; leave it).
- The re-materialize operator run (resumes at GATE 0 after this ships + R5 confirms).
