# Tracking-Marts Preflight Canary — Bounded + Scope-Aware (ADR-087 amendment) — Design

## 1. Problem

`preflight_tracking_marts --full` (the re-materialize preflight, required after any output-table wipe per `tracking_marts_drain.py:127`) **TIMES OUT** at the 1200 s preflight task budget (`terraform/modules/workflows/main.tf:1540`, `timeout_seconds = 1200`). Observed live 2026-10-09: run `414142904894469`, task `preflight_tracking_marts` → `TIMEDOUT`.

This BLOCKS the combined sk4118-P2 ∪ sk4128/§8 ∪ haloed re-materialize: the `--full` preflight cannot complete, so the drain cannot be reached.

## 2. Root cause (evidence)

1. `discover_tracking_units` (`tracking_marts_driver.py:62`) returns units **`sorted(...)`** → provider alphabetical → `gradientsports` is first (`g < i < m < s`).
2. `select_canary_units` (`canary.py`: `def` at :19, `return units[:1]` at :26) = **`units[:1]`** → the first unit = a `gradientsports` unit, the **densest** provider (~2.4 M rows/half, §1.1 of the executor-refactor spec). (Its docstring is stale — says budget "600 s" (live TF = 1200 s) and `run_canary` says "one unit per provider" though `units[:1]` was ever only one unit total; both corrected in the §3 rewrite, plan T3.)
3. `process(dry_run=True)` (`tracking_marts_processor.py:212-320`) runs **both stages in full**: Stage-1 windowed build+spill of the whole unit, then Stage-2 scoring (`int(scored_sdf.count())`) of all six marts. `select_canary_units`' own docstring: *"a full-processor dry-run costs ~one real drain unit (defcon / pitch-control dominate)."*

So on `--full`, `units[:1]` over a provider-sorted universe deterministically canaries the **worst-case dense unit** and pays ~one full drain unit of compute inside a 1200 s planning task. Never exercised pre-merge (no `--full` re-materialize under the ADR-088 two-stage path existed). Triply wrong for the current cycle: the canary dry-ran a `gradientsports` unit — the provider **carved out** of this re-materialize (D1, owner 2026-10-09).

Driver logs were not retrievable via `get-run-output` for the timed-out serverless task; the code path (sorted→GS-first + `units[:1]` + whole-unit dual-stage) is conclusive. The seam's dense no-OOM is independently proven (haloed T9 offline canary: `gradientsports:10502:1` built 1 912 916 rows, no OOM) — this is NOT a seam regression, it is a preflight-cost defect.

**Non-fix (rejected, owner 2026-10-09):** raising `timeout_seconds`. A planning preflight must not run a dense whole-unit compute; the fix bounds the work, not the clock. The 1200 s budget is UNCHANGED.

## 3. Design — gold-standard two-probe scope-aware canary

The canary guards two distinct failure classes; probe each at its **cheapest sufficient input**:

### 3.1 Probe M (memory fit) — GLOBAL densest in-scope WINDOW, build+spill only
Stage-1 windowed build+spill (`windowed_build_sdf`) of the **single densest frame-window across ALL in-scope units**, asserting it fits the UDF-group cap (no OOM). The ADR-089 memory bound is **per-window**, so the GLOBAL worst-case window proves the whole corpus is safe — and that window can live in a **non-densest unit** (gs-oom-canary diagnostic: per-window frame counts vary widely, a window ≫ the `_TM_WINDOW_FRAMES` target can sit in a mid-size unit). Picking the densest window only *within* the densest unit would **false-green** and let a real `--full` OOM through behind a green preflight — the exact failure the canary exists to catch. **No Stage-2 scoring** (scoring is whole-unit and irrelevant to the build-memory bound). Cost = O(one window) + a cheap global `GROUP BY _window_id COUNT` aggregate (no build). Makes the haloed T9 offline proof a **standing live gate** (re-proven every re-materialize) — [guards-that-cannot-fail].

### 3.2 Probe C (correctness smoke) — smallest in-scope unit PER PROVIDER, full dry-run
Full `process(dry_run=True)` on the **smallest whole unit of EACH in-scope provider**: both stages + all six scorers + real schemas. Per-provider because each provider has its own tracking builder/conversion (idsse/skillcorner/gradientsports `*_TRACKING_SELECT_COLS` + builders differ), so a provider-specific wiring/schema defect only surfaces on that provider's unit. The ADR-087 amendment (2026-09-23) went to ONE unit to avoid a per-provider fan of **dense** dry-runs blowing the budget — that concern does **not** apply here: the fan is over the **smallest** unit per provider, bounded. Whole-unit (preserves the global action→frame link, §1.1 — scoring is NOT window-decomposable; `tracking_marts_dispatch.py:271`). Cost = O(Σ smallest-per-provider) ≪ O(dense unit).

### 3.3 Scope-aware
Both probes select ONLY from the run's **in-scope providers**. Each preflight gains a `--providers` arg (comma inclusion; empty/unset = all `_TRACKING_PROVIDERS` — today's behaviour) threaded into discovery (`discover_tracking_units(providers=...)`), so a carved-out provider (gradientsports this cycle; metrica always, tracking-excluded) is never canaried — and the same scope filters the enqueue, giving the re-materialize its **GS-tracking carve-out lever** (D1). The GS carve-out passes `--providers idsse,skillcorner`.

### 3.4 Applicability — BOTH drains; Probe M is OPTIONAL (non-breaking)
`run_canary` + `select_canary_units` are SHARED — used by the tracking-marts preflight (`tracking_marts_drain.py:206`, `TrackingMartsProcessor`) AND the AC preflight (`action_context.py:1072`, `SparkGameProcessor`). The fix applies to BOTH (owner 2026-10-09, fold AC — the AC canary carries the identical `units[:1]`-on-sorted-universe latent defect, [audit-siblings]), but NON-BREAKING:
- **Shared:** Probe C (smallest in-scope unit per provider, §3.2) + scope-awareness (§3.3) apply to both drains.
- **Probe M (tracking-marts ONLY):** the global-densest-window build-fit (§3.1) guards the ADR-088 two-stage build-spill. AC is NOT two-stage (direct `mapInPandas`, §1.2 of the executor spec) → NO window-OOM class → AC gets **no Probe M**.
- **Port separation (no shared-Protocol break):** `build_fit_probe` lives on a NEW `BuildFitProbePort` Protocol implemented ONLY by `TrackingMartsProcessor`, NOT added to the shared `GameProcessorPort`. `run_canary` runs Probe M only when `probes.memory_window is not None` (set only by the tracking-marts preflight), so `SparkGameProcessor` never needs it and the AC path is untouched.

## 4. Requirements

- **R1 — unit size signal (faithful proxy, LOAD-BEARING).** `discover_tracking_units` returns a per-unit frame COUNT for Probe-C smallest-per-provider selection. The proxy MUST be a faithful frame-density signal — `spadl_action_context` ACTION row-count is NOT frames; confirm at impl (T1.2) whether it is monotone-faithful enough for "smallest unit" or whether a raw-tracking-frame count is required. Determinism of the triple ordering preserved. (Probe C needs only relative unit smallness, a weaker faithfulness bar than R2.)
- **R2 — GLOBAL densest window signal (LOAD-BEARING for Probe M worst-case).** Pick the densest frame-window across **ALL in-scope units** (NOT within the densest unit — CANARY-SPEC-01): `assign_frame_windows(raw_sdf, provider, target_window_frames=_TM_WINDOW_FRAMES)` over the in-scope raw tracking frames, then `GROUP BY (data_source, match_id, period_id, _window_id) COUNT(*)`, global max. One aggregate pass, NO build. The count must be the per-window BUILT-row proxy that actually drives the UDF-group peak — raw-frame rows per window (verify faithful at impl; if the build expands/filters frames, count the build input the UDF receives).
- **R3 — Probe M path.** A Stage-1-only build-fit entry (processor or dispatch) that builds+spills the ONE global-densest `(provider, match, period, _window_id)` (filter `raw_sdf` to that `_window_id`) and asserts non-empty built output (forces the build UDF), persists nothing, returns 0. Reuses `windowed_build_sdf`/`run_stage1_build_and_spill`; no new build logic.
- **R4 — Probe C unchanged scorer path.** Probe C calls the EXISTING `process(dry_run=True)` on the smallest in-scope unit **of each in-scope provider** (a bounded per-provider fan). No change to scoring semantics.
- **R5 — selection.** `select_canary_units` → `select_canary_probes(units_with_sizes, in_scope_providers)` returning `(global_densest_window_ref, {smallest_unit per in-scope provider})`, scope-filtered. Deterministic tie-break on `(provider, match_id, period, _window_id)`. Empty in-scope set → no-op (log, no raise).
- **R6 — scope arg.** Preflight accepts an optional provider-scope job param (reuse the existing `provider` job param if it already scopes discovery; else add `--provider`/`--exclude-provider`), threaded to discovery. Confirm the existing `provider` param's current semantics at impl before adding a new one (avoid a duplicate lever).
- **R7 — budget.** `timeout_seconds` UNCHANGED (1200 s). The design's success criterion: `--full` preflight completes well within it on the full corpus.
- **R8 — metrica (one rule).** metrica is NOT a seam provider (`SEAM_PROVIDERS = {idsse, skillcorner, gradientsports}`) and is not re-materialized here → **metrica is excluded from BOTH probes**. No conditional.

## 5. Non-vacuity (red-green, [guards-that-cannot-fail])

- **Probe M:** a seeded build-path defect (e.g. a build UDF raising on the densest window) MUST make Probe M raise; a no-op/empty-build MUST fail the assertion. Watch it fail first.
- **Probe C:** a seeded scorer/schema defect MUST make Probe C raise (preserve the current `run_canary` raise-on-first-failure). Watch it fail first.
- **Scope:** assert a carved-out provider is NEVER selected by either probe (the exact defect that dry-ran gradientsports).
- **Cost bound:** a test asserts Probe C selects the SMALLEST unit per in-scope provider (never the densest), and Probe M selects exactly ONE window = the GLOBAL densest across in-scope units (a denser window in a non-densest unit MUST be the one picked — the CANARY-SPEC-01 regression test).

## 6. Tests

- Pure-core selection tests (`select_canary_probes`): GLOBAL densest window picked even when it lives in a non-densest unit (CANARY-SPEC-01 regression); smallest-unit-per-provider picked; scope filter excludes carved-out providers; empty-scope no-op; tie-breaking deterministic.
- Probe M fixture test: one dense fixture window builds+spills + non-empty assertion; Docker real-Spark parity (mirrors `test_ac_windowed_dispatch_spark.py`).
- Probe C: smallest-unit-per-provider full dry-run on an idsse mini fixture completes + computes all scorers; seeded-defect raises.
- `discover_tracking_units` R1 count: byte-identical triple ordering + correct counts on a fixture.
- Full `uv run pytest` 0-failed; the ADR-087 existing canary tests updated (not deleted) to the two-probe API.

## 7. Scope / commit / approval discipline

- One feature branch `fix/tracking-marts-canary-bounded` off `origin/main` (no worktrees). One coherent, fully-tested commit (all 7 gates + `validate_workflow_cards` if any card changes — none expected). No micro-commits.
- Spec + plan committed **bundled** with the implementation PR ([specs-bundled]).
- **Explicit human-approval gate (hard):** `git commit` / `git push` / `gh pr create` / `gh pr merge` each a SEPARATE explicit approval. No operator job runs until post-merge `main` CI is green.
- Nothing deferred/dropped without owner approval.

## 8. ADR

- **Amend ADR-087** (preflight drain canary): record the two-probe bounded + scope-aware design, WHY `units[:1]` was a worst-case selector under `--full` + the ADR-088 two-stage path, and the per-window memory-fit standing gate. Cross-link ADR-088 (two-stage build-spill) + ADR-089 (haloed frame-window seam).

## 9. Out of scope

- The re-materialize operator run itself (separate, approval-gated; resumes after this ships + post-merge CI green).
- Any change to Stage-2 scoring semantics, the drain fan-out, or the gkdv pooling.
- gradientsports tracking re-materialize (rides the silly-kicks TF-65 ball-velocity-fix cycle).
- **IN scope (added, owner 2026-10-09):** the AC preflight canary — scope-aware + Probe-C (smallest-unit-per-provider), NO Probe M. The AC drain scorers / fan-out are NOT otherwise touched.
