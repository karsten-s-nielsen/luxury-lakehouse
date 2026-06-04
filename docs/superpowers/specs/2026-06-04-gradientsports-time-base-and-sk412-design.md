# GradientSports period-relative `time_seconds` + silly-kicks 4.12.0 time-base guard — Design

| Field | Value |
|---|---|
| **Date** | 2026-06-04 |
| **Status** | Implemented (Parts A + C); Part B = post-deploy operator runbook |
| **ADR** | [ADR-040](../adrs/ADR-040-gradientsports-period-relative-time-and-time-base-guard.md) |
| **silly-kicks** | 4.12.0 (silly-kicks ADR-017) |
| **Wheel** | 0.5.16 → 0.5.17 |

## Goal

Fix the GradientSports period-2 silent ~81% AC-1 data loss at its source (GS actions become period-relative, matching GS frames and silly-kicks' canonical convention), and adopt silly-kicks 4.12.0's per-period link-coverage contract — wiring `validate_time_base` as a loud per-work-unit guard so the time-base-mismatch class can never ship silently again.

## Scope

| Part | What | Status |
|---|---|---|
| **A** | Adapter fix: GS `time_seconds` → period-relative | ✅ implemented |
| **C** | silly-kicks 4.12.0 floor + `validate_time_base` work-unit guard + `on_low_coverage="ignore"` per-batch | ✅ implemented |
| **B** | Reprocess all GS SPADL→VAEP + full-refresh marts | ⏳ post-deploy runbook (below) |
| — | AC-1 GS data recompute | ⏸ deferred to the "delete + recompute all action-context" effort |

## Part A — adapter fix (implemented)

`src/ingestion/spadl_adapter.py::adapt_gradientsports_events`:
- `_GS_NOMINAL_PERIOD_START_SECONDS = {1:0, 2:2700, 3:5400, 4:6300, 5:7200}` + `_gs_nominal_period_start` helper.
- `time_seconds = startGameClock − nominal_offset[period]` (Step 2b). Exact: GS `startGameClock = nominal_offset[period] + period_elapsed` (verified on GS 10503). Inverse of `stg_spadl__action_values.sql`'s `minute` offsets.
- Test: `test_gradientsports_spadl.py::test_adapt_time_seconds_is_period_relative` (multi-period lock) + updated `test_adapt_derived_columns_from_game_events` (3500→800).

## Part C — silly-kicks 4.12.0 + work-unit time-base guard (implemented)

- **Floor → 4.12.0:** `pyproject.toml`, `scripts/submit_ac1_oneshot.py`, 6 trainer `_REQUIRED_SK_MIN`, `test_sk3_mig_b_orchestrator_invariants.py`, `terraform/modules/workflows/main.tf`, C4. Wheel → 0.5.17 (`bump_wheel.py`, 28 files).
- **Guard helper (frame-independent):** `src/analytics/action_context/time_base_guard.py::assert_work_unit_time_base(action_period_min)` — asserts each period's actions are period-relative (`min(time_seconds) >= 1800 s` ⇒ absolute match clock ⇒ raise). It deliberately does **not** wrap silly-kicks' `validate_time_base`: that frame-overlap metric can't distinguish a base mismatch from sparse/narrow frame coverage and false-raised on dead-ball / broadcast-gap work units (caught by the full suite during this PR). The action-only check needs no frames, so it never false-fires on sparse tracking.
- **Both drivers wired**, before the per-batch pre-filter, guarded by `if "time_seconds" in …columns`: `pipeline.run_work_unit` (local) + `ingestion.action_context._process_tracking_match` (Spark, from the in-driver `actions_pdf` — no extra Spark action). Source-level sentinel in `test_time_base_guard.py` keeps both in lockstep (Spark driver not locally runnable).
- **Per-batch `link_actions_to_frames` → `on_low_coverage="ignore"`** at `enrich.py` + `tracking_context.py` (per-batch coverage isn't meaningful after the pre-filter + M13 dedup; also suppresses 4.12.0's warn-by-default in the UDF).
- **Goldens:** unchanged (4.12.0 additive; IDSSE goldens healthy → guard/`on_low_coverage` don't fire). Verified by the full suite incl. `test_mini_golden`.

## Part B — reprocess runbook (operator, AFTER the PR merges + wheel deploys)

> The reprocess needs the deployed 0.5.17 wheel (with the Part-A adapter fix). Run only after main's post-merge workflows finish on the new SHA (`feedback_wait_for_post_merge_ci_before_operator_runtime`).

1. **Re-ingest all GS SPADL→VAEP** (rewrites `bronze.spadl_actions` + `bronze.vaep_action_values` for GS with period-relative `time_seconds`):
   - Run the GS events→SPADL conversion + VAEP compute for all GS matches (the `spadl_conversion` / `spadl_vaep` GS path). Confirm post-run: `SELECT period, MIN(time_seconds), MAX(time_seconds) FROM bronze.spadl_actions WHERE data_source='gradientsports' GROUP BY period` — period 2 must now be `~[0, ~3150]`, NOT `~[2700, ~5850]`.
2. **Full-refresh the affected marts** (NOT incremental — `action_value_id` hashes `time_seconds`, so changed GS rows get new keys; an incremental run would append duplicates):
   - `dbt build --full-refresh --select fct_action_values fct_gk_actions_detail` (or the equivalent operator path).
   - Spot-check the `minute` fix: a GS p2 action that previously showed `minute ≈ 103` should now show `≈ 58`.
3. **AC-1 (deferred):** not part of this reprocess. When the "delete + recompute all action-context" effort runs, GS bronze is already period-relative, so `assert_work_unit_time_base` passes. (If a one-off GS AC-1 reprocess is ever wanted sooner: `DELETE FROM bronze.spadl_action_context WHERE data_source='gradientsports'` then re-run AC-1 preflight+drain, or the `--match-ids "gradientsports:…"` drain bypass.)

## Hyrum / blast radius (verified)

- `fct_action_values.action_value_id` + inherited `fct_gk_actions_detail.gk_action_id` change for all GS rows → full-refresh required (above).
- GS `minute` was already double-counted in gold (p2 ≈ 103 vs correct ≈ 58); the fix corrects it mart-wide.
- All other `time_seconds` consumers are safe — each guards time-delta/sort by `period` (verified sweep: scoutgpt, export_scoutgpt, football2vec sorts, VAEP).
