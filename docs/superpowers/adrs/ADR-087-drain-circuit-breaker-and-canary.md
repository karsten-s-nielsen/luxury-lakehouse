# ADR-087: Drain circuit-breaker + in-preflight canary (systemic-failure fast-fail)

| Field | Value |
|---|---|
| **Date** | 2026-09-23 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (owner), luxury-lakehouse session |
| **Extends** | [ADR-037](ADR-037-action-context-worker-drain-fanout.md) (AC worker-drain), [ADR-067](ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) (unit-write atomicity / fail-loud), [ADR-068](ADR-068-ac-unit-events-and-drain-completeness-gate.md) (unit events + gate), [ADR-082](ADR-082-tracking-marts-drain-fanout.md) (tracking-marts drain) |
| **Spec/Plan** | `docs/superpowers/specs/2026-09-23-drain-hardening-design.md`, `docs/superpowers/plans/2026-09-23-drain-hardening.md` |

## Context

The P2 `compute_tracking_marts --full` run (`622418943470489`) was cancelled by the operator after ~5.5 h. Post-mortem: **654 `failed` unit-events**, every one `gk_decision: 'is_actor'`, across all four tracking providers. Two independent defects.

**Bug 1 — gk_decision was wired for SB360 only.** `score_gk_decision_reconstructed` (`gk_decision_writer.py`) unconditionally called sk `apply_actor_identities_to_frames`, an SB360-snapshot-only helper that reads an `is_actor` column (`keeper_identity.py:752`). Raw tracking frames (`_convert_tracking_batch`) have no `is_actor` → `KeyError('is_actor')` on every tracking unit. Deeper: even guarded, the default `frame_convention="per_action_ltr"` resolves each action's frame as `frame_id == action_id` (`sk _reconstruct.py:118-119`) — true only for SB360 snapshots; raw tracking frames need `match_ltr` (`link_actions_to_frames` + `resolve_defended_goals`, built internally at `:262`). So `fct_gk_decision` had never been populated for tracking providers; the drain-fold (sk4118 Phase E) wired only the SB360 path and never ran (crashed first). The writer's unit test hand-built SB360-style frames with `is_actor`, masking the live gap — the "tested on fixtures, fails on live inputs" class.

**Bug 2 — the shared drain had no fast-fail for systemic FAILURES.** `drain_worker` (`analytics/action_context/drain.py`) fast-aborts only a concurrent-abandoned-watchdog-thread ceiling (`max_abandoned`) — i.e. TIMEOUT storms. The per-unit FAILURE branch is fail-soft (`record failed; continue`), and `raise_on_failed_units` only fails the task at slice end. Nothing aborts a slice when a large fraction of units are *failing*, so a corpus-wide bug drains the full retry budget (× heavy per-unit compute × N workers) before the task fails. A wall-clock timeout does not help and one already exists (8 h per-iteration, unreached at ~5.5 h): a timeout cannot tell a failing run from a legitimately long one.

## Decision

One cycle: fix Bug 1, and harden the shared drain machinery so a systemic failure fails fast on BOTH drains.

**1 — gk_decision tracking wiring.** `score_gk_decision_reconstructed` / `score_gk_decision_unit` take `frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None`. When `None`, the convention AND the actor-bridge are BOTH inferred from the same signal — the presence of an `is_actor` column: SB360 snapshot (`is_actor` present) → bridge + `per_action_ltr` (unchanged); raw tracking frames (absent) → skip the bridge + `match_ltr`. Populates `fct_gk_decision` for tracking providers for the first time; proven non-empty on the real idsse `build_unit_inputs` fixture.

**2 — hybrid circuit-breaker in `drain_worker` (both drains inherit it).** A per-unit failure trips the slice when `consecutive_failures >= max_consecutive_failures` (default 6) OR the failure rate exceeds `systemic_failure_rate` (0.5) after at least `systemic_min_sample` (20) attempts (`attempted = processed + failed`; timeouts excluded — the abandon-ceiling owns those; a success resets the consecutive counter). On trip it flushes terminals, emits `slice_completed(abort_reason=...)` (carried on the EXISTING `error` column — no event-schema change — so the ADR-068 gate/operator can tell a tripped slice from a clean one), logs `ac1_drain_circuit_broken`, and raises `DrainCircuitBreakerError` — **bypassing `raise_on_failed_units`; the raise IS the fast failure.** Per-worker (each of N workers trips independently; a systemic bug trips them all within ~K units). The three knobs are CLI args on both worker entry points (empty → the shared `parse_breaker_config` defaults), so a run can tune them without a redeploy.

**3 — in-preflight canary (dry-run, one-per-provider, non-skippable).** `GameProcessorPort.process` gains `dry_run: bool = False`; both implementations (`TrackingMartsProcessor.process`, `SparkGameProcessor.process` via `_process_tracking_match`) run every scorer but write nothing under `dry_run` (tracking: skip `_write`; AC: `count()` the `applyInPandas` DAG to materialize the UDF, skip the write). Both preflights, after discovery + `ensure_tables` and after the nothing-to-do early return, dry-run one unit per distinct provider (`analytics.action_context.canary.run_canary`); any raise fails preflight, so the dependent `compute_*` is skipped — catching a systemic compute defect BEFORE the fan-out and before any writes. A benign bad-data canary unit blocks only that provider; the runtime breaker is the backstop for defects a canary unit does not exercise.

**No timeout change.** The 8 h per-iteration `timeout_seconds` already exists on both drains (`main.tf`, asserted by `test_tracking_marts_terraform.py`); the breaker is the mechanism that distinguishes failing from long.

## Consequences

**Positive** — a systemic drain failure fails at the canary (before fan-out) or within ~`max_consecutive_failures` units per worker (minutes, not hours), on both drains, from shared code. `fct_gk_decision` populates for tracking. Tripped/aborted state is legible to the completeness gate.

**Negative** — preflight now builds the processor + loads its models/xT to run the canary (one model load, ≤4 dry-run units). A benign bad-data canary unit can block a provider's drain (mitigated: per-provider, error names the unit, scoped `--max-units` runs remain).

**Neutral** — `DrainSummary` + `slice_completed` gain fields (additive; the `error` column already existed). sk unchanged — the fix is lakehouse-side (sk's own reconstruct path is already `is_actor`-guarded).

## Amendment (2026-09-23, wheel 0.5.116) — the canary is a SINGLE unit; preflight timeout raised 600 -> 1200

The first post-merge tracking-marts `--full` run (`575004868517163`) failed: `preflight_tracking_marts`
timed out (600 s). The canary as first shipped dry-ran ONE unit PER PROVIDER through the full processor,
and a full-processor dry-run costs ~one real drain unit (defcon / pitch-control dominate). On a HEALTHY
run (gk_decision fixed) every provider's canary unit ran to completion, exceeding the 600 s preflight
budget — a regression that would time out preflight on every non-empty run (both drains).

**Fix:** the canary now dry-runs a SINGLE representative unit (`select_canary_units` returns `units[:1]`,
the first discovered), and both preflight tasks' `timeout_seconds` was raised 600 -> 1200. One unit
catches a GLOBAL systemic defect (the class this cycle targets — `is_actor` hit every provider) before
the fan-out; a provider-specific defect the single canary unit does not exercise falls to the runtime
circuit-breaker (the primary fast-fail). The cost/benefit that sank the per-provider fan: the canary's
cost lands on EVERY full run, while its benefit only pays off on the RARE systemic-bug run — and the
breaker already fast-fails those.
