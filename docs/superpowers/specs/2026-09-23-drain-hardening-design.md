# Drain hardening — circuit-breaker + in-preflight canary — Design Spec

| Field | Value |
|---|---|
| **Date** | 2026-09-23 |
| **Author** | luxury-lakehouse session |
| **Status** | Draft (awaiting independent review) |
| **Repo HEAD at authoring** | `f6239cadda2a7ca24025be5dc06aca855588a9fb` |
| **Extends** | ADR-037 (AC worker-drain fan-out), ADR-067 (unit-write atomicity / fail-loud), ADR-068 (AC unit events + completeness gate), ADR-082 (tracking-marts drain fan-out) |

## Context

On 2026-09-23 the P2 `compute_tracking_marts` re-run (`--full`, run `622418943470489`) was **cancelled by the operator after ~5.5 h** of wall-clock burn. Post-mortem (unit-events + driver evidence):

- **654 `failed` unit-events**, all providers (SkillCorner 346, GradientSports 268, IDSSE 28, Metrica 12), 374 period-units, each retried once (`running`=693, `slice_completed`=8). Every failure message: `gk_decision: 'is_actor'`.
- `off_ball_runs` and `defensive_credit_attributions` nonetheless showed 182/182 matches written — because the write of the earlier writers committed before `gk_decision` raised, and `replace_where` is idempotent under retry, so re-attempts re-wrote identical rows and kept advancing `_ingested_at` (16:28 → 18:32). This *looked* like a benign re-processing loop; it was in fact a mass **failure** loop.

Two independent defects made this both possible and expensive.

### Bug 1 — `gk_decision` raises `KeyError: 'is_actor'` on every tracking unit

Call chain (all in `src/ingestion`):

- `tracking_marts_processor.py:296` — `process` (`def process` at `:225`) calls `score_gk_decision_unit(inputs.actions, inputs.frames, ...)`; the `except` at `:305` records `gk_decision: {exc}`.
- `gk_decision_writer.py:215` — `score_gk_decision_unit` calls `score_gk_decision_reconstructed(actions, frames, ...)`.
- `gk_decision_writer.py:172` — `score_gk_decision_reconstructed` calls, **unconditionally**, `apply_actor_identities_to_frames(frames, actions)`.
- silly-kicks `keeper_identity.py:718/731` — `apply_actor_identities_to_frames` is an **SB360-snapshot-only** helper ("Stamp each action's real `player_id` onto its single `is_actor` SB360 frame row"; docstring: "Snapshot-derived tracking frames carrying an `is_actor` column"). At `keeper_identity.py:752` it reads `out["is_actor"]` **unguarded**.

`is_actor` is a SB360-snapshot extension column (sk `tracking/_snapshot.py:184`, "TF-54b actor bridge"). The tracking-marts drain feeds **real tracking-provider frames** (SC/GS/IDSSE/Metrica) built by `analytics/action_context/unit_inputs.py` (`build_unit_inputs`) — which do **not** carry `is_actor` (grep: no matches). Result: `KeyError: 'is_actor'` on every tracking unit.

The bug was masked because `src/tests/test_gk_decision_writer.py:60-64` hand-builds SB360-style frames **with** `is_actor` — the classic "unit-tested on fixtures, fails on live inputs" boundary (the exact class ADR-067 and the P1-E validation-boundary note warn about). It was latent since `gk_decision` was folded into the tracking-marts drain (sk4118 Phase E) and never exposed, because the daily incremental drain had no new units, so `gk_decision` never ran on tracking frames until this first `--full` corpus pass.

sk's own reconstruct path is already defensive (`gk_decision/_reconstruct.py:170` — `if "is_actor" in frame.columns`); the unguarded access is reached only via the lakehouse-side unconditional bridge call.

**Bug 1 is deeper than the crash (found in execution).** Even with the bridge guarded, the reconstruction is still wired for SB360 only. `score_gk_decision_unit` / `score_gk_decision_reconstructed` default `frame_convention="per_action_ltr"` (`gk_decision_writer.py:198`), and the tracking-marts drain calls them with that default (`:296`). Under `per_action_ltr`, sk resolves each action's frame as **`frame_id == action_id`** (`_reconstruct.py:118-119`) — true only for SB360 snapshots (`snapshot_to_tracking_frames` sets `frame_id = action_id`). The drain feeds raw per-frame tracking frames (`_convert_tracking_batch`), whose `frame_id` is a tracking-frame id, so every decision drops as `no_frame` and gk_decision returns **empty** for every tracking unit — a silent-empty mart. The correct convention for raw tracking frames is `match_ltr` (`_reconstruct.py:120-122` links via `link_actions_to_frames`; the per-period orientation `match_map` is built internally by `resolve_defended_goals(frames)` at `:262`, nothing to pass). So `fct_gk_decision` has NEVER been populated for tracking providers — the drain-fold (P1-E) wired only the SB360 convention and never ran (crashed first). Both the SB360-only bridge and the SB360-only convention key off the same signal: the presence of an `is_actor` column.

### Bug 2 — the drain has no fast-fail for systemic per-unit failures

The shared `drain_worker` (`analytics/action_context/drain.py:226`, used by BOTH the AC drain via `action_context.main_drain_worker` and the tracking-marts drain via `tracking_marts_drain._run_worker:273`) has exactly one fast-abort: the **concurrent-abandoned-watchdog-thread ceiling** (`max_abandoned`, `drain.py:285`) — which fires only on **timeout** storms. The per-unit **failure** branch (`drain.py:305`) is deliberately fail-soft: record `failed`, `continue`. `raise_on_failed_units` (`action_context.py:1552`) only fails the task at **slice end**. There is no threshold that aborts a slice when a large fraction of units are *failing* — so a corpus-wide code bug drains the entire retry budget (× heavy per-unit defcon compute × N workers) before the task fails.

The fail-soft-per-unit design is correct for an isolated bad unit (ADR-067: "one bad unit must not destroy a 5.5 h drain"). It is wrong for a systemic failure where nearly every unit fails the same way.

A wall-clock timeout does **not** close this gap, and one already exists: `compute_tracking_marts_iteration` carries `timeout_seconds = 28800` (8 h), structurally identical to the AC drain (`main.tf:235`) and asserted by `test_tracking_marts_terraform.py:46`; both drains' parent `for_each` tasks are unset (this is why a live `get_run` on the parent shows `tmo_s=None` — that is the parent, not the iteration, and matches source — no drift). The 8 h per-worker bound existed and was **unreached** — the operator cancelled at ~5.5 h. A timeout cannot tell a failing run from a legitimately long one; had the drain run to 8 h it would have burned the full budget on failing work before timing out. The gap is therefore **only** the missing failure circuit-breaker, not a missing timeout.

## Decision

One cycle, one PR: fix Bug 1, then harden the shared drain machinery so a future systemic failure fails fast, on **both** drains. Three changes + tests + ADR. (No timeout change — the 8 h per-iteration bound already exists on both drains; see Context.)

### Change 1 — wire gk_decision for tracking frames (guard the bridge + auto-select the frame convention)

Both SB360-only assumptions key off the same signal — the presence of an `is_actor` column (SB360 snapshots have it; raw tracking frames do not). Fix both, in `score_gk_decision_reconstructed` (`gk_decision_writer.py:150-184`), driven by that one signal.

1. **Guard the actor-bridge** (`:172`): the bridge stamps the real `player_id` onto the anonymous SB360 actor row and reads `is_actor` unguarded; tracking frames already carry real ids, so skip it when the column is absent.
2. **Auto-select the frame convention**: default `per_action_ltr` (SB360, `frame_id == action_id`) drops every tracking decision as `no_frame`; tracking frames need `match_ltr` (sk links via `link_actions_to_frames` and builds the orientation map internally). Make the convention param default to `None` and infer from the same `is_actor` signal, so SB360 callers (and the existing tests that pass `per_action_ltr` explicitly) are unchanged and the drain (which passes no convention) gets `match_ltr` automatically.

```python
def score_gk_decision_reconstructed(
    actions,
    frames,
    *,
    keeper_ids,
    completion_model,
    params=None,
    visible_area=None,
    frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None,
) -> tuple[pd.DataFrame, Any]:
    ...
    is_snapshot = "is_actor" in frames.columns
    conv = frame_convention if frame_convention is not None else ("per_action_ltr" if is_snapshot else "match_ltr")
    frames_with_ids = apply_actor_identities_to_frames(frames, actions) if is_snapshot else frames
    gk_actions = actions[gk_distribution_mask(actions, frames_with_ids, resolve_gk="robust").to_numpy()]
    option_set = ReconstructedOptionSet(
        gk_actions,
        frames_with_ids,
        xpass=completion_model,
        params=resolved_params,
        keeper_ids=keeper_ids,
        frame_convention=conv,
        visible_area=visible_area,
    )
    return compute_gk_decision_value(option_set, extra_drops=option_set.drop_counts())
```

`score_gk_decision_unit` (`:187-198`) takes the same `frame_convention: ... | None = None` and forwards it. The tracking-marts drain call site (`tracking_marts_processor.py:296`) is unchanged (it passes no convention → inferred `match_ltr`). The SB360 fixture tests (`is_actor` present → inferred `per_action_ltr`) are unchanged.

This makes `fct_gk_decision` populate for tracking providers for the first time. The canary (Change 3) and a new **non-empty** tracking-frame fixture test (below) prove it produces rows, not just that it stops crashing.

### Change 2 — hybrid circuit-breaker in the shared `drain_worker`

In `analytics/action_context/drain.py::drain_worker`, add three keyword params (defaults chosen so an isolated bad unit never trips, a 100%-systemic bug trips in minutes, a partial-systemic bug trips after a representative sample):

| Param | Default | Meaning |
|---|---|---|
| `max_consecutive_failures` | `6` | Trip when this many units fail **in a row**. |
| `systemic_failure_rate` | `0.5` | Trip when `failed / attempted` exceeds this… |
| `systemic_min_sample` | `20` | …once at least this many units have been **attempted** (failed + succeeded; timeouts excluded). |

Semantics:

- Maintain `consecutive_failures`: `+1` in the failure branch (`drain.py:305`), reset to `0` on a `succeeded` unit. Timeouts leave it unchanged (the abandon-ceiling owns timeout storms; a timeout is not a code-defect signal).
- After recording a failure, evaluate the trip condition:
  `consecutive_failures >= max_consecutive_failures` **OR** (`attempted >= systemic_min_sample` **AND** `failed / attempted > systemic_failure_rate`).
- On trip: **flush terminals first** (same evidence-preservation contract as the abandon-ceiling raise, `drain.py:293-299`), then raise a new `DrainCircuitBreakerError` whose message carries `worker_id`, `failed/attempted`, and the last unit's error string. This raise **bypasses** `raise_on_failed_units` — it *is* the fast failure.
- `DrainSummary` gains `circuit_broken: bool` and `abort_reason: str | None`; `slice_completed` carries them so the completeness gate (ADR-068) and an operator can distinguish a tripped slice from a clean one and from an abandon-ceiling abort.

Per-worker semantics: `drain_worker` runs once per worker (`_N_*_WORKERS`), so each worker trips independently. For a systemic bug every worker trips within ~`max_consecutive_failures` units, and any worker's raise fails the `for_each` task — fast. No cross-worker coordination (no shared state) is introduced; it is unnecessary and the per-worker trip is already minutes-fast.

Both worker entry points (`action_context.main_drain_worker`, `tracking_marts_drain.main_tracking_marts_drain_worker`) expose the three as CLI args (empty → defaults), mirroring the existing `--watchdog-budget-s` plumbing, so a run can tune them without a redeploy.

### Change 3 — in-preflight canary (dry-run, one-per-provider, non-skippable)

Add `dry_run: bool = False` to the `GameProcessorPort.process` contract (`drain.py:128`) and both implementations (`TrackingMartsProcessor.process` at `tracking_marts_processor.py:225`, the AC processor's `process`): when `dry_run`, compute every writer (the code path where Bug-1-class defects live) but **skip every `_write`** — no side effects.

In BOTH preflights (`action_context.main_preflight`, `tracking_marts_drain.main_tracking_marts_preflight`), after unit discovery + `ensure_tables` and **before** filling the work-queue / the nothing-to-do early return:

1. If zero units discovered → skip the canary (nothing to smoke).
2. Construct the same processor the workers use (loads its models / xT once).
3. For **one unit per distinct provider present** in the discovered set, call `processor.process(unit, dry_run=True)`.
4. Any raise → log the offending `(provider, match_id, period)` + the error, then **raise** (fail preflight). `compute_*` is skipped by the existing task dependency.

This catches an `is_actor`-class systemic bug **before** the N-worker fan-out spins up and **before** any corpus writes — cheaper and cleaner than the runtime breaker, which remains the backstop for defects that a canary unit happens not to exercise. Placement mirrors the ADR-068 rule that preflight is the single owner of pre-fan-out work (`tracking_marts_drain` preflight already owns `ensure_tables` + queue fill; AC preflight already owns model caching + table creation).

**Edge case (stated, not silently accepted):** a canary unit whose *data* (not code) is bad would fail preflight and block that provider's drain. Mitigations: the canary is per-provider (only the affected provider is blocked, not the whole run), the error names the exact unit for triage, and scoped `--max-units` / `--provider` runs remain available. No `--skip-canary` flag is added (non-skippable by request); if operational experience shows benign canary-unit failures are common, a "provider passes if any of the first M units succeed" refinement is the documented next step (M configurable) — not in this cycle.

### Testing (TDD, non-vacuous — per the guards-that-cannot-fail rule)

- **Bug 1:** `score_gk_decision_unit` on a **tracking-shaped fixture** (raw per-frame frames — no `is_actor`, real `player_id`s, a GK-distribution action with frames around its timestamp) produces **≥1 scored row** (non-vacuous — proves the `match_ltr` link + `resolve_defended_goals` orientation actually work on tracking frames, not merely that the crash is gone). A companion assertion pins the pre-fix mechanism (the bridge on such frames raises `KeyError('is_actor')`). The existing SB360 (`is_actor` present) tests continue to pass unchanged (inferred `per_action_ltr`).
- **Breaker:** with a fake `GameProcessorPort` that fails every unit, `drain_worker` raises `DrainCircuitBreakerError` after ~`max_consecutive_failures` units (assert it raised *before* the slice end, not at it). A rate-trip test (below the consecutive threshold but over the rate after the min-sample). A **below-threshold** test (a handful of scattered failures interleaved with successes) that asserts the breaker does **not** trip — proving it is neither vacuous nor over-eager. An assertion that terminals were flushed before the raise (evidence preserved). `DrainSummary.circuit_broken` / `slice_completed` carry the flag.
- **Canary:** a preflight-level test where the injected processor raises on the canary unit → preflight raises (compute would be skipped); a healthy processor → queue filled + task values set. Non-vacuous: assert a broken processor is actually caught (not swallowed). Also assert `dry_run=True` writes nothing (no `_write` call).

### ADR + release + rollout

- **New ADR** (extends ADR-037/067/068/082): why the existing 8 h per-iteration timeout is the wrong tool for a *failing* run (a timeout cannot distinguish failing from legitimately long), the hybrid breaker (thresholds + per-worker semantics + flush-before-raise + bypass of `raise_on_failed_units`), the in-preflight canary (dry-run, one-per-provider, non-skippable), and the `gk_decision` `is_actor` root cause + guard.
- **Wheel bump** 0.5.114 → 0.5.115 via `pyproject.toml` + `scripts/bump_wheel.py` (all consumers); `bump_wheel --check` gate.
- **TODO.md** updated in-commit.
- **Rollout (operator, post-merge, wheel deployed, main CI green):** re-run the tracking-marts drain (`preflight_tracking_marts` + `compute_tracking_marts`, `tracking_marts_full=true`). Expected: canary green (gk_decision fixed), breaker idle, all 182 tracking matches produce `off_ball_runs` + `gk_decision` + `rest_defense` + `defensive_credit`. The clean re-run supersedes the cancelled run's partial output (idempotent `replace_where`); no separate cleanup of run `622418943470489`'s writes is needed. Then verify per-mart row counts + `gk_decision` non-empty per provider, and resume the remaining P2 steps.

## Alternatives considered

| Option | Why rejected |
|---|---|
| Fix `is_actor` only; no drain hardening | Leaves every future systemic bug free to burn the full retry budget again; the operator asked for the gold-standard prevention. |
| Cross-worker (global) circuit-breaker | Requires shared state (query events/queue mid-run); the per-worker trip already fails the task in minutes for a systemic bug. Complexity with no material gain. |
| Dedicated `smoke_<drain>` mega-job task | Clean separation, but +1 task and +1 cold start per drain, plus TF wiring; the in-preflight canary rides the existing preflight driver at no extra cold start and is equally non-skippable. |
| Consecutive-only or rate-only breaker | Consecutive-only misses a partial-systemic bug (successes reset the counter); rate-only is slower to catch a 100% bug (must reach the min-sample). The hybrid catches both. |
| Rely on the timeout alone | An 8 h per-iteration timeout already exists on both drains and did not help: it cannot distinguish a failing run from a legitimately long one, so it would either kill valid long drains or (as here) permit hours of failing work until it fires. The breaker is the right primary mechanism; no timeout change is made. |
| Canary that writes (idempotent) instead of dry-run | The drain re-does the unit anyway, but a writing canary mutates prod before the gate has passed; dry-run keeps the smoke side-effect-free. |

## Consequences

**Positive**
- A systemic drain failure fails at the canary (before fan-out) or within ~`max_consecutive_failures` units per worker — minutes, not hours — on both drains, from shared code.
- `gk_decision` runs on tracking frames correctly (guard + inferred `match_ltr`); `fct_gk_decision` populates for tracking providers for the first time (it was empty-by-crash before), proven non-empty by a tracking-frame fixture test.
- The tripped/aborted state is legible to the ADR-068 completeness gate and to operators (distinct error + summary flag).

**Negative / cost**
- Preflight now constructs the processor and loads its models/xT to run the canary — added preflight weight (one model load, ≤4 dry-run units). Acceptable for a non-skippable pre-fan-out gate.
- A benign bad-data canary unit can block a provider's drain (mitigations above).

**Neutral**
- `DrainSummary` + `slice_completed` gain fields (additive; the completeness gate reads them, existing readers ignore them).
- sk is unchanged — the `is_actor` guard is lakehouse-side (sk's own path is already guarded); no sk floor bump.

## Anchors (for the reviewer to verify against the tree)

- `src/ingestion/gk_decision_writer.py:150` `score_gk_decision_reconstructed`, `:172` bridge call, `:187` `score_gk_decision_unit`, `:215` reconstructed call.
- `src/ingestion/tracking_marts_processor.py:225` `def process`, `:292-306` gk_decision block, `:311` rest_defense.
- `src/analytics/action_context/drain.py:128` `GameProcessorPort.process`, `:226` `drain_worker`, `:285` abandon-ceiling, `:305` failure branch, `:293-299` flush-before-raise, `:347` `slice_completed`.
- `src/ingestion/action_context.py:1418` `def main_drain_worker` (its `drain_worker` call at `:1529`), `:1552` `raise_on_failed_units`.
- `src/ingestion/tracking_marts_drain.py:113` `discover_open_units`, `:138` preflight, `:238` `_run_worker` (its `drain_worker` call at `:273`), `:295` worker entry point.
- `src/analytics/action_context/unit_inputs.py` `build_unit_inputs` (frames carry no `is_actor`).
- `src/ingestion/gk_decision_writer.py:150-184` `score_gk_decision_reconstructed` (bridge `:172`, convention default `:198`), `:187` `score_gk_decision_unit`.
- silly-kicks (read-only, for context): `keeper_identity.py:718/731/752`, `gk_decision/_reconstruct.py:118-122` (convention → frame resolution), `:170` (guarded), `:262` (`resolve_defended_goals` builds the match_map internally for `match_ltr`), `tracking/_snapshot.py:184`.
- `terraform/modules/workflows/main.tf` — `compute_tracking_marts_iteration` already carries `timeout_seconds = 28800` (parity with AC `:235`); `test_tracking_marts_terraform.py:46` asserts it. No timeout edit in this cycle.
- `src/tests/test_gk_decision_writer.py:60-64` (SB360 fixture with `is_actor`).
