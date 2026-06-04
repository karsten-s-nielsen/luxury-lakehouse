# ADR-040: GradientSports period-relative `time_seconds` + work-unit time-base guard (silly-kicks 4.12.0)

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-06-04 |
| **Deciders** | Karsten S. Nielsen, Claude Opus 4.8 (1M) |
| **Tags** | cross-table-format-contract (ADR-018), silly-kicks-adoption, AC-1, GradientSports |

## Context

AC-1 enrichment silently dropped ~81% of **GradientSports period-2** actions in production (GS 10502 p2 = 13%, GS 10503 p2 = 19%; period 1 and every other provider = 100%). A manual source-vs-output coverage audit found it weeks after the data first landed — no error was ever raised.

**Root cause (confirmed, data + code):** two different time bases were compared. GS *frames* are timestamped **period-relative** (`period_elapsed_time`, resets to 0 each period); GS *actions* carried **absolute** match-clock time. `enrich_batch` pre-filters actions to each 250-frame batch's time-window, so the absolute-time p2 actions (≈ 2700–5835 s) never fell inside the period-relative frame window (≈ 0–3142 s) and were dropped before the linker ever saw them. The observed p2 output range `[2700, 3142]` was exactly the intersection of the two mismatched ranges.

The decisive finding (from the silly-kicks source audit, silly-kicks ADR-017): **silly-kicks' canonical `time_seconds` convention is period-relative, not absolute** — Opta (`spadl/opta.py:166`) subtracts cumulative period durations; StatsBomb (`spadl/statsbomb.py:237`) uses the period-elapsed timestamp; the linker is per-period scoped. GS *frames* already conform; GS *actions* were the lone non-conformer, injected lakehouse-side at `spadl_adapter.py` (the GS events converter passes `time_seconds` through verbatim at `spadl/gradientsports.py:416`).

Two further facts surfaced in the Hyrum's-Law sweep:
- The bug is **pre-existing and GS-only** (GS 10502 p2 was already 13% under the older whole-match path); IDSSE/Metrica/SkillCorner/StatsBomb are unaffected (period-relative on both sides).
- GS `minute` was **already wrong in the gold mart**: `stg_spadl__action_values.sql` *adds* a nominal period offset to derive match-absolute `minute`, so GS (already absolute) double-counted (p2 ≈ 103 min instead of ≈ 58). The fix corrects this everywhere, not just AC-1.

## Decision

1. **GS actions become period-relative at the adapter (Part A).** `ingestion.spadl_adapter.adapt_gradientsports_events` subtracts the nominal period-start offset: `time_seconds = startGameClock − _GS_NOMINAL_PERIOD_START_SECONDS[period]`, with `{1:0, 2:2700, 3:5400, 4:6300, 5:7200}`. GS `startGameClock` is a *nominal* absolute clock (`= nominal_offset[period] + period_elapsed`, verified exact on GS 10503: p2 game-clock `2700` = elapsed `0` + 2700), so a fixed subtraction is exact and aligns actions with GS tracking's `period_elapsed_time`. The offsets are the inverse of `stg_spadl__action_values.sql`'s `minute` derivation; the two are kept in lockstep. Period-relative (not "make frames absolute") is chosen because it matches silly-kicks' canonical convention and every other provider, leaving no latent cross-provider footgun.

2. **Adopt silly-kicks 4.12.0 + a per-work-unit time-base guard (Part C).** Floor `>=4.12.0,<5`. A shared pure helper (`analytics.action_context.time_base_guard.assert_work_unit_time_base`) runs once per work unit, **before** `enrich_batch`'s per-batch pre-filter, wired into **both** drivers — the local hexagon (`pipeline.run_work_unit`) and the Spark production driver (`ingestion.action_context._process_tracking_match`) — and kept in lockstep by a source-level sentinel (the Spark driver is not locally runnable, per `feedback_test_production_driver_entry_point`). The guard is **frame-independent**: it asserts the work unit's *actions* are period-relative — a period's earliest `time_seconds` must start near 0; an absolute-clock period `p>=2` starts at its nominal offset (≥2700 s), so `min(action time_seconds) >= 1800 s` is flagged. It deliberately does **not** use silly-kicks' `validate_time_base` (see Alternatives): that frame-overlap metric cannot distinguish a base mismatch from legitimate sparse/partial frame coverage, so it false-raised on narrow-frame work units. The action-only check needs no frames, so it never false-fires on sparse tracking.

3. **Per-batch `link_actions_to_frames` calls pass `on_low_coverage="ignore"`.** At `enrich.py` (AC-1) and `tracking_context.py`, per-batch coverage is structurally not meaningful (the pre-filter + M13 owned-action dedup intentionally drop actions per batch). Explicit `"ignore"` keeps behaviour bit-identical to pre-4.12.0 and suppresses the new warn-by-default from spamming executor logs.

## Alternatives considered

| Alternative | Verdict |
|---|---|
| Make GS *frames* absolute instead of actions period-relative | Rejected — both internally-consistent choices fix the data, but absolute makes GS the only provider whose time base differs from silly-kicks' canonical period-relative convention; the per-period linker and every future tracking helper would be a latent GS-specific trap. |
| Empirically subtract per-period `min(startGameClock)` instead of a fixed nominal offset | Rejected — GS uses a fixed nominal clock (p2 starts exactly at 2700), so the fixed offset is exact and matches the `period_elapsed_time` the frames already use; an empirical event-min would be off by the kickoff delay and misalign actions from frames. |
| Own GS time normalization in silly-kicks | Rejected (silly-kicks ADR-017 §3c) — it would pull bronze-schema knowledge across the I/O boundary the hexagon keeps caller-side. The lakehouse owns the bronze→converter mapping; silly-kicks stays a pure pass-through. |
| silly-kicks `validate_time_base` (frame-overlap) as the runtime guard | Rejected **after implementation** — its overlap metric (frame-span ÷ action-span) cannot distinguish an offset-clock mismatch from legitimate sparse/narrow frame coverage (both yield low overlap), so it false-raised on dead-ball / broadcast-gap work units and risked failing sparse-tracking periods in production. Replaced with the frame-independent action-only check; the Part-A adapter test remains the primary GS regression lock. |
| `on_low_coverage="raise"` at the per-batch link sites | Rejected — per-batch coverage varies legitimately (pre-filter + dedup); raising there would false-positive. The work-unit action-period-relative guard is the correct place. |

## Consequences

- **Positive:** GS period-2 AC-1 data is recoverable (after reprocess); GS `minute` is corrected mart-wide; the time-base-mismatch class fails loud at the work-unit boundary in both drivers instead of silently dropping a period.
- **Hyrum / behaviour:** changing GS bronze `time_seconds` from absolute to period-relative changes `fct_action_values.action_value_id` (it hashes `time_seconds`) for all GS rows → `fct_action_values` + the inherited `fct_gk_actions_detail.gk_action_id` must be **full-refreshed** (not incrementally run) for GS. Every other `time_seconds` consumer is safe (each guards time-delta/sort by `period`). silly-kicks 4.12.0's `link_actions_to_frames` warns by default; we pass `"ignore"` at our per-batch sites.
- **Reprocess (operator runtime, post-deploy):** re-ingest all GS SPADL→VAEP and full-refresh the two marts. AC-1 GS data recompute is deferred to the planned "delete + recompute all action-context" effort (by which point GS bronze is already period-relative, so the new guard passes).
- **Goldens unchanged:** 4.12.0 is purely additive (the linking algorithm is unchanged); the IDSSE goldens are healthy/period-relative so neither the guard nor `on_low_coverage` fires.

## Related

- **silly-kicks:** ADR-017 (period-relative `time_seconds` contract + per-period link-coverage guard + `validate_time_base`), shipped 4.12.0. Lakehouse-reviewed (3-round spec + plan review).
- **ADRs:** ADR-018 (cross-table format contracts), ADR-028 (AC-1 hexagon), ADR-002 §5 (hard-fail-first), ADR-029/035/039 (prior silly-kicks adoptions).
- **Tests:** `test_gradientsports_spadl.py::test_adapt_time_seconds_is_period_relative` (adapter convention lock — the GS coverage silly-kicks' library tests deliberately omit), `test_time_base_guard.py` (helper + source-level driver sentinel).
