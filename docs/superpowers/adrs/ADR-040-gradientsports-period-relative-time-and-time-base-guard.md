# ADR-040: GradientSports period-relative `time_seconds` + work-unit time-base guard (silly-kicks 4.12.0)

| Field | Value |
|---|---|
| **Status** | Accepted; **amended 2026-06-11 (SkillCorner dispatch re-base + two-sided guard + completeness invariant)** |
| **Date** | 2026-06-04 |
| **Deciders** | Karsten S. Nielsen, Claude Opus 4.8 (1M) |
| **Tags** | cross-table-format-contract (ADR-018), silly-kicks-adoption, AC-1, GradientSports, SkillCorner |

## Amendment (2026-06-11): the third member of the class — and why it must be the last

The original Hyrum's-Law sweep below states "IDSSE/Metrica/SkillCorner/StatsBomb are
unaffected (period-relative on both sides)". **That claim was wrong for SkillCorner**:
SC bronze tracking `timestamp` is the ABSOLUTE broadcast clock (P2 = 2700 s+). The
4.20.1 cycle re-based SC's *converter* output (`_bronze_skillcorner_to_frames`,
unit-tested by `test_skillcorner_frame_time_base`) — but the time base has TWO
consumers, and the *dispatch* layer (per-batch action-window filter + M13 ownership
in `enrich_batch`) reads the bronze column BEFORE conversion. Result, measured in the
scoped prod run `1020873732479562`: SC P2 emitted 65/536 (12.1%) and 50/573 (8.7%)
actions as "successful" units, plus 2 duplicate P1 `action_id`s from the de-aligned
ownership map. The mirror image of the GS bug, one layer up.

Why every defense missed it: no SkillCorner fixture existed at all; the only fixtures
with period ≥ 2 were GS (which is how GS's fix was proven); the time-base guard asserts
only the ACTIONS side; and nothing compared emitted rows to the unit's SPADL action
count, so 88% data loss terminated as `processed`.

**Amendment decisions (all shipped together):**

1. **SkillCorner dispatch re-base** in BOTH drivers (Spark `_process_tracking_match` +
   local `run_work_unit`), subtracting `_SKILLCORNER_PERIOD_START_SECONDS` — one
   imported constant, no second copy. Lockstep sentinel:
   `test_skillcorner_dispatch_time_base`. **Exactly ONE layer owns the re-base**: with
   the dispatch re-base in place, the converter's own 4.20.1 subtraction
   (`_bronze_skillcorner_to_frames`) double-subtracted — converted frames landed at
   ≈ −2700 s and the linker found nothing for P2 while rows still emitted (caught by
   the new e2e's linking assertion). The converter is now pass-through on the time
   base; the e2e resolution tests assert `frame_id` actually links, not just that
   rows exist.
2. **Two-sided time-base guard**: new `assert_frames_time_base` (same module, same
   1800 s min-based floor — sparse-coverage-safe) runs in both drivers AFTER all
   provider re-bases. The next provider with an absolute frames clock fails loud at
   dispatch instead of silently filtering.
3. **Per-unit action-completeness invariant** (`analytics.action_context.completeness`):
   emitted rows vs the actions the frames COVER (per-period frame window, ±buffer) —
   below 95% the unit RAISES. This is the deepest net: any future variant of "a filter
   quietly dropped rows" becomes a failed unit, not silent loss.
4. **Fixture parity rule** (`test_fixture_period_coverage`): every tracking provider
   must have a committed period ≥ 2 fixture (time-base bugs are invisible in period 1).
   SkillCorner `1886347_p2` and Metrica `Sample_Game_1_p2` fixtures added, with gated
   e2e resolution tests.
5. **M13 uniqueness net**: the local driver raises on duplicate `action_id`s pre-write;
   the dbt singular test `assert_action_context_action_unique` covers the distributed
   path where per-batch UDFs cannot see each other's output.
6. **M13 GLOBAL ownership anchors** (follow-up, same day): the net in (5) immediately
   caught a SECOND, distinct M13 bug — 2 duplicate P1 `action_id`s (346/365 on
   1899585) that persisted after the time-base fix. Root cause: ownership fitted the
   frame↔time line PER BATCH from each batch's own rows; on gappy tracking
   (SkillCorner broadcast: ~30% of frames missing) adjacent batches fit slightly
   different lines, and an action whose estimated frame lands within ~1 frame of a
   batch boundary is claimed by BOTH (or potentially neither). Fix: the dispatcher
   computes ONE per-period `(t0, f0, slope)` anchor over the whole unit
   (`pipeline.compute_ownership_anchors`; Spark side via a `min_by`/`max_by` extension
   of the existing per-period agg) and passes it into every batch — single ownership
   holds by construction, gap-invariant. Bug reproduced + fix proven in
   `test_m13_global_anchor`; lockstep sentinel covers both dispatchers.

All historical SkillCorner action-context data (any table or export computed before
this amendment) under-covers period ≥ 2 and must be recomputed.

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

## Amendment (2026-06-09): two more provider frame-orientation/time-base contracts (silly-kicks 4.20.1, wheel 0.5.25)

The first post-clean-slate AC-1 test run (`max_units=4`, all providers) surfaced two further provider-frame contracts in the same family as the original GS-actions class. Both were root-caused with reproductions and are fixed in this release.

### A. GradientSports frame ORIENTATION — `home_team_id` dtype contract

**Symptom:** `structural_sgm` (silly-kicks 4.19.2 TF-45 structural pass) blew up to **−88,955,384** on GS data (~3.2% of GS pass/cross rows `|sgm|>1e4`; IDSSE only 0.2%). `structural_sgm = 1/rho_r − 1/rho_p` over a Gaussian defender density; a value ~1e8 means a point was ~90 m from every defender.

**Root cause (reproduced to 0.002%):** the lakehouse passed `home_team_id=int(meta.home_team_id)` to silly-kicks `gradientsports.convert_to_frames(output_convention="ltr")`, while `converter_input.team_id` is a native **string** (`gs_team_side_to_id` maps to native-string ids). The dtype mismatch makes `play_left_to_right`'s `is_home` match **zero** players → every player is labelled `'ltr'` → the per-period LTR flip never fires → GS frames stay mis-oriented in switched-end periods (P2/P4: the *defending* home team sits at high x instead of being flipped to low x). `structural_pass` is the only metric that mirrors defenders into the action's attack-positive frame (`105−x` iff acting team is away); on the mis-oriented frame that mirror sends defenders ~90 m off → the SGM blow-up. (DAS/OBSO/etc. on the same frame are mis-oriented too, but silently — SGM merely *exploded* and made it visible.) Decisive test: `home_team_id=int(366)` → sgm −88,953,820 (matches prod); `home_team_id=str("366")` → sgm −0.1 (sane).

**Decision:** pass `home_team_id` to GS `convert_to_frames` in the SAME dtype as the frame `team_id` — the native **string** (`meta.home_team_id`, declared `str`), identical to the IDSSE/Sportec branch, which was always correct. silly-kicks' GS `convert_to_frames` annotates `home_team_id: int` (inconsistent with its own sportec converter) — that annotation is wrong; a justified `# type: ignore[arg-type]` documents it. Regression test `test_gradientsports_frame_orientation.py` asserts home attacks +x in every period (fails on the `int()` cast).

**Consequence:** **all GS tracking metrics** (not just SGM) were mis-oriented for switched-end periods (P2/P4) — re-validate + recompute GS AC after deploy. IDSSE/Metrica/SkillCorner were unaffected (they pass a string `home_team_id`). silly-kicks 4.20.1 also adds an SGM eps-floor (`_RHO_FLOOR`) as defense-in-depth (bounds `1/rho` regardless).

### B. SkillCorner frame TIME-BASE — period-relative re-base (the mirror image of the original class)

**Context:** silly-kicks 4.20.1 fixed a SkillCorner converter bug — its SPADL `time_seconds` was the continuous broadcast clock (2nd half = 45:00+), now re-based to PERIOD-RELATIVE (`skillcorner.py _PERIOD_START_SECONDS`). Our `bronze.skillcorner_tracking` frames carry the **absolute** broadcast clock (parsed from `HH:MM:SS`). With actions now period-relative and frames still absolute, the action↔frame linker would collapse in the 2nd half+ — the **mirror image** of the original GS class (there: actions absolute, frames period-relative; here: actions period-relative, frames absolute).

**Decision:** `_bronze_skillcorner_to_frames` (BOTH the AC hexagon copy in `convert.py` and the legacy `tracking_context.py` copy, kept identical by `test_convert_drift.py`) subtracts the nominal period-start offset (`_SKILLCORNER_PERIOD_START_SECONDS`, mirroring silly-kicks exactly: `{1:0, 2:2700, 3:5400, 4:6300, 5:7200}`) from frame `time_seconds`. Velocity is unaffected (derived per-period from `dt = 1/frame_rate`, never from `time_seconds`). Regression test `test_skillcorner_frame_time_base.py` asserts P2 frames reset to ~0.

**Consequence:** SkillCorner AC is now **unblocked** from the original ADR-040 work-unit guard (its actions are period-relative on 4.20.1, so the guard passes) once frames are re-based. SkillCorner SPADL also changed on 4.20.1 (period-relative time + goalkick result via `same_team_next`) → SkillCorner needs SPADL re-convert + VAEP re-score, alongside IDSSE (sportec `play_evaluation`-driven pass/set-piece results + cross-label fix). These drive a SPADL re-convert + **VAEP champion v8 retrain** (the corrected providers are intentionally in the training corpus) + full re-score, post-deploy.

**Goldens:** the IDSSE J03WMX p1 full + mini goldens are **value- and byte-identical** under 4.20.1 (regenerated + diff-reviewed): healthy full-tracking IDSSE never triggers the SGM eps-floor, and the SPADL-layer changes don't touch the AC enrichment that runs on frozen actions.

**Related:** the SGM blow-up root-cause + reproductions are in project memory `project_gs_home_team_id_orientation_bug`; silly-kicks 4.20.1 release (SkillCorner time/goalkick, sportec/IDSSE play_evaluation results + cross fix, SGM eps-floor).

## Amendment (2026-06-09b): Metrica frame-derived period-relative re-base (the THIRD provider in this class)

**Context:** This ADR's original Context claimed "IDSSE/Metrica/SkillCorner/StatsBomb are unaffected (period-relative on both sides)." That was **wrong for Metrica.** `bronze.metrica_events.start_time_s`/`end_time_s` and `bronze.metrica_tracking.timestamp` are both on the **ABSOLUTE** match clock (P2 ≈ 2885 s); neither silly-kicks' Metrica converter nor the lakehouse adapter (`adapt_metrica_events_for_silly_kicks`) ever re-bases them. So the work-unit time-base guard correctly aborted **every** Metrica unit — the same class as GS, just never previously exercised end-to-end (the local goldens are IDSSE-only). The open-data Metrica sample is 3 hand-curated games and is internally inconsistent: **Sample_Game_3's** tracking `timestamp` *resets to 0* in P2 while its frame **numbers** stay continuous — so a timestamp-subtraction re-base cannot reconcile it; only the frame number can.

**Decision:** Re-base Metrica time off the **continuous frame number**, keyed on each period's **first tracking frame** (`min(frame)` per `(match, period)` from `bronze.metrica_tracking`), in three drivers that MUST stay in lockstep: SPADL actions in `spadl_conversion._convert_metrica_from_bronze` (`start_time_s = (start_frame − period_min_frame)/fps`, `end_time_s` coalescing `end_frame`→`start_frame`), and the AC frame `timestamp` in BOTH `action_context._process_tracking_match` (Spark `Window`) and `pipeline.run_work_unit` (pandas). All three use the **identical** `min(frame)` reference, so actions and frames align exactly (an action at frame F maps to the frame whose timestamp == the action time). Frame-number based, so Sample_Game_3's timestamp reset is irrelevant. The re-base lives in the timestamp-prep step (NOT in `_bronze_metrica_to_frames`), so the `test_convert_drift.py` AST guard is unaffected. Sentinel `test_metrica_period_relative_time.py` locks the invariant + the three-driver lockstep.

**Consequence:** Metrica AC is unblocked from the work-unit guard. Because SPADL `time_seconds` changes, the Metrica `action_value_id` changes → Metrica needs **SPADL re-convert + VAEP re-score + `fct_action_values --provider metrica` re-derive** (3 games — trivial). This also fixes the Metrica `minute` derivation (previously double-counted on the absolute clock, the same class GS hit pre-ADR-040). The `.collect()` null-min check in the SPADL driver adds one small pass — fine at 3 games; revisit if Metrica scales.

**Related:** RC1 of the 2026-06-09 three-root-cause AC investigation (RC2/RC3 = ADR-044 executor env-drift guard).
