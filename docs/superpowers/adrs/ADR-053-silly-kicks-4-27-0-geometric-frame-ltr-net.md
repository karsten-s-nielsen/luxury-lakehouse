# ADR-053: silly-kicks 4.27.0 adoption — flag-free geometric frame-LTR correctness net

| Field | Value |
|---|---|
| **Date** | 2026-06-14 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

After ADR-052 (4.26.0) unified the per-action tracking geometry into the action-LTR frame, two
orientation defects remained in the action-context frame path, both rooted in the *home-team-LTR*
frame being wrong for some games/periods:

1. **metrica + skillcorner are built in ABSOLUTE orientation.** Their lakehouse bronze builders
   (`_bronze_metrica_to_frames` / `_bronze_skillcorner_to_frames`) emit `team_attacking_direction =
   None` and apply no per-period flip, so ~50% of periods land mirrored — the `pre_shot_gk_x`
   bimodality (`reference-ac-frame-orientation-per-provider`).
2. **GradientSports extra-time is per-match inconsistent between feeds.** Airtight raw-bronze proof
   (`reference-gs-et-flag-placeholder-unreliable`): the GS provider ships ET (period 3/4) *tracking*
   coordinates end-flipped relative to its (correct) *event* coordinates for some matches
   (10506/10517 flipped, 10508 consistent). Because SPADL actions and tracking frames share one
   `home_team_start_left_extratime`, no single flag can orient both feeds.

silly-kicks **4.27.0** shipped `orient_frames_to_ltr(frames, *, home_team_id, home_team_start_left,
home_team_start_left_extratime=None)` — the unlabeled-input sibling of `play_left_to_right`, intended
as the metrica/skillcorner orientation step. But it is **flag-driven** (`compute_attacking_direction`
+ `play_left_to_right`), so it is only as correct as `home_team_start_left` — and in the
action-context path the driver **defaults `home_start_left = True` for metrica/skillcorner**
(`ingestion/action_context.py:1444`,`:1464`); it is never derived there. A flag-based orient would
therefore mis-orient ~half those games, and could not address the GS per-feed ET conflict at all.

## Decision

Adopt silly-kicks **4.27.0** as the floor everywhere (pyproject `>=4.27.0,<5`, `uv.lock`, terraform
serverless env `==4.27.0`, all trainer `_REQUIRED_SK_MIN = (4, 27, 0)`, wheel **0.5.40**), AND
introduce a single **flag-free geometric correctness net**, `correct_frames_to_home_ltr`, applied at
the `_convert_tracking_batch` dispatch tail for **all four** tracking providers
(idsse / metrica / skillcorner / gradientsports).

Per period, the net reads the directional truth from goalkeeper geometry: the home GK sits deepest in
its own half, so in the canonical home-attacks-right (LTR) frame the home GK must be at LOW x. Any
period whose home-GK **median** x is on the attacking half (`> 52.5`) is point-reflected
(`x→105−x`, `y→68−y`, `vx→−vx`, `vy→−vy`; `speed` is a magnitude, unchanged). Direction labels are
populated when the builder left them null (metrica/skillcorner) and left untouched when already
present (idsse/GS, where `home="ltr"` becomes geometrically true post-flip). A zero-home-match guard
refuses to guess; every flip is logged at WARNING.

`orient_frames_to_ltr` is **not** used in this path — the geometric net is more robust given the
undrived/conflicting flags, and it *is* the per-game validation the helper's contract requires.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `orient_frames_to_ltr` with per-provider derived flags | Uses the blessed 4.27.0 helper; orientation stays in silly-kicks | Needs new event-read plumbing to derive `home_team_start_left` for metrica/skillcorner; flag still noisy (metrica period-1-shot inference); STILL needs a separate per-game geometric validation; cannot resolve the GS per-feed ET conflict | More moving parts, still flag-fragile, doesn't fix GS |
| B. Separate per-feed ET flags for GS (event-derived for SPADL, tracking-derived for frames) | Keeps the flag contract | Two derivations; GS-specific; deriving the tracking flag from GK geometry IS the geometric check, just round-tripped through a flag | Indirect; doesn't generalize to metrica/skillcorner |
| C. **Flag-free geometric net (chosen)** | One mechanism fixes metrica/skillcorner + GS ET; robust to undrived/wrong flags; self-validating; no-op when already correct; logs every correction | Lakehouse-side correction layer (not silly-kicks); per-batch (not unit-level) decision; leaves `orient_frames_to_ltr` unused in this path | — |

## Consequences

### Positive

- metrica + skillcorner frames orient correctly from absolute, and the GS extra-time flip is
  corrected — verified on real bronze: GS 10506 (`flipped [3,4]` → all four periods home-low) and
  metrica `Sample_Game_1` (`flipped [2]` → both periods home-low). idsse mini-golden **unchanged**
  (net is a no-op for already-correct frames).
- Robust to the action-context driver's defaulted `home_start_left` and to per-match provider
  orientation quirks — orientation is read from geometry, not trusted from metadata.
- Builders are untouched, so the `test_convert_drift` byte-identity gate is unaffected.

### Negative

- **`fct_tracking_context` is NOT covered.** The net is wired only in the action-context dispatch
  (`pipeline.py`); the legacy `tracking_context.py` builders carry the same absolute-orientation bug.
  An equivalent orientation step must be added there **before** `fct_tracking_context` is recomputed.
- **Per-batch decision.** Production dispatch is `(match, period, frame_batch)`, so the flip is
  decided per batch, not per period/unit. Mitigated by the median-over-~2500-frames anchor + binary
  threshold (a GK's batch-median x is reliably in its own half), but it is not a unit-level decision.
- `orient_frames_to_ltr` (the 4.27.0 helper) is unused in this path — relayed upstream.
- **Value-changing**: a full metrica/skillcorner/GS action-context recompute + `fct_action_context`
  rebuild was required to propagate the fix to live marts. **Done 2026-06-14**: per-provider scoped
  recompute (GS ET-rows deleted then re-driven → ET corrected; metrica + skillcorner full → 100%
  clean) + `fct_action_context` rebuilt via `rederive_synced_marts.py` (all 4 providers, 116,275
  rows / 84 matches, 0 null `match_key`, synced online). A cross-provider AC orientation golden was
  added — `src/tests/action_context/test_frame_orientation_golden.py` (idsse / skillcorner /
  gradientsports `10517_p3` extra-time); metrica is excluded because its committed fixture's
  `home_players` omits the GK jersey (a fixture-extraction quirk — its builder path is covered
  transitively by skillcorner). This closes the idsse-only golden gap that hid the original bugs.

### Neutral

- The **events** side is untouched (SPADL actions are per-acting-team LTR and were already correct).
- Schema-stable (value-only) — no bronze migration, no dbt contract change.
- The 4.27.0 *version* bump is independent of the mechanism (the net is flag-free and would run on
  4.26.0); the bump is adopted per the serverless exact-pin policy (ADR-046).

## Related

- **ADRs:** follows `ADR-052` (4.26.0 action-LTR unification); references `ADR-029` (per-period-absolute
  converters + ET direction), `ADR-046` (serverless exact pins).
- **Project memory:** `reference-gs-et-flag-placeholder-unreliable`,
  `reference-ac-frame-orientation-per-provider`, `project-sk-426-frame-ltr-cycle`.
- **External:** silly-kicks 4.27.0 (`orient_frames_to_ltr`, PR #130).
- **Wheel:** 0.5.39 → 0.5.40.

## Notes

The net lives in `analytics.action_context.pipeline.correct_frames_to_home_ltr` (not `convert.py`) to
keep the drift-tested builders untouched and to apply uniformly at the single dispatch point.
Validation: `src/tests/action_context/test_frame_ltr_correction.py` (5 cases: metrica-absolute orient,
no-op-when-correct, GS ET correction with labels preserved, velocity negation, speed invariance,
zero-match guard) + the idsse mini-golden no-op + real-bronze empirical checks above.
