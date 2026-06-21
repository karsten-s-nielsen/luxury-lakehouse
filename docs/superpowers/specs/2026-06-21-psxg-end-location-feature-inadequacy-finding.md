# PSxG feature fix — use the projected goal-line crossing, not raw `end_location`

- **Date:** 2026-06-21 · **Status:** ROOT-CAUSED + FIX CONFIRMED (not built). Deploy paused at Phase 0 pending the feature-fix code cycle.
- **Relates to:** [`2026-06-20-psxg-tracking-extension-design.md`](2026-06-20-psxg-tracking-extension-design.md) (spec) + [`../plans/2026-06-21-psxg-tracking-extension.md`](../plans/2026-06-21-psxg-tracking-extension.md) (plan, task 0.4) + ADR-059 (mart architecture) + **[ADR-060](../adrs/ADR-060-psxg-projected-goalmouth-four-feature-model.md) (the formal decision for this fix)**
- **Decision (user, 2026-06-21):** investigate the StatsBomb options first. Result: the goalmouth-target IS derivable in-pipeline (trajectory projection) — **no freeze-frames required**.

## TL;DR

After correcting the training population (D-0: true on-target `Goal/Saved/Post/Saved to Post`, 32,698 shots @ 29.9%) and adding **GroupKFold-by-match** OOS CV (plan 0.4), the 2-feature model on **raw `end_location_y/z`** is **near-random (OOS AUC 0.525, Brier 0.209 ≈ base-rate variance)** — not a bug, an inadequate feature. The previously-live model's apparent skill was an off-target-contamination artifact (no one had measured true on-target discrimination — the old trainer used a random split on contaminated data).

**Root cause + fix (confirmed):** `end_location` is the goal-line crossing **only for goals** (`end_location_x`=120). For saves it is the **save point** (`end_location_x`≈118) — not where the shot was heading. **Projecting the shot trajectory `location`→`end_location` onto the goal plane (x=120) and using distance-from-centre `|y_proj−40|` (the signal is a symmetric arch: near-post best, dead-centre saveable) lifts OOS AUC 0.525 → 0.672** (GroupKFold by match, n=32,698; `tmp/psxg_proj_test.py`). Goal rate by projected zone: central(0–1)=15.7%, mid(1–3)=28.7%, **near-post(3–4)=51.4%**, wide(>4)=11.6%. The fix uses only existing columns (`location_*`, `end_location_*`).

## Evidence

**1. The model learned nothing.** Final logistic coefficients on standardized features: `[0.015, −0.093]` (a 1-SD change in height moves the odds ratio ~1.1). OOS AUC 0.522 over 32,698 shots / 3,462 match groups.

**2. `end_location` is the ball's final/deflected position, not the goalmouth target.** Per-outcome `end_location_y` range (StatsBomb goal frame is y∈[36,44]):

| outcome | n | avg_z | avg y-offset | min_y | max_y |
|---|---|---|---|---|---|
| Goal | 9,788 | 0.939 | 2.47 | 35.8 | 44.4 |
| Saved | 20,780 | 0.932 | 2.56 | **21.0** | **59.0** |
| Post | 1,842 | 1.808 | 3.22 | 35.4 | 44.8 |
| Saved to Post | 288 | 1.393 | 2.71 | 28.1 | 50.0 |

Goals end **inside** the goal frame; **saved shots end at y∈[21,59]** — the parried/deflected final position, not where the shot was heading. So for the entire non-goal class the "placement" feature is the wrong coordinate.

**3. Even ignoring the deflections, placement does not discriminate.** Goal vs Save: `avg_z` 0.939 vs 0.932, `avg y-offset` 2.47 vs 2.56 — statistically identical. Coarse zone goal-rate: corner/high 29.5% vs central 30.3% (flat / slightly inverted).

**4. No correct feature exists in the pipeline.** `fct_shots` exposes only `location_*` (shot origin) and `end_location_*` (ball end), plus `distance_to_goal` / `shot_angle` (pre-shot geometry). There is **no goalmouth-crossing coordinate and no freeze-frame goalkeeper position** — i.e. no "where was the shot heading / where did the GK make the save" signal, which is precisely what a post-shot xGOT model needs.

## Root cause

A post-shot xG (xGOT) model must condition on **where the ball was going relative to the goal** (and ideally GK position, shot power). `end_location` answers "where did the ball end up," which equals the goal-mouth target **only for goals**. For saves/posts it is the post-contact location. The 2-feature `(end_location_y, end_location_z)` model therefore has a corrupted feature for ~67% of the population (the non-goals) and is near-random.

## The fix (confirmed in-pipeline — no freeze-frames)

Replace the raw `end_location_y/z` features with the **projected goal-line crossing**:

1. **Project to the goal plane (x=120):** `y_proj = location_y + (end_location_y − location_y) · (120 − location_x) / (end_location_x − location_x)` when `end_location_x > location_x`, else `end_location_y` (goals already have `end_location_x`=120). The same projection gives a crossing height; for saves the save point is only ~2 units short of the line, so `end_location_z` is already close to the crossing height (project it too for cleanliness).
2. **Use distance-from-centre `|y_proj − 40|`** (normalised), not raw y — the goal-vs-save signal is a symmetric arch (both posts high, centre low), which a logistic on a linear y cannot represent.
3. Confirmed: OOS AUC **0.525 → 0.672**, Brier 0.209 → 0.200 (GroupKFold by match, n=32,698).

**Where it lands:**
- **StatsBomb (`export_shots_on_target` / `goalkeeper._normalise_goalmouth`):** emit/compute `y_proj` + distance-from-centre. The export must add `location_x/y` (origin) so the projection is computable; or compute `y_proj` in SQL and export it directly.
- **Tracking modality:** `shot_crossing_y/z` (TF-48) is already the *actual* ball crossing from tracking frames (the true goal-line crossing) — so tracking needs **only** the same distance-from-centre transform + a goal-vs-save discrimination check, not the projection.
- **Future richer model (still deferred, spec §8/E):** clip distance at the post / on-frame indicator (the wide tail at 11.6% breaks monotonicity), plus shot power/distance/angle/GK-position — to push beyond 0.67.

This is a bounded feature-engineering + retrain cycle (export feature + normalization port + retrain), **not** a freeze-frame deferral.

## Impact on the plan

- **Phase 0 (retrain) is blocked** on the feature fix — a valid Champion cannot be produced from `end_location` alone.
- **Phases 1–2 (tracking marts, StatsBomb consolidation) are blocked on Phase 0** — they all score with this model. The tracking modality has the **same** problem: `shot_crossing_y/z` (TF-48) is the tracking analogue of the goalmouth crossing and must be validated for the same goal-vs-save discrimination before use.
- The marts/writer/scorer code (ADR-059, merged #396) and the trainer hardening (#397) remain **correct and reusable** once a valid model exists.

## State after the paused run (no harm to production)

- The local retrain **registered a new `soccer_analytics.dev_gold.psxg_model` version but did NOT set `@Champion`** (crash occurred in MLflow's own console logging on `start_run.__exit__`, before the alias/publish steps). HF `psxg-model`/`psxg-predictions` and the UC Volume were **not** modified. **Live PSxG is unchanged.** The dangling unaliased version can be deleted at leisure.
- The corrected dataset (`statsbomb-shots-on-target`, 32,698 rows) **was** republished and **27 stale Spark `part-*.parquet` files were deleted** (they were contaminating training via `load_shots`' glob — an ADR-049-class stale-part-file bug). This cleanup is correct and stands.

## Follow-up hardening (independent of the model fix)

Surfaced while debugging, worth fixing regardless:

1. **`publish_shots_on_target_hf.py` leaves stale files** — republish should delete prior `data/` files (the ADR-049 `delete_patterns=["**"]` lesson) so the dataset can't accumulate mixed-schema part-files.
2. **`load_shots` globs `data/*.parquet`** — should read the canonical single file (or the publisher must guarantee a clean dir) so training input is deterministic.
3. **`train_psxg_hf.py` `recorder.fail()`** is called without its required `error` argument — fix to `recorder.fail(exc)` (it masked the real error in the except handler).
4. **Run the trainer on HF Jobs (Linux/UTF-8), not locally on Windows** — MLflow prints a `🏃` emoji that crashes Windows cp1252 stdout (`PYTHONUTF8=1` also avoids it).
