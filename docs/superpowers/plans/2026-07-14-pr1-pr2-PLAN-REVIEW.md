# Plan review — PR-1 (TC-1 retirement) + PR-2 (orphan-mart deletions)

**Reviewer:** external session · **Date:** 2026-07-14
**Verdict: PR-2 is ready. PR-1 has one blocker (Task 8) — and it's a correction to my own earlier review — plus two refinements.**

Verified against source. PR-2's teardown matrix is the strongest artifact in either plan; PR-1's
task ordering (parity gate → harden → re-home → delete) is correct and the load-bearing sequence.

---

## BLOCKER — PR-1 Task 8: re-pointing the oracle regen at AC makes the AC-1 regression suite validate AC against itself

Task 8 Step 1: *"Re-point the regen query at `fct_action_context` … `SELECT <tc1 columns> FROM
fct_action_context`."*

I read the oracle machinery to check this, and it inverts the oracle's purpose. From
`src/tests/action_context/oracle_map.py:1-12`:

> *"Verified **AC-1-column → legacy-oracle** map … `tracking_context` (`fct_tracking_context`):
> same-name feature columns … This pipeline batched per 250 frames; AC-1 batched identically until
> ADR-047 … so geometric/at-action features still match tightly but WINDOW-DEPENDENT features
> legitimately diverge."*

`oracle_fct_tracking_context.parquet` is a **legacy, cross-pipeline** oracle: AC-1's output is checked
**against the independent TC-1 pipeline's values**. That is the entire point — it catches an AC-1
regression by comparing to a *different* implementation. The whole "geometric features match tightly,
window-dependent features legitimately diverge" structure only makes sense for a cross-pipeline check;
a self-golden oracle would have nothing that "legitimately diverges."

Re-point the regen at `fct_action_context` (AC-1's own output) and the suite becomes **AC-1 validated
against AC-1**. It goes permanently green and catches nothing: a future AC-1 regression in a geometric
feature would be captured into the "oracle" on the next regen and blessed as correct. This is the same
vacuity class as Task 5's circular assert and the review-2 mart-grain test — silent, because the suite
stays green while testing nothing.

**This is a correction to my own review.** Review-1/2 finding m2 offered two options — *"point the
regen at `fct_action_context` **or** document the oracle as frozen-forever and remove the dead regen
branch."* Option (a) was wrong, and I could only see why after reading `oracle_map.py`. Retract it.

**Correct fix for Task 8:**
- **Keep the parquet frozen** — exactly what the spec §3 "Keep" says ("a frozen parquet snapshot, not
  a live query"). Losing the ability to regenerate it is *correct*: you must never regenerate a
  cross-pipeline oracle from the pipeline it independently checks.
- **Remove the dead regen branch** in `extract_action_context_fixture.py`, or make it `raise` with a
  clear message ("legacy TC-1 oracle is frozen; source pipeline retired — do not regenerate"). Do
  **not** re-source it.
- **Update the `oracle_map.py` docstring** to say the `tracking_context` oracle is permanently frozen
  and why (its source pipeline is gone; it remains a valid historical baseline for the columns that
  matched tightly).
- The window-dependent-divergence caveat already in the docstring stays true and important.

If there is a genuine future need for a *self*-regression oracle on AC-1's geometric features, that is
a separate, deliberate artifact (regenerated at release and frozen between releases, to detect drift)
— not this legacy file, and not a silent re-source of it.

---

## MEDIUM — PR-1 Task 3 Step 2: the trap guard is never shown to be able to fail

Task 3 adds `assert_tracking_gk_provider_scope` (no `data_source` outside idsse/metrica/skillcorner)
to pin THE TRAP. But Step 2 is self-contradictory:

> *"Run it — **expect FAIL** (pre-change it reads TC-1 which is already scoped, so **this actually
> passes pre-change** … its purpose is to *stay green* across the swap)."*

The guard is green before the swap (TC-1 is scoped) and green after (the filter is added in the same
step). It is never observed red — so it is never demonstrated to catch the thing it exists for. This
is the kill-line gap: the guard is asserted-effective, not shown-effective.

The trap is real only in the window **after the ref-swap and before the filter**. Split Task 3 to walk
through it:

1. Swap the ref to `stg_action_context__values` **without** the `data_source` filter.
2. Build `int_tracking_goalkeepers` + run `assert_tracking_gk_provider_scope` → it **MUST fail**
   (gradientsports / statsbomb-360 admitted). *This is the demonstration.* If it does **not** fail,
   STOP — either AC doesn't populate the GK column for those providers (the trap is imaginary) or the
   guard is miswired.
3. Add the `and data_source in ('idsse','metrica','skillcorner')` filter.
4. Re-run → green, *because of* the filter.

Same rule the sibling silly-kicks plans now enforce: name the line whose deletion makes the test fail
(here, the filter), then watch it fail. The current Task 3 bundles swap+filter so the red never shows.

---

## MINOR

- **PR-1 Task 5 Step 2(b) — the re-base needs to be a containment check, not equality.** The
  recommended fix for the circular `assert_idsse_minutes_roster_vs_tracking_context` is to re-base it
  against *"`bronze.idsse_tracking` distinct players per match."* But the minutes roster is *players
  who generated a SPADL action*, while tracking bronze is *players who appear in frames* — a strict
  **superset** (a tracked sub who logged no action is in tracking, not in the action roster). An
  equality assertion would false-positive. Specify it as **minutes-roster ⊆ tracking-players** (every
  player you count minutes for must appear in tracking) — which is also the more meaningful guard.
  The recommendation is good; pin its direction or it fails on first run.
- **PR-1 Task 7 — verify the −2 before decrementing the anchor.** `test_workflows_tf_ordering.py:281`
  asserts `len(task_keys) == 42`. The plan decrements to 40, assuming **both**
  `compute_tracking_context` and `preflight_tracking_context` are among the 42 data_ingestion depth-2
  blocks. Confirm both are counted (grep the parsed set) before writing 40 — if `preflight` is on a
  different job/depth, the real answer is 41 and the test fails. Self-correcting (the test catches a
  wrong anchor), but a confirm-first step saves a cycle and the history comment should state which two
  tasks the −2 removes.

---

## PR-2 — ready

The teardown matrix (§ "The teardown matrix") is exactly right and is the part a naive deletion gets
wrong. Verified against source:

- **The `rederive_planner.py` registry trap is real and fully captured:** `fct_space_creation` in
  `_TABLE_MARTS` (`:43`), `fct_off_ball_xt` in `D_REPROCESS_MODELS` (`:33`), and `fct_space_creation`
  hard-coded in `test_rederive_planner.py:31`. Deleting the marts from `SYNCED_TABLES` without these
  trips `test_strand_safe_rederive.py::test_dtb_exhaustively_partition_the_triggered_set` — the matrix
  says so and routes each edit.
- **The TRIGGERED/SNAPSHOT split is correct:** `fct_gk_actions_detail` is SNAPSHOT (not in
  `triggered_synced_marts`, no `dbt_project.yml` edit) — matches source; the other three are TRIGGERED
  and must leave both lists.
- **The mart-vs-task distinction holds** and Task 0 Step 3 re-greps `stg_line_breaking__results` /
  `stg_off_ball_xt__results` to prove the staging views survive.
- Zero-consumer re-confirmation at execution time, and the line-drift/grep-the-symbol discipline, are
  both present.

One nicety worth adding: after removing `fct_off_ball_xt` from `D_REPROCESS_MODELS`, confirm no test
asserts that frozenset's *membership* directly (the exhaustive-partition test checks the union, which
stays valid; a per-class membership test, if one exists, would not). The final full `pytest` run
catches it either way.

---

## Summary

| | Item | Action |
|---|---|---|
| **Blocker** | PR-1 Task 8 re-points a cross-pipeline oracle at the pipeline it checks → vacuous | Keep frozen; remove/raise the regen branch; do **not** re-source (retracts my review-2 m2 option a) |
| Medium | PR-1 Task 3 trap guard never shown red | Split swap-then-filter; demonstrate the guard failing on the unfiltered swap |
| Minor | PR-1 Task 5(b) re-base direction | Make it ⊆ (roster in tracking), not equality |
| Minor | PR-1 Task 7 anchor | Confirm both tasks are in the 42 before writing 40 |
| — | PR-2 | Ready as written |

Fix Task 8 (it's the one that would ship a green-but-blind regression suite), demonstrate the Task 3
guard, pin the two minors, and PR-1 is executable. PR-2 can go as written.
