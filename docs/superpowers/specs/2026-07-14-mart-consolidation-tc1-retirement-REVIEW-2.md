# Critical review 2 — rev 2 of `2026-07-14-mart-consolidation-tc1-retirement-design.md`

**Reviewer:** external session · **Date:** 2026-07-14 · **Verdict: acceptable. Two corrections, one of which affects a regression guard.**

All four prior findings are fixed, and fixed at the code, not the prose:

| | fix | verified |
|---|---|---|
| **B1** view can't back the synced table | **PR-3 dropped.** §3 Phase 3 rejection captures all five points — the CDF/TRIGGERED incompatibility, the `triggered_synced_marts` + `refresh_synced_tables` coupling, the 5-join reality, the `--full-refresh`-masking history, and "already on AC → buys nothing". Clean. | ✅ |
| **M1** AC shares the dedup disease | **Measured:** AC bronze 937,324 rows = 937,324 distinct keys = 0 dups, explained by M13 work-unit ownership; G3 now says "measured, not asserted"; tiebreaker + invariant test added as defense-in-depth. | ✅ The right answer — proved AC is canonical rather than assuming it |
| **M2** parity gate confounds dedup + orientation | **Widened correctly.** idsse now expected identical *for the right reason* (oriented in both + same derivation → a difference is a genuine derivation change); metrica/SkillCorner explicitly admits both mechanisms, accepted iff AC is verified correct vs roster ground truth. | ✅ |
| **m1 / m2** omitted test + dead oracle regen | Both folded into §3 Phase 1 (`test_staging_coverage.py:87-88` removal; regen re-pointed at `fct_action_context`). | ✅ |

Dropping PR-3 and G4 was done without leaving dangling references — §2 strikes G4, §8 is now two PRs, the closing line confirms `fct_gk_tracking_actions` stays materialized.

---

## Two corrections

### R1 · The zero-dup invariant test must be at the **bronze** layer — one of the two options offered is vacuous

§3 Phase 1 (lines 140-142):

> "Add a zero-dup invariant test — a dbt singular test **(or `unique_combination_of_columns` on
> `fct_action_context`'s `(match_key, action_id)`)** … asserting AC bronze has no duplicate keys. …
> if the M13 ownership ever breaks, the gate catches it."

The parenthetical option cannot do the stated job. `fct_action_context` is built on
`stg_action_context__values`, which dedups to `row_number() = 1` — so the mart's `(match_key,
action_id)` is unique **by construction of the staging dedup**, whether or not bronze has duplicates.
If M13 ownership breaks tomorrow and bronze grows 5,000 divergent dups, staging silently collapses
them and the mart-grain test **still passes**. It is the exact wrong-layer guard this codebase keeps
being bitten by: the test validates the post-dedup output, not the pre-dedup invariant it claims to
defend.

The **tiebreaker you add in the same phase makes this strictly worse**: once the staging dedup is
deterministic, a bronze dup is collapsed *silently and reproducibly*, so the bronze layer becomes the
**only** place an M13 regression is observable. The guard therefore *must* be the bronze form — count
duplicate `(data_source, match_id, action_id)` keys in `source('action_context',
'spadl_action_context')` and assert zero. That is the singular test; keep it, drop the parenthetical.

Worth keeping the mart-grain uniqueness test too — the spec correctly notes AC's mart lacks one, and
TC-1's missing grain test is listed as a defect in §1's own table — but it defends a *different*
property (the mart's join integrity, e.g. a fan-out in the `dim_matches` join), not M13 ownership.
Add both; don't let the mart test stand in for the bronze guard. They catch different regressions.

### R2 · §5's synced-table breakdown is miscounted

> "Removes 5 gold marts and **5 Lakebase synced tables (4 TRIGGERED + 1 SNAPSHOT)**"

From `refresh_synced_tables.py`: `fct_line_breaking_results` (`:188`), `fct_off_ball_xt` (`:199`),
`fct_space_creation` (`:211`) are TRIGGERED; `fct_gk_actions_detail` (`:232`) and
`fct_tracking_context` (`:235`) carry **no policy argument → SNAPSHOT** (the default, `:82`). So it is
**3 TRIGGERED + 2 SNAPSHOT**, not 4 + 1. The total of 5 is right; the split is off by one each way.

Minor, but it is load-bearing for teardown: only the 3 TRIGGERED tables sit in
`triggered_synced_marts` (`dbt_project.yml`), and the spec's own §6 risk row leans on
`test_strand_safe_rederive.py`'s SYNCED_TABLES ↔ `triggered_synced_marts` parity. Getting the
per-table policy right is what keeps that teardown from tripping the test — so fix the count and,
in the deletion checklist, mark each synced table's policy so the two SNAPSHOT tables aren't looked
for in the triggered list.

---

## Bottom line

**Approve.** PR-1 and PR-2 are sound. Fix R1 (specify the invariant test at bronze grain — the
parenthetical mart-grain option is vacuous, and your new tiebreaker guarantees it) and R2 (correct
the 3-TRIGGERED-2-SNAPSHOT split) and the spec is ready for the implementation plan. Both are
small; neither touches the core, which — TC-1 is a redundant pipeline on worse frames, AC is the
proven-canonical superset — is now fully substantiated with measured numbers.
