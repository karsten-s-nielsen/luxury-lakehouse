# Critical review — xT-grid staleness fix (plan + ADR-063)

**Date:** 2026-06-28 · **Reviewer:** xT-GK analysis side · **Re:**
`plans/2026-06-28-xt-grid-staleness-and-guard-watermark-fix.md`, `adrs/ADR-063-….md`
**Verdict:** Mechanism is sound — green-light Tier A — but **block the one-time rebuild on H1 + H3**, and add H4.
M-items in this PR; L-items cleanup.

## What's right (keep)
Correct root cause (staleness, not orientation). Reusing the existing conformance-tested
`check_upstream_freshness`/`record_watermarks` instead of inventing a primitive. Cost-tiering the rollout (not
"rebuild everything"). TDD with a golden test built from the real stale-grid U-signature. Directionality assert as a
loud build failure. Honest alternatives table. One-time wipe leveraging `previous is None` to skip
`validate_differential` without a lingering bypass flag.

---

## HIGH — architectural / long-term

### H1. Transitive freshness gap — the fix stops one hop short (BLOCKER)
The plan watermarks `fct_action_values → grid` but not `grid → consumers`. The grid was static, so nothing
downstream watermarks on it. The moment Tier A makes it rebuild ~daily, every consumer — `fct_action_context`
(xt_gk), off-ball xT, `fct_tracking_context` — inherits the **same build-if-absent class** w.r.t. the grid: their
guards key on *their* inputs, and the grid is not a declared input. Result: a fresh grid against which
`action_context` is **not** re-materialized → staleness **relocated one hop**, not eliminated.
**Action:** verify whether the AC / off-ball pipelines declare `bronze.expected_threat_grids` as a watermark input.
If not, add it (the grid is now a dynamic upstream). Until that edge exists, ADR-063's "eliminate the class" claim
is not met.

### H2. "Cheap to rebuild" is measured on the wrong cost
Grid compute is minutes; its *consumers* are expensive (`fct_action_context` has its own re-materialize runbook). A
baseline that churns on every daily `fct_action_values` append forces either daily AC re-materialization (wasteful —
the grid barely moves once seeded) or AC drift (H1). Also, Delta-version watermarks fire on OPTIMIZE/VACUUM/no-op
commits, not just data changes.
**Action:** decouple the trigger from "every Delta version" — a content fingerprint (row-count + max event ts, or a
data hash) or a materiality threshold. Consider whether the grid belongs in Tier B (contract/material-change) rather
than Tier A given it gates expensive downstream work.

### H3. `validate_differential(0.30)` × watermark auto-rebuild can deadlock and silently re-stale (BLOCKER)
The one-time wipe sidesteps the differential via `previous is None`, but every *subsequent* daily rebuild has a
`previous`, so the 0.30 guard runs. A legitimate future >30% shift (next migration, data correction) is **hard-
rejected** → grid not written. Then: (a) if `record_watermarks` runs anyway → "fresh" recorded on an un-rebuilt grid
→ permanent silent staleness (the exact failure this ADR targets); or (b) if it doesn't → daily hard-fail until a
human wipes.
**Action:** (i) `record_watermarks` must run only after a successfully-written-AND-validated grid — assert this in a
test; (ii) demote `validate_differential` to WARN + alert (the directionality assert already catches the real
failure); (iii) let an intended re-derivation override it.

### H4. Add a cross-cutting staleness MONITOR, independent of the rebuild trigger
A cheap scheduled check — *for every derived table, alert if its watermark is older than max(upstream watermarks)* —
would have caught this in week 1, and covers **all three tiers**, including B (deferred) and C (which currently
relies on humans remembering a wipe checklist — the same human-memory failure that produced the 2-month stale grid).
Detect-and-alert is orthogonal to auto-rebuild and small. Recommend folding it into this ADR as the interim backstop,
not a future maybe.

---

## MEDIUM — correctness / robustness (fix in this PR)

### M5. Directionality check is statistically fragile + ADR/plan disagree on scope
- It uses single extreme columns `row_means[0]`/`row_means[-1]` — the sparsest, noisiest cells. Use a **thirds-mean
  ratio** (attacking-third ÷ defensive-third) or a **rank correlation** between `zone_x` and `row_mean`; far more
  robust and won't false-fail small competitions.
- It **drops monotonicity entirely** for a 2-point ratio — a grid can pass `ratio ≥ 5` yet dip/spike mid-pitch. Keep
  a coarse shape check alongside.
- **ADR says** per-comp grids above a min-action threshold are asserted; **plan only** passes
  `require_directional=True` for `global`. So per-comp grids (the ones that were *inverted*) get no guard going
  forward. Implement the per-comp gating or drop the ADR claim.

### M6. Per-competition grids — consumed at all? (Chesterton + YAGNI)
Every named consumer reads `global`. The per-comp grids are extra surface that was silently broken (inverted) and
caused confusion. If nothing consumes them, delete them (simplification + removes an unguarded staleness surface). If
something does, name it — and it needs the same guard.

### M7. `fct_tracking_context` affected but absent from the runbook
Listed as a consumer (ADR Consequences) but not re-materialized in Task 6. Confirm it's truly dead/deprecated, else
it's left stale after the rebuild.

### M8. Rollback is under-specified
"Snapshot row counts for rollback awareness" (Task 6 Step 1) is not a backup — after `DELETE` the old grid *values*
are gone. Rollback depends on Delta time-travel/`RESTORE`, which depends on VACUUM retention. State the mechanism
explicitly and confirm the window, or snapshot the (~96-row) grid to a side table before the wipe. Don't delete the
only copy of a production input before validating the replacement.

---

## LOW — hygiene / testing

- **L9.** `resolve_upstream_tables_from_card(self.workflow_id if False else "wf-xt-grids", …)` (Task 2 Step 4) — the
  `if False` is dead/placeholder code; use a module constant.
- **L10.** The guard tests mock the Spark chain (`_FakeSpark.…collect() → []`) — that tests wiring, not behavior;
  it passes even if the real query is wrong. Extract the guard's decision into a pure function tested without Spark,
  or use a local Delta fixture. Mock-heavy tests on the exact code whose silent failure caused this give false
  confidence.
- **L11.** Fail-loud needs a listener — confirm workflow-failure alerting actually pages on the directionality
  `raise`; otherwise a daily hard-fail is its own silent failure.
- **L12.** ADR title over-promises ("eliminate the class") — only Tier A ships. Honest framing: *fixes xT-grid
  staleness and establishes the tiered pattern*; the class isn't eliminated until B/C land — which is the argument
  for H4 as the interim backstop.

---

**Bottom line:** Tier A's mechanism is good and I'd ship it — but resolve **H1** and **H3** before the one-time
rebuild (both can manufacture a *new* silent-staleness bug the day after this lands), and add **H4** so the whole
class becomes observable rather than trusted. M5–M8 in this PR; L-items are cleanup.
