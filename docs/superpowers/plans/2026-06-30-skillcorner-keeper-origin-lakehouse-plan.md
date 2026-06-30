# SkillCorner keeper-origin + access_tier — Lakehouse Plan (item b + L4 scoping)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Adopt the silly-kicks SkillCorner keeper-origin fix and harden the per-match privacy default, so SkillCorner
`xt_gk` origins are correct and private SkillCorner can never inherit a public default — keeping the lakehouse thin.

**Architecture:** silly-kicks owns the geometry/resolver fix (S1–S4, separate CR); this plan is the lakehouse's thin
adoption (L1/L2) + the privacy hardening (H1) + the SkillCorner recompute + the deferred backfill-module widening.
`fct_tracking_frames` re-point (L4) is a **decoupled** third item, scoped here, **not** on the xt_gk critical path.

**Tech stack:** Python 3.10, pandas, PySpark/Delta, dbt, pytest, HuggingFace Hub. Spec:
`docs/superpowers/specs/2026-06-30-skillcorner-keeper-origin-rebuild-and-access-tier-completion.md`. silly-kicks CR:
`docs/superpowers/change-requests/2026-06-30-silly-kicks-skillcorner-keeper-origin-cr.md`. Builds on ADR-064.

**Dependency ordering (critical):** **Phase 0 has NO silly-kicks dependency** — it can land now. **Phase 1 requires
the silly-kicks release — now SATISFIED: silly-kicks 4.37.0 released (PR-S104).** **Phase 2 (L4) is decoupled** —
independently scoped, never gates Phase 1.

## Adoption deltas — silly-kicks 4.37.0 (read `docs/investigations/2026-06-30-silly-kicks-437-adoption-context.md`)
Real-bronze validation narrowed the fix vs the original spec. Adopt against **this**, not the spec's assumptions:
- **Distrust is GOAL-KICKS ONLY.** An open-play GK pass/throw's native origin **is** the keeper (0.4 m from the
  detected keeper — ball at the feet at release) → **open-play keeps native, unchanged**. Only **goal-kicks** carry a
  displaced native (broadcast ball ~14–20 m downfield) and use the ladder (detected keeper→`tracking_gk`; else
  rule-point→`goalkick_prior`). **Blast radius is small + goal-kick-concentrated.**
- **`unresolved` is RARE** (goal-kick / NaN-native edge only) — the NULL-`xt_gk` rendering still applies, to few rows.
- **`native` is the COMMON provenance** (open-play + full-tracking), not legacy.
- **S2/L1 satisfied upstream:** `convert_to_frames` preserves bronze `is_visible` as `visibility` — no extra lakehouse
  detection plumbing.
- **New observability (M2 — surface, don't emit-and-ignore):** `xt_gk_native_goalkick_out_of_region` (per-row S4
  flag), `XtGkReport.n_native_goalkick_out_of_region`, `TrackingConversionReport.n_gross_off_pitch`. CI/batch
  rate-gates are a tracked follow-up (thresholds from the recomputed corpus rate).
- **C1:** `compute_xt_gk` now raises uniformly on a >1-provider frame set (the `completion=` escape hatch is gone).
  Harmless — the AC driver dispatches per-`(match, period)` (one provider per call); Task 1.0 confirms it.

---

## Phase 0 — Privacy hardening + backfill completeness (no silly-kicks dependency — land now)

### Task 0.1: Generalize the classifier default to fail-safe-for-privacy (H1.1)

**Files:** Modify `src/shared/access_tier.py`; Test `src/tests/test_access_tier.py`; Doc
`docs/superpowers/adrs/ADR-064-per-match-access-tier.md`.

- [ ] **Step 1 — failing test.** In `test_access_tier.py`, flip the SkillCorner no-feed case, add GS symmetry, and —
  the P1 fix — add an **unknown-provider → RESTRICTED** case plus the allowlist assertion:

```python
# was: ("skillcorner", None, AccessTier.PUBLIC)  — the dangerous mixed-license default
("skillcorner", None, AccessTier.RESTRICTED),       # visibility-feed provider, no signal -> fail-safe restricted
("gradientsports", None, AccessTier.RESTRICTED),
("statsbomb", None, AccessTier.PUBLIC),             # open-data, on the allowlist
("metrica", None, AccessTier.PUBLIC),
("a_new_unclassified_provider", None, AccessTier.RESTRICTED),  # P1: unknown provider FAILS SAFE, not public
("a_new_unclassified_provider", "public", AccessTier.PUBLIC),  # explicit public signal is still honoured
```
and a test that `PUBLIC_BY_LICENSE_PROVIDERS == frozenset({"statsbomb", "wyscout", "idsse", "metrica"})`.

- [ ] **Step 2 — run, expect fail** (`skillcorner+None` and `unknown+None` currently return PUBLIC).
- [ ] **Step 3 — implement.** Make the no-signal default an **ALLOWLIST** (P1) — fail-safe-for-privacy, not a denylist:

```python
# ALLOWLIST of providers whose data is PUBLIC BY LICENSE (open data). Everything NOT on this list — any unknown/new
# provider, AND any visibility-feed provider with no per-match signal — fails SAFE to restricted. A wrong-restrict is
# fixed with one line here; a wrong-public of private data is unrecoverable. (Review P1; the live private RM data and
# the plan's own "fail-safe-for-privacy" framing both point this way.)
PUBLIC_BY_LICENSE_PROVIDERS: frozenset[str] = frozenset({"statsbomb", "wyscout", "idsse", "metrica"})
# Back-compat: still imported by hf_publish / the VAEP trainer gate as "produces restricted by default".
RESTRICTED_HF_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})

def classify_access_tier(*, provider: str, visibility: str | None) -> AccessTier:
    if visibility == "public":
        return AccessTier.PUBLIC                                  # explicit per-match public signal (any provider)
    if visibility is None and provider in PUBLIC_BY_LICENSE_PROVIDERS:
        return AccessTier.PUBLIC                                  # open-data provider, no per-match feed
    return AccessTier.RESTRICTED  # "private" | unknown visibility | unknown provider | feed-no-signal -> FAIL SAFE
```

- [ ] **Step 4 — run, expect pass.**
- [ ] **Step 5 — ADR-064 amendment.** Add a dated "Amendment (2026-06-30, review H1)" note: the
  `skillcorner+None→PUBLIC` default was a mixed-license leak shape; visibility-feed providers now default restricted;
  existing A-League is encoded explicit-public (Task 0.2), not derived from the default.

### Task 0.2: Encode existing A-League as explicit public, with a premise assertion (H1.4 + P3)

**Files:** Create `scripts/migrations/2026-07-01-skillcorner-existing-public-visibility.sql` (operator-applied);
Modify `src/ingestion/access_tier_backfill.py`; Test `src/tests/test_access_tier_backfill.py`.

- [ ] **Step 1 — premise assertion first (P3 + Note 3).** Mapping **verified 2026-06-30**: `competition_id = 61`
  ↔ `competition_name = 'A-League'` for all 360 existing rows. The migration guards on **both** (the human-meaningful
  name, not just the magic id) and aborts on any row that is not confirmed-public A-League:

```sql
-- A privacy-stamp must verify its premise; a wrong-public baked into the source of truth is unrecoverable.
-- Abort if ANY existing skillcorner_matches row is not the public A-League (id 61 AND name 'A-League').
SELECT assert_true(
  (SELECT COUNT(*) FROM soccer_analytics.bronze.skillcorner_matches
     WHERE NOT (competition_id = 61 AND competition_name = 'A-League')) = 0,
  'ABORT: non-A-League SkillCorner match present — do NOT mass-stamp visibility=public'
);
UPDATE soccer_analytics.bronze.skillcorner_matches
  SET visibility = 'public'
  WHERE visibility IS NULL AND competition_id = 61 AND competition_name = 'A-League';  -- access_tier already public
```

- [ ] **Step 2 — fix the backfill module's SkillCorner derivation.** With Task 0.1 flipping the default, the module
  can no longer derive existing-SkillCorner tier from `classify_access_tier(skillcorner, None)` (now restricted). Add
  an explicit confirmed-public override keyed on the provider, and widen `BACKFILL_TABLES` (the deferred fix) to
  enumerate **every** `access_tier` table, with a test asserting the list equals the live information_schema set.
- [ ] **Step 3 — test** the module yields `skillcorner → public` (explicit override, not the flipped default) and the
  table list is complete.

### Task 0.3: Generalize the publish guard to visibility-feed providers (H1.3)

**Files:** Modify `src/ingestion/hf_leak_guard.py`; Test `src/tests/test_hf_leak_guard.py`.

- [ ] **Step 1 — failing test:** a public frame with `data_source='skillcorner'`, `access_tier='public'`,
  `visibility` NULL/absent → `LeakDetectedError`; `visibility='public'` → passes; an **allowlisted** provider
  (statsbomb) with NULL visibility still passes; an **unknown** provider with NULL visibility in a public frame →
  `LeakDetectedError` (symmetric with the P1 classifier allowlist).
- [ ] **Step 2 — implement** in `assert_no_private_leak`, keyed on the **same allowlist** (P1 consistency): after the
  all-public check, for rows whose `data_source ∉ PUBLIC_BY_LICENSE_PROVIDERS`, require `visibility == 'public'`; else
  fail closed. (Requires publishers to carry `visibility` through to the guard for non-allowlisted providers — thread
  it where `access_tier` already rides.)
- [ ] **Step 3 — run, expect pass.** Note: GS has no live public rows, so this is future-proofing for GS (no behavior
  change today) AND closes the unknown-provider gap, proven by the test.

### Task 0.4: Consumer audit for the upcoming NULL-`xt_gk` contract (M5, prep)

- [ ] **Step 1** — enumerate consumers of `xt_gk`: confirm the cohort filters `xt_gk IS NOT NULL` (safe); grep
  `fct_gk_tracking_*`, the GK-page UI/query modules, and the defense report's `eg_cohort` pull. Record which assume
  non-NULL. Output: a short list pasted into Phase 1 Task 1.2 (no code yet — the enum lands in Phase 1).

---

## Phase 1 — Adopt silly-kicks 4.37.0 + recompute (release SATISFIED)

### Task 1.0: Confirm one-provider-per-match calls (C1)

- [ ] Confirm the AC driver never batches >1 provider into a single `compute_xt_gk` / frame call (it dispatches
  per-`(match, period)`). Add/confirm a sentinel test so a future multi-provider batch fails loud rather than at the
  new 4.37.0 raise. (Expected: already true — this is a guard, not a fix.)

### Task 1.1: Pin silly-kicks ==4.37.0 (L2 adopt)

**Files:** `pyproject.toml`, `uv.lock`, `terraform/modules/workflows/main.tf` (`==` pins), the 4 version sentinels
(`exec_visibility._REQUIRED_SK_MIN`, the 6 trainer mins, `test_sk3_mig_b_orchestrator` expected, guard-import
isolation), wheel via `bump_wheel.py`.

- [ ] Pin `silly-kicks==4.37.0`; move all sentinels in lockstep; `uv lock`; terraform `==` parity; run the **FULL**
  suite (a curated slice misses the sentinels — see the silly-kicks-bump memory). Regression gate: GS/idsse/metrica/
  sportec byte-identical (4.37.0 is default-off for them).

### Task 1.2: Provenance enum + observability + NULL-`xt_gk` contract on `fct_action_context` (L2 / Q-A3 / M2 / M5)

**Files:** `src/analytics/action_context/schema.py`, `dbt_project/models/marts/fct_action_context.sql`,
`_marts__models.yml`, a python SQL-text guard test, migration.

- [ ] **Provenance enum.** Add `xt_gk_origin_source` ∈ {`native`,`tracking_gk`,`goalkick_prior`,`unresolved`} to
  RESULT_COLUMNS/DDL; `accepted_values`. **`native` is the COMMON value** (open-play passes keep native per 4.37.0) —
  the contract test must NOT treat `native` as rare/legacy.
- [ ] **Observability (M2 — surface, don't drop).** Add `xt_gk_native_goalkick_out_of_region` (per-row S4 flag) to the
  schema/mart; capture `XtGkReport.n_native_goalkick_out_of_region` + `TrackingConversionReport.n_gross_off_pitch`
  into the AC run's structured log / observability table. **CI/batch rate-gate = tracked follow-up** (threshold from
  the recomputed corpus rate — note it in TODO, don't block on it).
- [ ] **NULL contract.** `unresolved → xt_gk IS NULL` (+ reported count), never imputed — now a **rare** subset
  (goal-kick / NaN-native edge), not the common case. Fix any Task-0.4 consumer that assumed non-NULL.
- [ ] +columns regen the AC goldens (`build_ac1_{mini,full}_golden.py`); plumb through staging→mart.

### Task 1.3: L1 satisfied + M3 keeper-rate diagnostic (no longer a hard validation gate)

L1 is satisfied upstream (4.37.0 `convert_to_frames` preserves `is_visible` as `visibility` — no lakehouse plumbing).
The 4.37.0 delta also **demotes the M3 branch**: distrust is goal-kicks-only, and goal-kicks resolve via the ladder
(detected keeper→`tracking_gk`; **else rule-point→`goalkick_prior` ≈(5.5,34)**). So **goal-kicks land in-box on the
public recompute regardless of keeper-detection reliability** — the public recompute IS a valid validation gate for
the headline goal-kick fix.

- [ ] **Diagnostic only** — keeper-only `is_visible` rate on a public match (keeper via the AC `is_goalkeeper` frame
  flag): tells us the **`tracking_gk` vs `goalkick_prior` split** (both land in-box), not whether the fix worked.
  If keeper detection is ~always-interpolated on the public feed, expect goal-kicks resolved mostly via
  `goalkick_prior` on public, more via `tracking_gk` on the RM course-raw bundle — neither blocks public validation.

### Task 1.4: SkillCorner recompute (AC-layer only — no re-ingest)

**The fix lives in the converter/resolver (`convert_to_frames` + `resolve_gk_geometry`), read against the EXISTING
raw-faithful bronze — so NO bronze re-ingest** (4.37.0 delta; `is_visible`/`ball_is_detected`/native coords already
present). Scope:

- [ ] **Re-run the AC recompute** (`compute_action_context`) for SkillCorner against existing bronze with 4.37.0 →
  rewrites `bronze.spadl_action_context` (xt_gk origins fixed; `access_tier` re-stamped — already public for the
  A-League, so unchanged) → rebuild `fct_action_context`.
- [ ] **SPADL re-conversion is CONDITIONAL (confirm, don't assume).** 4.37.0's keeper-origin fix is in the
  xt_gk/resolve layer; open-play SPADL `start_x/y` keep native (unchanged). Diff SkillCorner SPADL output old-vs-4.37.0
  on a sample — **only if `convert_to_actions` changed** do `spadl_actions`/`vaep_action_values`/`fct_action_values`
  also need re-materializing. Expected: not needed (verify before scoping the larger rebuild).
- [ ] **Rebuild the dim-resolved marts** `fct_tracking_frames`*/`fct_shot_psxg`/`fct_player_embeddings` (now correctly
  tiered via the closed dim_matches) → `refresh_synced_tables` + `gh workflow run lakebase-grants.yml`.
  (*`fct_tracking_frames` rebuild here only re-tiers + re-velocities; its off-pitch coords are L4, Phase 2.)
- [ ] Re-check ADR-037 2700s watchdog before the AC recompute; AC job per the topic memory. The tracking bronze
  `access_tier` (deferred, inert) is **not** re-stamped here — nothing reads it.

### Task 1.5: Validation (4.37.0 acceptance)

- [ ] **Goal-kick** origins ≈100% own-box (the headline fix). **Open-play pass** origins localize **via native**
  (they were already the keeper — expect them effectively unchanged, not ladder-resolved). `unresolved` count
  **small** (goal-kick/NaN edge). Within-pitch to tolerance (M4); `n_gross_off_pitch` ≈ 0.
- [ ] **Calibration (review Note 2 — expected, not a bug):** if the public feed's keeper detection is poor, goal-kicks
  collapse to the `goalkick_prior` constant ≈(5.5,34) → "≈100% own-box" passes **trivially** and goal-kick `xt_gk`
  carries **no keeper discrimination on the public feed**. The discriminating signal lives in **open-play passes
  (native = real varying keeper position)** + the **`tracking_gk` goal-kicks** that need the RM course-raw bundle's
  better detection. So "all public goal-kicks identical" is expected; the real metric test runs on the RM games.
- [ ] **Regression gate: GS / idsse / metrica / sportec byte-identical** (4.37.0 default-off for them).
- [ ] `access_tier`: SkillCorner public (A-League) / GS restricted; public datasets intact.
- [ ] Then **ping the analysis side** to run the end-to-end check on the recomputed gold **before the RM games flow**.

---

## Phase 2 — L4: `fct_tracking_frames` off-pitch (DECOUPLED — scope first, P1/P2)

**Not on the xt_gk critical path** (it does not feed xt_gk). Start as a **scoping task**, not code.

- [ ] **Step 1 — scope consumer impact (P2).** Enumerate `fct_tracking_frames` consumers (GK-tracking page,
  IDSSE-minutes, pitch-control inputs, synced tables) and what a re-point to the silly-kicks builder would change
  (coords/schema/GK-identity). Establish whether re-point (a) is blocked (cf. the un-migrated `fct_tracking_context`
  retirement that's blocked on GK-identity + IDSSE-minutes re-homing).
- [ ] **Step 2 — decide (a) re-point vs (b) patch.** Prefer **(a) re-point to `convert_to_frames`** (single geometry
  source, within-pitch by construction, TF-23 direction). Choose **(b) lakehouse transform fix + within-pitch
  assertion** ONLY if (a) is established-blocked — (b) re-creates a lakehouse geometry path we'd rather delete.
- [ ] **Step 3 — implement the chosen option + fold the rebuild into a convenient recompute** (its own timeline).

---

## Sequencing summary
1. **Phase 0** (privacy hardening + backfill completeness) — own PR, no silly-kicks dep. Closes H1 before any RM
   ingest.
2. **Phase 1** — release dependency SATISFIED (silly-kicks 4.37.0): pin + provenance/observability + AC-layer
   recompute + validate. This is the Jeff-critical xt_gk fix.
3. **RM ingestion** (5-then-94, spec §3) only after Phase 1 validates + H1 is live.
4. **Phase 2 (L4)** independently, whenever scoped — never blocks 1–3.

Phase 0 and Phase 1 can share one PR (all code) with the recompute (1.4) + RM ingestion gated separately, or split
0→1; either is fine — they are decision-decoupled (Phase 0 has no 4.37.0 dependency).
