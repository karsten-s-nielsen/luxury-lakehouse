# Spec: SkillCorner keeper-origin rebuild + `access_tier` completion (combined recompute)

**Date:** 2026-06-30 · **Status:** DRAFT for analysis-side + producer review · **Author:** lakehouse session
**Builds on:** ADR-064 (per-match `access_tier`, merged PR #414) · **Source finding:**
`docs/investigations/2026-06-29-skillcorner-keeper-origin-coordinate-scatter.md`

## 0. Why these two are one spec

Two independent threads both require **re-materializing SkillCorner SPADL + action-context**, and the data owner
has **paused all recalculation** until a revised plan exists. Doing them in one pass means SkillCorner is recomputed
**once**, not twice:

1. **Keeper-origin rebuild** (data-quality bug — analysis-side finding): SkillCorner GK-distribution origins are
   wrong, which corrupts `xt_gk`, keeper pressure, and PEV.
2. **`access_tier` completion** (the gap left by PR #414's value-free backfill): existing SkillCorner/GS rows are not
   yet correctly tiered in the dim_matches-resolved marts.

Neither is urgent (private RM data not ingested), but both must land — fix + completion + **one** recompute — before
the private Real Madrid games flow.

---

## 1. Thread A — SkillCorner keeper-origin rebuild

### 1.1 Problem (from the investigation)
SkillCorner keeper-action `start_x/y` is taken from the **broadcast ball-detection event location, not the keeper's
position** → a full-pitch scatter (goal-kick `start_x` min 0.8 / max 98.4 / SD 23.2; own-box rate 51% vs ~100% every
other provider). Two compounding issues:
- **Origin source is wrong** — `resolve_gk_geometry` trusts the non-NaN native ball coordinate (`xt_gk_origin_source
  = native`, conf 1.0) instead of imputing from the keeper, unlike GS (whose NaN goal-kick origins get the
  `goalkick_prior` / tracking-GK path).
- **Detection reliability** — broadcast tracking only sees the keeper ~24.7% of frames overall (~58% at the keeper's
  own action frames, ~70% within ±1s). The current public feed holds/interpolates the keeper into ~100% of frames
  with no usable `is_detected`, and coordinates run **off-pitch (x→123m)**.

### 1.2 The fix (NOT a clamp) — three parts
1. **Resolve keeper-action origins from real keeper detections**, never the interpolated/held position and never the
   ball-event coordinate.
2. **Fix the coordinate transform** — SkillCorner native is **center-origin** (x∈[−52.5,+52.5]), physical
   (ends switch each half). Transform to SPADL [0,105] + home-LTR; outputs must stay within the pitch (kill x>105).
3. **Tiered keeper-origin resolution, by action type** (detection is only ~58–70%, so a single rule is dishonest):
   - keeper **detected at / ±1s** of the action → tracked keeper position (transformed) — *best*;
   - else **goal-kick** → **rule-point prior ≈(5.5, 34)** — reliable (goal kicks always taken from the goal area;
     exactly GS's existing `goalkick_prior`);
   - else **open-play pass / throw, no detection** → **flag / exclude** (no honest prior; never impute a guess);
   - tag provenance per action (`tracking_gk` / `goalkick_prior` / `unresolved`).

### 1.3 Ownership — best-practice, long-term: silly-kicks owns the fix; the lakehouse stays thin

Every part of this is **general broadcast-tracking domain logic**, not lakehouse-specific: any silly-kicks consumer
ingesting SkillCorner broadcast tracking hits the same scatter / off-pitch / partial-detection problem. The
established pattern is unambiguous — **silly-kicks owns geometric + domain** (frame orientation is "owned UPSTREAM by
silly-kicks (geometric)", ADR-053/034/035; `resolve_gk_geometry` already lives there with the `goalkick_prior` path),
and the prior lakehouse-side orientation net was **deleted** precisely to stop duplicating domain logic downstream
(TF-23). So the recommendation is to put the fix where the domain already lives and keep the lakehouse a thin
adopter — not to split it 50/50.

**silly-kicks SHOULD own (the whole data-quality fix):**

| # | Concern | Why silly-kicks |
|---|---|---|
| S1 | **Coordinate transform correctness** — SkillCorner center-origin ±52.5 → SPADL [0,105] + home-LTR, with a **within-pitch invariant** (no x>105 / x<0 beyond tolerance) | Pure geometry; same machinery as the silly-kicks frame-orientation/LTR it already owns. A lakehouse clamp would re-create the deleted net (TF-23 anti-pattern). |
| S2 | **Carry `is_detected` through the tracking-frame model** (`convert_to_frames` preserves it; not interpolated away) | The signal must survive frame-building to be usable by the resolver; it's a property of the converter, not the warehouse. |
| S3 | **Tiered keeper-origin resolution in `resolve_gk_geometry`** — detected→tracked-keeper-position; goal-kick→`goalkick_prior`; else→`unresolved`; **stop trusting the broadcast ball-event coordinate as a keeper origin**; emit provenance | This is the exact module + a natural generalization of the GS NaN-imputation it already does. The "is a native origin trustworthy?" judgment is domain, not warehouse. |
| S4 | **Loud-validation** of an implausible *native* goal-kick origin (companion, defense-in-depth) | Belongs next to the resolver so every consumer fails loudly, not just this lakehouse. |

**The lakehouse SHOULD own only the thin edges:**
- **L1 — feed the raw signal.** Ensure SkillCorner raw `is_detected` reaches silly-kicks' frame builder (already
  captured as `is_visible` in `bronze.skillcorner_tracking`; for the RM *course-raw* bundle, confirm the richer
  per-frame signal survives `skillcorner_tracking.py` ingestion). The lakehouse provides the bit; silly-kicks decides
  how to use it.
- **L2 — adopt + plumb the contract.** Bump silly-kicks (version sentinels, terraform `==` env pins, wheel), and carry
  the new provenance through to the marts: surface `xt_gk_origin_source` ∈ {`tracking_gk`,`goalkick_prior`,`unresolved`}
  on `fct_action_context` (a mart contract change the lakehouse owns) and render `unresolved` as NULL `xt_gk` + a
  reported count, never a silent impute.
- **L3 — governance + orchestration.** `access_tier` (Thread B) + the combined recompute/sequencing (§3). Unrelated to
  the geometry; stays lakehouse.

**Net:** the lakehouse does **not** null origins, does **not** clamp coordinates, does **not** reimplement GK-origin
logic. It supplies `is_detected`, adopts the silly-kicks version, plumbs one new provenance enum to the mart, and runs
the recompute. Everything else is a silly-kicks change request.

**Remaining genuine questions (for the analysis + silly-kicks side):**
- **Q-A1 (silly-kicks).** Confirm S3's tier-3 is **flag-`unresolved`** (no impute) for open-play passes/throws with no
  detection — the spec's position, vs any weaker prior.
- **Q-A2 (lakehouse/L1).** Is the public A-League `is_visible` the *real* detection bit, or is detection-preservation
  only meaningful once the RM course-raw bundle lands? (Determines whether the public-feed recompute already improves,
  or only the RM games do.)
- **Q-A3 (lakehouse/L2).** Confirm the `fct_action_context` provenance-enum contract change + downstream `unresolved`
  rendering is acceptable, and whether `xt_gk` for `unresolved` is NULL (recommended) or omitted.

### 1.4 Acceptance (Thread A)
- SkillCorner goal-kick origins ≈100% own-box; pass origins localize; scatter SD collapses.
- SkillCorner tracking outputs within the pitch **to a few-metre tolerance** (M4): legitimate behind-goal keepers
  (±60 native) yield x slightly outside [0,105] — allowed; **gross** off-pitch (x→123) must be **impossible by a
  correct transform** and is a **loud upstream assertion, not a silent clamp**.
- `unresolved` subset is surfaced as a count, not imputed.
- GS / idsse / metrica **unchanged** (SkillCorner-only) — regression-gate.
- Spot-check goal-kicks: tracking/prior origin ≈(5.5,34) where the native event coord was downfield.

---

## 2. Thread B — `access_tier` completion (the backfill gap)

### 2.1 What the backfill did (updated 2026-06-30)
Two value-free, idempotent backfill passes were run:
- **Pass 1** — the three per-row fact tables `spadl_actions`, `vaep_action_values`, `spadl_action_context`
  (verified 0 NULL; skillcorner→public, GS→restricted, metrica/idsse/statsbomb/wyscout→public).
- **Pass 2** — the **match-info** tables (`skillcorner_matches`→public 360, `gradientsports_metadata`→restricted 64)
  + `psxg_tracking_predictions` (per `data_source`: public 50 / restricted 234). All 0 NULL.

A catalog `information_schema` scan confirms exactly **10 tables** carry `access_tier`; **6 are now 0-NULL**. The
remaining 4 are the raw tracking-frame tables (`gradientsports_tracking` 269.6M, `idsse_tracking` 21.9M,
`skillcorner_tracking` 9.6M, `metrica_tracking` 0.43M). **Operator decision (2026-06-30): do NOT standalone-backfill
those four** — no mart reads their per-row `access_tier`, so they are functionally inert; they get correctly
re-stamped for whatever providers are re-ingested in the recompute (§3), and the rest stay harmless-NULL (going-forward
ingestion stamps new rows). No 300M-row rewrite. Metrica is fully covered everywhere it is consumed (6,159 backfilled
public fact rows each table; `dim_matches` hardcodes metrica→public; 0 psxg rows) — `metrica_tracking` is just one of
the four inert tables.

### 2.2 Per-mart `access_tier` source (grounded in the dbt SQL)
| Mart | `access_tier` source | Existing-row correctness today |
|---|---|---|
| `fct_action_values` | **per-row** `av.access_tier` (bronze SPADL) | ✅ correct (backfilled) |
| `fct_action_context` | **per-row** (bronze AC passthrough) | ✅ correct (backfilled) |
| `fct_tracking_frames` | `dim_matches.access_tier` | ⚠️ skillcorner/GS = **NULL → fail-safe restricted** |
| `fct_shot_psxg` | `dim_matches.access_tier` | ⚠️ same |
| `fct_player_embeddings` | `dim_matches.access_tier` | ⚠️ same |

`dim_matches` hardcodes `'public'` for statsbomb/wyscout/idsse/metrica (no feed) — always correct. For
**skillcorner/GS** it reads `max(access_tier)` from the **match-info bronze** (`skillcorner_matches` /
`gradientsports_metadata`), whose existing rows are **NULL** → dim_matches NULL → the three dim-resolved marts
**fail-safe to restricted**. For GS that is correct; for the **existing public A-League SkillCorner** it is **wrong
(over-restriction)** — those matches would be wrongly withheld from the public datasets at the next rebuild.

### 2.3 Answers to the three questions you asked
- **Why weren't the match-info tables backfilled now?** The plan (spec R1) chose to populate match-info
  `visibility`/`access_tier` by **re-ingesting** each match (so the row carries its *real* pining `visibility`),
  rather than stamping a default — match-info is the authoritative per-match source of truth for the publish split,
  and a default-backfill bakes a guess into it. But re-ingest calls `fetch_match_list` with the owner token, which
  **discovers all matches including the 99 private RM** — entangling it with the private-game ingestion you paused.
  So it could not ride the "value-free backfill now" step.
- **What other tables are missing?** Of the 10 tables the migration touched, the backfill covered 3. Not backfilled:
  - **2 match-info** tables (`skillcorner_matches`, `gradientsports_metadata`) — **materially impactful** (feed
    dim_matches → the 3 dim-resolved marts).
  - **5 tracking/psxg bronze** tables (`skillcorner_tracking`, `idsse_tracking`, `metrica_tracking`,
    `gradientsports_tracking`, `psxg_tracking_predictions`) — got the column for schema-consistency, but **no mart
    reads their per-row `access_tier`** (the marts resolve from dim_matches), so their NULLs are presently
    **dead/harmless**.
- **Will they be populated going forward?** **New** data: yes — ingestion stamps `access_tier` (and `visibility` on
  match-info) at write time everywhere, so new skillcorner/GS matches carry real `visibility` → dim_matches correct.
  **Existing** data: the 3 fact tables are done; existing **match-info stays NULL** until re-ingested or
  default-backfilled (the open gap); existing tracking/psxg bronze stays NULL but harmless.

### 2.4 The fix (Thread B) — DONE for existing rows
**Resolved (2026-06-30):** the existing match-info rows were **provider-default backfilled** (skillcorner→public,
GS→restricted; `visibility` left NULL — real value arrives on natural re-ingest). This closed the `dim_matches` NULL
gap with zero recompute, consistent with "stop before recalc, backfill OK." No scoped re-ingest was needed.

**`access_tier_backfill.py` is now itself incomplete vs reality** — its `BACKFILL_TABLES` lists only the 3 fact
tables, but match-info + psxg were also (correctly) backfilled. Code follow-up (this cycle's PR): widen the module to
enumerate **every** `access_tier`-bearing table so the documented, re-runnable backfill == reality.

**Tracking-table `access_tier` (the 4 raw tables):** NOT standalone-backfilled (operator decision, §2.1) — folded into
the recompute (§3). No mart reads them, so leaving them NULL until then is functionally inert.

---

## 3. Combined recompute + rollout sequencing

The decisive efficiency point: Thread A and Thread B (and the access_tier per-row re-stamp on any re-converted
SkillCorner rows) all want the **same SkillCorner SPADL + AC re-materialize**. Order:

0. **Already done (2026-06-30):** fact-table + match-info + psxg `access_tier` backfill (§2.1). The dim-resolved
   downstream marts are **deliberately NOT rebuilt yet** (operator decision) — they wait for this recompute, which
   rebuilds them anyway, so a standalone rebuild now would be wasted.
1. **Land Thread A.** Order: **silly-kicks S1–S4** (transform+within-pitch invariant, `is_detected` through frames,
   tiered `resolve_gk_geometry`, loud-validation) ship first as a silly-kicks release; then the **lakehouse adopts**
   L1 (confirm `is_detected` feed) + L2 (version bump/pins/wheel + the `xt_gk_origin_source` provenance enum on
   `fct_action_context`). The lakehouse writes **no** geometry code.
2. **One scoped SkillCorner recompute** — re-ingest SkillCorner tracking (preserve `is_detected`, fix transform;
   re-stamps `skillcorner_tracking.access_tier` in passing) → re-convert SkillCorner SPADL → re-materialize
   `fct_action_values` / `fct_action_context` (per-row `access_tier` re-stamped) → **rebuild the dim-resolved marts**
   `fct_tracking_frames` / `fct_shot_psxg` / `fct_player_embeddings` (now correctly tiered via the backfilled
   dim_matches) → refresh synced + grants. The other providers' tracking-table `access_tier` stays inert-NULL unless
   they are independently re-ingested — fine, nothing reads it.
3. **Validate** on the existing **public A-League** SkillCorner: goal-kick origins ≈own-box, no off-pitch coords,
   `unresolved` count surfaced, access_tier all-public for SkillCorner / restricted for GS, public datasets intact.
4. **Ingest 5 private RM games first** (diagnostic — analysis side) → recompute just those → check goal-kick origins
   ≈own-box + they land **only** in the `-restricted` repos. **Hold** the remaining 94 until the 5 validate.
5. **Ingest the remaining 94** → recompute → publish.

**Guardrails:** nothing publishes until B-opt-1 has closed the dim_matches NULL gap (else the public A-League
SkillCorner is withheld and the leak guard sees NULL→restricted everywhere). The private games enter only after
Thread A is proven on public data.

---

## 3a. Thread B HARDENING — SkillCorner privacy default (review H1, **blocking before RM ingest**)

Review H1 is correct and is the most important change in this revision. SkillCorner is now a **mixed-license**
provider (public A-League + private RM), so `classify_access_tier("skillcorner", None) → PUBLIC` is a **leak shape**:
a SkillCorner match with no explicit signal must fail **restricted**, not public. A wrong-restrict is recoverable; a
wrong-publish of private RM is not. Required changes (amends ADR-064; plan work):

- **H1.1 — make the no-signal default an ALLOWLIST (review P1).** The policy core defaults to public **only** for
  providers on a `PUBLIC_BY_LICENSE_PROVIDERS` allowlist (statsbomb/wyscout/idsse/metrica — open data); **everything
  else fails safe to restricted** — skillcorner/gradientsports with no per-match signal, *and any unknown/new
  provider*. (A denylist — "feed providers default restricted, else public" — would still leak an unclassified new
  provider to public; the allowlist closes that.) Updates `classify_access_tier`, its truth-table test
  (`("skillcorner", None) → RESTRICTED`, `(unknown, None) → RESTRICTED`), and the ADR-064 default narrative.
- **H1.2 — RM games carry real `visibility=private` at ingest, structurally.** `MatchInfo.visibility` is already
  REQUIRED-no-default (`test_visibility_required`), so ingestion cannot silently default — keep that invariant and add
  an explicit-public assertion (below).
- **H1.3 — publish guard keyed on the SAME allowlist (P1 consistency): a row whose provider is NOT in
  `PUBLIC_BY_LICENSE_PROVIDERS` reaches a PUBLIC repo only with an explicit `visibility='public'`,** never a default.
  Covers skillcorner, gradientsports, **and any unknown provider** — symmetric with H1.1. GS is restricted today so no
  live leak; the guard is future-proofing + the unknown-provider backstop. `hf_leak_guard` fails closed otherwise.
- **H1.4 — reconcile the existing backfill.** My 2026-06-30 backfill set existing SkillCorner `access_tier='public'`
  with `visibility=NULL` (correct tier — the A-League IS public — but not "explicit public" under H1.3). Fix: set
  `visibility='public'` on the **existing** `skillcorner_matches` (and stamp it through where the guard reads), since
  those matches are confirmed public A-League. This is value-free and makes them explicit-public; the flipped default
  (H1.1) then makes any *future* unconfirmed SkillCorner row restricted. **Note:** once H1.1 lands, the
  `access_tier_backfill.py` SkillCorner default can no longer be `classify(skillcorner, None)` (that's now restricted)
  — existing SkillCorner must be encoded as confirmed-public explicitly, not derived from the (now fail-safe) default.

No emergency today (no private SkillCorner ingested; the required-`visibility` invariant blocks a silent ingest
default), but **H1 must land before the RM-5 gate** (§3 step 4).

## 4. Out of scope / separate
- silly-kicks loud-validation companion (doc §Companion) — tracked with the analysis side, not required here.
- statsbomb action-context (held — `fct_action_context` has no statsbomb rows; unaffected).
- Any change to the 4 event providers (this is SkillCorner-only + the GS/SkillCorner match-info tier).

## 5. Review checklist (for the other session)
- [ ] **§1.3 ownership** — agree silly-kicks owns S1–S4 and the lakehouse stays thin (L1–L3). This is the central
  decision; the rest follows from it.
- [ ] Q-A1 (silly-kicks) tier-3 = flag-`unresolved`, no impute; Q-A2 (lakehouse) public-feed `is_visible` trust;
  Q-A3 (lakehouse) `xt_gk_origin_source` mart-contract change + `unresolved` rendering.
- [ ] §1.4 acceptance sufficient? Anything to add for the RM course-raw `is_detected`.
- [ ] §3 sequencing — silly-kicks-first, then lakehouse adoption; 5-then-94 private gate; marts rebuilt in the
  recompute (not now).
- [ ] §2 access_tier — confirm the deferred tracking tables + the `access_tier_backfill.py` widening are acceptable.

On approval this splits into **two** work items, mirroring the ADR-064 flow: (a) a **silly-kicks change request**
(S1–S4 — for whoever owns that repo / the analysis side), and (b) a **thin lakehouse TDD plan** (L1–L3 + H1 hardening
+ the recompute + the `access_tier_backfill.py` widening) that adopts the silly-kicks release. The lakehouse plan is
deliberately small — adopt, plumb one enum, orchestrate, govern, and harden the privacy default.

## 6. Analysis-side review responses (2026-06-30)

Boundary (§1.3) **approved** by the analysis side (bronze confirmed raw-faithful: `is_visible` + `ball_is_detected` +
native ±52.5 present → silly-kicks owning S1–S4 is converter/resolver work, not the library doing lakehouse work).

- **H1 (privacy default) — ACCEPTED, see new §3a.** Flip SkillCorner no-signal default → restricted; RM real
  `visibility` at ingest; publish guard requires explicit `visibility='public'`; reconcile the existing A-League to
  explicit public. Blocking before the RM-5 gate.
- **H2 (mart-from-converter) — VERIFIED, and it splits the symptom in two.** The **xt_gk keeper-origin scatter** (the
  reported bug) is on the AC path, which builds frames via `convert_skillcorner_bronze_to_frames` →
  `sk_frame_adapters` → silly-kicks `convert_to_frames` (TF-23, `pipeline.py:161`) — so **S1–S3 do fix it**. But
  **`fct_tracking_frames` is a SEPARATE path**: it is materialized from `stg_skillcorner__tracking` → raw
  `bronze.skillcorner_tracking` with a **lakehouse-side** transform (anisotropic SB-unit scaling, `fct_tracking_frames
  .sql:24`), **not** `convert_to_frames`. So the **off-pitch x→123m in `fct_tracking_frames` is NOT fixed by adopting
  silly-kicks.** Decision (best-practice, long-term): **re-point `fct_tracking_frames` to the silly-kicks builder**
  (one transform, within-pitch by construction — the TF-23 direction), rather than patch the lakehouse transform. This
  is a **new L-item (L4)** and is likely the larger part of the lakehouse plan; flag whether the GK-identity /
  IDSSE-minutes consumers block it (cf. the un-migrated `fct_tracking_context` retirement). *If* re-pointing is
  blocked, the fallback is an L4-lakehouse transform fix + within-pitch assertion — but that re-creates a lakehouse
  geometry path we'd rather delete.
- **M3 (is_visible varies) — VERIFIED real:** public match 2017461 = 434,173 false / 454,715 true (≈49/51), so
  detection is genuine on the public feed, **not** always-true. Caveat the doc raised: that's across *all* players;
  the **keeper-specific** rate may still be ~100%-present (interpolated). Plan adds a keeper-only
  `is_visible`-rate check before relying on the §3 public-data validation gate — if the keeper is always-interpolated
  on the public feed, validate Thread A on the **RM-5** gate, not the public recompute.
- **M4 (within-pitch = tolerance + fail-loud, not clamp) — ADOPTED.** Bronze legitimately has players to ±60 native
  (keepers behind the goal line) → the transform yields some x slightly outside [0,105]; use a few-metre tolerance.
  A correct native→SPADL transform makes **gross** off-pitch (x=123) impossible by construction, so S1 treats residual
  gross off-pitch as a **loud assertion pointing upstream**, never a silent clamp. §1.4 updated.
- **M5 (NULL `xt_gk` mart-contract reach) — ACCEPTED, stated explicitly.** Contract: `unresolved` → `xt_gk IS NULL`
  + reported count, never imputed. L2 enumerates consumers before landing: the cohort filters `xt_gk IS NOT NULL`
  (safe); audit `fct_gk_tracking_*`, GK-page UI/marts, and the defense report's `eg_cohort` pull for any that assume
  non-NULL. Add a dbt/sql-text guard documenting the new NULL source.
- **L6 (S4 reconcile) — FIXED:** S4 (loud-validation) belongs in **silly-kicks**, tracked separately, **not gating**
  this recompute. §1.3 lists ownership; §4 lists scheduling — consistent reading: owned by silly-kicks, scheduled
  separately.
- **L7 (decouple Thread A / B) — ACCEPTED:** Thread B's existing-row backfill + dim_matches gap are **already closed**
  and must not be held hostage to Thread A slippage (or vice-versa). They merely *share* the recompute for efficiency;
  the dim-resolved marts can rebuild on Thread A's schedule without re-doing B. §3 step 0 already records B as done.

**New L4** (from H2) joins the lakehouse plan: re-point `fct_tracking_frames` to the silly-kicks frame builder (or, if
blocked, fix + assert the lakehouse transform). This is the one place the "lakehouse stays thin" story has a caveat —
flagged honestly.

## 7. Review #2 responses (analysis side, 2026-06-30) — pre-plan sign-off

Approved for planning. Three carry-into-plan items + two confirms:

- **P1 (structural) — L4 is a THIRD, DECOUPLED work item; it must NOT gate the xt_gk path.** `fct_tracking_frames`
  does **not** feed xt_gk (the AC `convert_to_frames` path does — H2), so its off-pitch x→123 is a *separate mart's*
  problem (GK-tracking page / IDSSE-minutes) with its own consumer-migration risk. **Priority ships without it:**
  silly-kicks S1–S3 + lakehouse L1/L2 + H1 + recompute + the Jeff xt_gk validation. `fct_tracking_frames` stays
  as-is until L4 is independently scoped; fold its rebuild into a *convenient* recompute later. The combined recompute
  is fine as efficiency, but **the timelines are not coupled.** → work-item structure below is now **three** items.
- **P2 — scope L4's consumer impact BEFORE choosing re-point (a) vs patch (b).** Re-point to the silly-kicks builder
  is the right long-term call (single geometry source, TF-23 direction) but may change coords/schema/GK-identity for
  its consumers. The fallback (b) re-creates a lakehouse geometry path we'd rather delete, so (b) is chosen **only if
  (a) is established-blocked**, not assumed. L4 starts as a **scoping task**, not a code task.
- **P3 — H1.4 premise VERIFIED (2026-06-30):** `bronze.skillcorner_matches` = **360 rows, all competition_id 61
  "A-League" 2024/2025** — no private match present. So the `visibility='public'` stamp on existing SkillCorner is
  safe. The plan still runs this assertion at execution time (cheap insurance; a privacy-stamp verifies its premise).
- **M3 keeper-specific gate — explicit plan branch:** before trusting the §3 public-data validation, check
  **keeper-only** `is_visible` rate on a public match. **If the keeper is ~always-interpolated on the public feed →
  validate Thread A on the RM-5 gate, not the public recompute.** Two named branches in the plan.
- **H1.3 generalized** (above) — guard covers any visibility-feed provider reaching public.

### Work-item structure (final)
- **(a) silly-kicks CR — S1–S4.** Transform+within-pitch invariant (M4 tolerance), `is_detected` through frames,
  tiered `resolve_gk_geometry` (tier-3 = flag-`unresolved`), loud-validation (S4, non-gating).
- **(b) Thin lakehouse plan — PRIORITY (xt_gk-critical).** L1 (`is_detected` feed confirm) + L2 (silly-kicks adopt:
  version sentinels/`==` pins/wheel + `xt_gk_origin_source` provenance enum on `fct_action_context` + consumer audit)
  + **H1** privacy hardening (H1.1 classifier flip + truth-table/ADR-064 amend, H1.2 ingest invariant, H1.3 generalized
  guard, H1.4 A-League explicit-public with the P3 assertion) + the SkillCorner recompute (with the M3 branch) +
  widening `access_tier_backfill.py`.
- **(c) L4 — DECOUPLED.** Scope consumer impact (P2) → re-point `fct_tracking_frames` to the silly-kicks builder (or
  fallback patch only if (a) blocked). Not on the xt_gk critical path.
