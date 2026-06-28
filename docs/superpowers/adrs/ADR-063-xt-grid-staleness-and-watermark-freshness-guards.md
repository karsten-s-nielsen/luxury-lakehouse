# ADR-063: Fix xT-grid staleness + establish a tiered freshness pattern and a cross-cutting staleness monitor

| Field | Value |
|---|---|
| **Date** | 2026-06-28 |
| **Status** | Accepted (rev 4 — M6/R5 decisions confirmed by Karsten, 2026-06-28; `…-xt-grid-fix-decisions.md`) |
| **Deciders** | Karsten Nielsen, xT-GK analysis side, Claude |

> **Revision 4 (decisions confirmed — design locked, ready to build):**
> - **M6 = KEEP-AND-GUARD, gating IMPLEMENTED.** ExT v2 is active. Keep per-comp grids AND actually apply
>   `require_directional` to per-comp grids **above a `min_actions` threshold** (small comps exempt — with a test
>   that a below-threshold comp does NOT false-fail). This resolves the M5 ADR↔plan mismatch **in the ADR's favour**
>   (the plan must implement per-comp gating, not only `global`). §6's deletion is **cancelled**.
> - **R5 = INTERIM full-AC-all-matches re-materialize on a *material* grid change; DEFER the projection split.** Do
>   not refactor `compute_xt_gk_projection` in this initiative. The waste (recomputing grid-independent features) is
>   acceptable *only because R4 keeps material grid changes rare*. Revisit the split later as a measured optimization
>   if re-materialization cost proves painful.
> - **R4 is LOAD-BEARING (not optional).** R5-interim is safe *only if* R4 has BOTH: (1) **material-only**
>   propagation (ε measured from real drift; relative/zone-aware), and (2) **drift-bounded vs the last-PROPAGATED
>   grid** (not the previous compute). Without (1)+(2), sub-ε drift accumulates and re-introduces H1 one hop down —
>   in which case R5 must revert to the projection split. **This coupling is a hard build constraint.**
> - **H1 + H3 remain blockers before the one-time rebuild (R7).** H4 monitor ships as the interim backstop. M7 stays
>   **refuted** (evidence: `tracking_context.py:1450` fits its own xT; it does not read the bronze grid → no
>   re-materialize needed). M8 (Delta time-travel rollback, not row-count snapshot) + L-items stand.

> **Scope note (rev 2, per review L12):** This ADR *fixes the xT-grid staleness* and *establishes* the tiered
> watermark pattern + a staleness monitor. It does **not** "eliminate the class" — that holds only once Tier B/C land.
> The cross-cutting monitor (Decision §5) is the interim backstop that makes the *whole* class observable now.

> **Revision 3 (sign-off resolutions, 2026-06-28) — these OVERRIDE the rev-2 Decision where they conflict:**
> - **M6 → KEEP-AND-GUARD (not delete).** Per-competition xT is on the **ExT v2** roadmap
>   (`specs/2026-04-25-ext-v2-reproduction-design.md`; grids "parameterized by competition + global"). Per the
>   reviewer's rule, a roadmapped artifact is kept — but **guarded**: the §1 directionality assert applies to
>   per-comp grids **above a min-action threshold** too (the danger was being *unguarded*, which keep-and-guard
>   removes as fully as deletion). So §1 reverts to "global always + per-comp above threshold"; §6's deletion is
>   **withdrawn** (pending the operator confirming ExT v2 is still active — if it's parked, revert to delete).
> - **R5 → SPLIT the grid-derived projection.** `xt_gk` lives inside the monolithic per-match AC UDF
>   (`enrich.py:_enrich_tracking_match`), so column-scoping is infeasible; the correct axis is **all matches,
>   grid-derived columns only**. Resolution: extract the `xt_gk_*` computation into a **separate cheap grid-derived
>   pass** (off-ball xT is already separate) that refreshes on the grid watermark — dissolving the H2 cadence/cost
>   tension (a cheap xt_gk-only refresh tracks the grid without re-running ghost-GK/DAS). `xt_gk` covers only
>   ~5,227 GK-distribution rows, so the split refresh is trivial vs a full tracking AC recompute.
> - **R4 → drift-BOUNDED materiality gate.** Do not ship a bare `ε=1e-3`. (i) **Measure** real append-to-append
>   grid drift first; (ii) use a **relative / zone-aware** metric (max relative change among cells above a value
>   floor, weighted toward the keeper/defensive zones where `xt_gk` lives at 0.007–0.02 — a flat 1e-3 there is ~10%
>   relative); (iii) **anchor** materiality to downstream precision (`xt_gk`/DZV reported to ~1e-3, DZV~0.009 → a
>   change is material iff it moves any `xt_gk` component by > ~1e-4, translated to grid units); (iv) **CRITICAL —
>   compare against the *last-propagated* grid** (the one consumers were last materialized on), not the previous
>   compute, so slow sub-ε daily drift cannot accumulate unbounded. Store the last-propagated grid as the gate
>   baseline.

## Context

The `global` xT grid in `bronze.expected_threat_grids` is **non-directional** (per-`zone_x` mean is a U:
0.0176 → 0.0053 → 0.0172; attack/defence ratio **0.98**). This is the root cause of the negative `xt_gk_dzv`
the xT-GK side rejected (φ-amplification of a spuriously-high own-goal zone pushes `max V_GK` into the defensive
third → `M = φ·(1 − V_GK/max V_GK) < 1` → `(M−1)·V_GK < 0`). Full investigation:
`docs/investigations/2026-06-28-xt-grid-stale-not-directionality-root-cause.md`.

The grid is **not** mis-oriented at build time. It is **stale**:

- The source SPADL is canonically LTR (shots 99.8–100% in the attacking half, every provider/period).
- The live core compute (`analytics.expected_threat.compute_expected_threat_grid`) on current LTR data produces a
  correctly directional grid (att/def **9.55**, max 0.17). The math is correct.
- `bronze.expected_threat_grids` was last written **2026-05-02** and never since — a frozen snapshot from when the
  SPADL was still mid-migration to LTR (ADR-022). The grid is ~2 months stale.

**Why it never refreshed** — the `expected_threat` skip guard uses the **build-if-absent** pattern:
`find_new_ids()` returns only competitions *absent* from the results table, and `need_global = "global" not in
existing` rebuilds the global grid only when it is *missing*. Once built, a grid is **never recomputed**, even when
its upstream (`fct_action_values`) is fully re-derived. Two further guards entrench the staleness:
`XTGrid.validate_differential(max_relative_change=0.30)` would *reject* the correct rebuild (0.054 → 0.17 peak), and
`XTGrid.validate_structural`'s directionality check (`np.all(np.diff(row_means) >= -0.01)`) is too lax — a gentle U
passes.

**This is a class, not a one-off.** `find_new_ids` keys on **ID presence**, not on whether an ID's *input* changed.
Any upstream re-derivation (the SPADL→LTR migration; future contract changes) silently leaves already-present
outputs stale. The framework **already** ships the correct primitive — `guards.check_upstream_freshness` /
`record_watermarks` (a Delta-version watermark per `(workflow_id, upstream_table)`), adopted by `dbt_runner` and
`hf_sync` and enforced by `test_guard_conformance.py`. The stale-prone guards simply never adopted it.

## Decision

1. **Add a directionality assertion to the xT grid build** (review M5 — robust form). `XTGrid.validate_structural(
   ..., require_directional=…)` asserts the global grid is materially attacking-directional using a **thirds-mean
   ratio** (mean xT over the attacking third ÷ mean over the defensive third) `>= 3` **and** a coarse shape
   check (Spearman rank correlation between `zone_x` and per-zone_x mean
   `>= 0.6`). This replaces both the too-lax `np.diff(...) >= -0.01` tolerance and the fragile single-extreme-column
   ratio. Build fails loud on a non-directional grid. Applied to the **global** grid only — per the rev-2 decision to
   **delete the per-competition grids** (§6), there are no per-comp grids to gate.

2. **Grid producer = daily recompute, write-only-on-material-change** (review H2). Migrate the `expected_threat`
   guard to `check_upstream_freshness` on `dev_gold.fct_action_values` so it recomputes when the upstream changes
   (the recompute is cheap — minutes). But the grid **gates expensive consumers** (AC re-materialization), so it must
   not churn them on daily micro-drift: the producer compares the freshly-computed grid to the stored one and
   **writes only if it changed materially** (max abs per-cell delta ≥ `1e-3`, or the directionality signature
   changed). An immaterial daily recompute is a no-op write → no Delta-version bump → no downstream churn. (Note:
   `check_upstream_freshness` already filters to `_DATA_CHANGING_OPS` and does **not** fire on OPTIMIZE/VACUUM —
   the review's concern there is already handled; the daily WRITE/MERGE to `fct_action_values` is the real driver,
   which write-if-changed absorbs.)

3. **Close the transitive freshness gap** (review H1 — BLOCKER). The grid is now a *dynamic* upstream, so its
   consumers must watermark on it. Declare `bronze.expected_threat_grids` as a watermark input for
   **`compute_action_context`** and **`compute_off_ball_xt`** so they re-materialize when the grid's version bumps.
   Because the producer only bumps the version on *material* change (§2), this re-materializes consumers rarely
   (on a real grid correction), not daily. Until this edge exists, staleness is merely relocated one hop, so this
   ships in Tier A, not deferred.

4. **`validate_differential` → WARN, not hard-fail; `record_watermarks` only on validated success** (review H3 —
   BLOCKER). In an auto-rebuild world a legitimate large shift (a future migration/correction) must not be hard-
   rejected (which would either block the write forever or, worse, record a "fresh" watermark on an un-rebuilt grid →
   the exact silent-staleness this ADR targets). Demote `validate_differential(0.30)` to a WARN + alert; the
   directionality assert (§1) is the real correctness gate. `record_watermarks` runs **only after** the grid is
   written-and-validated — never after a `validate_structural` raise — enforced by a test (review H3-i).

5. **Cross-cutting staleness MONITOR — interim backstop covering all tiers** (review H4). Add a cheap scheduled check
   that, for every registered derived table, alerts when its recorded watermark lags `max(upstream watermarks)` by
   more than a threshold. This is detect-and-alert, orthogonal to auto-rebuild; it would have caught the 2-month
   stale grid in week 1, and it covers Tier B (deferred) and Tier C (which otherwise relies on humans remembering a
   wipe checklist — the same human-memory failure that produced this bug). Ships with this ADR.

6. **Delete the unused per-competition grids** (review M6 — YAGNI). Verified: every consumer reads
   `competition_id='global'` only; nothing reads per-competition grids (and several were silently *inverted*). The
   per-comp grids are unguarded dead surface — drop them from the producer and the table. (If a future consumer needs
   per-comp grids, it reintroduces them *with* the §1 guard.)

7. **Tiered systemic rollout** (the "full systemic" decision) — guards are migrated by cost, because "rebuild on
   every upstream change" is right for cheap aggregates but wrong for expensive retrains:

   | Tier | Artifacts | Trigger | Mechanism |
   |---|---|---|---|
   | **A — cheap aggregate** | `expected_threat` (global xT grid) | upstream change → recompute; write/notify only on material change | `check_upstream_freshness` + write-if-changed + consumer watermark edges (§2–§3) |
   | **B — expensive model retrain** | `xg_model_v2`, `player_embeddings_v1`/`v2` | contract/feature-version change or manual force — NOT every append | new `check_contract_version` fingerprint guard + `force_rebuild` param (follow-up; design TBD) |
   | **C — per-id incremental** | `defcon_lite_*`, `tracking_context`, `elastic_sync`, `entity_resolution`, `tracking_metadata`, `line_breaking`, `pausa`, `formations_*`, `spadl_vaep` | per-id input re-derivation | documented wipe-and-recompute norm + the §5 monitor as the safety net; optional per-id content-version later |

   Tier A + the §5 monitor ship now. Tier B/C are scoped follow-ups (Tier B needs the new primitive; Tier C is a
   documented norm). The monitor makes B/C *observable* in the interim.

8. **One-time corrective rebuild (non-destructive)** (review M8). Do **not** `DELETE` the grid before validating the
   replacement — the only production copy must not be removed first. Instead: deploy Tier A → the producer's first
   post-deploy run (no stored watermark) recomputes, `validate_structural(require_directional=True)` gates it, and it
   **overwrites** via `replace_where` (Delta history retains the prior version for time-travel rollback; also snapshot
   the ~96-row grid to a side table first as belt-and-suspenders). Then the consumer watermark edges (§3) trigger the
   `fct_action_context` re-materialize + off-ball-xT recompute. No wipe needed (the watermark guard fires on first
   run); `validate_differential` is WARN so the 0.054→0.17 jump does not block.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Fix `compute_xt_grid_hf.py::_normalize_attack_direction` (the other side's first hypothesis) | targets a real-looking bug | that script is **not** the live bronze writer; the live core has no normalizer and is correct; the source is already LTR | does not touch the live grid; misdiagnoses |
| B. One-time manual rebuild, no guard change | smallest | re-stales on the next contract change; the directionality gap stays unguarded | treats the symptom, not the recurrence |
| C. Watermark-rebuild **everything** (incl. expensive retrains) on any upstream change | uniform | would retrain xG / embeddings (HF Jobs) on every daily `fct_action_values` write — expensive and needless | wrong granularity for Tier B |
| D. Directionality assert + watermark for cheap aggregates + contract-version for expensive models + documented manual norm for per-id (chosen) | fixes the urgent grid, generalises correctly per cost tier, reuses the existing watermark primitive | more design surface than a one-off | — |

## Consequences

### Positive
- The xT grid auto-refreshes when `fct_action_values` changes; the directionality assert makes a stale/symmetric grid
  a loud build failure, so this class cannot silently recur for the xT grid.
- Reuses the existing, conformance-tested watermark mechanism (`test_guard_conformance.py` already enforces the
  card-inputs + `record_watermarks` contract), so Tier-A adoption is low-risk.
- Fixes the downstream blast radius — the **two** real consumers of the global grid, **xT-GK** (`fct_action_context`
  base/rav/pev/dzv) and **off-ball xT**, move onto a correct surface and re-materialize automatically via the §3
  watermark edges. (Review M7: `fct_tracking_context` is **NOT** a consumer — `tracking_context` fits its *own* xT
  in-process via `silly_kicks ExpectedThreat().fit()` on LTR `bronze.spadl_actions`, so it is unaffected by the
  bronze-grid staleness. The earlier ADR draft listed it incorrectly.)

### Negative
- The corrective rebuild makes every xT-GK term move (the grid goes ~3× higher amplitude, 0.054 → 0.17) — `fct_action_context`
  must be re-materialised and the xT-GK cohort re-analysed (analysis side will re-baseline; treat `dzv_avg ≈ +0.01`
  as a sanity band, not a gate).
- Tier B needs a new small guard primitive (`check_contract_version`); Tier C ships as documentation + an optional
  future enhancement, so the per-id staleness risk is mitigated operationally, not yet automatically.
- The xT grid now rebuilds ≈daily (was effectively never). Cost is minutes; acceptable.

### Neutral
- `compute_xt_grid_hf.py`'s `_normalize_attack_direction` is wrong for LTR input (its no-shot "teams swap sides"
  inference flips already-correct team-periods) but is not the live writer. It is made a no-op / removed as a side
  cleanup if the HF dataset is still seeded from it.
- Tier-B/C exact mechanisms are deliberately left to the follow-up plan once the urgent Tier-A fix lands.

## Related
- **Investigations:** `docs/investigations/2026-06-28-xt-grid-stale-not-directionality-root-cause.md` (root cause),
  `…-directionality-root-cause.md` (superseded), `…-feedback-greenlight.md` (sign-off),
  `…-xt-gk-dzv-negative-root-cause.md` (the downstream symptom).
- **ADRs:** ADR-022 (SPADL→LTR migration that re-derived the upstream), ADR-013 (global xT grid as the xT-GK
  baseline), ADR-062 (silly-kicks 4.35.0 xT-GK — the consumer that exposed this).
- **Code:** `ingestion/guards.py` (`check_upstream_freshness`/`record_watermarks`/`find_new_ids`),
  `ingestion/expected_threat.py` (guard + writer), `analytics/expected_threat.py` (`XTGrid.validate_structural`),
  `test_guard_conformance.py` (watermark contract enforcement).
- **Implementation plan:** `docs/superpowers/plans/2026-06-28-xt-grid-staleness-and-guard-watermark-fix.md`.
