# GK Analytics — insight-first redesign of the two GK views (Distribution Value + Shot Review)

**Date:** 2026-06-21 · **Author:** Karsten S. Nielsen (with Claude) · **Status:** Draft — for
cross-session review. **Supersedes** the three-tab layout in
`docs/superpowers/specs/2026-06-11-gk-analytics-redesign-design.md` (the data architecture there —
`fct_gk_tracking_actions` / `fct_gk_tracking_stats`, provider gating, ghost-grid port, env-flag
staging — is REUSED; only the view design and the defensive thesis change).

**Approved mockups (normative for layout):**
`docs/ui-cycles/gk-redesign/mockups/offensive-v4.html` + `defensive-v4.html`
(full-document HTML + inline SVG; open in a browser. v4 reflects the review resolutions in §0 —
**signed** offensive axis + **per-provider** cohort. They supersede v3 / the `v3_tab{1,2,3}_*.png`
three-tab mockups and the v2 / approach iterations.)

## 0. Review resolutions (implementing-session critic review, 2026-06-21 — verified live)

**These resolutions OVERRIDE any conflicting earlier text below.** Each was checked against
`soccer_analytics.dev_gold`.

- **B1 (xT-GK is ~83–90% NEGATIVE — confirmed live: avg −0.019, range −0.079…+0.032).** View 1
  renders xT-GK as **signed values centered at 0** ("added / removed threat vs baseline; negative =
  conservative / risk-carrying"); the within-keeper hero is the **least-negative / best-fit** model,
  NOT "value created." Fit-ladder *shape* is unchanged (within-keeper ordering is sign-invariant).
  The offensive mockup's positive 0.028–0.061 axis is **void** and must be redrawn signed.
  Available positive supporting metrics if wanted: **`xt_gk_rav`** (+89% positive live) or
  **`gk_completion`** (avg 0.751) — but the supporting strip is **OUT of v1 by default** (verify
  `xt_gk_rav` sign live first, same caution as `xt_gk_base`; the signed primary suffices).
- **C1.** The actual-style marker is a **measured context chip** ("plays a deep, narrow block" from
  `defensive_line_x` / `team_shape_*`), NOT a 1-of-6 game-model classification. The VERDICT keys on
  the **fit-spread**, never auto-asserts "Mis-deployed <model>" from an unvalidated guess.
- **C2/C3 → PER-PROVIDER COHORT (owner-decided).** Comparisons are **within the keeper's own
  tracking provider** (provider effect confirmed live: pitch-control 0.137 GS vs 0.200 SkillCorner;
  closing 1.54 s vs ~1.8 s). This removes the need for cross-provider merge (none exists — verified)
  and cross-provider variance rollup. Goals-prevented uses **`fct_gk_shot_stopping_pooled`**'s
  precomputed `goals_prevented_ci_low/high` **per (player_key, competition, season, data_source)**.
  `canonical_player_key` is used only to dedupe display identity. (`fct_gk_shot_stopping.psxg_variance_sum`
  exists at match grain if cross-provider rollup is ever needed.)
- **D1 / read-path (CORRECTED — verified live).** `fct_goalkeeper_stats(_synced)` holds **only
  statsbomb + wyscout — NO tracking rows**; it is NOT the tracking source. Tracking goals-prevented
  is in **`fct_gk_shot_stopping` / `fct_gk_shot_stopping_pooled`** (gold, **must be synced** — add to
  `SYNCED_TABLES`). Distribution + sweeper come from `fct_gk_tracking_stats_synced`.
- **C4.** VERDICT + BIG STORY are a **pure templater with a pinned decision table** (§11a) — handles
  well-matched / mismatched / low-sample / inconclusive, not just the dramatic case.
- **D2.** Reference-band membership excludes sub-floor GKs; dispersion stat = **IQR** (robust at
  n≈10–40).
- **D3 (owner-decided).** **GS per-player DISPLAY on a public Space is allowed** — only the
  downloadable HF artifact is restricted. §9 updated accordingly.
- **D4.** Defensive-line Deep/Mid/High terciles computed **within competition** (line norms differ
  by league).
- **E.** All new computations are **pure, unit-tested functions** (reference-band stat, tercile
  classifier, style-context, goals-prevented band, verdict templater) — pinned for writing-plans.

**v2.1 correction (2026-06-21 — IDSSE goals-prevented bug, verified live):**
`fct_gk_shot_stopping_pooled` has **0 IDSSE rows** (a bug, not a gap): IDSSE rows in
`fct_gk_shot_stopping` have **NULL `season_id`**, and the pooled mart's `INNER JOIN … ON
competition_key AND season_id` drops them (`NULL=NULL` is UNKNOWN). → **Goals-prevented source for v1
is the match-grain `fct_gk_shot_stopping`** (which HAS IDSSE rows + `psxg_variance_sum`), rolled up
in-app per `(player_key, competition_key, season_id, data_source)` with NULL season handled:
`goals_prevented = Σ`, band `= ±1.96·√Σ psxg_variance_sum`, `low_sample` from `Σ shots_faced_total <
5`. **Sync `fct_gk_shot_stopping`** (not just `_pooled`). The pooled layer may be re-adopted once the
producer (a) resolves IDSSE `season_id` and (b) makes the join NULL-safe (`<=>` / LEFT JOIN). Do NOT
render "IDSSE unavailable" — the data exists at match grain. (Producer bug filed.)

**v2-review additions (2026-06-21, implementing-session N-items):**
- **N1 (self-consistency):** §3.1/§3.2/§3.4, §4.1/§4.2 and §7 rewritten to match §0 — the old
  "inferred system / mis-deployed / canonical cross-provider rollup" wording is **removed**, not just
  overridden; mockup refs repointed to **v4**.
- **N2 (offensive verdict = descriptive):** no style→model divergence branch (it would re-smuggle the
  inference C1 removed). Verdict = best-fit name + fit-spread; "system fit unverified." (§11a.)
- **N6 (defensive verdict = defined, owned rule):** the "underused sweeper" call is an explicit
  spatial-capacity rule (upper-tercile command + deep within-comp line), reasoning owned in §11a —
  NOT a formation recommendation; the mockup's "suits a high line" marker is softened to it.
- **N3 (min cohort for a band):** a reference band renders only if the keeper's provider cohort has
  **≥8 qualifying GKs** after sub-floor exclusion; below that show "provider cohort too small — no
  reference band" (the value still renders, without a band). Applies to BOTH distribution and sweeper
  families. (Live: SkillCorner distribution cohort is ~3 GKs ≥50 → will often hit this; IDSSE sweeper
  ~10 is borderline.)
- **N4 (multi-provider keeper render rule):** `canonical_player_key` dedupes the selector to one
  display name; if that keeper has rows in >1 tracking provider, render the **provider with the most
  observations** (`n_defended_actions` defensive / `n_distributions` offensive) and expose a small
  **provider sub-selector** chip. No cross-provider blending.

---

## 1. Executive summary

The implemented GK page is mechanically rich but **insight-poor**: a bump chart of tiny negative
xT-GK ranks, pass-arrow hairballs, single-shot positioning anecdotes ("6.8 m off optimum" with no
good/bad anchor). This redesign rebuilds the **two** GK views as **insight-first** screens in the
house style of the Match Summary page (KPI cards with a baked-in reference, an explicit **OUR
VERDICT**, a **★ BIG STORY**, a plain-language **DIRECTION** column, citations).

The two views are kept and organised offensive vs defensive, and both tell **one** thesis —
**"right keeper, wrong system"**:

| View | Question it answers | Big Story | Hero visual |
|---|---|---|---|
| **1 · Distribution Value** (offensive) | Which game model best fits his passing? | "Transition-fit distributor — possible fit gap (hypothesis)" | **Fit-ladder** — his **signed** xT-GK under all 6 models, within-keeper |
| **2 · Shot Review** (defensive) | Is he standing/sweeping where his system needs? | "A sweeper used as a line-keeper" | **Sweeper profile** — reachable area / pitch-control / closing time vs cohort band |

Two constraints (verified live, see §5) shape everything:

1. **No GK ranking/percentile is possible** (gated ≥20 GKs × ≥20 shots — false everywhere). Every
   cross-keeper comparison is a **value + pooled reference band**, NEVER a rank.
2. **The shot-faced sample is tiny** (285 on-target tracking shots / 80 GKs; max 14/GK, avg 3.6;
   `low_sample` is the norm). So shot-facing metrics (positioning accuracy, goals-prevented) are
   shown as **value ± uncertainty band** and never lead. The defensive view instead leads on the
   **per-action sweeper family**, which is non-null on 100% of ~150+ defended actions per GK.

---

## 2. Design principles (apply to both views)

- **Insight-first house style.** Each view = context bar + cited blurb → KPI cards (each carrying
  a reference) → **★ BIG STORY** → hero chart → DIRECTION table / honest secondary. The number
  always answers "is this good, and what do I do?".
- **Within-keeper before cross-keeper.** The headline of each view is a *within-keeper* comparison
  (his value across the 6 models; his sweeper attributes vs the line his own team plays) — robust
  at any sample size. Cross-keeper context appears only as a **pooled reference band** (mean ±
  dispersion), explicitly not a rank.
- **Honest sampling is first-class.** Show `n`. Below the show-value floor → render a band, not a
  point, and a `low_sample` pill. Never silently substitute or pad (UX standard: scale + direction
  on every value; never silently substitute data).
- **No ranking anywhere.** `ranking_enabled=false`; do not render `goals_prevented_pctile` or any
  GK leaderboard while disabled.
- **Provider scope (in-app):** `gradientsports`, `skillcorner`, `idsse`. `metrica` excluded
  (anonymised players; structurally-null shot-z). StatsBomb is absent from `fct_action_context`.
  ⚠️ **GS is usable in-app but must not be published to HF** (see §9).

**Floors (owner-set defaults; tune in implementation):**

| Metric family | Show-value floor | Below floor | Rank |
|---|---|---|---|
| Distribution fit-ladder (per-pass) | always (within-keeper) | flag `low_sample` if `n_distributions < 20` | never |
| Sweeper (per defended action) | `n_defended_actions ≥ 30` | value + wide band + `low_sample` | never |
| Ghost positioning / shot-facing | `shots_faced ≥ 5` | "n too low" placeholder / band | never |
| Goals-prevented | `shots_faced_total ≥ 5` (`low_sample` flag from `fct_gk_shot_stopping`) | value ± CI, `low_sample` | never (gated ≥20×20) |

---

## 3. View 1 — Distribution Value (offensive)

### 3.1 Insight & Big Story
Re-value the keeper's **own** distributions under each of the six pre-computed game models, rendered
**signed** (0 = neutral; negative = conservative/risk — the keeper norm; §0 B1). The
**within-keeper** spread answers "which model best fits him" — his **least-negative / net-positive**
model. The headline is **descriptive** (best-fit model + fit-spread), NOT a deployment claim: there
is no validated style→model mapping, so the spec never asserts "mis-deployed" (§0 C1/N2, §11a). Big
Story example: *"Net-positive only in transition — under a Counter model his distribution adds threat
(+0.004/dist), the one system where he's above water; conservative under the other five."*

### 3.2 Layout (per `offensive-v4.html`)
- **Context bar:** cohort (**his own provider**), keeper, **measured context chip** (e.g. "deep,
  narrow block" — observed line height/shape; `measured` pill; NOT a model name), sample
  (`n_distributions`).
- **Ranking banner:** ranking gated off; cross-keeper context = his-provider reference band only.
- **KPI cards (4):** Best-fit model (signed value); Typical / median model (signed); Fit spread
  (max−min across models); **OUR VERDICT** (descriptive per §11a — e.g. "Transition-fit"; never
  "mis-deployed").
- **★ BIG STORY** (descriptive; any system-fit read is an explicit *hypothesis*, never asserted).
- **Hero — fit-ladder (signed):** x-axis = **signed mean xT-GK per distribution** centered at 0;
  one row per model; **best-fit in amber**; his-provider cohort median as a dashed reference.
  **No "actual model" marker** — the measured context is a chip, not a ladder row (§0 C1).
- **DIRECTION table:** per model → signed xT-GK/dist, signed Δ vs his-provider median (not a rank),
  plain-language read.

### 3.3 Data — exact columns
All from **`soccer_analytics.dev_gold.fct_action_context`** (grain `match_key` + `action_id`),
domain marker **`xt_gk IS NOT NULL`** (acting GK distribution actions only — pass/goalkick) and
`player_key IS NOT NULL`:

- Six game models (each a stored counterfactual composite; NOT client-derivable from components):
  `xt_gk` (default), `xt_gk_possession`, `xt_gk_counter`, `xt_gk_direct`, `xt_gk_high_press`,
  `xt_gk_low_block`.
- Optional component/context (for an expandable PEV/DZV/RAV strip, secondary): `xt_gk_pev`
  (≈0 on unpressured restarts by construction — must caption), `xt_gk_dzv`, `xt_gk_rav`,
  `xt_gk_pressure`, `xt_gk_base` (currently 100% negative — verify before surfacing),
  `gk_completion`, `pressure_on_actor__andrienko_oval`.

**Aggregation:** the per-model means already exist in
**`fct_gk_tracking_stats`** (`dist_xt_gk_mean`, `dist_xt_gk_{possession,counter,direct,high_press,low_block}_mean`,
`n_distributions`) at **(gk_player_key, match_key)** grain — the page aggregates these across
matches to **(gk_player_key, competition_id, season_id, data_source)** for display (volume-weight by
`n_distributions`). **Values are signed and centered at 0 (§0 B1)** — mostly negative; the hero is
the *least-negative / best-fit* model. Cohort reference = **his own provider's** distribution of
each per-model mean (median ± IQR, sub-floor GKs excluded) — a signed Δ context, **not** a
rank/percentile.

### 3.4 Measured context chip (NOT a model classification — §0 C1/N2)
There is **no** team playing-style / game-model label in the data, and `xt_gk_{preset}` are pure
counterfactual re-valuations. The page therefore does **NOT** classify the side into one of the six
models. It shows a **measured descriptive chip** of the side's observed shape — e.g. "deep, narrow
block" — from **`defensive_line_x`** (line height) + **`team_shape_*`** (width/compactness) +
**`das_team`**, optionally **`fct_formation_labels`**. ⚠️ Do **NOT** use `fct_match_summary`
possession% / PPDA (StatsBomb/Wyscout-only; absent for the tracking cohort). The chip carries a
`measured` label and is **descriptive context only** — it does NOT drive a deployment verdict. The
style→model judgment that "mis-deployed" would require is unvalidated and explicitly excluded from
v1 (§11a keeps the offensive verdict descriptive).

---

## 4. View 2 — Shot Review (defensive, space-command)

### 4.1 Insight & Big Story
A keeper's defensive contribution reframed as **space command** — how much ground he covers behind
the line and how fast he reaches the box — measured on **every defended action** (robust), not the
handful of shots he faces. The "right system?" read applies the **defined spatial-capacity rule**
(§0 N6 / §11a): upper-tercile sweeping command behind a deep line = capacity structurally unused.
Big Story example: *"Upper-cohort sweeping range (152 defended actions), and his side defends deep —
spatial command unused behind a deep line."* (descriptive of the owned rule; no specific formation
is recommended.)

### 4.2 Layout (per `defensive-v4.html`)
- **Context bar:** cohort, keeper, his line (`Deep/Mid/High` + avg metres), sample (`n_defended`,
  `shots_faced`).
- **Banner:** sweeper metrics robust; shot-facing metrics thin → honest bands, never ranked.
- **KPI cards (4):** Reachable area; Pitch-control share; Closing time · 6-yd (lower=better);
  **OUR VERDICT** ("Underused sweeper").
- **★ BIG STORY.**
- **Hero — sweeper profile:** three metrics, each as a dot on **his own provider's cohort spread**
  (shaded IQR band + median tick + "better →" direction), provider labelled. Position in the spread,
  not a rank. (Per-provider per §0 — pooling all providers would partly measure tracking methodology,
  not skill.)
- **"Right defensive system?" panel:** his side's line (from avg `defensive_line_x`, within-comp
  tercile) shown alongside his sweeping-command position; when the §11a rule fires (upper-tercile
  command + deep line) the caption reads "spatial command unused behind a deep line" — **descriptive
  of the owned rule, NOT a recommendation of a specific formation** (§0 N6).
- **Honest secondary strip (`low_sample`):** ghost-positioning deviation (value ± band, n shots);
  goals-prevented (**LIVE** — value ± Poisson band + `low_sample`; thin on the tracking cohort so
  the band usually straddles 0, shown not hidden).

### 4.3 Data — exact columns
**Sweeper hero — robust, per defended action.** From `fct_action_context`
(domain `defending_gk_player_key IS NOT NULL`; verified non-null on 100% of defended actions):
`gk_pitch_control_share_weighted`, `gk_reachable_area_m2`,
`gk_closing_time_min_s__six_yard_box` (+ `__near_post`, `__far_post`; `mean` variants available).
Pre-aggregated in **`fct_gk_tracking_stats`** at (gk_player_key, match_key):
`pc_share_mean`, `reachable_area_mean_m2`, `closing_min_six_yard_mean_s`,
`closing_min_near_post_mean_s`, `closing_min_far_post_mean_s`, `n_defended_actions`,
`shots_faced`, `goals_conceded`. Page
aggregates across matches → (gk, competition, season, **data_source**). Cohort reference band =
**his own provider's** cohort GKs (median ± IQR; exclude sub-floor GKs per §0 D2). NOT pooled across
providers (provider effect confirmed live — §5).

**Actual defensive line — NEW small aggregate.** `AVG(fct_action_context.defensive_line_x)` per
(team, match) → per GK's team; map to Deep/Mid/High terciles (data-driven). Optional structure
context: `back_line_high_x`, `compactness_x`, `lateral_width`, `max_lateral_gap`, `back_n_count`.

**Shot-facing secondary — thin.** Ghost deviation: `fct_gk_tracking_stats.ghost_deviation_mean_m`
(computed in `fct_gk_tracking_actions` as actual-vs-ghost on shot rows only; `ghost_gk_x/y` are
canonical/LTR, `pre_shot_gk_x/y` are frame-oriented — reconcile via the `pre_shot_gk_distance_to_goal`
anchor, single-macro, per the 06-11 spec; segment on `ghost_gk_method`). Sample = `shots_faced`.

### 4.4 Goals-prevented (additive secondary — PRODUCER BUILT & LIVE as of 2026-06-21)
The PSxG subsystem is built, deployed and verified (**ADR-060**: 4-feature logistic —
projected/measured goalmouth crossing + distance-to-goal + shot angle; OOS **AUC 0.818 / Brier
0.153**, GroupKFold by match, n=32,698). `goals_prevented = psxg_faced − goals_conceded`
(positive = stopped more than expected). Live tables (`soccer_analytics.dev_gold`):

- ⚠️ **CORRECTION (verified live):** `fct_goalkeeper_stats(_synced)` holds **only statsbomb +
  wyscout — NO tracking rows** — so it is NOT the source for this (tracking) page, despite the
  producer handoff. Tracking goals-prevented lives in `fct_shot_psxg` (shot grain),
  `fct_gk_shot_stopping` (GK × match, has `psxg_variance_sum`), and **`fct_gk_shot_stopping_pooled`**
  (GK × competition × season × **data_source**). **`_pooled` is buggy for IDSSE (§0 v2.1 — drops
  NULL-season rows), so v1 sources the match-grain `fct_gk_shot_stopping`** instead. Both gold-only,
  NOT synced → add **`fct_gk_shot_stopping`** to `SYNCED_TABLES` in
  `src/ingestion/refresh_synced_tables.py`, sync, reapply grants.
- **Per-provider (§0 / v2.1):** roll up `fct_gk_shot_stopping` rows for the keeper's own
  `data_source` per `(player_key, competition_key, season_id)` (NULL season grouped): `goals_prevented = Σ`,
  band `= ±1.96·√Σ psxg_variance_sum`, `low_sample = Σ shots_faced_total < 5`. No cross-provider rollup.

**Display (per-provider, §0/v2.1):** show the rolled-up `goals_prevented` + its **Poisson-binomial
band** (`±1.96·√Σ psxg_variance_sum`), with a scale+direction caption ("higher = better; 0 = as
expected"), the `low_sample` flag, and shots-faced n. **Never a rank** (ranking gated off everywhere).
**On the tracking cohort this is very thin** (GS 234 / SC 37 / IDSSE 13 shots total; most GKs <5) →
the band almost always straddles 0; that inconclusiveness IS the signal — shown, not hidden.
Additive: no layout change. (The precomputed `_pooled` `ci_low/high` may be used instead once the
producer fixes the IDSSE `season_id` + makes the join NULL-safe.)

---

## 5. Verified volumes (queried `soccer_analytics.dev_gold` live, 2026-06-21)

Script: `.superpowers/brainstorm/<session>/gk_volume_check.py` (uses `DATABRICKS_*` env).

- **Sweeper family non-null on 100% of defended actions.** Avg defended actions/GK: GS **873**,
  IDSSE **843**, SkillCorner **145** (vs avg shots faced 2.8–16). GKs with ≥50 sweeper obs: GS 41,
  SC 30, IDSSE 10. → defensive hero is well-grounded.
- **Distributions/GK:** GS avg **55.8** (32 ≥50, 16 ≥100), IDSSE **35.4**, SkillCorner **9.0**
  (3 ≥50). Modest on SC, but within-keeper fit is stable at low n (same passes re-valued); absolute
  values carry noise → bands.
- **Shot-faced corpus (whole tracking cohort):** 285 on-target shots / 80 GKs; max 14, avg 3.6;
  0 GKs ≥30, 4 ≥10, 25 ≥5 → ranking impossible; `low_sample` is the default.
- **PSxG coverage (producer v2, live):** StatsBomb 32,698 shots (rich — e.g. Valdés 218 PSxG faced
  / 255 matches) + tracking GS 234 / SkillCorner 37 / IDSSE 13. **StatsBomb is rich for
  shot-stopping but has NO tracking distribution/sweeper families** (absent from
  `fct_action_context`); the tracking cohort has the families but thin shot-stopping. So on THIS
  (tracking) page goals-prevented stays a thin, honest secondary — StatsBomb's rich shot-stopping
  belongs to a separate StatsBomb-cohort view, not here. (v2 also lowered values vs the stale v1:
  Valdés +52→+29.9, ter Stegen +46→+26.5 — any old figures are void.)

---

## 6. Data architecture summary

**No new GK mart family is required for v1** — the defensive hero and offensive fit-ladder are
buildable on the **existing live** `fct_action_context` + `fct_gk_tracking_stats` (both in
`soccer_analytics.dev_gold`). Additions are small:

| Item | Type | Source | Status |
|---|---|---|---|
| 6 game-model means, per-GK | consume | `fct_gk_tracking_stats.dist_xt_gk_*_mean` | LIVE |
| Sweeper means + `n_defended_actions` | consume | `fct_gk_tracking_stats` (`pc_share_mean`, `reachable_area_mean_m2`, `closing_min_*_mean_s`) | LIVE |
| Ghost deviation + `shots_faced` | consume | `fct_gk_tracking_stats.ghost_deviation_mean_m`, `shots_faced` | LIVE |
| Pooled cohort reference bands (per metric) | NEW (small) | app-side or tiny stats view over the above | TODO |
| Avg `defensive_line_x` per GK-team | NEW (small) | aggregate `fct_action_context.defensive_line_x` | TODO |
| Offensive actual-style inference | NEW (analytical) | `defensive_line_x`, `team_shape_*`, `das_team`, `fct_formation_labels` | TODO |
| Goals-prevented + band (per provider) | consume + roll up | **`fct_gk_shot_stopping`** (match grain — has `psxg_variance_sum`; `_pooled` drops IDSSE, §0 v2.1). Σ per (player_key, comp, season, **data_source**); band = ±1.96·√Σ variance | LIVE gold — **NOT synced; add `fct_gk_shot_stopping` to `SYNCED_TABLES`** |
| ⚠️ `fct_goalkeeper_stats_synced` | NOT used here | statsbomb + wyscout only — **no tracking rows** (verified) | n/a for this page |
| Display identity dedup | consume | `canonical_player_key` / `canonical_player_id` (`dim_players`) | LIVE — dedup only; no cross-provider rollup (§0) |

Outcome/minutes (if needed): goals-conceded-on-shots is in the §10 contract; per-90 minutes (if
used) come from `fct_player_stats`. `fct_player_percentiles` is **outfield-only** and is **not**
used (no GK percentiles; ranking is off).

---

## 7. Dependencies & cross-session

**UPDATE 2026-06-21 — the PSxG producer is BUILT & DEPLOYED (see §4.4); the feedback below is now
mostly RESOLVED by the producer.** Recorded for traceability:

⚠️ **NEW FLAG TO PRODUCER (verified live):** `fct_goalkeeper_stats(_synced)` has **no tracking rows**
(statsbomb + wyscout only) — the handoff's "use `fct_goalkeeper_stats_synced` for the per-match
number today" does NOT hold for the tracking cohort. We will consume `fct_gk_shot_stopping_pooled`;
please **sync it** (add to `SYNCED_TABLES`) and confirm it is the intended tracking source.

1. ✅ **GS-not-to-HF — RESOLVED.** GS shots live in private `luxury-lakehouse/psxg-shots-restricted`
   (org-only); public `psxg-shots` carries StatsBomb/SkillCorner/IDSSE only. Honour this split if
   the app ever links source data.
2. **De-prioritised vs the page.** Defensive view leads on the live sweeper family; goals-prevented
   is additive. The page does **not** block on `fct_gk_shot_stopping`.
3. **Honesty over coverage.** Keep the on-target gate strict; we render value ± CI + `low_sample` +
   `coverage_pct` and prefer a trustworthy thin number to a padded one.
4. ✅ **Consumption grain — pooled layer exists** (`fct_gk_shot_stopping_pooled`, GK × comp ×
   season). Remaining on OUR side: sync it (add to `SYNCED_TABLES`) to read the band in-app.
5. **Ranking stays off.** `ranking_enabled=false` / NULL `pctile` is correct; don't build the
   ranking path for us.
6. **StatsBomb path** isn't surfaced here (SB absent from AC; tracking cohort only).

**GK-page consumption / data-layer tasks (this workstream):** sync **`fct_gk_shot_stopping`**
(match grain; add to `SYNCED_TABLES` + grants) and roll up the **per-provider** goals-prevented band
in-app (§0 v2.1 — `_pooled` drops IDSSE); **per-provider**
reference bands (median ± IQR, ≥8-GK min per §0 N3) for the sweeper/distribution metrics; avg
`defensive_line_x` per GK-team (within-comp terciles) for the measured context chip; the verdict
templater (§11a). `canonical_player_key` is **display dedup + the multi-provider render rule (§0
N4) only — NO cross-provider rollup**. `fct_goalkeeper_stats_synced` is **NOT used** (SB/WS only —
§0 D1). Caption-only caveat IF StatsBomb goals-prevented is ever surfaced (not on this tracking
page): SB defending-GK is lineup-attributed (sub mis-attribution; ~2.8% NULL GK); tracking providers
are 0% NULL.

---

## 8. Relationship to the 2026-06-11 spec

> **⚑ DEPLOYMENT SUPERSEDED — owner decision 2026-06-21:** the 06-11 "side-by-side, env-flag,
> prod-bit-identical, separate-cutover" approach is **REPLACED**. The new page **replaces BOTH**
> legacy GK pages this cycle — the event-based `Goalkeeper-Analytics` (`pages/goalkeeper.py` …) AND
> the staging-gated `Goalkeeper-Tracking` (`gk_tracking.py`) — takes the `Goalkeeper-Analytics`
> route, is registered **UNCONDITIONALLY**, and **`LL_GK_TRACKING_PAGE` is retired**. Owner accepts
> that the prod GK page becomes **tracking-cohort-only** (GS/SC/IDSSE); StatsBomb/Wyscout keepers no
> longer have a GK page. ⚠️ **The page MUST state its tracking-only scope** so users of non-tracking
> competitions aren't silently confused by an absent keeper. With no in-code kill-switch, **staging
> sign-off is the mandatory merge gate**; recommend the legacy-page DELETION land as its own PR after
> the new page is validated on staging (reduces blast radius — owner's call on PR granularity).

- **Reused:** the `fct_gk_tracking_actions` / `fct_gk_tracking_stats` marts (now LIVE in dev_gold);
  provider gating constant (`GK_TRACKING_PROVIDERS`); ghost-grid hexagonal port
  (`StoredSpreadProvider` default, `ModelGridProvider` fast-follow); citations & glossary; read-side
  contract reconciliation tests. (Env-flag staging is **retired** — see the deployment note above.)
- **Changed:** the three tabs (Distribution Value / Defensive Positioning / Shot-Stopping Geometry)
  collapse into **two insight-first views**; the bump chart → within-keeper **fit-ladder**; the
  shot-tercile positioning hero → **sweeper profile**; **all percentile/rank UI is removed**; the
  pressure-split / preset-bump / radar charts are dropped. The pre-shot cone scene and pass-arrow
  maps are dropped (no insight at this volume).

---

## 9. Provider scope & publication constraints

- **In-app cohort:** `gradientsports` + `skillcorner` + `idsse`. `metrica` excluded (anonymised
  players violate "real names only"; structurally-null shot-z). StatsBomb absent from AC.
- **GS publication (owner-confirmed 2026-06-21):** the **only** restriction on GradientSports is
  that it must not be published to HF as a **durable downloadable artifact**. **Showing GS
  per-player numbers on UI pages — including a public Space — is fine** (no private/org-visibility
  requirement; this **supersedes** 06-11 security note S1). The PSxG producer already honours the
  artifact split: GS shots are in private `luxury-lakehouse/psxg-shots-restricted`; public
  `psxg-shots` is StatsBomb/SkillCorner/IDSSE.

---

## 10. UX standards & citations

- Every value carries scale + direction (help text / reference band / verbal label). Real names
  only (dim-joined display names; no raw ids). Provenance captioned (`pitch_control_method`,
  `ghost_gk_method`). `inferred` and `low_sample` always labelled; `goals_prevented` carries
  "higher = better; 0 = as expected". Never silently substitute data.
- Citations (page + NOTICE): Eyestone **xT-GK**; ghost-GK conditional-density lineage (silly-kicks
  model card); GK influence / pitch control (Spearman lineage); **PSxG model = ADR-060** (4-feature
  logistic; lineage Butcher et al. 2025 xGOT + goalmouth geometry Anzer & Bauer 2021 / TF-48; OOS
  AUC 0.818). Source shots: HF `luxury-lakehouse/psxg-shots` (public: StatsBomb/SkillCorner/IDSSE) +
  `psxg-shots-restricted` (private: GradientSports — do not link publicly).

---

## 11. Open items / decisions made

- **Offensive xT-GK rendered signed, centered at 0** (§0 B1); hero = least-negative / best-fit model.
- **Offensive actual-style = MEASURED context chip** (deep/narrow block), NOT a model classification
  (§0 C1); verdict keys on fit-spread, never auto-asserts "mis-deployed <model>".
- **Defensive view = space-command / sweeper profile** (owner-approved); shot-facing demoted to
  honest secondary.
- **Per-provider cohort** comparisons; **no ranking / no percentiles** anywhere (data-gated).
- **Cross-provider rollup NOT done**; `canonical_player_key` for display-identity dedup only (§0 C2).
- **Goals-prevented LIVE** from `fct_gk_shot_stopping_pooled` (per provider; must be synced) — thin,
  band straddles 0 for most, shown not hidden.
- **Dispersion = IQR**; defensive-line Deep/Mid/High terciles **within competition** (§0 D2/D4).
- Open (implementation): exact floor values (§2 defaults); tercile cut points; whether a positive
  supporting strip (`xt_gk_rav` / `gk_completion`) ships in v1; the measured style-chip phrasing set.

## 11a. VERDICT / BIG STORY decision table (C4 — pure templater, unit-tested)

Both the OUR VERDICT card and the ★ BIG STORY are emitted by a **pure function**; every branch
unit-tested, including the inconclusive cases (no false "mis-deployed" for a well-deployed or low-n
keeper).

**Offensive — DESCRIPTIVE only (N2: no style→model inference).** Inputs: `best_fit_model`,
`fit_spread` (max−min of the 6 signed means), `n_distributions`. The `measured style_chip` is shown
as context but does **NOT** enter the verdict (deciding "diverges from style" would require an
unvalidated style→model map — excluded).

| Condition | VERDICT | BIG STORY tone |
|---|---|---|
| `n_distributions < 20` | "Indicative only — small sample" | hedge; show shape, make no claim |
| `fit_spread` below threshold | "System-agnostic distributor" | even across models |
| `fit_spread` large | "<best_fit>-fit" (e.g. "Transition-fit") | "Strongest under <best_fit>; system fit unverified" — best-fit named, **no deployment claim** |

**Defensive — DEFINED, OWNED spatial-capacity rule (N6).** Inputs: sweeping-command position in his
provider cohort `{upper,mid,lower}` (a composite of reachable-area + pitch-control + closing-time),
avg line within-comp tercile `{deep,mid,high}`, `n_defended_actions`. **Reasoning we own:** a
sweeper's value IS space covered behind the line; a deep line structurally reduces that space, so
upper-command + deep-line = capacity unused. This is a spatial-capacity statement, **not** a
tactical-formation recommendation.

| Condition | VERDICT |
|---|---|
| `n_defended_actions < 30` | "Indicative only — small sample" |
| command upper + line deep | "Underused sweeper — command unused behind a deep line" |
| command upper + line high | "Well-deployed sweeper" |
| command lower | "Line-keeper profile" |
| otherwise | "Typical box-keeper" |

Goals-prevented is always a **separate sub-line** with its band; when the band straddles 0 →
"shot-stopping inconclusive at this n" (never folded into the headline verdict).

---

## 12. Next step

Per the brainstorming → writing-plans flow: on owner approval of this spec, hand to the
**writing-plans** skill to produce the implementation plan (app modules `gkt_*`, the two new small
data aggregates, the reference-band computation, and the goals-prevented consumption stub).
