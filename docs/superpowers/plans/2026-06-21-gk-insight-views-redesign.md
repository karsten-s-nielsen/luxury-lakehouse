# GK Analytics insight-views redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the staging-gated Goalkeeper page with a two-view, insight-first design (Distribution Value + Shot Review) in the Match-Summary house style, built on existing live tracking marts + a robust in-app goals-prevented rollup.

**Architecture:** Pure, unit-tested domain functions in `hf_taipy_app/src/services/gk_insight.py` (reference-band, terciles, measured style-chip, verdict templater, goals-prevented Poisson band) following the existing `services/ghost_grid.py` port pattern; a thin SQL/query layer; a thin Taipy state adapter (`gka_` prefix); a declarative `PageConfig` with two `SubView`s; one new synced table (`fct_gk_shot_stopping`). No new dbt mart family.

**Tech Stack:** Python 3.10, Taipy GUI, Plotly, pandas/numpy, Lakebase (Postgres synced tables), Databricks SQL (`uv run`), pytest.

**Spec:** `docs/superpowers/specs/2026-06-21-gk-insight-views-redesign-design.md` (§-tags below reference it). Mockups (normative): `docs/ui-cycles/gk-redesign/mockups/{offensive,defensive}-v4.html`.

---

## Decisions locked before tasks (from spec §0 + handoff; all verified live 2026-06-21)

- **xT-GK is SIGNED** (~83–90% negative live; `dist_xt_gk_mean` avg −0.017, range −0.079…+0.032). Hero = least-negative/best-fit; axis centered at 0. No positive-only axis. RAV/completion supporting strip OUT of v1.
- **PER-PROVIDER cohort only** (provider effect real). No cross-provider pooling/merge/rollup. `canonical_player_key` = display-identity dedup + the multi-provider render rule (render the provider with the most observations + a provider sub-selector chip).
- **Goals-prevented source = `fct_gk_shot_stopping_pooled` read DIRECTLY** (Plan-review-v3 reversal). The mart already computes the SUM rollup, `goals_prevented`, the Poisson band (`goals_prevented_ci_low/high`), `low_sample`, and ranking deferral. Its IDSSE-dropping `INNER JOIN … ON season_id` is fixed in-PR (Task 1.0, one-line NULL-safe `<=>`) + guarded by a dbt singular test. The app does **no** in-app rollup (no second source of truth; no `pandas.groupby(dropna)` IDSSE re-drop). `goalkeeper_enabled: true` already set.
- **`fct_goalkeeper_stats_synced` is StatsBomb/Wyscout only (no tracking rows) — NOT used here.**
- **No ranking/percentiles anywhere** (`ranking_enabled` false everywhere).
- **Reference band:** dispersion = IQR; render only if the provider cohort has **≥8 qualifying GKs** after sub-floor exclusion, else show the value with "provider cohort too small — no reference band".
- **Defensive-line Deep/Mid/High terciles computed WITHIN competition.**
- **Verdicts are a pure templater** (§11a): offensive = descriptive (no style→model inference); defensive = the owned spatial-capacity rule.
- **GS per-player display on a public Space is allowed** (only the downloadable HF artifact is restricted) — no Space-visibility gate needed.

**Scope (owner-decided 2026-06-21 — both GK pages replaced this cycle):** the new design REPLACES **both** existing GK pages:
1. the **legacy event-based `Goalkeeper-Analytics`** page (`pages/goalkeeper.py`, `state/goalkeeper.py`, `queries/goalkeepers.py`, `gk_*` prefix) — **completely removed**; the new page **takes the `Goalkeeper-Analytics` route**.
2. the **staging-gated `Goalkeeper-Tracking`** page (`{pages,state,queries}/gk_tracking.py`, `gkt_*`) — removed.

The new page uses a fresh **`gka_`** prefix, displayed title "Goalkeeper Analytics", and is **registered UNCONDITIONALLY** on the `Goalkeeper-Analytics` route (it replaces an always-on prod page). The **`LL_GK_TRACKING_PAGE` flag is retired** — staging verification is via the feature branch + staging Space, not an in-code flag (this deviates from spec §8's flag reuse, which existed only to keep prod bit-identical while the tracking page was experimental; that rationale is gone now that we are deliberately replacing the prod page). New page is a **SubView page** (per `test_tier_a_canon.py` canonical map + the shared sub-view scope mechanism).

**Verified column inventory (live):**
- `fct_gk_tracking_stats_synced` (22 cols): `gk_player_key, match_key, data_source, n_distributions, dist_xt_gk_mean, dist_xt_gk_{possession,counter,direct,high_press,low_block}_mean, dist_completion_mean, dist_pressure_mean, n_defended_actions, shots_faced, goals_conceded, ghost_deviation_mean_m, closing_min_{six_yard,near_post,far_post}_mean_s, reachable_area_mean_m2, pc_share_mean, gk_match_stat_id`. (No `competition_key`/`season_id` → join `dim_matches_synced` for those.)
- `fct_action_context_synced` (147 cols) carries: `team_key, player_key, defending_gk_player_key, defensive_line_x, back_line_high_x, compactness_x, lateral_width, max_lateral_gap, back_n_count, team_shape_*` (14), `das_team`, `match_key, data_source, xt_gk, xt_gk_{possession,counter,direct,high_press,low_block}`.
- `fct_gk_shot_stopping` (gold, NOT synced yet): `gk_shot_stopping_id, player_key, match_key, competition_key, season_id, data_source, shots_faced, shots_faced_total, goals_conceded_on_shots, psxg_faced, goals_prevented, psxg_variance_sum, low_sample, …`. Provider rows: statsbomb 6560 / GS 112 / SC 27 / **IDSSE 6 (all season_id NULL)**.
- `dim_players` has `canonical_player_key` (bigint, "xref-resolved canonical pointer (SB>WS>IDSSE), or self").

---

## Review-v2 dispositions (2026-06-21 — other-session review; these OVERRIDE conflicting task text below)

- **B1(a) — ACCEPTED.** Add a visible **tracking-cohort-scope note** to the page (blurb + empty-state): "Tracking-data cohort only — GradientSports, SkillCorner, IDSSE. StatsBomb/Wyscout keepers do not appear here." Assert its presence in the e2e (Task 5.1).
- **B1(b) — REJECTED (owner: single cycle/commit/PR, no follow-up).** Legacy deletion (Task 4.5) stays in THIS PR; do not split. Mitigation for the missing kill-switch is the mandatory Phase-5 staging sign-off before the merge.
- **B2 — ACCEPTED (BLOCKER fix).** The defensive-line aggregate must key on **`defending_gk_player_key`**, never `team_key` (which is the in-possession/attacking team). Fixed by the new mart in Task 1.1b (grain includes `defending_gk_player_key`) — "avg `defensive_line_x` over rows where keeper X is the DEFENDING GK". Removes the unspecified keeper→team join entirely.
- **S1 — ACCEPTED.** Cohort band unit == displayed-keeper unit == **one value per GK**. New pure `cohort_values()` (Task 2.1b) volume-weights each GK's per-(comp,season) rows to a single value, **drops sub-floor GKs (total weight < floor) BEFORE** the ≥8-cohort gate, then feeds `reference_band()`. No multi-season double-count.
- **S2 — RESOLVED → provider-first (verified: 0 of 206 canonical keepers span >1 tracking provider).** Keep the user **Provider dropdown** (Task 4.3) → Keeper-within-provider; **DELETE `_pick_provider_for_keeper` and the whole N4 auto-pick/sub-selector path** (dead — no keeper is multi-provider). `canonical_player_key` not needed for dedup (no dupes); display names from `dim_players.player_display_name`. (Data-backed deviation from spec N4.)
- **S3 — ACCEPTED.** Replace the live `AVG(...)` scan over `fct_action_context_synced` (147 cols / ~120K rows) with a **small precomputed dbt mart** `fct_gk_defensive_line` (Task 1.1b), synced. The page reads the tiny per-GK aggregate, not the fact. (Also satisfies the 06-11 EXPLAIN/index-scan requirement: the page query is now a PK-scan on a small table.)
- **S4 — ACCEPTED.** Add a **read-side contract reconciliation test** (Task 1.3): assert `_DIST_MODEL_COLS`, `_SWEEP_COLS`, and the goals-prevented column set are each ⊆ the live mart columns (A3/A4 pattern — a producer rename fails CI, not the Space).
- **M1 — NOTED.** `SyncedTableConfig.scheduling_policy` default IS `"SNAPSHOT"` (verified in `refresh_synced_tables.py`), so Task 1.1 Step 3 omitting it is correct; the test assertion holds.
- **M2 — ACCEPTED.** Fit-ladder must NOT encode sign by red/green alone (red-green CVD hazard). Use a **ColorBrewer-safe diverging palette (orange↔blue)** + retain the prominent 0-line and bar position as redundant cues. Add **chart-choice-audit + cognitive-interface-audit** to Phase 5 (Task 5.3).
- **M3 — RESOLVED (verified key-space match).** `gk_player_key` == `fct_gk_shot_stopping.player_key` (same surrogate). Band joins by that key; keepers with no shot-stopping row → "no shots faced" (graceful). Pre-flight P-5 + e2e assert the band is the SELECTED keeper's.
- **M4 — ACCEPTED.** `offensive_verdict` `spread_threshold` is calibrated as the **median per-keeper `fit_spread` across the provider cohort** (computed in Task 5.2 against live data and frozen as the constant); documented at the constant's definition.
- **Task 4.2 contradiction — FIXED.** SubView layout with `Metric` cards only (no `StatCard`/dashboard). `fit_ladder` consumes a pure tested **column→model-name map** `_MODEL_LABEL` (Task 2.6). Chart tests assert the **0-line presence + per-sign palette** so a positive-only-axis regression fails (Task 3.1).

## Plan-review-v3 dispositions (2026-06-22 — other-session review; these OVERRIDE conflicting task text below)

- **🔴 BLOCKER — ACCEPTED (reverse the goals-prevented route-around).** Verified against
  `fct_gk_shot_stopping_pooled.sql`: it already computes the SUM rollup (L30-43), `goals_prevented` (L63), the
  Poisson band `±1.96·√psxg_variance_sum` → `goals_prevented_ci_low/high` (L83-84), `low_sample` (L85), and ranking
  deferral (L88-94). The IDSSE drop is the one line at **L67-68** (`inner join cohort … on p.season_id = c.season_id`;
  NULL≠NULL). `goalkeeper_enabled: true` is already set (`dbt_project.yml:98`), so the mart is built. **New plan:**
  fix the join, **read the pooled mart directly**, and **delete the in-app rollup** (Task 2.5 `goals_prevented_band`,
  the match-grain sync, and the pandas grouping). This respects the dbt/presentation layer boundary (CLAUDE.md) and
  removes the second source of truth.
  - **New Task 1.0 (producer-side, in this PR): fix the mart + regression test.** In
    `fct_gk_shot_stopping_pooled.sql` change L67-68 to a NULL-safe join: `on p.competition_key = c.competition_key
    and p.season_id <=> c.season_id` (Spark `<=>` = NULL-safe equals). Add a dbt **singular test**
    `dbt_project/tests/assert_psxg_pooled_keeps_idsse.sql` (config `enabled=var('goalkeeper_enabled', false)`):
    `select 1 from {{ ref('fct_gk_shot_stopping_pooled') }} where data_source='idsse' having count(*) = 0` (fails if
    IDSSE rows vanish). `dbt build --select fct_gk_shot_stopping_pooled` + run the test. **CI reality (v4):** this runs
    in the **daily** `dbt-live-ci.yml` (≤24h post-merge), NOT at PR time (`dbt-ci.yml` is parse-only). The actual
    merge-time guard is the pure SQL text-assertion unit test (Task 1.0 Step 6). Recommended-but-separate (flag to producer, not gating
    this page): resolve IDSSE `season_id` upstream in `fct_gk_shot_stopping` so the NULL bucket disappears for all
    consumers.
  - **Task 1.1 → sync `fct_gk_shot_stopping_pooled`** (the per-(player,comp,season,data_source) mart with the band
    precomputed), NOT the match-grain `fct_gk_shot_stopping`.
  - **Task 1.2 `build_goals_prevented_sql` → `SELECT` the pre-aggregated pooled rows** for the provider (no
    `LIMIT`-on-a-SUM footgun; the result set is intrinsically small — one row per keeper×comp×season). Reads
    `goals_prevented, goals_prevented_ci_low, goals_prevented_ci_high, low_sample, shots_faced_total`.
  - **DELETE Task 2.5** (`goals_prevented_band`) and any in-app SUM/groupby. Keep only a thin display formatter
    (value + "± (ci_high−value)" + caption). No `pandas.groupby` on `season_id` → the `dropna=True` IDSSE re-drop
    risk is eliminated by construction.
- **🔴 pandas `dropna` re-drop — MOOTED** by reading the pre-aggregated mart (no in-app season grouping remains). (If any residual groupby is added later, it MUST pass `dropna=False` + a None-season unit test.)
- **🟠 CI guards don't run — ACCEPTED.** `conftest.py:25` autouse-sets `LAKEBASE_HOST="test-host"`, defeating the
  `if not LAKEBASE_HOST: skip` predicate. **Fixes:** (1) the **merge-time** IDSSE-fix guard is the pure SQL
  text-assertion unit test (Task 1.0 Step 6, runs in python-ci); the dbt singular test is a **daily-live** automated
  guard (≤24h post-merge), and the app-side contract/property tests are **manual operator** gates — state this
  honestly, none of the three blocks the PR; (2) change the live-test skip predicate to detect a REAL host — `_HOST = os.environ.get("LAKEBASE_HOST","");
  if not _HOST or _HOST == "test-host" or "example" in _HOST: pytest.skip(...)`; (3) state honestly in the plan that
  the app-side contract (1.3) + property test (5.1) are **operator/live-run gates, not CI gates** — CI coverage is the
  pure unit tests + the dbt singular test.
- **🟠 LIMIT-on-SUM — MOOTED** (pooled mart is pre-aggregated; no LIMIT on summed rows). The keeper-LOV/stat queries keep their `LIMIT` (bounded small result sets, not summed).
- **🟠 Big-bang no kill-switch — ACCEPTED w/ mitigation, fast-follow REJECTED (owner: one PR, no follow-up).**
  Mitigation kept: **Task 4.5 (legacy deletion) is the LAST commit** on the branch, so a single `git revert` of that
  one commit restores the legacy page cleanly without unwinding the new page. Staging Space hits the same Lakebase path
  as prod (already true). No separate fast-follow PR.
- **🟡 `spread_threshold` — ACCEPTED, make it DYNAMIC.** Do not freeze an empirical constant (staleness class).
  Compute it at render time as the **median per-keeper `fit_spread` across the in-memory provider cohort** (already
  fetched). `offensive_verdict` takes `spread_threshold` as a parameter (unchanged signature); the state layer derives
  it from the cohort each refresh. Delete the Task-5.2 calibration step (M4).
- **🟡 `fct_gk_defensive_line` B2 keying — ACCEPTED.** Add an ADR-018-style singular test
  `assert_gk_defensive_line_resolves_defending_gk.sql`: every `gk_player_key` in the mart resolves against
  `dim_players.player_key` (locks the defending-GK keying). Fix the Task-1.1b Step-3 copy-paste: the synced PK is
  `("gk_defensive_line_id",)` (add `gk_defensive_line_id = generate_surrogate_key([...])` to the model).
- **🟡 "e2e golden" → rename "property/smoke test"** (Task 5.1 asserts invariants, not frozen fixtures).
- **🟡 AI governance — ACCEPTED (one-line check).** Add a pre-flight: confirm the page surfaces no NEW evaluative model
  (PSxG already governed in its own cycle; xT-GK/sweeper are existing) → no `AI_GOVERNANCE.md`/model-card obligation.
  Record the check; if it trips, add the governance task.
- **🟡 Drop dead `canonical_player_key`** from the keeper-LOV SQL (S2 concluded no multi-provider keeper; dedup unused).
- **🟡 IDSSE never clears the ≥8-cohort band gate (6 GKs) — OWNER ACK (open item).** IDSSE keepers will show
  context-free values ("provider cohort too small — no band") indefinitely at current volume. Honest, but a thin
  whole-provider experience. Flagged for owner acceptance, not silently shipped.

---

## Conventions (apply to every task)

- **Test-first.** Each task writes the failing test first, watch it fail, implement, watch it pass.
- **App tests** live as `hf_taipy_app/src/test_*.py` and run with `uv run --project .. --extra taipy-app pytest src/test_X.py` **from the `hf_taipy_app/` directory** (plotly/state need the taipy-app extra; `hf_taipy_app/src/conftest.py` autouse-injects dummy `LAKEBASE_*`). Pure-only tests (no plotly import) also run from repo root with `uv run --extra taipy-app pytest hf_taipy_app/src/test_X.py`.
- **`pytest | tail` lies** — its exit code is tail's. Verify via `grep -c "FAILED\|ERROR"` over the full log, never the pipeline exit.
- **Local gate before "done":** `uv run ruff check hf_taipy_app/src/...`, `uv run ruff format --check ...`, `uv run pyright hf_taipy_app/src` (CI checks the app tree too), the task's pytest.
- **Commit boundaries:** one logical task per commit; **NO commits/PRs without explicit user approval** (CLAUDE.md). The plan author proposes commits; the user runs them.
- **Pure functions never import Taipy/Spark/DB.** `services/gk_insight.py` imports only stdlib + numpy. State (`state/gk_analytics.py`) is the only adapter that touches `state`/queries.
- **Done-when** per task = test green + local gate clean + the acceptance bullet satisfied.
- **No `pytest-benchmark` gate here — intentional (optimization-audit #2).** CLAUDE.md mandates benchmarks for *tracking-scale hot paths*; every function in this feature operates on tiny in-memory data (cohort ≤ ~40 GKs, fit-ladder = 6 items, goals-prevented = a few match rows) and every query hits small tables (`fct_gk_tracking_stats` = 254 rows; tracking slice of `fct_gk_shot_stopping` ≈ 145). These are not hot paths — unit tests + the e2e golden are the correct coverage. Do not add benchmark wrappers (they'd be false signal). The user-facing cost is the L2 query layer, which is sub-ms on these tables.

## Pre-flight (verify, don't assume — run once before Phase 1)

- [ ] **P-1.** Confirm `fct_action_context_synced` has rows for all three providers and `defensive_line_x` non-null density:
  `uv run --extra sdk python -c "..."` selecting `data_source, COUNT(*), COUNT(defensive_line_x)` grouped — expect GS/SC/IDSSE present, `defensive_line_x` mostly non-null on defended actions. If a provider is sparse, note it (the style-chip degrades to "context unavailable", not an error).
- [ ] **P-2.** Confirm `fct_gk_tracking_stats_synced.match_key` joins to `dim_matches_synced` for `competition_key`/`season_id` on the tracking cohort (sample 20 rows; expect non-null competition_key, season_id may be NULL for IDSSE — handled).
- [ ] **P-3.** Re-confirm `fct_gk_shot_stopping` IDSSE rows = 6, all `season_id` NULL, `psxg_variance_sum` non-null (the rollup's variance term). (Already verified 2026-06-21; re-run if data changed.)
- [ ] **P-6. AI-governance check.** Confirm the page surfaces **no NEW evaluative ML model**: PSxG/goals-prevented is already governed in its own cycle (ADR-060 + model card), and xT-GK / sweeper / ghost are existing methodologies. So no new `AI_GOVERNANCE.md` §5 entry or model card is required (`uv run pytest src/tests/test_ai_governance_md.py` should stay green untouched). If this assumption is wrong (e.g. a new workflow card is added), add the governance task before merge.
- [ ] **P-5. (M3) Confirm key-space match** before relying on the band join: `gk_player_key` (tracking_stats) and `fct_gk_shot_stopping.player_key` are the same surrogate (verified 2026-06-21: 73/80 shot-stopping GKs join; the rest have no tracking-stats row → render "no shots faced", not an error). Re-confirm if data changed.
- [ ] **P-4. Enumerate legacy-page reverse-deps before deletion (Chesterton's fence).** Known touch-points to handle in Task 4.5: `main.py:21-22` (import `pages.goalkeeper`), `main.py:54` (`from state.goalkeeper import *`), `main.py:118` (`PageEntry("Goalkeeper-Analytics", goalkeeper_config, goalkeeper_page)`); `test_tier_a_canon.py:13,30` (imports `pages.goalkeeper` into a canonical page-config map keyed `"Goalkeeper-Analytics"`); `filters.py:210 search_goalkeepers` (legacy keeper search); `state/shared.py:215` (loading message keyed `"Goalkeeper-Analytics"`). Run `rg -n "pages\.goalkeeper|state\.goalkeeper|queries\.goalkeepers|search_goalkeepers|\bgk_selected_player\b|_GK_PAGES" hf_taipy_app/src` and confirm the set is exactly these files before deleting — if anything else imports `state/goalkeeper.py` (e.g. shared palette constants), inline the needed constant into the new module rather than leaving a dangling import.

---

## Phase 0 — Cleanup / rollback of temporary GK work

> The working tree carries my uncommitted Competition→Game→Keeper Shot-Review cascade on top of merged `main`, plus the independent `dim_competitions_synced` re-key. The redesign replaces the page, so the cascade UI is superseded; the synced re-key is a generic correctness fix and is KEPT.

### Task 0.1 — Preserve the independent synced-table fix, discard the superseded cascade UI

**Files:**
- Keep (already modified): `src/ingestion/refresh_synced_tables.py:244` (`dim_competitions_synced` keyed on `competition_key`).
- Discard working-tree changes: `hf_taipy_app/src/{pages,state,queries}/gk_tracking.py`, `hf_taipy_app/src/template.py` (the gkt cascade widgets), `hf_taipy_app/src/test_gk_tracking_queries.py`, `hf_taipy_app/src/test_gk_tracking_state.py`, `hf_taipy_app/src/page_template.py`, `.gitignore`.

- [ ] **Step 1: Snapshot the diff for safety**

```bash
git stash push -m "gkt-cascade-superseded-2026-06-21" -- \
  hf_taipy_app/src/pages/gk_tracking.py hf_taipy_app/src/state/gk_tracking.py \
  hf_taipy_app/src/queries/gk_tracking.py hf_taipy_app/src/template.py \
  hf_taipy_app/src/test_gk_tracking_queries.py hf_taipy_app/src/test_gk_tracking_state.py \
  hf_taipy_app/src/page_template.py .gitignore
```
Expected: a stash entry is created; the cascade files return to the merged-`main` baseline. (Recoverable via `git stash show -p stash@{0}` if any cascade helper turns out reusable.)

- [ ] **Step 2: Verify only the intended change remains staged-for-commit**

Run: `git status --short`
Expected: `M src/ingestion/refresh_synced_tables.py` and untracked `docs/...` only; the 8 cascade files no longer listed as modified.

- [ ] **Step 3: Confirm the re-key is the sole functional code change + lint it**

Run: `git --no-pager diff src/ingestion/refresh_synced_tables.py` then `uv run ruff check src/ingestion/refresh_synced_tables.py`
Expected: one-line PK change `("competition_id",)` → `("competition_key",)`; ruff clean.

- [ ] **Step 4: Commit the independent fix (await user approval)**

```bash
git add src/ingestion/refresh_synced_tables.py
git commit -m "fix(synced): key dim_competitions_synced on competition_key (Kimball surrogate)

Legacy competition_id is NULL for all tracking providers, so the synced
dim dropped every tracking competition. Re-key on competition_key to match
dim_matches/players/teams. Table already recreated in dev Lakebase.
Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Acceptance: working tree clean except the deprecated `gk_tracking` page files (removed in Task 4.6) and untracked docs; the synced re-key committed once.

> Note: the live `dim_competitions_synced` table was already recreated in dev Lakebase this session; this commit only lands the code so the daily `lakebase-grants.yml` / future rebuilds stay consistent.

---

## Phase 1 — Data layer

### Task 1.0 — Fix the pooled mart's IDSSE-dropping join + regression test (BLOCKER, producer-side)

**Files:**
- Modify: `dbt_project/models/marts/fct_gk_shot_stopping_pooled.sql` (L67-68)
- Create: `dbt_project/tests/assert_psxg_pooled_keeps_idsse.sql`

- [ ] **Step 1: NULL-safe the join.** Change L67-68 from `on p.competition_key = c.competition_key and p.season_id = c.season_id` to:

```sql
    inner join cohort c
        on p.competition_key = c.competition_key
        and p.season_id <=> c.season_id   -- NULL-safe (IDSSE season_id is NULL; = would drop all IDSSE)
```

- [ ] **Step 2: Add the regression singular test** `dbt_project/tests/assert_psxg_pooled_keeps_idsse.sql`:

```sql
{{ config(enabled=var('goalkeeper_enabled', false), severity='error') }}
-- Regression guard for the NULL-season INNER JOIN that dropped all IDSSE keepers.
-- Fails if IDSSE has match-grain shot-stopping rows but none survive the pooled rollup.
with ss as (select count(*) n from {{ ref('fct_gk_shot_stopping') }} where data_source = 'idsse'),
     pooled as (select count(*) n from {{ ref('fct_gk_shot_stopping_pooled') }} where data_source = 'idsse')
select 1 from ss, pooled where ss.n > 0 and pooled.n = 0
```

- [ ] **Step 3: Build + test.** `cd dbt_project && uv run dbt build --select fct_gk_shot_stopping_pooled --profiles-dir .` then `uv run dbt test --select assert_psxg_pooled_keeps_idsse --profiles-dir .` → PASS (IDSSE now survives: expect ~5 IDSSE rows). `goalkeeper_enabled: true` is already set (`dbt_project.yml:98`).
- [ ] **Step 4: Commit (await approval)** — `fix(psxg)!: NULL-safe season join in fct_gk_shot_stopping_pooled (keeps IDSSE)`.
- [ ] **Step 5: Wheel bump** (dbt change ships in the wheel) — `uv run python scripts/bump_wheel.py`.
- [ ] **Step 6: Add the MERGE-TIME guard — a pure SQL text-assertion unit test (runs in python-ci; no warehouse).** The dbt singular test only runs in the daily live build, so it does NOT gate the PR. This pure test is the only thing that fails a PR if someone reverts the `<=>` fix. Create `src/tests/test_gk_pooled_join_null_safe.py`:

```python
from pathlib import Path

def test_pooled_season_join_is_null_safe():
    sql = Path("dbt_project/models/marts/fct_gk_shot_stopping_pooled.sql").read_text(encoding="utf-8")
    # IDSSE has NULL season_id; a plain `=` join silently drops it. Require NULL-safe equality.
    assert "season_id <=> c.season_id" in sql or "season_id is not distinct from" in sql.lower(), (
        "fct_gk_shot_stopping_pooled season join must be NULL-safe (<=>) or IDSSE keepers vanish"
    )

def test_goalkeeper_marts_enabled():
    cfg = Path("dbt_project/dbt_project.yml").read_text(encoding="utf-8")
    assert "goalkeeper_enabled: true" in cfg  # the GK marts (incl. the pooled mart) must be built
```

Run `uv run pytest src/tests/test_gk_pooled_join_null_safe.py -v` → PASS. (This file lives in the root `src/tests/` suite, which python-ci runs with real intent — it has no DB dependency.)

> Recommended-but-separate (flag to producer; NOT gating this page): resolve IDSSE `season_id` upstream in `fct_gk_shot_stopping` so the NULL bucket disappears for every consumer.

### Task 1.1 — Sync `fct_gk_shot_stopping_pooled` (the page's goals-prevented source)

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py` (`SYNCED_TABLES`)
- Modify (count-pinned guard): `src/tests/test_refresh_synced_tables.py`

- [ ] **Step 1: Update the count-pinned test first (TDD for config)** — bump `len(SYNCED_TABLES)` by 1; add:

```python
def test_gk_shot_stopping_pooled_synced_registered():
    from ingestion.refresh_synced_tables import SYNCED_TABLES
    cfg = next((c for c in SYNCED_TABLES if c.name == "fct_gk_shot_stopping_pooled_synced"), None)
    assert cfg is not None
    assert cfg.source_table == "fct_gk_shot_stopping_pooled"
    assert cfg.primary_key_columns == ("gk_pooled_id",)
    assert cfg.scheduling_policy == "SNAPSHOT"  # default; omitting it in the config is fine (M1)
```

- [ ] **Step 2: Run it, watch it fail** — `uv run pytest src/tests/test_refresh_synced_tables.py::test_gk_shot_stopping_pooled_synced_registered -v` → FAIL.

- [ ] **Step 3: Add the config** in `SYNCED_TABLES`:

```python
    SyncedTableConfig("fct_gk_shot_stopping_pooled_synced", "fct_gk_shot_stopping_pooled", ("gk_pooled_id",)),
```

- [ ] **Step 4: Run the synced-table test file green** — `uv run pytest src/tests/test_refresh_synced_tables.py -v` → PASS.

- [ ] **Step 5: Create + grant live (operator step, after Task 1.0 rebuild so IDSSE rows exist)**

```bash
uv run --extra sdk python scripts/create_synced_table.py fct_gk_shot_stopping_pooled_synced
uv run python scripts/run_lakebase_grants.py --sp-uuid 1a1dbf08-df56-48de-b97a-276b2a4232d8
```
Acceptance: online; SELECT granted. Verify `data_source, COUNT(*)` shows GS/SC/**IDSSE** rows (IDSSE ≈ 5 — the Task-1.0 fix; pooled grain so fewer than the 6 match rows).

- [ ] **Step 6: Commit (await approval)** — `feat(synced): sync fct_gk_shot_stopping_pooled for goals-prevented`.

### Task 1.1b — New small dbt mart `fct_gk_defensive_line` (B2 + S3) + sync

**Files:**
- Create: `dbt_project/models/marts/fct_gk_defensive_line.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (contract), `src/ingestion/refresh_synced_tables.py` (+ count test), `workflow-cards/wf-dbt-build-output-marts.yaml` (TableRef — `test_card_dbt_model_field`)

> Per-GK defensive-line aggregate keyed on the **defending GK** (B2: `team_key` is the attacking team — wrong). Small (≤ a few hundred rows). Replaces the live 120K-row scan (S3).

- [ ] **Step 1: Write the model**

```sql
{{ config(materialized='table', tags=['marts', 'gk']) }}
-- Per-(defending GK, competition) defensive-line + shape aggregate for the tracking cohort.
-- Keyed on defending_gk_player_key (the line IN FRONT OF that keeper) — NOT team_key,
-- which is the in-possession/attacking team (B2). Feeds the measured style chip + the
-- within-competition Deep/Mid/High tercile + the "right defensive system?" verdict.
select
    {{ dbt_utils.generate_surrogate_key(['a.defending_gk_player_key', 'm.competition_key', 'a.data_source']) }}
                                                      as gk_defensive_line_id,
    a.defending_gk_player_key                         as gk_player_key,
    m.competition_key,
    a.data_source,
    avg(a.defensive_line_x)                           as avg_line_x,
    avg(a.team_shape_team_width_defending)            as avg_width,
    avg(a.compactness_x)                              as avg_compactness,
    count(*)                                          as n_actions
from {{ ref('fct_action_context') }} a
join {{ ref('dim_matches') }} m on m.match_key = a.match_key
where a.defending_gk_player_key is not null
  and a.defensive_line_x is not null
  and a.data_source in ('gradientsports', 'idsse', 'skillcorner')
group by a.defending_gk_player_key, m.competition_key, a.data_source
```

- [ ] **Step 2: Build + contract + B2-keying test.** Add the model to `_marts__models.yml` with `contract: {enforced: true}` and column types (`gk_defensive_line_id string, gk_player_key bigint, competition_key bigint, data_source string, avg_line_x double, avg_width double, avg_compactness double, n_actions bigint`). Add an ADR-018-style singular test `dbt_project/tests/assert_gk_defensive_line_resolves_defending_gk.sql` (config `enabled=var('goalkeeper_enabled', false)`): every `gk_player_key` resolves against `dim_players.player_key` — locks the B2 defending-GK keying (a row keyed on the attacking team would not resolve):
  ```sql
  select l.gk_player_key from {{ ref('fct_gk_defensive_line') }} l
  left join {{ ref('dim_players') }} p on p.player_key = l.gk_player_key
  where p.player_key is null
  ```
  Run `cd dbt_project && uv run dbt build --select fct_gk_defensive_line --profiles-dir .` + `dbt test --select assert_gk_defensive_line_resolves_defending_gk --profiles-dir .`. Expected: builds (small row count); test PASS.
- [ ] **Step 3: Register synced** — the model emits a surrogate `gk_defensive_line_id` (Step 1); key the synced table on it. Add to `SYNCED_TABLES`:
  ```python
  SyncedTableConfig("fct_gk_defensive_line_synced", "fct_gk_defensive_line", ("gk_defensive_line_id",)),
  ```
  Bump the count-pinned test in `src/tests/test_refresh_synced_tables.py`.
- [ ] **Step 4: Create + grant live** — `uv run --extra sdk python scripts/create_synced_table.py fct_gk_defensive_line_synced` then `run_lakebase_grants.py --sp-uuid 1a1dbf08-df56-48de-b97a-276b2a4232d8`. Verify IDSSE/GS/SC rows present.
- [ ] **Step 5: Commit (await approval)** — `feat(gk): fct_gk_defensive_line per-keeper line aggregate (defending-GK keyed)`.
- [ ] **Step 6: Wheel bump** — this is a dbt mart change; `uv run python scripts/bump_wheel.py` (the wheel force-includes `dbt_project/`).

### Task 1.2 — Query module: `queries/gk_analytics.py`

**Files:**
- Create: `hf_taipy_app/src/queries/gk_analytics.py`
- Test: `hf_taipy_app/src/test_gk_analytics_queries.py`

> Mirrors the read-side contract discipline of the old `queries/gk_tracking.py`: a provider-gate constant, explicit column lists (no `SELECT *`), parameterized SQL builders returning `(sql, params)`, and `@ttl_cache()` fetch wrappers. SQL builders are pure (no DB) → unit-tested without a connection.

- [ ] **Step 1: Write failing builder tests**

```python
from queries.gk_analytics import (
    GK_TRACKING_PROVIDERS,
    build_gk_keeper_lov_sql,
    build_distribution_stats_sql,
    build_sweeper_stats_sql,
    build_line_context_sql,
    build_goals_prevented_sql,
)

def test_provider_gate():
    assert GK_TRACKING_PROVIDERS == ("gradientsports", "idsse", "skillcorner")

def test_distribution_stats_sql_is_per_provider_and_joins_dims():
    sql, params = build_distribution_stats_sql(data_source="skillcorner")
    assert "fct_gk_tracking_stats_synced" in sql and "dim_matches_synced" in sql
    assert "dist_xt_gk_counter_mean" in sql and "data_source = %s" in sql
    assert "SELECT *" not in sql
    assert params[-1] == "skillcorner"

def test_goals_prevented_sql_reads_pre_aggregated_pooled_mart():
    sql, params = build_goals_prevented_sql(data_source="idsse")
    # Read the pooled mart directly (band precomputed; IDSSE preserved by the Task-1.0 NULL-safe join).
    # No in-app SUM, no LIMIT-on-a-summed-input.
    assert "fct_gk_shot_stopping_pooled_synced" in sql
    assert "goals_prevented_ci_low" in sql and "goals_prevented_ci_high" in sql and "low_sample" in sql
    assert "data_source = %s" in sql and "SUM(" not in sql.upper()

def test_line_context_sql_reads_mart_keyed_on_defending_gk():
    sql, params = build_line_context_sql(data_source="gradientsports")
    # B2/S3: read the small per-defending-GK mart, NOT a live scan of fct_action_context,
    # and NEVER group the defending line by the attacking team_key.
    assert "fct_gk_defensive_line_synced" in sql
    assert "fct_action_context_synced" not in sql and "team_key" not in sql
    assert "gk_player_key" in sql and params[-1] == "gradientsports"
```

- [ ] **Step 2: Run, watch fail**

Run (from `hf_taipy_app/`): `uv run --project .. --extra taipy-app pytest src/test_gk_analytics_queries.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement the query module**

```python
"""GK Analytics insight-views queries (per-provider; ADR-051 marts reused).

Read-side contract surface for the redesigned GK page. Per-provider by design
(spec §0 C2/C3: provider effect is real). Goals-prevented reads the pre-aggregated
fct_gk_shot_stopping_pooled (the band, low_sample, and ranking deferral are computed
in the mart; the Task-1.0 NULL-safe season join preserves IDSSE). No in-app rollup.
"""
from __future__ import annotations

import pandas as pd
from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

GK_TRACKING_PROVIDERS: tuple[str, ...] = ("gradientsports", "idsse", "skillcorner")
# (No _PROVIDER_SQL multi-provider constant — every builder is single-provider, one %s.)

_DIST_MODEL_COLS = (
    "dist_xt_gk_mean", "dist_xt_gk_possession_mean", "dist_xt_gk_counter_mean",
    "dist_xt_gk_direct_mean", "dist_xt_gk_high_press_mean", "dist_xt_gk_low_block_mean",
)
_SWEEP_COLS = (
    "pc_share_mean", "reachable_area_mean_m2",
    "closing_min_six_yard_mean_s", "closing_min_near_post_mean_s", "closing_min_far_post_mean_s",
)


def build_gk_keeper_lov_sql(data_source: str) -> tuple[str, tuple]:
    """Keepers with tracking stats for one provider, display names. (No canonical_player_key:
    S2 verified 0 keepers span providers — provider-first selection needs no cross-provider dedup.)"""
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions,0)) AS n_dist, "
        f"       SUM(COALESCE(s.n_defended_actions,0)) AS n_def "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE s.data_source = %s "
        f"GROUP BY s.gk_player_key, p.player_display_name "
        f"ORDER BY n_def DESC, n_dist DESC LIMIT 500"
    )
    return sql, (data_source,)


def build_distribution_stats_sql(data_source: str) -> tuple[str, tuple]:
    """Per-(GK, comp, season) volume-weighted signed per-model means for one provider."""
    wsum = ", ".join(
        f"SUM(s.{c} * s.n_distributions) / NULLIF(SUM(s.n_distributions),0) AS {c}" for c in _DIST_MODEL_COLS
    )
    sql = (
        f"SELECT s.gk_player_key, m.competition_key, m.season_id, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions,0)) AS n_distributions, {wsum} "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_matches_synced')} m ON m.match_key = s.match_key "
        f"WHERE s.data_source = %s "
        f"GROUP BY s.gk_player_key, m.competition_key, m.season_id, s.data_source LIMIT 2000"
    )
    return sql, (data_source,)


def build_sweeper_stats_sql(data_source: str) -> tuple[str, tuple]:
    """Per-(GK, comp, season) volume-weighted sweeper means for one provider."""
    wsum = ", ".join(
        f"SUM(s.{c} * s.n_defended_actions) / NULLIF(SUM(s.n_defended_actions),0) AS {c}" for c in _SWEEP_COLS
    )
    sql = (
        f"SELECT s.gk_player_key, m.competition_key, m.season_id, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_defended_actions,0)) AS n_defended_actions, "
        f"       SUM(COALESCE(s.shots_faced,0)) AS shots_faced, "
        f"       SUM(s.ghost_deviation_mean_m * s.shots_faced) / NULLIF(SUM(s.shots_faced),0) AS ghost_deviation_mean_m, "
        f"       {wsum} "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_matches_synced')} m ON m.match_key = s.match_key "
        f"WHERE s.data_source = %s "
        f"GROUP BY s.gk_player_key, m.competition_key, m.season_id, s.data_source LIMIT 2000"
    )
    return sql, (data_source,)


def build_line_context_sql(data_source: str) -> tuple[str, tuple]:
    """Per-(defending GK, competition) line + shape for one provider — feeds the
    within-competition Deep/Mid/High terciles + the measured style chip.
    Reads the small precomputed mart (B2: keyed on the DEFENDING GK, not team_key; S3: not a live scan)."""
    sql = (
        f"SELECT gk_player_key, competition_key, data_source, "  # noqa: S608
        f"       avg_line_x, avg_width, avg_compactness, n_actions "
        f"FROM {t('fct_gk_defensive_line_synced')} "
        f"WHERE data_source = %s LIMIT 2000"
    )
    return sql, (data_source,)


def build_goals_prevented_sql(data_source: str) -> tuple[str, tuple]:
    """Pre-aggregated goals-prevented rows (one per keeper x comp x season) for one provider,
    read directly from fct_gk_shot_stopping_pooled — the band, low_sample, and ranking deferral
    are computed in the mart (Task 1.0 NULL-safe join keeps IDSSE). No in-app rollup, no LIMIT-on-SUM."""
    sql = (
        f"SELECT player_key, competition_key, season_id, data_source, "  # noqa: S608
        f"       goals_prevented, goals_prevented_ci_low, goals_prevented_ci_high, "
        f"       shots_faced_total, low_sample "
        f"FROM {t('fct_gk_shot_stopping_pooled_synced')} "
        f"WHERE data_source = %s LIMIT 500"
    )
    return sql, (data_source,)


@ttl_cache()
def fetch_gk_keepers(data_source: str) -> pd.DataFrame:
    sql, params = build_gk_keeper_lov_sql(data_source)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_distribution_stats(data_source: str) -> pd.DataFrame:
    sql, params = build_distribution_stats_sql(data_source)
    return execute_query(sql, params)


@ttl_cache()
def fetch_sweeper_stats(data_source: str) -> pd.DataFrame:
    sql, params = build_sweeper_stats_sql(data_source)
    return execute_query(sql, params)


@ttl_cache()
def fetch_line_context(data_source: str) -> pd.DataFrame:
    sql, params = build_line_context_sql(data_source)
    return execute_query(sql, params)


@ttl_cache()
def fetch_goals_prevented(data_source: str) -> pd.DataFrame:
    sql, params = build_goals_prevented_sql(data_source)
    return execute_query(sql, params)
```

- [ ] **Step 4: Run builder tests green**

Run (from `hf_taipy_app/`): `uv run --project .. --extra taipy-app pytest src/test_gk_analytics_queries.py -v`
Expected: PASS.

- [ ] **Step 5: Live smoke (operator)**

```bash
cd hf_taipy_app && set -a && source .env && set +a && \
  .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); \
  from queries.gk_analytics import fetch_distribution_stats, fetch_goals_prevented; \
  print(fetch_distribution_stats('idsse').shape); print(fetch_goals_prevented('idsse').shape)"
```
Expected: non-empty frames; `fetch_goals_prevented('idsse')` returns the IDSSE pooled rows (≈5, pooled grain) with non-null `goals_prevented_ci_low/high` — proves the Task-1.0 NULL-safe join (the unfixed mart returned 0 IDSSE).

- [ ] **Step 6: Commit (await approval)** — `feat(gk): per-provider GK analytics query module`.

### Task 1.3 — Read-side contract reconciliation test (S4, A3/A4 pattern)

**Files:** Test `hf_taipy_app/src/test_gk_analytics_read_contract.py` (live-DB; skip if `LAKEBASE_*` unset).

- [ ] **Step 1: Write the test** — assert each app column constant is a subset of the live mart columns, so a producer rename fails CI here, not silently in the Space:

```python
import os, pytest
pytest.importorskip("plotly")
# conftest.py autouse-sets LAKEBASE_HOST="test-host" — so `if not LAKEBASE_HOST` is DEFEATED.
# Detect a REAL host; this test is an operator/live-run gate, NOT a CI gate (CI runs with the dummy).
_HOST = os.environ.get("LAKEBASE_HOST", "")
if not _HOST or _HOST == "test-host" or "example" in _HOST:
    pytest.skip("needs a real Lakebase host (operator/live run only)", allow_module_level=True)
from queries.common import execute_query, t
from queries.gk_analytics import _DIST_MODEL_COLS, _SWEEP_COLS

def _cols(table):
    return set(execute_query(f"SELECT * FROM {t(table)} LIMIT 0", ()).columns)

def test_distribution_and_sweeper_cols_subset_of_stats_mart():
    live = _cols("fct_gk_tracking_stats_synced")
    assert set(_DIST_MODEL_COLS) <= live, set(_DIST_MODEL_COLS) - live
    assert set(_SWEEP_COLS) <= live, set(_SWEEP_COLS) - live

def test_goals_prevented_cols_subset_of_pooled_mart():
    live = _cols("fct_gk_shot_stopping_pooled_synced")  # the synced source (Task 1.1)
    assert {"player_key","competition_key","season_id","data_source",
            "goals_prevented","goals_prevented_ci_low","goals_prevented_ci_high",
            "shots_faced_total","low_sample"} <= live  # exactly what build_goals_prevented_sql selects

def test_line_cols_subset_of_defensive_line_mart():
    live = _cols("fct_gk_defensive_line_synced")
    assert {"gk_player_key","competition_key","data_source","avg_line_x","avg_width","avg_compactness"} <= live
```

- [ ] **Step 2: Run green** (live). **Step 3: Commit** — `test(gk): read-side contract reconciliation`.

---

## Phase 2 — Pure domain functions (`services/gk_insight.py`)

> The testable core the reviewer will scrutinize. Pure: stdlib + numpy only. No Taipy/DB/pandas-required. Each function has a frozen-dataclass result. TDD throughout.

### Task 2.1 — Reference band (IQR + ≥8-cohort gate)

**Files:** Create `hf_taipy_app/src/services/gk_insight.py`; Test `hf_taipy_app/src/test_gk_insight.py`.

- [ ] **Step 1: Failing test**

```python
import math
from services.gk_insight import reference_band, ReferenceBand

def test_reference_band_returns_iqr_when_enough_members():
    vals = [float(i) for i in range(1, 21)]  # 1..20, n=20
    band = reference_band(vals, min_cohort=8)
    assert isinstance(band, ReferenceBand)
    assert band.n == 20
    assert band.median == 10.5
    assert band.q1 < band.median < band.q3

def test_reference_band_none_below_min_cohort():
    assert reference_band([1.0, 2.0, 3.0], min_cohort=8) is None

def test_reference_band_excludes_nan_then_applies_floor():
    vals = [1.0, float("nan"), 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]  # 7 finite → below 8
    assert reference_band(vals, min_cohort=8) is None
```

- [ ] **Step 2: Run, watch fail** — `uv run --extra taipy-app pytest hf_taipy_app/src/test_gk_insight.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

```python
"""Pure GK-insight domain functions (spec §0 E). No Taipy/DB imports — stdlib + numpy only.
Mirrors the services/ghost_grid.py port discipline: pure, caller supplies all data."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ReferenceBand:
    median: float
    q1: float
    q3: float
    n: int


def reference_band(values, *, min_cohort: int = 8) -> ReferenceBand | None:
    """IQR band over a provider cohort. Returns None when fewer than ``min_cohort``
    finite members remain (caller renders the value without a band)."""
    arr = np.asarray([v for v in values], dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < min_cohort:
        return None
    q1, med, q3 = (float(x) for x in np.percentile(arr, [25, 50, 75]))
    return ReferenceBand(median=med, q1=q1, q3=q3, n=int(arr.size))
```
(Note: keyword-only `min_cohort` — update the Task 2.1 test calls to `reference_band(vals, min_cohort=8)`; they already pass it positionally-as-keyword via `min_cohort=`. Keep `*` to prevent silent misuse.)

- [ ] **Step 4: Run green.** **Step 5: Commit** — `feat(gk): pure reference-band (IQR + min-cohort gate)`.

### Task 2.1b — Cohort values: one value per GK, sub-floor excluded (S1)

> The cohort fed to `reference_band` MUST be one value per GK (== the displayed-keeper unit), with sub-floor GKs dropped BEFORE the ≥8 gate — else `n` counts (GK×comp×season) rows and a multi-season GK is double-counted.

- [ ] **Step 1: Failing test**

```python
from services.gk_insight import cohort_values

def test_cohort_values_volume_weights_per_gk_and_drops_sub_floor():
    # gk A: two season rows (vol-weighted mean); gk B: below floor → dropped
    rows = [("A", 0.04, 30), ("A", 0.00, 10), ("B", 0.99, 5)]
    vals = cohort_values(rows, floor=20)
    assert len(vals) == 1                      # B dropped (total weight 5 < 20)
    assert abs(vals[0] - (0.04*30 + 0.0*10)/40) < 1e-9   # A vol-weighted, single value

def test_cohort_values_skips_nan():
    rows = [("A", float("nan"), 50), ("A", 0.02, 50)]
    assert abs(cohort_values(rows, floor=20)[0] - 0.02) < 1e-9
```

- [ ] **Step 2: fail. Step 3: implement** (append to `gk_insight.py`):

```python
from collections import defaultdict


def cohort_values(rows, *, floor: float) -> list[float]:
    """rows: iterable of (gk_id, value, weight). Volume-weight each GK's rows to ONE value,
    drop GKs whose total weight < ``floor``, return the per-GK values (unit == displayed keeper)."""
    num: dict = defaultdict(float)
    den: dict = defaultdict(float)
    for gk, v, w in rows:
        if v is None or w is None:
            continue
        v, w = float(v), float(w)
        if not (np.isfinite(v) and np.isfinite(w) and w > 0):
            continue
        num[gk] += v * w
        den[gk] += w
    return [num[g] / den[g] for g in den if den[g] >= floor]
```

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): per-GK cohort values (vol-weighted, sub-floor drop)`.

### Task 2.2 — Tercile position + within-competition line tercile

- [ ] **Step 1: Failing test**

```python
from services.gk_insight import tercile_position

def test_tercile_position_low_mid_high():
    cohort = [float(i) for i in range(1, 100)]  # 1..99
    assert tercile_position(10, cohort) == "low"
    assert tercile_position(50, cohort) == "mid"
    assert tercile_position(90, cohort) == "high"

def test_tercile_position_lower_is_better_flips():
    cohort = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    # closing time: a LOW raw value is GOOD → "high" command
    assert tercile_position(1.0, cohort, lower_is_better=True) == "high"
    assert tercile_position(9.0, cohort, lower_is_better=True) == "low"

def test_tercile_position_tiny_cohort_returns_mid():
    assert tercile_position(5.0, [5.0], ) == "mid"
```

- [ ] **Step 2: fail. Step 3: implement** (append to `gk_insight.py`):

```python
from typing import Literal, Sequence

Tercile = Literal["low", "mid", "high"]


def tercile_position(value: float, cohort: Sequence[float], *, lower_is_better: bool = False) -> Tercile:
    """Classify ``value`` into low/mid/high by the cohort's 33rd/67th percentiles.
    ``lower_is_better`` flips the labels (e.g. closing time). Degenerate cohorts → 'mid'."""
    arr = np.asarray([c for c in cohort], dtype=float)
    arr = arr[np.isfinite(arr)]
    if value is None or not np.isfinite(value) or arr.size < 3:
        return "mid"
    p33, p67 = (float(x) for x in np.percentile(arr, [33.33, 66.67]))
    if p33 == p67:
        return "mid"
    raw: Tercile = "low" if value < p33 else ("high" if value > p67 else "mid")
    if lower_is_better:
        return {"low": "high", "high": "low", "mid": "mid"}[raw]
    return raw
```

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): pure tercile classifier`.

### Task 2.3 — Sweeping-command composite + measured style chip

- [ ] **Step 1: Failing test**

```python
from services.gk_insight import sweeping_command, measured_style_chip

def test_sweeping_command_majority_upper():
    # reachable high, pc high, closing fast → upper
    assert sweeping_command(reach="high", pc="high", closing="high") == "upper"

def test_sweeping_command_mixed_is_mid():
    assert sweeping_command(reach="high", pc="mid", closing="low") == "mid"

def test_sweeping_command_lower():
    assert sweeping_command(reach="low", pc="low", closing="mid") == "lower"

def test_measured_style_chip_deep_narrow():
    assert measured_style_chip(line="low", width="low") == "deep, narrow block"

def test_measured_style_chip_high_wide():
    assert measured_style_chip(line="high", width="high") == "high, wide line"

def test_measured_style_chip_unavailable():
    assert measured_style_chip(line=None, width="mid") == "shape context unavailable"
```

- [ ] **Step 2: fail. Step 3: implement** (append):

```python
CommandPos = Literal["upper", "mid", "lower"]
_TERCILE_RANK = {"low": 0, "mid": 1, "high": 2}


def sweeping_command(*, reach: Tercile, pc: Tercile, closing: Tercile) -> CommandPos:
    """Composite of the three sweeper terciles (each already oriented better=high).
    Mean rank → upper / mid / lower."""
    mean = (_TERCILE_RANK[reach] + _TERCILE_RANK[pc] + _TERCILE_RANK[closing]) / 3.0
    if mean >= 1.5:
        return "upper"
    if mean <= 0.5:
        return "lower"
    return "mid"


_LINE_WORD = {"low": "deep", "mid": "mid", "high": "high"}
_WIDTH_WORD = {"low": "narrow", "mid": "balanced", "high": "wide"}


def measured_style_chip(*, line: Tercile | None, width: Tercile | None) -> str:
    """Descriptive observed-shape chip (NOT a game-model classification — spec §0 C1/N2)."""
    if line is None or width is None:
        return "shape context unavailable"
    line_w, width_w = _LINE_WORD[line], _WIDTH_WORD[width]
    shape = "line" if line == "high" else "block"
    return f"{line_w}, {width_w} {shape}"
```

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): sweeping-command composite + measured style chip`.

### Task 2.4 — Verdict templater (§11a, both views)

- [ ] **Step 1: Failing test (every branch)**

```python
from services.gk_insight import offensive_verdict, defensive_verdict, Verdict

def test_offensive_low_sample():
    v = offensive_verdict(best_fit="Counter", fit_spread=0.04, n_distributions=10, spread_threshold=0.02)
    assert v.phrase == "Indicative only — small sample"

def test_offensive_system_agnostic():
    v = offensive_verdict(best_fit="Counter", fit_spread=0.005, n_distributions=50, spread_threshold=0.02)
    assert v.phrase == "System-agnostic distributor"

def test_offensive_best_fit_descriptive_no_deployment_claim():
    v = offensive_verdict(best_fit="Counter", fit_spread=0.04, n_distributions=50, spread_threshold=0.02)
    assert v.phrase == "Counter-fit"
    assert "unverified" in v.detail.lower()
    assert "mis-deployed" not in v.detail.lower()  # N2: never a deployment claim

def test_defensive_low_sample():
    v = defensive_verdict(command="upper", line="low", n_defended=10)
    assert v.phrase == "Indicative only — small sample"

def test_defensive_underused_sweeper():
    v = defensive_verdict(command="upper", line="low", n_defended=152)
    assert v.phrase == "Underused sweeper"
    assert "deep" in v.detail.lower()

def test_defensive_well_deployed():
    assert defensive_verdict(command="upper", line="high", n_defended=152).phrase == "Well-deployed sweeper"

def test_defensive_line_keeper():
    assert defensive_verdict(command="lower", line="mid", n_defended=152).phrase == "Line-keeper profile"

def test_defensive_typical_box_keeper():
    assert defensive_verdict(command="mid", line="mid", n_defended=152).phrase == "Typical box-keeper"
```

- [ ] **Step 2: fail. Step 3: implement** (append):

```python
@dataclass(frozen=True)
class Verdict:
    phrase: str
    detail: str


def offensive_verdict(*, best_fit: str, fit_spread: float, n_distributions: int,
                      spread_threshold: float) -> Verdict:
    """Spec §11a offensive — DESCRIPTIVE only (N2: no style->model inference)."""
    if n_distributions < 20:
        return Verdict("Indicative only — small sample", "too few distributions to characterise fit")
    if fit_spread < spread_threshold:
        return Verdict("System-agnostic distributor", "even value across game models")
    return Verdict(f"{best_fit}-fit", f"strongest under {best_fit}; system fit unverified")


def defensive_verdict(*, command: CommandPos, line: Tercile, n_defended: int) -> Verdict:
    """Spec §11a defensive — DEFINED spatial-capacity rule (N6). Not a formation recommendation."""
    if n_defended < 30:
        return Verdict("Indicative only — small sample", "too few defended actions")
    if command == "upper" and line == "low":
        return Verdict("Underused sweeper", "command unused behind a deep line")
    if command == "upper" and line == "high":
        return Verdict("Well-deployed sweeper", "sweeping range matched by a high line")
    if command == "lower":
        return Verdict("Line-keeper profile", "stays on his line")
    return Verdict("Typical box-keeper", "mid-cohort command and line")
```

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): pure verdict templater (offensive descriptive + defensive spatial rule)`.

### Task 2.5 — REMOVED (BLOCKER reversal — the mart owns the rollup + band)

> Deleted per Plan-review-v3: `fct_gk_shot_stopping_pooled` already computes `goals_prevented`,
> `goals_prevented_ci_low/high` (Poisson-binomial), and `low_sample`. The app reads those columns
> directly (Task 1.2 `build_goals_prevented_sql`). **No in-app rollup function** (`goals_prevented_band`)
> and **no pandas season grouping** — which also removes the `dropna=True` IDSSE re-drop risk by
> construction. The only presentation-side piece is a thin display formatter (Task 3.3 / state):
> `f"{gp:+.2f} (95% CI {ci_low:+.2f}…{ci_high:+.2f})"` + `straddles_zero = ci_low <= 0 <= ci_high`
> computed inline from the two mart columns + the "higher = better; 0 = as expected" caption. No new
> pure dataclass needed.

### Task 2.6 — Fit-ladder helper (signed best-fit + spread)

- [ ] **Step 1: Failing test**

```python
from services.gk_insight import fit_ladder, FitLadder

def test_fit_ladder_orders_signed_and_picks_best():
    means = {"Counter": 0.004, "Direct": -0.008, "Possession": -0.018,
             "High Press": -0.022, "Default": -0.026, "Low Block": -0.038}
    fl = fit_ladder(means)
    assert isinstance(fl, FitLadder)
    assert fl.best_fit == "Counter"
    assert fl.rows[0][0] == "Counter" and fl.rows[-1][0] == "Low Block"
    assert math.isclose(fl.fit_spread, 0.004 - (-0.038))

def test_fit_ladder_drops_nan_models():
    fl = fit_ladder({"Counter": 0.004, "Direct": float("nan")})
    assert [r[0] for r in fl.rows] == ["Counter"]
```

- [ ] **Step 2: fail. Step 3: implement** (append):

```python
@dataclass(frozen=True)
class FitLadder:
    rows: tuple[tuple[str, float], ...]  # (model, signed_mean) desc by value
    best_fit: str
    fit_spread: float


def fit_ladder(model_means: dict[str, float]) -> FitLadder | None:
    finite = {k: float(v) for k, v in model_means.items() if v is not None and np.isfinite(v)}
    if not finite:
        return None
    rows = tuple(sorted(finite.items(), key=lambda kv: kv[1], reverse=True))
    vals = [v for _, v in rows]
    return FitLadder(rows=rows, best_fit=rows[0][0], fit_spread=float(max(vals) - min(vals)))
```

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): pure fit-ladder helper`.

> Also add (same module, same task) the pure **`_MODEL_LABEL`** map + a test — the canonical column→display-name mapping the state layer uses to build the `model_means` dict for `fit_ladder`:
> ```python
> _MODEL_LABEL = {
>     "dist_xt_gk_mean": "Default", "dist_xt_gk_possession_mean": "Possession",
>     "dist_xt_gk_counter_mean": "Counter", "dist_xt_gk_direct_mean": "Direct",
>     "dist_xt_gk_high_press_mean": "High Press", "dist_xt_gk_low_block_mean": "Low Block",
> }
> ```
> Test: `set(_MODEL_LABEL) == set(queries.gk_analytics._DIST_MODEL_COLS)` (keeps the map and the query column list in lockstep).

---

## Phase 3 — Render helpers (charts + RawHtml callouts)

### Task 3.1 — Signed fit-ladder Plotly builder

**Files:** Create `hf_taipy_app/src/state/gk_analytics_charts.py`; Test `hf_taipy_app/src/test_gk_analytics_charts.py` (importorskip plotly).

- [ ] **Step 1: Failing test**

```python
import pytest
pytest.importorskip("plotly")
from services.gk_insight import fit_ladder
from state.gk_analytics_charts import build_fit_ladder_figure

def test_fit_ladder_figure_centered_at_zero_with_cvd_safe_palette():
    fl = fit_ladder({"Counter": 0.004, "Low Block": -0.038, "Default": -0.026,
                     "Direct": -0.008, "Possession": -0.018, "High Press": -0.022})
    fig = build_fit_ladder_figure(fl, cohort_median=-0.020, best_fit="Counter")
    assert fig is not None
    # zero reference line present (redundant cue #1 — guards against a positive-only-axis regression)
    assert fig.layout.shapes and any(getattr(s, "x0", None) == 0 for s in fig.layout.shapes)
    # M2: NOT red/green. Bar colors use the ColorBrewer-safe diverging pair (orange neg / blue pos).
    bar = next(t for t in fig.data if t.type == "bar")
    colors = set(bar.marker.color) if isinstance(bar.marker.color, (list, tuple)) else {bar.marker.color}
    assert _POS_COLOR in colors or _NEG_COLOR in colors  # exported from the chart module
    assert "#3fb950" not in colors and "#e5544b" not in colors  # no red/green sign encoding
```

- [ ] **Step 2: fail. Step 3: implement** — horizontal `go.Bar(orientation="h")`, bars from x=0 to the signed mean; **M2: ColorBrewer-safe diverging palette — `_POS_COLOR = "#2c7bb6"` (blue, ≥0) / `_NEG_COLOR = "#fdae61"` (orange, <0)**, NOT red/green; amber outline on the best-fit bar (redundant cue #2); a prominent **0-line** via `fig.add_shape(type="line", x0=0, x1=0, ...)` (redundant cue #1 — position + line, not color, carry the sign); dashed cohort-median line `fig.add_vline(x=cohort_median, line_dash="dash")`; `_LAYOUT` spread + override; `xaxis=dict(zeroline=True, range=[min−pad, max+pad])` symmetric enough to show 0. Export `_POS_COLOR`/`_NEG_COLOR` for the test.

- [ ] **Step 4: green. Step 5: commit** — `feat(gk): signed fit-ladder chart`.

### Task 3.2 — Sweeper-profile Plotly builder (dot on IQR band)

- [ ] **Step 1: Failing test** asserting three metric rows + that a `None` band still plots the dot without the shaded IQR rect.

```python
import pytest; pytest.importorskip("plotly")
from services.gk_insight import ReferenceBand
from state.gk_analytics_charts import build_sweeper_profile_figure

def test_sweeper_profile_plots_value_with_and_without_band():
    metrics = [
        ("Reachable area", 298.0, ReferenceBand(250, 220, 300, 30), False),
        ("Closing time · 6-yd", 1.5, None, True),  # no band → cohort too small
    ]
    fig = build_sweeper_profile_figure(metrics)
    assert fig is not None
```

- [ ] **Step 2-5:** implement (three horizontal tracks; shaded IQR rect when band present; "better →"/"faster →" arrow per `lower_is_better`; "cohort too small — no band" annotation when band is None), run green, commit.

### Task 3.3 — Big-story + direction-table RawHtml builders

**Files:** add to `state/gk_analytics_charts.py` (or a sibling `state/gk_analytics_render.py`); reuse `RawHtml` from `state/workflows_dag.py` and the `ll-big-story-*` / `ll-delta-table-*` CSS idioms from `state/match_summary_render.py`.

- [ ] **Step 1: Failing test** (pure string assembly — assert the `★ Big story` label + the verdict phrase + the per-model direction rows appear; assert no raw player ids).

```python
from services.gk_insight import Verdict
from state.gk_analytics_render import render_big_story_html, render_direction_table_html

def test_big_story_contains_label_and_phrase():
    html = render_big_story_html(Verdict("Counter-fit", "strongest under Counter; system fit unverified"),
                                 body="Net-positive only in transition.")
    s = html.html if hasattr(html, "html") else str(html)
    assert "★ Big story" in s and "Counter-fit" in s

def test_direction_table_rows_signed_and_no_ids():
    rows = [("Counter", 0.004, 0.024, "His only net-positive model"),
            ("Low Block", -0.038, -0.018, "Loses most threat parked deep")]
    html = render_direction_table_html(rows)
    s = html.html if hasattr(html, "html") else str(html)
    assert "Counter" in s and "+0.004" in s and "player_key" not in s
```

- [ ] **Step 2-5:** implement (mirror `match_summary_render.render_moments_html` / `render_delta_table_html`; star the top row; signed formatting `{:+.3f}`; `★ Big story` literal), run green, commit.

---

## Phase 4 — Taipy view assembly (`gka_` prefix)

### Task 4.1 — State module skeleton + provider/keeper selection

**Files:** Create `hf_taipy_app/src/state/gk_analytics.py`; Test `hf_taipy_app/src/test_gk_analytics_state.py` (importorskip plotly; pure-helper tests only — Taipy `state` objects aren't unit-testable).

- [ ] **Step 1: Failing test for the pure helpers in the state module** (provider-first model — S2; NO auto-pick/sub-selector, verified 0 cross-provider keepers). Test the keeper-LOV map builder + the cohort-assembly that wires queries→`gk_insight`:

```python
import pytest; pytest.importorskip("plotly")
import pandas as pd
from state.gk_analytics import _build_keeper_map, _cohort_for_metric

def test_build_keeper_map_is_per_provider_display_names():
    df = pd.DataFrame({"gk_player_key": [11, 22], "player_display_name": ["A. Keeper", "B. Keeper"]})
    m = _build_keeper_map(df)
    assert m == {"A. Keeper": 11, "B. Keeper": 22}

def test_cohort_for_metric_drops_sub_floor_then_bands(monkeypatch):
    # 8 GKs >= floor + 2 below → reference_band over 8 (== min_cohort)
    rows = pd.DataFrame({
        "gk_player_key": list(range(10)),
        "dist_xt_gk_counter_mean": [0.01*i for i in range(10)],
        "n_distributions": [50]*8 + [5, 5],
    })
    band = _cohort_for_metric(rows, value_col="dist_xt_gk_counter_mean",
                              weight_col="n_distributions", floor=20, min_cohort=8)
    assert band is not None and band.n == 8
```

- [ ] **Step 2-5:** implement the state module (provider-FIRST): `gka_` state vars (`gka_selected_provider`, `gka_selected_keeper`, sub-view; KPI/verdict/story/chart vars), `__all__`, `register_page_refresher("Goalkeeper-Analytics", gka_refresh)`, callbacks (`gka_on_provider_change` resets keeper; `gka_on_keeper_change`), and `gka_refresh(state)` dispatching per sub-view. `_cohort_for_metric` collapses query rows → `cohort_values()` → `reference_band()`. **No `_pick_provider_for_keeper`, no provider sub-selector** (dead — S2). Wire queries → `gk_insight` pure fns → state vars. Run pure-helper tests green; commit.

  > **PERF — fetch the provider cohort ONCE per refresh, slice the keeper from it (no N+1; optimization-audit #1).** Each `gka_refresh` calls `fetch_distribution_stats(provider)` / `fetch_sweeper_stats(provider)` / `fetch_goals_prevented(provider)` **exactly once** — these return the WHOLE provider cohort; the selected keeper's row is sliced in-pandas, and the SAME frame feeds `_cohort_for_metric`. Never call a fetch inside a per-keeper or per-metric loop. The fetches are `@ttl_cache`d on `data_source`, so switching keeper *within* a provider recomputes only the in-app numpy slice (sub-ms, well under the 500 ms cached-interaction budget); switching provider pays ~4 small cached-after-first queries (under the 3 s first-load budget). Add a unit assertion that `gka_refresh` issues ≤1 fetch per family per call (e.g. monkeypatch the fetch fns with call counters and assert counts == 1).

(Implementation detail — the refresh functions call `fetch_distribution_stats(provider)` etc., compute the cohort `reference_band`/`tercile_position` over the provider cohort, the keeper's `fit_ladder`/`sweeping_command`, the `offensive_verdict`/`defensive_verdict`, and for goals-prevented **slice the selected keeper's single pre-aggregated row from `fetch_goals_prevented(provider)`** and format `goals_prevented ± CI` inline from the mart's `goals_prevented_ci_low/high` (no in-app grouping, no band function — deleted in Task 2.5). All heavy logic lives in the Phase-2 pure functions; the state module only fetches + adapts + assigns.)

### Task 4.2 — Page config (two sub-views, dashboard house style)

**Files:** Create `hf_taipy_app/src/pages/gk_analytics.py`.

- [ ] **Step 1:** Build `page_config: PageConfig` — **SubView layout, `Metric` cards only (no `stats`/`StatCard`)** (Task-4.2 disposition). Two `SubView`s (Distribution Value / Shot Review), each with `metrics=[Metric(...)]` for the KPI cards **and the OUR VERDICT card** (verdict phrase+detail baked into a `Metric` var string by state, Match-Summary style — value carries its own reference). Each SubView `content` rows hold: the `chart` ContentBlock (fit-ladder / sweeper-profile), the `html` Big-Story block, the `html` Direction-table block, and the honest-secondary strip. Page-level: a **tracking-cohort-scope blurb** (B1a) in `description`; `scope_dims=[ScopeDim("Provider", "gka_provider_label"), ScopeDim("Keeper", "gka_keeper_label")]`; `citations=[Citation("Eyestone — xT-GK ..."), Citation("Spearman (2018) ...", "..."), Citation("PSxG — ADR-060 ...")]`; `freshness_var`, `warning_var`; an `empty_message` stating the tracking-only cohort (B1a). `page_md = build_page(page_config)`.
- [ ] **Step 2:** `register_page_refresher("Goalkeeper-Analytics", gka_refresh)` — **SubView layout** (confirmed required by `test_tier_a_canon.py`, which lists Goalkeeper-Analytics as a SubView page carrying scope per SubView). Use `sub_views` with per-view `metrics` (`Metric` cards, like the prior GK page); the OUR VERDICT + KPI references live in the `Metric` var strings (value + reference baked in by state, per the Match-Summary pattern). Do NOT use the dashboard (`stats`/`is_dashboard=True`) layout — it would break the SubView scope contract.
- [ ] **Step 3:** no test (declarative config); verified by the import test in 4.4 + e2e in Phase 5. Commit.

### Task 4.3 — Sidebar widgets

**Files:** Modify `hf_taipy_app/src/template.py`.

- [ ] **Step 1:** Add `_GKA_PAGES = ("Goalkeeper-Analytics",)`; add it to `_SUB_VIEW_PAGES`. Add two `SidebarWidget`s gated on `current_page in _GKA_PAGES`: a **Provider** dropdown (`gka_selected_provider` → `gka_on_provider_change`, lov `gka_provider_lov`) and a **Keeper** dropdown (`gka_selected_keeper` → `gka_on_keeper_change`, lov `gka_keeper_lov`, `depends_on="gka_selected_provider"`). Sub-view selector reuses the shared `selected_sub_view` mechanism. Update `PAGE_TERMS["Goalkeeper-Analytics"]` to the redesign's terms (xT-GK, Game-Model fit, Sweeper / space-command, Reachable area, Pitch-control share, Closing time, Goals prevented, Ghost-positioning deviation).
- [ ] **Step 2:** Verify glossary coverage — every new term in `PAGE_TERMS["Goalkeeper-Analytics"]` exists in `GLOSSARY`; add missing entries. Commit.

### Task 4.4 — Register the new page on the `Goalkeeper-Analytics` route (unconditional)

**Files:** Modify `hf_taipy_app/src/main.py`, `hf_taipy_app/src/test_tier_a_canon.py`.

- [ ] **Step 1: Update the canonical-map test first** — repoint `test_tier_a_canon.py:13,30` from the legacy page to the new one:

```python
# was: from pages.goalkeeper import page_config as goalkeeper_config
from pages.gk_analytics import page_config as goalkeeper_config   # route key stays "Goalkeeper-Analytics"
```
(The map entry `"Goalkeeper-Analytics": goalkeeper_config` at line 30 is unchanged — same route, new config object. Keep the comment at line 67 listing Goalkeeper-Analytics as a SubView page.)

- [ ] **Step 2: Run it, watch it fail** — `uv run --project .. --extra taipy-app pytest src/test_tier_a_canon.py -v` → FAIL (`pages.gk_analytics` not importable yet / page_config missing required pieces).

- [ ] **Step 3: Swap the registration in `main.py`** — replace the legacy import + registration (lines 21-22, 54, 118) with the new page, registered **unconditionally** (it replaces an always-on page):

```python
# top imports (replacing pages.goalkeeper import):
from pages.gk_analytics import page_config as gk_analytics_config
from pages.gk_analytics import page_md as gk_analytics_page
import state.gk_analytics as _gka_state
globals().update({name: getattr(_gka_state, name) for name in _gka_state.__all__})
# (delete `from state.goalkeeper import *`)

# in PAGE_REGISTRY (replacing the legacy PageEntry):
PageEntry("Goalkeeper-Analytics", gk_analytics_config, gk_analytics_page),
```
Delete the entire `if os.environ.get("LL_GK_TRACKING_PAGE") == "1":` block (the gkt registration) — the flag is retired.

- [ ] **Step 4: Run green** — `uv run --project .. --extra taipy-app pytest src/test_tier_a_canon.py -v` → PASS. (No flag needed; the page is always registered.)

### Task 4.5 — Delete both deprecated GK pages + clean reverse-deps

**Files:**
- Delete (legacy event page): `hf_taipy_app/src/pages/goalkeeper.py`, `hf_taipy_app/src/state/goalkeeper.py`, `hf_taipy_app/src/queries/goalkeepers.py`, and any `hf_taipy_app/src/test_*goalkeeper*.py` legacy tests.
- Delete (tracking page): `hf_taipy_app/src/{pages,state,queries}/gk_tracking.py`, `hf_taipy_app/src/test_gk_tracking_{queries,state}.py`.
- Modify `hf_taipy_app/src/template.py`: remove the legacy `gk_*` `SidebarWidget`s + `_GK_PAGES`, the gkt `SidebarWidget`s + `_GKT_PAGES`, and the stale `PAGE_TERMS["Goalkeeper-Tracking"]`; keep/refresh `PAGE_TERMS["Goalkeeper-Analytics"]` (Task 4.3) and `_GKA_PAGES`.
- Modify `hf_taipy_app/src/filters.py`: remove `search_goalkeepers` (line 210) **only if** no remaining caller (verify with `rg "search_goalkeepers" hf_taipy_app/src`); else leave with a `# used by …` note.
- Modify `hf_taipy_app/src/state/shared.py:215`: keep the `"Goalkeeper-Analytics"` loading-message key (route reused), update text if desired.
- Modify any read-contract test that referenced gkt-specific `GK_ACTIONS_COLUMNS`/`GK_STATS_COLUMNS` — move the live-mart column coverage to the new `queries/gk_analytics.py` column constants (don't lose mart-contract coverage; see `src/tests/test_gk_tracking_read_contract.py` if present).

- [ ] **Step 1: Delete + clean, then grep for stragglers**

Run: `rg -n "gk_tracking|gkt_|Goalkeeper-Tracking|pages\.goalkeeper|state\.goalkeeper|queries\.goalkeepers|LL_GK_TRACKING_PAGE|_GK_PAGES|gk_selected_player|search_goalkeepers" hf_taipy_app/src`
Expected: zero hits (every reference removed or repointed to `gk_analytics`/`gka_`).

- [ ] **Step 2: Full app gate**

Run (from `hf_taipy_app/`): `uv run --project .. --extra taipy-app pytest src -q` (verify via `grep -c "FAILED\|ERROR"` over the full log, not the pipe exit); then `uv run pyright hf_taipy_app/src` and `uv run ruff check hf_taipy_app/src`.
Expected: all green; no import errors from deleted modules.

- [ ] **Step 3: Commit (await approval)** — `refactor(gk)!: replace both legacy GK pages with insight-views Goalkeeper-Analytics; retire LL_GK_TRACKING_PAGE flag`.

---

## Phase 5 — property/smoke test + deploy

### Task 5.1 — Property / smoke test through the read path (NOT a frozen golden)

**Files:** Create `hf_taipy_app/src/test_gk_analytics_smoke.py` (live-DB, **operator/live-run gate — NOT a CI gate**; the CI regression guard is the dbt singular test in Task 1.0). Use the **real-host skip predicate** from Task 1.3 (the dummy `test-host` from conftest must skip, not connect). Asserts structural/property invariants, not frozen expected values.

- [ ] **Step 1: Write the property/smoke test** — for one real provider+keeper (most defended actions in GS): fetch via `queries/gk_analytics`, run the `gk_insight` pipeline, assert (invariants, not frozen values): fit-ladder has 6 **signed** rows and `best_fit == max`; **IDSSE fix** — `fetch_goals_prevented("idsse")` returns rows (proves the Task-1.0 NULL-safe join; the unfixed mart returned 0 IDSSE), and the selected IDSSE keeper's row has non-null `goals_prevented_ci_low/high`; **M3** — that row's `player_key == the selected keeper's gk_player_key`; a thin keeper's band straddles 0 (`ci_low <= 0 <= ci_high`); `ranking_enabled` never surfaced / `goals_prevented_pctile` never read; display uses names not ids; **B1a** — the page `description`/`empty_message` contains the tracking-cohort-scope note (GS/SC/IDSSE; StatsBomb/Wyscout absent).
- [ ] **Step 2: Run** `uv run --project .. --extra taipy-app pytest src/test_gk_analytics_smoke.py -v` (operator/live run — skips in CI by the real-host predicate) → PASS. **Step 3: Commit.**

### Task 5.3 — Post-implementation audits (M2)

- [ ] **Step 1:** Run `mad-scientist-skills:chart-choice-audit` on `state/gk_analytics_charts.py` (fit-ladder + sweeper profile) and `mad-scientist-skills:cognitive-interface-audit` on the page. Confirm: CVD-safe diverging palette (no red/green sign encoding), every value has scale+direction, `inferred`/`low_sample`/`measured` labelled, no raw ids. Fix findings; re-run. **Step 2: Commit** any fixes.

### Task 5.2 — Local verification + staging deploy

- [ ] **Step 1:** Boot locally (`cd hf_taipy_app && set -a && source .env && set +a && .venv/Scripts/python.exe src/main.py` — **no flag**; the page is now unconditional), open `:7860` → Goalkeeper Analytics; check both sub-views render, signed fit-ladder, sweeper profile, verdict cards, goals-prevented band (incl. an IDSSE keeper), `low_sample` pills, names-only. Confirm the legacy event page is gone and the route serves the new page.
- [ ] **Step 1b: REMOVED — `spread_threshold` is derived dynamically, not calibrated/frozen (Plan-review-v3).** The state layer computes it each refresh as the **median per-keeper `fit_spread` across the in-memory provider cohort** (the cohort is already fetched) and passes it to `offensive_verdict(..., spread_threshold=...)`. No frozen constant, no staleness, no calibration query. Add a small pure helper `median_fit_spread(cohort_fit_spreads) -> float` in `gk_insight.py` with a unit test, and assert in the state test that `gka_refresh` passes a cohort-derived threshold (not a literal).
- [ ] **Step 2:** Full local gate: `uv run ruff check hf_taipy_app/src`, `uv run ruff format --check hf_taipy_app/src`, `uv run pyright hf_taipy_app/src`, `uv run --project .. --extra taipy-app pytest hf_taipy_app/src -q` (verify via `grep -c "FAILED\|ERROR"`).
- [ ] **Step 3 (await approval):** wheel bump if any dbt/SQL or `SYNCED_TABLES` change shipped (it did — Task 1.1); `uv run python scripts/bump_wheel.py`; deploy `uv run python scripts/manage_space.py deploy staging --skip-grants-check` after verifying app-SP grants. Poll to RUNNING; verify on the direct URL `https://luxury-lakehouse-staging.hf.space`.

---

## Open items for reviewer (defensible defaults chosen; flag to change)

- **Floor/threshold constants** (named in `gk_insight.py`, tunable): `reference_band` min_cohort=8; `offensive_verdict` `spread_threshold` is **derived dynamically** (median cohort `fit_spread`, not a frozen constant — Plan-review-v3); sweeper `n_defended` floor 30; distribution `n_distributions` floor 20; terciles at 33.3/66.7 pct. (`low_sample` is computed in the pooled mart at `shots_faced_total < 5` — not an app constant.)
- **Page layout** — RESOLVED: SubView layout with per-view `Metric` cards (required by `test_tier_a_canon`; OUR VERDICT + KPI references baked into `Metric` var strings, Match-Summary style). Not the dashboard StatCard strip.
- **Both legacy GK pages replaced this cycle** (owner-decided) — RESOLVED: new page takes the `Goalkeeper-Analytics` route unconditionally; `LL_GK_TRACKING_PAGE` flag retired; both `pages/goalkeeper.py` and `gk_tracking.py` deleted (Tasks 4.4/4.5). No route collision.
- **Pooled-mart IDSSE bug — fixed in-PR (Task 1.0)**: the NULL-season `INNER JOIN` is replaced with a NULL-safe `<=>` join + a dbt singular regression test; the app reads `fct_gk_shot_stopping_pooled` directly. Recommended-but-separate (flag to producer, not gating): resolve IDSSE `season_id` upstream in `fct_gk_shot_stopping` so the NULL bucket disappears for all consumers.
- **CI-guard reality (corrected, Plan-review-v4)**: `dbt-ci.yml` runs `dbt parse` only — the singular test (Task 1.0) does NOT gate the PR; it runs in the **daily** `dbt-live-ci.yml` (≤24h post-merge, automated). The app-side contract (1.3) + property test (5.1) are **manual operator/live** gates. **Nothing blocks the merge itself** except the pure unit tests — including the new SQL text-assertion guard (Task 1.0 Step 6) that fails a PR if the `<=>` fix is reverted. Given the page hits prod with no kill-switch, that text guard is the only merge-time protection for the IDSSE fix.
- **Flag-retirement risk note:** because the page is now unconditional, the swap goes live on merge to main (prod). Safety is the feature-branch + staging-Space verification (Phase 5) before merge — there is no in-code kill-switch, so Phase 5 staging sign-off is mandatory before the merge that lands this.

---

## Self-review notes (author)

- Spec coverage: §3 fit-ladder (2.6/3.1/4.1), §4 sweeper (2.3/3.2/4.1), measured chip (2.3), verdicts §11a (2.4), **goals-prevented = fixed pooled mart read directly (Task 1.0 join fix + 1.1 sync + 1.2 select; Task 2.5 removed)**, per-provider §0 C2 (queries 1.2 + provider-first 4.1), bands/floors §0 D2/N3 (2.1), terciles within-comp §0 D4 (2.2 + 1.1b line mart), no-ranking (never surfaced), cleanup/rollback (Phase 0). ✓
- Type consistency: `Tercile`/`CommandPos`/`Verdict`/`ReferenceBand`/`FitLadder` defined in 2.x and consumed unchanged in 3.x/4.x. (`GoalsPreventedBand` removed with Task 2.5 — goals-prevented columns come straight from the pooled mart; no app dataclass.) ✓
- Route-name collision RESOLVED (owner: both legacy GK pages replaced this cycle; new page owns `Goalkeeper-Analytics` unconditionally; `LL_GK_TRACKING_PAGE` retired). No open blocking decisions remain; residuals: the upstream IDSSE `season_id` data-quality fix (recommended to producer; routed around via Task 1.0) and the IDSSE-no-band owner-ack. **Merge-time protection = pure unit tests incl. the Task-1.0 Step-6 `<=>` text guard; the dbt singular test + app live tests are post-merge (daily-live / manual operator).**
