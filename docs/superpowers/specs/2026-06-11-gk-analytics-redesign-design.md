# GK Analytics Page Redesign — design spec

**Date:** 2026-06-11 · **Author:** Karsten (with Claude, d32 session) · **Status:** Draft — for
cross-session review. **Origin:** GK page redo investigation (2026-06-10), three decision rounds
with owner, v2 synthetic + v3 real-data prototypes.

## 1. Executive summary

Complete redo of the Goalkeeper Analytics page as a **three-tab, tracking-data-only** page built on
`fct_action_context`-derived marts — the platform's first consumer of the new AC GK column families
(xT-GK with six switchable philosophy presets, Ghost-GK positioning, GK influence zones, pre-shot
geometry). The page targets professional-coach credibility: every tab answers a coaching question,
and the flagship interaction — re-valuing the same distributions under a different game model,
instantly — exists nowhere else publicly.

**Deployment strategy (owner-decided):** new marts + new page modules land **side-by-side** with
the existing GK page and marts (untouched — nothing else consumes action-context yet). The new
page registers only when `LL_GK_TRACKING_PAGE=1` (set on the **staging** Space, absent in
production), so production is bit-identical until final sign-off. Cutover (old page retirement,
flag removal) is a separate, later, explicitly-approved PR.

**Pre-review audits:** planning-mode architecture/security/observability audits ran 2026-06-11;
all findings fixed inline — record at
`docs/superpowers/specs/2026-06-11-gk-analytics-redesign-audit-findings.md`. Audit-mode passes
(chart-choice, cognitive-interface, optimization/EXPLAIN) run post-implementation (plan Task 9).

**Prototypes (normative for layout):** `docs/ui-cycles/gk-redesign/mockups/v3_tab{1,2,3}_*.png`
(real bronze data) with `v2_*` synthetic predecessors; reproducible via
`docs/ui-cycles/gk-redesign/{extract_proto_data,generate_mockups}.py`. Chart choices were audited
against Kirk's methodology (`docs/ui-cycles/gk-redesign/kirk-chart-audit.md`): pressure/closing
comparisons are connected dot plots (the gap is the story), the preset switcher is a bump chart,
radar was dropped (owner: too common).

## 2. The three tabs

| Tab | Question it answers | Primary visuals (mockup) |
|---|---|---|
| **1 · Distribution Value** | "What is his passing worth under YOUR game model?" | Philosophy chip selector → preset-rank bump chart; same-passes-two-presets pitch-map pair; per-distribution xT-GK map (v3_tab1) |
| **2 · Defensive Positioning & Box Command** | "Is he standing where the league's keepers would — and when does he leave that line?" | Ghost-GK tether scene (real frame, density grid); deviation vs defensive-line-height (and game-state, once upstream fix lands); closing-time dumbbells + reachable area / box pitch-control share (v3_tab2) |
| **3 · Shot-Stopping Geometry** | "Was the save makeable from where he stood?" | Pre-shot cone scene; all-shots map (distance-off-line × lateral error, goals starred) (v3_tab3) |

Tab 3 gains **Goals Prevented** when TF-48 (silly-kicks shot-goalmouth → PSxG reuse) lands —
designed as an additive column/metric, not a layout change.

## 3. Provider gating

- **In scope:** `gradientsports`, `idsse`, `skillcorner` — full tracking, all three tabs fully
  populated. **Metrica: excluded** (anonymized players violate "raw IDs never reach the user").
- **StatsBomb 360: excluded from v1, door left open.** SB360 unlocks at most a *subset* of Tab 2/3
  (closing times + pre-shot geometry from freeze-frames) under a **different estimator**
  (`pitch_control_method='voronoi'` — the dataset card explicitly forbids pooling with the
  tracking providers' `spearman` rows), with sparse non-random coverage, and **cannot support
  Tab 1 at all** (xT-GK requires tracking). A provider whose flagship tab is empty recreates the
  dead-end-dropdown failure the old page fixed. **Mechanism:** the marts stay provider-agnostic
  (rows flow for whatever providers AC produces, `pitch_control_method` carried); the UI gates on
  a single `GK_TRACKING_PROVIDERS` constant in the queries module. Adding SB360 later as a
  labeled "limited coverage" tier is a one-constant change plus per-chart estimator segmentation
  — explicitly deferred. (Empirically, the current v4 bronze sample has zero statsbomb GK-metric
  rows anyway.)

## 4. Data architecture (new, side-by-side)

Two new marts, ADR-013-style consumption of the AC staging view; **no existing mart is modified**.

### 4.1 `fct_gk_tracking_actions` (action grain)

One row per tracking-provider action from `stg_action_context__values`, projecting the GK families
plus Kimball keys and two computed columns:

- Identity/keys: `gk_action_id` (surrogate: match_key+action_id), `match_key`, `team_key`,
  `player_key`, `defending_gk_player_key`, `data_source`, `action_id`, `period_id`,
  `time_seconds`, `type_name`, `game_state`, coords, `frame_id`.
- Distribution family (NULL off-domain): `xt_gk` + 5 preset columns + 5 component columns +
  `gk_completion`, `pressure_on_actor__andrienko_oval`, `gk_was_distributing`.
- Defensive family: `ghost_gk_x/y/density_spread`, `ghost_gk_method`,
  `gk_pitch_control_share_weighted`, `gk_reachable_area_m2`, six `gk_closing_time_*` columns,
  `defensive_line_x`, `pitch_control_method`.
- Shot family (NULL off-domain): six `pre_shot_gk_*` columns.
- Computed: **`line_height_m`** (defensive line distance from the defending goal,
  orientation-normalized), **`ghost_deviation_m`** (actual-vs-ghost distance, shot rows only),
  and **`gk_actual_x` / `gk_actual_y` / `gk_frame_mirrored`** — the canonical-frame actual GK
  position + the mirror flag, stored so the orientation heuristic is applied EXACTLY ONCE (in
  the dbt macro) and never re-derived client-side (architecture-audit A1, 2026-06-11; the app
  uses `gk_frame_mirrored` only to flip frame-oriented PLAYER dots in the scene).
- Outcome: `action_result` via `LEFT JOIN fct_action_values ON (match_key, action_id)` (nullable
  by design — outcome enrichment, not an identity gate).
- Filter: `data_source IN ('gradientsports','idsse','skillcorner','metrica')` — metrica kept in
  the MART (data layer stays neutral), excluded in the UI constant.
- Incremental merge on `gk_action_id`, liquid-clustered by `match_key`, CDF on,
  `contract: enforced: true`, `on_schema_change: append_new_columns`.

**Known data-contract caveats** (cross-session review 2026-06-11 incorporated):

- **Orientation reconciliation:** `ghost_gk_*` is canonical (defended goal x≈0) while
  `pre_shot_gk_*` is frame-oriented. The mirror flag is derived from the **stored
  `pre_shot_gk_distance_to_goal` anchor** (review H3): the defended goal is whichever end's
  distance residual matches the stored value — exact for every GK position including sweeping
  keepers, where the naive `|Δx| > 52.5` rule mis-mirrors by ~15 m on precisely the rows Tab 3
  calls interesting. The 52.5 positional rule survives only as the documented residual-tie
  tiebreak. Single-macro home unchanged.
- **Domain coupling is ONE-directional** (review C1, verified live): GS v4 has 139
  `gk_completion` rows vs 124 `xt_gk` rows — the completion model scores some distributions where
  xT-GK aborts. The singular test encodes `xt_gk IS NOT NULL → gk_completion IS NOT NULL` only;
  the completion-only GS rows are an open upstream question (relayed).
- **Value-level caveats, split by status** (review M1/M2): *confirmed by design* — `xt_gk_pev≈0`
  (silly-kicks verdict 2026-06-11: pev = ρ·max(0, forward progress), ρ=0 with no opponent in the
  ~9 m oval — structurally 0 on goal kicks; any PEV display must carry a "≈0 on unpressured
  restarts by construction" caption); *verify-before-calling-bug* — `xt_gk_base` 100 % negative
  (plausibly the risk-adjusted floor); *open upstream* — `game_state` 99.9 % 'drawing' on the v4
  sample. Only the last blocks anything: the game-state split ships UI-ready but hidden behind
  the data check, with caption.

### 4.2 `fct_gk_tracking_stats` (GK × match grain)

Grain: `(gk_player_key, match_key)`. A goalkeeper appears in two roles, aggregated separately
from the actions mart: **distribution_** aggregates (rows where the GK is the actor and
`gk_was_distributing`) — n, mean/sum for each of the 6 preset composites, mean `gk_completion`,
mean pressure; **defense_** aggregates (rows where the GK is `defending_gk_player_key`) — shots
faced, goals conceded (`action_result='success'` on shot rows against; own goals excluded by
construction — their result is `'owngoal'`, not `'success'`), mean `ghost_deviation_m`,
mean closing times per zone, mean reachable area / pitch-control share. Surrogate
`gk_match_stat_id`; contract enforced; **incremental/merge on `gk_match_stat_id` with a
full-recompute-then-merge body** (review H1: a `table`-materialized TRIGGERED synced mart
STRANDS its synced table on every rebuild per ADR-043 amendment 2, triggering the ADR-041 heal's
re-snapshot downtime — merge writes avoid the strand entirely; full recompute is cheap at this
grain).

### 4.3 Lakebase

Both marts get synced tables (`fct_gk_tracking_actions_synced` TRIGGERED on
`gk_action_id`; `fct_gk_tracking_stats_synced` TRIGGERED on `gk_match_stat_id`), registered in
**BOTH** registries (review C2): `refresh_synced_tables.py` `SYNCED_TABLES` AND the
`triggered_synced_marts` var in `dbt_project/dbt_project.yml` — `test_strand_safe_rederive.py`
enforces parity, and the ADR-043 on-run-start tripwire then forbids `--full-refresh` selecting
them. Created via `scripts/create_synced_table.py`, grants/indexes via the ADR-005 maintenance
pattern. Indexes (actions table; ~120K rows at full corpus):
`(defending_gk_player_key, match_key)`, `(player_key, match_key)`, `(match_key, action_id)`.
Stats table: dimension-sized, no custom indexes. **Scene-frame query** (review M5): verify a
covering composite index on `fct_tracking_frames_synced (match_key, period, frame)` exists —
it is the platform's largest synced table and that query is on the ≤500 ms interaction path;
add it to the EXPLAIN gate list.

## 5. App architecture

New modules, prefix `gkt_`; the old page/state/queries remain untouched.

| File | Responsibility |
|---|---|
| `hf_taipy_app/src/queries/gk_tracking.py` | SQL (psycopg2 `%s` params, LIMITs, `GK_TRACKING_PROVIDERS` constant with DERIVED placeholder string — review M4); LOVs from the stats table; **pool-wide stats query** (review H2 — feeds the Tab 1 bump chart and every "vs sample" right-rail delta); `GK_ACTIONS_COLUMNS` + `GK_STATS_COLUMNS` constants (review M3 — both reconciled against the dbt contracts) |
| `hf_taipy_app/src/state/gk_tracking.py` | `gkt_` state vars, 3 sub-views, chart builders (Plotly, app palette), preset switching (client-side column swap — all 6 preset aggregates fetched in one row) |
| `hf_taipy_app/src/pages/gk_tracking.py` | `PageConfig` (sub_views, metrics with help_text, citations, scope dims) |
| `hf_taipy_app/src/services/ghost_grid.py` | **Hexagonal port** `GhostGridProvider` with two adapters: `StoredSpreadProvider` (blob from `ghost_gk_x/y/density_spread` — always works) and `ModelGridProvider` (silly-kicks ghost-GK model rendering the true 60×64 grid; owner decision #6). **Adapters are PURE computation** — the port signature takes `frame_players: pd.DataFrame | None`; the STATE layer fetches the frame from Lakebase and passes it in (architecture-audit A2: no services→queries I/O edge, adapters testable without a DB). Selection by env (`LL_GHOST_GRID=model|stored`, default `stored`); failures degrade loudly to stored (ERROR log, on-chart caption switches per "never silently substitute data") |
| `hf_taipy_app/src/main.py` | conditional registration: import + `PageEntry` only when `LL_GK_TRACKING_PAGE=1` |
| `hf_taipy_app/src/template.py` | `PAGE_TERMS["Goalkeeper-Tracking"]` + new `GLOSSARY` entries (xT-GK, PEV, RAV, DZV, Ghost GK, closing time, reachable area, line height) |

Citations (page + `NOTICE` in same change): Eyestone xT-GK (practitioner, course materials);
Poole USWNT GK profile (IGCC 2022, course materials); ghost-GK conditional-density lineage
(silly-kicks model card + NFL-Ghosts-style CDE); GK influence zones (Spearman pitch-control
lineage, already in NOTICE via AC). UX standards: every value gets scale+direction; real names
only (dim joins); the REAL-DATA-vs-model-grid provenance is surfaced on the ghost scene.

Per-tab metric columns (right rail), from the stats table for the selected GK: Tab 1 — xT-GK/90*
(selected preset, with n), completion vs sample, pressure-split delta; Tab 2 — mean deviation (m),
closing-time vs sample, reachable area (pct); Tab 3 — shots faced, goals conceded, mean off-line
distance. (*per-distribution mean until volumes justify /90 — labels carry n.)

## 6. Testing strategy (TDD / hexagonal / e2e)

- **dbt:** contracts enforced on both marts; schema tests (unique surrogate, accepted providers);
  singular tests: `assert_gk_actions_domain_consistency.sql` (xt_gk ⇔ gk_completion non-null
  together; ghost_deviation only on shot rows), `assert_gk_stats_reconciles_actions.sql`
  (stats n_distributions == count over actions mart). Live CI builds on PR (dbt-live-ci).
- **App unit tests** (existing `hf_taipy_app/src/test_*.py` pattern, run by root pytest via
  pyright/pytest path config): query builders return (sql, params) — assert provider gate, LIMITs,
  `%s` placement; state pure helpers (formatting, preset column mapping, deviation labels);
  ghost service port contract with a fake adapter + stored-spread adapter math; model adapter
  behind `importorskip("silly_kicks")`.
- **Read-side contract reconciliation** (architecture-audit A3/A4 — the ADR-002 §4 parity
  pattern, applied read-side): the app declares its expected mart columns as module constants
  (`GK_ACTIONS_COLUMNS`, `PRESET_COLUMN` values); query builders enumerate columns from those
  constants (no `SELECT *`); a test parses the `_marts__models.yml` contracts and asserts the
  app's expected columns ⊆ contract columns — a mart column rename then fails CI on the read
  side, not silently in the Space.
- **e2e:** local Taipy launch (`cd hf_taipy_app && python src/main.py` with `LL_GK_TRACKING_PAGE=1`
  + Lakebase env) + puppeteer screenshot pass of all three tabs against the v3 mockups; EXPLAIN
  ANALYZE on every new synced-table query (Index Scan required on the actions table).
- **Audits (explicit plan tasks):** `mad-skills:chart-choice-audit` (page),
  `mad-scientist-skills:cognitive-interface-audit` (audit mode),
  `mad-scientist-skills:optimization-audit` scope: new queries + state modules;
  `test_ai_governance_md` + `test_citation_consistency` + NOTICE/appendix-D parity re-run.

## 7. Dependencies & sequencing

1. **AC recompute + staging-view rebuild (other session, in flight)** — hard dependency for live
   mart builds; dbt `parse`/compile and all app unit tests proceed without it.
2. Upstream silly-kicks value fixes (PEV, base sign, game_state) — value-level only; no schema
   coupling. Coordinate-convention unification → single-macro change here.
3. TF-48 PSxG — additive follow-up (Tab 3 Goals Prevented).
4. Old-page cutover + Metrica/SB360 revisit — separate future PRs.

## 8. Rollout & guardrails

Branch `feat/gk-tracking-page`; **single commit, only after `/final-review` and explicit owner
approval** (repo git policy). Staging deploy via `manage_space.py deploy staging` with
`LL_GK_TRACKING_PAGE=1` set in the staging Space settings only. **Security-audit S1: the staging
Space must be private/org-visibility while the flag is on** — it exposes both an unreviewed page
and GS per-player WC2022 metrics; verify visibility before setting the flag, and re-verify the
GS-display decision at cutover. Production deploys remain safe at any time (flag absent → page
not registered; marts are additive and unconsumed elsewhere). Ghost model supply chain
(security-audit S2): the `ModelGridProvider` loader pins the HF Hub model **revision** (commit
hash, not `main`); loader format is npz — confirm no `allow_pickle` path at implementation.
Grids are cached per `gk_action_id` (S3, also a latency win).
ADR-051 (number reconciled at PR time) records: side-by-side mart family, env-flag staging gating,
in-Space ghost-model rendering with stored fallback, the deviation orientation heuristic, and the
SB360/Metrica gating decision.

## 9. Open items — RESOLVED (cross-session review 2026-06-11; owner can override)

1. Mart names: **keep** `fct_gk_tracking_actions/stats` (reviewer: consistent with grain-suffix
   convention).
2. Stats grain: **per-match confirmed**, contingent on the H1 merge materialization (applied).
3. `ModelGridProvider`: **v1 ships the port + StoredSpreadProvider only**; the model adapter is a
   fast-follow gated on silly-kicks exposing a PUBLIC loader entrypoint (`_ghost_gk_model_cached`
   is private — shipping against it bakes a break into the next silly-kicks bump; request filed
   with the silly-kicks session).
4. Game-state split: **keep data-gated + captioned** (the 99.9 % 'drawing' sample is a real open
   upstream question; hidden-with-caption is correct for v1).
