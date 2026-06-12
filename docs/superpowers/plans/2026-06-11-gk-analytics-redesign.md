# GK Analytics Page Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans (inline — this repo's
> owner policy prefers inline execution; Agent calls need per-call approval). Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** Three-tab tracking-only GK Analytics page (Distribution Value / Defensive Positioning &
Box Command / Shot-Stopping Geometry) on two new `fct_action_context`-derived marts, deployed
side-by-side with the existing page, staging-gated via env flag.

**Architecture:** dbt staging view → two new contracted marts (action grain + GK×match grain) →
Lakebase synced tables → new Taipy queries/state/page modules (prefix `gkt_`), ghost-grid service
behind a hexagonal port. Old GK page/marts untouched. Spec is the contract:
`docs/superpowers/specs/2026-06-11-gk-analytics-redesign-design.md` — where plan and spec
disagree, the spec wins; flag, don't improvise.

**Tech Stack:** dbt (Databricks), Lakebase/psycopg2, Taipy + Plotly, silly-kicks (ghost model
adapter only), pytest.

**REPO POLICY OVERRIDES of the generic skill template (these win):**
- **NO per-task commits.** `git commit`, `git push`, `gh pr create` happen ONCE, at the end, only
  after `/final-review` AND explicit owner approval. Every "Commit" step in the generic template
  is replaced by "run the relevant tests".
- **No worktrees.** Branch `feat/gk-tracking-page` off `main`.
- Shift Left before declaring done: `uv run ruff format --check src/ scripts/ hf_taipy_app/` AND
  `uv run ruff check src/ scripts/ hf_taipy_app/` AND `uv run pyright src/` AND
  `uv run pytest src/tests/ -v` all green.
- **Wheel bump rule:** dbt YAML/SQL changes ride the wheel — bump `pyproject.toml` version and run
  `uv run python scripts/bump_wheel.py` in the same change (Task 10).
- **Hard dependency:** live mart builds need the AC recompute + staging-view rebuild (other
  session). Tasks 2–3 are written to pass `dbt parse` locally regardless; live `dbt build` of the
  new marts is gated on that landing (Task 11 checks it explicitly).

---

## File map

| File | Action | Responsibility |
|---|---|---|
| `docs/superpowers/adrs/ADR-051-gk-tracking-page-side-by-side.md` | Create | decision record (number re-verified at PR time) |
| `dbt_project/macros/gk_tracking_geometry.sql` | Create | ONE home for the orientation heuristic (mirror flag, deviation, line height) |
| `dbt_project/models/marts/fct_gk_tracking_actions.sql` | Create | action-grain GK mart |
| `dbt_project/models/marts/fct_gk_tracking_stats.sql` | Create | GK×match aggregate mart |
| `dbt_project/models/marts/_marts__models.yml` | Modify | contracts + schema tests for both marts |
| `dbt_project/tests/assert_gk_actions_domain_consistency.sql` | Create | domain-coupling singular test |
| `dbt_project/tests/assert_gk_stats_reconciles_actions.sql` | Create | stats↔actions reconciliation |
| `src/ingestion/refresh_synced_tables.py` | Modify | register 2 synced tables |
| `src/tests/test_refresh_synced_tables_detect.py` | Modify | registration covered by existing parametrized tests (verify) |
| `hf_taipy_app/src/queries/gk_tracking.py` | Create | SQL builders + fetchers, `GK_TRACKING_PROVIDERS` |
| `hf_taipy_app/src/test_gk_tracking_queries.py` | Create | query-builder unit tests |
| `hf_taipy_app/src/services/ghost_grid.py` | Create | GhostGridProvider port + stored/model adapters |
| `hf_taipy_app/src/test_ghost_grid.py` | Create | port-contract + adapter tests |
| `hf_taipy_app/src/state/gk_tracking.py` | Create | `gkt_` state, 3 sub-views, chart builders |
| `hf_taipy_app/src/test_gk_tracking_state.py` | Create | pure-helper tests |
| `hf_taipy_app/src/pages/gk_tracking.py` | Create | PageConfig |
| `hf_taipy_app/src/main.py` | Modify | env-flag-gated registration |
| `hf_taipy_app/src/template.py` | Modify | PAGE_TERMS + GLOSSARY entries |
| `NOTICE` | Modify | Eyestone / Poole / ghost-GK CDE entries |
| `pyproject.toml` + `src/shared/wheel.py` (via script) | Modify | wheel bump |

### Task 0: Branch + baseline

- [ ] **Step 0.1:** `git checkout main && git pull && git checkout -b feat/gk-tracking-page`
- [ ] **Step 0.2:** Baseline: `uv run pytest src/tests/ -q -x --ignore=src/tests/action_context`
      → PASS (record runtime). `uv run dbt parse --project-dir dbt_project` → PASS.

### Task 1: ADR-051

**Files:** Create `docs/superpowers/adrs/ADR-051-gk-tracking-page-side-by-side.md`
(verify 050 is next: `ls docs/superpowers/adrs/`).

- [ ] **Step 1.1:** Write the ADR in the Nygard template (`ADR-TEMPLATE.md`), recording exactly
      these decisions (full rationale per spec §3/§4/§5/§8):
      1. New side-by-side mart family (`fct_gk_tracking_actions/stats`) instead of extending
         `fct_goalkeeper_stats` — old marts untouched; cutover is a later PR.
      2. Env-flag staging gating (`LL_GK_TRACKING_PAGE=1`) instead of a branch-divergent deploy.
      3. In-Space ghost-model rendering as a hexagonal port with stored-spread fallback
         (`LL_GHOST_GRID=model|stored`, default `stored`).
      4. The orientation reconciliation for `gk_actual_*`/`ghost_deviation_m`/`line_height_m`
         (review R1 — record the CURRENT design, not the superseded one): the mirror flag is
         anchored on the stored `pre_shot_gk_distance_to_goal` — the defended goal is whichever
         end's distance residual matches the stored value (exact for sweeping keepers); the
         positional `x > 52.5` rule survives ONLY as the residual-tie tiebreak. Single-macro
         home (`gk_tracking_geometry.sql`), REVISIT marker pointing at the upstream
         coordinate-convention relay (2026-06-11).
      4b. **Orphan-row policy for the merge marts (review R2):** merge never deletes. The stats
         mart self-heals via an orphan-sweep `post_hook` (anti-join against the actions mart —
         exact and cheap at its grain). The ACTIONS mart's orphans (AC wipe / selective
         recompute / provider re-ingest) are handled by the EXISTING platform practice for the
         AC mart family — operator `DELETE FROM fct_gk_tracking_actions WHERE match_key ...` (or
         full-table delete) before re-derive, alongside the ADR-043 re-derive tooling
         (`scripts/rederive_synced_marts.py`); record this in the ADR Operations note so the
         next AC wipe doesn't leave a stats/actions disagreement.
      5. Provider gating: GS/IDSSE/SkillCorner in UI; Metrica + SB360 excluded with the spec §3
         reasoning; mart stays provider-agnostic.
- [ ] **Step 1.2:** `uv run pytest src/tests/test_architecture_md_appendix.py -q` → PASS (no new
      academic authors yet; Eyestone/Poole are practitioner citations — appendix unaffected;
      verify, don't assume).

### Task 2: Geometry macro + `fct_gk_tracking_actions`

**Files:** Create `dbt_project/macros/gk_tracking_geometry.sql`,
`dbt_project/models/marts/fct_gk_tracking_actions.sql`,
`dbt_project/tests/assert_gk_actions_domain_consistency.sql`; Modify
`dbt_project/models/marts/_marts__models.yml`.

- [ ] **Step 2.1: Macro (the ONE home for the heuristic):**

```sql
-- gk_tracking_geometry.sql
-- Orientation reconciliation between ghost_gk_* (canonical, defended goal at x~0) and
-- pre_shot_gk_* / defensive_line_x (frame-oriented; defended end varies by team/period).
-- ANCHOR (review H3, 2026-06-11): the defended goal is identified from the STORED
-- pre_shot_gk_distance_to_goal — whichever end's distance residual matches the stored value.
-- Exact for every GK position incl. sweeping keepers (the naive |dx|>52.5 rule mis-mirrors a
-- GK at frame x~60 by ~15 m). The positional rule survives ONLY as the residual-tie tiebreak.
-- REVISIT when the upstream AC coordinate convention is unified — this macro is the single
-- change site. See ADR-051 section 4.

{% macro _gk_dist_residual(frame_x, frame_y, dist_to_goal, goal_x) %}
    abs(sqrt(pow({{ frame_x }} - {{ goal_x }}, 2) + pow({{ frame_y }} - 34.0, 2)) - {{ dist_to_goal }})
{% endmacro %}

{% macro gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) %}
    (case
        when {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '105.0') }}
           < {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '0.0') }} then true
        when {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '0.0') }}
           < {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '105.0') }} then false
        else {{ frame_x }} > 52.5  -- residual tie (degenerate midfield case): positional tiebreak
    end)
{% endmacro %}

{% macro gk_actual_canonical_x(frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 105.0 - {{ frame_x }} else {{ frame_x }} end)
{% endmacro %}

{% macro gk_actual_canonical_y(frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 68.0 - {{ frame_y }} else {{ frame_y }} end)
{% endmacro %}

{% macro gk_line_height_m(defensive_line_x, frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 105.0 - {{ defensive_line_x }} else {{ defensive_line_x }} end)
{% endmacro %}
```

- [ ] **Step 2.2: Mart SQL:**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='gk_action_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart'],
    tblproperties={'delta.enableChangeDataFeed': 'true'}
) }}
-- fct_gk_tracking_actions.sql
-- Action-grain GK analytics projection of action-context, tracking providers only.
-- SIDE-BY-SIDE with the legacy GK marts (ADR-051) — nothing legacy is modified.
-- Grain: one row per (match_key, action_id).

with ac as (
    select *
    from {{ ref('stg_action_context__values') }}
    where data_source in ('gradientsports', 'idsse', 'skillcorner', 'metrica')
),

keyed as (
    select
        dm.match_key,
        dt.team_key,
        dp.player_key,
        dp_gk.player_key as defending_gk_player_key,
        ac.*
    from ac
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = ac.data_source and dm.native_match_id = ac.native_match_id
    left join {{ ref('dim_teams') }} dt
        on dt.provider = ac.data_source and dt.native_team_id = ac.team_id_native
    left join {{ ref('dim_players') }} dp
        on dp.provider = ac.data_source and dp.native_player_id = ac.player_id_native
    left join {{ ref('dim_players') }} dp_gk
        on dp_gk.provider = ac.data_source
       and dp_gk.native_player_id = ac.defending_gk_player_id_native
),

with_outcome as (
    select
        k.*,
        av.action_result
    from keyed k
    left join {{ ref('fct_action_values') }} av
        on av.match_key = k.match_key and av.action_id = k.action_id
)

select
    {{ dbt_utils.generate_surrogate_key(['match_key', 'action_id']) }} as gk_action_id,
    match_key,
    team_key,
    player_key,
    defending_gk_player_key,
    data_source,
    action_id,
    period_id,
    time_seconds,
    type_name,
    game_state,
    start_x, start_y, end_x, end_y,
    frame_id,
    action_result,
    -- distribution family (NULL off-domain by upstream design)
    gk_was_distributing,
    xt_gk, xt_gk_possession, xt_gk_counter, xt_gk_direct, xt_gk_high_press, xt_gk_low_block,
    xt_gk_base, xt_gk_pev, xt_gk_rav, xt_gk_dzv, xt_gk_pressure,
    gk_completion,
    pressure_on_actor__andrienko_oval,
    -- defensive family
    ghost_gk_x, ghost_gk_y, ghost_gk_density_spread, ghost_gk_method,
    gk_pitch_control_share_weighted, gk_reachable_area_m2,
    gk_closing_time_mean_s__six_yard_box, gk_closing_time_min_s__six_yard_box,
    gk_closing_time_mean_s__near_post, gk_closing_time_min_s__near_post,
    gk_closing_time_mean_s__far_post, gk_closing_time_min_s__far_post,
    defensive_line_x,
    pitch_control_method,
    -- shot family
    pre_shot_gk_x, pre_shot_gk_y,
    pre_shot_gk_distance_to_goal, pre_shot_gk_distance_to_shot,
    pre_shot_gk_angle_to_shot_trajectory, pre_shot_gk_angle_off_goal_line,
    -- computed (single-macro heuristic anchored on pre_shot_gk_distance_to_goal — review H3;
    -- ADR-051 section 4; architecture-audit A1: canonical actual position + mirror flag are
    -- STORED so the app never re-derives orientation)
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_frame_mirror_flag('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_frame_mirrored,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_actual_canonical_x('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_actual_x,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
         then {{ gk_actual_canonical_y('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as gk_actual_y,
    case when pre_shot_gk_x is not null and pre_shot_gk_distance_to_goal is not null
              and ghost_gk_x is not null
         then sqrt(
            pow({{ gk_actual_canonical_x('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }} - ghost_gk_x, 2)
          + pow({{ gk_actual_canonical_y('pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }} - ghost_gk_y, 2))
    end as ghost_deviation_m,
    case when defensive_line_x is not null and pre_shot_gk_x is not null
              and pre_shot_gk_distance_to_goal is not null
         then {{ gk_line_height_m('defensive_line_x', 'pre_shot_gk_x', 'pre_shot_gk_y', 'pre_shot_gk_distance_to_goal') }}
    end as line_height_m
from with_outcome
```

- [ ] **Step 2.3: Contract + schema tests** — append to `_marts__models.yml` (mirror the
      `fct_action_context` entry's style; every column above with its type:
      `gk_action_id/match_key/team_key/player_key/defending_gk_player_key/data_source/type_name/
      game_state/action_result/ghost_gk_method/pitch_control_method` → `string`;
      `action_id/period_id/frame_id` → `bigint`; `gk_was_distributing` → `boolean`; all remaining
      metric columns → `double`). Tests:
      `gk_action_id`: `unique` + `not_null`; `data_source`:
      `accepted_values: ['gradientsports','idsse','skillcorner','metrica']`;
      `match_key`: `not_null`; `dbt_utils.unique_combination_of_columns: [match_key, action_id]`.
      Contract additionally includes `gk_frame_mirrored boolean`, `gk_actual_x double`,
      `gk_actual_y double` (A1 columns).
- [ ] **Step 2.4: Singular test:**

```sql
-- assert_gk_actions_domain_consistency.sql
-- Domain coupling is ONE-DIRECTIONAL (review C1, verified live 2026-06-11): GS v4 carries 15
-- gk_completion-only rows (139 completion vs 124 xt_gk) — the completion model scores some
-- distributions where xT-GK aborts. So: xt_gk -> gk_completion holds; the symmetric belief is
-- already falsified. (Completion-only rows are an open upstream question, relayed.)
-- ghost_deviation_m exists only where the shot family exists. Own goals never inflate
-- goals_conceded downstream: their action_result is 'owngoal', not 'success'.
select gk_action_id, 'xtgk_without_completion' as violation
from {{ ref('fct_gk_tracking_actions') }}
where xt_gk is not null and gk_completion is null
union all
select gk_action_id, 'deviation_without_preshot' as violation
from {{ ref('fct_gk_tracking_actions') }}
where ghost_deviation_m is not null and pre_shot_gk_x is null
```

- [ ] **Step 2.5:** `uv run dbt parse --project-dir dbt_project` → PASS. **Parse-level only**
      (review M6): parse validates jinja/refs/contract syntax — it does NOT prove the staging
      view's columns exist. Column resolution is proven in Task 11; do not treat this PASS as
      schema proof.

### Task 3: `fct_gk_tracking_stats`

**Files:** Create `dbt_project/models/marts/fct_gk_tracking_stats.sql`,
`dbt_project/tests/assert_gk_stats_reconciles_actions.sql`; Modify `_marts__models.yml`.

- [ ] **Step 3.1: Mart SQL:**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='gk_match_stat_id',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart'],
    tblproperties={'delta.enableChangeDataFeed': 'true'},
    post_hook="""
        delete from {{ this }} t
        where not exists (
            select 1 from {{ ref('fct_gk_tracking_actions') }} a
            where a.match_key = t.match_key
              and (a.player_key = t.gk_player_key or a.defending_gk_player_key = t.gk_player_key)
        )
    """
) }}
-- fct_gk_tracking_stats.sql
-- ORPHAN SWEEP (review R2): merge never deletes — without the post_hook above, a
-- (gk, match) row whose underlying actions disappear (AC wipe + selective recompute — which
-- happened twice the week this was designed) lingers with stale values and trips the
-- reconciliation test. The anti-join is exact and cheap at this grain. The ACTIONS mart's
-- orphan policy is operator-driven (ADR-051 Operations note).
-- Grain: one row per (gk_player_key, match_key). A GK appears in two roles:
-- actor of distributions (player_key + gk_was_distributing) and defender of shots
-- (defending_gk_player_key) — aggregated separately, FULL OUTER joined.
--
-- MATERIALIZATION (review H1): incremental/merge with a FULL-recompute body — deliberately NO
-- is_incremental() filter. The aggregate is recomputed in full every run (cheap at this grain)
-- but written via MERGE, because a `table` rebuild of a TRIGGERED synced mart STRANDS its
-- synced table (ADR-043 amendment 2) and forces the ADR-041 heal's re-snapshot downtime.
-- Merge writes never change the Delta table id, so the synced table stays attached.

with actions as (
    select * from {{ ref('fct_gk_tracking_actions') }}
),

distribution as (
    select
        player_key as gk_player_key,
        match_key,
        max(data_source) as data_source,
        count(*) as n_distributions,
        avg(xt_gk) as dist_xt_gk_mean,
        avg(xt_gk_possession) as dist_xt_gk_possession_mean,
        avg(xt_gk_counter) as dist_xt_gk_counter_mean,
        avg(xt_gk_direct) as dist_xt_gk_direct_mean,
        avg(xt_gk_high_press) as dist_xt_gk_high_press_mean,
        avg(xt_gk_low_block) as dist_xt_gk_low_block_mean,
        avg(gk_completion) as dist_completion_mean,
        avg(pressure_on_actor__andrienko_oval) as dist_pressure_mean
    from actions
    where gk_was_distributing and xt_gk is not null and player_key is not null
    group by player_key, match_key
),

defense as (
    select
        defending_gk_player_key as gk_player_key,
        match_key,
        max(data_source) as data_source,
        count(*) as n_defended_actions,
        sum(case when pre_shot_gk_x is not null then 1 else 0 end) as shots_faced,
        sum(case when pre_shot_gk_x is not null and action_result = 'success' then 1 else 0 end)
            as goals_conceded,
        avg(ghost_deviation_m) as ghost_deviation_mean_m,
        avg(gk_closing_time_min_s__six_yard_box) as closing_min_six_yard_mean_s,
        avg(gk_closing_time_min_s__near_post) as closing_min_near_post_mean_s,
        avg(gk_closing_time_min_s__far_post) as closing_min_far_post_mean_s,
        avg(gk_reachable_area_m2) as reachable_area_mean_m2,
        avg(gk_pitch_control_share_weighted) as pc_share_mean
    from actions
    where defending_gk_player_key is not null
    group by defending_gk_player_key, match_key
)

select
    {{ dbt_utils.generate_surrogate_key(['coalesce(d.gk_player_key, f.gk_player_key)',
                                         'coalesce(d.match_key, f.match_key)']) }}
        as gk_match_stat_id,
    coalesce(d.gk_player_key, f.gk_player_key) as gk_player_key,
    coalesce(d.match_key, f.match_key) as match_key,
    coalesce(d.data_source, f.data_source) as data_source,
    d.n_distributions,
    d.dist_xt_gk_mean, d.dist_xt_gk_possession_mean, d.dist_xt_gk_counter_mean,
    d.dist_xt_gk_direct_mean, d.dist_xt_gk_high_press_mean, d.dist_xt_gk_low_block_mean,
    d.dist_completion_mean, d.dist_pressure_mean,
    f.n_defended_actions, f.shots_faced, f.goals_conceded, f.ghost_deviation_mean_m,
    f.closing_min_six_yard_mean_s, f.closing_min_near_post_mean_s, f.closing_min_far_post_mean_s,
    f.reachable_area_mean_m2, f.pc_share_mean
from distribution d
full outer join defense f
    on d.gk_player_key = f.gk_player_key and d.match_key = f.match_key
```

- [ ] **Step 3.2: Contract + tests in yml:** `gk_match_stat_id` unique+not_null;
      `gk_player_key`/`match_key` not_null; types: ids/keys/`data_source` string,
      `n_distributions`/`n_defended_actions`/`shots_faced`/`goals_conceded` bigint,
      all means double.
- [ ] **Step 3.3: Reconciliation singular test:**

```sql
-- assert_gk_stats_reconciles_actions.sql
select s.gk_match_stat_id
from {{ ref('fct_gk_tracking_stats') }} s
left join (
    select player_key, match_key, count(*) as n
    from {{ ref('fct_gk_tracking_actions') }}
    where gk_was_distributing and xt_gk is not null and player_key is not null
    group by player_key, match_key
) a on a.player_key = s.gk_player_key and a.match_key = s.match_key
where s.n_distributions is not null and s.n_distributions != coalesce(a.n, 0)
```

- [ ] **Step 3.4:** `uv run dbt parse --project-dir dbt_project` → PASS.

### Task 4: Synced-table registration (BOTH registries — review C2)

**Files:** Modify `src/ingestion/refresh_synced_tables.py` (insert after the
`fct_action_context_synced` line, ~line 236) AND `dbt_project/dbt_project.yml`
(`vars.triggered_synced_marts`, ~line 114).

- [ ] **Step 4.1a:** `refresh_synced_tables.py`:

```python
    SyncedTableConfig("fct_gk_tracking_actions_synced", "fct_gk_tracking_actions", ("gk_action_id",), "TRIGGERED"),
    SyncedTableConfig("fct_gk_tracking_stats_synced", "fct_gk_tracking_stats", ("gk_match_stat_id",), "TRIGGERED"),
```

- [ ] **Step 4.1b:** `dbt_project/dbt_project.yml` — append to `vars.triggered_synced_marts`
      (review C2: `test_strand_safe_rederive.py` enforces parity between this var and the
      TRIGGERED set in `refresh_synced_tables.py`; the ADR-043 on-run-start tripwire then
      forbids `--full-refresh` selecting these models):

```yaml
    - fct_gk_tracking_actions
    - fct_gk_tracking_stats
```

- [ ] **Step 4.2:** `uv run pytest src/tests/test_refresh_synced_tables_detect.py src/tests/test_synced_table_lifecycle_ports.py src/tests/test_strand_safe_rederive.py -q`
      → PASS (the parity test is the C2 gate; if any test enumerates configs by count, update the
      expectation — investigate, never relax).
- [ ] **Step 4.3:** Document (in the ADR's Operations note) the operator runbook for AFTER the
      live mart build: `uv run --extra sdk python scripts/create_synced_table.py` per table, then
      `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh` (grants +
      indexes per ADR-005); indexes on the actions table:
      `(defending_gk_player_key, match_key)`, `(player_key, match_key)`, `(match_key, action_id)`
      — added to the maintenance index catalog the same way the `fct_action_context` indexes are
      registered (mirror that entry; verify with `scripts/create_indexes.py --verify`).
      **Review M5:** also verify (create if absent) a covering composite index on
      `fct_tracking_frames_synced (match_key, period, frame)` — the scene-frame query hits the
      platform's largest synced table on the ≤500 ms interaction path; add that query to the
      Task 9.3/11.5 EXPLAIN list.

### Task 5: Queries module (TDD)

**Files:** Create `hf_taipy_app/src/queries/gk_tracking.py`,
`hf_taipy_app/src/test_gk_tracking_queries.py`.

- [ ] **Step 5.1: Failing tests:**

```python
"""GK tracking queries — SQL-builder unit tests (no DB)."""

from queries.gk_tracking import (
    GK_TRACKING_PROVIDERS,
    build_gk_lov_sql,
    build_gk_actions_sql,
    build_gk_pool_stats_sql,
    build_gk_stats_sql,
    build_scene_frame_sql,
)


def test_provider_gate_constant():
    assert GK_TRACKING_PROVIDERS == ("gradientsports", "idsse", "skillcorner")
    assert "metrica" not in GK_TRACKING_PROVIDERS  # owner decision: anonymized players


def test_lov_sql_gates_providers_and_limits():
    sql, params = build_gk_lov_sql()
    assert "fct_gk_tracking_stats_synced" in sql and "dim_players_synced" in sql
    # review N2: derive the expected placeholder string from the constant — adding a provider
    # later must not require editing this assertion (single-source property of M4)
    expected = f"data_source IN ({', '.join(['%s'] * len(GK_TRACKING_PROVIDERS))})"
    assert expected in sql and params == GK_TRACKING_PROVIDERS
    assert "LIMIT 500" in sql


def test_actions_sql_filters_gk_and_limits():
    sql, params = build_gk_actions_sql(gk_player_key="abc", family="distribution")
    assert "fct_gk_tracking_actions_synced" in sql
    assert "gk_was_distributing" in sql and "xt_gk IS NOT NULL" in sql
    assert params[-1] == "abc" and "LIMIT 2000" in sql


def test_actions_sql_defense_family_keys_on_defending_gk():
    sql, params = build_gk_actions_sql(gk_player_key="abc", family="defense")
    assert "defending_gk_player_key = %s" in sql


def test_stats_sql_single_row_per_gk():
    sql, params = build_gk_stats_sql(gk_player_key="abc")
    assert "GROUP BY" in sql or "gk_player_key = %s" in sql
    assert "LIMIT 500" in sql


def test_scene_frame_sql_bounds():
    sql, params = build_scene_frame_sql(match_key="mk", period=2, frame=123)
    assert "fct_tracking_frames_synced" in sql and "LIMIT 60" in sql
    assert params == ("mk", 2, 123)


def test_pool_stats_sql_aggregates_all_gks():
    # review H2: the Tab 1 bump chart + every "vs sample" delta come from THIS query
    sql, params = build_gk_pool_stats_sql(min_distributions=10)
    assert "GROUP BY s.gk_player_key" in sql and "LIMIT 500" in sql
    assert "dist_xt_gk_counter_mean" in sql and "NULLIF(SUM(s.n_distributions), 0)" in sql
    assert params == (*GK_TRACKING_PROVIDERS, 10)
```

- [ ] **Step 5.2:** `uv run pytest hf_taipy_app/src/test_gk_tracking_queries.py -v` → FAIL
      (module missing).
- [ ] **Step 5.3: Implement** (pattern: `queries/goalkeepers.py` — `t()` for table names,
      `execute_query`, `ttl_cache`, `decode_unicode_columns`; builders are PURE so tests need no
      DB):

```python
"""GK tracking-page queries (new page, side-by-side with queries/goalkeepers.py).

Provider gate: GK_TRACKING_PROVIDERS (spec section 3) — Metrica excluded (anonymized),
SB360 deferred. The marts are provider-agnostic; the gate lives ONLY here.
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

GK_TRACKING_PROVIDERS: tuple[str, ...] = ("gradientsports", "idsse", "skillcorner")
# Placeholder string DERIVED from the constant (review M4): adding SB360 later is a
# one-tuple change, never a two-site edit.
_PROVIDER_SQL = f"data_source IN ({', '.join(['%s'] * len(GK_TRACKING_PROVIDERS))})"


def build_gk_lov_sql() -> tuple[str, tuple]:
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions, 0)) AS n_distributions, "
        f"       SUM(COALESCE(s.shots_faced, 0)) AS shots_faced "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE s.{_PROVIDER_SQL} "
        f"GROUP BY s.gk_player_key, p.player_display_name, s.data_source "
        f"ORDER BY n_distributions DESC LIMIT 500"
    )
    return sql, GK_TRACKING_PROVIDERS


def build_gk_actions_sql(gk_player_key: str, family: str) -> tuple[str, tuple]:
    if family == "distribution":
        where = "gk_was_distributing AND xt_gk IS NOT NULL AND player_key = %s"
    elif family == "defense":
        where = "defending_gk_player_key = %s"
    elif family == "shots":
        where = "pre_shot_gk_x IS NOT NULL AND defending_gk_player_key = %s"
    else:  # pragma: no cover - guarded by tests
        raise ValueError(f"unknown family: {family}")
    cols = ", ".join(GK_ACTIONS_COLUMNS)  # explicit list (A4): no SELECT * under append_new_columns
    sql = (
        f"SELECT {cols} FROM {t('fct_gk_tracking_actions_synced')} "  # noqa: S608
        f"WHERE {_PROVIDER_SQL} AND {where} "
        f"ORDER BY match_key, period_id, time_seconds LIMIT 2000"
    )
    return sql, (*GK_TRACKING_PROVIDERS, gk_player_key)
```

      with the module-level constant (single source for A3's reconciliation test):

```python
GK_ACTIONS_COLUMNS: tuple[str, ...] = (
    "gk_action_id", "match_key", "team_key", "player_key", "defending_gk_player_key",
    "data_source", "action_id", "period_id", "time_seconds", "type_name", "game_state",
    "start_x", "start_y", "end_x", "end_y", "frame_id", "action_result",
    "gk_was_distributing",
    "xt_gk", "xt_gk_possession", "xt_gk_counter", "xt_gk_direct", "xt_gk_high_press",
    "xt_gk_low_block", "xt_gk_base", "xt_gk_pev", "xt_gk_rav", "xt_gk_dzv", "xt_gk_pressure",
    "gk_completion", "pressure_on_actor__andrienko_oval",
    "ghost_gk_x", "ghost_gk_y", "ghost_gk_density_spread", "ghost_gk_method",
    "gk_pitch_control_share_weighted", "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box", "gk_closing_time_min_s__six_yard_box",
    "gk_closing_time_mean_s__near_post", "gk_closing_time_min_s__near_post",
    "gk_closing_time_mean_s__far_post", "gk_closing_time_min_s__far_post",
    "defensive_line_x", "pitch_control_method",
    "pre_shot_gk_x", "pre_shot_gk_y", "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot", "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    "gk_frame_mirrored", "gk_actual_x", "gk_actual_y", "ghost_deviation_m", "line_height_m",
)


GK_STATS_COLUMNS: tuple[str, ...] = (
    "gk_match_stat_id", "gk_player_key", "match_key", "data_source",
    "n_distributions",
    "dist_xt_gk_mean", "dist_xt_gk_possession_mean", "dist_xt_gk_counter_mean",
    "dist_xt_gk_direct_mean", "dist_xt_gk_high_press_mean", "dist_xt_gk_low_block_mean",
    "dist_completion_mean", "dist_pressure_mean",
    "n_defended_actions", "shots_faced", "goals_conceded", "ghost_deviation_mean_m",
    "closing_min_six_yard_mean_s", "closing_min_near_post_mean_s", "closing_min_far_post_mean_s",
    "reachable_area_mean_m2", "pc_share_mean",
)

_PRESET_MEANS = ("dist_xt_gk_mean", "dist_xt_gk_possession_mean", "dist_xt_gk_counter_mean",
                 "dist_xt_gk_direct_mean", "dist_xt_gk_high_press_mean", "dist_xt_gk_low_block_mean")


def build_gk_stats_sql(gk_player_key: str) -> tuple[str, tuple]:
    cols = ", ".join(GK_STATS_COLUMNS)  # explicit list (review M3 — own A4 finding, applied)
    sql = (
        f"SELECT {cols} FROM {t('fct_gk_tracking_stats_synced')} "  # noqa: S608
        f"WHERE {_PROVIDER_SQL} AND gk_player_key = %s LIMIT 500"
    )
    return sql, (*GK_TRACKING_PROVIDERS, gk_player_key)


def build_gk_pool_stats_sql(min_distributions: int = 10) -> tuple[str, tuple]:
    """Pool-wide per-GK aggregates (review H2): feeds the Tab 1 bump chart (rank ALL GKs under
    every preset) and every 'vs sample' right-rail delta. Distribution-weighted preset means."""
    wmeans = ", ".join(
        f"SUM(s.{c} * s.n_distributions) / NULLIF(SUM(s.n_distributions), 0) AS {c}"
        for c in _PRESET_MEANS
    )
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions, 0)) AS n_distributions, {wmeans}, "
        f"       SUM(s.dist_completion_mean * s.n_distributions) "
        f"         / NULLIF(SUM(s.n_distributions), 0) AS dist_completion_mean, "
        f"       SUM(COALESCE(s.shots_faced, 0)) AS shots_faced, "
        f"       SUM(COALESCE(s.goals_conceded, 0)) AS goals_conceded, "
        # defense-side means are WEIGHTED like the distribution side (review N1): deviation by
        # shots_faced (it only exists on shots), closing/reachable by n_defended_actions —
        # a 1-shot match and a 10-shot match must not count equally.
        f"       SUM(s.ghost_deviation_mean_m * s.shots_faced) "
        f"         / NULLIF(SUM(s.shots_faced), 0) AS ghost_deviation_mean_m, "
        f"       SUM(s.closing_min_six_yard_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_six_yard_mean_s, "
        f"       SUM(s.closing_min_near_post_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_near_post_mean_s, "
        f"       SUM(s.closing_min_far_post_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_far_post_mean_s, "
        f"       SUM(s.reachable_area_mean_m2 * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS reachable_area_mean_m2 "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE s.{_PROVIDER_SQL} "
        f"GROUP BY s.gk_player_key, p.player_display_name, s.data_source "
        f"HAVING SUM(COALESCE(s.n_distributions, 0)) >= %s OR SUM(COALESCE(s.shots_faced, 0)) > 0 "
        f"LIMIT 500"
    )
    return sql, (*GK_TRACKING_PROVIDERS, min_distributions)


def build_scene_frame_sql(match_key: str, period: int, frame: int) -> tuple[str, tuple]:
    sql = (
        f"SELECT player_id, team_id, x, y, ball_x, ball_y, is_goalkeeper "  # noqa: S608
        f"FROM {t('fct_tracking_frames_synced')} "
        f"WHERE match_key = %s AND period = %s AND frame = %s LIMIT 60"
    )
    return sql, (match_key, period, frame)


@ttl_cache()
def fetch_gk_lov() -> pd.DataFrame:
    sql, params = build_gk_lov_sql()
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_gk_actions(gk_player_key: str, family: str) -> pd.DataFrame:
    sql, params = build_gk_actions_sql(gk_player_key, family)
    return execute_query(sql, params)


@ttl_cache()
def fetch_gk_stats(gk_player_key: str) -> pd.DataFrame:
    sql, params = build_gk_stats_sql(gk_player_key)
    return execute_query(sql, params)


@ttl_cache()
def fetch_gk_pool_stats(min_distributions: int = 10) -> pd.DataFrame:
    sql, params = build_gk_pool_stats_sql(min_distributions)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_scene_frame(match_key: str, period: int, frame: int) -> pd.DataFrame:
    sql, params = build_scene_frame_sql(match_key, period, frame)
    return execute_query(sql, params)
```

- [ ] **Step 5.4:** `uv run pytest hf_taipy_app/src/test_gk_tracking_queries.py -v` → PASS.
      (Adjust assertions only if the implementation legitimately differs — e.g. exact SQL casing.)
- [ ] **Step 5.5: Read-side contract reconciliation test (A3 — ADR-002 §4 pattern, read-side).**
      Add to `src/tests/` (it needs the repo's dbt yml, not the app path):

```python
# src/tests/test_gk_tracking_read_contract.py
"""App-expected GK mart columns must exist in the dbt contract (read-side parity, ADR-051)."""

from pathlib import Path

import yaml

_YML = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "_marts__models.yml"


def _contract_columns(model_name: str) -> set[str]:
    doc = yaml.safe_load(_YML.read_text(encoding="utf-8"))
    model = next(m for m in doc["models"] if m["name"] == model_name)
    return {c["name"] for c in model["columns"]}


def test_actions_query_columns_subset_of_contract():
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2] / "hf_taipy_app" / "src"))
    from queries.gk_tracking import GK_ACTIONS_COLUMNS
    missing = set(GK_ACTIONS_COLUMNS) - _contract_columns("fct_gk_tracking_actions")
    assert not missing, f"app expects columns absent from the dbt contract: {missing}"


def test_stats_query_columns_subset_of_contract():
    # review M3: the stats constant gets the same parity as actions
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2] / "hf_taipy_app" / "src"))
    from queries.gk_tracking import GK_STATS_COLUMNS
    missing = set(GK_STATS_COLUMNS) - _contract_columns("fct_gk_tracking_stats")
    assert not missing, f"app expects columns absent from the stats contract: {missing}"


def test_preset_columns_subset_of_stats_contract():
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2] / "hf_taipy_app" / "src"))
    from state.gk_tracking import PRESET_COLUMN
    missing = set(PRESET_COLUMN.values()) - _contract_columns("fct_gk_tracking_stats")
    assert not missing, f"PRESET_COLUMN names absent from the stats contract: {missing}"
```

      (Review LOW: follow the repo's existing `sys.path` conftest convention for
      `hf_taipy_app/src` — pyproject already inserts it for `src/tests/`; if the conftest covers
      it, DROP the manual inserts — two competing path setups is how import shadowing starts.
      Also confirm `queries.common` imports cleanly in the core-CI env, no module-scope taipy
      import; otherwise guard with `pytest.importorskip`.) Run:
      `uv run pytest src/tests/test_gk_tracking_read_contract.py -v` → PASS once Tasks 2/3/7
      land; until then it is the natural red test of this TDD sequence.

### Task 6: Ghost-grid service (hexagonal port)

**Files:** Create `hf_taipy_app/src/services/ghost_grid.py`,
`hf_taipy_app/src/test_ghost_grid.py` (create `services/__init__.py` if absent).

- [ ] **Step 6.1: Failing tests:**

```python
"""GhostGridProvider port contract + stored-spread adapter math."""

import numpy as np

from services.ghost_grid import GhostGrid, StoredSpreadProvider, resolve_provider


def test_stored_spread_grid_peaks_at_optimum():
    p = StoredSpreadProvider()
    g = p.grid(ghost_x=10.0, ghost_y=30.0, density_spread=9.0, frame_players=None)
    assert isinstance(g, GhostGrid)
    iy, ix = np.unravel_index(np.argmax(g.z), g.z.shape)
    assert abs(g.xs[ix] - 10.0) < 1.0 and abs(g.ys[iy] - 30.0) < 1.0
    assert g.source == "stored"


def test_resolve_provider_defaults_to_stored(monkeypatch):
    monkeypatch.delenv("LL_GHOST_GRID", raising=False)
    assert isinstance(resolve_provider(), StoredSpreadProvider)


def test_model_provider_failure_degrades_loudly(monkeypatch):
    # model mode but silly_kicks unavailable -> stored result with source='stored-fallback'
    import pandas as pd
    monkeypatch.setenv("LL_GHOST_GRID", "model")
    p = resolve_provider(model_loader=lambda: (_ for _ in ()).throw(RuntimeError("no model")))
    g = p.grid(ghost_x=5.0, ghost_y=34.0, density_spread=4.0,
               frame_players=pd.DataFrame({"x": [1.0], "y": [2.0]}))
    assert g.source == "stored-fallback"


def test_model_provider_without_frame_is_stored_fallback(monkeypatch):
    # A2: adapters are PURE — no frame passed in means the model CANNOT run; loud fallback.
    monkeypatch.setenv("LL_GHOST_GRID", "model")
    p = resolve_provider(model_loader=lambda: object())
    g = p.grid(ghost_x=5.0, ghost_y=34.0, density_spread=4.0, frame_players=None)
    assert g.source == "stored-fallback"
```

- [ ] **Step 6.2:** Run → FAIL (module missing).
- [ ] **Step 6.3: Implement:**

```python
"""Ghost-GK density grid service (hexagonal port; ADR-051 section 3).

Port: GhostGridProvider.grid(...) -> GhostGrid. Adapters:
- StoredSpreadProvider: Gaussian blob from the stored optimum + density_spread. Always works.
- ModelGridProvider: loads the silly-kicks ghost-GK model + the linked frame from Lakebase and
  renders the true conditional-density grid. Heavy; enabled via LL_GHOST_GRID=model.
Failures NEVER silently substitute: the result carries `source`, the chart caption renders it,
and failures log at ERROR (ADR-002 telemetry rule).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Protocol

import numpy as np

logger = logging.getLogger(__name__)

_GRID_X, _GRID_Y = 64, 60  # matches ghost-gk-v1 grid shape


@dataclass(frozen=True)
class GhostGrid:
    xs: np.ndarray  # (64,) pitch x in [0, 36] (defensive third, canonical)
    ys: np.ndarray  # (60,) pitch y in [0, 68]
    z: np.ndarray   # (60, 64) density
    source: str     # 'stored' | 'model' | 'stored-fallback'


class GhostGridProvider(Protocol):
    """A2 (architecture audit): adapters are PURE computation — the STATE layer fetches the
    frame from Lakebase (queries.gk_tracking.fetch_scene_frame) and passes it in. No adapter
    performs I/O; both adapters are unit-testable without a DB."""

    def grid(self, *, ghost_x: float, ghost_y: float, density_spread: float,
             frame_players: "pd.DataFrame | None") -> GhostGrid: ...


def _stored_grid(ghost_x: float, ghost_y: float, density_spread: float, source: str) -> GhostGrid:
    xs = np.linspace(0.0, 36.0, _GRID_X)
    ys = np.linspace(0.0, 68.0, _GRID_Y)
    gx, gy = np.meshgrid(xs, ys)
    sigma = float(np.clip(np.sqrt(max(density_spread, 1e-6)) / 3.0, 1.5, 6.0))
    z = np.exp(-(((gx - ghost_x) / sigma) ** 2 + ((gy - ghost_y) / (sigma * 1.15)) ** 2))
    return GhostGrid(xs=xs, ys=ys, z=z, source=source)


class StoredSpreadProvider:
    def grid(self, *, ghost_x, ghost_y, density_spread, frame_players=None) -> GhostGrid:
        return _stored_grid(ghost_x, ghost_y, density_spread, "stored")


class ModelGridProvider:
    """Renders the true model grid from a CALLER-SUPPLIED frame (pure — no I/O here);
    degrades to stored on ANY failure (ERROR log)."""

    def __init__(self, model_loader: Callable[[], object]) -> None:
        self._loader = model_loader
        self._model: object | None = None

    def grid(self, *, ghost_x, ghost_y, density_spread, frame_players=None) -> GhostGrid:
        try:
            if frame_players is None or frame_players.empty:
                raise RuntimeError("no frame rows supplied")
            if self._model is None:
                self._model = self._loader()
            z, xs, ys = self._render(frame_players)
            return GhostGrid(xs=xs, ys=ys, z=z, source="model")
        except Exception:
            logger.exception("ghost model grid failed — stored fallback")
            return _stored_grid(ghost_x, ghost_y, density_spread, "stored-fallback")

    def _render(self, frame_players):
        # Frame -> silly-kicks input -> model grid. Exact call per the silly-kicks ghost-GK
        # model API (verified against the installed package in Step 6.5; never guessed);
        # covered by the importorskip integration test.
        raise NotImplementedError  # replaced in Step 6.5
```

      plus the resolver:

```python
def _default_model_loader() -> object:
    from silly_kicks.tracking.features import _ghost_gk_model_cached  # noqa: PLC0415
    return _ghost_gk_model_cached()


def resolve_provider(model_loader: Callable[[], object] | None = None) -> GhostGridProvider:
    if os.environ.get("LL_GHOST_GRID") == "model":
        return ModelGridProvider(model_loader or _default_model_loader)
    return StoredSpreadProvider()
```

- [ ] **Step 6.4:** `uv run pytest hf_taipy_app/src/test_ghost_grid.py -v` → PASS (the
      NotImplementedError path is exactly what the fallback test exercises).
- [ ] **Step 6.5 (DEFERRED to fast-follow — spec §9 resolution 3):** v1 ships the port +
      `StoredSpreadProvider` ONLY. The `ModelGridProvider._render` body stays
      NotImplementedError-backed (loud stored-fallback path, already tested), and
      `_default_model_loader` is NOT implemented in v1: it currently names a PRIVATE silly-kicks
      symbol (`_ghost_gk_model_cached`) — review LOW#5: a public loader entrypoint has been
      requested from the silly-kicks session; implement the adapter in the fast-follow PR against
      that public API (verify the call surface, never guess) with the
      `pytest.importorskip("silly_kicks")` integration test asserting a (60, 64) grid from a
      22-row synthetic frame. Until then `LL_GHOST_GRID=model` simply produces `stored-fallback`
      grids with an ERROR log — safe, visible, and honest on-chart.
      **Security (S2/S3):** the model loader pins the HF Hub REVISION (commit hash constant, not
      `main`); confirm the npz weight load has no `allow_pickle=True` path; memoize grids per
      `gk_action_id` (simple dict cache in the provider) so repeated scene views don't re-render.
      **Observability (O1):** wrap the render in a timer and log at INFO —
      `logger.info("ghost grid rendered: action=%s source=%s cache=%s duration_ms=%d", ...)` —
      the render sits on the user path under the app's ≤3 s first-load / ≤500 ms cached budget;
      the Task 11 e2e asserts the scene interaction stays within budget with `LL_GHOST_GRID=model`.
- [ ] **Step 6.6:** Re-run Step 6.4 → PASS.

### Task 7: State module (TDD on pure helpers)

**Files:** Create `hf_taipy_app/src/state/gk_tracking.py`,
`hf_taipy_app/src/test_gk_tracking_state.py`.

- [ ] **Step 7.1: Failing tests (pure helpers only — Taipy state objects are not unit-testable):**

```python
import numpy as np
import pandas as pd

from state.gk_tracking import (
    GKT_SUB_VIEW_LOV,
    PRESET_COLUMN,
    _format_metric,
    _preset_rank_frame,
    _line_height_terciles,
)


def test_sub_views_and_presets():
    assert GKT_SUB_VIEW_LOV == ["Distribution Value", "Defensive Positioning", "Shot Stopping"]
    assert PRESET_COLUMN["Counter"] == "dist_xt_gk_counter_mean"
    assert len(PRESET_COLUMN) == 6


def test_format_metric_nan_is_em_dash():
    assert _format_metric(float("nan"), "{:.3f}") == "—"
    assert _format_metric(0.1234, "{:.3f}") == "0.123"


def test_preset_rank_frame_ranks_within_preset():
    df = pd.DataFrame({
        "gk": ["A", "B"], "dist_xt_gk_counter_mean": [0.02, 0.01],
        "dist_xt_gk_possession_mean": [0.01, 0.03],
    })
    ranks = _preset_rank_frame(df, ["Counter", "Possession"])
    assert ranks.loc["A", "Counter"] == 1 and ranks.loc["A", "Possession"] == 2


def test_line_height_terciles_labels_carry_n():
    df = pd.DataFrame({"line_height_m": np.arange(9.0), "ghost_deviation_m": np.arange(9.0)})
    cats, means = _line_height_terciles(df)
    assert len(cats) == 3 and all("n=3" in c for c in cats)
```

- [ ] **Step 7.2:** Run → FAIL. **Step 7.3: Implement** the module: state vars (`gkt_` prefix
      throughout — never `tp_`), `GKT_SUB_VIEW_LOV`, `PRESET_COLUMN` mapping, the three pure
      helpers exactly as tested, chart builders ported 1:1 from the v3 prototype functions in
      `docs/ui-cycles/gk-redesign/generate_mockups.py` (`tab1_xtgk_real` → bump + paired maps;
      `tab2_ghost_defense_real` → scene via `services.ghost_grid.resolve_provider()` + splits +
      closing dumbbells; `tab3_shot_geometry_real` → cone + outcome map), refresh dispatch per
      sub-view (pattern: `state/goalkeeper.py:_dispatch_refresh`), GK selector LOV from
      `fetch_gk_lov`, `register_page_refresher("Goalkeeper-Tracking", gkt_refresh)`. The
      game-state split renders ONLY when `nunique(game_state) >= 2` in the fetched data (spec
      open item 4) — otherwise a caption explains why it's hidden.
      **Review H2 wiring:** the bump chart's rank frame and EVERY "vs sample" right-rail delta
      (completion vs sample, closing vs sample, deviation vs sample) are sourced from
      `fetch_gk_pool_stats()` — `_preset_rank_frame` consumes the pool frame, never a single-GK
      fetch; the selected GK's row is highlighted within the pool.
- [ ] **Step 7.4:** `uv run pytest hf_taipy_app/src/test_gk_tracking_state.py -v` → PASS.

### Task 8: Page config + registration + glossary + NOTICE

**Files:** Create `hf_taipy_app/src/pages/gk_tracking.py`; Modify `hf_taipy_app/src/main.py`
(registry at ~line 108), `hf_taipy_app/src/template.py` (PAGE_TERMS at ~line 277 + GLOSSARY),
`NOTICE`.

- [ ] **Step 8.1: Page config** (every Metric with help_text; scale+direction everywhere):

```python
"""Goalkeeper Tracking Analytics page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS, Citation, ContentBlock, ContentRow, Metric, PageConfig,
    ScopeDim, SubView, build_page,
)

page_config = PageConfig(
    title="Goalkeeper Tracking",
    icon="sports_handball",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Tracking-data goalkeeper analytics: distribution value under six switchable game-model "
        "presets (xT-GK), model-optimal positioning (Ghost GK), box command, and pre-shot "
        "geometry. Tracking providers only (GradientSports, IDSSE, SkillCorner)."
    ),
    freshness_var="gkt_data_freshness",
    citations=[
        Citation("Eyestone — xT-GK: Expected Threat for Goalkeepers (course materials)"),
        Citation("Poole (2022) — USWNT Goalkeeper Profile, IGCC (course materials)"),
        Citation("Spearman (2018) — Beyond Expected Goals", "https://www.researchgate.net/publication/327139841"),
    ],
    empty_message="", empty_condition="",
    scope_dims=[ScopeDim("Goalkeeper", "gkt_scope_player"), ScopeDim("Preset", "gkt_scope_preset")],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Distribution Value"',
            content=[
                ContentRow([ContentBlock("chart", "gkt_bump_figure",
                                         condition="gkt_bump_figure is not None",
                                         header="Rank under every game-model preset")]),
                ContentRow([
                    ContentBlock("chart", "gkt_map_selected_figure",
                                 condition="gkt_map_selected_figure is not None",
                                 header="Distributions valued under the selected preset"),
                    ContentBlock("chart", "gkt_map_compare_figure",
                                 condition="gkt_map_compare_figure is not None",
                                 header="The same passes under the comparison preset"),
                ]),
            ],
            metrics=[
                Metric("xT-GK / pass", "gkt_xtgk_mean_val",
                       "Mean xT-GK per distribution under the selected preset. Scale roughly "
                       "-0.05 to +0.10; higher = more attacking value created per pass."),
                Metric("Completion", "gkt_completion_val",
                       "Mean model probability that his attempted distributions succeed "
                       "(0-1; lower = riskier pass selection, not worse passing)."),
                Metric("n", "gkt_n_dist_val",
                       "Number of distributions behind these values. Small n = treat as noisy."),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see distribution value.",
            empty_condition="gkt_bump_figure is None and gkt_selected_player is None",
        ),
        SubView(
            condition='selected_sub_view == "Defensive Positioning"',
            content=[
                ContentRow([ContentBlock("chart", "gkt_scene_figure",
                                         condition="gkt_scene_figure is not None",
                                         header="Actual vs model-optimal position (Ghost GK)")]),
                ContentRow([
                    ContentBlock("chart", "gkt_context_figure",
                                 condition="gkt_context_figure is not None",
                                 header="When does he leave the model line?"),
                    ContentBlock("chart", "gkt_closing_figure",
                                 condition="gkt_closing_figure is not None",
                                 header="Command of the box"),
                ]),
            ],
            metrics=[
                Metric("Deviation", "gkt_deviation_val",
                       "Mean distance from the ghost-model optimum on shots faced (meters; "
                       "lower = more orthodox positioning)."),
                Metric("Closing (6yd)", "gkt_closing_val",
                       "Mean minimum time to reach the six-yard box (seconds; lower = better)."),
                Metric("Reach", "gkt_reach_val",
                       "Mean reachable area around his position (m²; higher = better)."),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see positioning.",
            empty_condition="gkt_scene_figure is None and gkt_selected_player is None",
        ),
        SubView(
            condition='selected_sub_view == "Shot Stopping"',
            content=[
                ContentRow([
                    ContentBlock("chart", "gkt_cone_figure",
                                 condition="gkt_cone_figure is not None",
                                 header="Pre-shot geometry"),
                    ContentBlock("chart", "gkt_shotmap_figure",
                                 condition="gkt_shotmap_figure is not None",
                                 header="Every shot faced — where was he standing?"),
                ]),
            ],
            metrics=[
                Metric("Shots faced", "gkt_shots_val",
                       "On-target-linked shots with tracked GK geometry in scope."),
                Metric("Goals", "gkt_goals_val",
                       "Goals conceded on those shots (Goals Prevented arrives with PSxG/TF-48)."),
                Metric("Off line", "gkt_offline_val",
                       "Mean distance off the goal line at the shot (meters; context, not a "
                       "grade — high values can be sweeping duty)."),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see shot geometry.",
            empty_condition="gkt_cone_figure is None and gkt_selected_player is None",
        ),
    ],
)
page_md = build_page(page_config)
```

- [ ] **Step 8.1b (review N5):** Verify `build_page` treats `empty_message=""` /
      `empty_condition=""` as "no top-level empty state" (the SubViews own their empties) —
      one-line check against `page_template.py`; if it requires `None` or omission, adjust the
      PageConfig accordingly (the old GK page uses the same `""` pattern — confirm, don't assume).
- [ ] **Step 8.2: main.py gated registration** — imports + registry entry wrapped:

```python
import os

if os.environ.get("LL_GK_TRACKING_PAGE") == "1":
    from pages.gk_tracking import page_config as gkt_config, page_md as gkt_page  # noqa: E402
    import state.gk_tracking  # noqa: E402,F401  (registers the refresher)
    PAGE_REGISTRY.append(PageEntry("Goalkeeper-Tracking", gkt_config, gkt_page))
```

      placed immediately after the `PAGE_REGISTRY` literal closes (verify the exact close line;
      keep ALL other entries untouched).
- [ ] **Step 8.3: template.py** — add `"Goalkeeper-Tracking": ["xT-GK", "PEV", "RAV", "DZV",
      "Ghost GK", "Closing time", "Reachable area", "Line height", "PSxG"]` to PAGE_TERMS and a
      GLOSSARY entry for each term not already present (one-sentence, scale + direction included).
- [ ] **Step 8.4: NOTICE** — add entries: Eyestone xT-GK (practitioner methodology), Poole USWNT
      GK profile (practitioner methodology), ghost-GK conditional-density model (silly-kicks
      `ghost-gk-v1`). Run `uv run pytest src/tests/test_citation_consistency.py -q` → PASS.
- [ ] **Step 8.5:** Smoke: `LL_GK_TRACKING_PAGE=1` + Lakebase env →
      `cd hf_taipy_app && python src/main.py` boots; WITHOUT the flag the page is absent and the
      app is byte-identical in behavior (check the nav). Document both checks' output.
- [ ] **Step 8.6 (O2):** Verify what `fetch_data_freshness()` actually reads — if it does not
      reflect the NEW marts' build time, point `gkt_data_freshness` at a source that does (e.g.
      max sync timestamp of `fct_gk_tracking_stats_synced`); a freshness badge that ignores the
      page's own tables is a silent-substitution violation.

### Task 9: Audits (the new logic + page)

- [ ] **Step 9.1:** `mad-skills:chart-choice-audit` on `hf_taipy_app/src/pages/gk_tracking.py`
      (+ state module) → findings fixed or explicitly accepted in the report
      (`docs/superpowers/specs/kirk-poc-findings/gk-tracking.md`). The v3 mockup audit
      (`docs/ui-cycles/gk-redesign/kirk-chart-audit.md`) is the baseline — re-run, don't assume.
- [ ] **Step 9.2:** `mad-scientist-skills:cognitive-interface-audit` (audit mode) on the new page
      → Critical/High findings fixed in-cycle (National Park principle).
- [ ] **Step 9.3:** `mad-scientist-skills:optimization-audit` scoped to
      `hf_taipy_app/src/queries/gk_tracking.py`, `state/gk_tracking.py`,
      `services/ghost_grid.py` + the two mart SQL files. Plus: EXPLAIN ANALYZE on every new
      synced query (after Task 11) — Index Scan required on `fct_gk_tracking_actions_synced`
      (never rewrite queries blindly; measure first).
- [ ] **Step 9.4:** Governance: `uv run pytest src/tests/test_ai_governance_md.py -v` → if the new
      page surfaces a per-player evaluative system not yet in the inventory, follow the
      CLAUDE.md AI-governance checklist (likely already covered by the AC workflow card —
      verify, don't assume).

### Task 10: Wheel bump + Shift-Left gate

- [ ] **Step 10.1:** Bump version in `pyproject.toml`; `uv run python scripts/bump_wheel.py`
      (dbt YAML/SQL rides the wheel — never edit `src/shared/wheel.py` manually).
- [ ] **Step 10.2:** `uv run ruff format --check src/ scripts/ hf_taipy_app/` → clean (format first
      if needed); `uv run ruff check src/ scripts/ hf_taipy_app/` → clean;
      `uv run pyright src/` → 0 errors; `uv run pytest src/tests/ -v` → PASS;
      `uv run pytest hf_taipy_app/src/test_gk_tracking_queries.py hf_taipy_app/src/test_ghost_grid.py hf_taipy_app/src/test_gk_tracking_state.py -v` → PASS.

### Task 11: Live build + e2e (gated on the AC recompute)

- [ ] **Step 11.1:** PRECONDITION check: `stg_action_context__values` current (xt_gk columns
      resolve — as of 2026-06-11 the DEPLOYED view is schema-stale even though the repo SQL is
      current; a scoped `dbt run --select stg_action_context__values` fixes it) AND
      `fct_action_context` populated. **The full AC population is expected imminently (owner,
      2026-06-11)** — re-verify rather than assume; if either check fails → STOP, report; do not
      improvise against bronze. Note (state as of round-2 review, 2026-06-11): silly-kicks
      4.22.2 was REJECTED; **4.23.0 adoption is staged (wheel 0.5.35)** with the degenerate
      `space_created_m2_opponent ≡ 0` accepted pending an upstream fix — irrelevant to this page
      (it reads no space columns). The full AC recompute remains owner-gated; Tasks 0–10 are
      independent of all of this by design.
- [ ] **Step 11.2:** Live build via the dbt-live-ci flow on the PR (or operator-run
      `scripts/dbt_build_and_refresh.py` for the two models) → both marts build, contracts hold,
      all 4 new tests PASS.
- [ ] **Step 11.3:** Operator: create the 2 synced tables + run maintenance (Task 4.3 runbook);
      `scripts/create_indexes.py --verify` → Index Scan on the actions table.
- [ ] **Step 11.4:** e2e: launch locally with `LL_GK_TRACKING_PAGE=1`; puppeteer pass — navigate
      to each tab, screenshot, compare against `docs/ui-cycles/gk-redesign/mockups/v3_*.png`
      (layout parity, real names, no raw IDs, captions present); record screenshots under
      `docs/ui-cycles/gk-redesign/e2e/`.
- [ ] **Step 11.5:** EXPLAIN ANALYZE per Step 9.3.

### Task 12: Final review + single approval-gated commit + staging deploy

- [ ] **Step 12.1:** Run `/final-review` (mandatory pre-commit gate; includes ADR scan + C4).
- [ ] **Step 12.2:** Present diff summary to owner; **request explicit commit approval**. On
      approval: ONE commit
      `feat(gk): tracking-based GK Analytics page + fct_gk_tracking_* marts (staging-gated, ADR-051)`
      then PR (squash) — again only with explicit approval.
- [ ] **Step 12.3:** After merge + operator steps: **verify the STAGING Space visibility is
      private/org-only (security-audit S1 — unreviewed page + GS per-player data)**, then
      `uv run python scripts/manage_space.py deploy staging`; set `LL_GK_TRACKING_PAGE=1` (and
      optionally `LL_GHOST_GRID=model`) in the STAGING Space settings only. Verify production
      Space variables unchanged. Sign-off review happens on staging; cutover is a separate future
      plan (and re-confirms the GS public-display decision).

---

## Self-review (run after writing, fixed inline)

- **Spec coverage:** §2 tabs → Tasks 7/8; §3 gating → Task 5 constant + ADR; §4 marts/macro/synced
  → Tasks 2/3/4; §5 app modules → Tasks 5–8; §6 testing/audits → every task's test steps + Task 9;
  §7 dependencies → Task 11.1 precondition + plan header; §8 rollout → Tasks 0/12; §9 items are
  RESOLVED in the spec (review rounds 1–2): names kept, per-match grain + merge materialization,
  model adapter deferred to fast-follow (Step 6.5), game-state data-gated (Step 7.3) — no open
  decisions remain for the implementer beyond owner overrides.
- **Known judgment points for the implementer:** chart-builder port from prototype code is 1:1 by
  design (the mockups are normative); the silly-kicks model API in Task 6.5 must be verified
  against the installed package, never guessed; any test that fails against live data semantics
  (e.g. `action_result` vocabulary) is investigated, not relaxed.
- **Type consistency:** `gk_player_key`/`match_key` are STRING surrogates throughout; query
  builders return `(sql, params)` tuples; `GhostGrid.source` literals match the state module's
  caption logic; `PRESET_COLUMN` values match the stats-mart column names defined in Task 3.
