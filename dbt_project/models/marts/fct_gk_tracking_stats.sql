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
              and ((a.player_key = t.gk_player_key and a.xt_gk is not null)
                   or a.defending_gk_player_key = t.gk_player_key)
        )
    """
) }}
-- fct_gk_tracking_stats.sql
-- Grain: one row per (gk_player_key, match_key). A GK appears in two roles:
-- actor of distributions (player_key + gk_was_distributing) and defender of shots
-- (defending_gk_player_key) — aggregated separately, FULL OUTER joined.
--
-- MATERIALIZATION (review H1): incremental/merge with a FULL-recompute body — deliberately NO
-- is_incremental() filter. The aggregate is recomputed in full every run (cheap at this grain)
-- but written via MERGE, because a `table` rebuild of a TRIGGERED synced mart STRANDS its
-- synced table (ADR-043 amendment 2) and forces the ADR-041 heal's re-snapshot downtime.
-- Merge writes never change the Delta table id, so the synced table stays attached.
--
-- ORPHAN SWEEP (review R2): merge never deletes — without the post_hook above, a
-- (gk, match) row whose underlying actions disappear (AC wipe + selective recompute) lingers
-- with stale values and trips the reconciliation test. The keep-condition MUST mirror this
-- mart's SOURCE GRAIN — a row exists only if the GK is a current DISTRIBUTOR (xt_gk-not-null
-- actor, the `distribution` CTE grain) OR a current DEFENDER (`defense` CTE grain). A looser
-- `a.player_key = t.gk_player_key` (ANY actor) kept GKs who are merely non-distribution actors,
-- so their stale n_distributions (from a prior build) survived the merge and failed
-- reconciliation across ALL tracking providers (2026-07-06 fix). The ACTIONS mart's orphan
-- policy is operator-driven (ADR-051 Operations note).

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
    -- xt_gk (Eyestone GK-distribution value) is non-null ONLY on the acting GK's distribution
    -- actions (pass/goalkick) — the authoritative domain marker. gk_was_distributing is a
    -- DISJOINT silly-kicks pre-shot feature on SHOT actions (was the *defending* GK distributing
    -- at a shot); ANDing it here zeroed the entire distribution family (ADR-051 follow-up).
    where xt_gk is not null and player_key is not null
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
    d.dist_xt_gk_mean,
    d.dist_xt_gk_possession_mean,
    d.dist_xt_gk_counter_mean,
    d.dist_xt_gk_direct_mean,
    d.dist_xt_gk_high_press_mean,
    d.dist_xt_gk_low_block_mean,
    d.dist_completion_mean,
    d.dist_pressure_mean,
    f.n_defended_actions,
    f.shots_faced,
    f.goals_conceded,
    f.ghost_deviation_mean_m,
    f.closing_min_six_yard_mean_s,
    f.closing_min_near_post_mean_s,
    f.closing_min_far_post_mean_s,
    f.reachable_area_mean_m2,
    f.pc_share_mean
from distribution d
full outer join defense f
    on d.gk_player_key = f.gk_player_key and d.match_key = f.match_key
