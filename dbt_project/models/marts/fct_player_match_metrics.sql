-- fct_player_match_metrics.sql
-- Gold-layer wide-by-granularity player-match mart (silly-kicks 4.120.0 TF-54/TF-54b + TF-55; sk4118 P1
-- Phase C). Grain: one row per (match_key, player_key). Carries BOTH player-match families:
--   * territory (the 20-col counterfactual union) — ingestion.territory_writer /
--     compute_territorial_dominance BOTH methods (event-only, injected fitted xT + PassCompletionModel).
--   * duels (6 Glicko-2 cols + provenance) — ingestion.duels_writer / compute_duel_ratings (event-only,
--     stateful single-driver ordered pass).
--
-- Wide-by-granularity (AGENTS.md "metric marts = WIDE by GRANULARITY not per-feature"): the two families
-- share the (match, player) grain, so they land in ONE mart, not two per-feature marts. A player-match may
-- carry territory but not duels (a defender with a hull but no ground duel) or vice versa, so the two
-- staging views are FULL-OUTER-JOINed on the native grain, then the native ids are resolved to Kimball
-- surrogates from dim_matches / dim_players (ADR-013: writers emit native ids only; surrogates resolve
-- here). data_source / access_tier / competition_key / season_id ride through; the surrogate PK
-- player_match_metrics_id is a deterministic hash of (data_source, native_match_id, player_id_native).
--
-- TWO-CONTRACT parity [SPEC-R5-01]: the territory v1 metric surface (12) is registry-guarded against
-- METRIC_CONTRACTS['territory']; the 5 counterfactual-only cols are guarded SEPARATELY against
-- columns_for_method('counterfactual') − columns_for_method('completed_failed') — NEVER folded into the
-- registry-12 assertion. duels' 6 cols are registry-guarded against METRIC_CONTRACTS['duels'].
--
-- GOVERNANCE: territory + duels are per-PLAYER evaluative systems (a per-defender territorial-dominance
-- measure; a per-player ground-duel rating). Both wf-territory / wf-duels ARE members of
-- PER_PLAYER_EVALUATIVE_CARDS (src/tests/test_ai_governance_md.py) and carry EU-AI-Act model cards.
-- territory is RANKING-LICENSED (sk defender-ranking census ICC lo=0.121>0).

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with territory as (

    select * from {{ ref('stg_territory') }}

),

duels as (

    select * from {{ ref('stg_duels') }}

),

-- FULL OUTER on the native grain: coalesce the shared identity/team/access_tier so a player-match present
-- in only one family still ships (COALESCE prefers territory, falls back to duels).
combined as (

    select
        coalesce(t.data_source, d.data_source)                  as data_source,
        coalesce(t.native_match_id, d.native_match_id)          as native_match_id,
        coalesce(t.player_id_native, d.player_id_native)        as player_id_native,
        coalesce(t.team_id_native, d.team_id_native)            as team_id_native,
        coalesce(t.access_tier, d.access_tier)                  as access_tier,

        -- territory v1 metric cols (12)
        t.territory_xt_conceded,
        t.territory_xt_prevented,
        t.territory_xt_net,
        t.territory_xt_conceded_forward,
        t.territory_xt_prevented_forward,
        t.territory_passes_into_hull,
        t.territory_xt_conceded_rate,
        t.territory_xt_prevented_rate,
        t.territory_hull_area_m2,
        t.territory_hull_centroid_x,
        t.territory_hull_centroid_y,
        t.territory_defensive_actions_in_hull,
        -- territory v1 provenance
        t.territory_hull_source,
        -- territory counterfactual-only cols (5): 4 metric + target_source provenance
        t.territory_expected_threat_faced,
        t.territory_xt_prevented_above_expectation,
        t.territory_passes_aimed_into_hull,
        t.territory_mean_completion_faced,
        t.territory_target_source,

        -- duels (6 metric cols + provenance)
        d.duel_rating,
        d.duel_rating_deviation,
        d.duel_volatility,
        d.duels_contested,
        d.duels_won,
        d.duels_lost,
        d.duel_winner_source

    from territory t
    full outer join duels d
        on  t.data_source = d.data_source
       and t.native_match_id = d.native_match_id
       and t.player_id_native = d.player_id_native

)

select
    {{ dbt_utils.generate_surrogate_key([
        'c.data_source',
        'c.native_match_id',
        'c.player_id_native'
    ]) }}                                                           as player_match_metrics_id,

    dm.match_key,
    dp.player_key,
    dt.team_key,
    dm.competition_key,
    dm.season_id,
    c.data_source,
    c.access_tier,

    -- territory v1 metric cols (12)
    c.territory_xt_conceded,
    c.territory_xt_prevented,
    c.territory_xt_net,
    c.territory_xt_conceded_forward,
    c.territory_xt_prevented_forward,
    c.territory_passes_into_hull,
    c.territory_xt_conceded_rate,
    c.territory_xt_prevented_rate,
    c.territory_hull_area_m2,
    c.territory_hull_centroid_x,
    c.territory_hull_centroid_y,
    c.territory_defensive_actions_in_hull,
    -- territory v1 provenance
    c.territory_hull_source,
    -- territory counterfactual-only cols (5)
    c.territory_expected_threat_faced,
    c.territory_xt_prevented_above_expectation,
    c.territory_passes_aimed_into_hull,
    c.territory_mean_completion_faced,
    c.territory_target_source,

    -- duels (6 metric cols + provenance)
    c.duel_rating,
    c.duel_rating_deviation,
    c.duel_volatility,
    c.duels_contested,
    c.duels_won,
    c.duels_lost,
    c.duel_winner_source

from combined c
inner join {{ ref('dim_matches') }} dm
    on  dm.provider = c.data_source
   and dm.native_match_id = c.native_match_id
left join {{ ref('dim_players') }} dp
    on  dp.provider = c.data_source
   and dp.native_player_id = c.player_id_native
left join {{ ref('dim_teams') }} dt
    on  dt.provider = c.data_source
   and dt.native_team_id = c.team_id_native
