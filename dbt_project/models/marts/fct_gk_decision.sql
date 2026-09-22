-- fct_gk_decision.sql
-- Gold-layer per-DECISION GK decision-value mart (silly-kicks 4.120.0 TF-62; sk4118 P1 Task B2 / Phase E).
-- Grain: one row per (match_key, player_key, decision_id) = a single goalkeeper build-up DISTRIBUTION
-- DECISION (chosen-vs-available). sk compute_gk_decision_value returns per-DECISION samples
-- (decision_id == the SPADL action_id of the GK-distribution action), so this fact is per-decision — a
-- per-keeper-match summary is a downstream aggregate (silly_kicks.gk_decision.summarize_gk_decision), NOT
-- this grain. Written by ingestion.gk_decision_writer (RECONSTRUCTION TIER) into bronze.gk_decision;
-- surrogates resolve here (ADR-013: writers emit native ids only).
--   * decision_value / chosen_ev / best_ev / sel_efficiency / decision_pct — the 5 sk metric cols.
--   * n_options / option_set_source — provenance (option_set_source='reconstructed'; the native
--     SkillCorner GI tier is not-yet-sourced — see docs/huggingface/model-cards/gk-decision.md).
--
-- Native ids -> Kimball surrogates: INNER JOIN dim_matches on (provider, native_match_id) — the identity
-- fact; LEFT JOIN dim_players on (provider, native_player_id) — a keeper without a dim_players row lands
-- NULL player_key and is filtered out to preserve not_null(player_key). data_source / access_tier ride
-- through; the surrogate PK gk_decision_id hashes (data_source, native_match_id, decision_id) — unique per
-- decision.
--
-- GOVERNANCE: gk_decision is per-KEEPER evaluative (wf-gk-decision in PER_PLAYER_EVALUATIVE_CARDS,
-- model card docs/huggingface/model-cards/gk-decision.md). It is an INSTRUMENT (how good was this
-- decision vs available), NOT a ranking — the reconstruction-tier correlation to ground truth is only
-- moderate. See AI_GOVERNANCE.md §5/§6.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with decisions as (

    select * from {{ ref('stg_gk_decision') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'd.data_source',
        'd.native_match_id',
        'd.decision_id'
    ]) }}                                                       as gk_decision_id,

    dm.match_key,
    dp.player_key,
    dm.competition_key,
    dm.season_id,
    d.data_source,
    d.access_tier,
    d.period_id,
    -- decision_id == the SPADL action_id of the GK-distribution action (the per-decision key).
    d.decision_id,

    -- gk_decision (5 metric cols)
    d.decision_value,
    d.chosen_ev,
    d.best_ev,
    d.sel_efficiency,
    d.decision_pct,

    -- provenance
    d.n_options,
    d.option_set_source

from decisions d
inner join {{ ref('dim_matches') }} dm
    on  dm.provider = d.data_source
   and dm.native_match_id = d.native_match_id
left join {{ ref('dim_players') }} dp
    on  dp.provider = d.data_source
   and dp.native_player_id = d.native_player_id
where dp.player_key is not null
