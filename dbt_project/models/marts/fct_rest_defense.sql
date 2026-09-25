-- fct_rest_defense.sql
-- Gold-layer rest-defense mart (silly-kicks 4.102/4.103 restdefense L1 + L2; sk4118 P1 Phase E).
-- Grain: one row per (match_key, period_id, team_key, action_id) = the DEFENDING team's rest-defence
-- geometry at a single in-possession on-ball action. Written by ingestion.restdefense_writer (ADR-013:
-- native ids only) into bronze.rest_defense; surrogates resolve here.
--   * Layer 1 (11 cols): numerical superiority, zone occupancy, line height, compactness, width, depth,
--     staggered-shape flag, GK line height / to-line distance — descriptive geometry, always present.
--   * Layer 2 (5 cols): attacker space control, danger-behind-line (± GK), GK coverage / reachable area —
--     xT / pitch-control weighted; NaN without a fitted xt (ADR-033), by design.
--
-- Native ids -> Kimball surrogates:
--   * match_key / competition_key <- INNER JOIN fct_action_values (the identity fact) on the native
--     in-possession-action key (data_source, match_id_native, action_id).
--   * team_key = the DEFENDING team <- LEFT JOIN dim_teams on (provider, native_team_id = team_id_native).
--     NOT the acting team fct_action_values carries — restdefense measures the OUT-of-possession team.
--
-- GOVERNANCE: rest_defense is descriptive / team-structural — it measures a team's rest-defence GEOMETRY,
-- never an individual's rating — so it is NOT per-player-evaluative and carries no EU-AI-Act model card.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with samples as (

    select * from {{ ref('stg_rest_defense') }}

),

identity as (

    select distinct
        data_source,
        match_id_native,
        match_key,
        competition_key
    from {{ ref('fct_action_values') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        's.data_source',
        's.native_match_id',
        's.period_id',
        's.team_id_native',
        's.action_id'
    ]) }}                                                       as rest_defense_id,

    i.match_key,
    dt.team_key,
    i.competition_key,
    s.data_source,
    s.access_tier,
    s.period_id,
    s.action_id,
    s.possession_id,
    s.is_possession_loss,

    -- Layer 1 (descriptive geometry)
    s.rd_num_superiority,
    s.rd_num_superiority_gk,
    s.rd_zone_occupancy,
    s.rd_line_height,
    s.rd_line_height_relative,
    s.rd_compactness_x,
    s.rd_width,
    s.rd_depth,
    s.rd_shape_2_3_vs_3_2,
    s.rd_gk_line_height,
    s.rd_gk_to_line_distance,

    -- Layer 2 (xT / space-control weighted; NaN without a fitted xt)
    s.rd_attacker_space_control,
    s.rd_danger_behind_line,
    s.rd_danger_behind_line_gk,
    s.rd_gk_coverage_behind_line,
    s.rd_gk_reachable_coverage_m2,

    -- Provenance
    s.rd_geometry_source

from samples s
inner join identity i
    on  i.data_source = s.data_source
   and i.match_id_native = s.native_match_id
left join {{ ref('dim_teams') }} dt
    on  dt.provider = s.data_source
   and dt.native_team_id = s.team_id_native
