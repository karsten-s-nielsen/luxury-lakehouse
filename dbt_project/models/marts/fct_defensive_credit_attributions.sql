-- fct_defensive_credit_attributions.sql
-- Gold-layer long-form defensive-credit attributions (silly-kicks 4.87.0 TF-51
-- compute_defensive_credits; spec §7.5, Task 17f). Grain: one row per credit
-- EVENT — (triggering action, credited player, rule) is NOT a unique key (e.g.
-- synchronized_final_third_pressure credits a defender once per synchronized
-- presser), so this is an event log, not a keyed dimension.
--
-- The Python writer (ingestion.defensive_credit_writer) emits native identifiers
-- + the 11 silly-kicks credit columns to bronze.defensive_credit_attributions
-- (ADR-013). This mart resolves the Kimball surrogates:
--   * match_key / competition_key <- INNER JOIN the AC identity fact
--     fct_action_values on the native action key (data_source, match_id,
--     action_id).
--   * player_key / team_key (the CREDITED player/team, usually the DEFENDING
--     side, not the actor) <- LEFT JOIN dim_players / dim_teams on
--     (provider, native_id).
--
-- signed_value is a nullable DOUBLE (a credit can fire while its xT/xG-sized
-- magnitude is unresolvable). rule / anchor_type / sizing / resolution are the
-- silly-kicks closed vocabularies.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with attributions as (

    select * from {{ ref('stg_defensive_credit_attributions') }}

),

identity as (

    select
        data_source,
        match_id_native,
        action_id,
        match_key,
        competition_key
    from {{ ref('fct_action_values') }}

)

select
    i.match_key,
    i.competition_key,
    a.data_source,
    a.game_id,
    a.period_id,
    a.action_id,
    dp.player_key,
    a.player_id_native,
    dt.team_key,
    a.team_id_native,
    a.rule,
    a.signed_value,
    a.anchor_type,
    a.frame_id,
    a.sizing,
    a.resolution

from attributions a
inner join identity i
    on  i.data_source = a.data_source
   and i.match_id_native = a.native_match_id
   and i.action_id = a.action_id
left join {{ ref('dim_players') }} dp
    on  dp.provider = a.data_source
   and dp.native_player_id = a.player_id_native
left join {{ ref('dim_teams') }} dt
    on  dt.provider = a.data_source
   and dt.native_team_id = a.team_id_native
