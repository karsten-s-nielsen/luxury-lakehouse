-- fct_action_defensive.sql
-- Gold-layer per-action defending-team defensive credit (silly-kicks 4.87.0
-- TF-51; spec §7.5, Task 17d). Grain: one row per action (match_key, action_id).
--
-- DAG position (spec §7.5 / review-4): this mart is DOWNSTREAM of fct_shot_xg,
-- NOT a column on fct_action_values. The credit is xG-sized, and fct_shot_xg
-- already ref()s fct_action_values, so adding an xG-derived column to
-- fct_action_values would be a dbt CYCLE. Instead the Python writer
-- (ingestion.defensive_credit_writer) LEFT-JOINs per-shot xG onto the actions,
-- scores add_defensive_credit, and lands bronze.action_defensive_credit
-- (ADR-013); this mart resolves the Kimball surrogates from the AC identity fact
-- fct_action_values (INNER JOIN on the native action key) and LEFT-JOINs
-- fct_shot_xg to expose the shot's own xG on shot-action rows (the meaningful
-- downstream edge). No cycle: this mart is a sink of both fct_shot_xg and
-- fct_action_values.
--
-- Enabled via var 'xg_v3_enabled' (same gate as fct_shot_xg — it ref()s it).
-- team_key is the ACTING team of the action; the credit columns are the
-- DEFENDING team's aggregate earned against that action (add_defensive_credit
-- semantics). defensive_credit_net/_plus/_minus are 0.0 (never NULL) where no
-- credit fired; n_defensive_credits is 0.

{{ config(
    materialized='table',
    enabled=var('xg_v3_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with credit as (

    select * from {{ ref('stg_action_defensive') }}

),

identity as (

    select
        data_source,
        match_id_native,
        action_id,
        match_key,
        competition_key,
        team_key,
        player_key
    from {{ ref('fct_action_values') }}

),

shot_xg as (

    select match_key, action_id, xg as shot_xg
    from {{ ref('fct_shot_xg') }}

)

select
    i.match_key,
    c.action_id,
    c.data_source,
    c.defensive_credit_net,
    c.defensive_credit_plus,
    c.defensive_credit_minus,
    c.n_defensive_credits,
    i.competition_key,
    i.team_key,
    i.player_key,
    x.shot_xg

from credit c
inner join identity i
    on  i.data_source = c.data_source
   and i.match_id_native = c.native_match_id
   and i.action_id = c.action_id
left join shot_xg x
    on  x.match_key = i.match_key
   and x.action_id = c.action_id
