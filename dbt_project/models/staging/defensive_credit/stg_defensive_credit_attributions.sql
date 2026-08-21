-- stg_defensive_credit_attributions.sql
-- Staging view for the long-form defensive-credit attributions (spec §7.5, Task 17f; ADR-013
-- writer-fed). Source: bronze.defensive_credit_attributions, written by ingestion.defensive_credit_writer
-- (silly-kicks compute_defensive_credits). Renames match_id -> native_match_id and player_id/team_id ->
-- *_native for the Kimball-side resolution in fct_defensive_credit_attributions.
--
-- NO dedup: the long-form is a per-credit-EVENT log with NO natural unique key — a single
-- (action, player, rule) can legitimately carry MANY rows (e.g. the synchronized_final_third_pressure
-- rule credits a defender once per synchronized presser). A ROW_NUMBER dedup on (action, player, rule)
-- would silently DROP those rows. Idempotency is guaranteed UPSTREAM instead: the writer overwrites each
-- match atomically via replaceWhere (data_source, match_id), so bronze never accumulates cross-write
-- duplicates for a match.

with source as (

    select * from {{ source('defensive_credit', 'defensive_credit_attributions') }}

)

select
    cast(data_source as string)   as data_source,
    cast(match_id as string)      as native_match_id,
    cast(game_id as bigint)       as game_id,
    cast(period_id as bigint)     as period_id,
    cast(action_id as bigint)     as action_id,
    cast(player_id as string)     as player_id_native,
    cast(team_id as string)       as team_id_native,
    cast(rule as string)          as rule,
    cast(signed_value as double)  as signed_value,
    cast(anchor_type as string)   as anchor_type,
    cast(frame_id as bigint)      as frame_id,
    cast(sizing as string)        as sizing,
    cast(resolution as string)    as resolution

from source
