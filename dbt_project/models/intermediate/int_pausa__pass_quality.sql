-- int_pausa__pass_quality.sql
-- Ephemeral CTE joining PAUSA values with player and team dimension tables.
--
-- Enriches raw PAUSA scores with human-readable player names and team names
-- for downstream mart aggregation and Streamlit display.

{{ config(materialized='ephemeral', enabled=var('pausa_enabled', false)) }}

with pausa as (

    select * from {{ ref('stg_pausa__values') }}

),

players as (

    select
        player_id,
        player_display_name
    from {{ ref('dim_players') }}

),

final as (

    select
        p.pass_id,
        p.match_id,
        p.player_id,
        p.team,
        p.period,
        p.timestamp_seconds,
        p.frame_id,
        p.temporal_judgment,
        p.spatial_selection,
        p.pausa_score,
        p.actual_obso,
        p.peak_obso,
        p.optimal_obso,
        p.receiver_x,
        p.receiver_y,
        pl.player_display_name

    from pausa p
    left join players pl
        on cast(p.player_id as string) = cast(pl.player_id as string)

)

select * from final
