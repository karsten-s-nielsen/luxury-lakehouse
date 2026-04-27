-- int_pausa__pass_quality.sql
-- Ephemeral CTE joining PAUSA values with player display names.
--
-- Enriches PAUSA scores with human-readable player names for downstream mart
-- aggregation and Streamlit display.
--
-- PR 7 (ADR-013 second application): reads from fct_pausa_values (the
-- dbt-built gold mart that inherits Kimball FKs via INNER JOIN to fct_passes)
-- instead of stg_pausa__values. Surfaces match_key + team_key + player_key
-- so downstream marts (fct_pausa_rankings, fct_pass_timing) can passthrough
-- the surrogate FKs.

{{ config(materialized='ephemeral', enabled=var('pausa_enabled', false)) }}

with pausa as (

    select * from {{ ref('fct_pausa_values') }}

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
        p.match_key,
        p.player_id,
        p.player_key,
        p.team,
        p.team_key,
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
