-- stg_shape_graphs__positions.sql
-- Deduplicate frame-level position labels from the shape graph algorithm.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, frame_id, player_id),
-- latest _ingested_at wins. Handles pipeline re-runs producing duplicate rows.
--
-- Reference: Sotudeh, S. (2026). Shape graph formation detection.

with source as (

    select * from {{ source('shape_graphs', 'player_positions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, frame_id, player_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_id as string)          as match_id,
        cast(frame_id as bigint)          as frame_id,
        cast(player_id as string)         as player_id,
        cast(team as string)              as team,
        cast(position_label as string)    as position_label,
        cast(vertical_level as string)    as vertical_level,
        cast(horizontal_level as string)  as horizontal_level,
        cast(detector as string)          as detector,
        cast(_ingested_at as timestamp)   as _ingested_at,

        -- PR 7 (ADR-011): derive source_provider from the match_id prefix
        -- convention used by the upstream tracking stagings (fct_tracking_frames
        -- inherits these prefixes). The shape-graph algorithm runs on
        -- IDSSE/Metrica/SkillCorner only — StatsBomb / Wyscout don't produce
        -- tracking frames so cannot collide here. Single source of truth for
        -- downstream marts that JOIN dim_matches / dim_teams / dim_players on
        -- (provider, native_id).
        case
            when match_id like 'idsse_%'        then 'idsse'
            when match_id like 'Sample_Game_%'  then 'metrica'
            else 'skillcorner'
        end                                as source_provider

    from deduplicated
    where _row_num = 1

)

select * from cleaned
