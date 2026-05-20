-- stg_shape_graphs__positions.sql
-- Deduplicate frame-level position labels from the shape graph algorithm.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, frame_id, player_id),
-- latest _ingested_at wins. source_provider is written at bronze ingestion
-- time — no derivation from match_id patterns.
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
        cast(match_id as string) as match_id,
        cast(frame_id as bigint)          as frame_id,
        -- PR 7 hotfix #3: synth Metrica player_id to match dim_players' recipe.
        -- Bronze formations player_id is bare numeric ('5'); dim_players is
        -- synth form 'metrica_<match>_<side>_5'. IDSSE/SkillCorner already
        -- emit dim-compatible IDs (DFL-OBJ-* / numeric SkillCorner IDs) so
        -- pass through unchanged.
        case
            when cast(match_id as string) like 'Sample_Game_%'
                then concat('metrica_', cast(match_id as string), '_', cast(team as string), '_', cast(player_id as string))
            else cast(player_id as string)
        end                                as player_id,
        cast(team as string)              as team,
        cast(position_label as string)    as position_label,
        cast(vertical_level as string)    as vertical_level,
        cast(horizontal_level as string)  as horizontal_level,
        cast(detector as string)          as detector,
        cast(_ingested_at as timestamp)   as _ingested_at,

        cast(source_provider as string)    as source_provider

    from deduplicated
    where _row_num = 1

)

select * from cleaned
