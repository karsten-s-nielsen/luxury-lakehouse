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
        -- PR 7 hotfix #3: strip the `idsse_` prefix at staging boundary so
        -- mart-side JOINs to dim_matches.native_match_id match. Pre-fix:
        -- IDSSE rows had pp.match_id='idsse_J03WMX' that couldn't JOIN
        -- dim_matches.native_match_id='J03WMX'. source_provider derivation
        -- below uses the still-prefixed bronze match_id BEFORE the strip.
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,
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

        -- PR-1.5 (SK3-MIG-B): source_provider now comes from bronze directly.
        -- The shape graph algorithm propagates source_provider from fct_tracking_frames
        -- to bronze.player_positions. For pre-PR-1.5 rows that lack the column,
        -- fall back to pattern derivation (backward compat).
        coalesce(
            cast(source_provider as string),
            case
                when match_id like 'skillcorner_%'  then 'skillcorner'
                when match_id like 'Sample_Game_%'  then 'metrica'
                when match_id like 'idsse_%'        then 'idsse'
                else 'idsse'  -- Un-prefixed IDSSE (post-PR7-hotfix format)
            end
        )                                  as source_provider

    from deduplicated
    where _row_num = 1

)

select * from cleaned
