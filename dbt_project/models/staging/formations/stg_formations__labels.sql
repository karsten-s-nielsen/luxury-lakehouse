-- stg_formations__labels.sql
-- Clean and deduplicate formation detection results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (match_id, period, team, window_start_s),
-- latest _ingested_at wins. Handles pipeline re-runs producing duplicate rows.
--
-- Reference: Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team
-- strategy in professional football."

with source as (

    select * from {{ source('formations', 'formation_labels') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by match_id, period, team, window_start_s
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(match_id as string)           as match_id,
        cast(period as int)                as period,
        cast(team as string)               as team,
        cast(window_start_s as double)     as window_start_s,
        cast(window_end_s as double)       as window_end_s,
        cast(formation_label as string)    as formation_label,
        cast(cost as double)               as cost,
        coalesce(cast(detector as string), 'efpi') as detector,
        cast(_ingested_at as timestamp)    as _ingested_at,

        -- PR 7 (ADR-011): derive source_provider from the match_id prefix
        -- convention used by the upstream tracking stagings (fct_tracking_frames
        -- inherits these prefixes). The formation-detection algorithm runs on
        -- IDSSE/Metrica/SkillCorner only — StatsBomb / Wyscout don't produce
        -- tracking frames so cannot collide here. Single source of truth for
        -- downstream marts that JOIN dim_matches / dim_teams on
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
