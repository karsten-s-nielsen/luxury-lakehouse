{{ config(enabled=var('space_creation_enabled', false)) }}

with source as (
    select * from {{ source('space_creation', 'space_creation_values') }}
),

deduplicated as (
    select *, row_number() over (
        partition by match_id, frame_id, player_id
        order by _ingested_at desc
    ) as _rn
    from source
),

cleaned as (
    select
        cast(match_id as string) as match_id,
        cast(frame_id as int) as frame_id,
        cast(player_id as string) as player_id,
        cast(team as string) as team,
        cast(period as int) as period,
        cast(space_created_m2 as double) as space_created_m2,
        cast(space_destroyed_m2 as double) as space_destroyed_m2,
        cast(net_space_m2 as double) as net_space_m2,

        -- PR 7 (ADR-011): derive source_provider from match_id prefix
        -- (space-creation pipeline reads from fct_tracking_frames; SB/WS
        -- don't contribute tracking data so cannot collide here).
        case
            when match_id like 'idsse_%'        then 'idsse'
            when match_id like 'Sample_Game_%'  then 'metrica'
            else 'skillcorner'
        end as source_provider
    from deduplicated
    where _rn = 1
)

select * from cleaned
