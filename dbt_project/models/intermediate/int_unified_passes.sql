{{ config(materialized='view', schema='silver') }}
-- int_unified_passes.sql
-- Union StatsBomb, Wyscout, IDSSE, and Metrica pass events into a common
-- Kimball-conformed shape. Each per-source CTE emits an identical schema;
-- the final `keyed` CTE joins `dim_matches` to assign `match_key` (the
-- Kimball surrogate BIGINT FK, ADR-011). Native `match_id` is deliberately
-- not present in the output — recover via JOIN dim_matches ON match_key.
--
-- Materialization: view (upgraded from ephemeral in PR 2 for debuggability
-- and so test_dbt_passes_kimball_migration.py can assert against this
-- model's content directly).
--
-- Progressive pass: end point is at least 25% closer to the opponent's
-- goal centre than the start point (by Euclidean distance).
--
-- Known gaps for IDSSE + Metrica (see stg_idsse__passes.sql /
-- stg_metrica__passes.sql for details):
--   * team_id / player_id / pass_recipient_id NULL (string IDs in source)
--   * end_x / end_y NULL for IDSSE (no end coord on DFL <Play> row)

with statsbomb_passes as (

    select
        cast(event_id as string)                                as event_id,
        cast(match_id as string)                                as native_match_id,
        'statsbomb'                                             as provider,
        cast(player_id as int)                                  as player_id,
        cast(team_id as int)                                    as team_id,
        cast(pass_recipient_id as int)                          as pass_recipient_id,
        -- PR 7 (ADR-011): native_* STRING columns surfaced for dim_teams /
        -- dim_players JOINs in fct_passes. SB native IDs are real BIGINTs;
        -- cast to string preserves identity. NULL passthrough preserved.
        cast(team_id as string)                                 as native_team_id,
        cast(player_id as string)                               as native_player_id,
        cast(pass_recipient_id as string)                       as native_recipient_id,
        cast(period as bigint)                                  as period,
        cast(minute as bigint)                                  as minute,
        cast(second as bigint)                                  as second,
        cast(location_x as double)                              as start_x,
        cast(location_y as double)                              as start_y,
        cast(get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 0) as double) as end_x,
        cast(get(from_json(pass_end_location, 'ARRAY<DOUBLE>'), 1) as double) as end_y,
        pass_type,
        pass_height,
        pass_body_part                                          as body_part,
        cast(pass_length as double)                             as pass_length,
        cast(pass_angle as double)                              as pass_angle_radians,
        pass_outcome,
        coalesce(pass_cross, false)                             as is_cross,
        coalesce(pass_switch, false)                            as is_switch,
        coalesce(pass_through_ball, false)                      as is_through_ball,
        {{ distance_to_goal(
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 0)',
            'get(from_json(pass_end_location, \'ARRAY<DOUBLE>\'), 1)'
        ) }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('location_x', 'location_y') }}
                                                                as is_progressive,
        'statsbomb'                                             as data_source
    from {{ ref('stg_statsbomb__events') }}
    where event_type = 'Pass'

),

wyscout_passes as (

    select
        cast(event_sk as string)                                as event_id,
        cast(match_id as string)                                as native_match_id,
        'wyscout'                                               as provider,
        cast(player_id as int)                                  as player_id,
        cast(team_id as int)                                    as team_id,
        cast(null as int)                                       as pass_recipient_id,
        -- PR 7 (ADR-011): WS open-data has no recipient field (kloppy strips
        -- it). team / player native IDs are real BIGINTs cast to string.
        cast(team_id as string)                                 as native_team_id,
        cast(player_id as string)                               as native_player_id,
        cast(null as string)                                    as native_recipient_id,
        cast(period as bigint)                                  as period,
        cast(floor(event_sec / 60) as bigint)                   as minute,
        cast(cast(event_sec as int) % 60 as bigint)             as second,
        cast(start_x as double)                                 as start_x,
        cast(start_y as double)                                 as start_y,
        cast(end_x as double)                                   as end_x,
        cast(end_y as double)                                   as end_y,
        sub_event_type                                          as pass_type,
        cast(null as string)                                    as pass_height,
        cast(null as string)                                    as body_part,
        cast(sqrt(power(end_x - start_x, 2) + power(end_y - start_y, 2)) as double) as pass_length,
        cast(atan2(end_y - start_y, end_x - start_x) as double) as pass_angle_radians,
        case when is_accurate then 'Complete' else 'Incomplete' end as pass_outcome,
        sub_event_type in ('Cross', 'Head cross')               as is_cross,
        sub_event_type = 'Launch'                               as is_switch,
        sub_event_type = 'Through pass'                         as is_through_ball,
        {{ distance_to_goal('end_x', 'end_y') }}
            < {{ var('progressive_pass_ratio') }} * {{ distance_to_goal('start_x', 'start_y') }}
                                                                as is_progressive,
        'wyscout'                                               as data_source
    from {{ ref('stg_wyscout__events') }}
    where event_type = 'Pass'

),

idsse_passes as (

    select
        cast(event_id as string)                                as event_id,
        cast(match_id as string)                                as native_match_id,
        'idsse'                                                 as provider,
        cast(player_id as int)                                  as player_id,
        cast(team_id as int)                                    as team_id,
        cast(pass_recipient_id as int)                          as pass_recipient_id,
        -- PR 7 (ADR-011): IDSSE legacy INT IDs are forced NULL upstream
        -- (DFL native IDs are STRING). Surface the real DFL identifiers
        -- (CLU / OBJ) for dim_teams + dim_players JOINs in fct_passes.
        team_id_native                                          as native_team_id,
        player_id_native                                        as native_player_id,
        pass_recipient_id_native                                as native_recipient_id,
        cast(period as bigint)                                  as period,
        cast(minute as bigint)                                  as minute,
        cast(second as bigint)                                  as second,
        cast(start_x as double)                                 as start_x,
        cast(start_y as double)                                 as start_y,
        cast(end_x as double)                                   as end_x,
        cast(end_y as double)                                   as end_y,
        pass_type,
        pass_height,
        body_part,
        cast(pass_length as double)                             as pass_length,
        cast(pass_angle_radians as double)                      as pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        is_progressive,
        data_source
    from {{ ref('stg_idsse__passes') }}

),

metrica_passes as (

    select
        cast(event_id as string)                                as event_id,
        cast(match_id as string)                                as native_match_id,
        'metrica'                                               as provider,
        cast(player_id as int)                                  as player_id,
        cast(team_id as int)                                    as team_id,
        cast(pass_recipient_id as int)                          as pass_recipient_id,
        -- PR 7 (ADR-011): Metrica anonymised — synthesize native IDs that
        -- match the dim_teams.metrica_anon_teams + dim_players.metrica_anon_players
        -- patterns from PR 5a (concat('metrica_', match_id, '_', side, ...)).
        -- team_side is 'Home'/'Away' from bronze; lower() to match dim synth.
        concat('metrica_', match_id, '_', lower(team_side))     as native_team_id,
        case
            when player_id_native is not null
                then concat('metrica_', match_id, '_', lower(team_side), '_', player_id_native)
        end                                                     as native_player_id,
        case
            when pass_recipient_id_native is not null
                then concat('metrica_', match_id, '_', lower(team_side), '_', pass_recipient_id_native)
        end                                                     as native_recipient_id,
        cast(period as bigint)                                  as period,
        cast(minute as bigint)                                  as minute,
        cast(second as bigint)                                  as second,
        cast(start_x as double)                                 as start_x,
        cast(start_y as double)                                 as start_y,
        cast(end_x as double)                                   as end_x,
        cast(end_y as double)                                   as end_y,
        pass_type,
        pass_height,
        body_part,
        cast(pass_length as double)                             as pass_length,
        cast(pass_angle_radians as double)                      as pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        is_progressive,
        data_source
    from {{ ref('stg_metrica__passes') }}

),

unioned as (

    select * from statsbomb_passes
    union all
    select * from wyscout_passes
    union all
    select * from idsse_passes
    union all
    select * from metrica_passes

),

keyed as (

    select
        u.event_id,
        dm.match_key,
        u.player_id,
        u.team_id,
        u.pass_recipient_id,
        -- PR 7 (ADR-011): native_* STRINGs flow through to fct_passes for
        -- dim_teams / dim_players JOINs. Provider stays implicit via data_source.
        u.native_team_id,
        u.native_player_id,
        u.native_recipient_id,
        u.period,
        u.minute,
        u.second,
        u.start_x,
        u.start_y,
        u.end_x,
        u.end_y,
        u.pass_type,
        u.pass_height,
        u.body_part,
        u.pass_length,
        u.pass_angle_radians,
        u.pass_outcome,
        u.is_cross,
        u.is_switch,
        u.is_through_ball,
        u.is_progressive,
        u.data_source

    from unioned u
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = u.provider
       and dm.native_match_id = u.native_match_id

)

select * from keyed
