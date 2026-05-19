{{ config(
    materialized='view',
    schema='silver',
    tags=['intermediate_mart']
) }}
-- int_minutes_played_per_match.sql
-- Provider-agnostic per-match minutes played per player.
--
-- Grain: one row per (match_key, player_key).
-- Outputs surrogates only -- no native IDs. IDSSE uses DFL string IDs
-- (e.g. "DFL-MAT-J03WN9") that cannot be cast to BIGINT, so a uniform
-- surrogate-only contract avoids the type mismatch across providers.
--
-- Provider legs:
--   StatsBomb: event-based (lineup + substitution events + max-minute duration)
--   Wyscout:   event-based (stg_wyscout__lineups minute_on/minute_off + last event_sec)
--   IDSSE:     event-based (tracking_context roster + substitution events + FinalWhistle)
--   SkillCorner: metadata-direct (pre-computed minutes_played from match.json)
--   Metrica:   excluded (anonymized sample data, no substitution events)

-- ===== StatsBomb leg =====

with sb_lineups as (

    select
        match_id,
        player_id
    from {{ ref('stg_statsbomb__lineups') }}
    where position_name is not null

),

sb_events as (

    select
        match_id,
        minute,
        event_type,
        player_id,
        substitution_replacement_id
    from {{ ref('stg_statsbomb__events') }}

),

sb_match_duration as (

    select
        match_id,
        max(minute) + 1                                 as match_end_minute
    from sb_events
    group by match_id

),

sb_sub_off as (

    select
        match_id,
        player_id,
        minute                                           as off_minute
    from sb_events
    where event_type = 'Substitution'

),

sb_sub_on as (

    select
        match_id,
        cast(substitution_replacement_id as int)         as player_id,
        minute                                           as on_minute
    from sb_events
    where event_type = 'Substitution'
      and substitution_replacement_id is not null

),

sb_player_minutes as (

    -- Starting XI
    select
        l.match_id,
        l.player_id,
        coalesce(so.off_minute, md.match_end_minute)     as minutes_played
    from sb_lineups l
    inner join sb_match_duration md
        on l.match_id = md.match_id
    left join sb_sub_off so
        on l.match_id = so.match_id
        and l.player_id = so.player_id

    union all

    -- Substitutes coming on
    select
        son.match_id,
        son.player_id,
        coalesce(soff.off_minute, md.match_end_minute) - son.on_minute as minutes_played
    from sb_sub_on son
    inner join sb_match_duration md
        on son.match_id = md.match_id
    left join sb_sub_off soff
        on son.match_id = soff.match_id
        and son.player_id = soff.player_id

),

sb_deduped as (

    select
        match_id,
        player_id,
        'statsbomb'                                      as data_source,
        cast(max(minutes_played) as double)              as minutes_played
    from sb_player_minutes
    group by match_id, player_id

),

-- ===== Wyscout leg =====

ws_lineups as (

    select
        match_id,
        player_id,
        minute_on,
        minute_off
    from {{ ref('stg_wyscout__lineups') }}

),

ws_match_duration as (

    -- Last event second per match, converted to minutes.
    -- Fallback 90 applied only when the match has zero events.
    select
        match_id,
        coalesce(max(event_sec) / 60.0, 90.0)           as match_end_minute
    from {{ ref('stg_wyscout__events') }}
    group by match_id

),

ws_player_minutes as (

    select
        wl.match_id,
        wl.player_id,
        'wyscout'                                        as data_source,
        cast(
            coalesce(wl.minute_off, wd.match_end_minute) - wl.minute_on
        as double)                                       as minutes_played
    from ws_lineups wl
    inner join ws_match_duration wd
        on wl.match_id = wd.match_id

),

-- ===== IDSSE leg =====

idsse_roster as (

    -- Player roster per match from tracking context (TC-1).
    -- Every player who generated at least one SPADL action appears here.
    select distinct
        native_match_id,
        player_id_native
    from {{ ref('stg_spadl__tracking_context') }}
    where data_source = 'idsse'

),

idsse_events as (

    select
        cast(match_id as string)                         as native_match_id,
        event_type,
        period,
        timestamp_seconds,
        sub_player_in,
        sub_player_out,
        sub_team
    from {{ ref('stg_idsse__events') }}

),

idsse_period_duration as (

    -- FinalWhistle timestamp_seconds per period (period-local).
    select
        native_match_id,
        period,
        max(timestamp_seconds)                           as period_end_seconds
    from idsse_events
    where event_type = 'FinalWhistle'
    group by native_match_id, period

),

idsse_match_duration as (

    -- Total match duration = sum of all period durations.
    select
        native_match_id,
        sum(period_end_seconds)                          as match_end_seconds
    from idsse_period_duration
    group by native_match_id

),

idsse_period1_duration as (

    -- Period 1 duration for converting period 2 timestamps to match-absolute.
    select
        native_match_id,
        period_end_seconds                               as p1_end_seconds
    from idsse_period_duration
    where period = 1

),

idsse_subs as (

    -- Substitution events with match-absolute seconds.
    select
        e.native_match_id,
        cast(e.sub_player_in as string)                  as player_in_native,
        cast(e.sub_player_out as string)                 as player_out_native,
        case
            when e.period = 1 then e.timestamp_seconds
            else coalesce(p1.p1_end_seconds, 0) + e.timestamp_seconds
        end                                              as sub_absolute_seconds
    from idsse_events e
    left join idsse_period1_duration p1
        on e.native_match_id = p1.native_match_id
    where e.event_type = 'Substitution'
      and e.sub_player_in is not null

),

idsse_sub_on as (

    select
        native_match_id,
        player_in_native                                 as player_id_native,
        sub_absolute_seconds                             as on_seconds
    from idsse_subs

),

idsse_sub_off as (

    select
        native_match_id,
        player_out_native                                as player_id_native,
        sub_absolute_seconds                             as off_seconds
    from idsse_subs

),

idsse_corrected as (

    select
        r.native_match_id,
        r.player_id_native,
        'idsse'                                          as data_source,
        cast(case
            when son.on_seconds is not null
            -- Substitute: played from sub entry to sub exit or match end.
            then (coalesce(soff.off_seconds, md.match_end_seconds) - son.on_seconds) / 60.0
            -- Starter: played from 0 to sub exit or match end.
            else coalesce(soff.off_seconds, md.match_end_seconds) / 60.0
        end as double)                                   as minutes_played
    from idsse_roster r
    inner join idsse_match_duration md
        on r.native_match_id = md.native_match_id
    left join idsse_sub_on son
        on  r.native_match_id = son.native_match_id
        and r.player_id_native = son.player_id_native
    left join idsse_sub_off soff
        on  r.native_match_id = soff.native_match_id
        and r.player_id_native = soff.player_id_native

),

-- ===== SkillCorner leg =====

sc_player_minutes as (

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        'skillcorner'                                    as data_source,
        cast(minutes_played as double)                   as minutes_played
    from {{ ref('stg_skillcorner__matches') }}
    where minutes_played is not null

),

-- ===== UNION all legs (native IDs as STRING for uniform dim resolution) =====

unioned as (

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        data_source,
        minutes_played
    from sb_deduped

    union all

    select
        cast(match_id as string)                         as native_match_id,
        cast(player_id as string)                        as player_id_native,
        data_source,
        minutes_played
    from ws_player_minutes

    union all

    select
        native_match_id,
        player_id_native,
        data_source,
        minutes_played
    from idsse_corrected

    union all

    select
        native_match_id,
        player_id_native,
        data_source,
        minutes_played
    from sc_player_minutes

),

-- ===== Single dim resolution -- surrogate-only output =====
-- IDSSE native IDs are DFL strings (e.g. "DFL-MAT-J03WN9") that cannot
-- be cast to BIGINT. Outputting surrogates only avoids the type mismatch.
-- Downstream consumers JOIN on (match_key, player_key).

final as (

    select
        dm.match_key,
        dp.player_key,
        u.data_source,
        u.minutes_played
    from unioned u
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = u.data_source
        and dm.native_match_id = u.native_match_id
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = u.data_source
        and dp.native_player_id = u.player_id_native

)

select
    match_key,
    player_key,
    data_source,
    minutes_played
from final
where minutes_played is not null
  and minutes_played >= 0
