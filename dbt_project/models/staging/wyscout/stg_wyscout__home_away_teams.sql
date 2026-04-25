-- stg_wyscout__home_away_teams.sql
-- Bridge: one row per (match_id, side, team_id) for Wyscout matches.
--
-- Wyscout's nested `teamsData` MAP (parsed in stg_wyscout__matches per PR 1.5)
-- carries per-team data keyed by team_id string. The map's STRUCT values
-- carry `side` ('home' / 'away'). PR 5a adopts this as the canonical
-- home/away bridge so dim_teams + fct_match_summary can populate Wyscout
-- team_ids (previously NULL for ~36% of Wyscout rows).
--
-- Synth fallback: when `teams_data_parsed` is NULL or empty (parse failure),
-- emit two synthesised rows with is_synthesized=true +
-- synthesis_reason='wyscout_unresolved_teamsdata'. This guarantees every
-- match produces home+away rows so fct_funnel_stages_agg.opponent_team_id
-- can flip warn→error even if source data is patchy.

with matches as (

    select
        match_id,
        teams_data_parsed
    from {{ ref('stg_wyscout__matches') }}

),

-- Primary path: explode teams_data_parsed to (match_id, side, team_id)
exploded as (

    select
        match_id,
        case
            when v.side = 'home' then 'home'
            when v.side = 'away' then 'away'
            else null
        end                                            as side,
        cast(k as int)                                 as team_id,
        false                                          as is_synthesized,
        cast(null as string)                           as synthesis_reason
    from matches
    lateral view explode(teams_data_parsed) AS k, v
    where teams_data_parsed is not null
      and size(map_keys(teams_data_parsed)) > 0

),

-- Fallback path: synth rows for matches where parse yielded NULL/empty map
synth as (

    select
        match_id,
        'home'                                          as side,
        cast(null as int)                               as team_id,
        true                                            as is_synthesized,
        'wyscout_unresolved_teamsdata'                  as synthesis_reason
    from matches
    where teams_data_parsed is null
       or size(map_keys(teams_data_parsed)) = 0

    union all

    select
        match_id,
        'away'                                          as side,
        cast(null as int)                               as team_id,
        true                                            as is_synthesized,
        'wyscout_unresolved_teamsdata'                  as synthesis_reason
    from matches
    where teams_data_parsed is null
       or size(map_keys(teams_data_parsed)) = 0

),

combined as (

    select * from exploded
    union all
    select * from synth

),

final as (

    select
        match_id,
        side,
        -- native_team_id: string-form for dim_teams surrogate derivation.
        -- Real rows: cast int team_id to string. Synth rows: deterministic
        -- 'wyscout_unresolved_<match_id>_<side>' so dim_teams can mark them
        -- with synthesis_reason + generate a stable team_key via xxhash.
        case
            when is_synthesized
                then concat('wyscout_unresolved_', cast(match_id as string), '_', side)
            else cast(team_id as string)
        end                                             as native_team_id,
        team_id,
        is_synthesized,
        synthesis_reason
    from combined
    where side is not null

)

select * from final
