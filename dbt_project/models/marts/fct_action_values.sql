{{ config(
    materialized='incremental',
    unique_key='action_value_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_action_values.sql
-- Gold-layer SPADL action values with VAEP scores, possession context,
-- and per-action game state.
--
-- Contains every on-ball action from all data sources converted to the
-- SPADL unified format, scored with offensive, defensive, and net VAEP
-- values. Enables player ranking by total contribution beyond goals/assists.
--
-- PR 4b (2026-04-23): Kimball-conformed per ADR-011. Emits `match_key` +
-- `competition_key` (BIGINT Kimball surrogates, resolved via LEFT JOIN
-- dim_matches on (native_match_id, provider)). Retains legacy `match_id`
-- and `competition_id` (both BIGINT nullable) for the 90-day dual-column
-- window — removed on or after 2026-07-22 per ADR-011 policy.
--
-- Coordinate system: 105x68 meters (SPADL academic standard).
-- One row per action.

with action_values as (

    select * from {{ ref('stg_spadl__action_values') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }} where match_id is not null)
    {% endif %}

),

-- LL1 (silly-kicks 1.5.0+): possession_id and possession_team_id are now
-- preserved through SPADL conversion via the silly-kicks ``preserve_native``
-- kwarg, sourced upstream from ``stg_spadl__action_values`` rather than via
-- a late-join to ``stg_statsbomb__events``. Eliminates a per-row JOIN on
-- ``original_event_id`` that ran for every StatsBomb action. Plus we now
-- carry ``play_pattern`` and ``under_pressure`` for analytics use.

running_score as (

    select * from {{ ref('int_running_score') }}

),

-- Resolve Kimball surrogates via dim_matches. dim_matches is keyed on
-- (provider, native_match_id); our source column is (data_source, match_id),
-- which is the same concept under different names.
match_attrs as (

    select
        match_key,
        competition_key,
        provider,
        native_match_id
    from {{ ref('dim_matches') }}

),

-- Join each action to its most recent score milestone.
-- The kickoff row (period=1, minute=0, second=0) ensures every action
-- in period >= 1 has at least one matching score row.
actions_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'av.match_id',
            'av.period',
            'av.time_seconds',
            'av.player_id',
            'av.type_id',
            'av.data_source'
        ]) }}                                       as action_value_id,

        -- Kimball surrogates (new canonical)
        ma.match_key,
        ma.competition_key,

        -- Legacy columns for 90-day dual-column window (sunset 2026-07-22).
        -- av.match_id is already BIGINT on the source; retain as-is.
        -- av.competition_id is already typed; retain as-is.
        av.match_id,
        av.competition_id,

        av.player_id,
        av.team_id,
        -- PR 7 (ADR-011): Kimball surrogate FKs via dim_teams / dim_players
        -- JOINs on (provider, native_id). SPADL is SB+WS only — native IDs
        -- are real BIGINTs cast to string for the JOIN. PR 8 drops the
        -- legacy INT cols.
        dt.team_key,
        dp.player_key,
        av.season_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,

        -- SPADL coordinates (105x68 meters)
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,

        -- Action classification
        av.action_type,
        av.action_result,
        av.bodypart,

        -- VAEP scores
        av.offensive_value,
        av.defensive_value,
        av.vaep_value,

        -- Possession context (StatsBomb-only; NULL for Wyscout / IDSSE / SkillCorner).
        -- Sourced upstream via silly-kicks 1.5.0 preserve_native kwarg (LL1).
        av.statsbomb_possession_id                  as possession_id,
        -- Legacy `possession_team_id` retained inside the ADR-011 dual-column
        -- window (sunset 2026-07-22 alongside team_id / match_id / competition_id).
        -- The Kimball surrogate `possession_team_key` is the canonical FK.
        av.statsbomb_possession_team_id             as possession_team_id,
        -- Kimball surrogate FK for the possession team — same dim_teams
        -- resolution pattern as `team_key` / `player_key` above.
        dt_poss.team_key                            as possession_team_key,

        -- Pure descriptors (no FK semantics) — StatsBomb-only.
        av.statsbomb_play_pattern                   as play_pattern,
        av.statsbomb_under_pressure                 as under_pressure,

        -- Running score for game state derivation
        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id                             as _rs_home_team_id,

        -- Rank to pick the most recent score milestone
        row_number() over (
            partition by
                av.match_id, av.period, av.time_seconds,
                av.player_id, av.type_id, av.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        )                                           as _score_rn,

        -- Provenance
        av.data_source,
        av.original_event_id

    from action_values av
    left join match_attrs ma
        on cast(av.match_id as string) = ma.native_match_id
        and av.data_source = ma.provider
    left join running_score rs
        on rs.match_id = av.match_id
        and (
            rs.period < av.period
            or (rs.period = av.period
                and (rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second))
        )
    -- PR 7 (ADR-011): dim_teams / dim_players JOINs by (provider, native_id).
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = av.data_source
       and dt.native_team_id = cast(av.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = av.data_source
       and dp.native_player_id = cast(av.player_id as string)
    -- LL1 (silly-kicks 1.5.0+): resolve `possession_team_key` via dim_teams
    -- using the StatsBomb-native team ID. Same (provider, native_id) pattern
    -- as `team_key` / `player_key` above. NULL on non-StatsBomb sources where
    -- statsbomb_possession_team_id is NULL.
    left join {{ ref('dim_teams') }} dt_poss
        on  dt_poss.provider = av.data_source
       and dt_poss.native_team_id = cast(av.statsbomb_possession_team_id as string)

),

final as (

    select
        action_value_id,
        match_key,
        competition_key,
        match_id,
        competition_id,
        player_id,
        team_id,
        team_key,
        player_key,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        action_type,
        action_result,
        bodypart,
        offensive_value,
        defensive_value,
        vaep_value,
        possession_id,
        possession_team_id,
        possession_team_key,
        play_pattern,
        under_pressure,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end                                         as game_state,
        data_source,
        original_event_id,
        current_timestamp()                         as _loaded_at

    from actions_with_score
    where _score_rn = 1

)

select * from final
