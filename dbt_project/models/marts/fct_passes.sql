{{ config(
    materialized='incremental',
    unique_key='pass_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
-- fct_passes.sql
-- Gold-layer pass fact table — every pass from all four providers
-- (StatsBomb, Wyscout, IDSSE, Metrica) keyed by `match_key` (Kimball
-- surrogate FK to `dim_matches` per ADR-011). Native `match_id` is
-- deliberately NOT present on this mart; recover via
-- `JOIN dim_matches ON match_key` when the provider-native ID is needed.
--
-- Incremental strategy is merge-on-pass_id; a previous match_id-based
-- "skip already-ingested matches" predicate was dropped in PR 2 because
-- merge already guarantees idempotency per primary key and correctness
-- trumps incremental-build speed for a gold mart.
--
-- Progressive pass: end point is >=25% closer to the opponent's goal
-- centre than the start point (by Euclidean distance).
--
-- Line-breaking: computed externally (Ward hierarchical clustering on
-- tracking frames), persisted to bronze.line_breaking_results, joined
-- here on event_id via stg_line_breaking__results.
--
-- Known gaps for IDSSE/Metrica rows (see stg_idsse__passes.sql /
-- stg_metrica__passes.sql for rationale):
--   * team_id / player_id / pass_recipient_id are NULL (source IDs
--     are strings; cross-provider surrogate keys are a later PR).
--   * end_x / end_y NULL for IDSSE (DFL <Play> row carries start only).
--   * home_score_after / away_score_after NULL — int_running_score is
--     SB+WS only; game_state defaults to 'drawing' for IDSSE/Metrica.

with unified_passes as (

    select * from {{ ref('int_unified_passes') }}

),

match_attrs as (

    select
        match_key,
        -- PR 2 (ADR-011) Kimball surrogate FK for competition.
        -- NULL for Metrica (no competition metadata).
        competition_key,
        try_cast(competition_id as int)                 as competition_id,
        try_cast(season_id as int)                      as season_id
    from {{ ref('dim_matches') }}

),

line_breaking as (

    select * from {{ ref('stg_line_breaking__results') }}

),

running_score as (

    select * from {{ ref('int_running_score') }}

),

passes_with_score as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'unified_passes.match_key',
            'unified_passes.event_id',
            'unified_passes.data_source',
        ]) }}                                           as pass_id,

        unified_passes.match_key,
        unified_passes.player_id,
        unified_passes.team_id,
        unified_passes.pass_recipient_id,

        -- PR 7 (ADR-011): Kimball surrogate FKs resolved via dim_teams /
        -- dim_players JOINs on (provider, native_id). Coexists with legacy
        -- INT cols during the 2026-07-22 dual-column window; PR 8 drops the
        -- legacy INT cols.
        dt.team_key,
        dp_passer.player_key                            as passer_player_key,
        dp_recipient.player_key                         as recipient_player_key,

        match_attrs.competition_key,
        match_attrs.competition_id,
        match_attrs.season_id,

        unified_passes.period,
        unified_passes.minute,
        unified_passes.second,

        unified_passes.start_x,
        unified_passes.start_y,
        unified_passes.end_x,
        unified_passes.end_y,

        unified_passes.pass_type,
        unified_passes.pass_height,
        unified_passes.body_part,
        unified_passes.pass_length,
        unified_passes.pass_angle_radians,
        unified_passes.pass_outcome,
        unified_passes.is_cross,
        unified_passes.is_switch,
        unified_passes.is_through_ball,

        case
            when unified_passes.pass_outcome = 'Complete'
                 or unified_passes.pass_outcome is null
            then true
            else false
        end                                             as is_complete,

        unified_passes.is_progressive,

        case
            when unified_passes.end_x is null or unified_passes.start_x is null then null
            when unified_passes.end_x > unified_passes.start_x + {{ var('pass_direction_threshold') }} then 'forward'
            when unified_passes.end_x < unified_passes.start_x - {{ var('pass_direction_threshold') }} then 'backward'
            else 'lateral'
        end                                             as pass_direction,

        coalesce(lb.is_line_breaking, false)            as is_line_breaking,
        coalesce(lb.lines_broken, 0)                    as lines_broken,
        lb.line_breaking_type,

        unified_passes.data_source,

        rs.home_score_after,
        rs.away_score_after,
        rs.home_team_id as _rs_home_team_id,

        row_number() over (
            partition by unified_passes.match_key,
                         unified_passes.event_id,
                         unified_passes.data_source
            order by rs.period desc, rs.minute desc, rs.second desc
        ) as _score_rn

    from unified_passes
    left join match_attrs
        on unified_passes.match_key = match_attrs.match_key
    left join line_breaking lb
        on unified_passes.event_id = lb.event_id
    left join running_score rs
        on unified_passes.match_key = rs.match_key
        and (
            rs.period < unified_passes.period
            or (rs.period = unified_passes.period
                and (rs.minute * 60 + rs.second)
                    <= (unified_passes.minute * 60 + unified_passes.second))
        )
    -- PR 7 (ADR-011): dim_teams / dim_players JOINs by (provider, native_id).
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = unified_passes.data_source
       and dt.native_team_id = unified_passes.native_team_id
    left join {{ ref('dim_players') }} dp_passer
        on  dp_passer.provider = unified_passes.data_source
       and dp_passer.native_player_id = unified_passes.native_player_id
    left join {{ ref('dim_players') }} dp_recipient
        on  dp_recipient.provider = unified_passes.data_source
       and dp_recipient.native_player_id = unified_passes.native_recipient_id

),

final as (

    select
        pass_id,
        match_key,
        player_id,
        team_id,
        pass_recipient_id,
        team_key,
        passer_player_key,
        recipient_player_key,
        competition_key,
        competition_id,
        season_id,
        period,
        minute,
        second,
        start_x,
        start_y,
        end_x,
        end_y,
        pass_type,
        pass_height,
        body_part,
        pass_length,
        pass_angle_radians,
        pass_outcome,
        is_cross,
        is_switch,
        is_through_ball,
        is_complete,
        is_progressive,
        pass_direction,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        case
            when coalesce(home_score_after, 0) = coalesce(away_score_after, 0)
                then 'drawing'
            when (team_id = _rs_home_team_id
                      and home_score_after > away_score_after)
                 or (team_id != _rs_home_team_id
                      and away_score_after > home_score_after)
                then 'winning'
            else 'losing'
        end as game_state,
        data_source
    from passes_with_score
    where _score_rn = 1

)

select * from final
