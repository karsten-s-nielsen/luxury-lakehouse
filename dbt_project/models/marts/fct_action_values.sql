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
-- PR-LL2 Path B (2026-04-29): β-consistent rename of 4 StatsBomb-native
-- mart aliases (possession_id / possession_team_id / play_pattern /
-- under_pressure) to bronze names with `statsbomb_` prefix. Introduces a
-- new canonical `possession_id` sourced from `av.possession_id_heuristic`
-- (silly-kicks add_possessions, populated for ALL sources). Adds 6 LL2
-- enrichment columns + action_id + 5 LL2 Path B native string identifiers.
-- For IDSSE / Metrica, dim_matches / dim_teams joins use `match_id_native`
-- and `team_id_native` (since legacy BIGINT IDs are NULL on those rows).
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

        -- Legacy BIGINT IDs for 90-day dual-column window (sunset 2026-07-22).
        -- For IDSSE / Metrica these are NULL except `match_id` which carries
        -- a deterministic SHA-256 hash of the bronze string match_id (so
        -- VAEP groupBy(match_id) works). Joins to dim_* use `*_native`.
        av.match_id,
        av.competition_id,

        av.player_id,
        av.team_id,
        -- PR 7 (ADR-011): Kimball surrogate FKs via dim_teams / dim_players
        -- JOINs on (provider, native_id). LL2 Path B updates the join key
        -- from cast(team_id as string) to team_id_native — works for ALL 4
        -- sources (SB/WS = stringified int, IDSSE = DFL-CLU-XXXXXX, Metrica
        -- = synthetic). Same change for dim_players (player_id_native NOT
        -- yet on spadl_actions — IDSSE/Metrica player_key resolves NULL).
        dt.team_key,
        dp.player_key,
        av.season_id,
        av.period,
        av.time_seconds,
        av.minute,
        av.second,
        -- LL2: per-match action sequence number (was 100% NULL pre-LL2).
        av.action_id,

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

        -- LL2 Path B: canonical possession_id sourced from silly-kicks's
        -- heuristic add_possessions output — populated for ALL sources.
        -- Replaces the previous LL1 alias of `statsbomb_possession_id`.
        av.possession_id_heuristic                  as possession_id,

        -- Provider-namespaced StatsBomb-native passthroughs (β-consistent
        -- LL2 rename: previously aliased to plain canonical names; now
        -- exposed under their bronze names — see ADR-016 for the naming
        -- rule). NULL for Wyscout / IDSSE / Metrica.
        av.statsbomb_possession_id,
        av.statsbomb_possession_team_id,
        av.statsbomb_play_pattern,
        av.statsbomb_under_pressure,
        -- Kimball surrogate FK for the possession team — same dim_teams
        -- resolution pattern as `team_key` / `player_key` above. NULL when
        -- statsbomb_possession_team_id is NULL (non-StatsBomb sources).
        dt_poss.team_key                            as possession_team_key,

        -- LL2 enrichment columns (provider-agnostic, populated for ALL
        -- sources from apply_spadl_enrichments — see ADR-016).
        av.gk_role,
        av.gk_was_distributing,
        av.gk_was_engaged,
        av.gk_actions_in_possession,
        av.defending_gk_player_id,

        -- LL2 Path B: native string identifiers (Kimball-aligned).
        av.team_id_native,
        av.home_team_id_native,
        av.competition_native_id,
        av.season_native_id,
        av.match_id_native,

        -- PR-Cycle-A.4 (2026-04-30, ADR-018 alignment): silly-kicks 2.5.0
        -- sportec tackle qualifier passthrough. ``<col>_native`` STRING +
        -- ``<col>_key`` BIGINT surrogate per LL2 Path B convention. NULL
        -- on non-sportec rows and on sportec rows where the DFL XML
        -- qualifier was absent.
        av.tackle_winner_player_id_native,
        av.tackle_winner_player_key,
        av.tackle_winner_team_id_native,
        av.tackle_winner_team_key,
        av.tackle_loser_player_id_native,
        av.tackle_loser_player_key,
        av.tackle_loser_team_id_native,
        av.tackle_loser_team_key,

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
    -- LL2 Path B: dim_matches join on (provider, native_match_id) — works
    -- across all 4 sources because match_id_native carries the bronze
    -- string identifier (SB/WS = stringified int, IDSSE = 'idsse_J03WMX',
    -- Metrica = 'Sample_Game_N'). Replaces the LL1 cast(match_id as string)
    -- which broke for IDSSE/Metrica (BIGINT match_id is hashed, not native).
    left join match_attrs ma
        on av.match_id_native = ma.native_match_id
        and av.data_source = ma.provider
    left join running_score rs
        on rs.match_id = av.match_id
        and (
            rs.period < av.period
            or (rs.period = av.period
                and (rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second))
        )
    -- LL2 Path B: dim_teams / dim_players JOINs use the LL2 native cols
    -- (always populated) instead of cast(legacy_id as string) which is NULL
    -- for IDSSE/Metrica.
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = av.data_source
       and dt.native_team_id = av.team_id_native
    -- player_id_native is NOT yet on spadl_actions (PR-LL3); for IDSSE/
    -- Metrica this JOIN resolves NULL until then.
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
        possession_team_key,
        season_id,
        period,
        time_seconds,
        minute,
        second,
        action_id,
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
        -- LL2 Path B: canonical possession_id (heuristic, populated for ALL sources).
        possession_id,
        -- β-consistent: provider-namespaced StatsBomb-native passthroughs.
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure,
        -- LL2 enrichment columns.
        gk_role,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        defending_gk_player_id,
        -- LL2 Path B: native string identifiers.
        team_id_native,
        home_team_id_native,
        competition_native_id,
        season_native_id,
        match_id_native,
        -- PR-Cycle-A.4 (2026-04-30, ADR-018 alignment): silly-kicks 2.5.0
        -- sportec tackle qualifier passthrough — _native STRING + _key BIGINT.
        tackle_winner_player_id_native,
        tackle_winner_player_key,
        tackle_winner_team_id_native,
        tackle_winner_team_key,
        tackle_loser_player_id_native,
        tackle_loser_player_key,
        tackle_loser_team_id_native,
        tackle_loser_team_key,
        -- Game state — uses _rs_home_team_id (BIGINT from running_score) which
        -- only resolves for SB/WS (numeric team_id). For IDSSE/Metrica where
        -- team_id is NULL, comparison `team_id = _rs_home_team_id` is NULL,
        -- so game_state falls into the 'drawing' default if scores tied else
        -- the 'losing' branch. PR-LL3 should switch this to team_id_native
        -- comparison via running_score's home_team_id_native (does not yet
        -- exist on int_running_score). For now, IDSSE/Metrica game_state is
        -- best-effort (mostly correct on tied/no-goals matches; biased away
        -- from 'winning' on goals matches). Document in ADR-016.
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
