-- stg_wyscout__matches.sql
-- Wyscout match metadata. Source-faithful — competition_id is the RAW
-- wyscout-native ID (e.g., 795 for La Liga, NOT mapped to statsbomb's 11).
--
-- PR-LL2 Path B close-out (2026-04-29, ADR-018): the staging-time
-- coalesce(statsbomb_competition_id, raw) mapping was dropped. Reasons:
--
-- 1. ADR-016 conformance — `<entity>_native` and the legacy `competition_id`
--    column should both be PROVIDER-NATIVE (source-faithful). The previous
--    mapping made the legacy column statsbomb-native and silently broke
--    consistency with bronze.spadl_actions.competition_native_id which was
--    raw wyscout (after PR-LL2 added the `_native` columns).
-- 2. Cross-table format-contract (ADR-018) — bronze + dim must agree on the
--    same value for `(provider, native_competition_id)` JOINs to resolve.
--    Both sides now use the same RAW wyscout value.
-- 3. Single-source semantics — statsbomb / idsse / metrica all keep raw
--    native IDs at staging; wyscout was the only outlier with a baked-in
--    cross-provider transform.
--
-- The `competition_id_mapping` seed is RETAINED but no longer applied at
-- staging — it remains as documentation of cross-provider competition
-- equivalence (e.g., for query-layer joins or a future
-- `dim_competition_xref` that maps semantically-equivalent competitions
-- across providers). Existing fct_action_values.competition_id values
-- shift from statsbomb-mapped to wyscout-raw on next mart rebuild;
-- hardcoded query filters that relied on mapped values must update.

with source as (

    select * from {{ source('wyscout', 'wyscout_matches') }}

),

final as (

    select
        cast(s.wyId as bigint)                           as match_id,
        cast(s.competitionId as int)                      as competition_id,
        cast(s.seasonId as int)                          as season_id,
        s.competition_name,
        s.dateutc                                        as match_date,

        -- PR 1.5 expansion — surface previously-dropped match metadata.
        s.status                                         as match_status,
        cast(s.roundId as bigint)                        as round_id,
        cast(s.gameweek as int)                          as gameweek,
        cast(s.winner as bigint)                         as winner_team_id,
        s.venue,
        s.label                                          as match_label,
        s.date                                           as match_date_local,
        s.duration                                       as match_duration,
        s.groupName                                      as group_name,
        s.referees                                       as referees_json,

        -- teamsData: Wyscout's nested per-team struct (keyed by team_id).
        -- Parsed as MAP<STRING, STRUCT<...>>. Downstream consumers can
        -- `LATERAL VIEW explode` this or access specific team_ids.
        -- Structure: {team_id: {side, score, formation: {lineup, bench}, ...}}
        from_json(
            s.teamsData,
            'MAP<STRING, STRUCT<side: STRING, teamId: BIGINT, coachId: BIGINT, score: BIGINT, scoreET: BIGINT, scoreP: BIGINT, hasFormation: STRING, formation: STRUCT<lineup: ARRAY<STRUCT<playerId: BIGINT, assists: STRING, goals: STRING, ownGoals: STRING, redCards: STRING, yellowCards: STRING>>, bench: ARRAY<STRUCT<playerId: BIGINT, assists: STRING, goals: STRING, ownGoals: STRING, redCards: STRING, yellowCards: STRING>>, substitutions: ARRAY<STRUCT<playerIn: BIGINT, playerOut: BIGINT, minute: BIGINT>>>>>'
        )                                                as teams_data_parsed,
        s.teamsData                                      as teams_data_json,

        'wyscout'                                        as data_source

    from source s

)

select * from final
