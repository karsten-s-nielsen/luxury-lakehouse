-- stg_spadl__action_values.sql
-- Clean and deduplicate VAEP action values from the bronze layer.
--
-- SPADL coordinate system:
--   - Pitch is 105 x 68 meters (academic standard)
--   - Origin (0,0) is bottom-left
--   - All actions normalized to attack left-to-right by silly-kicks
--
-- Time convention (2026-04-19): minute is MATCH-ABSOLUTE across all periods,
-- NOT period-local. SPADL's bronze `time_seconds` is period-local per the
-- academic convention (seconds since kickoff of the current period), so
-- we add a period offset before deriving minute:
--   period 1 → 0      (first half)
--   period 2 → +2700  (45 * 60, second half)
--   period 3 → +5400  (90 * 60, ET first half)
--   period 4 → +6300  (105 * 60, ET second half)
--   period 5 → +7200  (120 * 60, penalties placeholder)
-- This aligns fct_action_values.minute with fct_shots.minute (match-absolute)
-- and with bronze statsbomb_events.minute, eliminating the cross-mart
-- discrepancy that surfaced in the Match Summary redesign (Big Story card
-- at 8' while xG race chart at 53' for the same goal event).
-- `second` is unchanged — period offsets are all divisible by 60 so the
-- modulo-60 derivation is numerically stable.
--
-- Dedup: ROW_NUMBER partitioned by natural key, latest _ingested_at wins.

with source as (

    select * from {{ source('spadl', 'vaep_action_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by game_id, period_id, time_seconds, player_id, type_id, data_source
            order by _ingested_at desc
        ) as _row_num
    from source

),

-- PR-LL2 Path B close-out (2026-04-29, ADR-018): the wyscout→statsbomb
-- competition_id mapping that previously coalesced at this layer was
-- dropped. competition_id is now provider-native everywhere — wyscout
-- rows carry raw wyscout IDs (e.g., 795 for La Liga), statsbomb rows
-- carry statsbomb IDs (11 for La Liga). Cross-provider competition
-- equivalence is a query-layer concern; the `competition_id_mapping`
-- seed remains as documentation. See `stg_wyscout__matches.sql` header
-- for full rationale.

cleaned as (

    select
        -- Identifiers
        game_id                                         as match_id,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        original_event_id,
        action_id,

        -- Temporal — minute is match-absolute (see header comment)
        cast(period_id as int)                          as period,
        time_seconds,
        cast(
            floor(
                (
                    case cast(period_id as int)
                        when 1 then 0
                        when 2 then 2700
                        when 3 then 5400
                        when 4 then 6300
                        when 5 then 7200
                        else 0
                    end
                    + time_seconds
                ) / 60
            )
            as int
        )                                               as minute,
        cast(floor(time_seconds % 60) as int)           as second,

        -- SPADL coordinates (105x68 meters for StatsBomb/Wyscout/IDSSE;
        -- normalised [0, 1] for Metrica — Metrica adapter scales to SPADL frame
        -- before silly-kicks's converter, but bronze.metrica_events row coords
        -- arrive here already in SPADL frame because the spadl_actions writer
        -- emits post-silly-kicks output. Coordinate normalisation is a pre-
        -- conversion adapter concern; spadl_actions is always 105x68 SPADL.)
        start_x,
        start_y,
        end_x,
        end_y,

        -- Action classification
        cast(type_id as int)                            as type_id,
        action_type,
        cast(result_id as int)                          as result_id,
        action_result,
        cast(bodypart_id as int)                        as bodypart_id,
        bodypart,

        -- VAEP scores
        offensive_value,
        defensive_value,
        vaep_value,

        -- Provenance
        data_source,
        cast(competition_id as int)                      as competition_id,
        cast(season_id as int)                          as season_id,

        -- Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+
        -- preserve_native passthrough). NULL for non-StatsBomb sources.
        statsbomb_possession_id,
        statsbomb_possession_team_id,
        statsbomb_play_pattern,
        statsbomb_under_pressure,

        -- LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
        -- See ADR-016. Populated for ALL sources; deterministic from canonical SPADL.
        possession_id_heuristic,
        gk_role,
        gk_was_distributing,
        gk_was_engaged,
        gk_actions_in_possession,
        defending_gk_player_id,

        -- LL2 Path B: native string identifiers for Kimball-aligned joins to
        -- dim_teams / dim_competitions on (provider, native_id). For
        -- StatsBomb / Wyscout these are stringified ints; for IDSSE these are
        -- 'DFL-CLU-XXXXXX' / 'DFL-COM-XXXXXX' / 'DFL-SEA-XXXXXX'; for Metrica
        -- these are synthetic IDs from bronze.metrica_events. Always populated
        -- for all 4 sources post-LL2 deploy.
        team_id_native,
        home_team_id_native,
        competition_native_id,
        season_native_id,
        match_id_native,

        -- PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
        -- tackle qualifier passthrough. NULL on non-sportec rows + on
        -- sportec rows where the DFL ``tackle_winner`` qualifier was
        -- absent. Per ADR-016 + ADR-018 + silly-kicks ADR-001.
        tackle_winner_player_id,
        tackle_winner_team_id,
        tackle_loser_player_id,
        tackle_loser_team_id

    from deduplicated d
    where d._row_num = 1
      -- PR 7 hotfix #3 followup: Wyscout open-data uses `playerId: 0` as an
      -- "unknown player" sentinel (16,133 of 2,465,557 = 0.65% action rows).
      -- Same pattern as int_unified_passes (PR #215) and int_unified_shots
      -- (PR #217). Drop here at the staging boundary so dim_players LEFT JOIN
      -- in fct_action_values resolves 100% on every Wyscout row.
      and not (d.data_source = 'wyscout' and (d.player_id is null or cast(d.player_id as int) = 0))

)

select * from cleaned
