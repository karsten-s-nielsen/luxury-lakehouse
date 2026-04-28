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

-- Wyscout → StatsBomb competition ID mapping so all SPADL actions use
-- a single competition ID space (StatsBomb IDs).
comp_mapping as (

    select
        wyscout_competition_id,
        statsbomb_competition_id
    from {{ ref('competition_id_mapping') }}

),

cleaned as (

    select
        -- Identifiers
        game_id                                         as match_id,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        original_event_id,

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

        -- SPADL coordinates (105x68 meters)
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
        cast(coalesce(cm.statsbomb_competition_id,
                 cast(competition_id as int)) as int)    as competition_id,
        cast(season_id as int)                          as season_id

    from deduplicated d
    left join comp_mapping cm
        on cast(d.competition_id as int) = cm.wyscout_competition_id
    where d._row_num = 1
      -- PR 7 hotfix #3 followup: Wyscout open-data uses `playerId: 0` as an
      -- "unknown player" sentinel (16,133 of 2,465,557 = 0.65% action rows).
      -- Same pattern as int_unified_passes (PR #215) and int_unified_shots
      -- (PR #217). Drop here at the staging boundary so dim_players LEFT JOIN
      -- in fct_action_values resolves 100% on every Wyscout row.
      and not (d.data_source = 'wyscout' and (d.player_id is null or cast(d.player_id as int) = 0))

)

select * from cleaned
