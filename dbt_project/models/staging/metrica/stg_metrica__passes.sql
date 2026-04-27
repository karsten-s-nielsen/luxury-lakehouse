-- stg_metrica__passes.sql
-- Metrica sample-data pass events in SPADL-like shape, ready to union
-- into `int_unified_passes`.
--
-- Source: bronze.metrica_events WHERE type='PASS'. Bronze coordinates
-- are in Metrica's native normalised [0,1] space (y-axis flipped);
-- scale to the shared 120x80 via the `metrica` normalisation macro.
--
-- Metrica open-data is anonymised:
--   * team ∈ {'Home', 'Away'} (no real team identities)
--   * player values are string labels like 'Player 10' (no integer ID)
--   * no competition_id / season_id / match_date
--
-- Like IDSSE, fct_passes.{player_id, team_id, pass_recipient_id} are
-- NULL for Metrica — the contract types them as INT and Metrica
-- identifiers are strings. Raw identifiers are surfaced as
-- *_native cols for future cross-provider reconciliation.
--
-- Bronze-completeness: every bronze column that carries pass-relevant
-- information is surfaced as a pass-through column so downstream UI
-- reviews can discover valuable additions without re-ingesting.
--
-- Pass completion heuristic: Metrica encodes pass failure in the
-- subtype column ('HEAD-Loss', 'GOAL-Loss', 'INTERCEPTION', ...).
-- Plain PASS rows with subtype=NULL are treated as Complete. Richer
-- classification via `subtypes_all_json` is available as a bronze
-- passthrough for future UI consumers.

with source as (

    select * from {{ source('metrica', 'metrica_events') }}
    where type = 'PASS'

),

scaled as (

    select
        source.*,
        {{ normalize_x('start_x', 'metrica') }} as start_x_120,
        {{ normalize_y('start_y', 'metrica') }} as start_y_80,
        {{ normalize_x('end_x', 'metrica') }}   as end_x_120,
        {{ normalize_y('end_y', 'metrica') }}   as end_y_80
    from source

),

final as (

    select
        cast(event_id as string)                                as event_id,
        match_id                                                as match_id,
        cast(null as int)                                       as player_id,
        cast(null as int)                                       as team_id,
        cast(null as int)                                       as pass_recipient_id,

        -- Metrica identity strings, normalized to match dim_players' synth recipe.
        -- Bronze format varies between sample matches: 'Player19' (no space,
        -- Sample_Game_1 / 2) and 'Player 8' (with space, Sample_Game_3). The
        -- dim_players generator (stg_metrica__team_players) reads the raw
        -- Metrica tracking JSON map keys which are BARE numbers ('1', '11', ...)
        -- and synthesizes `metrica_<match>_<side>_<bare_number>`. For the
        -- INNER JOIN to dim_players to resolve in fct_passes, the native_id
        -- propagated up from this staging row must, after `concat('metrica_'
        -- || match_id || '_' || side || '_' || ...)` in int_unified_passes,
        -- equal the dim_players' synthesized form. So strip the 'Player' /
        -- 'Player ' prefix here at the staging boundary. Falls back to the
        -- raw value if the regex doesn't match (forward-compat for any
        -- subscription-data shape that emits real player IDs).
        regexp_replace(cast(player as string), '^Player[ ]?', '') as player_id_native,
        cast(team as string)                                    as team_side,
        regexp_replace(cast(`to` as string), '^Player[ ]?', '')   as pass_recipient_id_native,

        cast(period as int)                                     as period,
        cast(floor(start_time_s / 60.0) as int)                 as minute,
        cast(cast(start_time_s as int) % 60 as int)             as second,

        start_x_120                                             as start_x,
        start_y_80                                              as start_y,
        end_x_120                                               as end_x,
        end_y_80                                                as end_y,

        subtype                                                 as pass_type,
        cast(null as string)                                    as pass_height,
        cast(null as string)                                    as body_part,

        sqrt(
            power(end_x_120 - start_x_120, 2)
          + power(end_y_80  - start_y_80,  2)
        )                                                       as pass_length,

        atan2(end_y_80 - start_y_80, end_x_120 - start_x_120)   as pass_angle_radians,

        case
            when lower(coalesce(subtype, '')) like '%loss%'
                 or lower(coalesce(subtype, '')) like '%intercep%'
            then 'Incomplete'
            else 'Complete'
        end                                                     as pass_outcome,

        false                                                   as is_cross,
        false                                                   as is_switch,
        false                                                   as is_through_ball,

        -- is_progressive: end ≥25% closer to opponent goal than start.
        {{ distance_to_goal('end_x_120', 'end_y_80') }}
            < {{ var('progressive_pass_ratio') }} *
              {{ distance_to_goal('start_x_120', 'start_y_80') }}
                                                                as is_progressive,

        -- Bronze passthrough — Metrica pass-event attributes not used
        -- by int_unified_passes today, but surfaced for future UI
        -- reviews so we never re-ingest to "check if X was in bronze."
        type                                                    as event_type,
        subtype                                                 as subtype,
        subtypes_all_json,
        start_frame,
        end_frame,
        start_time_s,
        end_time_s,
        pitch_length_m,
        pitch_width_m,

        'metrica'                                               as data_source

    from scaled

)

select * from final
