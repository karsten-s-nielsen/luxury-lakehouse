-- stg_metrica__tracking.sql
-- Parse and normalize Metrica Sports 25fps tracking data.
--
-- Key transformations:
--   1. Explode per-frame JSON into one row per player per frame
--   2. Scale coordinates from [0, 1] normalized to 120x80 pitch system
--   3. Separate home and away player data into a uniform schema
--   4. Broadcast frame-level ball coordinates to each player row
--
-- Coordinate system alignment:
--   Metrica: (0,0) = top-left, (1,1) = bottom-right, normalized [0,1]
--   Target:  (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Scaling: x * 120, (1 - y) * 80 to flip vertical axis

with source as (

    select * from {{ source('metrica', 'metrica_tracking') }}

),

home_players_exploded as (

    select
        match_id,
        period,
        frame,
        timestamp                                       as timestamp_seconds,
        timestamp,
        frame_rate,
        'home'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y,
        ball_x                                          as raw_ball_x,
        ball_y                                          as raw_ball_y,
        gk_jersey_numbers,
        home_players,
        away_players,
        pitch_length_m,
        pitch_width_m,
        is_anonymized
    from source
    lateral view explode(
        from_json(home_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) as player_key, player_value

),

away_players_exploded as (

    select
        match_id,
        period,
        frame,
        timestamp                                       as timestamp_seconds,
        timestamp,
        frame_rate,
        'away'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y,
        ball_x                                          as raw_ball_x,
        ball_y                                          as raw_ball_y,
        gk_jersey_numbers,
        home_players,
        away_players,
        pitch_length_m,
        pitch_width_m,
        is_anonymized
    from source
    lateral view explode(
        from_json(away_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) as player_key, player_value

),

all_players as (

    select * from home_players_exploded
    union all
    select * from away_players_exploded

),

normalized as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'frame', 'player_id']) }} as tracking_id,

        -- Match context
        match_id,

        -- Frame identifiers
        cast(period as int)                             as period,
        cast(frame as int)                              as frame,
        timestamp_seconds,
        frame_rate,

        -- Bronze passthrough — raw timestamp (seconds from period start)
        timestamp,

        -- Player identity
        player_id,
        team,

        -- Team_id derivation (PR 7, ADR-011): synthesizes the team_id that
        -- matches dim_teams.native_team_id for Metrica anonymized teams.
        -- PR 5a created dim_teams Metrica rows with native_team_id =
        -- concat('metrica_', match_id, '_', team_role) where team_role is
        -- 'home'/'away'. Surface here so downstream tracking marts can
        -- LEFT JOIN dim_teams cleanly without re-deriving.
        concat('metrica_', match_id, '_', team)         as team_id,

        -- Source provider
        'metrica'                                       as source_provider,

        -- Goalkeeper flag (from gk_jersey_numbers JSON array, jersey #1 heuristic)
        array_contains(
            from_json(gk_jersey_numbers, 'ARRAY<STRING>'),
            player_id
        )                                               as is_goalkeeper,

        -- Bronze passthrough — raw goalkeeper jersey number JSON array
        gk_jersey_numbers,

        -- Bronze passthrough — raw per-frame player tracking JSON objects
        home_players,
        away_players,

        -- Bronze passthrough — pitch dimensions (meters) denormalized per row
        pitch_length_m,
        pitch_width_m,

        -- Bronze passthrough — PR 5a (ADR-011) sample-vs-subscription flag.
        -- True for anonymised Metrica sample CSV matches (current); False
        -- when future subscription-API ingestion lands with real identities.
        -- Downstream dim_teams / dim_players branch on this flag to select
        -- synthesised-identity vs real-identity paths. Ref:
        -- docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §4.
        is_anonymized,

        -- Scaled player coordinates (120x80)
        {{ normalize_x('raw_x', 'metrica') }} as x,
        {{ normalize_y('raw_y', 'metrica') }} as y,

        -- Ball coordinates broadcast from frame-level bronze columns
        {{ normalize_x('raw_ball_x', 'metrica') }} as ball_x,
        {{ normalize_y('raw_ball_y', 'metrica') }} as ball_y

    from all_players
    where raw_x is not null
      and raw_y is not null

)

select * from normalized
