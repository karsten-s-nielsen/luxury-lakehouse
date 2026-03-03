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
        frame_rate,
        'home'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y,
        ball_x                                          as raw_ball_x,
        ball_y                                          as raw_ball_y
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
        frame_rate,
        'away'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y,
        ball_x                                          as raw_ball_x,
        ball_y                                          as raw_ball_y
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

        -- Player identity
        player_id,
        team,

        -- Source provider
        'metrica'                                       as source_provider,

        -- Scaled player coordinates (120x80)
        raw_x * 120.0                                   as x,
        (1.0 - raw_y) * 80.0                            as y,

        -- Ball coordinates broadcast from frame-level bronze columns
        raw_ball_x * 120.0                              as ball_x,
        (1.0 - raw_ball_y) * 80.0                       as ball_y

    from all_players
    where raw_x is not null
      and raw_y is not null

)

select * from normalized
