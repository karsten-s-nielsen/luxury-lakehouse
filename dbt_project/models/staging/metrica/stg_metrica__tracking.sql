-- stg_metrica__tracking.sql
-- Parse and normalize Metrica Sports 25fps tracking data.
--
-- Key transformations:
--   1. Explode per-frame JSON into one row per player per frame
--   2. Scale coordinates from [0, 1] normalized to 120x80 pitch system
--   3. Separate home and away player data into a uniform schema
--   4. Ball coordinates are void (NULL) in bronze — not available per-frame
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
        'home'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y
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
        'away'                                          as team,
        player_key                                      as player_id,
        player_value.x                                  as raw_x,
        player_value.y                                  as raw_y
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

        -- Player identity
        player_id,
        team,

        -- Scaled player coordinates (120x80)
        raw_x * 120.0                                   as x,
        (1.0 - raw_y) * 80.0                            as y,

        -- Ball coordinates not available per-frame in bronze (void type)
        cast(null as double)                            as ball_x,
        cast(null as double)                            as ball_y

    from all_players
    where raw_x is not null
      and raw_y is not null

)

select * from normalized
