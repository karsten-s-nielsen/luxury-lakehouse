-- stg_skillcorner__tracking.sql
-- Normalize SkillCorner broadcast tracking data to the shared 120×80 coordinate system.
--
-- Coordinate system alignment:
--   SkillCorner: center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34) on 105×68m
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = (x + 52.5) / 105.0 * 120.0
--              y_out = (y + 34.0) / 68.0 * 80.0
--
-- Note: SkillCorner data is 10fps (vs 25fps for Metrica/IDSSE). The frame_rate
-- column enables per-row velocity calculation in fct_tracking_frames.

with source as (

    select * from {{ source('skillcorner', 'skillcorner_tracking') }}

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
        timestamp                                       as timestamp_seconds,
        frame_rate,

        -- Player identity
        player_id,
        team,

        -- Team_id derivation (PR 7, ADR-011): SkillCorner emits real team_ids
        -- via home_team_id / away_team_id bronze passthroughs. Project the
        -- correct one based on the team='home'/'away' role so downstream
        -- tracking marts can LEFT JOIN dim_teams cleanly.
        case
            when team = 'home' then home_team_id
            when team = 'away' then away_team_id
        end                                             as team_id,

        -- Source provider
        'skillcorner'                                   as source_provider,

        -- Goalkeeper flag (from kloppy Player.position)
        cast(is_goalkeeper as boolean)                  as is_goalkeeper,

        -- Scaled player coordinates (120×80)
        {{ normalize_x('x', 'center_m') }} as x,
        {{ normalize_y('y', 'center_m') }} as y,

        -- Ball coordinates scaled to 120×80
        {{ normalize_x('ball_x', 'center_m') }} as ball_x,
        {{ normalize_y('ball_y', 'center_m') }} as ball_y,

        -- Bronze passthroughs (PR 2 completeness sweep)
        home_team_id,
        away_team_id,
        ball_owning_team_id,
        ball_state,
        ball_z,
        is_visible,
        position_name

    from source
    where x is not null
      and y is not null

)

select * from normalized
