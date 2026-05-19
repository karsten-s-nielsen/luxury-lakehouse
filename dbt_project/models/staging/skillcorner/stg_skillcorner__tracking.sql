-- stg_skillcorner__tracking.sql
-- Normalize SkillCorner broadcast tracking data to the shared 120×80 coordinate system.
--
-- Coordinate system alignment:
--   SkillCorner: center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34) on 105×68m
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = (x + 52.5) / 105.0 * 120.0
--              y_out = (y + 34.0) / 68.0 * 80.0
--
-- Team resolution: the new bronze schema (pining-for-the-data API) does not carry
-- team affiliation per tracking row. We JOIN to stg_skillcorner__matches (roster
-- format) to resolve player_id → team_id, home_team_id, away_team_id.
--
-- Note: SkillCorner data is 10fps (vs 25fps for Metrica/IDSSE). The frame_rate
-- column enables per-row velocity calculation in fct_tracking_frames.

with source as (

    select * from {{ source('skillcorner', 'skillcorner_tracking') }}

),

matches as (

    -- Roster-format: one row per (match_id, player_id) with team info.
    -- Deduplicate to avoid fan-out (should already be one-per-player-per-match).
    select distinct
        match_id,
        player_id,
        team_id,
        home_team_id,
        away_team_id,
        position_name
    from {{ ref('stg_skillcorner__matches') }}

),

joined as (

    select
        s.*,
        m.team_id,
        m.home_team_id,
        m.away_team_id,
        m.position_name
    from source s
    left join matches m
        on  s.match_id = m.match_id
       and s.player_id = m.player_id

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

        -- Player identity — cast to string for type consistency across
        -- the 3-provider UNION in fct_tracking_frames (IDSSE/Metrica already
        -- produce string; SkillCorner bronze carries bigint from the API).
        cast(player_id as string)                       as player_id,

        -- Team side derivation from match roster
        case
            when team_id = home_team_id then 'home'
            when team_id = away_team_id then 'away'
        end                                             as team,

        -- Team_id from match roster JOIN — cast to string (same reason).
        cast(team_id as string)                         as team_id,

        -- Source provider
        'skillcorner'                                   as source_provider,

        -- Goalkeeper flag (from match roster position_name)
        case
            when lower(position_name) = 'goalkeeper' then true
            else false
        end                                             as is_goalkeeper,

        -- Scaled player coordinates (120×80)
        {{ normalize_x('x', 'center_m') }} as x,
        {{ normalize_y('y', 'center_m') }} as y,

        -- Ball coordinates scaled to 120×80
        {{ normalize_x('ball_x', 'center_m') }} as ball_x,
        {{ normalize_y('ball_y', 'center_m') }} as ball_y,

        -- Bronze passthroughs
        home_team_id,
        away_team_id,
        ball_z,
        is_visible,
        ball_is_detected,
        _ingested_at

    from joined
    where x is not null
      and y is not null

)

select * from normalized
