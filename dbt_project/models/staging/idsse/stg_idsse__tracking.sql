-- stg_idsse__tracking.sql
-- Normalize IDSSE Bundesliga tracking data to the shared 120×80 coordinate system.
--
-- Coordinate system alignment:
--   IDSSE (DFL): center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34) on 105×68m
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = (x + 52.5) / 105.0 * 120.0
--              y_out = (y + 34.0) / 68.0 * 80.0

with source as (

    select * from {{ source('idsse', 'idsse_tracking') }}

),

normalized as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'frame', 'player_id']) }} as tracking_id,

        -- Match context
        -- PR 7 hotfix #3: strip the `idsse_` bronze prefix at staging boundary so
        -- downstream consumers (fct_tracking_frames, dim_matches JOINs) receive the
        -- canonical native form. Pre-fix: 100% of fct_tracking_frames IDSSE rows had
        -- match_key=NULL because match_id='idsse_J03WMX' couldn't match
        -- dim_matches.native_match_id='J03WMX'. Downstream regexp_replace strips in
        -- the deleted stg_idsse__home_away_teams (subsumed by
        -- int_tracking__match_side_team_bridge) and stg_idsse__passes.ball_at_end_frame
        -- become idempotent no-ops on already-clean strings — the regex matches
        -- nothing and returns input unchanged.
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,

        -- Frame identifiers
        cast(period as int)                             as period,
        cast(frame as int)                              as frame,
        timestamp                                       as timestamp_seconds,
        -- Bronze passthrough of the original `timestamp` col — kept alongside
        -- the renamed `timestamp_seconds` so the bronze-completeness coverage
        -- test sees every source col surfaced under its bronze name.
        timestamp                                       as timestamp,
        frame_rate,

        -- Player identity
        player_id,
        team,
        -- PR 5a: surface real DFL TeamId (e.g., 'DFL-CLU-XXXXXX') from bronze.
        -- Present since PR 1.8 (src/ingestion/idsse.py:580) but not previously
        -- consumed in staging. Feeds stg_idsse__home_away_teams bridge +
        -- dim_teams IDSSE CTE. Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2
        team_id,

        -- Source provider
        'idsse'                                         as source_provider,

        -- Goalkeeper flag (from DFL match info PlayingPosition='TW')
        cast(is_goalkeeper as boolean)                  as is_goalkeeper,

        -- Scaled player coordinates (120×80)
        {{ normalize_x('x', 'center_m') }} as x,
        {{ normalize_y('y', 'center_m') }} as y,

        -- Ball coordinates scaled to 120×80
        {{ normalize_x('ball_x', 'center_m') }} as ball_x,
        {{ normalize_y('ball_y', 'center_m') }} as ball_y,

        -- Bronze passthrough — PR 5a bronze-completeness sweep.
        -- These 13 DFL tracking attrs were present in bronze.idsse_tracking
        -- since PR 1.8 (src/ingestion/idsse.py) but not previously surfaced
        -- in staging or declared in _idsse__sources.yml. PR 5a closed the
        -- source-YAML gap; this block closes the staging-passthrough gap so
        -- the bronze-completeness principle (feedback_bronze_completeness_principle)
        -- holds across the idsse_tracking pipeline. Downstream consumers
        -- may opt in to these attrs without re-ingesting bronze.
        t                                               as t,
        s                                               as s,
        a                                               as a,
        d                                               as d,
        m                                               as m,
        ball_z                                          as ball_z,
        ball_s                                          as ball_s,
        ball_a                                          as ball_a,
        ball_d                                          as ball_d,
        ball_m                                          as ball_m,
        ball_t                                          as ball_t,
        ball_possession                                 as ball_possession,
        ball_status                                     as ball_status,

        -- Per-match metadata (session 69 — parity with bronze.idsse_events).
        -- Sourced from <General> in matchinformation XML by
        -- ingestion.idsse._parse_match_metadata. Constant per match.
        -- Single source of truth for stg_idsse__matches; downstream models
        -- can also opt in here without joining to stg_idsse__matches.
        competition_native_id                           as competition_native_id,
        season_native_id                                as season_native_id,
        home_team_id_native                             as home_team_id_native,
        away_team_id_native                             as away_team_id_native,

        -- Per-match HF redistribution tier (ADR-064), stamped on bronze at ingest. Surfaced
        -- here so the bronze contract is complete and the signal is visible to downstream
        -- models without re-reading bronze — the same gap that left it undocumented on
        -- Gradient Sports until PR-2a.
        access_tier                                     as access_tier,
        _ingested_at                                    as _ingested_at

    from source
    where x is not null
      and y is not null

)

select * from normalized
