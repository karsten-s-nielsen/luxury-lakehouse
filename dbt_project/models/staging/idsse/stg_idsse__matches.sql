-- stg_idsse__matches.sql
-- One row per IDSSE Bundesliga match. 7 matches total (static collection).
--
-- Sources:
--   - bronze.idsse_tracking (distinct match_id for match presence)
--   - stg_tracking__player_metadata (home/away team display names)
--   - Hardcoded DFL competition mappings (see src/ingestion/idsse.py)
--
-- Strips the 'idsse_' prefix from bronze match_id to yield the native
-- DFL MatchId (e.g., 'idsse_J03WMX' -> 'J03WMX'). The provider column
-- (constant 'idsse') disambiguates against other providers' native IDs
-- via the surrogate key in dim_matches.

with tracking_matches as (

    select distinct
        match_id as prefixed_match_id
    from {{ source('idsse', 'idsse_tracking') }}

),

idsse_competitions as (

    -- DFL competition mapping from src/ingestion/idsse.py._MATCH_COMPETITION.
    -- 5 matches in DFL-COM-000002, 2 in DFL-COM-000001.
    -- Keep in sync with the Python source until a proper DFL metadata
    -- bronze table exists.
    select * from (
        values
            ('idsse_J03WMX', 'DFL-COM-000001'),
            ('idsse_J03WN1', 'DFL-COM-000001'),
            ('idsse_J03WPY', 'DFL-COM-000002'),
            ('idsse_J03WOH', 'DFL-COM-000002'),
            ('idsse_J03WQQ', 'DFL-COM-000002'),
            ('idsse_J03WOY', 'DFL-COM-000002'),
            ('idsse_J03WR9', 'DFL-COM-000002')
    ) as t(prefixed_match_id, competition_id)

),

team_names as (

    select
        match_id,
        max(case when team_side = 'home' then team_display_name end) as home_team_name,
        max(case when team_side = 'away' then team_display_name end) as away_team_name
    from {{ ref('stg_tracking__player_metadata') }}
    where provider = 'idsse'
    group by match_id

),

final as (

    select
        -- Native DFL MatchId with the 'idsse_' prefix stripped
        regexp_replace(tm.prefixed_match_id, '^idsse_', '') as native_match_id,
        'idsse'                                              as provider,
        ic.competition_id,
        tn.home_team_name,
        tn.away_team_name,
        tm.prefixed_match_id                                 as bronze_match_id

    from tracking_matches tm
    left join idsse_competitions ic
        on tm.prefixed_match_id = ic.prefixed_match_id
    left join team_names tn
        on tm.prefixed_match_id = tn.match_id

)

select * from final
