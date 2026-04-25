-- stg_metrica__matches.sql
-- One row per Metrica sample-data match. 3 matches total (static).
--
-- Metrica open-data is anonymized:
--   - No real team names (using generic 'Home' / 'Away')
--   - No competition_id, no season_id, no match_date
--   - Native match_id format: 'Sample_Game_{1,2,3}'
--
-- Sources: distinct match_id from bronze.metrica_tracking.

with tracking_matches as (

    select distinct match_id
    from {{ source('metrica', 'metrica_tracking') }}

),

final as (

    select
        match_id                 as native_match_id,
        'metrica'                as provider,
        -- PR 5a (ADR-011): pseudo-competition sentinel so dim_matches
        -- auto-resolves competition_key for Metrica passes. Matching
        -- Metrica pseudo-row in dim_competitions. Ref TODO #32.
        'metrica-sample'         as competition_id,
        'Home'                   as home_team_name,
        'Away'                   as away_team_name

    from tracking_matches

)

select * from final
