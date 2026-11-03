{{ config(materialized='table') }}
-- One row per competition-season of a provider (spec §14.4: the grain of a tournament), from the match records.
-- Seasons are not resolved across providers in cycle 1, so the canonical pointer is the season's own key.
select
    season_key,
    provider,
    native_season_id,
    season_key as canonical_season_key,
    min(competition_key) as competition_key,
    min(season_name) as season_name
from {{ source('ops', 'ops_entity_records') }}
where entity = 'match' and season_key is not null
group by season_key, provider, native_season_id
