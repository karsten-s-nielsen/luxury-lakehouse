-- stg_player_embeddings.sql
-- Clean and deduplicate player embedding vectors from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (canonical_player_id, match_id),
-- latest _ingested_at wins.

{{ config(
    materialized='view',
    enabled=var('embeddings_enabled', false)
) }}

with source as (

    select
        canonical_player_id,
        match_id,
        data_source,
        behavioral_vector,
        stat_vector,
        _ingested_at,
        row_number() over (
            partition by canonical_player_id, match_id
            order by _ingested_at desc
        ) as _row_num
    from {{ source('embeddings', 'player_embeddings_raw') }}

)

select
    canonical_player_id,
    match_id,
    data_source,
    behavioral_vector,
    stat_vector
from source
where _row_num = 1
