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
            -- D62 2026-04-15: data_source is part of the dedup partition so
            -- v2 rows (128d, data_source='statsbomb'/'wyscout') and 360 rows
            -- (144d, data_source='football2vec_360') coexist for the same
            -- (player, match) pair. Previously the dedup collapsed them,
            -- silently losing one side of the collision.
            partition by canonical_player_id, match_id, data_source
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
