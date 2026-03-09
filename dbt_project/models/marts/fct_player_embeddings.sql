-- fct_player_embeddings.sql
-- Per-match player embedding vectors for similarity search.
--
-- Dual-vector design:
--   - behavioral_vector (32-dim): football2vec Doc2Vec embedding capturing
--     playing style from event sequences (action type + pitch grid location tokens)
--   - stat_vector (13-dim): z-score normalized per-90 statistics capturing
--     output metrics
--
-- Grain: one row per player per match.
-- Downstream: fct_player_embeddings_season and _career aggregate via
-- element-wise mean.

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'match_id']) }} as embedding_id,
    canonical_player_id,
    match_id,
    data_source,
    behavioral_vector,
    stat_vector
from {{ ref('stg_player_embeddings') }}
