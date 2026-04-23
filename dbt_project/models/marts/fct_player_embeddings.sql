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
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge'
) }}

-- D62 (2026-04-15) introduced a 360-embeddings variant alongside the
-- original Doc2Vec (v2) embeddings, so stg_player_embeddings now partitions
-- dedup by (canonical_player_id, match_id, data_source) — both can coexist
-- for the same (player, match). This mart's surrogate must include
-- data_source too; otherwise two source rows collapse to one embedding_id
-- and the incremental MERGE aborts with DELTA_MULTIPLE_SOURCE_ROW_MATCHING.
select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'match_id', 'data_source']) }} as embedding_id,
    canonical_player_id,
    match_id,
    data_source,
    behavioral_vector,
    stat_vector
from {{ ref('stg_player_embeddings') }}
