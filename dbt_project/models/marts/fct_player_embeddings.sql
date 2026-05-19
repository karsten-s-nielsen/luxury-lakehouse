-- fct_player_embeddings.sql
-- Per-match player embedding vectors for similarity search.
--
-- Dual-vector design:
--   - behavioral_vector (32-dim): football2vec Doc2Vec embedding capturing
--     playing style from event sequences (action type + pitch grid location tokens)
--   - stat_vector (13-dim): z-score normalized per-90 statistics capturing
--     output metrics
--
-- Grain: one row per player per match per data_source.
--
-- PR 5b (ADR-011) Kimball surrogate keys:
--   - player_key BIGINT FK -> dim_players.player_key. Resolved via LEFT JOIN
--     on canonical_player_id (the legacy hash preserved by dim_players for
--     Hyrum's Law). LEFT JOIN, not INNER, so zero-row Metrica + offline
--     embedding rows survive.
--   - match_key BIGINT FK -> dim_matches.match_key. LEFT JOIN on
--     (provider='statsbomb', try_cast(native_match_id as bigint) = match_id).
--     Retires the dim_matches bridge that PR 5a's CI-triage added to
--     fct_player_embeddings_season + _season_360.
--
-- Downstream: fct_player_embeddings_season and _career aggregate via
-- element-wise mean.

{{ config(
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['canonical_player_id', 'match_key'],
    tags=['marts', 'output_mart']
) }}

-- D62 (2026-04-15) introduced a 360-embeddings variant alongside the
-- original Doc2Vec (v2) embeddings, so stg_player_embeddings now partitions
-- dedup by (canonical_player_id, match_id, data_source) -- both can coexist
-- for the same (player, match). This mart's surrogate must include
-- data_source too; otherwise two source rows collapse to one embedding_id
-- and the incremental MERGE aborts with DELTA_MULTIPLE_SOURCE_ROW_MATCHING.
select
    {{ dbt_utils.generate_surrogate_key(['e.canonical_player_id', 'e.match_id', 'e.data_source']) }} as embedding_id,
    e.canonical_player_id,
    dp.player_key,
    e.match_id,
    dm.match_key,
    e.data_source,
    e.behavioral_vector,
    e.stat_vector
from {{ ref('stg_player_embeddings') }} e
left join {{ ref('dim_players') }} dp
    on dp.canonical_player_id = e.canonical_player_id
left join {{ ref('dim_matches') }} dm
    on dm.provider = 'statsbomb'
   and try_cast(dm.native_match_id as bigint) = e.match_id
