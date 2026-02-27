-- fct_player_embeddings.sql
-- Player movement pattern embeddings for similarity search.
--
-- This table stores dense vector representations of player behavior
-- derived from tracking data. These embeddings enable:
--   - Player similarity search (find players with similar movement patterns)
--   - Clustering (group players by playing style)
--   - Replacement analysis (find alternatives to a departing player)
--
-- Embedding generation pipeline:
--   1. Aggregate tracking data into per-match movement features:
--      - Average position (heatmap centroid)
--      - Distance covered per period
--      - Sprint frequency and duration
--      - Positional spread (convex hull area)
--      - Time in offensive/defensive/middle thirds
--   2. Feed features into an autoencoder or PCA (Python/MLflow)
--   3. Write resulting embedding vectors back to the warehouse
--   4. This model joins the embeddings with player metadata
--
-- For pgvector similarity queries:
--   SELECT p.player_name, e.embedding_vector <-> query_vector AS distance
--   FROM fct_player_embeddings e
--   JOIN dim_players p ON e.player_id = p.player_id
--   ORDER BY distance
--   LIMIT 10;
--
-- Note: The actual embedding vectors are generated externally by the
-- ML pipeline in src/models/ and written to a staging table. This
-- model joins them with dimensional context.

with tracking_features as (

    -- Aggregate per-match tracking features for each player
    -- These features form the input to the embedding model

    select
        player_id,
        match_id,

        -- Positional features
        avg(x)                                          as avg_x,
        avg(y)                                          as avg_y,
        stddev(x)                                       as stddev_x,
        stddev(y)                                       as stddev_y,
        min(x)                                          as min_x,
        max(x)                                          as max_x,
        min(y)                                          as min_y,
        max(y)                                          as max_y,

        -- Distance to ball features
        avg(distance_to_ball)                           as avg_distance_to_ball,

        -- Speed features
        avg(speed)                                      as avg_speed,
        max(speed)                                      as max_speed,
        -- Sprint count: frames where speed exceeds 7 m/s threshold
        sum(case when speed > 7.0 then 1 else 0 end)   as sprint_count,

        -- Third occupancy (percentage of frames in each pitch third)
        -- Defensive third: x < 40, Middle third: 40 <= x < 80, Attacking third: x >= 80
        avg(case when x < 40 then 1.0 else 0.0 end)    as pct_defensive_third,
        avg(case when x >= 40 and x < 80 then 1.0 else 0.0 end) as pct_middle_third,
        avg(case when x >= 80 then 1.0 else 0.0 end)   as pct_attacking_third,

        count(*)                                        as total_frames

    from {{ ref('fct_tracking_frames') }}
    where player_id is not null
    group by player_id, match_id

),

-- TODO: Join with externally generated embedding vectors
-- The ML pipeline writes embeddings to: soccer_analytics.bronze.player_embeddings_raw
-- with columns: player_id, match_id, embedding_vector (array<float>)

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['player_id', 'match_id']) }} as embedding_id,

        player_id,
        match_id,

        -- Raw tracking features (useful for interpretability)
        avg_x,
        avg_y,
        stddev_x,
        stddev_y,
        avg_speed,
        max_speed,
        sprint_count,
        pct_defensive_third,
        pct_middle_third,
        pct_attacking_third,
        avg_distance_to_ball,
        total_frames,

        -- Dense embedding vector (from external ML model)
        -- TODO: Join with embeddings table once ML pipeline is operational
        cast(null as array<double>)                     as embedding_vector

    from tracking_features

)

select * from final
