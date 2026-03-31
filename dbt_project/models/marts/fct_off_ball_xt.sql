{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='off_ball_xt_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
-- fct_off_ball_xt.sql
-- Gold-layer off-ball expected threat (xT) results per player per match.
--
-- Each row contains aggregated off-ball xT metrics for a single player
-- in a single match: total accumulated xT from off-ball movement, average
-- xT per sampled frame, and the number of frames sampled.
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (player_id, match_id).

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

off_ball_xt as (

    select * from {{ ref('stg_off_ball_xt__results') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'off_ball_xt.player_id',
            'off_ball_xt.match_id'
        ]) }}                                       as off_ball_xt_id,

        off_ball_xt.player_id,
        off_ball_xt.match_id,
        off_ball_xt.total_off_ball_xt,
        off_ball_xt.avg_off_ball_xt,
        off_ball_xt.frames_sampled

    from off_ball_xt

)

select * from final
