{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='off_ball_xt_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_off_ball_xt.sql
-- Gold-layer off-ball expected threat (xT) results per player per match.
--
-- Each row contains aggregated off-ball xT metrics for a single player
-- in a single match: total accumulated xT from off-ball movement, average
-- xT per sampled frame, and the number of frames sampled.
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (player_id, match_id, source_provider).
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + player_key
-- via dim_matches/dim_players LEFT JOINs on the staging-derived source_provider.
-- Surrogate-key inputs gain source_provider for provider-stable IDs.

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
            'off_ball_xt.match_id',
            'off_ball_xt.source_provider'
        ]) }}                                       as off_ball_xt_id,

        off_ball_xt.player_id,
        dp.player_key,
        off_ball_xt.match_id,
        dm.match_key,
        off_ball_xt.total_off_ball_xt,
        off_ball_xt.avg_off_ball_xt,
        off_ball_xt.frames_sampled,
        off_ball_xt.source_provider                 as data_source

    from off_ball_xt
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = off_ball_xt.source_provider
       and dm.native_match_id = off_ball_xt.match_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = off_ball_xt.source_provider
       and dp.native_player_id = off_ball_xt.player_id

)

select * from final
