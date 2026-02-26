-- fct_passes.sql
-- Gold-layer pass fact table with progressive pass metrics.
--
-- Contains every pass from all data sources with:
--   - Pass success/failure classification
--   - Progressive pass identification (25% closer to goal)
--   - Pass direction and distance metrics
--   - Pass type categorization
--
-- Progressive pass definition:
--   A pass moves the ball at least 25% closer to the opponent's goal center.
--   Formally: distance_to_goal(end) < 0.75 * distance_to_goal(start)
--   This metric (popularized by @Ssjocke) captures passes that advance play.
--
-- Downstream consumers:
--   - fct_player_stats (pass aggregations per player)
--   - Pass network analysis (graph construction)
--   - Dashboard visualizations (pass maps, progressive pass heatmaps)

with unified_passes as (

    select * from {{ ref('int_unified_passes') }}

),

matches as (

    select * from {{ ref('stg_statsbomb__matches') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['unified_passes.event_id', 'unified_passes.data_source']) }} as pass_id,

        -- Foreign keys
        unified_passes.match_id,
        unified_passes.player_id,
        unified_passes.team_id,

        -- Match context
        matches.competition_id,
        matches.season_id,

        -- Temporal context
        unified_passes.period,
        unified_passes.minute,
        unified_passes.second,

        -- Pass locations
        unified_passes.start_x,
        unified_passes.start_y,
        unified_passes.end_x,
        unified_passes.end_y,

        -- Pass attributes
        unified_passes.pass_type,
        unified_passes.pass_height,
        unified_passes.body_part,
        unified_passes.pass_length,
        unified_passes.pass_angle_radians,
        unified_passes.pass_outcome,
        unified_passes.is_cross,
        unified_passes.is_switch,
        unified_passes.is_through_ball,

        -- Derived: pass success
        case
            when unified_passes.pass_outcome = 'Complete'
                 or unified_passes.pass_outcome is null  -- StatsBomb: null = complete
            then true
            else false
        end                                             as is_complete,

        -- Progressive pass flag
        unified_passes.is_progressive,

        -- Pass direction (categorical)
        -- TODO: Calculate based on angle — forward/backward/lateral
        case
            when unified_passes.end_x > unified_passes.start_x + 5 then 'forward'
            when unified_passes.end_x < unified_passes.start_x - 5 then 'backward'
            else 'lateral'
        end                                             as pass_direction,

        -- Data provenance
        unified_passes.data_source

    from unified_passes
    left join matches
        on unified_passes.match_id = matches.match_id

)

select * from final
