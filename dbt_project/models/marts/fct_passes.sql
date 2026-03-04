-- fct_passes.sql
-- Gold-layer pass fact table with progressive and line-breaking pass metrics.
--
-- Contains every pass from all data sources with:
--   - Pass success/failure classification
--   - Progressive pass identification (25% closer to goal)
--   - Line-breaking pass detection (Ward clustering + straddle test)
--   - Pass direction and distance metrics
--   - Pass type categorization
--
-- Progressive pass definition:
--   A pass moves the ball at least 25% closer to the opponent's goal center.
--   Formally: distance_to_goal(end) < 0.75 * distance_to_goal(start)
--   This metric (popularized by @Ssjocke) captures passes that advance play.
--
-- Line-breaking pass definition:
--   A pass whose trajectory intersects at least one opponent defensive line,
--   detected via Ward hierarchical clustering of opponent positions into 3
--   lines and a cross-product straddle test. Available for StatsBomb 360
--   matches and Metrica tracking matches only.
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

line_breaking as (

    select * from {{ ref('stg_line_breaking__results') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['unified_passes.event_id', 'unified_passes.data_source']) }} as pass_id,

        -- Foreign keys
        unified_passes.match_id,
        unified_passes.player_id,
        unified_passes.team_id,
        unified_passes.pass_recipient_id,

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

        -- Pass direction (categorical, 5-yard threshold)
        case
            when unified_passes.end_x is null or unified_passes.start_x is null then null
            when unified_passes.end_x > unified_passes.start_x + {{ var('pass_direction_threshold') }} then 'forward'
            when unified_passes.end_x < unified_passes.start_x - {{ var('pass_direction_threshold') }} then 'backward'
            else 'lateral'
        end                                             as pass_direction,

        -- Line-breaking pass detection
        coalesce(lb.is_line_breaking, false)             as is_line_breaking,
        coalesce(lb.lines_broken, 0)                     as lines_broken,
        lb.line_breaking_type,

        -- Data provenance
        unified_passes.data_source

    from unified_passes
    left join matches
        on unified_passes.match_id = matches.match_id
    left join line_breaking lb
        on unified_passes.event_id = lb.event_id

)

select * from final
