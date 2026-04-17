{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_heatmap_agg.sql
-- Pre-aggregated heatmap grid counts for Taipy Heat Map page.
--
-- Motivation (2026-04-16 optimization audit):
-- The live heat-map query aggregates raw passes + shots into a 12x8 coordinate
-- grid at request time.  With fct_passes at 5.05M rows, a comp-only query
-- (WHERE competition_id = X, no team filter) runs as Parallel Seq Scan +
-- external sort (12 MB/worker spill) for ~6,864 ms measured against live
-- Lakebase for comp=11.  Pre-aggregating to (competition_id, team_id,
-- action_type, x_bin, y_bin) grain yields ~60-100K rows total — small enough
-- to serve comp-only queries by summing across teams at the client (<10ms),
-- and comp+team queries by direct key lookup (<5ms).
--
-- Grain: (competition_id, team_id, action_type, x_bin, y_bin)
--
-- Coordinate binning: round(x/10)*10 + 5 — StatsBomb 120x80 pitch, bin centres
-- at (5, 15, 25, ... 115) x (5, 15, 25, ... 75).  Identical formula to the
-- live query in queries/tracking.py::fetch_heatmap_actions so comparing
-- counts is 1:1.
--
-- For comp+player or match-level filters, the app falls through to the
-- original fct_passes/fct_shots query paths (those already hit the
-- idx_passes_comp_player / idx_passes_comp_team_match composites and are
-- fast at <100 ms).

with pass_events as (

    select
        competition_id,
        team_id,
        cast(round(start_x / 10) * 10 + 5 as int)    as x_bin,
        cast(round(start_y / 10) * 10 + 5 as int)    as y_bin,
        'pass'                                       as action_type
    from {{ ref('fct_passes') }}
    where start_x is not null
      and start_y is not null
      and competition_id is not null
      and team_id is not null

),

shot_events as (

    select
        competition_id,
        team_id,
        cast(round(location_x / 10) * 10 + 5 as int) as x_bin,
        cast(round(location_y / 10) * 10 + 5 as int) as y_bin,
        'shot'                                       as action_type
    from {{ ref('fct_shots') }}
    where location_x is not null
      and location_y is not null
      and competition_id is not null
      and team_id is not null

),

unioned as (

    select * from pass_events
    union all
    select * from shot_events

),

aggregated as (

    select
        competition_id,
        team_id,
        action_type,
        x_bin,
        y_bin,
        count(*)                                     as event_count
    from unioned
    group by competition_id, team_id, action_type, x_bin, y_bin

),

final as (

    select
        cast(competition_id as int)                  as competition_id,
        cast(team_id as int)                         as team_id,
        cast(action_type as string)                  as action_type,
        cast(x_bin as int)                           as x_bin,
        cast(y_bin as int)                           as y_bin,
        cast(event_count as bigint)                  as event_count,
        current_timestamp()                          as _loaded_at

    from aggregated

)

select * from final
