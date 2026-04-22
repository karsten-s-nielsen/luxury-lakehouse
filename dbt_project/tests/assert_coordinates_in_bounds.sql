-- assert_coordinates_in_bounds.sql
-- Custom data test: Ensure all event coordinates are within pitch bounds.
--
-- StatsBomb coordinate system:
--   x: 0 to 120 (pitch length in yards)
--   y: 0 to 80  (pitch width in yards)
--
-- NOTE: A small number of events may exceed bounds due to off-pitch actions
-- (throw-ins, goalkeeper events). This test uses warn severity.
--
-- This test passes if the query returns ZERO rows.

{{ config(severity='warn') }}

select
    shot_id,
    match_key,
    player_id,
    location_x,
    location_y,
    data_source

from {{ ref('fct_shots') }}

where
    location_x is not null
    and location_y is not null
    and (
        location_x < 0.0
        or location_x > 120.0
        or location_y < 0.0
        or location_y > 80.0
    )
