-- assert_coordinates_in_bounds.sql
-- Custom data test: Ensure all event coordinates are within pitch bounds.
--
-- StatsBomb coordinate system:
--   x: 0 to 120 (pitch length in yards)
--   y: 0 to 80  (pitch width in yards)
--
-- Coordinates outside these bounds indicate:
--   - Incorrect coordinate scaling (e.g. Wyscout 0-100 not properly converted)
--   - Metrica 0-1 normalization not applied
--   - Corrupted source data
--
-- This test checks shot locations in fct_shots. Similar tests should
-- exist for fct_passes and tracking data.
--
-- This test passes if the query returns ZERO rows.

select
    shot_id,
    match_id,
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
