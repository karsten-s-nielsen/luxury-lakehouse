-- assert_xg_between_0_and_1.sql
-- Custom data test: Ensure all xG values fall within the valid [0, 1] range.
--
-- Expected goals (xG) is a probability, so it must be between 0 and 1.
-- Values outside this range indicate a data quality issue upstream
-- (e.g. incorrect JSON parsing, wrong field extraction, or a regression
-- in the StatsBomb data format).
--
-- This test passes if the query returns ZERO rows.
-- Any rows returned represent violations.

select
    shot_id,
    match_id,
    player_id,
    statsbomb_xg,
    data_source

from {{ ref('fct_shots') }}

where
    statsbomb_xg is not null
    and (
        statsbomb_xg < 0.0
        or statsbomb_xg > 1.0
    )
