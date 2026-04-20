-- assert_fct_action_values_minute_match_absolute.sql
--
-- Guards the match-absolute minute convention on fct_action_values (set in
-- stg_spadl__action_values, 2026-04-19). If a regression reintroduces the
-- period-local convention, period-2 actions would have minute in 0..44,
-- period-3 actions in 0..14, etc. — this test catches that.
--
-- Thresholds: the *minimum* match-absolute minute for each period's first
-- second is period_offset / 60. Real kickoff-adjacent events (time_seconds
-- close to 0) legitimately land at the threshold, so the predicate is `<`,
-- not `<=`.
--
--   period 1 → minute >= 0
--   period 2 → minute >= 45
--   period 3 → minute >= 90
--   period 4 → minute >= 105
--   period 5 → minute >= 120 (penalties placeholder)
--
-- A test `select` that returns rows is a FAILURE in dbt's singular-test
-- convention. Zero rows = pass.

select
    match_id,
    period,
    minute,
    second,
    time_seconds,
    data_source
from {{ ref('fct_action_values') }}
where (period = 2 and minute < 45)
   or (period = 3 and minute < 90)
   or (period = 4 and minute < 105)
   or (period = 5 and minute < 120)
