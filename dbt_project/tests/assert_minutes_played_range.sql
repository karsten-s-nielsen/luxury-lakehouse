-- assert_minutes_played_range.sql
-- Minutes must be between 0 and 150.
-- Upper bound 150 accommodates extra time (30 min) + injury time in knockout
-- tournaments (StatsBomb World Cup data has values up to ~140 min).
select
    match_key,
    player_key,
    data_source,
    minutes_played
from {{ ref('int_minutes_played_per_match') }}
where minutes_played < 0
   or minutes_played > 150
