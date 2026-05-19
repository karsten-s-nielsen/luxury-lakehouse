-- assert_minutes_played_roster_count.sql
-- Per match, 22–36 players with minutes > 0.
-- Upper bound 36 accommodates expanded tournament squads (26-player rosters
-- with 5 subs per team = up to 36 unique players in FIFA World Cup data).
select
    match_key,
    data_source,
    count(*) as player_count
from {{ ref('int_minutes_played_per_match') }}
where minutes_played > 0
group by match_key, data_source
having count(*) < 22
    or count(*) > 36
