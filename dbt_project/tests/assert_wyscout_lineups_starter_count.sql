-- assert_wyscout_lineups_starter_count.sql
-- Per match, exactly 22 starters (11 per team). Hard failure.

select
    match_id,
    count(*) as starter_count
from {{ ref('stg_wyscout__lineups') }}
where is_starter = true
group by match_id
having count(*) != 22
