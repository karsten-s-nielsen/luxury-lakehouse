-- int_tracking_goalkeepers must cover ONLY the tracking providers that have real tracking frames
-- (idsse, metrica, skillcorner). After the PR-1 AC re-home, an omitted data_source filter would
-- admit gradientsports + statsbomb-360 rows (AC carries all 6 providers; TC-1 carried only these 3).
--
-- THE TRAP guard (PR-1 Task 3 / spec). int_tracking_goalkeepers exposes only (match_key, player_key),
-- so provider is resolved via dim_matches. This test was demonstrated RED (returns gradientsports /
-- statsbomb) on a filter-less draft and GREEN once the data_source filter was restored.
{{ config(severity='error') }}

select dm.provider
from {{ ref('int_tracking_goalkeepers') }} g
inner join {{ ref('dim_matches') }} dm
    on dm.match_key = g.match_key
where dm.provider not in ('idsse', 'metrica', 'skillcorner')
group by dm.provider
