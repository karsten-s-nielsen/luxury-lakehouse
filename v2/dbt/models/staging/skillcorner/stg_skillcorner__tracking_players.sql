-- The shared tracking core of skillcorner (spec §11.5), one row per player per frame. `team_key` is a provider extra
-- column (beyond the core) surfaced for the tracking computes (spec D7). skillcorner tracking players carry no team on
-- the frame rows (the trackable objects resolve to a player, whose team is in `skillcorner_players`), so team_key joins
-- that per-match roster on (match, player); the `roster` CTE selects only the key columns (no `_visibility`), so the
-- bare `_visibility` the macro emits stays unambiguous across the join.
{% if lake_table_present('skillcorner_tracking_players') %}
with roster as (
    {% if lake_table_present('skillcorner_players') %}
    select distinct match_key, player_key, team_key
    from {{ source('bronze_skillcorner', 'skillcorner_players') }}
    {% else %}
    select cast(null as bigint) as match_key, cast(null as bigint) as player_key, cast(null as bigint) as team_key
    where false
    {% endif %}
)
select
    cast(p.match_key as bigint) as match_key,
    cast(p.period as tinyint) as period,
    cast(p.frame as integer) as frame,
    cast(p.player_key as bigint) as player_key,
    cast(r.team_key as bigint) as team_key,
    cast(p.x_mm as integer) as x_mm,
    cast(p.y_mm as integer) as y_mm,
    cast(p.is_visible as boolean) as is_visible,
    {{ bronze_visibility_column('skillcorner_tracking_players') }}
from {{ source('bronze_skillcorner', 'skillcorner_tracking_players') }} as p
left join roster as r on r.match_key = p.match_key and r.player_key = p.player_key
{% else %}
select
    cast(null as bigint) as match_key,
    cast(null as tinyint) as period,
    cast(null as integer) as frame,
    cast(null as bigint) as player_key,
    cast(null as bigint) as team_key,
    cast(null as integer) as x_mm,
    cast(null as integer) as y_mm,
    cast(null as boolean) as is_visible,
    {{ bronze_visibility_column('skillcorner_tracking_players') }}
where false
{% endif %}
