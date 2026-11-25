-- The shared tracking core of gradientsports (spec §11.5), one row per player per frame. `team_key` is a provider
-- extra column (beyond the core) surfaced for the tracking computes (spec D7). gradientsports tracking carries only a
-- `team_side` ('home'/'away') per player, so team_key is resolved from the match's home/away team keys — the `matches`
-- CTE selects only the three key columns (no `_visibility`), so the bare `_visibility` the macro emits stays
-- unambiguous across the join.
-- Silver makes the choice bronze leaves to it (parser docstring; owner 2026-11-08): only the RAW positions (v1 scored
-- raw and let silly-kicks smooth them; the provider-smoothed rows are `smoothed = true`), and only the FIRST record of
-- each (period, frame) by `record_index` (GS writes a frame once per possession event, up to 16 copies; v1 ADR-030
-- keep-first). Before this, every GS frame reached the computes with each player twice.
{% if lake_table_present('gradientsports_tracking_players') %}
with matches as (
    {% if lake_table_present('gradientsports_matches') %}
    select match_key, home_team_key, away_team_key
    from {{ source('bronze_gradientsports', 'gradientsports_matches') }}
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as bigint) as home_team_key,
        cast(null as bigint) as away_team_key
    where false
    {% endif %}
)
select
    cast(p.match_key as bigint) as match_key,
    cast(p.period as tinyint) as period,
    cast(p.frame as integer) as frame,
    cast(p.player_key as bigint) as player_key,
    cast(case p.team_side when 'home' then mt.home_team_key when 'away' then mt.away_team_key end as bigint) as team_key,
    cast(p.x_mm as integer) as x_mm,
    cast(p.y_mm as integer) as y_mm,
    cast(p.is_visible as boolean) as is_visible,
    {{ bronze_visibility_column('gradientsports_tracking_players') }}
from {{ source('bronze_gradientsports', 'gradientsports_tracking_players') }} as p
left join matches as mt on mt.match_key = p.match_key
where not p.smoothed
qualify p.record_index = min(p.record_index) over (partition by p.match_key, p.period, p.frame)
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
    {{ bronze_visibility_column('gradientsports_tracking_players') }}
where false
{% endif %}
