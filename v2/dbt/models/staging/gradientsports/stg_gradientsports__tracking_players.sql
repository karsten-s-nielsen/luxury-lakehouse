-- The shared tracking core of gradientsports (spec §11.5), one row per player per frame.
{% if lake_table_present('gradientsports_tracking_players') %}
select
    cast(match_key as bigint) as match_key,
    cast(period as tinyint) as period,
    cast(frame as integer) as frame,
    cast(player_key as bigint) as player_key,
    cast(x_mm as integer) as x_mm,
    cast(y_mm as integer) as y_mm,
    cast(is_visible as boolean) as is_visible,
    {{ bronze_visibility_column('gradientsports_tracking_players') }}
from {{ source('bronze_gradientsports', 'gradientsports_tracking_players') }}
{% else %}
select
    cast(null as bigint) as match_key,
    cast(null as tinyint) as period,
    cast(null as integer) as frame,
    cast(null as bigint) as player_key,
    cast(null as integer) as x_mm,
    cast(null as integer) as y_mm,
    cast(null as boolean) as is_visible,
    {{ bronze_visibility_column('gradientsports_tracking_players') }}
where false
{% endif %}
