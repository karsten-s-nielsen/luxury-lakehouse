-- Layer-1 staging view over the spadl_actions bronze (plan Task 4b). `lake_table_present`-guarded so it is
-- empty-but-typed before the SPADL compute first writes (spec D2). `fct_actions` (built by COMPUTE_L1_SELECT
-- "+fct_actions" between the layer-1 and layer-2 compute stages) selects it; line_breaking reads fct_actions.
{% if lake_table_present('spadl_actions') %}
select
    canonical_match_key,
    cast(period_id as tinyint) as period_id,
    cast(action_id as integer) as action_id,
    cast(original_event_id as varchar) as original_event_id,
    canonical_player_key,
    canonical_team_key,
    cast(time_seconds as double) as time_seconds,
    cast(start_x as double) as start_x,
    cast(start_y as double) as start_y,
    cast(end_x as double) as end_x,
    cast(end_y as double) as end_y,
    cast(type_id as integer) as type_id,
    cast(result_id as integer) as result_id,
    cast(bodypart_id as integer) as bodypart_id
from {{ source('bronze_compute', 'spadl_actions') }}
{% else %}
select
    cast(null as bigint) as canonical_match_key,
    cast(null as tinyint) as period_id,
    cast(null as integer) as action_id,
    cast(null as varchar) as original_event_id,
    cast(null as bigint) as canonical_player_key,
    cast(null as bigint) as canonical_team_key,
    cast(null as double) as time_seconds,
    cast(null as double) as start_x,
    cast(null as double) as start_y,
    cast(null as double) as end_x,
    cast(null as double) as end_y,
    cast(null as integer) as type_id,
    cast(null as integer) as result_id,
    cast(null as integer) as bodypart_id
where false
{% endif %}
