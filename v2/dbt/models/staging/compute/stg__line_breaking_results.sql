-- Layer-2 staging view over the line_breaking_results bronze (plan Task 5). `lake_table_present`-guarded so it is
-- empty-but-typed before the compute first writes (spec D2); it builds in the final `+fct_tracking_metrics` pass, which
-- the mart `ref()`s. One row per pass/cross action.
{% if lake_table_present('line_breaking_results') %}
select
    canonical_match_key,
    cast(period_id as tinyint) as period_id,
    cast(action_id as integer) as action_id,
    canonical_player_key,
    cast(line_break as boolean) as line_break,
    cast(lines_broken as integer) as lines_broken,
    cast(line_breaking_type as varchar) as line_breaking_type
from {{ source('bronze_compute', 'line_breaking_results') }}
{% else %}
select
    cast(null as bigint) as canonical_match_key,
    cast(null as tinyint) as period_id,
    cast(null as integer) as action_id,
    cast(null as bigint) as canonical_player_key,
    cast(null as boolean) as line_break,
    cast(null as integer) as lines_broken,
    cast(null as varchar) as line_breaking_type
where false
{% endif %}
