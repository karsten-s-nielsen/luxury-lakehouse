-- Layer-1 staging view over the pitch_control_values bronze (plan Task 4). `lake_table_present`-guarded so it is
-- empty-but-typed before the compute first writes (spec D2); it builds in the final `+fct_tracking_metrics` pass, which
-- the mart `ref()`s. One row per team per frame.
{% if lake_table_present('pitch_control_values') %}
select
    canonical_match_key,
    cast(period as tinyint) as period,
    cast(frame as integer) as frame,
    canonical_team_key,
    cast(control_fraction as double) as control_fraction
from {{ source('bronze_compute', 'pitch_control_values') }}
{% else %}
select
    cast(null as bigint) as canonical_match_key,
    cast(null as tinyint) as period,
    cast(null as integer) as frame,
    cast(null as bigint) as canonical_team_key,
    cast(null as double) as control_fraction
where false
{% endif %}
