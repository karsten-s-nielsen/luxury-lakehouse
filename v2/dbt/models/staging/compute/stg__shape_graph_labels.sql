-- Layer-1 staging view over the shape_graph_labels bronze (plan Task 6). `lake_table_present`-guarded so it is
-- empty-but-typed before the compute first writes (spec D2); it builds in the final `+fct_tracking_metrics` pass, which
-- the mart `ref()`s. One row per outfield player per frame.
{% if lake_table_present('shape_graph_labels') %}
select
    canonical_match_key,
    cast(period as tinyint) as period,
    cast(frame as integer) as frame,
    canonical_player_key,
    cast(position_label as varchar) as position_label
from {{ source('bronze_compute', 'shape_graph_labels') }}
{% else %}
select
    cast(null as bigint) as canonical_match_key,
    cast(null as tinyint) as period,
    cast(null as integer) as frame,
    cast(null as bigint) as canonical_player_key,
    cast(null as varchar) as position_label
where false
{% endif %}
