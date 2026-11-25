-- fct_actions: the gold SPADL actions view (spec D9). Built by COMPUTE_L1_SELECT "+fct_actions" between the layer-1
-- SPADL compute and the layer-2 line_breaking compute, which reads it (actions x frames). Contract enforced; it stores
-- no data (a view over stg__spadl_actions).
select
    canonical_match_key,
    period_id,
    action_id,
    original_event_id,
    canonical_player_key,
    canonical_team_key,
    time_seconds,
    start_x,
    start_y,
    end_x,
    end_y,
    type_id,
    result_id,
    bodypart_id
from {{ ref('stg__spadl_actions') }}
