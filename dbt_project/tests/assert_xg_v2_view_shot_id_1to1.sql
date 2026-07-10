-- fct_xg_predictions_v2 (back-compat TABLE projecting fct_shot_xg) grain guard (Task 2.4 / C-b).
-- NOTE: the "v2_view" in this filename is historical — fct_xg_predictions_v2 is now a materialized
-- table (C-b, ADR-066), not a view. It reconstructs shot_id via the bridge
--   fct_shot_xg.(match_key, action_id) -> fct_action_values.original_event_id
--   -> fct_shots.event_id / shot_id.
-- shot_id must stay 1:1 with fct_shots.shot_id: no fan-out from that bridge (a native
-- event_id mapping to >1 shot action, or an action_id collision, would double-count a
-- shot). Returns any shot_id emitted more than once (empty = pass).
{{ config(enabled=var('xg_v3_enabled', false), severity='error') }}

select
    shot_id,
    count(*) as n_rows
from {{ ref('fct_xg_predictions_v2') }}
group by shot_id
having count(*) > 1
