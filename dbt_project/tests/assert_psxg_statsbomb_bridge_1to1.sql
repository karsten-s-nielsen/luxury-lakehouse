-- C4 / 2.2: the StatsBomb shot -> SPADL action bridge must be 1:1.
-- A native event_id mapping to >1 shot action would fan out fct_shot_psxg's
-- (match_key, action_id) grain and double-count StatsBomb psxg. Returns the
-- offending event_ids (test passes when empty). Promoted to severity='error'
-- after the first clean deploy (2026-06-21, ADR-060 4-feature v2 cutover) — hard gate.
{{ config(enabled=var('goalkeeper_enabled', false), severity='error') }}

select
    s.event_id,
    count(*) as n_shot_actions
from {{ ref('fct_shots') }} s
inner join {{ ref('fct_action_values') }} av
    on av.original_event_id = s.event_id
   and av.data_source = s.data_source
where s.data_source = 'statsbomb'
  and av.action_type in ('shot', 'shot_freekick', 'shot_penalty')
group by s.event_id
having count(*) > 1
