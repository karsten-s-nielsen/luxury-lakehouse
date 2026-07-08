-- Cross-mart AV<->AC key-integrity gate (spec 2026-07-07 §8 / B2 / N3, Task 1.10).
-- For every TRACKING-provider shot, its (match_key, action_id) in fct_action_values
-- (the xG source) MUST exist in fct_action_context (where pressure / RM context lives).
-- Both marts derive action_id from the SAME bronze.spadl_actions SPADL conversion, so
-- the downstream `fct_shot_xg <-> fct_action_context` join pairs xG and pressure on the
-- SAME physical shot. The anti-join pins that they never diverge (empty = pass).
--
-- KEY ANTI-JOIN ONLY (spec N3): this test deliberately does NOT compare (start_x, start_y)
-- coordinates. fct_action_values (acting-team-LTR per ADR-028) and fct_action_context
-- (home-LTR) may store DIFFERENT orientations, so the same physical shot can carry MIRROR
-- coordinates. A raw coordinate equality would false-fail; an orientation-convention
-- mismatch must NEVER read as a key-integrity failure.
{{ config(enabled=var('xg_v3_enabled', false), severity='error') }}

select
    av.match_key,
    av.action_id,
    av.data_source
from {{ ref('fct_action_values') }} av
left join {{ ref('fct_action_context') }} ac
    on av.match_key = ac.match_key
   and av.action_id = ac.action_id
where av.data_source in ('gradientsports', 'skillcorner', 'idsse', 'metrica')
  and av.action_type in ('shot', 'shot_freekick', 'shot_penalty')
  and ac.match_key is null
