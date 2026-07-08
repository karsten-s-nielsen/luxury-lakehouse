-- fct_shot_xg key-integrity guard (spec 2026-07-07 §7 / Task 1.10).
-- Every fct_shot_xg (match_key, action_id) MUST resolve to a row in
-- fct_action_values — the identity fact the mart's Kimball surrogates
-- (competition_key / team_key / player_key) are INNER-JOINed from per ADR-013.
-- The shot key is (match_key, action_id) (action_id is per-match, never global).
-- Anti-join returns offending shot keys (empty = pass).
{{ config(enabled=var('xg_v3_enabled', false), severity='error') }}

select
    sx.match_key,
    sx.action_id
from {{ ref('fct_shot_xg') }} sx
left join {{ ref('fct_action_values') }} av
    on sx.match_key = av.match_key
   and sx.action_id = av.action_id
where av.match_key is null
