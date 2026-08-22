-- assert_gk_actions_domain_consistency.sql
-- Domain coupling is ONE-DIRECTIONAL (cross-session review C1, re-keyed onto xt_gk_v2 at spec §7.4):
-- the completion model scores some distributions where xT-GK v2 does not, so gk_completion is a
-- SUPERSET of the xt_gk_v2 domain. So: xt_gk_v2 -> gk_completion holds; the symmetric belief is
-- falsified (completion-only rows are an open upstream question, relayed).
-- ghost_deviation_m exists only where the shot family exists. Own goals never inflate
-- goals_conceded downstream: their action_result is 'owngoal', not 'success'.
select gk_action_id, 'xtgk_without_completion' as violation
from {{ ref('fct_gk_tracking_actions') }}
where xt_gk_v2 is not null and gk_completion is null
union all
select gk_action_id, 'deviation_without_preshot' as violation
from {{ ref('fct_gk_tracking_actions') }}
where ghost_deviation_m is not null and pre_shot_gk_x is null
