-- assert_gk_actions_domain_consistency.sql
-- Domain coupling is ONE-DIRECTIONAL (cross-session review C1, verified live 2026-06-11): GS v4
-- carries 15 gk_completion-only rows (139 completion vs 124 xt_gk) — the completion model scores
-- some distributions where xT-GK aborts. So: xt_gk -> gk_completion holds; the symmetric belief
-- is already falsified. (Completion-only rows are an open upstream question, relayed.)
-- ghost_deviation_m exists only where the shot family exists. Own goals never inflate
-- goals_conceded downstream: their action_result is 'owngoal', not 'success'.
select gk_action_id, 'xtgk_without_completion' as violation
from {{ ref('fct_gk_tracking_actions') }}
where xt_gk is not null and gk_completion is null
union all
select gk_action_id, 'deviation_without_preshot' as violation
from {{ ref('fct_gk_tracking_actions') }}
where ghost_deviation_m is not null and pre_shot_gk_x is null
