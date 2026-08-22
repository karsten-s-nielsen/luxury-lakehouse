-- assert_gk_stats_reconciles_actions.sql
-- The stats mart's distribution counts must reconcile exactly with the actions mart it
-- aggregates (catches orphaned stats rows + drift between the two merge-materialized marts).
select s.gk_match_stat_id
from {{ ref('fct_gk_tracking_stats') }} s
left join (
    select player_key, match_key, count(*) as n
    from {{ ref('fct_gk_tracking_actions') }}
    -- MUST mirror the stats mart's distribution CTE predicate (xt_gk_v2 domain marker, NOT the
    -- disjoint pre-shot gk_was_distributing flag) or reconciliation fails by construction.
    where xt_gk_v2 is not null and player_key is not null
    group by player_key, match_key
) a on a.player_key = s.gk_player_key and a.match_key = s.match_key
where s.n_distributions is not null and s.n_distributions != coalesce(a.n, 0)
