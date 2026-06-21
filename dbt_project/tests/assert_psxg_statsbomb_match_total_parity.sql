-- 2.4 value-parity guard (attribution-independent): the per-match total StatsBomb
-- psxg + on-target shot count in fct_shot_psxg must equal the source predictions
-- (stg_psxg__predictions joined to fct_shots on-target). This isolates the bridge +
-- value pass-through from the GK-attribution change (A2) — the per-GK attribution
-- reconciliation (legacy lineup vs per-shot) is a separate deploy-data step. Returns
-- mismatching matches (test passes when empty). Promote to severity='error' after the
-- first clean deploy.
{{ config(enabled=var('goalkeeper_enabled', false), severity='warn') }}

with legacy as (

    select
        s.match_key,
        sum(psxg.psxg) as psxg_total,
        count(*)       as n_shots
    from {{ ref('stg_psxg__predictions') }} psxg
    inner join {{ ref('fct_shots') }} s
        on s.shot_id = psxg.event_id
    where s.data_source = 'statsbomb'
      and s.shot_outcome in ('Goal', 'Saved', 'Post', 'Saved to Post')
      and s.end_location_z is not null
    group by s.match_key

),

new_fact as (

    select
        match_key,
        sum(psxg) as psxg_total,
        count(*)  as n_shots
    from {{ ref('fct_shot_psxg') }}
    where psxg_input_source = 'statsbomb_freeze_frame'
    group by match_key

)

select
    coalesce(l.match_key, n.match_key)        as match_key,
    l.psxg_total                              as legacy_psxg_total,
    n.psxg_total                              as new_psxg_total,
    l.n_shots                                 as legacy_n_shots,
    n.n_shots                                 as new_n_shots
from legacy l
full outer join new_fact n
    on l.match_key = n.match_key
where abs(coalesce(l.psxg_total, 0) - coalesce(n.psxg_total, 0)) > 0.01
   or coalesce(l.n_shots, 0) != coalesce(n.n_shots, 0)
