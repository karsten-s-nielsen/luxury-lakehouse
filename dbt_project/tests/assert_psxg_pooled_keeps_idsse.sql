{{ config(enabled=var('goalkeeper_enabled', false), severity='error') }}
-- Regression guard for the NULL-season INNER JOIN that dropped all IDSSE keepers from the
-- pooled rollup (fixed via the NULL-safe `<=>` season join in fct_gk_shot_stopping_pooled.sql).
-- Fails if IDSSE has match-grain shot-stopping rows but none survive the pooled rollup.
with ss as (
    select count(*) as n from {{ ref('fct_gk_shot_stopping') }} where data_source = 'idsse'
),
pooled as (
    select count(*) as n from {{ ref('fct_gk_shot_stopping_pooled') }} where data_source = 'idsse'
)
select 1
from ss, pooled
where ss.n > 0 and pooled.n = 0
