-- assert_tracking_frames_gk_count_by_provider.sql
-- TC-2: Every match must have at least 2 GKs for IDSSE/SkillCorner,
-- and at least 1 for any provider. Metrica is allowed 1 due to
-- de-identified data (home GK only).
--
-- Counts GKs without pre-filtering — a match where is_goalkeeper = false
-- for ALL players still produces a group with n_gks = 0.
-- Returns rows that FAIL the expectation — 0 rows = all pass.

with match_gk_counts as (

    select
        match_key,
        data_source,
        count(distinct case when is_goalkeeper then player_key end) as n_gks
    from {{ ref('fct_tracking_frames') }}
    group by match_key, data_source

)

select *
from match_gk_counts
where
    (data_source in ('idsse', 'skillcorner') and n_gks < 2)
    or n_gks = 0
