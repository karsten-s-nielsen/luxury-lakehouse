-- assert_skillcorner_minutes_parity.sql
-- SkillCorner minutes are a direct passthrough — SUM per match must match source.
-- Uses match_key (surrogate) since int_minutes_played_per_match outputs surrogates only.

with new_agg as (

    select
        match_key,
        sum(minutes_played) as total_minutes
    from {{ ref('int_minutes_played_per_match') }}
    where data_source = 'skillcorner'
    group by match_key

),

source_agg as (

    -- Resolve SkillCorner native match_id to match_key for comparison.
    select
        dm.match_key,
        sum(cast(sm.minutes_played as double)) as total_minutes
    from {{ ref('stg_skillcorner__matches') }} sm
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'skillcorner'
        and dm.native_match_id = cast(sm.match_id as string)
    where sm.minutes_played is not null
    group by dm.match_key

)

select
    n.match_key,
    n.total_minutes as new_total,
    s.total_minutes as source_total,
    abs(n.total_minutes - s.total_minutes) as delta
from new_agg n
inner join source_agg s
    on n.match_key = s.match_key
where abs(n.total_minutes - s.total_minutes) > 0.01
