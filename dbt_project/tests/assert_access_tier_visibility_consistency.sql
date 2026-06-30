-- H1.3 / ADR-064 amendment (2026-06-30, review P1 approach A): the access_tier <-> visibility divergence invariant.
-- A provider NOT on the public-by-license allowlist (skillcorner, gradientsports, any future provider) that has
-- reached access_tier='public' MUST carry an explicit visibility='public' — never a default. Enforced at the SOURCE
-- where the per-match visibility lives (dim_matches), AND on the row-level facts whose access_tier is stamped from
-- bronze (not derived from dim_matches) — those join dim_matches for the match's visibility. Together with the
-- per-publish leak guard (ingestion.hf_leak_guard) this makes per-row visibility threading unnecessary: post the
-- allowlist flip, access_tier already encodes the per-row visibility decision.
--
-- Severity = dbt default (error): a divergence FAILS the build, gating the marts before the dependent publish tasks
-- (terraform DAG: hf publishers depend_on the dbt_build_*_marts tasks).
--
-- The allowlist is the single dbt var `public_by_license_providers`, mirrored from
-- shared.access_tier.PUBLIC_BY_LICENSE_PROVIDERS (drift blocked by test_access_tier_visibility_consistency_allowlist).

{% set allow = var('public_by_license_providers') %}

with violations as (

    -- (1) the per-match source of truth
    select 'dim_matches' as source, provider as data_source, cast(native_match_id as string) as match_ref,
           access_tier, visibility
    from {{ ref('dim_matches') }}
    where provider not in ('{{ allow | join("','") }}')
        and access_tier = 'public'
        and (visibility is null or visibility <> 'public')

    union all

    -- (2) row-level fact: access_tier stamped from bronze; visibility resolved by joining dim_matches
    select 'fct_action_context', f.data_source, cast(f.match_key as string), f.access_tier, dm.visibility
    from {{ ref('fct_action_context') }} f
    left join {{ ref('dim_matches') }} dm on f.match_key = dm.match_key
    where f.data_source not in ('{{ allow | join("','") }}')
        and f.access_tier = 'public'
        and (dm.visibility is null or dm.visibility <> 'public')

    union all

    select 'fct_action_values', f.data_source, cast(f.match_key as string), f.access_tier, dm.visibility
    from {{ ref('fct_action_values') }} f
    left join {{ ref('dim_matches') }} dm on f.match_key = dm.match_key
    where f.data_source not in ('{{ allow | join("','") }}')
        and f.access_tier = 'public'
        and (dm.visibility is null or dm.visibility <> 'public')

)

select * from violations
