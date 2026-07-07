-- ADR-064 recurrence guard on the PUBLISHED FACT (2026-07-06): public open-data action rows must
-- NOT be fail-safe-blocked out of the public HF dataset.
--
-- Companion to assert_tracking_access_tier_not_blocking_public.sql (which guards the raw bronze
-- TRACKING sources). That guard explicitly excludes statsbomb/wyscout ("event-only") and does NOT
-- cover the GOLD fact `fct_action_values` — the table publish_spadl_vaep_hf.py actually splits on
-- access_tier. So a stale/NULL access_tier on the fact (event providers built before the mart's
-- access_tier passthrough, never re-derived) went uncaught until a full-split republish routed the
-- public open data into the PRIVATE companion repo and swept the public dataset (2026-07-06 incident;
-- backfilled by scripts/migrations/2026-07-06-backfill-fct-action-values-access-tier.sql). This test
-- blocks the class at BUILD time — the daily build fails before any publish can mis-route.
--
-- Invariant on fct_action_values: every PUBLIC_BY_LICENSE_PROVIDERS row (statsbomb/wyscout/idsse/
-- metrica) carries access_tier = 'public' (never NULL, never 'restricted'); every public-visibility
-- skillcorner match's rows carry 'public'. gradientsports (RESTRICTED) is intentionally excluded --
-- NULL fail-safes to 'restricted' there, which is the CORRECT outcome (public data is not the concern).
-- Severity = dbt default (error).

-- Compile-time drift guard: hard-codes the event open-data providers against the allowlist var.
{% set allow = var('public_by_license_providers') %}
{% if 'statsbomb' not in allow or 'wyscout' not in allow or 'idsse' not in allow or 'metrica' not in allow %}
    {{ exceptions.raise_compiler_error(
        "assert_action_values_access_tier_not_blocking_public assumes statsbomb/wyscout/idsse/metrica "
        ~ "are open-data (public_by_license_providers); the allowlist changed -- revisit this guard"
    ) }}
{% endif %}

with violations as (

    -- open-data event/action providers: every published action row must be 'public'
    select data_source, cast(match_key as string) as match_ref, access_tier
    from {{ ref('fct_action_values') }}
    where data_source in ('statsbomb', 'wyscout', 'idsse', 'metrica')
      and (access_tier is null or access_tier <> 'public')

    union all

    -- skillcorner: public-visibility matches' action rows must be 'public'
    -- (private RM matches are correctly 'restricted').
    select av.data_source, cast(av.match_key as string) as match_ref, av.access_tier
    from {{ ref('fct_action_values') }} av
    join {{ ref('dim_matches') }} dm
        on dm.match_key = av.match_key
    where av.data_source = 'skillcorner'
      and dm.visibility = 'public'
      and (av.access_tier is null or av.access_tier <> 'public')

)

-- distinct so a broken slice surfaces its (source, match, tier) combos, not millions of rows
select distinct data_source, match_ref, access_tier
from violations
