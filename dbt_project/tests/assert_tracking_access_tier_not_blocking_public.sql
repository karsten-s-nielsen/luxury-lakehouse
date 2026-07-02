-- H1 / ADR-064 recurrence guard (2026-07-02): public tracking data must NOT be fail-safe-blocked.
--
-- Root cause this guards: every bronze tracking table was populated BEFORE H1 (per-match
-- access_tier) added tier stamping to the tracking writers, so historical rows carried
-- access_tier = NULL. split_restricted() fail-safes NULL -> restricted, which WITHHELD public
-- (open-data / MIT) tracking from the public HF datasets. Fixed by the backfill
-- scripts/migrations/2026-07-02-backfill-tracking-access-tier.sql; this test blocks the class from
-- recurring -- e.g. a public tracking table re-ingested/wiped and left NULL, or a NEW
-- public-by-license tracking provider ingested without stamping access_tier.
--
-- Invariant: public tracking rows carry access_tier = 'public' (never NULL, never 'restricted').
--   * idsse / metrica -- PUBLIC_BY_LICENSE_PROVIDERS open-data (the tracking-bearing subset of the
--     allowlist; statsbomb/wyscout are event-only): EVERY tracking row must be 'public'.
--   * skillcorner     -- per-match visibility feed: tracking of a visibility='public' match must be
--     'public' (private RM matches are correctly 'restricted').
-- gradientsports (a RESTRICTED provider) is intentionally excluded: NULL fail-safes to 'restricted',
-- which is the CORRECT outcome there -- public data is not the concern.
--
-- Companion to assert_access_tier_visibility_consistency.sql, which catches the opposite (leak)
-- direction. Severity = dbt default (error): a violation FAILS the daily build.

-- Compile-time drift guard: this test hard-codes idsse+metrica as the open-data tracking
-- providers. If either leaves the allowlist (becomes non-open-data), that assumption is wrong.
{% set allow = var('public_by_license_providers') %}
{% if 'idsse' not in allow or 'metrica' not in allow %}
    {{ exceptions.raise_compiler_error(
        "assert_tracking_access_tier_not_blocking_public assumes idsse+metrica are open-data "
        ~ "(public_by_license_providers); the allowlist changed -- revisit this guard's hard-coded sources"
    ) }}
{% endif %}

with violations as (

    -- open-data tracking providers: every row must be 'public'
    select 'idsse_tracking' as source, cast(match_id as string) as match_ref, access_tier
    from {{ source('idsse', 'idsse_tracking') }}
    where access_tier is null or access_tier <> 'public'

    union all

    select 'metrica_tracking' as source, cast(match_id as string) as match_ref, access_tier
    from {{ source('metrica', 'metrica_tracking') }}
    where access_tier is null or access_tier <> 'public'

    union all

    -- skillcorner: public-visibility matches' tracking must be 'public'
    -- (dedupe roster-format matches to one visibility per match_id before the join)
    select 'skillcorner_tracking' as source, cast(t.match_id as string) as match_ref, t.access_tier
    from {{ source('skillcorner', 'skillcorner_tracking') }} t
    join (
        select distinct match_id, visibility
        from {{ source('skillcorner', 'skillcorner_matches') }}
    ) m
        on t.match_id = m.match_id
    where m.visibility = 'public'
        and (t.access_tier is null or t.access_tier <> 'public')

)

-- distinct so a broken table surfaces its (source, match, tier) combos, not millions of rows
select distinct source, match_ref, access_tier
from violations
