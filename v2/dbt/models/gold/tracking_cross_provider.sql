-- The thin cross-provider tracking view (spec §16): the shared core of every tracking provider (spec §11.5), with the
-- canonical match, player and team keys from the canonical pointers. It stores no data.
-- The pointers are looked up with scalar subqueries, not LEFT joins (perf spec §7.4a item 2). The values are the same:
-- resolution writes one pointer per (entity, record_key), and its writer refuses a duplicate. A per-match read
-- filtered on `match_key` made DuckDB flip the joins so the tracking rows became the build side, and the key hash then
-- ran on one thread. A duplicate pointer now fails the read loudly ("More than one row returned by a subquery")
-- instead of silently duplicating rows (owner ruling 2026-11-23).
with core as (
    select 'idsse' as provider, * from {{ ref('stg_idsse__tracking_players') }}
    union all
    select 'skillcorner' as provider, * from {{ ref('stg_skillcorner__tracking_players') }}
    union all
    select 'gradientsports' as provider, * from {{ ref('stg_gradientsports__tracking_players') }}
)
select
    core.provider,
    core.match_key,
    coalesce(
        (
            select m.canonical_key from {{ source('ops', 'ops_canonical_pointers') }} as m
            where m.entity = 'match' and m.record_key = core.match_key
        ),
        core.match_key
    ) as canonical_match_key,
    core.period,
    core.frame,
    core.player_key,
    coalesce(
        (
            select p.canonical_key from {{ source('ops', 'ops_canonical_pointers') }} as p
            where p.entity = 'player' and p.record_key = core.player_key
        ),
        core.player_key
    ) as canonical_player_key,
    core.team_key,
    coalesce(
        (
            select t.canonical_key from {{ source('ops', 'ops_canonical_pointers') }} as t
            where t.entity = 'team' and t.record_key = core.team_key
        ),
        core.team_key
    ) as canonical_team_key,
    core.x_mm,
    core.y_mm,
    core.is_visible,
    core._visibility
from core
