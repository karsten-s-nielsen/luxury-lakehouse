-- The thin cross-provider tracking view (spec §16): the shared core of every tracking provider (spec §11.5), joined to
-- the canonical player and match pointers. It stores no data.
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
    coalesce(m.canonical_key, core.match_key) as canonical_match_key,
    core.period,
    core.frame,
    core.player_key,
    coalesce(p.canonical_key, core.player_key) as canonical_player_key,
    core.x_mm,
    core.y_mm,
    core.is_visible,
    core._visibility
from core
left join {{ source('ops', 'ops_canonical_pointers') }} as m
    on m.entity = 'match' and m.record_key = core.match_key
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = 'player' and p.record_key = core.player_key
