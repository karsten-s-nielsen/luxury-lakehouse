-- Layer-0 roster-goalkeeper view (spec D8 amended 2026-11-05; plan Task 3c): EVERY rostered or substituted goalkeeper
-- per (canonical_match_key, canonical_team_key) -> canonical_player_key. The frame builder flags is_goalkeeper for a
-- team's WHOLE keeper SET, so a GK substitution stays correct -- the starter and the sub are each flagged on their own
-- frames (review B-CALC-IMPL-01). Each provider's own GK token, now WITHOUT the one-per-team pick that discarded the
-- bench/sub keeper:
--   gradientsports: positionGroupType in {GK, GOALKEEPER} -- starter AND bench (the `started` flag no longer filters);
--   idsse/DFL: starters via persons PlayingPosition='TW', UNION the sub-in keepers from the Substitution events
--     (@PlayingPosition='TW', @PlayerIn joined back to the person for the key) -- persons PlayingPosition is starters-only;
--   skillcorner: player_role.acronym='GK' over skillcorner_players, which the parser loads as the FULL match squad (the
--     match file `players` list, starters AND subs -- skillcorner.py), so the role filter ALREADY includes a sub keeper;
--     no Substitution union is needed, unlike idsse whose persons are starters-only (CALC-IMPL-09, verified 2026-11-05).
-- Substitution-safety basis per provider: gradientsports = bench rows in the roster; idsse = Substitution events;
-- skillcorner = full-squad `players`. A provider whose roster omitted a sub keeper would fall to convert_to_frames'
-- derive for that keeper's frames (still substitution-safe at the derive's frame-count grain, not silently wrong).
-- A team with no row here falls to convert_to_frames' positional derive (R7). It stores no data. Tagged compute_input.
{{ config(tags=['compute_input']) }}

with gradientsports as (
    {% if lake_table_present('gradientsports_rosters') %}
    select match_key, team_key, player_key
    from {{ source('bronze_gradientsports', 'gradientsports_rosters') }}
    where upper(position_group_type) in ('GK', 'GOALKEEPER')
    {% else %}
    select cast(null as bigint) as match_key, cast(null as bigint) as team_key, cast(null as bigint) as player_key
    where false
    {% endif %}
),

idsse_start as (
    {% if lake_table_present('idsse_persons') %}
    select match_key, team_key, player_key
    from {{ source('bronze_idsse', 'idsse_persons') }}
    where person_kind = 'player' and playing_position = 'TW'
    {% else %}
    select cast(null as bigint) as match_key, cast(null as bigint) as team_key, cast(null as bigint) as player_key
    where false
    {% endif %}
),

idsse_sub as (
    -- The sub-in keeper: a Substitution whose PlayingPosition is 'TW'; its @PlayerIn is the DFL person id, joined back
    -- to the person row for the surrogate player_key + the team_key (persons PlayingPosition='TW' is starters-only).
    {% if lake_table_present('idsse_persons') and lake_table_present('idsse_events') %}
    select p.match_key, p.team_key, p.player_key
    from {{ source('bronze_idsse', 'idsse_events') }} as e
    inner join {{ source('bronze_idsse', 'idsse_persons') }} as p
        on p.match_key = e.match_key and p.person_id = e.substitution_player_in
    where e.substitution_playing_position = 'TW'
    {% else %}
    select cast(null as bigint) as match_key, cast(null as bigint) as team_key, cast(null as bigint) as player_key
    where false
    {% endif %}
),

skillcorner as (
    {% if lake_table_present('skillcorner_players') %}
    select match_key, team_key, player_key
    from {{ source('bronze_skillcorner', 'skillcorner_players') }}
    where player_role_acronym = 'GK'
    {% else %}
    select cast(null as bigint) as match_key, cast(null as bigint) as team_key, cast(null as bigint) as player_key
    where false
    {% endif %}
),

-- UNION (not UNION ALL) dedups a keeper listed twice; only one provider's CTE has rows for a given match.
core as (
    select match_key, team_key, player_key from gradientsports
    union
    select match_key, team_key, player_key from idsse_start
    union
    select match_key, team_key, player_key from idsse_sub
    union
    select match_key, team_key, player_key from skillcorner
)

select distinct
    coalesce(m.canonical_key, core.match_key) as canonical_match_key,
    coalesce(t.canonical_key, core.team_key) as canonical_team_key,
    coalesce(p.canonical_key, core.player_key) as canonical_player_key
from core
left join {{ source('ops', 'ops_canonical_pointers') }} as m
    on m.entity = 'match' and m.record_key = core.match_key
left join {{ source('ops', 'ops_canonical_pointers') }} as t
    on t.entity = 'team' and t.record_key = core.team_key
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = 'player' and p.record_key = core.player_key
where core.player_key is not null
