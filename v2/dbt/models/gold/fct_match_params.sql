-- Layer-0 per-match converter parameters (spec D8 ruling R9; plan Task 3c): one row per (canonical_match_key) with
-- the inputs each silly-kicks `convert_to_frames` needs — provider, canonical home_team_key, the orientation flags,
-- frame_rate (Hz) and pitch size (metres). The frame builder reads this instead of reaching into raw provider bronze
-- (clean-forward, D1). Per provider:
--   gradientsports: home_team_start_left[_extra_time] + fps direct from the match; pitch is the converter's fixed
--                   105x68 (it offsets x_centered+52.5), so 105/68 here is nominal.
--   idsse/sportec:  home_team_start_left DERIVED from the first-half KickOff (which team is on the left == home?);
--                   25 Hz; pitch nominal 105x68 (sportec also offsets +52.5).
--   skillcorner:    orientation is GEOMETRIC (home_team_start_left null -> the converter's geometric net), so null
--                   here is correct; real pitch_length/width from the match (skillcorner scales to 105x68). fps is
--                   the bronze tracking `frame_rate`, which the parser derives from the match-clock cadence (the v1
--                   rule): the owner-tier match file has no `match_periods`, so the match file cannot give it.
-- It stores no data. Tagged compute_input.
{{ config(tags=['compute_input']) }}

with gradientsports as (
    {% if lake_table_present('gradientsports_matches') %}
    select
        match_key,
        'gradientsports' as provider,
        home_team_key,
        cast(home_team_start_left as boolean) as home_team_start_left,
        cast(home_team_start_left_extra_time as boolean) as home_team_start_left_extra_time,
        cast(fps as double) as frame_rate,
        cast(105.0 as double) as pitch_length,
        cast(68.0 as double) as pitch_width
    from {{ source('bronze_gradientsports', 'gradientsports_matches') }}
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as varchar) as provider,
        cast(null as bigint) as home_team_key,
        cast(null as boolean) as home_team_start_left,
        cast(null as boolean) as home_team_start_left_extra_time,
        cast(null as double) as frame_rate,
        cast(null as double) as pitch_length,
        cast(null as double) as pitch_width
    where false
    {% endif %}
),

idsse as (
    {% if lake_table_present('idsse_match_information') %}
    select
        mi.match_key,
        'idsse' as provider,
        mi.home_team_key,
        {% if lake_table_present('idsse_events') %}
        (ko.kick_off_team_left = mi.general_home_team_id)
        {% else %}
        cast(null as boolean)
        {% endif %} as home_team_start_left,
        cast(null as boolean) as home_team_start_left_extra_time,
        cast(25.0 as double) as frame_rate,
        cast(105.0 as double) as pitch_length,
        cast(68.0 as double) as pitch_width
    from {{ source('bronze_idsse', 'idsse_match_information') }} as mi
    {% if lake_table_present('idsse_events') %}
    left join (
        select match_key, any_value(kick_off_team_left) as kick_off_team_left
        from {{ source('bronze_idsse', 'idsse_events') }}
        where event_type = 'KickOff' and kick_off_game_section = 'firstHalf'
        group by match_key
    ) as ko on ko.match_key = mi.match_key
    {% endif %}
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as varchar) as provider,
        cast(null as bigint) as home_team_key,
        cast(null as boolean) as home_team_start_left,
        cast(null as boolean) as home_team_start_left_extra_time,
        cast(null as double) as frame_rate,
        cast(null as double) as pitch_length,
        cast(null as double) as pitch_width
    where false
    {% endif %}
),

skillcorner as (
    {% if lake_table_present('skillcorner_matches') %}
    select
        m.match_key,
        'skillcorner' as provider,
        m.home_team_key,
        cast(null as boolean) as home_team_start_left,  -- skillcorner orients geometrically (D8 / silly-kicks)
        cast(null as boolean) as home_team_start_left_extra_time,
        cast(t.frame_rate as double) as frame_rate,
        cast(m.pitch_length as double) as pitch_length,
        cast(m.pitch_width as double) as pitch_width
    from {{ source('bronze_skillcorner', 'skillcorner_matches') }} as m
    -- An INNER join: a skillcorner match without tracking frames (an events-only delivery) has nothing to convert,
    -- so it has no converter parameters and no row here. Its rate would be null, and the not_null on frame_rate
    -- would fail the whole run (owner 2026-11-24, impl review r2 B-08).
    {% if lake_table_present('skillcorner_tracking_frames') %}
    join (
        select match_key, any_value(frame_rate) as frame_rate
        from {{ source('bronze_skillcorner', 'skillcorner_tracking_frames') }}
        group by match_key
    ) as t on t.match_key = m.match_key
    {% else %}
    join (select cast(null as bigint) as match_key, cast(null as integer) as frame_rate where false) as t
        on t.match_key = m.match_key
    {% endif %}
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as varchar) as provider,
        cast(null as bigint) as home_team_key,
        cast(null as boolean) as home_team_start_left,
        cast(null as boolean) as home_team_start_left_extra_time,
        cast(null as double) as frame_rate,
        cast(null as double) as pitch_length,
        cast(null as double) as pitch_width
    where false
    {% endif %}
),

core as (
    select * from gradientsports
    union all
    select * from idsse
    union all
    select * from skillcorner
)

select
    coalesce(p.canonical_key, core.match_key) as canonical_match_key,
    core.provider,
    coalesce(t.canonical_key, core.home_team_key) as home_team_key,
    core.home_team_start_left,
    core.home_team_start_left_extra_time,
    core.frame_rate,
    core.pitch_length,
    core.pitch_width
from core
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = 'match' and p.record_key = core.match_key
left join {{ source('ops', 'ops_canonical_pointers') }} as t
    on t.entity = 'team' and t.record_key = core.home_team_key
