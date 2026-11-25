-- Layer-0 ball + possession input view for the tracking computes (spec D7 + R8; plan Task 3b/3c): per
-- (canonical_match_key, period, frame) the ball position, a possession_team_key derived from each provider's OWN
-- possession signal — idsse ball_possession 1/2 -> home/guest, skillcorner possession_group -> home/away,
-- gradientsports has none -> null (never invented; spec D7 anti-goal) — and `time_seconds`, the PERIOD-RELATIVE clock
-- the frame builder feeds the long converters (R8): idsse/gradientsports carry `timestamp_us`, int64 microseconds
-- since the period start on every write path (ADR-018), so period-relative seconds are `timestamp_us / 1e6`;
-- skillcorner -> null (its wide shaper derives frame/frame_rate; frame_rate is the bronze cadence-derived column). It
-- stores no data.
-- Tagged compute_input (the layer-0 marker). `match_key` is the provider record key: the compute key reads filter on it
-- as well as on `canonical_match_key`, because only a `match_key` filter reaches the gradientsports dedup window (perf
-- spec §7.4a). The pointers are scalar lookups, as in tracking_cross_provider: a duplicate pointer fails the read
-- loudly instead of duplicating ball rows (owner ruling 2026-11-23).
{{ config(tags=['compute_input']) }}

with idsse as (
    {% if lake_table_present('idsse_tracking_frames') %}
    select
        f.match_key,
        cast(f.period as tinyint) as period,
        cast(f.frame as integer) as frame,
        cast(f.ball_x_mm as integer) as ball_x_mm,
        cast(f.ball_y_mm as integer) as ball_y_mm,
        cast(f.ball_detected as boolean) as ball_detected,
        f.timestamp_us / 1e6 as time_seconds,  -- int64 microseconds -> seconds (R8, ADR-018)
        case f.ball_possession when 1 then m.home_team_key when 2 then m.guest_team_key end as possession_team_key
    from {{ source('bronze_idsse', 'idsse_tracking_frames') }} as f
    left join {{ source('bronze_idsse', 'idsse_match_information') }} as m on m.match_key = f.match_key
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as tinyint) as period,
        cast(null as integer) as frame,
        cast(null as integer) as ball_x_mm,
        cast(null as integer) as ball_y_mm,
        cast(null as boolean) as ball_detected,
        cast(null as double) as time_seconds,
        cast(null as bigint) as possession_team_key
    where false
    {% endif %}
),

skillcorner as (
    {% if lake_table_present('skillcorner_tracking_frames') %}
    select
        f.match_key,
        cast(f.period as tinyint) as period,
        cast(f.frame as integer) as frame,
        cast(f.ball_x_mm as integer) as ball_x_mm,
        cast(f.ball_y_mm as integer) as ball_y_mm,
        cast(f.ball_detected as boolean) as ball_detected,
        cast(null as double) as time_seconds,  -- no per-frame clock; the wide shaper derives frame/frame_rate
        case f.possession_group
            when 'home team' then m.home_team_key
            when 'away team' then m.away_team_key
        end as possession_team_key
    from {{ source('bronze_skillcorner', 'skillcorner_tracking_frames') }} as f
    left join {{ source('bronze_skillcorner', 'skillcorner_matches') }} as m on m.match_key = f.match_key
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as tinyint) as period,
        cast(null as integer) as frame,
        cast(null as integer) as ball_x_mm,
        cast(null as integer) as ball_y_mm,
        cast(null as boolean) as ball_detected,
        cast(null as double) as time_seconds,
        cast(null as bigint) as possession_team_key
    where false
    {% endif %}
),

gradientsports as (
    {% if lake_table_present('gradientsports_tracking_frames') %}
    select
        match_key,
        cast(period as tinyint) as period,
        cast(frame as integer) as frame,
        cast(ball_x_mm as integer) as ball_x_mm,
        cast(ball_y_mm as integer) as ball_y_mm,
        cast(ball_detected as boolean) as ball_detected,
        timestamp_us / 1e6 as time_seconds,  -- int64 microseconds -> seconds (R8, ADR-018)
        cast(null as bigint) as possession_team_key  -- gradientsports carries no per-frame possession team (D7)
    from {{ source('bronze_gradientsports', 'gradientsports_tracking_frames') }}
    -- One record per (period, frame): the FIRST by `record_index` (GS writes a frame once per possession event;
    -- v1 ADR-030 keep-first; owner 2026-11-08), the same record the player rows keep.
    qualify row_number() over (partition by match_key, period, frame order by record_index) = 1
    {% else %}
    select
        cast(null as bigint) as match_key,
        cast(null as tinyint) as period,
        cast(null as integer) as frame,
        cast(null as integer) as ball_x_mm,
        cast(null as integer) as ball_y_mm,
        cast(null as boolean) as ball_detected,
        cast(null as double) as time_seconds,
        cast(null as bigint) as possession_team_key
    where false
    {% endif %}
),

core as (
    select * from idsse
    union all
    select * from skillcorner
    union all
    select * from gradientsports
)

select
    coalesce(
        (
            select m.canonical_key from {{ source('ops', 'ops_canonical_pointers') }} as m
            where m.entity = 'match' and m.record_key = core.match_key
        ),
        core.match_key
    ) as canonical_match_key,
    core.match_key,
    core.period,
    core.frame,
    core.ball_x_mm,
    core.ball_y_mm,
    core.ball_detected,
    core.time_seconds,
    coalesce(
        (
            select t.canonical_key from {{ source('ops', 'ops_canonical_pointers') }} as t
            where t.entity = 'team' and t.record_key = core.possession_team_key
        ),
        core.possession_team_key
    ) as possession_team_key
from core
