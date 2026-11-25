-- Layer-2 mart (spec D3; plan Task 7): one row per (canonical_match_key, canonical_player_key), aggregating the three
-- layer-1/2 tracking-metric staging views to the player-match grain. A view with an enforced contract (never a native
-- table). Built in the final `+fct_tracking_metrics` pass, after every compute has written its bronze.
--   pitch_control -> the player's mean controlled-area fraction (their team's control over the frames they appear in)
--   shape_graphs  -> the player's modal position label + the fraction of frames spent in it
--   line_breaking -> the player's count of line-breaking pass/cross actions
{{ config(tags=['tracking_metric']) }}

with players as (
    -- one row per (match, player); `_visibility` is the match's authoritative row visibility (spec §6.4) carried from
    -- the frames so the read seam can filter restricted mart rows. `min` takes the most restrictive if ever mixed
    -- ('private' < 'public'); null on an unencrypted-local or HF lake, where the bucket split enforces visibility.
    select
        canonical_match_key,
        canonical_player_key,
        min(_visibility) as _visibility
    from {{ ref('fct_tracking_frames') }}
    group by canonical_match_key, canonical_player_key
),

pitch_control as (
    select
        f.canonical_match_key,
        f.canonical_player_key,
        avg(pc.control_fraction) as mean_controlled_fraction
    from {{ ref('fct_tracking_frames') }} as f
    inner join {{ ref('stg__pitch_control_values') }} as pc
        on pc.canonical_match_key = f.canonical_match_key
        and pc.period = f.period
        and pc.frame = f.frame
        and pc.canonical_team_key = f.canonical_team_key
    group by f.canonical_match_key, f.canonical_player_key
),

shape_counts as (
    select canonical_match_key, canonical_player_key, position_label, count(*) as n
    from {{ ref('stg__shape_graph_labels') }}
    group by canonical_match_key, canonical_player_key, position_label
),

shape as (
    select
        canonical_match_key,
        canonical_player_key,
        arg_max(position_label, n) as modal_position,
        cast(max(n) as double) / sum(n) as time_in_position
    from shape_counts
    group by canonical_match_key, canonical_player_key
),

line_breaking as (
    select
        canonical_match_key,
        canonical_player_key,
        count(*) filter (where line_break) as line_breaking_passes
    from {{ ref('stg__line_breaking_results') }}
    group by canonical_match_key, canonical_player_key
)

select
    p.canonical_match_key,
    p.canonical_player_key,
    pc.mean_controlled_fraction,
    sg.modal_position,
    sg.time_in_position,
    cast(coalesce(lb.line_breaking_passes, 0) as integer) as line_breaking_passes,
    p._visibility
from players as p
left join pitch_control as pc
    on pc.canonical_match_key = p.canonical_match_key and pc.canonical_player_key = p.canonical_player_key
left join shape as sg
    on sg.canonical_match_key = p.canonical_match_key and sg.canonical_player_key = p.canonical_player_key
left join line_breaking as lb
    on lb.canonical_match_key = p.canonical_match_key and lb.canonical_player_key = p.canonical_player_key
