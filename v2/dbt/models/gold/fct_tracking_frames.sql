-- Layer-0 input view for the tracking computes (spec D3; plan Task 3 + Task 3c): one row per player per frame,
-- canonical keys, positions, team, and `provider`. It stores no data. Velocity and goalkeeper resolution are derived
-- in the compute pass by silly-kicks (spec Constraints + D1/D8), never in SQL, so this view carries neither.
-- `provider` (spec D8 / ruling R6) tells the frame builder which per-match pitch size + roster source and which
-- `convert_to_frames` adapter to use. Tagged `compute_input` as the layer-0 marker the compute steps and the AI/ML
-- Workflows page read. `match_key` is the provider record key: the compute key reads filter on it as well as on
-- `canonical_match_key`, because only a `match_key` filter reaches the per-provider dedup windows (perf spec §7.4a).
{{ config(tags=['compute_input']) }}
select
    canonical_match_key,
    match_key,
    canonical_player_key,
    team_key,
    canonical_team_key,
    provider,
    period,
    frame,
    x_mm,
    y_mm,
    is_visible,
    _visibility
from {{ ref('tracking_cross_provider') }}
