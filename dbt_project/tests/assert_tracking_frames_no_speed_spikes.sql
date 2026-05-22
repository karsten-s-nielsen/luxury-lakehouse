-- assert_tracking_frames_no_speed_spikes.sql
-- Verifies that spike detection in fct_tracking_frames NULLs all speed_ms
-- values exceeding the threshold (default 15 m/s, the physical ceiling for
-- human players). SkillCorner coordinate teleportation produces speeds up to
-- 747 m/s on camera-switch frames; these must be NULLed, not emitted.
-- Returns rows that FAIL — 0 rows = all spikes cleaned.

select
    match_id,
    player_id,
    frame,
    speed_ms
from {{ ref('fct_tracking_frames') }}
where speed_ms > {{ var('speed_spike_threshold_ms', 15) }}
limit 10
