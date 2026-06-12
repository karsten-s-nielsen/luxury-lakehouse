-- gk_tracking_geometry.sql
-- Orientation reconciliation between ghost_gk_* (canonical, defended goal at x~0) and
-- pre_shot_gk_* / defensive_line_x (frame-oriented; defended end varies by team/period).
-- ANCHOR (cross-session review H3, 2026-06-11): the defended goal is identified from the STORED
-- pre_shot_gk_distance_to_goal — whichever end's distance residual matches the stored value.
-- Exact for every GK position incl. sweeping keepers (the naive |dx|>52.5 rule mis-mirrors a
-- GK at frame x~60 by ~15 m). The positional rule survives ONLY as the residual-tie tiebreak.
-- REVISIT when the upstream AC coordinate convention is unified — this macro is the single
-- change site. See ADR-051 section 4.

{% macro _gk_dist_residual(frame_x, frame_y, dist_to_goal, goal_x) %}
    abs(sqrt(pow({{ frame_x }} - {{ goal_x }}, 2) + pow({{ frame_y }} - 34.0, 2)) - {{ dist_to_goal }})
{% endmacro %}

{% macro gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) %}
    (case
        when {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '105.0') }}
           < {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '0.0') }} then true
        when {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '0.0') }}
           < {{ _gk_dist_residual(frame_x, frame_y, dist_to_goal, '105.0') }} then false
        else {{ frame_x }} > 52.5  -- residual tie (degenerate midfield case): positional tiebreak
    end)
{% endmacro %}

{% macro gk_actual_canonical_x(frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 105.0 - {{ frame_x }} else {{ frame_x }} end)
{% endmacro %}

{% macro gk_actual_canonical_y(frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 68.0 - {{ frame_y }} else {{ frame_y }} end)
{% endmacro %}

{% macro gk_line_height_m(defensive_line_x, frame_x, frame_y, dist_to_goal) %}
    (case when {{ gk_frame_mirror_flag(frame_x, frame_y, dist_to_goal) }}
          then 105.0 - {{ defensive_line_x }} else {{ defensive_line_x }} end)
{% endmacro %}
