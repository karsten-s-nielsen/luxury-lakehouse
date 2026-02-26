{% macro shot_angle(x_col, y_col) %}
{#
    Calculate the angle subtended by the goal posts from the shot location.

    Uses arctan geometry to compute the visible angle of the goal from
    the shooter's perspective. A wider angle means the shooter can "see"
    more of the goal, which correlates with higher xG.

    Goal post positions (StatsBomb 120x80 coordinate system):
      - Left post:  (120, 36)   — goal center at y=40, goal width = 8 yards
      - Right post: (120, 44)   — so posts are at y = 40 +/- 4

    Geometry (from Soccermatics Chapter 02):
      The angle is calculated using the formula for the angle at point P
      between two lines to the goal posts:

        angle = arctan((goal_width * dx) / (dx^2 + dy^2 - (goal_width/2)^2))

      Where:
        dx = goal_x - shot_x   (distance to goal line)
        dy = goal_y - shot_y   (lateral offset from goal center)
        goal_width = 8 yards   (standard goal width)

      This handles the edge case where the shooter is directly in front
      of goal (dy = 0) and at extreme angles near the touchline.

    Args:
      x_col: Column name or expression for the x coordinate (0-120)
      y_col: Column name or expression for the y coordinate (0-80)

    Returns:
      Shot angle in radians (0 to ~pi). Convert to degrees with * 180 / pi.

    Example usage:
      {{ shot_angle('location_x', 'location_y') }} as shot_angle_rad

    Reference: Soccermatics by David Sumpter, Chapter 02
               "The Geometry of Shooting"
#}

    {%- set goal_width = 8 -%}
    {%- set half_goal_width = 4 -%}

    abs(
        atan2(
            {{ goal_width }} * ({{ var('goal_x') }} - {{ x_col }}),
            power({{ var('goal_x') }} - {{ x_col }}, 2)
            + power({{ var('goal_y') }} - {{ y_col }}, 2)
            - power({{ half_goal_width }}, 2)
        )
    )

{% endmacro %}
