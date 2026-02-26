{% macro distance_to_goal(x_col, y_col) %}
{#
    Calculate Euclidean distance from a shot/event location to the center of the goal.

    Uses the StatsBomb coordinate system:
      - Pitch: 120 x 80 yards
      - Goal center: (120, 40) — center of the goal line on the attacking end

    Formula:
      distance = sqrt((goal_x - x)^2 + (goal_y - y)^2)

    Args:
      x_col: Column name or expression for the x coordinate (0-120)
      y_col: Column name or expression for the y coordinate (0-80)

    Returns:
      Distance in yards (same unit as the coordinate system)

    Example usage:
      {{ distance_to_goal('location_x', 'location_y') }} as distance_to_goal

    Reference: Soccermatics Chapter 02 — Expected Goals modeling
#}

    sqrt(
        power({{ var('goal_x') }} - {{ x_col }}, 2)
        + power({{ var('goal_y') }} - {{ y_col }}, 2)
    )

{% endmacro %}
