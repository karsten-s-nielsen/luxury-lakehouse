{% macro normalize_coordinates() %}
{#
    Coordinate normalization macros for multi-provider pitch data.

    All providers are normalized to the StatsBomb coordinate system:
      - Pitch: 120 x 80 yards (origin at top-left)
      - x: 0 = own goal line, 120 = opponent goal line
      - y: 0 = right touchline, 80 = left touchline

    Supported coordinate systems:

      'metrica'   — Metrica Sports: [0, 1] normalized, y-axis flipped
                    x → x * pitch_length,  y → (1 - y) * pitch_width

      'center_m'  — IDSSE / SkillCorner: center-origin meters
                    x ∈ [-52.5, 52.5], y ∈ [-34, 34]
                    x → (x + L/2) / L * pitch_length
                    y → (y + W/2) / W * pitch_width

      'pitch_m'   — Pitch-origin meters (0–105, 0–68)
                    x → x / L * pitch_length
                    y → y / W * pitch_width

      'pct'       — Percentage [0, 100]
                    x → x / 100 * pitch_length
                    y → y / 100 * pitch_width

    Where L = pitch_length_m (105), W = pitch_width_m (68).

    Usage:
      {{ normalize_x('location_x', 'metrica') }} as location_x,
      {{ normalize_y('location_y', 'metrica') }} as location_y

    Reference: docs/coordinate-systems.md
#}
{% endmacro %}


{% macro normalize_x(x_col, system) %}
{#
    Normalize an x coordinate to the StatsBomb 120-yard scale.

    Args:
      x_col:  Column name or expression for the x coordinate
      system: One of 'metrica', 'center_m', 'pitch_m', 'pct'

    Returns:
      SQL expression producing x in StatsBomb coordinates (0–120)
#}

    {% if system == 'metrica' %}
    ({{ x_col }} * {{ var('pitch_length') }})
    {% elif system == 'center_m' %}
    (({{ x_col }} + {{ var('pitch_length_m') }} / 2.0) / {{ var('pitch_length_m') }} * {{ var('pitch_length') }})
    {% elif system == 'pitch_m' %}
    ({{ x_col }} / {{ var('pitch_length_m') }} * {{ var('pitch_length') }})
    {% elif system == 'pct' %}
    ({{ x_col }} / 100.0 * {{ var('pitch_length') }})
    {% else %}
    {{ exceptions.raise_compiler_error("normalize_x: unknown coordinate system '" ~ system ~ "'. Expected one of: metrica, center_m, pitch_m, pct") }}
    {% endif %}

{% endmacro %}


{% macro normalize_y(y_col, system) %}
{#
    Normalize a y coordinate to the StatsBomb 80-yard scale.

    Args:
      y_col:  Column name or expression for the y coordinate
      system: One of 'metrica', 'center_m', 'pitch_m', 'pct'

    Returns:
      SQL expression producing y in StatsBomb coordinates (0–80)
#}

    {% if system == 'metrica' %}
    ((1.0 - {{ y_col }}) * {{ var('pitch_width') }})
    {% elif system == 'center_m' %}
    (({{ y_col }} + {{ var('pitch_width_m') }} / 2.0) / {{ var('pitch_width_m') }} * {{ var('pitch_width') }})
    {% elif system == 'pitch_m' %}
    ({{ y_col }} / {{ var('pitch_width_m') }} * {{ var('pitch_width') }})
    {% elif system == 'pct' %}
    ({{ y_col }} / 100.0 * {{ var('pitch_width') }})
    {% else %}
    {{ exceptions.raise_compiler_error("normalize_y: unknown coordinate system '" ~ system ~ "'. Expected one of: metrica, center_m, pitch_m, pct") }}
    {% endif %}

{% endmacro %}
