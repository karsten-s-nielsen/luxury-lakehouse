{% macro extract_json_field(column, path, cast_type='string') %}
{#
    Extract a field from a JSON column using Databricks SQL syntax.

    Databricks uses colon notation for JSON path access:
      column:field          → top-level field
      column:field.subfield → nested field
      column:field[0]       → array element

    Args:
      column: The JSON column name
      path: Dot-notation path to the field (e.g. 'type.name')
      cast_type: SQL type to cast the result to (default: 'string')

    Example usage:
      {{ extract_json_field('type', 'name', 'string') }}
      → cast(type:name as string)
#}

    cast({{ column }}:{{ path }} as {{ cast_type }})

{% endmacro %}


{% macro extract_json_array_element(column, index, cast_type='double') %}
{#
    Extract an element from a JSON array column.

    Useful for splitting location arrays [x, y] into separate columns.

    Args:
      column: The JSON array column name
      index: Zero-based array index
      cast_type: SQL type to cast the result to (default: 'double')

    Example usage:
      {{ extract_json_array_element('location', 0, 'double') }} as location_x
      {{ extract_json_array_element('location', 1, 'double') }} as location_y
#}

    cast({{ column }}[{{ index }}] as {{ cast_type }})

{% endmacro %}


{% macro flatten_json_array(source_table, array_column, alias, schema=none) %}
{#
    Generate a LATERAL VIEW EXPLODE clause for flattening a JSON array.

    Databricks SQL approach for exploding nested JSON arrays into rows.

    Args:
      source_table: The source table alias
      array_column: The JSON array column to explode
      alias: Alias for the exploded column
      schema: Optional schema string for from_json (e.g. 'array<struct<x:double, y:double>>')

    Example usage:
      from source
      {{ flatten_json_array('source', 'lineup', 'player_element', 'array<struct<player_id:int, player_name:string>>') }}
#}

    {% if schema %}
        lateral view explode(from_json({{ source_table }}.{{ array_column }}, '{{ schema }}')) as {{ alias }}
    {% else %}
        lateral view explode({{ source_table }}.{{ array_column }}) as {{ alias }}
    {% endif %}

{% endmacro %}


{% macro scale_coordinates(x_col, y_col, source_system='percentage') %}
{#
    Scale coordinates from a source coordinate system to the standard 120x80 system.

    Supported source systems:
      - 'percentage': Wyscout (0-100) → multiply by 1.2 and 0.8
      - 'normalized': Metrica (0-1) → multiply by 120 and 80 (with y-axis flip)
      - 'statsbomb': Already 120x80 → no transformation

    Args:
      x_col: Column name for x coordinate
      y_col: Column name for y coordinate
      source_system: One of 'percentage', 'normalized', 'statsbomb'

    Example usage:
      {{ scale_coordinates('raw_x', 'raw_y', 'percentage') }}
      → returns two expressions for x and y
#}

    {% if source_system == 'percentage' %}
        ({{ x_col }} / 100.0) * {{ var('pitch_length') }} as {{ x_col }}_scaled,
        ({{ y_col }} / 100.0) * {{ var('pitch_width') }} as {{ y_col }}_scaled
    {% elif source_system == 'normalized' %}
        {{ x_col }} * {{ var('pitch_length') }} as {{ x_col }}_scaled,
        (1 - {{ y_col }}) * {{ var('pitch_width') }} as {{ y_col }}_scaled
    {% elif source_system == 'statsbomb' %}
        {{ x_col }} as {{ x_col }}_scaled,
        {{ y_col }} as {{ y_col }}_scaled
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown source_system: " ~ source_system ~ ". Use 'percentage', 'normalized', or 'statsbomb'.") }}
    {% endif %}

{% endmacro %}
