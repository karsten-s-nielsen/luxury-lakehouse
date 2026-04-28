{#
    ensure_bronze_columns
    =====================

    Idempotently adds new columns to a bronze Delta table when they're
    missing — bridges the chicken-and-egg between a Python writer that
    introduces new columns via Spark ``mergeSchema=true`` and a downstream
    dbt model that selects them by name.

    Without this, a freshly-cut PR that adds bronze columns AND the
    downstream dbt projection in one commit fails CI's ``dbt build`` against
    the live (pre-write) bronze table with
    ``UNRESOLVED_COLUMN.WITH_SUGGESTION``.

    Usage (as a ``pre_hook`` on a staging model):

        {{ config(
            pre_hook=ensure_bronze_columns(
                source('spadl', 'vaep_action_values'),
                [
                    ('statsbomb_possession_id', 'BIGINT'),
                    ('statsbomb_possession_team_id', 'BIGINT'),
                    ('statsbomb_play_pattern', 'STRING'),
                    ('statsbomb_under_pressure', 'BOOLEAN'),
                ]
            )
        ) }}

    Returns a list of ``ALTER TABLE ... ADD COLUMNS (...)`` statements,
    each guarded against re-execution by introspecting the table's existing
    columns first. Empty list (no-op) when all columns already exist.

    Implementation notes:

    - ``adapter.get_columns_in_relation`` returns ``None`` during dbt parse
      (the relation may not be resolved yet). The ``execute`` guard yields
      an empty list during parse and only emits ALTERs at execute time.
    - Column-name comparison is case-insensitive (Databricks default).
    - Single ``ADD COLUMNS (...)`` clause per call (atomic schema bump).
#}

{% macro ensure_bronze_columns(relation, columns_to_add) %}
    {%- if execute -%}
        {%- set existing_cols = adapter.get_columns_in_relation(relation) -%}
        {%- if existing_cols is none -%}
            {%- do return([]) -%}
        {%- endif -%}
        {%- set existing_names = existing_cols | map(attribute='name') | map('lower') | list -%}
        {%- set missing = [] -%}
        {%- for name, dtype in columns_to_add -%}
            {%- if name | lower not in existing_names -%}
                {%- do missing.append(name ~ ' ' ~ dtype) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if missing | length > 0 -%}
            {%- set ddl = 'ALTER TABLE ' ~ relation ~ ' ADD COLUMNS (' ~ (missing | join(', ')) ~ ')' -%}
            {%- do return([ddl]) -%}
        {%- else -%}
            {%- do return([]) -%}
        {%- endif -%}
    {%- else -%}
        {%- do return([]) -%}
    {%- endif -%}
{% endmacro %}
