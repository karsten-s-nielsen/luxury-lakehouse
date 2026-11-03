{#- True when lake.main.<table> exists, or while dbt parses (then the model keeps its source() reference, so the
    lineage and the manifest name the bronze table). A provider whose module never ran has no bronze tables yet: its
    staging view then selects no rows with the same columns and types. -#}
{% macro lake_table_present(table) -%}
  {%- if not execute -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(adapter.get_relation(database='lake', schema='main', identifier=table) is not none) }}
{%- endmacro %}
