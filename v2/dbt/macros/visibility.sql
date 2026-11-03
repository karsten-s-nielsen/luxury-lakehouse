{#- True when lake.main.<table> has column <column>. False while dbt parses (the column is not a source() ref, so
    parse-time lineage is unaffected) and when the table is absent. Used to carry the encrypted-local `_visibility`
    audit column (plan T9/T11) only when the bronze table actually has it. -#}
{% macro lake_column_present(table, column) -%}
  {%- if not execute -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set relation = adapter.get_relation(database='lake', schema='main', identifier=table) -%}
  {%- if relation is none -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set names = [] -%}
  {%- for col in adapter.get_columns_in_relation(relation) -%}
    {%- do names.append(col.name) -%}
  {%- endfor -%}
  {{ return(column in names) }}
{%- endmacro %}


{#- The `_visibility` column for a staging model over a bronze table. On an encrypted-local lake the bronze table
    carries `_visibility` (the authoritative restricted copy, plan T9), so the staging view carries it through; on
    an unencrypted-local or HF lake, or while the table is absent, it emits `cast(null as varchar) as _visibility`.
    The column is therefore ALWAYS present with the same name and type, so the cross-provider `union all` stays
    schema-consistent and the gold contract (which lists `_visibility`) holds in every mode. A null on HF is safe:
    tracking is operator-only (not in `ANON_VIEWABLE_ROUTES`), and the read seam shows an operator every row. -#}
{% macro bronze_visibility_column(table) -%}
  {%- if lake_column_present(table, '_visibility') -%}
    _visibility
  {%- else -%}
    cast(null as varchar) as _visibility
  {%- endif -%}
{%- endmacro %}
