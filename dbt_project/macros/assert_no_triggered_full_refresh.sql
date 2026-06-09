{%- macro assert_no_triggered_full_refresh() -%}
  {#-
    on-run-start tripwire (ADR-043). UNIFIED RULE (evidence-driven): abort iff a --full-refresh build
    selects a TRIGGERED synced source. Live evidence (strand ledger + DESCRIBE HISTORY, 2026-06-09):
    routine builds (incremental MERGE; table CREATE OR REPLACE) do NOT strand — only --full-refresh
    does, and the two `table` marts are DAILY output_marts, so an "abort any table build" rule would
    abort the production stage-3 build. Escaped only by --vars '{allow_triggered_full_refresh: true}',
    which ONLY scripts/rederive_synced_marts.py's B path passes.
  -#}
  {%- if not execute or not flags.FULL_REFRESH -%}{{ return('') }}{%- endif -%}
  {%- if var('allow_triggered_full_refresh', false) == true -%}{{ return('') }}{%- endif -%}
  {%- set triggered = var('triggered_synced_marts', []) -%}   {#- flat list of dbt model names -#}
  {%- set hit = [] -%}
  {%- for uid in selected_resources -%}                        {#- N-a: on-run-start exposes unique_ids (dbt >=1.5) -#}
    {%- set node = graph.nodes.get(uid) -%}
    {%- set name = node.name if node else uid.split('.')[-1] -%}
    {%- if name in triggered -%}{%- do hit.append(name) -%}{%- endif -%}
  {%- endfor -%}
  {%- if hit | length > 0 -%}
    {%- do exceptions.raise_compiler_error(
        "Refusing --full-refresh of TRIGGERED synced source(s) " ~ (hit | join(', ')) ~
        " — it strands the Lakebase synced table. Use "
        "`uv run --extra sdk python scripts/rederive_synced_marts.py --select <selector>` (strand-safe). "
        "Tool-only override: --vars '{allow_triggered_full_refresh: true}'.") -%}
  {%- endif -%}
{%- endmacro -%}
