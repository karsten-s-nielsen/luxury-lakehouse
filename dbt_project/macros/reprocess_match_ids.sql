{#-
  Strand-safe per-match re-derive macros (ADR-043).

  Applied to the 7 TRIGGERED + incremental + match_id-filtered "D" marts so an
  operator re-derive MERGEs changed matches (CDF partial-update, synced table
  keeps its streaming checkpoint) instead of needing a strand-inducing
  --full-refresh. Daily builds are unchanged: every macro is a no-op unless
  var('reprocess_match_ids') is set.

  See docs/superpowers/specs/2026-06-09-strand-safe-synced-rederive-design.md.
-#}

{%- macro reprocess_match_ids_list() -%}
  {#- var('reprocess_match_ids') coerced to a list[int] (injection-safe). [] if unset.
      m2: tolerate an operator passing a scalar (--vars '{reprocess_match_ids: 5}') by wrapping it. -#}
  {%- set raw = var('reprocess_match_ids', none) -%}
  {%- if raw is none -%}
    {{ return([]) }}
  {%- endif -%}
  {%- if raw is string or raw is number -%}
    {{ return([raw | int]) }}
  {%- endif -%}
  {{ return(raw | map('int') | list) }}
{%- endmacro -%}


{%- macro reprocess_predicate(match_col='match_id') -%}
  {#-
    OR-include clause that re-admits reprocessed matches into an incremental SELECT.

    MUST be placed INSIDE parentheses that wrap the existing `not in (...)` filter,
    so the OR scopes ONLY to the match-exclusion and cannot defeat sibling AND
    predicates (e.g. `where player_id is not null and (<not in> <predicate>)`):

        where (match_id not in (select ...) {{ reprocess_predicate('match_id') }})

    Safety net (ADR-043 / review N1): if the reprocess_delete_hook DELETE is not yet
    visible to this SELECT (commit-ordering), this OR still re-includes the match, so
    a deleted match is never left un-reinserted. Renders empty when no reprocess ids.
  -#}
  {%- set ids = reprocess_match_ids_list() -%}
  {%- if ids | length > 0 -%}
    or {{ match_col }} in ({{ ids | join(', ') }})
  {%- endif -%}
{%- endmacro -%}


{%- macro reprocess_delete_hook(match_col='match_id') -%}
  {#-
    Model pre_hook. Deletes the reprocessed matches up-front so a re-derive that
    DROPS rows cannot orphan them (a pure MERGE never deletes), and a surrogate-key
    shift (e.g. time_seconds change) cannot strand the old key. reprocess_predicate
    re-inserts fresh rows in the same run. No-op unless incremental AND ids are set
    (so first-build, full-refresh, and daily runs never hit it — {{ this }} may not
    exist on first build, which is why is_incremental() guards it).
  -#}
  {%- set ids = reprocess_match_ids_list() -%}
  {%- if execute and is_incremental() and ids | length > 0 -%}
    delete from {{ this }} where {{ match_col }} in ({{ ids | join(', ') }})
  {%- endif -%}
{%- endmacro -%}
