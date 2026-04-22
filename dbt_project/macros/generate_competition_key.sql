{% macro generate_competition_key(provider_col, native_competition_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_competitions` dimension.

    Deterministic 64-bit hash of (provider, native_competition_id) via
    Spark's `xxhash64`, returning a signed BIGINT compatible with
    PostgreSQL BIGINT semantics on Lakebase synced tables.

    Mirrors `generate_match_key` (same xxhash64 + concat_ws('|') pattern)
    so behaviour, collision bounds, and rationale are identical. Used
    to unify StatsBomb + Wyscout (INT native competition_ids stringified)
    with IDSSE ('DFL-COM-000001' etc.) and Metrica (NULL, yielding NULL
    key). See ADR-011 for the broader surrogate-key rationale; this
    macro extends it from matches to competitions.

    The delimiter in `concat_ws` prevents concatenation ambiguities:
    (provider='ab', native='') would collide with (provider='a', native='b')
    without it. The '|' character is not present in any provider name or
    native competition ID format we ingest.

    `cast(... as string)` normalizes mixed-type natives: StatsBomb and
    Wyscout use BIGINT competition_ids natively; IDSSE uses STRING
    ('DFL-COM-XXXXXX'). Both hash identically once stringified.

    Args:
      provider_col: Column name or expression for the provider identifier
                    (e.g. 'statsbomb', 'wyscout', 'idsse', 'metrica')
      native_competition_id_col: Column name or expression for the
                                 provider's native competition ID
                                 (BIGINT or STRING)

    Returns:
      BIGINT surrogate key. NULL when native_competition_id is NULL
      (e.g. Metrica, which has no competition metadata in open-data).

    Reference: ADR-011 — Kimball surrogate keys for conformed dimensions
               across StatsBomb / Wyscout / IDSSE / Metrica; extended
               from dim_matches to dim_competitions in PR 2.
#}
    case
        when {{ native_competition_id_col }} is null then null
        else xxhash64(
            concat_ws(
                '|',
                {{ provider_col }},
                cast({{ native_competition_id_col }} as string)
            )
        )
    end
{% endmacro %}
