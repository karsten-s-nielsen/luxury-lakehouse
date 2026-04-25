{% macro generate_team_key(provider_col, native_team_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_teams` dimension.

    Deterministic 64-bit hash of (provider, native_team_id) via Spark's
    `xxhash64`, returning a signed BIGINT compatible with PostgreSQL BIGINT
    semantics on Lakebase synced tables.

    Mirrors `generate_match_key` + `generate_competition_key` (same xxhash64 +
    concat_ws('|') pattern) so behaviour, collision bounds, and rationale are
    identical. Unifies StatsBomb + Wyscout (INT native team_ids stringified)
    with IDSSE DFL TeamIds ('DFL-CLU-XXXXXX') and Metrica synthesised IDs
    ('metrica_Sample_Game_1_home', etc.).

    The delimiter in `concat_ws` prevents concatenation ambiguities:
    (provider='ab', native='') would collide with (provider='a', native='b')
    without it. The '|' character is not present in any provider name or
    native team ID format we ingest.

    `cast(... as string)` normalizes mixed-type natives: StatsBomb/Wyscout
    use BIGINT natively; IDSSE/Metrica use STRING. Both hash identically
    once stringified.

    Args:
      provider_col: Column name or expression for the provider identifier
                    (e.g. 'statsbomb', 'wyscout', 'idsse', 'metrica')
      native_team_id_col: Column name or expression for the provider's
                          native team ID (BIGINT or STRING)

    Returns:
      BIGINT surrogate key. NULL when native_team_id is NULL.

    Reference: ADR-011 — Kimball surrogate keys for conformed dimensions
               across StatsBomb / Wyscout / IDSSE / Metrica; extended
               from dim_matches to dim_teams in PR 5a.
#}
    case
        when {{ native_team_id_col }} is null then null
        else xxhash64(
            concat_ws(
                '|',
                {{ provider_col }},
                cast({{ native_team_id_col }} as string)
            )
        )
    end
{% endmacro %}
