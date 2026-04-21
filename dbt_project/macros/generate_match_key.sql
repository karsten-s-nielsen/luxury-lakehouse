{% macro generate_match_key(provider_col, native_match_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_matches` dimension.

    Deterministic 64-bit hash of (provider, native_match_id) using Spark's
    `xxhash64`, which returns a signed BIGINT matching PostgreSQL BIGINT
    semantics on Lakebase synced tables.

    Collision probability (birthday bound on a 64-bit hash):
      ~2.7e-12 at 10k matches per provider
      ~4.3e-11 across ~40k matches total (4 providers, shared keyspace)
    Comfortably below any operational threshold. If the dimension ever
    exceeds ~100M rows, revisit with xxhash128 or uuid_v5.

    The delimiter in `concat_ws` prevents concatenation ambiguities:
    (provider='ab', native='') would collide with (provider='a', native='b')
    without it. The '|' character is not present in any provider name or
    native ID format we ingest.

    `cast(... as string)` normalizes mixed-type natives: StatsBomb/Wyscout
    use BIGINT natively; IDSSE/Metrica use STRING. Both hash identically
    once stringified.

    Args:
      provider_col: Column name or expression for the provider identifier
                    (e.g. 'statsbomb', 'wyscout', 'idsse', 'metrica')
      native_match_id_col: Column name or expression for the provider's
                           native match ID (BIGINT or STRING)

    Returns:
      BIGINT surrogate key (signed 64-bit, Postgres BIGINT compatible)

    Reference: ADR-011 — Kimball surrogate keys for the conformed match
               dimension across StatsBomb / Wyscout / IDSSE / Metrica.
#}
    xxhash64(
        concat_ws(
            '|',
            {{ provider_col }},
            cast({{ native_match_id_col }} as string)
        )
    )
{% endmacro %}
