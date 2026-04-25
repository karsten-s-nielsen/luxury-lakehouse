{% macro generate_player_key(provider_col, native_player_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_players` dimension.

    Deterministic 64-bit hash of (provider, native_player_id) via Spark's
    `xxhash64`. Mirrors generate_team_key / generate_match_key /
    generate_competition_key.

    Unifies StatsBomb (INT player_id) + Wyscout (INT wyId) + IDSSE
    (DFL PersonId STRING) + Metrica (synthesised STRING
    'metrica_<match>_<side>_<key>').

    The delimiter in `concat_ws` prevents concatenation ambiguities.
    `cast(... as string)` normalizes mixed-type natives.

    Args:
      provider_col: Column name or expression for the provider identifier.
      native_player_id_col: Column name or expression for the provider's
                             native player ID (BIGINT or STRING).

    Returns:
      BIGINT surrogate key. NULL when native_player_id is NULL.

    Reference: ADR-011 — Kimball surrogate keys; PR 5a extends pattern
               from dim_matches/dim_competitions to dim_players.
#}
    case
        when {{ native_player_id_col }} is null then null
        else xxhash64(
            concat_ws(
                '|',
                {{ provider_col }},
                cast({{ native_player_id_col }} as string)
            )
        )
    end
{% endmacro %}
