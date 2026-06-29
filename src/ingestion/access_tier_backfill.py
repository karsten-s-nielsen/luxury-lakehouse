"""One-time backfill of ``access_tier`` on EXISTING bronze rows (spec 2026-06-29, BLOCKER 1).

``ALTER TABLE ... ADD COLUMNS (access_tier STRING)`` leaves every historical row
``access_tier = NULL``; ingestion stamps only NEW pulls. Without a backfill the marts rebuild
all-NULL for the public providers, the publish-time fail-safe routes NULL→restricted, and every
public publish hard-fails / the public datasets empty. This module backfills existing bronze rows
to their **provider default** tier.

The per-provider default is DERIVED from the pure policy core (``classify_access_tier``) — never
hand-encoded in SQL — so the backfill and the live ingestion stamp can never disagree. Existing
rows have no per-match ``visibility`` signal (the column was just added), so the no-feed provider
default is exactly the right value for them (existing SkillCorner = the public A-League matches;
GradientSports = restricted).

Idempotent by construction: every UPDATE is gated ``WHERE access_tier IS NULL``.

NOTE (R1, the Task 7 ↔ 8b contract): the **match-info** bronze tables
(``skillcorner_matches`` / ``gradientsports_metadata``) are NOT NULL-backfilled here — they are
re-ingested so each row carries its REAL ``visibility``. This module owns only the
per-action / per-frame fact tables that carry a ``data_source`` column.
"""

from __future__ import annotations

from shared.access_tier import classify_access_tier

# Providers with EXISTING bronze rows that carry a ``data_source`` column.
EXISTING_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Per-action / per-frame bronze fact tables that carry a ``data_source`` column and whose rows must
# be backfilled to the provider default. (Match-info tables are re-ingested, not backfilled — R1.)
BACKFILL_TABLES: tuple[str, ...] = (
    "spadl_actions",
    "vaep_action_values",
    "spadl_action_context",
)


def default_tier_for_provider(provider: str) -> str:
    """Provider-default ``access_tier`` for a no-feed (existing) row.

    DERIVED from the policy core — the backfill default for a no-visibility-signal row IS
    ``classify_access_tier(provider, visibility=None)``. No hand-encoded policy here.
    """
    return classify_access_tier(provider=provider, visibility=None).value


def build_backfill_statements(
    *,
    catalog: str = "soccer_analytics",
    bronze_schema: str = "bronze",
    tables: tuple[str, ...] = BACKFILL_TABLES,
    providers: tuple[str, ...] = EXISTING_PROVIDERS,
) -> list[str]:
    """Build the idempotent ``UPDATE`` statements (one per (table, provider)).

    Each statement is gated ``WHERE access_tier IS NULL AND data_source = '<provider>'`` and
    string-interpolates the *derived* tier (never a hand-typed literal). Re-runnable: once a row
    is populated it no longer matches ``access_tier IS NULL``.
    """
    statements: list[str] = []
    for table in tables:
        fq = f"{catalog}.{bronze_schema}.{table}"
        for provider in providers:
            tier = default_tier_for_provider(provider)
            # `tier` is a classifier-derived literal ("public"/"restricted") and `provider` is from
            # the EXISTING_PROVIDERS allowlist — no user-supplied input (S608 not applicable).
            where = f"WHERE access_tier IS NULL AND data_source = '{provider}'"
            statements.append(f"UPDATE {fq} SET access_tier = '{tier}' {where}")  # noqa: S608
    return statements
