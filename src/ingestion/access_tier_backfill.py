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

from shared.access_tier import AccessTier, classify_access_tier

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
# be backfilled to the provider tier. ``psxg_tracking_predictions`` is data_source-keyed too.
BACKFILL_TABLES: tuple[str, ...] = (
    "spadl_actions",
    "vaep_action_values",
    "spadl_action_context",
    "psxg_tracking_predictions",
)

# Single-provider match-info tables (NO ``data_source`` column; also carry ``visibility``). Handled by a
# dedicated operator migration (premise-asserted ``visibility='public'`` for the confirmed A-League), NOT the
# data_source loop here.
MATCH_INFO_TABLES: tuple[str, ...] = ("skillcorner_matches", "gradientsports_metadata")

# Single-provider raw tracking tables. NO mart reads their per-row ``access_tier`` (the marts resolve tier from
# ``dim_matches``), so their NULLs are functionally inert. Operator-DEFERRED (a ~300M-row rewrite for nothing read);
# going-forward ingestion stamps new rows. Listed here so they are a CONSCIOUS exclusion, never a silent miss.
DEFERRED_INERT_TRACKING_TABLES: tuple[str, ...] = (
    "skillcorner_tracking",
    "idsse_tracking",
    "metrica_tracking",
    "gradientsports_tracking",
)

# Complete inventory of EVERY bronze table carrying ``access_tier``. Every table here must fall into exactly one
# category above. The completeness test asserts this partition + that it equals the live information_schema set
# (the operator runs the live check) — so a NEW access_tier table forces a conscious backfill/defer decision and can
# never be silently dropped (the gap that motivated review).
ALL_ACCESS_TIER_TABLES: frozenset[str] = (
    frozenset(BACKFILL_TABLES) | frozenset(MATCH_INFO_TABLES) | frozenset(DEFERRED_INERT_TRACKING_TABLES)
)

# Providers whose no-signal DEFAULT is now RESTRICTED (P1 allowlist flip) but whose EXISTING bronze is confirmed
# public by inspection. SkillCorner: the existing rows are the public A-League (competition 61, verified 2026-06-30);
# no private Real Madrid match is ingested yet. The backfill must encode this CONFIRMED fact explicitly — it can NOT
# derive it from ``classify_access_tier(skillcorner, None)``, which (correctly) now fails safe to restricted. NEW
# SkillCorner rows get their real per-match ``visibility`` at ingest, never this override.
_EXISTING_CONFIRMED_PUBLIC: dict[str, str] = {"skillcorner": AccessTier.PUBLIC.value}


def default_tier_for_provider(provider: str) -> str:
    """``access_tier`` for an EXISTING no-signal bronze row of ``provider``.

    For most providers this IS ``classify_access_tier(provider, None)`` (no hand-encoding). For a provider in
    ``_EXISTING_CONFIRMED_PUBLIC`` (skillcorner) it is the explicit confirmed-public override — because the P1
    allowlist flip makes the classifier default restricted, but the existing rows are verified public A-League.
    """
    if provider in _EXISTING_CONFIRMED_PUBLIC:
        return _EXISTING_CONFIRMED_PUBLIC[provider]
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
