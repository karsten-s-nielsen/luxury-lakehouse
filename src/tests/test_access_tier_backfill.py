"""One-time backfill maps each existing provider to its policy default tier (spec 2026-06-29, Task 8b)."""

from __future__ import annotations

from ingestion.access_tier_backfill import (
    ALL_ACCESS_TIER_TABLES,
    BACKFILL_TABLES,
    DEFERRED_INERT_TRACKING_TABLES,
    EXISTING_PROVIDERS,
    MATCH_INFO_TABLES,
    build_backfill_statements,
    default_tier_for_provider,
)
from shared.access_tier import classify_access_tier


def test_default_tier_matches_the_classifier_except_confirmed_public_overrides() -> None:
    # No hand-encoding: for providers WITHOUT a confirmed-public override the backfill tier IS
    # classify_access_tier(provider, None). SkillCorner is the documented exception (override) — see below.
    for p in EXISTING_PROVIDERS:
        if p == "skillcorner":
            continue
        assert default_tier_for_provider(p) == classify_access_tier(provider=p, visibility=None).value


def test_skillcorner_existing_is_explicit_confirmed_public_override_not_the_classifier_default() -> None:
    # P1: the classifier now FAILS SAFE to restricted for skillcorner+None — but existing rows are the public
    # A-League, so the backfill encodes confirmed-public EXPLICITLY (the override), diverging from the classifier.
    assert classify_access_tier(provider="skillcorner", visibility=None).value == "restricted"
    assert default_tier_for_provider("skillcorner") == "public"


def test_gradientsports_defaults_restricted_others_public() -> None:
    assert default_tier_for_provider("gradientsports") == "restricted"
    for p in ["statsbomb", "wyscout", "idsse", "metrica", "skillcorner"]:
        assert default_tier_for_provider(p) == "public"


def test_every_access_tier_table_is_classified_exactly_once() -> None:
    # The inventory must PARTITION ALL_ACCESS_TIER_TABLES — every access_tier table is either backfilled here,
    # a match-info table (migration), or a deferred-inert tracking table. No overlaps, nothing unclassified.
    # (A new access_tier table added to one list without the others trips this — the silent-miss guard.)
    cats = [frozenset(BACKFILL_TABLES), frozenset(MATCH_INFO_TABLES), frozenset(DEFERRED_INERT_TRACKING_TABLES)]
    union = frozenset().union(*cats)
    assert union == ALL_ACCESS_TIER_TABLES
    assert sum(len(c) for c in cats) == len(ALL_ACCESS_TIER_TABLES)  # disjoint partition
    assert len(ALL_ACCESS_TIER_TABLES) == 10  # the live information_schema count (operator verifies against catalog)


def test_backfill_statements_are_null_gated_and_per_provider() -> None:
    stmts = build_backfill_statements(catalog="soccer_analytics", bronze_schema="bronze")
    # One statement per (table, provider).
    assert len(stmts) == len(BACKFILL_TABLES) * len(EXISTING_PROVIDERS)
    for s in stmts:
        # Idempotent by construction: every UPDATE is gated on the NULL-tier rows only.
        assert "WHERE access_tier IS NULL" in s
        assert "data_source = '" in s


def test_backfill_interpolates_derived_tier_never_hand_typed() -> None:
    stmts = build_backfill_statements()
    gs = [s for s in stmts if "data_source = 'gradientsports'" in s]
    assert gs and all("SET access_tier = 'restricted'" in s for s in gs)
    sb = [s for s in stmts if "data_source = 'statsbomb'" in s]
    assert sb and all("SET access_tier = 'public'" in s for s in sb)


def test_backfill_targets_per_action_per_frame_tables_only() -> None:
    # Match-info tables are re-ingested (R1), not NULL-backfilled — they must not appear here.
    assert "skillcorner_matches" not in BACKFILL_TABLES
    assert "gradientsports_metadata" not in BACKFILL_TABLES
    assert "spadl_actions" in BACKFILL_TABLES
    assert "spadl_action_context" in BACKFILL_TABLES
