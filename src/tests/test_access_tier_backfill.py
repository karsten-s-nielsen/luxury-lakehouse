"""One-time backfill maps each existing provider to its policy default tier (spec 2026-06-29, Task 8b)."""

from __future__ import annotations

from ingestion.access_tier_backfill import (
    BACKFILL_TABLES,
    EXISTING_PROVIDERS,
    build_backfill_statements,
    default_tier_for_provider,
)
from shared.access_tier import classify_access_tier


def test_default_tier_matches_the_classifier() -> None:
    # No hand-encoding: the backfill default for a no-feed provider IS classify_access_tier(provider, None).
    for p in EXISTING_PROVIDERS:
        assert default_tier_for_provider(p) == classify_access_tier(provider=p, visibility=None).value


def test_gradientsports_defaults_restricted_others_public() -> None:
    assert default_tier_for_provider("gradientsports") == "restricted"
    for p in ["statsbomb", "wyscout", "idsse", "metrica", "skillcorner"]:
        # existing SkillCorner rows are the public A-League — provider default public.
        assert default_tier_for_provider(p) == "public"


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
