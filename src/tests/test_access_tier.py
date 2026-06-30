"""Pure truth table for the per-match access-tier policy (spec §4 / D2)."""

from __future__ import annotations

import pytest

from shared.access_tier import (
    PUBLIC_BY_LICENSE_PROVIDERS,
    RESTRICTED_HF_PROVIDERS,
    AccessTier,
    classify_access_tier,
)


@pytest.mark.parametrize(
    ("provider", "visibility", "expected"),
    [
        # Literal pining values (pining models.py — ^(public|private)$).
        ("skillcorner", "private", AccessTier.RESTRICTED),
        ("skillcorner", "public", AccessTier.PUBLIC),
        ("gradientsports", "private", AccessTier.RESTRICTED),
        ("gradientsports", "public", AccessTier.PUBLIC),
        # No feed -> allowlist (P1): open-data providers public, everything else fail-safe restricted.
        ("statsbomb", None, AccessTier.PUBLIC),
        ("wyscout", None, AccessTier.PUBLIC),
        ("idsse", None, AccessTier.PUBLIC),
        ("metrica", None, AccessTier.PUBLIC),
        ("gradientsports", None, AccessTier.RESTRICTED),  # visibility-feed provider, no signal -> restricted
        ("skillcorner", None, AccessTier.RESTRICTED),  # H1.1: mixed-license, no signal -> FAIL SAFE (was PUBLIC)
        # P1: an UNKNOWN/new provider with no signal must fail safe to RESTRICTED (the leak that a denylist left open).
        ("a_new_unclassified_provider", None, AccessTier.RESTRICTED),
        ("a_new_unclassified_provider", "public", AccessTier.PUBLIC),  # explicit public signal still honoured
        # Fail-safe: any unknown visibility value -> RESTRICTED (D1).
        ("skillcorner", "embargoed", AccessTier.RESTRICTED),
        ("skillcorner", "", AccessTier.RESTRICTED),
    ],
)
def test_classify_access_tier(provider: str, visibility: str | None, expected: AccessTier) -> None:
    assert classify_access_tier(provider=provider, visibility=visibility) is expected


def test_enum_values_are_the_canonical_strings() -> None:
    assert AccessTier.PUBLIC.value == "public"
    assert AccessTier.RESTRICTED.value == "restricted"


def test_public_by_license_allowlist_is_the_open_data_providers() -> None:
    # The no-signal default is an ALLOWLIST (P1). If a provider is added here it becomes public-by-default —
    # that is a deliberate, reviewed act, never an accident of omission.
    assert PUBLIC_BY_LICENSE_PROVIDERS == frozenset({"statsbomb", "wyscout", "idsse", "metrica"})
    assert all(p == p.lower() for p in PUBLIC_BY_LICENSE_PROVIDERS)


def test_restricted_default_providers_is_frozenset_lowercase() -> None:
    assert isinstance(RESTRICTED_HF_PROVIDERS, frozenset)
    assert all(p == p.lower() for p in RESTRICTED_HF_PROVIDERS)
