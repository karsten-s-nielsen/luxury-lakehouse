"""Pure truth table for the per-match access-tier policy (spec §4 / D2)."""

from __future__ import annotations

import pytest

from shared.access_tier import RESTRICTED_HF_PROVIDERS, AccessTier, classify_access_tier


@pytest.mark.parametrize(
    ("provider", "visibility", "expected"),
    [
        # Literal pining values (pining models.py — ^(public|private)$).
        ("skillcorner", "private", AccessTier.RESTRICTED),
        ("skillcorner", "public", AccessTier.PUBLIC),
        ("gradientsports", "private", AccessTier.RESTRICTED),
        ("gradientsports", "public", AccessTier.PUBLIC),
        # No feed -> provider default.
        ("gradientsports", None, AccessTier.RESTRICTED),  # in RESTRICTED_HF_PROVIDERS
        ("statsbomb", None, AccessTier.PUBLIC),
        ("wyscout", None, AccessTier.PUBLIC),
        ("idsse", None, AccessTier.PUBLIC),
        ("metrica", None, AccessTier.PUBLIC),
        ("skillcorner", None, AccessTier.PUBLIC),  # NOT in the default set (existing rows = public A-League)
        # Fail-safe: any unknown value -> RESTRICTED (D1).
        ("skillcorner", "embargoed", AccessTier.RESTRICTED),
        ("skillcorner", "", AccessTier.RESTRICTED),
    ],
)
def test_classify_access_tier(provider: str, visibility: str | None, expected: AccessTier) -> None:
    assert classify_access_tier(provider=provider, visibility=visibility) is expected


def test_enum_values_are_the_canonical_strings() -> None:
    assert AccessTier.PUBLIC.value == "public"
    assert AccessTier.RESTRICTED.value == "restricted"


def test_restricted_default_providers_is_frozenset_lowercase() -> None:
    assert isinstance(RESTRICTED_HF_PROVIDERS, frozenset)
    assert all(p == p.lower() for p in RESTRICTED_HF_PROVIDERS)
