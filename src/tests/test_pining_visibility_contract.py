"""Cross-repo contract: the pining `visibility` vocabulary maps explicitly under the access-tier policy.

This is the lakehouse-side mirror of pining's producer-side schema test (spec C2 / Task 20). It guards
the seam between an EXTERNAL signal (the pining-for-the-data `/skillcorner/matches` discovery endpoint)
and our redistribution policy (`shared.access_tier.classify_access_tier`):

  * EVERY `visibility` value the API returns is in the closed vocabulary ``{"public", "private"}``;
  * the policy maps each KNOWN value EXPLICITLY (``public -> PUBLIC``, ``private -> RESTRICTED``);
  * any UNRECOGNISED value fail-safes to ``RESTRICTED`` and can NEVER route to ``PUBLIC`` (spec D1) —
    so a new producer-side enum value cannot silently leak before this contract is updated.

The hermetic variant runs everywhere off a recorded fixture. The live variant (``PINING_LIVE_CONTRACT``)
hits the real API with the owner token and re-asserts the vocabulary against the production feed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from shared.access_tier import AccessTier, classify_access_tier

_FIXTURE = Path(__file__).parent / "fixtures" / "pining" / "skillcorner_matches_response.json"

# The closed vocabulary pining's canonical model pins (models.py:60 — ``pattern=r"^(public|private)$"``).
# A value outside this set is a producer-side schema change that MUST be reviewed before it can flow.
_PINING_VISIBILITY_VOCABULARY = frozenset({"public", "private"})

# The explicit, audited mapping the policy MUST honour for every KNOWN value.
_EXPECTED_TIER_BY_VISIBILITY = {
    "public": AccessTier.PUBLIC,
    "private": AccessTier.RESTRICTED,
}


def _load_recorded_visibilities() -> list[str]:
    """Return the raw ``visibility`` strings from the recorded /matches response (parsed via MatchInfo)."""
    from ingestion.skillcorner_common import MatchInfo

    raw = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    matches = [MatchInfo.model_validate(m) for m in raw["matches"]]
    assert matches, "recorded fixture must contain at least one match"
    return [m.visibility for m in matches]


def test_recorded_fixture_visibilities_are_in_the_closed_vocabulary() -> None:
    for visibility in _load_recorded_visibilities():
        assert visibility in _PINING_VISIBILITY_VOCABULARY, (
            f"pining returned visibility={visibility!r} outside {sorted(_PINING_VISIBILITY_VOCABULARY)} — "
            "producer-side schema change; update the policy + this contract before it flows"
        )


def test_recorded_fixture_covers_both_visibility_values() -> None:
    # The fixture must exercise BOTH branches so a regression on either mapping is caught hermetically.
    seen = set(_load_recorded_visibilities())
    assert seen == _PINING_VISIBILITY_VOCABULARY, (
        f"fixture must record both public AND private to exercise both policy branches; saw {sorted(seen)}"
    )


def test_classifier_maps_each_recorded_visibility_explicitly() -> None:
    for visibility in _load_recorded_visibilities():
        expected = _EXPECTED_TIER_BY_VISIBILITY[visibility]
        actual = classify_access_tier(provider="skillcorner", visibility=visibility)
        assert actual is expected, f"visibility={visibility!r} must map to {expected}, got {actual}"


@pytest.mark.parametrize("unknown", ["embargoed", "internal", "PUBLIC", "Private", "", "unknown"])
def test_unrecognised_visibility_fail_safes_to_restricted_never_public(unknown: str) -> None:
    # The leak-critical invariant (spec D1): a value the policy does not explicitly recognise must route
    # to RESTRICTED. It must NEVER be PUBLIC — that is the only outcome that would leak a restricted match.
    tier = classify_access_tier(provider="skillcorner", visibility=unknown)
    assert tier is AccessTier.RESTRICTED
    assert tier is not AccessTier.PUBLIC


@pytest.mark.skipif(
    not os.getenv("PINING_LIVE_CONTRACT"),
    reason="live pining contract is opt-in (set PINING_LIVE_CONTRACT=1 with an owner token configured)",
)
def test_live_pining_matches_visibility_vocabulary() -> None:
    """ENV-GATED live variant: every visibility on the real owner-token feed is in the closed vocabulary.

    Mirrors pining's producer-side schema test against the production API. Requires a resolvable owner
    token (``PINING_FOR_THE_DATA_TOKEN`` or the Databricks ``pining/token`` secret).
    """
    from ingestion.skillcorner_common import fetch_match_list, resolve_pining_token

    matches = fetch_match_list(resolve_pining_token())
    assert matches, "live /matches returned no matches — cannot assert the visibility vocabulary"
    for match in matches:
        assert match.visibility in _PINING_VISIBILITY_VOCABULARY, (
            f"LIVE pining returned visibility={match.visibility!r} for match {match.id} outside "
            f"{sorted(_PINING_VISIBILITY_VOCABULARY)} — producer-side schema drift"
        )
        # And the policy must still map every live value into the explicit table (no fail-safe surprises).
        assert (
            classify_access_tier(provider="skillcorner", visibility=match.visibility)
            is (_EXPECTED_TIER_BY_VISIBILITY[match.visibility])
        )
