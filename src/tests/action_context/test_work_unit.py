from __future__ import annotations

import pytest

from analytics.action_context.work_unit import WorkUnit, provider_tier, resolve_frame_tier


def test_provider_tiering() -> None:
    # Frames-required pipeline (ADR-057): tracking providers classify as "tracking";
    # statsbomb defers to the FrameSource (always sb360, since discovery only enqueues
    # statsbomb matches that have freeze-frames).
    assert provider_tier(WorkUnit("idsse", "M", period=1)) == "tracking"
    assert provider_tier(WorkUnit("metrica", "M")) == "tracking"
    assert provider_tier(WorkUnit("skillcorner", "M")) == "tracking"
    assert provider_tier(WorkUnit("gradientsports", "M")) == "tracking"
    assert provider_tier(WorkUnit("statsbomb", "M")) == "statsbomb"


def test_provider_tier_rejects_non_frame_providers() -> None:
    # wyscout (and any pure event-only provider) no longer exists for action-context (ADR-057).
    with pytest.raises(ValueError, match="not an action-context provider"):
        provider_tier(WorkUnit("wyscout", "M"))


def test_resolve_frame_tier() -> None:
    # static ProviderTier -> runtime FrameTier (the single mapping site).
    assert resolve_frame_tier("tracking") == "tracking"
    assert resolve_frame_tier("statsbomb") == "sb360"
