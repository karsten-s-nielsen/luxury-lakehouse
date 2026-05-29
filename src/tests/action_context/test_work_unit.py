from __future__ import annotations

from analytics.action_context.work_unit import WorkUnit, provider_tier


def test_provider_tiering() -> None:
    assert provider_tier(WorkUnit("idsse", "M", period=1)) == "tracking"
    assert provider_tier(WorkUnit("metrica", "M")) == "tracking"
    assert provider_tier(WorkUnit("skillcorner", "M")) == "tracking"
    assert provider_tier(WorkUnit("gradientsports", "M")) == "tracking"
    assert provider_tier(WorkUnit("wyscout", "M")) == "event_only"
    # statsbomb deferred to FrameSource (sb360 vs event_only via 360 presence).
    assert provider_tier(WorkUnit("statsbomb", "M")) == "statsbomb"
