"""Domain value types for the action-context pipeline.

``WorkUnit`` is the chunking abstraction (mirrors production: IDSSE = one
period, other tracking = match, event-only = match, profiling = frame slice).
``FrameBundle`` carries tier-appropriate frames + a resolved tier tag so the
pipeline dispatches to the correct enrich tier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

_TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
# statsbomb is resolved to sb360 vs event_only by the FrameSource (360 presence),
# so it is NOT listed here; provider_tier returns "statsbomb" for it.
_EVENT_ONLY_PROVIDERS: frozenset[str] = frozenset({"wyscout"})


@dataclass(frozen=True)
class WorkUnit:
    """One unit of action-context work."""

    provider: str
    match_id: str
    period: int | None = None
    frame_range: tuple[int, int] | None = None


@dataclass(frozen=True)
class MatchMeta:
    """Driver-resolved match-level metadata passed into enrichment."""

    home_team_id: str
    home_start_left: bool
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None


@dataclass(frozen=True)
class FrameBundle:
    """Tier-appropriate frames for a work unit.

    ``tier`` is one of: ``tracking`` | ``sb360`` | ``event_only``.
    ``frames`` is tracking frames, synthetic freeze-frames, or empty (event-only).
    """

    tier: str
    frames: pd.DataFrame
    extra: dict[str, Any] = field(default_factory=dict)


def provider_tier(wu: WorkUnit) -> str:
    """Static provider classification.

    Returns ``tracking`` | ``event_only`` | ``statsbomb``. The ``statsbomb``
    case is resolved to ``sb360`` vs ``event_only`` at runtime by the
    ``FrameSource`` (it depends on 360 freeze-frame availability), so it is
    returned as-is here.
    """
    if wu.provider in _TRACKING_PROVIDERS:
        return "tracking"
    if wu.provider in _EVENT_ONLY_PROVIDERS:
        return "event_only"
    return "statsbomb"
