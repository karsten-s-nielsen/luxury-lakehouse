"""Domain value types for the action-context pipeline.

``WorkUnit`` is the chunking abstraction (mirrors production: IDSSE = one
period, other tracking = match, event-only = match, profiling = frame slice).
``FrameBundle`` carries tier-appropriate frames + a resolved tier tag so the
pipeline dispatches to the correct enrich tier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd

_TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
# statsbomb is resolved to the sb360 FrameTier at runtime by the FrameSource (freeze-frame
# presence), so it is NOT in _TRACKING_PROVIDERS; provider_tier returns "statsbomb" for it.
# There is no event-only provider: action-context is frames-required (ADR-057).

# Two distinct tier vocabularies, kept apart at the type level so pyright catches a crossed
# static/runtime value (review M2): ProviderTier is static (provider_tier), FrameTier is runtime
# (FrameBundle / enrich_batch). resolve_frame_tier is the single mapping between them.
ProviderTier = Literal["tracking", "statsbomb"]
FrameTier = Literal["tracking", "sb360"]


@dataclass(frozen=True)
class WorkUnit:
    """One unit of action-context work.

    ``kde_backend`` is domain policy (how to compute ghost-GK), resolved once at the adapter boundary
    and carried per-unit through the queue (single source of truth across the preflight→drain task
    boundary). Validated here so a bad value is rejected before it enters the queue rather than failing
    deep in silly-kicks. See ``analytics.action_context.ghost_gk_backend``.
    """

    provider: str
    match_id: str
    period: int | None = None
    frame_range: tuple[int, int] | None = None
    # Default kept in sync with ghost_gk_backend.DEFAULT_GHOST_GK_BACKEND (a literal here to avoid a
    # module-scope import — work_unit.py must import offline; __post_init__ imports the allowlist lazily).
    kde_backend: str = "fft-cic"

    def __post_init__(self) -> None:
        # Belt-and-braces against a value that bypasses resolve_ghost_gk_backend (e.g. a direct
        # WorkUnit(kde_backend="typo")). Reads only — valid on a frozen dataclass.
        from analytics.action_context.ghost_gk_backend import GHOST_GK_KDE_BACKENDS

        if self.kde_backend not in GHOST_GK_KDE_BACKENDS:
            raise ValueError(f"Unknown ghost-GK backend {self.kde_backend!r}. Valid: {sorted(GHOST_GK_KDE_BACKENDS)}")


@dataclass(frozen=True)
class MatchMeta:
    """Driver-resolved match-level metadata passed into enrichment.

    ``home_team_start_left_extratime`` is required by silly-kicks 4.0+'s
    symmetric ET guard (``require_et_direction``) on every per-period-absolute
    converter (Sportec/Metrica/GradientSports, tracking + events). ``None`` is
    safe when the match has no ET periods (silly-kicks 4.0 guard only raises
    when ET periods AND flag-is-None coincide). Resolved by the per-provider
    derivers in ``src/ingestion/spadl_adapter.py`` (authoritative for
    IDSSE/GS; empirical for Metrica).
    """

    home_team_id: str
    home_start_left: bool
    home_team_start_left_extratime: bool | None = None
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None


@dataclass(frozen=True)
class FrameBundle:
    """Tier-appropriate frames for a work unit.

    ``tier`` is one of: ``tracking`` | ``sb360`` (action-context is frames-required; ADR-057).
    ``frames`` is tracking frames or synthetic freeze-frames — never empty.
    """

    tier: FrameTier
    frames: pd.DataFrame
    extra: dict[str, Any] = field(default_factory=dict)


def provider_tier(wu: WorkUnit) -> ProviderTier:
    """Static provider classification for the FRAMES-REQUIRED action-context pipeline.

    Returns ``tracking`` (idsse/metrica/skillcorner/gradientsports) or ``statsbomb``
    (resolved to the ``sb360`` FrameTier at runtime by the FrameSource). Event-only
    providers do NOT exist for action-context (ADR-057) — they raise.
    """
    if wu.provider in _TRACKING_PROVIDERS:
        return "tracking"
    if wu.provider == "statsbomb":
        return "statsbomb"
    raise ValueError(f"{wu.provider!r} is not an action-context provider (frames-required; ADR-057)")


def resolve_frame_tier(pt: ProviderTier) -> FrameTier:
    """Map the static ProviderTier to the runtime FrameTier — THE single mapping site.

    Frames-required: an ENQUEUED statsbomb unit always has freeze-frames (discovery
    semi-joins statsbomb_360), so it is always ``sb360`` — there is no event-only outcome.
    """
    return "tracking" if pt == "tracking" else "sb360"
