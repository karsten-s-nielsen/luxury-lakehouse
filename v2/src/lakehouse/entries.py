"""The listed-match types that channel adapters produce and provider parsers consume (spec §7, §17).

They live outside both packages, so adapters and parsers stay independent of each other (spec §6, import contract).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from lakehouse.config.model import ModuleId

__all__ = ["ArtifactRef", "SourceEntry"]


@dataclass(frozen=True)
class ArtifactRef:
    """One artifact of a listed match: the channel's key for it, its change signal, and (where the channel says so)
    the SHA-256 and size of the stored bytes (pining `artifact_meta`)."""

    key: str
    change_signal: str | None
    expected_sha256: str | None
    size: int | None


@dataclass(frozen=True)
class SourceEntry:
    module_id: ModuleId
    native_match_id: str
    competition_id: str | None
    competition_name: str | None
    season_id: str | None
    season_name: str | None
    visibility: Literal["public", "private"]
    artifacts: Mapping[str, ArtifactRef]
    listed_at: datetime
    has_360: bool | None = None  # StatsBomb adapters only (§29.13)
    channel_match_id: str | None = None  # the id as the channel delivers it, where it differs (pining; §9 lineage)
