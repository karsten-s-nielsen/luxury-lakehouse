"""The shared types of entity resolution (spec §14.3, §15; plan Task 22).

A record is one provider's view of a real player, team, competition or match. A candidate is a scored pair of
records of two providers. Scores run from 0 to 100, as in the IRIS resolver; the bands of §15.5 use the same unit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal

from lakehouse.ids.native import Provider

__all__ = ["ENTITIES", "PLURALS", "PROVIDER_RANK", "Candidate", "Entity", "EntityRecord", "RecordRef", "pair"]

Entity = Literal["player", "team", "competition", "match"]
# Resolution order: match candidates use the team clusters of the same run.
ENTITIES: Final[tuple[Entity, ...]] = ("competition", "team", "player", "match")
PLURALS: Final[dict[Entity, str]] = {
    "player": "players",
    "team": "teams",
    "competition": "competitions",
    "match": "matches",
}
# §14.3: the canonical pointer names the member of the highest-ranked provider (v1 order, not the IRIS order).
PROVIDER_RANK: Final[dict[Provider, int]] = {
    "statsbomb": 1,
    "wyscout": 2,
    "idsse": 3,
    "skillcorner": 4,
    "gradientsports": 5,
}


@dataclass(frozen=True, order=True)
class RecordRef:
    """A record's natural key: the provider and its canonical native id (spec §14.3)."""

    provider: Provider
    native_id: str


@dataclass(frozen=True)
class EntityRecord:
    """One provider's record of an entity, with the attributes the matchers read. `birth_date` is `YYYY-MM-DD`."""

    ref: RecordRef
    key: int
    name: str
    short_name: str | None = None
    birth_date: str | None = None
    team: str | None = None
    jersey: str | None = None
    position: str | None = None
    area: str | None = None
    match_date: date | None = None
    home: str | None = None
    away: str | None = None
    home_team: RecordRef | None = None
    away_team: RecordRef | None = None
    matches: int | None = None
    native_competition_id: str | None = None
    competition_key: int | None = None
    native_season_id: str | None = None
    season_key: int | None = None
    season_name: str | None = None


def pair(a: RecordRef, b: RecordRef) -> tuple[RecordRef, RecordRef]:
    """The order-free key of a pair: the smaller ref first."""
    return (a, b) if a <= b else (b, a)


@dataclass(frozen=True)
class Candidate:
    """A scored pair of records of two providers. `runner_up` is the best score of a competing candidate of either
    record (`None` when there is none); the margin of §15.6 is `score - runner_up`."""

    a: RecordRef
    b: RecordRef
    score: float
    layer: int
    method: str
    runner_up: float | None = None

    @classmethod
    def of(
        cls, x: RecordRef, y: RecordRef, *, score: float, layer: int, method: str, runner_up: float | None = None
    ) -> Candidate:
        if x.provider == y.provider:
            raise ValueError(f"a candidate pairs two providers, not {x} and {y}")
        a, b = pair(x, y)
        return cls(a, b, score, layer, method, runner_up)

    @property
    def key(self) -> tuple[RecordRef, RecordRef]:
        return (self.a, self.b)
