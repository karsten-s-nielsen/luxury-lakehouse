"""Competition resolution (spec §15.2; plan Task 22): the team name matcher of `teams.py` on competition names.

Layer 1 when both records name the same area (country or region), layer 2 otherwise; the bands of §15.5, measured
on the truth set, decide what links.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence

from lakehouse.ids.native import Provider
from lakehouse.resolve.model import Candidate, EntityRecord
from lakehouse.resolve.players import normalize_name
from lakehouse.resolve.teams import match_names

__all__ = ["competition_candidates"]


def competition_candidates(records: Mapping[Provider, Sequence[EntityRecord]]) -> list[Candidate]:
    """The competition candidates of every provider pair."""
    found: list[Candidate] = []
    for provider_a, provider_b in itertools.combinations(sorted(records), 2):
        for a, b, score, runner_up in match_names(records[provider_a], records[provider_b]):
            same_area = bool(a.area and b.area and normalize_name(a.area) == normalize_name(b.area))
            found.append(
                Candidate.of(
                    a.ref,
                    b.ref,
                    score=score,
                    layer=1 if same_area else 2,
                    method="competition_name_rapidfuzz",
                    runner_up=runner_up,
                )
            )
    return sorted(found, key=lambda c: c.key)
