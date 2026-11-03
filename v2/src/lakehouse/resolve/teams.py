"""Team resolution, ported from the IRIS `team_resolution.py` (spec §15.1; plan Task 22).

The IRIS algorithm, unchanged: rapidfuzz `token_sort_ratio`, the best match of each team of provider A among the
teams of provider B, a confidence of at least 70, then a collision resolver that keeps one edge per target team (the
highest confidence; ties: the smaller native id), so the mapping is one-to-one. Extended from StatsBomb ↔ Wyscout to
every provider pair (A before B by name, as IRIS required `source_a < source_b`). Added: the runner-up score — the
second-best target of the team, or a team that lost the same target in the collision resolver.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from typing import Final

from rapidfuzz import fuzz, process

from lakehouse.ids.native import Provider
from lakehouse.resolve.model import Candidate, EntityRecord

__all__ = ["CONFIDENCE_THRESHOLD", "match_names", "team_candidates"]

CONFIDENCE_THRESHOLD: Final = 70.0


def match_names(
    records_a: Sequence[EntityRecord], records_b: Sequence[EntityRecord], *, cutoff: float = CONFIDENCE_THRESHOLD
) -> list[tuple[EntityRecord, EntityRecord, float, float | None]]:
    """One-to-one best name matches from A to B, as `(a, b, score, runner_up)`."""
    if not records_a or not records_b:
        return []
    names_b = [record.name for record in records_b]
    edges: list[tuple[EntityRecord, int, float, float | None]] = []
    for record in records_a:
        found = process.extract(record.name, names_b, scorer=fuzz.token_sort_ratio, limit=2)
        if not found or found[0][1] < cutoff:
            continue
        _, score, index = found[0]
        second = float(found[1][1]) if len(found) > 1 else None
        edges.append((record, int(index), round(float(score), 4), second))
    by_target: dict[int, list[tuple[EntityRecord, int, float, float | None]]] = {}
    for edge in edges:
        by_target.setdefault(edge[1], []).append(edge)
    result: list[tuple[EntityRecord, EntityRecord, float, float | None]] = []
    for target, competing in sorted(by_target.items()):
        competing.sort(key=lambda e: (-e[2], e[0].ref.native_id))
        winner, *losers = competing
        rivals = [r for r in (winner[3], *(loser[2] for loser in losers)) if r is not None]
        result.append((winner[0], records_b[target], winner[2], max(rivals) if rivals else None))
    return result


def team_candidates(records: Mapping[Provider, Sequence[EntityRecord]]) -> list[Candidate]:
    """The team candidates of every provider pair."""
    found: list[Candidate] = []
    for provider_a, provider_b in itertools.combinations(sorted(records), 2):
        for a, b, score, runner_up in match_names(records[provider_a], records[provider_b]):
            found.append(
                Candidate.of(a.ref, b.ref, score=score, layer=1, method="team_name_rapidfuzz", runner_up=runner_up)
            )
    return sorted(found, key=lambda c: c.key)
