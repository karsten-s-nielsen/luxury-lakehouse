"""Match resolution (spec §15.2; plan Task 22): the same real match across providers, for example GS World Cup 2022
and StatsBomb World Cup 2022.

A pair of matches is a candidate when their dates are at most one day apart (providers date a match in local time
or in UTC) and their teams agree:

- layer 1 (`date_team_links`): both teams are in the same team clusters of this run;
- layer 2 (`date_team_names`): the home names and the away names match (rapidfuzz `token_sort_ratio` on normalised
  names, the weaker of the two at least 70);
- layer 3 (`date_team_names_swapped`): the same with home and away swapped.

The score is 40 % date (100 for the same day, 90 for one day apart) and 60 % teams (100 for linked teams, else the
weaker name score). Each match keeps only its mutual best partner in each other provider, with the runner-up score.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Final

from rapidfuzz import fuzz

from lakehouse.ids.native import Provider
from lakehouse.resolve.model import Candidate, EntityRecord, RecordRef
from lakehouse.resolve.players import normalize_name

__all__ = ["match_candidates"]

DATE_WEIGHT: Final = 0.4
TEAM_WEIGHT: Final = 0.6
TEAM_CUTOFF: Final = 70.0


def _date_score(a: EntityRecord, b: EntityRecord) -> float | None:
    if a.match_date is None or b.match_date is None:
        return None
    days = abs((a.match_date - b.match_date).days)
    return 100.0 if days == 0 else 90.0 if days == 1 else None


def _similar(x: str | None, y: str | None) -> float:
    return float(fuzz.token_sort_ratio(normalize_name(x), normalize_name(y))) if x and y else 0.0


def _linked(x: RecordRef | None, y: RecordRef | None, clusters: Mapping[RecordRef, RecordRef]) -> bool:
    return x is not None and y is not None and x in clusters and y in clusters and clusters[x] == clusters[y]


def _teams(a: EntityRecord, b: EntityRecord, clusters: Mapping[RecordRef, RecordRef]) -> tuple[float, int, str] | None:
    options: list[tuple[float, int, str]] = []
    if _linked(a.home_team, b.home_team, clusters) and _linked(a.away_team, b.away_team, clusters):
        options.append((100.0, 1, "date_team_links"))
    straight = min(_similar(a.home, b.home), _similar(a.away, b.away))
    if straight >= TEAM_CUTOFF:
        options.append((straight, 2, "date_team_names"))
    swapped = min(_similar(a.home, b.away), _similar(a.away, b.home))
    if swapped >= TEAM_CUTOFF:
        options.append((swapped, 3, "date_team_names_swapped"))
    return min(options, key=lambda o: (-o[0], o[1])) if options else None


def _pair(
    records_a: Sequence[EntityRecord], records_b: Sequence[EntityRecord], clusters: Mapping[RecordRef, RecordRef]
) -> list[Candidate]:
    by_day: dict[object, list[int]] = {}
    for j, record in enumerate(records_b):
        if record.match_date is not None:
            by_day.setdefault(record.match_date, []).append(j)
    scored: dict[tuple[int, int], tuple[float, int, str]] = {}
    for i, a in enumerate(records_a):
        if a.match_date is None:
            continue
        for day in (a.match_date - timedelta(days=1), a.match_date, a.match_date + timedelta(days=1)):
            for j in by_day.get(day, []):
                date_score, teams = _date_score(a, records_b[j]), _teams(a, records_b[j], clusters)
                if date_score is None or teams is None:
                    continue
                score = round(DATE_WEIGHT * date_score + TEAM_WEIGHT * teams[0], 4)
                scored[(i, j)] = (score, teams[1], teams[2])
    best_a: dict[int, tuple[float, int]] = {}
    best_b: dict[int, tuple[float, int]] = {}
    for (i, j), (score, _, _) in sorted(scored.items()):
        if i not in best_a or score > best_a[i][0]:
            best_a[i] = (score, j)
        if j not in best_b or score > best_b[j][0]:
            best_b[j] = (score, i)
    found: list[Candidate] = []
    for i, (score, j) in sorted(best_a.items()):
        if best_b[j][1] != i:
            continue
        rivals = [s for (x, y), (s, _, _) in scored.items() if (x == i) != (y == j)]
        _, layer, method = scored[(i, j)]
        found.append(
            Candidate.of(
                records_a[i].ref,
                records_b[j].ref,
                score=score,
                layer=layer,
                method=method,
                runner_up=max(rivals) if rivals else None,
            )
        )
    return found


def match_candidates(
    records: Mapping[Provider, Sequence[EntityRecord]], team_clusters: Mapping[RecordRef, RecordRef]
) -> list[Candidate]:
    """The match candidates of every provider pair; `team_clusters` maps each team record to its canonical member."""
    found: list[Candidate] = []
    for provider_a, provider_b in itertools.combinations(sorted(records), 2):
        found.extend(_pair(records[provider_a], records[provider_b], team_clusters))
    return sorted(found, key=lambda c: c.key)
