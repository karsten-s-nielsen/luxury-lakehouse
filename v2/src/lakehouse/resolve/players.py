"""Player resolution, ported from the IRIS `entity_resolution.py` (spec §15.1; plan Task 22).

Three progressive layers (inspired by US Soccer's glass_onion, BSD 3-Clause; bidirectional validation from
parmacalcio1913/players-matcher, Apache-2.0):

- Layer 1 (strict): name, date of birth, jersey and team, threshold 90.
- Layer 2 (standard): name and date of birth with month/day swap detection, threshold 80.
- Layer 3 (relaxed): name and position, threshold 75.

Each layer generates candidates with TF-IDF character n-grams, scores them with rapidfuzz on several attributes, and
keeps only mutual best matches. A matched player leaves the pool before the next layer. A global threshold of 70
applies at the end. Ported changes: plain Python records instead of pandas frames, every provider pair (the n-way
orchestrator of IRIS), and the runner-up score of each match for the margin of §15.6. Scores are rounded to 4
decimals.
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from rapidfuzz import fuzz
from unidecode import unidecode

from lakehouse.ids.native import Provider
from lakehouse.logging import get_logger
from lakehouse.resolve.model import PROVIDER_RANK, Candidate, EntityRecord
from lakehouse.resolve.tfidf import top_pairs

__all__ = [
    "LayerConfig",
    "ResolutionConfig",
    "encode_dob",
    "match_dob",
    "normalize_name",
    "player_candidates",
    "score_candidate",
]

_log = get_logger(__name__)

_MONTH_LETTERS = "ABCDEFGHIJKL"  # Jan=A ... Dec=L
_CLEAN_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")
# Wyscout role codes, then StatsBomb position-name keywords; the first key found in the position names the group.
_POSITION_GROUP: dict[str, str] = {
    "GK": "Goalkeeper",
    "Goalkeeper": "Goalkeeper",
    "DF": "Defender",
    "Defender": "Defender",
    "MF": "Midfielder",
    "Midfielder": "Midfielder",
    "FW": "Forward",
    "Forward": "Forward",
    "Back": "Defender",
    "Wing Back": "Defender",
    "Midfield": "Midfielder",
    "Wing": "Forward",
    "Striker": "Forward",
}


def normalize_name(name: str | None) -> str:
    """unidecode, lower case, punctuation to spaces, whitespace collapsed."""
    if not name:
        return ""
    text = unidecode(str(name)).lower()
    text = _CLEAN_RE.sub(" ", text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def encode_dob(dob: str | None) -> str:
    """A date of birth as a 5-character TF-IDF token `DDXYY`, X the month letter: 1993-12-28 gives `28L93`."""
    if not dob:
        return ""
    parts = str(dob).split("-")
    if len(parts) != 3:
        return ""
    year, month, day = parts
    try:
        month_index = int(month) - 1
        day_number = int(day)
    except ValueError:
        return ""
    if not 0 <= month_index < 12:
        return ""
    return f"{day_number:02d}{_MONTH_LETTERS[month_index]}{year[-2:]}"


def _position_group(position: str | None) -> str:
    if not position:
        return ""
    lowered = position.lower()
    for key, group in _POSITION_GROUP.items():
        if key.lower() in lowered:
            return group
    return ""


def match_dob(dob_a: str | None, dob_b: str | None) -> float | None:
    """100 for the same date, 90 for one day apart in the same month, 80 for month and day swapped, 0 otherwise;
    `None` when either date is missing."""
    if not dob_a or not dob_b:
        return None
    a, b = str(dob_a).strip(), str(dob_b).strip()
    if a == b:
        return 100.0
    try:
        ya, ma, da = a.split("-")
        yb, mb, db = b.split("-")
    except ValueError:
        return 0.0
    if ya == yb and ma == mb:
        try:
            if abs(int(da) - int(db)) <= 1:
                return 90.0
        except ValueError:
            pass
    if ya == yb and ma == db and da == mb:
        return 80.0
    return 0.0


def score_candidate(
    name_a: str,
    name_b: str,
    dob_a: str | None = None,
    dob_b: str | None = None,
    position_a: str | None = None,
    position_b: str | None = None,
    jersey_a: str | None = None,
    jersey_b: str | None = None,
    short_name_a: str | None = None,
    short_name_b: str | None = None,
) -> float:
    """Name 60 %, date of birth 25 %, position group 15 %; a missing attribute drops out and the weights are
    redistributed. The same jersey adds 3 (no penalty: numbers change). The name score is the best of the full and
    short name combinations."""
    name_scores = [fuzz.token_sort_ratio(name_a, name_b)]
    if short_name_a:
        name_scores.append(fuzz.token_sort_ratio(short_name_a, name_b))
        if short_name_b:
            name_scores.append(fuzz.token_sort_ratio(short_name_a, short_name_b))
    if short_name_b:
        name_scores.append(fuzz.token_sort_ratio(name_a, short_name_b))
    weights: list[tuple[float, float]] = [(0.60, max(name_scores))]
    dob = match_dob(dob_a, dob_b)
    if dob is not None:
        weights.append((0.25, dob))
    group_a, group_b = _position_group(position_a), _position_group(position_b)
    if group_a and group_b:
        weights.append((0.15, 100.0 if group_a == group_b else 25.0))
    total = sum(weight for weight, _ in weights)
    score = sum((weight / total) * value for weight, value in weights)
    if jersey_a and jersey_b and str(jersey_a) == str(jersey_b):
        score = min(100.0, score + 3.0)
    return round(score, 4)


@dataclass(frozen=True)
class LayerConfig:
    threshold: float
    use_dob: bool = True
    use_jersey: bool = False
    use_team: bool = False
    use_position: bool = True
    tfidf_threshold: float = 0.3


def _default_layers() -> tuple[LayerConfig, ...]:
    return (
        LayerConfig(threshold=90.0, use_dob=True, use_jersey=True, use_team=True, tfidf_threshold=0.4),
        LayerConfig(threshold=80.0, use_dob=True, use_jersey=False, use_team=False, tfidf_threshold=0.3),
        LayerConfig(threshold=75.0, use_dob=False, use_jersey=False, use_team=False, tfidf_threshold=0.25),
    )


@dataclass(frozen=True)
class ResolutionConfig:
    tfidf_top_n: int = 5
    ngram_size: int = 3
    confidence_threshold: float = 70.0
    layers: tuple[LayerConfig, ...] = field(default_factory=_default_layers)


@dataclass(frozen=True)
class _Prepared:
    record: EntityRecord
    name: str
    short: str
    searchable: str
    team: str


def _prepare(record: EntityRecord) -> _Prepared:
    name, short = normalize_name(record.name), normalize_name(record.short_name)
    searchable = f"{name} {short} {encode_dob(record.birth_date)}".strip()
    return _Prepared(record, name, short, searchable, normalize_name(record.team))


def _score(a: _Prepared, b: _Prepared, layer: LayerConfig) -> float:
    ra, rb = a.record, b.record
    return score_candidate(
        a.name,
        b.name,
        dob_a=ra.birth_date if layer.use_dob else None,
        dob_b=rb.birth_date if layer.use_dob else None,
        position_a=ra.position if layer.use_position else None,
        position_b=rb.position if layer.use_position else None,
        jersey_a=ra.jersey if layer.use_jersey else None,
        jersey_b=rb.jersey if layer.use_jersey else None,
        short_name_a=a.short or None,
        short_name_b=b.short or None,
    )


def _best(scores: Mapping[tuple[int, int], float]) -> dict[int, int]:
    """For each left index, the right index of its highest score (ties: the smaller right index)."""
    best: dict[int, tuple[float, int]] = {}
    for (left, right), value in sorted(scores.items()):
        if left not in best or value > best[left][0]:
            best[left] = (value, right)
    return {left: right for left, (_, right) in best.items()}


def _run_layer(
    pool_a: list[_Prepared], pool_b: list[_Prepared], layer: LayerConfig, config: ResolutionConfig
) -> list[tuple[_Prepared, _Prepared, float, float | None]]:
    """One layer: TF-IDF candidates both ways, scores, mutual best above the layer threshold, with runner-ups."""
    if layer.use_team:
        common = {p.team for p in pool_a if p.team} & {p.team for p in pool_b if p.team}
        if not common:
            return []
        pool_a = [p for p in pool_a if p.team in common]
        pool_b = [p for p in pool_b if p.team in common]
    if not pool_a or not pool_b:
        return []
    names_a, names_b = [p.searchable for p in pool_a], [p.searchable for p in pool_b]

    def candidates(left: list[str], right: list[str]) -> list[tuple[int, int, float]]:
        return top_pairs(
            left, right, top_n=config.tfidf_top_n, threshold=layer.tfidf_threshold, ngram_size=config.ngram_size
        )

    forward = {(i, j): _score(pool_a[i], pool_b[j], layer) for i, j, _ in candidates(names_a, names_b)}
    reverse = {(j, i): _score(pool_a[i], pool_b[j], layer) for j, i, _ in candidates(names_b, names_a)}
    if not forward or not reverse:
        return []
    best_forward, best_reverse = _best(forward), _best(reverse)
    found: list[tuple[_Prepared, _Prepared, float, float | None]] = []
    for i, j in sorted(best_forward.items()):
        if best_reverse.get(j) != i:
            continue
        score = forward[(i, j)]
        if score < layer.threshold:
            continue
        rivals = [v for (x, y), v in forward.items() if x == i and y != j]
        rivals += [v for (x, y), v in reverse.items() if x == j and y != i]
        found.append((pool_a[i], pool_b[j], score, max(rivals) if rivals else None))
    return found


def _resolve_pair(
    records_a: Sequence[EntityRecord], records_b: Sequence[EntityRecord], config: ResolutionConfig
) -> list[Candidate]:
    pool_a, pool_b = [_prepare(r) for r in records_a], [_prepare(r) for r in records_b]
    matched_a: set[int] = set()
    matched_b: set[int] = set()
    found: list[Candidate] = []
    for number, layer in enumerate(config.layers, start=1):
        left = [p for p in pool_a if id(p) not in matched_a]
        right = [p for p in pool_b if id(p) not in matched_b]
        if not left or not right:
            break
        layer_found = _run_layer(left, right, layer, config)
        for a, b, score, runner_up in layer_found:
            matched_a.add(id(a))
            matched_b.add(id(b))
            if score >= config.confidence_threshold:
                found.append(
                    Candidate.of(
                        a.record.ref,
                        b.record.ref,
                        score=score,
                        layer=number,
                        method=f"layer{number}_tfidf_rapidfuzz_bidirectional",
                        runner_up=runner_up,
                    )
                )
        _log.info(
            "player layer",
            extra={"layer": number, "rows": len(layer_found), "threshold": layer.threshold, "team": layer.use_team},
        )
    return found


def player_candidates(
    records: Mapping[Provider, Sequence[EntityRecord]], config: ResolutionConfig | None = None
) -> list[Candidate]:
    """The player candidates of every provider pair."""
    chosen = config if config is not None else ResolutionConfig()
    providers = sorted(records, key=lambda p: PROVIDER_RANK[p])
    found: list[Candidate] = []
    for provider_a, provider_b in itertools.combinations(providers, 2):
        if records[provider_a] and records[provider_b]:
            found.extend(_resolve_pair(records[provider_a], records[provider_b], chosen))
    return sorted(found, key=lambda c: c.key)
