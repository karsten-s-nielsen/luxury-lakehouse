"""N-way clustering (spec §14.3, §15.1; plan Task 22): union-find over every provider pair, the same-source conflict
guard and the canonical pointer rank."""

from __future__ import annotations

from lakehouse.ids.native import Provider
from lakehouse.resolve.model import PROVIDER_RANK, Candidate, RecordRef
from lakehouse.resolve.nway import canonical_of, resolve_clusters


def _ref(provider: Provider, native: str) -> RecordRef:
    return RecordRef(provider, native)


def _link(a: RecordRef, b: RecordRef, score: float) -> Candidate:
    return Candidate.of(a, b, score=score, layer=1, method="test")


def test_canonical_pointer_rank() -> None:
    """§14.3: statsbomb > wyscout > idsse > skillcorner > gradientsports (v1 order; IRIS put SkillCorner first)."""
    assert [p for p, _ in sorted(PROVIDER_RANK.items(), key=lambda item: item[1])] == [
        "statsbomb",
        "wyscout",
        "idsse",
        "skillcorner",
        "gradientsports",
    ]
    members = [_ref("gradientsports", "g1"), _ref("skillcorner", "s1"), _ref("idsse", "i1")]
    assert canonical_of(members) == _ref("idsse", "i1")
    assert canonical_of([*members, _ref("wyscout", "w1")]) == _ref("wyscout", "w1")
    assert canonical_of([*members, _ref("wyscout", "w1"), _ref("statsbomb", "9")]) == _ref("statsbomb", "9")


def test_clusters_join_every_provider_pair() -> None:
    sb, ws, idsse, sc = _ref("statsbomb", "1"), _ref("wyscout", "2"), _ref("idsse", "3"), _ref("skillcorner", "4")
    lonely = _ref("gradientsports", "5")
    clusters = resolve_clusters([sb, ws, idsse, sc, lonely], [_link(ws, idsse, 90), _link(idsse, sc, 85)])
    assert clusters[ws] == clusters[idsse] == clusters[sc] == ws
    assert clusters[sb] == sb
    assert clusters[lonely] == lonely  # a singleton points to itself


def test_nway_same_source_guard() -> None:
    """Two StatsBomb records reach one cluster through one Wyscout record: the guard splits them, and the Wyscout
    record joins the StatsBomb record it links to with the higher confidence."""
    sb1, sb2, ws = _ref("statsbomb", "1"), _ref("statsbomb", "2"), _ref("wyscout", "7")
    idsse = _ref("idsse", "J1")
    clusters = resolve_clusters([sb1, sb2, ws, idsse], [_link(sb1, ws, 80), _link(sb2, ws, 95), _link(sb1, idsse, 90)])
    assert clusters[sb1] != clusters[sb2]
    assert clusters[ws] == clusters[sb2] == sb2
    assert clusters[idsse] == clusters[sb1] == sb1
    for root in set(clusters.values()):
        providers = [ref.provider for ref, canon in clusters.items() if canon == root]
        assert len(providers) == len(set(providers))  # no cluster holds two ids of one provider


def test_guard_splits_by_the_provider_with_most_ids() -> None:
    """The provider with the most ids in a conflicted component gives the anchors; each other record joins its
    strongest anchor that does not yet hold its provider."""
    sb1, sb2 = _ref("statsbomb", "1"), _ref("statsbomb", "2")
    ws1, ws2, ws3 = _ref("wyscout", "1"), _ref("wyscout", "2"), _ref("wyscout", "3")
    links = [_link(sb1, ws1, 90), _link(sb1, ws2, 80), _link(sb2, ws2, 85), _link(sb2, ws3, 70)]
    clusters = resolve_clusters([sb1, sb2, ws1, ws2, ws3], links)
    for root in set(clusters.values()):
        providers = [ref.provider for ref, canon in clusters.items() if canon == root]
        assert len(providers) == len(set(providers))
    assert clusters[ws1] == sb1
    assert clusters[ws2] == sb2  # the stronger edge of the two anchors that do not yet hold a Wyscout record


def test_clusters_are_deterministic() -> None:
    refs = [_ref("wyscout", str(n)) for n in range(5)] + [_ref("idsse", f"J{n}") for n in range(5)]
    links = [_link(_ref("wyscout", str(n)), _ref("idsse", f"J{n}"), 90 - n) for n in range(5)]
    first = resolve_clusters(refs, links)
    assert resolve_clusters(list(reversed(refs)), list(reversed(links))) == first
