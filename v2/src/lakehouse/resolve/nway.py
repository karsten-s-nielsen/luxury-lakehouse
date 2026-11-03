"""N-way clustering, ported from the IRIS `nway_resolution.py` (spec §14.3, §15.1; plan Task 22).

The links of every provider pair are joined with union-find. A same-source conflict guard splits any component that
holds two ids of one provider: the provider with the most ids in the component gives the anchors, and each other
record joins the anchor it links to with the highest confidence among the anchors that do not yet hold its provider
(ties: provider rank, then native id). The canonical member of a cluster is the member of the highest-ranked
provider (§14.3), then the smallest native id, so the result never depends on input order.
"""

from __future__ import annotations

from collections.abc import Iterable

from lakehouse.ids.native import Provider
from lakehouse.logging import get_logger
from lakehouse.resolve.model import PROVIDER_RANK, Candidate, RecordRef, pair

__all__ = ["canonical_of", "resolve_clusters"]

_log = get_logger(__name__)


def _rank(ref: RecordRef) -> tuple[int, str]:
    return (PROVIDER_RANK[ref.provider], ref.native_id)


def canonical_of(members: Iterable[RecordRef]) -> RecordRef:
    """The cluster member that the canonical pointer names (§14.3)."""
    return min(members, key=_rank)


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[RecordRef, RecordRef] = {}

    def find(self, x: RecordRef) -> RecordRef:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: RecordRef, b: RecordRef) -> None:
        self.parent[self.find(a)] = self.find(b)


def _split(members: list[RecordRef], best: dict[tuple[RecordRef, RecordRef], float]) -> dict[RecordRef, RecordRef]:
    """The same-source guard for one conflicted component: each record mapped to its anchor."""
    by_provider: dict[Provider, list[RecordRef]] = {}
    for ref in sorted(members, key=_rank):
        by_provider.setdefault(ref.provider, []).append(ref)
    # the provider with the most ids gives the anchors; a tie goes to the higher-ranked provider
    anchor_provider = max(sorted(by_provider, key=lambda p: PROVIDER_RANK[p]), key=lambda p: len(by_provider[p]))
    anchors = by_provider[anchor_provider]
    claimed: dict[RecordRef, set[Provider]] = {anchor: {anchor_provider} for anchor in anchors}
    placed: dict[RecordRef, RecordRef] = {anchor: anchor for anchor in anchors}

    def confidence(x: RecordRef, y: RecordRef) -> float:
        return best.get(pair(x, y), 0.0)

    others = [ref for ref in members if ref not in claimed]
    for ref in sorted(others, key=lambda r: (-max(confidence(r, a) for a in anchors), *_rank(r))):
        for anchor in sorted(anchors, key=lambda a: (-confidence(ref, a), *_rank(a))):
            if ref.provider not in claimed[anchor]:
                placed[ref] = anchor
                claimed[anchor].add(ref.provider)
                break
        else:
            placed[ref] = ref  # every anchor already holds this provider: the record stays alone
    _log.info(
        "nway conflict split",
        extra={"providers": sorted(by_provider), "component_size": len(members), "anchors": len(anchors)},
    )
    return placed


def resolve_clusters(records: Iterable[RecordRef], links: Iterable[Candidate]) -> dict[RecordRef, RecordRef]:
    """Each record mapped to the canonical member of its cluster; a record without links maps to itself."""
    refs = sorted(set(records))
    known = set(refs)
    uf = _UnionFind()
    for ref in refs:
        uf.find(ref)
    best: dict[tuple[RecordRef, RecordRef], float] = {}
    for link in links:
        if link.a not in known or link.b not in known:
            raise ValueError(f"a link names a record that is not in the set: {link.a} - {link.b}")
        best[link.key] = max(best.get(link.key, 0.0), link.score)
        uf.union(link.a, link.b)
    components: dict[RecordRef, list[RecordRef]] = {}
    for ref in refs:
        components.setdefault(uf.find(ref), []).append(ref)
    cluster_of: dict[RecordRef, RecordRef] = {}
    for members in components.values():
        providers = [ref.provider for ref in members]
        if len(providers) == len(set(providers)):
            for ref in members:
                cluster_of[ref] = members[0]
        else:
            cluster_of.update(_split(members, best))
    by_cluster: dict[RecordRef, list[RecordRef]] = {}
    for ref, root in cluster_of.items():
        by_cluster.setdefault(root, []).append(ref)
    result: dict[RecordRef, RecordRef] = {}
    for members in by_cluster.values():
        providers = [ref.provider for ref in members]
        if len(providers) != len(set(providers)):  # the invariant of the guard
            raise AssertionError(f"the same-source guard failed: a cluster holds two ids of one provider: {members}")
        canonical = canonical_of(members)
        for ref in members:
            result[ref] = canonical
    return result
