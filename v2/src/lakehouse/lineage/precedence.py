"""Precedence when two channels deliver the same match (spec §9, §29.2; guess 21).

The pining copy is kept when it has extra fields: a field that exists in the pining copy, has at least one non-null
value, and is missing from the public copy. Otherwise the public copy is kept. The match takes the visibility flag of
the kept copy, so a public pining copy keeps the match public. The choice uses only the current copies, so it never
depends on their order of arrival, and it is made again whenever either copy changes.
"""

from __future__ import annotations

from collections.abc import Mapping, Set
from dataclasses import dataclass
from typing import Literal

from lakehouse.ids.native import Channel

__all__ = ["Choice", "CopyInfo", "choose_copy", "extra_fields"]


@dataclass(frozen=True)
class CopyInfo:
    """One channel's copy of a match: its own flag, its content hash, and per field whether any value is non-null."""

    channel: Channel
    visibility: Literal["public", "private"]
    content_sha256: str
    nonnull_fields: Mapping[str, bool]


@dataclass(frozen=True)
class Choice:
    kept: Channel
    visibility: Literal["public", "private"]
    reason: str


def extra_fields(pining_nonnull: Mapping[str, bool], public_fields: Set[str]) -> frozenset[str]:
    """The pining fields with a non-null value that the public copy does not have at all."""
    return frozenset(name for name, nonnull in pining_nonnull.items() if nonnull and name not in public_fields)


def choose_copy(public: CopyInfo | None, pining: CopyInfo | None) -> Choice:
    """The kept copy and the match's visibility (the kept copy's own flag)."""
    if public is not None and public.channel != "public":
        raise ValueError("the public copy must come from the public channel")
    if pining is not None and pining.channel != "pining":
        raise ValueError("the pining copy must come from the pining channel")
    if pining is None:
        if public is None:
            raise ValueError("a choice needs at least one copy")
        return Choice(kept="public", visibility=public.visibility, reason="only the public copy")
    if public is None:
        return Choice(kept="pining", visibility=pining.visibility, reason="only the pining copy")
    extras = extra_fields(pining.nonnull_fields, set(public.nonnull_fields))
    if extras:
        listed = ", ".join(sorted(extras))
        return Choice(kept="pining", visibility=pining.visibility, reason=f"the pining copy has extra fields: {listed}")
    return Choice(kept="public", visibility=public.visibility, reason="no extra fields in the pining copy")
