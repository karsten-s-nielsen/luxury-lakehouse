"""Channel precedence and the kept copy's visibility (spec §9, §29.2; §23.6 "Precedence is order-independent",
"Kept-copy visibility")."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime

from lakekit import LakeKit

from lakehouse.lineage.model import LINEAGE_TABLE, LineageRow, read_lineage, write_lineage
from lakehouse.lineage.precedence import Choice, CopyInfo, choose_copy, extra_fields

PUBLIC = CopyInfo(
    channel="public",
    visibility="public",
    content_sha256="a" * 64,
    nonnull_fields={"x": True, "y": True, "speed": False},
)
PINING_EXTRAS_PRIVATE = CopyInfo(
    channel="pining", visibility="private", content_sha256="b" * 64, nonnull_fields={"x": True, "y": True, "z": True}
)
PINING_EXTRAS_PUBLIC = CopyInfo(
    channel="pining", visibility="public", content_sha256="c" * 64, nonnull_fields={"x": True, "y": True, "z": True}
)
PINING_PLAIN = CopyInfo(
    channel="pining", visibility="private", content_sha256="d" * 64, nonnull_fields={"x": True, "y": False, "z": False}
)


def test_extra_fields_rule() -> None:
    # a field counts only when the pining copy has a non-null value and the public copy lacks the field entirely
    assert extra_fields({"x": True, "z": True, "w": False}, {"x", "y"}) == frozenset({"z"})
    assert extra_fields({"speed": True}, {"speed"}) == frozenset()  # present in public, even if all null there


def test_private_pining_with_extras_private() -> None:
    choice = choose_copy(PUBLIC, PINING_EXTRAS_PRIVATE)
    assert (choice.kept, choice.visibility) == ("pining", "private")
    assert "z" in choice.reason


def test_public_pining_with_extras_stays_public() -> None:
    choice = choose_copy(PUBLIC, PINING_EXTRAS_PUBLIC)
    assert (choice.kept, choice.visibility) == ("pining", "public")


def test_no_extras_public_wins() -> None:
    choice = choose_copy(PUBLIC, PINING_PLAIN)
    assert (choice.kept, choice.visibility) == ("public", "public")
    assert "no extra fields" in choice.reason


def test_single_copies() -> None:
    assert choose_copy(PUBLIC, None) == Choice(kept="public", visibility="public", reason="only the public copy")
    only = choose_copy(None, PINING_PLAIN)
    assert (only.kept, only.visibility) == ("pining", "private")


def _latest(order: tuple[CopyInfo, ...] | list[CopyInfo]) -> tuple[CopyInfo | None, CopyInfo | None]:
    latest: dict[str, CopyInfo] = {}
    for copy in order:
        latest[copy.channel] = copy
    return latest.get("public"), latest.get("pining")


def test_order_independent() -> None:
    """The choice depends on the current copies only, never on their order of arrival."""
    copies = [PUBLIC, PINING_EXTRAS_PRIVATE]
    assert len({choose_copy(*_latest(order)) for order in itertools.permutations(copies)}) == 1
    # an update: the pining copy changes from no extras to extras, and the public copy arrives at any point
    timelines = [
        [PUBLIC, PINING_PLAIN, PINING_EXTRAS_PRIVATE],
        [PINING_PLAIN, PUBLIC, PINING_EXTRAS_PRIVATE],
        [PINING_PLAIN, PINING_EXTRAS_PRIVATE, PUBLIC],
    ]
    assert {choose_copy(*_latest(t)) for t in timelines} == {choose_copy(PUBLIC, PINING_EXTRAS_PRIVATE)}
    # and back: the extras disappear again, so the public copy wins again
    assert choose_copy(*_latest([PINING_EXTRAS_PRIVATE, PUBLIC, PINING_PLAIN])) == choose_copy(PUBLIC, PINING_PLAIN)


def test_lineage_rows_roundtrip(kit: LakeKit) -> None:
    cat = kit.catalog()
    when = datetime(2026, 10, 26, 12, tzinfo=UTC)
    rows = [
        LineageRow(
            match_key=-42,
            provider="idsse",
            channel="pining",
            channel_id="DFL-MAT-J03WPY",
            artifact="positions",
            change_signal="2026-10-01T00:00:00Z",
            delivery_sha256="1" * 64,
            content_sha256="2" * 64,
            wrapper="gzip",
            first_seen=when,
            last_listed=when,
            visibility="private",
            kept=True,
            reason="the pining copy has extra fields: z",
        ),
        LineageRow(
            match_key=-42,
            provider="idsse",
            channel="public",
            channel_id="J03WPY",
            artifact="positions",
            change_signal=None,
            delivery_sha256="3" * 64,
            content_sha256="4" * 64,
            wrapper="none",
            first_seen=when,
            last_listed=when,
            visibility="public",
            kept=False,
            reason="not kept",
        ),
    ]
    write_lineage(cat.conn, rows)
    assert read_lineage(cat.conn) == sorted(rows, key=lambda r: (r.match_key, r.channel, r.artifact, r.content_sha256))
    assert LINEAGE_TABLE == "ops_lineage"
    cat.close()
