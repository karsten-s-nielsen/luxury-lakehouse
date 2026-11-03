"""The raw-file half of a private move (spec §9, §10.8, §29.12): carried from Tasks 11 and 12 to after Task 13."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from lakekit import LakeKit, move_now

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.moves import (
    delete_moved_leftovers,
    prune_settled_raw_records,
    read_moves,
)
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.lineage.model import LineageRow, public_contents, read_lineage, write_lineage
from lakehouse.raw.archive import Delivery, RawArchive, raw_key
from lakehouse.storage.keys import CanonicalKey, Root

M = 5001
NOW = datetime(2026, 10, 26, tzinfo=UTC)


def _pining_delivery(kit: LakeKit, cat: WorkingCatalog, *, shared_with_public: bool) -> str:
    """Match M: a public bronze file and a pining delivery flagged public, archived in the lake root. With
    `shared_with_public`, a public-channel delivery of another match has the same content."""
    kit.add_match(cat, M, visibility="public", frames=3, offset=0)
    delivery = Delivery.received(
        provider="idsse",
        channel="pining",
        native_match_id="J03WPY",
        artifact="positions",
        raw=b"<positions/>" * 40,
        visibility="public",
        change_signal="2026-10-01",
        fetched_at=NOW,
    )
    stored = RawArchive(kit.storage, enabled=True).store(delivery)
    assert stored == CanonicalKey("lake", raw_key(delivery.content_sha256))
    rows = [_row(M, "pining", delivery.content_sha256)]
    if shared_with_public:
        rows.append(_row(M + 1, "public", delivery.content_sha256))
    write_lineage(cat.conn, rows)
    kit.commit(cat)
    return delivery.content_sha256


def _row(match_key: int, channel: str, content: str) -> LineageRow:
    return LineageRow(
        match_key=match_key,
        provider="idsse",
        channel="pining" if channel == "pining" else "public",
        channel_id="DFL-MAT-J03WPY" if channel == "pining" else "J03WPY",
        artifact="positions",
        change_signal=None,
        delivery_sha256="9" * 64,
        content_sha256=content,
        wrapper="none",
        first_seen=NOW,
        last_listed=NOW,
        visibility="public",
        kept=channel == "pining",
        reason="test",
    )


def _manifest(kit: LakeKit) -> frozenset[CanonicalKey]:
    return frozenset(read_manifest(kit.storage, read_current(kit.storage).seq).canonical_keys)


def _run_start(kit: LakeKit, cat: WorkingCatalog) -> tuple[int, int]:
    moves = read_moves(cat.conn)
    public = public_contents(read_lineage(cat.conn))
    deleted = delete_moved_leftovers(kit.storage, moves, current_manifest=_manifest(kit), public_contents=public)
    return deleted, prune_settled_raw_records(cat.conn, kit.storage, public_contents=public)


def test_private_move_moves_pining_raw(kit: LakeKit) -> None:
    cat = kit.catalog()
    content = _pining_delivery(kit, cat, shared_with_public=False)
    records = move_now(cat, kit.storage, M, "restricted", archive=RawArchive(kit.storage, enabled=True))
    raw = [r for r in records if r.old_key.path.startswith("raw/")]
    assert [(r.old_key, r.new_key) for r in raw] == [
        (CanonicalKey("lake", raw_key(content)), CanonicalKey("restricted", raw_key(content)))
    ]
    assert kit.storage.exists("restricted", raw_key(content))
    assert not kit.storage.exists("lake", raw_key(content))
    assert RawArchive(kit.storage, enabled=True).read(content) == b"<positions/>" * 40
    cat.close()


class _CountingArchive(RawArchive):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.calls = 0

    def copy_to(self, content_sha256: str, root: Root) -> CanonicalKey | None:
        self.calls += 1
        return super().copy_to(content_sha256, root)

    def locate(self, content_sha256: str) -> CanonicalKey | None:
        self.calls += 1
        return super().locate(content_sha256)


def test_private_move_archive_off_no_raw(kit: LakeKit) -> None:
    """With the archive off, a private move makes no raw call and writes no raw record, even when an older lake-root
    raw object of the match exists (for example from before the archive was switched off)."""
    cat = kit.catalog()
    content = _pining_delivery(kit, cat, shared_with_public=False)
    archive = _CountingArchive(kit.storage, enabled=False)
    records = move_now(cat, kit.storage, M, "restricted", archive=archive)
    assert archive.calls == 0
    assert [r for r in records if r.old_key.path.startswith("raw/")] == []
    assert kit.objects("restricted", "raw/") == []
    assert kit.storage.exists("lake", raw_key(content))
    cat.close()


def test_shared_raw_object_stays(kit: LakeKit) -> None:
    cat = kit.catalog()
    content = _pining_delivery(kit, cat, shared_with_public=True)
    records = move_now(cat, kit.storage, M, "restricted", archive=RawArchive(kit.storage, enabled=True))
    assert any(r.old_key == CanonicalKey("lake", raw_key(content)) for r in records)
    assert kit.storage.exists("restricted", raw_key(content))
    assert kit.storage.exists("lake", raw_key(content))  # public at its source: it stays
    deleted, pruned = _run_start(kit, cat)
    assert deleted == 0
    assert kit.storage.exists("lake", raw_key(content))  # the run-start delete skips it as well
    assert pruned == 1  # skipped for that reason, so its record is settled
    assert not [r for r in read_moves(cat.conn) if r.old_key.path.startswith("raw/")]
    cat.close()


def test_raw_record_survives_failed_delete(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """§10.8: the private move's raw delete fails without stopping the run; that run's cleanup keeps the raw record;
    the next run start deletes the lake-root raw object and only then prunes the record."""
    cat = kit.catalog("run-1")
    content = _pining_delivery(kit, cat, shared_with_public=False)
    real_delete = kit.storage.delete

    def raw_delete_fails(root: Root, key: str, *, missing_ok: bool = False) -> None:
        if key.startswith("raw/"):
            raise OSError("the storage refused the delete")
        real_delete(root, key, missing_ok=missing_ok)

    monkeypatch.setattr(kit.storage, "delete", raw_delete_fails)
    move_now(cat, kit.storage, M, "restricted", archive=RawArchive(kit.storage, enabled=True))
    monkeypatch.undo()
    assert kit.storage.exists("lake", raw_key(content))
    time.sleep(0.01)
    kit.cleanup(cat)  # that run's cleanup keeps the raw record
    assert [r.old_key for r in read_moves(cat.conn) if r.old_key.path.startswith("raw/")] == [
        CanonicalKey("lake", raw_key(content))
    ]
    cat.close()
    cat = kit.catalog("run-2")
    deleted, pruned = _run_start(kit, cat)
    assert deleted == 1
    assert not kit.storage.exists("lake", raw_key(content))
    assert pruned == 1
    assert not [r for r in read_moves(cat.conn) if r.old_key.path.startswith("raw/")]
    cat.close()
