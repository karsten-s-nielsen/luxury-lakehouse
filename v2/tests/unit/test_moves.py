"""Visibility moves per match and their records (spec §9, §29.7, §29.10, §29.11, §29.12)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from lakekit import PLAYERS, LakeKit, content_sum, move_now

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.moves import (
    MoveRecord,
    assert_no_moved_key_registered,
    delete_moved_leftovers,
    match_lake_objects,
    read_moves,
)
from lakehouse.catalog.publish import manifest_key, read_current, version_key
from lakehouse.errors import GuardStopError
from lakehouse.storage.keys import CanonicalKey

M = 7001


class _Crash(BaseException):
    """A process death: nothing catches it."""


def _manifest_now(kit: LakeKit) -> frozenset[CanonicalKey]:
    return frozenset(read_manifest(kit.storage, read_current(kit.storage).seq).canonical_keys)


def test_match_lake_objects_sets(kit: LakeKit) -> None:
    cat = kit.catalog()
    first = kit.add_match(cat, M, visibility="public", frames=10, offset=0)
    kit.add_match(cat, M + 1, visibility="public", frames=3, offset=0)  # another match: never part of the sets
    cat.commit("run_end")
    second = kit.replace_match(cat, M, visibility="public", frames=10, offset=5)
    cat.commit("run_end")
    stray = f"data/bronze/{PLAYERS}/{M}-v1-{'f' * 32}.parquet"
    kit.storage.put("lake", stray, b"an upload of a crashed run")
    objects = match_lake_objects(cat, kit.storage, M)
    assert objects.registered == frozenset({second.key})
    assert objects.kept == frozenset({first.key})  # version 1's manifest still names it
    assert objects.tracked_only == frozenset()
    assert objects.stray == frozenset({CanonicalKey("lake", stray)})
    cat.close()


def test_public_to_private_move_deletes_lake_copy(kit: LakeKit) -> None:
    cat = kit.catalog()
    public = kit.add_match(cat, M, visibility="public", frames=10, offset=0)
    cat.commit("run_end")
    records = move_now(cat, kit.storage, M, "restricted")
    # right after the move's catalog commit: the lake object is gone and the record exists
    assert not kit.storage.exists("lake", public.key.path)
    assert [(r.direction, r.old_key) for r in records] == [("to_private", public.key)]
    new_key = records[0].new_key
    assert new_key is not None
    assert new_key.root == "restricted"
    assert kit.storage.exists("restricted", new_key.path)
    assert read_moves(cat.conn) == records
    assert kit.objects("lake") == []
    assert kit.read_match(M) == (10, content_sum(10, 0))
    cat.close()


def test_private_public_private_roundtrip(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="private", frames=8, offset=3)
    cat.commit("run_end")
    move_now(cat, kit.storage, M, "lake")
    current = _manifest_now(kit)
    assert {k.root for k in current} == {"lake"}
    assert kit.read_match(M) == (8, content_sum(8, 3))
    move_now(cat, kit.storage, M, "restricted")
    current = _manifest_now(kit)
    assert {k.root for k in current} == {"restricted"}
    assert kit.objects("lake") == []
    assert kit.read_match(M) == (8, content_sum(8, 3))
    cat.close()


def test_crash_between_commit_and_delete(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    cat = kit.catalog()
    public = kit.add_match(cat, M, visibility="public", frames=4, offset=0)
    cat.commit("run_end")

    def crash(root: str, key: str, *, missing_ok: bool = False) -> None:
        raise _Crash("simulated crash after the move's catalog commit")

    monkeypatch.setattr(kit.storage, "delete", crash)
    with pytest.raises(_Crash):
        move_now(cat, kit.storage, M, "restricted")
    monkeypatch.undo()
    assert kit.storage.exists("lake", public.key.path)  # the crash left the lake copy
    assert read_current(kit.storage).seq == 2  # but the move's commit was published
    deleted = delete_moved_leftovers(kit.storage, read_moves(cat.conn), current_manifest=_manifest_now(kit))
    assert deleted == 1
    assert not kit.storage.exists("lake", public.key.path)
    assert delete_moved_leftovers(kit.storage, read_moves(cat.conn), current_manifest=_manifest_now(kit)) == 0
    cat.close()


def test_move_back_gets_new_key(kit: LakeKit) -> None:
    cat = kit.catalog()
    public = kit.add_match(cat, M, visibility="public", frames=5, offset=0)
    cat.commit("run_end")
    to_private = move_now(cat, kit.storage, M, "restricted")
    to_public = move_now(cat, kit.storage, M, "lake")
    assert [r.direction for r in to_private] == ["to_private"]
    assert [r.direction for r in to_public] == ["to_public"]
    final = to_public[0].new_key
    assert final is not None
    assert final.root == "lake"
    assert final != to_private[0].old_key == public.key
    assert [r.direction for r in read_moves(cat.conn)] == ["to_private", "to_public"]
    assert kit.read_match(M) == (5, content_sum(5, 0))
    cat.close()


def test_moved_key_registered_guard(kit: LakeKit) -> None:
    cat = kit.catalog()
    public = kit.add_match(cat, M, visibility="public", frames=3, offset=0)
    cat.commit("run_end")
    crafted = MoveRecord(
        match_key=M, direction="to_private", old_key=public.key, new_key=None, at=datetime.now(UTC), run_id="bug"
    )
    with pytest.raises(GuardStopError, match=str(public.key)):
        assert_no_moved_key_registered([crafted], _manifest_now(kit))
    with pytest.raises(GuardStopError):
        delete_moved_leftovers(kit.storage, [crafted], current_manifest=_manifest_now(kit))
    assert kit.storage.exists("lake", public.key.path)
    cat.close()


def test_replace_then_private_later_run(kit: LakeKit) -> None:
    cat = kit.catalog("run-k-1")
    lx = kit.add_match(cat, M, visibility="public", frames=6, offset=0)
    cat.commit("run_end")  # version 1 names Lx
    cat.close()
    cat = kit.catalog("run-k")
    ly = kit.replace_match(cat, M, visibility="public", frames=6, offset=100)
    cat.commit("run_end")  # version 2 names Ly; version 1 still names Lx
    cat.close()
    cat = kit.catalog("run-k+1")
    records = move_now(cat, kit.storage, M, "restricted")
    cat.close()
    assert not kit.storage.exists("lake", lx.key.path)
    assert not kit.storage.exists("lake", ly.key.path)
    by_old = {r.old_key: r for r in records}
    assert set(by_old) == {lx.key, ly.key}
    for record in by_old.values():
        assert record.direction == "to_private"
        assert record.new_key is not None
        assert kit.storage.exists("restricted", record.new_key.path)
    assert kit.read_match(M) == (6, content_sum(6, 100))


def test_tracked_only_object_gets_record(kit: LakeKit) -> None:
    cat = kit.catalog()
    old = kit.add_match(cat, M, visibility="public", frames=4, offset=0)
    cat.commit("run_end")
    new = kit.replace_match(cat, M, visibility="public", frames=4, offset=9)
    cat.commit("run_end")
    for key in (version_key(1), manifest_key(1)):  # version 1 left retention: nothing kept names `old` any more
        kit.storage.delete("lake", key)
    objects = match_lake_objects(cat, kit.storage, M)
    assert objects.tracked_only == frozenset({old.key})
    records = move_now(cat, kit.storage, M, "restricted")
    by_old = {r.old_key: r for r in records}
    assert by_old[old.key].new_key is None
    assert by_old[new.key].new_key is not None
    assert not kit.storage.exists("lake", old.key.path)
    cat.close()


def test_stray_upload_of_newly_private_match_deleted(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=3, offset=0)
    cat.commit("run_end")
    stray = f"data/bronze/{PLAYERS}/{M}-v1-{'e' * 32}.parquet"
    kit.storage.put("lake", stray, b"an unregistered upload")
    other = f"data/bronze/{PLAYERS}/{M}0-v1-{'e' * 32}.parquet"  # match 70010: not this match
    kit.storage.put("lake", other, b"another match")
    records = move_now(cat, kit.storage, M, "restricted")
    stray_record = next(r for r in records if r.old_key == CanonicalKey("lake", stray))
    assert stray_record.new_key is None
    assert not kit.storage.exists("lake", stray)
    assert kit.storage.exists("lake", other)
    cat.close()


def test_change_and_private_same_run(kit: LakeKit) -> None:
    cat = kit.catalog("run-1")
    kit.add_match(cat, M, visibility="public", frames=5, offset=0)
    cat.commit("run_end")
    cat.close()
    cat = kit.catalog("run-2")
    move_now(cat, kit.storage, M, "restricted")  # move first ...
    kit.replace_match(cat, M, visibility="private", frames=5, offset=50)  # ... then replace (§29.10)
    cat.commit("run_end")
    cat.close()
    assert kit.objects("lake") == []
    assert kit.read_match(M) == (5, content_sum(5, 50))


def test_move_without_files_is_a_no_op(kit: LakeKit, tmp_path: Path) -> None:
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    cat.commit("run_end")
    assert move_now(cat, kit.storage, M + 5, "restricted") == []
    assert move_now(cat, kit.storage, M, "lake") == []  # already public: no restricted files
    assert read_current(kit.storage).seq == 1
    cat.close()
