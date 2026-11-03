"""Catalog restore (spec §10.8, §29.10, §29.11, §29.12; §23.6 "Catalog restore")."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import pytest
from lakekit import PLAYERS, LakeKit, content_sum, move_now

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.moves import MoveRecord, read_moves, record_move
from lakehouse.catalog.publish import listed_sequences, read_current, version_key
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.restore import read_restores, restore
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.errors import ConflictError, GuardStopError, IntegrityError
from lakehouse.runs.history import MatchRow, read_table, replace_match
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.lease import Lease, RunAnchor, anchor_intervals, anchor_key, marker_key

M = 8001


def _three_versions(kit: LakeKit) -> None:
    """Versions 1, 2, 3 with 1, 2, 3 rows of match M: each version's content is distinct."""
    cat = kit.catalog()
    for version in (1, 2, 3):
        if version == 1:
            kit.add_match(cat, M, visibility="public", frames=1, offset=0)
        else:
            kit.replace_match(cat, M, visibility="public", frames=version, offset=0)
        cat.commit("run_end")
    cat.close()


def test_restore_takes_lease_and_records(kit: LakeKit) -> None:
    _three_versions(kit)
    holder = Lease.acquire(kit.storage, holder_id="a-run", run_id="run-busy")
    assert holder is not None
    with pytest.raises(ConflictError, match="lease"):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage).seq == 3
    holder.release()
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert (record.operator, record.restored_seq, record.new_seq) == ("ops", 1, 4)
    # the restore released its lease: the next run can take it at once
    after = Lease.acquire(kit.storage, holder_id="next", run_id="run-next")
    assert after is not None
    after.release()


def test_restore_drops_newer_versions(kit: LakeKit) -> None:
    _three_versions(kit)
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert record.dropped_seqs == [2, 3]
    assert read_current(kit.storage).seq == 4
    assert listed_sequences(kit.storage) == [1, 4]
    assert kit.read_match(M) == (1, content_sum(1, 0))


def test_restore_record_in_copy(kit: LakeKit) -> None:
    _three_versions(kit)
    record = restore(kit.storage, target_seq=2, operator="ops")
    with CatalogReader(kit.storage).session() as conn:
        rows = read_restores(conn)
    assert rows == [record]
    assert record.dropped_seqs == [3]
    assert read_manifest(kit.storage, record.new_seq).kind == "run_end"


def test_next_run_starts_from_restored(kit: LakeKit) -> None:
    _three_versions(kit)
    restore(kit.storage, target_seq=1, operator="ops")
    cat = kit.catalog("after-restore")
    assert cat.current is not None
    assert cat.current.seq == 4
    assert cat.next_seq == 5
    count = cat.conn.execute(f"SELECT count(*) FROM lake.{PLAYERS}").fetchone()  # noqa: S608 - constant name
    assert count == (1,)
    cat.close()


def test_restore_refuses_current_and_missing(kit: LakeKit) -> None:
    _three_versions(kit)
    with pytest.raises(ValueError, match="already"):
        restore(kit.storage, target_seq=3, operator="ops")
    with pytest.raises(FileNotFoundError):
        restore(kit.storage, target_seq=9, operator="ops")


def test_restore_checks_every_named_file_exists(kit: LakeKit) -> None:
    cat = kit.catalog()
    first = kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    cat.commit("run_end")
    kit.replace_match(cat, M, visibility="public", frames=3, offset=0)
    cat.commit("run_end")
    cat.close()
    kit.storage.delete("lake", first.key.path)  # version 1 names a file that is gone
    before = read_current(kit.storage)
    with pytest.raises(IntegrityError, match=str(first.key)):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage) == before
    assert listed_sequences(kit.storage) == [1, 2]


def test_two_successive_restores_keep_match_private(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=4, offset=0)
    cat.commit("run_end")  # 1: public
    move_now(cat, kit.storage, M, "restricted")  # 2: the move's commit
    kit.add_match(cat, M + 1, visibility="public", frames=1, offset=0)
    cat.commit("run_end")  # 3
    cat.close()
    moves_before = None
    for target in (2, 1):  # N-1, then N-2
        restore(kit.storage, target_seq=target, operator="ops")
        manifest = read_manifest(kit.storage, read_current(kit.storage).seq)
        match_keys = [k for k in manifest.canonical_keys if f"/{M}-" in k.path]
        assert match_keys
        assert {k.root for k in match_keys} == {"restricted"}
        assert kit.objects("lake", f"data/bronze/{PLAYERS}/{M}-") == []
        assert kit.read_match(M) == (4, content_sum(4, 0))
        with CatalogReader(kit.storage).session() as conn:
            moves = read_moves(conn)
        assert [m.direction for m in moves] == ["to_private"]
        moves_before = moves_before or moves
        assert moves == moves_before


def test_restore_across_both_moves_latest_visibility(kit: LakeKit) -> None:
    cat = kit.catalog()
    original = kit.add_match(cat, M, visibility="public", frames=5, offset=7)
    cat.commit("run_end")  # 1
    move_now(cat, kit.storage, M, "restricted")  # 2
    move_now(cat, kit.storage, M, "lake")  # 3
    cat.close()
    restore(kit.storage, target_seq=1, operator="ops")
    manifest = read_manifest(kit.storage, read_current(kit.storage).seq)
    keys = [k for k in manifest.canonical_keys if f"/{M}-" in k.path]
    assert len(keys) == 1
    assert keys[0].root == "lake"
    assert keys[0] != original.key  # the deleted lake key resolves to a new lake copy
    assert kit.read_match(M) == (5, content_sum(5, 7))


def test_restore_resolves_deleted_lake_key_when_public_again(kit: LakeKit) -> None:
    cat = kit.catalog()
    original = kit.add_match(cat, M, visibility="public", frames=3, offset=1)
    cat.commit("run_end")
    move_now(cat, kit.storage, M, "restricted")
    move_now(cat, kit.storage, M, "lake")
    cat.close()
    assert not kit.storage.exists("lake", original.key.path)
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert record.visibility_fixes >= 2  # the deleted key resolved, then copied back to the lake root
    manifest = read_manifest(kit.storage, record.new_seq)
    assert original.key not in manifest.canonical_keys
    assert all(kit.storage.exists(k.root, k.path) for k in manifest.canonical_keys)
    with CatalogReader(kit.storage).session() as conn:
        directions = [m.direction for m in read_moves(conn)]
    assert directions == ["to_private", "to_public", "to_public"]
    assert kit.read_match(M) == (3, content_sum(3, 1))


def test_restore_private_to_public_copies_from_restricted(kit: LakeKit) -> None:
    cat = kit.catalog()
    private = kit.add_match(cat, M, visibility="private", frames=4, offset=20)
    cat.commit("run_end")  # 1: private
    kit.replace_match(cat, M, visibility="private", frames=4, offset=30)
    cat.commit("run_end")  # 2: new private content
    move_now(cat, kit.storage, M, "lake")  # 3: public now
    cat.close()
    record = restore(kit.storage, target_seq=1, operator="ops")
    manifest = read_manifest(kit.storage, record.new_seq)
    keys = [k for k in manifest.canonical_keys if f"/{M}-" in k.path]
    assert [k.root for k in keys] == ["lake"]
    assert kit.storage.get("lake", keys[0].path) == kit.storage.get("restricted", private.key.path)
    assert kit.read_match(M) == (4, content_sum(4, 20))  # the target version's content


def test_restore_after_replace_then_private_reads_old_content(kit: LakeKit) -> None:
    """§29.11 (A-SPEC-48 / B26): restore to before run k gives run k-1's content from the restricted root."""
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=6, offset=0)
    cat.commit("run_end")  # 1: Lx
    kit.replace_match(cat, M, visibility="public", frames=6, offset=100)
    cat.commit("run_end")  # 2: Ly
    move_now(cat, kit.storage, M, "restricted")  # 3: private
    cat.close()
    restore(kit.storage, target_seq=1, operator="ops")
    manifest = read_manifest(kit.storage, read_current(kit.storage).seq)
    assert {k.root for k in manifest.canonical_keys if f"/{M}-" in k.path} == {"restricted"}
    assert kit.objects("lake", f"data/bronze/{PLAYERS}/{M}-") == []
    assert kit.read_match(M) == (6, content_sum(6, 0))


def test_restore_after_change_and_private_same_run(kit: LakeKit) -> None:
    cat = kit.catalog("run-1")
    kit.add_match(cat, M, visibility="public", frames=5, offset=0)
    cat.commit("run_end")
    cat.close()
    cat = kit.catalog("run-2")
    move_now(cat, kit.storage, M, "restricted")
    kit.replace_match(cat, M, visibility="private", frames=5, offset=50)
    cat.commit("run_end")
    cat.close()
    restore(kit.storage, target_seq=1, operator="ops")
    assert kit.objects("lake", f"data/bronze/{PLAYERS}/{M}-") == []
    assert kit.read_match(M) == (5, content_sum(5, 0))


def test_restore_stops_on_record_without_copy(kit: LakeKit) -> None:
    cat = kit.catalog()
    old = kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    cat.commit("run_end")  # 1 names `old`
    kit.replace_match(cat, M, visibility="public", frames=2, offset=4)
    cat.commit("run_end")  # 2
    # version 1's manifest leaves the kept set, so the move gives `old` a record with no copy
    manifest_one = kit.storage.get("lake", "catalog/v/1.files.json")
    kit.storage.delete("lake", "catalog/v/1.files.json")
    move_now(cat, kit.storage, M, "restricted")
    cat.close()
    kit.storage.put("lake", "catalog/v/1.files.json", manifest_one)  # an older kept version names it again
    before = read_current(kit.storage)
    with pytest.raises(GuardStopError, match=str(old.key)):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage) == before
    assert kit.storage.exists("lake", version_key(1))


def test_restore_refuses_lake_file_of_private_match(kit: LakeKit) -> None:
    """Pass 2 guard: a now-private match whose copy names a lake-root file that no record covers stops the restore."""
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    cat.commit("run_end")  # 1
    other = CanonicalKey("lake", f"data/bronze/{PLAYERS}/{M}-v1-{'d' * 32}.parquet")  # not the file version 1 names
    record_move(cat, MoveRecord(M, "to_private", other, None, datetime.now(UTC), "crafted"))
    cat.commit("run_end")  # 2: the match is private now, by its newest record
    cat.close()
    before = read_current(kit.storage)
    with pytest.raises(GuardStopError, match="private now"):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage) == before


def test_restore_checks_target_version_hash(kit: LakeKit) -> None:
    _three_versions(kit)
    data = bytearray(kit.storage.get("lake", version_key(1)))
    data[-1] ^= 0xFF
    kit.storage.delete("lake", version_key(1))
    kit.storage.put("lake", version_key(1), bytes(data))
    before = read_current(kit.storage)
    with pytest.raises(IntegrityError, match="version 1"):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage) == before


def _flag(cat: WorkingCatalog, match_key: int, visibility: Literal["public", "private"]) -> None:
    """The match's `ops_matches` row with its visibility flag, as a run writes it (Task 21)."""
    row = MatchRow(
        match_key=match_key,
        provider="idsse",
        native_match_id=str(match_key),
        visibility=visibility,
        kept_channel="pining",
        kept_reason="only copy",
        competition_id=None,
        competition_name=None,
        season_id=None,
        season_name=None,
        has_360_frames=None,
        run_id=cat.run_id,
        updated_at=datetime.now(UTC),
    )
    replace_match(cat.conn, row, [], [])


def _flags(kit: LakeKit) -> dict[int, str]:
    with CatalogReader(kit.storage).session() as conn:
        return {row["match_key"]: row["visibility"] for row in read_table(conn, "ops_matches")}


def test_restore_sets_visibility_flag_from_latest_move(kit: LakeKit) -> None:
    """Plan Task 11 step 3, carried to Task 21: the restored copy's `ops_matches` flag is the match's latest
    visibility, and the flag fix counts in `visibility_fixes`."""
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    _flag(cat, M, "public")
    kit.add_match(cat, M + 1, visibility="public", frames=1, offset=0)
    _flag(cat, M + 1, "public")
    cat.commit("run_end")  # 1: both public
    move_now(cat, kit.storage, M, "restricted")
    _flag(cat, M, "private")
    cat.commit("run_end")  # 2: M private, as a run moves it
    cat.close()
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert _flags(kit) == {M: "private", M + 1: "public"}  # M + 1 has no move record: its flag stays
    assert record.visibility_fixes == 2  # one repointed file, one flag
    assert kit.read_match(M) == (2, content_sum(2, 0))


def test_restore_stops_when_flag_and_move_record_disagree(kit: LakeKit) -> None:
    """The pre-restore `CURRENT` says private by its newest move record but public by its flag: nothing is
    published."""
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=2, offset=0)
    _flag(cat, M, "public")
    cat.commit("run_end")  # 1
    move_now(cat, kit.storage, M, "restricted")
    cat.commit("run_end")  # 2: moved, but the flag still says public
    cat.close()
    before, versions = read_current(kit.storage), listed_sequences(kit.storage)
    with pytest.raises(GuardStopError, match=f"match {M}.*public.*to_private"):
        restore(kit.storage, target_seq=1, operator="ops")
    assert read_current(kit.storage) == before
    assert listed_sequences(kit.storage) == versions


# --- the run anchors of undone runs (T3 spec D5; plan round 3 Task 7) -----------------------------------------------


def _anchor_for(kit: LakeKit, generation: int, run_id: str) -> None:
    body = RunAnchor(generation=generation, run_id=run_id).model_dump_json().encode()
    kit.storage.put("lake", anchor_key(generation), body)
    kit.storage.put("lake", marker_key(generation), body)


def _undone_run(kit: LakeKit) -> None:
    """Version 1 (run-0), then version 2 published by run-1; anchors 5 (run-1) and 6 (run-x, published nothing)."""
    for run_id, match in (("run-0", 1), ("run-1", 2)):
        cat = kit.catalog(run_id)
        kit.add_match(cat, match, visibility="public", frames=2, offset=0)
        cat.commit("run_end")
        cat.close()
    _anchor_for(kit, 5, "run-1")
    _anchor_for(kit, 6, "run-x")


def test_restore_removes_anchors_of_dropped_runs_before_publish(kit: LakeKit) -> None:
    _undone_run(kit)
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert record.dropped_seqs == [2]
    runs = {interval.generation: interval.run_id for interval in anchor_intervals(kit.storage)}
    assert 5 not in runs  # run-1's versions were dropped: its files count again at the next sweep
    assert runs[6] == "run-x"


def test_failed_restore_publish_has_already_removed_the_anchor(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    _undone_run(kit)

    def failing(self: WorkingCatalog, kind: str) -> None:
        raise OSError("the publish failed")

    monkeypatch.setattr(WorkingCatalog, "commit", failing)
    with pytest.raises(OSError, match="publish failed"):
        restore(kit.storage, target_seq=1, operator="ops")
    assert 5 not in {interval.generation for interval in anchor_intervals(kit.storage)}  # fail closed
