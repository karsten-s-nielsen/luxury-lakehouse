"""Cleanup: version retention, the guarded DuckLake cleanup and the orphan sweep (spec §10.3 steps 6-8, §10.7, §10.8,
§29.11; §23.6 "Cleanup guard", "Sweep with all path forms", "Crashed-run version cap")."""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest
from lakekit import PLAYERS, T0, LakeKit, StubLease, content_sum, move_now

from lakehouse.catalog import cleanup
from lakehouse.catalog.cleanup import kept_file_set, prune_versions
from lakehouse.catalog.manifest import VersionKind, read_manifest
from lakehouse.catalog.moves import MoveRecord, read_moves, record_move
from lakehouse.catalog.publish import listed_sequences, manifest_key, read_current, version_key
from lakehouse.catalog.restore import restore
from lakehouse.errors import ConflictError, GuardStopError, LeaseLostError
from lakehouse.storage.keys import CanonicalKey, Root, RootMap, StorageRootInfo
from lakehouse.storage.lease import (
    LEASE_TTL,
    TAKEOVER_GRACE,
    Lease,
    RunAnchor,
    anchor_intervals,
    anchor_key,
    marker_key,
)
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

BASE = 25  # matches in the base lake: one orphan stays well under the 10 % sweep guard


def _base(kit: LakeKit, *, private: tuple[int, ...] = ()) -> dict[int, CanonicalKey]:
    """Day 0: a run-end version with matches 1..BASE (those in `private` in the restricted root)."""
    kit.day(0)
    cat = kit.catalog("run-0")
    keys = {
        m: kit.add_match(cat, m, visibility="private" if m in private else "public", frames=3, offset=m).key
        for m in range(1, BASE + 1)
    }
    kit.commit(cat)
    cat.close()
    return keys


def _orphan(kit: LakeKit, match_key: int = 900, root: str = "lake") -> CanonicalKey:
    key = f"data/bronze/{PLAYERS}/{match_key}-v1-{uuid.uuid4().hex}.parquet"
    kit.storage.put("lake" if root == "lake" else "restricted", key, b"an object no catalog names")
    return CanonicalKey("lake" if root == "lake" else "restricted", key)


def _exists(kit: LakeKit, key: CanonicalKey) -> bool:
    return kit.storage.exists(key.root, key.path)


def _run_with_cleanup(
    kit: LakeKit, day: float, run_id: str, *, allow_large_sweep: bool = False
) -> cleanup.CleanupReport:
    kit.day(day)
    cat = kit.catalog(run_id)
    try:
        return kit.cleanup(cat, allow_large_sweep=allow_large_sweep)
    finally:
        cat.close()


def test_sweep_removes_unlisted_orphan(kit: LakeKit) -> None:
    keys = _base(kit)
    orphan = _orphan(kit)
    time.sleep(0.01)  # the run starts after the orphan was stored
    report = _run_with_cleanup(kit, 1, "run-1")
    assert report.swept == [orphan]
    assert not _exists(kit, orphan)
    assert all(_exists(kit, key) for key in keys.values())


def test_kept_file_is_deferred(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """Option (b): a scheduled file a kept version names is deferred (left scheduled), not a fatal stop (spec D3.1)."""
    keys = _base(kit)
    kept = keys[3]

    def schedules_kept(cat: object) -> list[tuple[int, CanonicalKey, datetime]]:
        return [(1, kept, T0)]

    monkeypatch.setattr(cleanup, "_scheduled_for_deletion", schedules_kept)
    kit.day(1)
    cat = kit.catalog("run-1")
    report = cleanup.run_cleanup(
        cat, kit.storage, settings=kit.settings, lease=kit.lease_of(cat), allow_large_sweep=False, now=kit.clock.now
    )
    cat.close()
    assert report.deferred == 1  # the kept file is deferred (left scheduled), not deleted and not a stop
    assert report.missing_deferred == 0  # the kept file is still on disk
    assert report.ducklake_deleted == []
    assert _exists(kit, kept)


def test_sweep_skips_ducklake_tracked(kit: LakeKit) -> None:
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    kit.replace_match(cat, 1, visibility="public", frames=3, offset=500)
    kit.commit(cat)  # version 2 (day 0); version 1 still names the replaced file
    cat.close()
    replaced = keys[1]
    report = _run_with_cleanup(kit, 7.5, "run-7")  # version 1 leaves the kept set; its snapshots stay (8 days)
    assert 1 in report.pruned_versions
    assert replaced not in kept_file_set(kit.storage, [])
    assert _exists(kit, replaced)  # DuckLake still tracks it, so the sweep keeps it
    assert report.swept == []
    report = _run_with_cleanup(kit, 8.5, "run-8")  # the snapshots expire: DuckLake's own list names it
    assert report.ducklake_deleted == [replaced]
    assert not _exists(kit, replaced)
    assert kit.read_match(1) == (3, content_sum(3, 500))


def test_sweep_keeps_young_unlisted(kit: LakeKit, tmp_path: Path) -> None:
    _base(kit)
    kit.day(1)
    cat = kit.catalog("run-1")  # the lease starts here
    time.sleep(0.01)
    young = _orphan(kit, 901)
    old_local = tmp_path / "old.parquet"
    old_local.write_bytes(b"a move copy from an old local file")
    past = time.time() - 30 * 24 * 3600
    os.utime(old_local, (past, past))
    copy_key = f"data/bronze/{PLAYERS}/902-v1-{uuid.uuid4().hex}.parquet"
    kit.storage.put("restricted", copy_key, old_local)
    report = kit.cleanup(cat)
    cat.close()
    assert report.swept == []
    assert _exists(kit, young)
    assert kit.storage.exists("restricted", copy_key)


def test_stale_holder_sweep_spares_new_holder_files(kit: LakeKit) -> None:
    _base(kit)
    real = datetime.now(UTC)
    holder_a = Lease.acquire(kit.storage, holder_id="a", run_id="run-a", clock=lambda: real)
    assert holder_a is not None
    cat = kit.catalog("run-a", lease=None)
    later = real + LEASE_TTL + TAKEOVER_GRACE + timedelta(seconds=1)
    holder_b = Lease.acquire(kit.storage, holder_id="b", run_id="run-b", clock=lambda: later)
    assert holder_b is not None
    time.sleep(0.01)
    b_files = [_orphan(kit, 950 + i) for i in range(2)]  # uploads of the new holder, not yet in any manifest
    with pytest.raises(LeaseLostError):
        cleanup.run_cleanup(
            cat, kit.storage, settings=kit.settings, lease=holder_a, allow_large_sweep=True, now=kit.clock.now
        )
    assert all(_exists(kit, key) for key in b_files)
    stub = StubLease(run_id="run-a", started_storage_time=holder_a.started_storage_time)  # assert_held passes
    report = cleanup.run_cleanup(
        cat, kit.storage, settings=kit.settings, lease=stub, allow_large_sweep=True, now=kit.clock.now
    )
    cat.close()
    assert report.swept == []
    assert all(_exists(kit, key) for key in b_files)  # the age fence alone keeps them


def test_moved_key_registered_guard_in_cleanup(kit: LakeKit) -> None:
    keys = _base(kit)
    orphan = _orphan(kit)
    kit.day(1)
    cat = kit.catalog("run-1")
    record_move(cat, MoveRecord(2, "to_private", keys[2], None, datetime.now(UTC), "a-bug"))
    versions_before = listed_sequences(kit.storage)
    with pytest.raises(GuardStopError, match=str(keys[2])):
        cleanup.run_cleanup(
            cat, kit.storage, settings=kit.settings, lease=kit.lease_of(cat), allow_large_sweep=False, now=kit.clock.now
        )
    cat.close()
    assert _exists(kit, orphan)
    assert listed_sequences(kit.storage) == versions_before


def test_dropped_versions_not_kept(kit: LakeKit) -> None:
    _base(kit)
    kit.day(1)
    cat = kit.catalog("run-1")
    undone = kit.add_match(cat, 40, visibility="public", frames=3, offset=0)
    kit.commit(cat)  # version 2
    cat.close()
    saved = {key: kit.storage.get("lake", key) for key in (version_key(2), manifest_key(2))}
    record = restore(kit.storage, target_seq=1, operator="ops")
    assert record.dropped_seqs == [2]
    for key, data in saved.items():  # a crash in restore step 7 left the dropped version in storage
        kit.storage.put("lake", key, data)
    time.sleep(0.01)
    # the restore publishes on the real clock; the dropped version goes once that successor is past the grace (D4)
    restored_at = read_manifest(kit.storage, read_current(kit.storage).seq).published_at
    grace = timedelta(minutes=kit.settings.checkpoint.grace_minutes)
    kit.clock.now = restored_at + grace + timedelta(minutes=1)
    cat = kit.catalog("run-2")
    try:
        report = kit.cleanup(cat)
    finally:
        cat.close()
    assert 2 in report.pruned_versions
    assert not kit.storage.exists("lake", version_key(2))
    assert report.swept == [undone.key]


def test_sweep_never_touches_other_prefixes(kit: LakeKit) -> None:
    _base(kit)
    roots: tuple[Root, ...] = ("lake", "restricted")
    others: list[tuple[Root, str]] = [
        (root, key) for root in roots for key in ("raw/abc.zst", "config/x.json", "runs/x.json")
    ]
    others.append(("lake", "catalog/unrelated.json"))
    for root, key in others:
        kit.storage.put(root, key, b"x")
    time.sleep(0.01)
    _run_with_cleanup(kit, 1, "run-1", allow_large_sweep=True)
    for root, key in others:
        assert kit.storage.exists(root, key)


def test_sweep_all_path_forms_local(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The roots name an `hf://` bucket and an `s3://` path besides their local folders; DuckLake references in
    all three forms map to the same canonical keys, so the sweep deletes only the unlisted orphan."""
    roots = RootMap(
        lake=StorageRootInfo(local_path=tmp_path / "lake", hf_bucket="ns/lake", s3_bucket_path="s3://ns/lake"),
        restricted=StorageRootInfo(
            local_path=tmp_path / "lake-restricted", hf_bucket="ns/restricted", s3_bucket_path="s3://ns/restricted"
        ),
    )
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "boot.json")
    kit = LakeKit(base=tmp_path, roots=roots, storage=LocalStorage(roots))
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    cat.conn.execute("CREATE TABLE lake.main.dims AS SELECT range AS x_mm FROM range(5000)")  # a DuckLake-written file
    kit.replace_match(cat, 1, visibility="public", frames=3, offset=9)
    kit.commit(cat)
    cat.close()
    real_refs = cleanup._tracked_refs  # pyright: ignore[reportPrivateUsage] - the seam under test

    def in_all_forms(conn: duckdb.DuckDBPyConnection) -> list[str]:
        refs = real_refs(conn)
        lake = (tmp_path / "lake").as_posix()
        out: list[str] = []
        for ref in refs:
            posix = ref.replace("\\", "/")
            if f"/bronze/{PLAYERS}/1-" in posix:
                out.append("hf://buckets/ns/lake/" + posix.split(lake + "/", 1)[1])  # an hf:// registration
            elif "/main/dims/" in posix:
                out.append("s3://ns/lake/" + posix.split(lake + "/", 1)[1])  # a native file on the s3:// data path
            else:
                out.append(ref)
        return out

    monkeypatch.setattr(cleanup, "_tracked_refs", in_all_forms)
    orphan = _orphan(kit)
    time.sleep(0.01)
    report = _run_with_cleanup(kit, 7.5, "run-7")  # version 1 leaves the kept set: keys[1] is DuckLake-tracked only
    assert report.swept == [orphan]
    assert _exists(kit, keys[1])
    named = read_manifest(kit.storage, read_current(kit.storage).seq).canonical_keys
    assert all(_exists(kit, key) for key in named)


def test_sweep_aborts_on_broken_mapping(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    _base(kit, private=(5,))
    orphan = _orphan(kit)
    real = cleanup.canonical_key

    def drops_root(ref: str, *, roots: RootMap, relative_to_data_path: bool) -> CanonicalKey:
        key = real(ref, roots=roots, relative_to_data_path=relative_to_data_path)
        return CanonicalKey("lake", key.path)  # a mapping bug: every key lands in the lake root

    monkeypatch.setattr(cleanup, "canonical_key", drops_root)
    kit.day(1)
    cat = kit.catalog("run-1")
    with pytest.raises(GuardStopError, match="CURRENT manifest"):
        cleanup.run_cleanup(
            cat, kit.storage, settings=kit.settings, lease=kit.lease_of(cat), allow_large_sweep=True, now=kit.clock.now
        )
    cat.close()
    assert _exists(kit, orphan)


def test_sweep_aborts_above_ten_percent(kit: LakeKit) -> None:
    _base(kit)
    orphans = [_orphan(kit, 900 + i) for i in range(4)]  # 4 of 29 objects: above 10 %
    time.sleep(0.01)
    with pytest.raises(GuardStopError, match="10"):
        _run_with_cleanup(kit, 1, "run-1")
    assert all(_exists(kit, key) for key in orphans)
    report = _run_with_cleanup(kit, 1, "run-1b", allow_large_sweep=True)
    assert sorted(map(str, report.swept)) == sorted(map(str, orphans))


def test_sweep_now_lifts_the_guard_under_the_lease(kit: LakeKit) -> None:
    """`lakehouse sweep --allow-large` (§10.3 step 7): one cleanup with the large-sweep guard lifted, under the real
    lease, then a run-end version; the lease is free again afterwards."""
    _base(kit)
    orphans = [_orphan(kit, 900 + i) for i in range(4)]  # above 10 %: a run's own cleanup stops here
    time.sleep(0.01)
    kit.day(1)
    before = read_current(kit.storage).seq
    report = cleanup.sweep_now(kit.storage, settings=kit.settings, work_dir=kit.base / "work-sweep", clock=kit.clock)
    assert sorted(map(str, report.swept)) == sorted(map(str, orphans))
    assert not any(_exists(kit, key) for key in orphans)
    after = read_current(kit.storage)
    assert after.seq == before + 1 and read_manifest(kit.storage, after.seq).kind == "run_end"
    lease = Lease.acquire(kit.storage, holder_id="next", run_id="next")
    assert lease is not None
    lease.release()


def test_sweep_now_maintains_without_a_budget(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """T12 spec D3: `sweep_now` is the same maintenance outside a run, so it runs the maintenance first, unbudgeted."""
    _base(kit)
    budgets: list[float | None] = []
    real = cleanup.maintain_tables

    def spy(cat: Any, **kwargs: Any) -> Any:
        budgets.append(kwargs.get("budget_s"))
        return real(cat, **kwargs)

    monkeypatch.setattr(cleanup, "maintain_tables", spy)
    kit.day(1)
    cleanup.sweep_now(kit.storage, settings=kit.settings, work_dir=kit.base / "work-sweep", clock=kit.clock)
    assert budgets == [None]


def test_sweep_dry_run_lists_without_deleting(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reviewer B T12-PLAN-02: `lakehouse sweep --dry-run` lists what the lifted guard would delete (the same
    computation), deletes nothing, runs no maintenance and publishes nothing; it needs no `--allow-large`."""
    _base(kit)
    orphans = [_orphan(kit, 900 + i) for i in range(4)]  # above 10 %
    time.sleep(0.01)
    kit.day(1)
    before = read_current(kit.storage).seq

    def no_maintenance(cat: Any, **kwargs: Any) -> Any:
        raise AssertionError("the dry run maintains nothing")

    monkeypatch.setattr(cleanup, "maintain_tables", no_maintenance)
    preview = cleanup.sweep_preview(kit.storage, settings=kit.settings, work_dir=kit.base / "work-dry", clock=kit.clock)
    assert sorted(map(str, preview.doomed)) == sorted(map(str, orphans))
    assert preview.objects_listed > 0 and preview.counted == len(orphans)
    assert all(_exists(kit, key) for key in orphans)
    assert read_current(kit.storage).seq == before


def test_allow_large_deletes_only_expected_keys(kit: LakeKit) -> None:
    """Reviewer B T12-PLAN-05: `sweep --allow-large --expect <list>` stops before any delete when its own computation
    finds a key outside the list; with every key listed it deletes them."""
    _base(kit)
    orphans = [_orphan(kit, 900 + i) for i in range(4)]
    time.sleep(0.01)
    kit.day(1)
    with pytest.raises(GuardStopError, match=orphans[3].path.split("/")[-1]):
        cleanup.sweep_now(
            kit.storage,
            settings=kit.settings,
            work_dir=kit.base / "w1",
            clock=kit.clock,
            expected=frozenset(orphans[:3]),
        )
    assert all(_exists(kit, key) for key in orphans), "nothing is deleted when a key is not expected"
    report = cleanup.sweep_now(
        kit.storage, settings=kit.settings, work_dir=kit.base / "w2", clock=kit.clock, expected=frozenset(orphans)
    )
    assert sorted(map(str, report.swept)) == sorted(map(str, orphans))


def test_sweep_now_needs_the_lease(kit: LakeKit) -> None:
    _base(kit)
    orphan = _orphan(kit)
    time.sleep(0.01)
    held = Lease.acquire(kit.storage, holder_id="a-run", run_id="run-9")
    assert held is not None
    try:
        with pytest.raises(ConflictError, match="lease"):
            cleanup.sweep_now(kit.storage, settings=kit.settings, work_dir=kit.base / "work-sweep")
    finally:
        held.release()
    assert _exists(kit, orphan)


def test_sweep_after_restore(kit: LakeKit) -> None:
    keys = _base(kit)
    kit.day(1)
    cat = kit.catalog("run-1")
    added = kit.add_match(cat, 41, visibility="public", frames=3, offset=0)
    replaced = kit.replace_match(cat, 2, visibility="public", frames=3, offset=77)
    kit.commit(cat)  # version 2: the run that the restore undoes
    cat.close()
    restore(kit.storage, target_seq=1, operator="ops")
    time.sleep(0.01)
    report = _run_with_cleanup(kit, 2, "run-2")
    assert sorted(map(str, report.swept)) == sorted([str(added.key), str(replaced.key)])
    for seq in listed_sequences(kit.storage):
        assert all(_exists(kit, key) for key in read_manifest(kit.storage, seq).canonical_keys)
    assert _exists(kit, keys[2])
    assert kit.read_match(2) == (3, content_sum(3, 2))


def test_moved_key_not_found_accepted(kit: LakeKit) -> None:
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    move_now(cat, kit.storage, 1, "restricted")  # deletes the lake copy; DuckLake still tracks it
    kit.commit(cat)
    cat.close()
    assert not _exists(kit, keys[1])
    for day in (1, 4, 7.5):
        assert _run_with_cleanup(kit, day, f"run-{day}").moved_not_found == []
    report = _run_with_cleanup(kit, 8.5, "run-8")
    assert report.moved_not_found == [keys[1]]
    assert kit.read_match(1) == (3, content_sum(3, 1))


def test_other_missing_key_is_counted_not_fatal(kit: LakeKit) -> None:
    """Option (b) idempotency: a `to_delete` file already gone that no move record names is counted in
    `missing_treated_as_deleted`, not an `IntegrityError` (spec D3.1 + Crash-safety)."""
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    kit.replace_match(cat, 2, visibility="public", frames=3, offset=8)
    kit.commit(cat)
    cat.close()
    kit.storage.delete("lake", keys[2].path)  # deleted by hand: no move record names it
    _run_with_cleanup(kit, 7.5, "run-7")
    report = _run_with_cleanup(kit, 8.5, "run-8")  # the replaced file is now to_delete, missing: counted, not fatal
    assert report.missing_treated_as_deleted >= 1


def test_unreferenced_version_pruned(kit: LakeKit) -> None:
    _base(kit)
    cat = kit.catalog("run-crashed-cas")
    seq = cat.next_seq
    kit.storage.put("lake", version_key(seq), kit.storage.get("lake", version_key(1)))
    manifest = read_manifest(kit.storage, 1).model_copy(update={"seq": seq, "kind": "in_run", "run_id": "lost-cas"})
    kit.storage.put("lake", manifest_key(seq), manifest.to_bytes())  # never named by CURRENT
    cat.close()
    kit.day(1)
    cat = kit.catalog("run-1")
    kit.add_match(cat, 42, visibility="public", frames=2, offset=0)
    kit.commit(cat)  # a run-end version above the unreferenced one
    cat.close()
    report = _run_with_cleanup(kit, 2, "run-2")
    assert seq in report.pruned_versions
    assert not kit.storage.exists("lake", version_key(seq))


def _moves_and_tracked_only(kit: LakeKit) -> tuple[CanonicalKey, CanonicalKey]:
    """Day 0: match 1 replaced (A then B), version 1 out of storage, then moved private: A gets a record with no
    new key (only DuckLake tracks it), B a record with a restricted copy."""
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    second = kit.replace_match(cat, 1, visibility="public", frames=3, offset=300)
    kit.commit(cat)
    for key in (version_key(1), manifest_key(1)):
        kit.storage.delete("lake", key)
    records = move_now(cat, kit.storage, 1, "restricted")
    kit.commit(cat)
    cat.close()
    by_old = {r.old_key: r for r in records}
    assert by_old[keys[1]].new_key is None
    assert by_old[second.key].new_key is not None
    return keys[1], second.key


def _record_keys(kit: LakeKit) -> set[CanonicalKey]:
    cat = kit.catalog("probe")
    try:
        return {r.old_key for r in read_moves(cat.conn)}
    finally:
        cat.close()


def test_move_record_survives_daily_runs(kit: LakeKit) -> None:
    first, second = _moves_and_tracked_only(kit)
    gone_on: dict[CanonicalKey, float] = {}
    for day in range(1, 11):
        report = _run_with_cleanup(kit, day + 0.5, f"run-{day}")
        assert all(key in (first, second) for key in report.moved_not_found)
        remaining = _record_keys(kit)
        for key in (first, second):
            if key not in remaining and key not in gone_on:
                gone_on[key] = day + 0.5
                assert key in report.moved_not_found  # pruned only in the run whose cleanup removed it
    assert set(gone_on) == {first, second}
    assert min(gone_on.values()) > 8  # never before DuckLake's own cleanup (snapshots kept 8 days)


def test_move_record_not_pruned_early(kit: LakeKit) -> None:
    first, second = _moves_and_tracked_only(kit)
    report = _run_with_cleanup(kit, 5, "run-5")
    assert {first, second} <= _record_keys(kit)
    assert report.moved_not_found == []
    report = _run_with_cleanup(kit, 10, "run-10")  # no run for 5 days, then DuckLake's cleanup removes both
    assert sorted(map(str, report.moved_not_found)) == sorted([str(first), str(second)])
    assert not ({first, second} & _record_keys(kit))


def test_version_cap_after_crash(kit: LakeKit) -> None:
    """§23.6 "Crashed-run version cap": a crashed run's in-run versions leave the kept set after 7 days, so DuckLake's
    list of old files never meets a kept file, and the next cleanup succeeds after a long pause."""
    keys = _base(kit)
    kit.day(1)
    cat = kit.catalog("run-crashed")
    kit.add_match(cat, 43, visibility="public", frames=2, offset=0)
    kit.commit(cat, "in_run")  # version 2 still names match 1's first file
    kit.replace_match(cat, 1, visibility="public", frames=3, offset=60)
    kit.commit(cat, "in_run")  # version 3 (CURRENT); then the run crashes: no run-end version
    cat.close()
    report = _run_with_cleanup(kit, 10, "run-10")
    assert {1, 2} <= set(report.pruned_versions)
    assert report.ducklake_deleted == [keys[1]]
    assert read_current(kit.storage).seq == 4


def test_prune_versions_rules(kit: LakeKit) -> None:
    _base(kit)  # version 1, run end, day 0
    steps: list[tuple[int, VersionKind]] = [(1, "in_run"), (1, "run_end"), (2, "in_run")]
    for day, kind in steps:
        kit.day(day)
        cat = kit.catalog(f"run-{day}-{kind}")
        kit.add_match(cat, 60 + len(listed_sequences(kit.storage)), visibility="public", frames=1, offset=0)
        kit.commit(cat, kind)
        cat.close()
    # versions: 1 run_end d0, 2 in_run d1 (superseded by 3), 3 run_end d1, 4 in_run d2 (CURRENT)
    retention, none = kit.settings.retention, frozenset[int]()
    assert prune_versions(kit.storage, retention, dropped=none, now=T0 + timedelta(days=3), grace=timedelta(0)) == [2]
    assert prune_versions(kit.storage, retention, dropped=none, now=T0 + timedelta(days=7.5), grace=timedelta(0)) == [1]
    assert prune_versions(kit.storage, retention, dropped=none, now=T0 + timedelta(days=30), grace=timedelta(0)) == [3]
    assert listed_sequences(kit.storage) == [4]


def _versions_by_minute(kit: LakeKit, steps: list[tuple[float, VersionKind]]) -> None:
    for minute, kind in steps:
        kit.clock.now = T0 + timedelta(minutes=minute)
        cat = kit.catalog(f"run-{minute}-{kind}")
        kit.add_match(cat, 70 + len(listed_sequences(kit.storage)), visibility="public", frames=1, offset=0)
        cat.commit(kind)
        cat.close()


def test_plan_version_pruning_grace_holds_recent(kit: LakeKit) -> None:
    """T3 spec D4: a superseded version stays until its successor has been published for at least the grace."""
    _versions_by_minute(kit, [(0, "in_run"), (1, "run_end"), (6, "in_run")])  # 1 is superseded by 2 at minute 1
    plan = cleanup.plan_version_pruning(
        kit.storage,
        kit.settings.retention,
        dropped=frozenset(),
        now=T0 + timedelta(minutes=6),
        current_seq=3,
        grace=timedelta(minutes=10),
    )
    assert plan == []  # 1's successor is 5 minutes old


def test_plan_version_pruning_grace_releases_old(kit: LakeKit) -> None:
    _versions_by_minute(kit, [(0, "in_run"), (1, "run_end"), (12, "in_run")])
    plan = cleanup.plan_version_pruning(
        kit.storage,
        kit.settings.retention,
        dropped=frozenset(),
        now=T0 + timedelta(minutes=12),
        current_seq=3,
        grace=timedelta(minutes=10),
    )
    assert plan == [1]  # 1's successor is 11 minutes old


def test_plan_version_pruning_prunes_manifestless_without_grace(kit: LakeKit) -> None:
    """A version object without a manifest is a crashed commit: never `CURRENT`, so no reader holds it."""
    _versions_by_minute(kit, [(0, "run_end")])
    kit.storage.put("lake", version_key(2), b"a crashed commit's catalog copy")
    plan = cleanup.plan_version_pruning(
        kit.storage,
        kit.settings.retention,
        dropped=frozenset(),
        now=T0,
        current_seq=1,
        grace=timedelta(minutes=10),
    )
    assert plan == [2]


def test_delete_versions_returns_confirmed(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    _versions_by_minute(kit, [(0, "in_run"), (1, "in_run"), (2, "run_end")])
    monkeypatch.setattr(cleanup.sys, "platform", "win32")
    real_delete = kit.storage.delete

    def locked(root: Root, key: str, *, missing_ok: bool = False) -> None:
        if key == version_key(2):
            raise PermissionError("a reader holds it")
        real_delete(root, key, missing_ok=missing_ok)

    monkeypatch.setattr(kit.storage, "delete", locked)
    assert cleanup.delete_versions(kit.storage, [1, 2]) == [1]
    assert listed_sequences(kit.storage) == [2, 3]


def test_sweep_skips_tracked_native_file(kit: LakeKit) -> None:
    """A replaced DuckLake-written file (a relative path under schema and table) that no kept manifest names any more
    stays while DuckLake tracks it."""
    _base(kit)
    cat = kit.catalog("run-0b")
    cat.conn.execute("CREATE TABLE lake.main.dims AS SELECT range AS x_mm FROM range(5000)")
    kit.commit(cat)  # version 2 names the first native file
    native = [k for k in read_manifest(kit.storage, 2).canonical_keys if k.path.startswith("data/main/dims/")]
    cat.conn.execute("DELETE FROM lake.dims")
    cat.conn.execute("INSERT INTO lake.dims SELECT range + 1 FROM range(5000)")
    kit.commit(cat)  # version 3: a new native file; the first one is an ended row
    cat.close()
    assert len(native) == 1
    report = _run_with_cleanup(kit, 7.5, "run-7")  # versions 1 and 2 leave the kept set
    assert {1, 2} <= set(report.pruned_versions)
    assert report.swept == []
    assert _exists(kit, native[0])


def test_sweep_keeps_restricted_copy_of_kept_version(kit: LakeKit) -> None:
    """§29.10: a kept version names the moved key; the kept set maps it to the record's restricted copy, so the sweep
    keeps that copy and a restore to that version still reads by value."""
    _base(kit)
    cat = kit.catalog("run-0b")
    kit.replace_match(cat, 1, visibility="public", frames=3, offset=200)
    kit.commit(cat)  # version 2: version 1 still names the first file of match 1
    records = move_now(cat, kit.storage, 1, "restricted")
    kit.commit(cat)
    cat.close()
    copies = [r.new_key for r in records if r.new_key is not None]
    assert len(copies) == 2  # the registered file and the kept (version 1) file
    time.sleep(0.01)
    report = _run_with_cleanup(kit, 1, "run-1")
    assert report.swept == []
    assert all(key is not None and _exists(kit, key) for key in copies)
    restore(kit.storage, target_seq=1, operator="ops")
    assert kit.read_match(1) == (3, content_sum(3, 1))


# --- the run anchors and the large-sweep guard (T3 spec D5; plan round 3 Task 7) ------------------------------------


def _stamp(kit: LakeKit, key: CanonicalKey | str, when: datetime) -> None:
    root, path = (key.root, key.path) if isinstance(key, CanonicalKey) else ("lake", key)
    base = kit.roots.lake.local_path if root == "lake" else kit.roots.restricted.local_path
    assert base is not None
    os.utime(base / path, (when.timestamp(), when.timestamp()))


def _anchor(kit: LakeKit, generation: int, *, run_id: str, marker_at: datetime | None) -> None:
    body = RunAnchor(generation=generation, run_id=run_id).model_dump_json().encode()
    kit.storage.put("lake", anchor_key(generation), body)
    if marker_at is not None:
        kit.storage.put("lake", marker_key(generation), body)
        _stamp(kit, marker_key(generation), marker_at)


def _orphans_at(kit: LakeKit, count: int, when: datetime) -> list[CanonicalKey]:
    keys = [_orphan(kit, 900 + index) for index in range(count)]
    for key in keys:
        _stamp(kit, key, when)
    return keys


def _sweep_started(kit: LakeKit, started: datetime, run_id: str = "run-s") -> cleanup.CleanupReport:
    cat = kit.catalog(run_id, lease=StubLease(run_id=run_id, started_storage_time=started))
    try:
        return kit.cleanup(cat)
    finally:
        cat.close()


def _anchors(kit: LakeKit) -> list[int]:
    return [interval.generation for interval in anchor_intervals(kit.storage)]


def test_sweep_guard_ignores_orphans_inside_an_anchor_interval(kit: LakeKit) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    _anchor(kit, 1, run_id="run-crashed", marker_at=t0)
    orphans = _orphans_at(kit, 3, t0 + timedelta(minutes=10))  # 3 of 28 objects: over the 10 % guard
    _anchor(kit, 2, run_id="run-s", marker_at=t0 + timedelta(minutes=20))
    report = _sweep_started(kit, t0 + timedelta(minutes=30))
    assert sorted(report.swept, key=str) == sorted(orphans, key=str)
    assert report.exempted == 3
    assert not any(_exists(kit, key) for key in orphans)
    assert report.anchors_removed == [1]  # its interval ended before the sweeping run started
    assert _anchors(kit) == [2]


def test_sweep_guard_still_trips_outside_every_interval(kit: LakeKit) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    orphans = _orphans_at(kit, 3, t0 + timedelta(minutes=10))
    _anchor(kit, 1, run_id="run-1", marker_at=t0 + timedelta(minutes=15))  # starts after the orphans
    _anchor(kit, 2, run_id="run-s", marker_at=t0 + timedelta(minutes=20))
    with pytest.raises(GuardStopError, match="more than 10%"):
        _sweep_started(kit, t0 + timedelta(minutes=30))
    assert all(_exists(kit, key) for key in orphans)
    assert _anchors(kit) == [1, 2]  # a sweep that raises removes no anchor


def test_lost_lease_in_flight_files_fall_in_the_next_interval(kit: LakeKit) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    _anchor(kit, 1, run_id="run-old", marker_at=t0)
    _anchor(kit, 2, run_id="run-new", marker_at=t0 + timedelta(minutes=10))
    late = _orphans_at(kit, 3, t0 + timedelta(minutes=15))  # the old writer's in-flight files, after 2's marker
    _anchor(kit, 3, run_id="run-s", marker_at=t0 + timedelta(minutes=29))
    report = _sweep_started(kit, t0 + timedelta(minutes=30))
    assert report.exempted == 3
    assert not any(_exists(kit, key) for key in late)


def test_anchor_without_marker_exempts_nothing_and_is_removed(kit: LakeKit) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    _anchor(kit, 1, run_id="run-abandoned", marker_at=None)  # no marker: wrote nothing, or its marker was pruned
    [orphan] = _orphans_at(kit, 1, t0 + timedelta(minutes=10))  # one orphan: under the guard
    report = _sweep_started(kit, t0 + timedelta(minutes=30))
    assert report.swept == [orphan]
    assert report.exempted == 0
    assert report.anchors_removed == [1]
    assert _anchors(kit) == []


def test_sweep_removes_only_examined_anchors(kit: LakeKit) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    for generation, minute in ((1, 0), (2, 10), (3, 20)):
        _anchor(kit, generation, run_id=f"run-{generation}", marker_at=t0 + timedelta(minutes=minute))
    report = _sweep_started(kit, t0 + timedelta(minutes=19))  # generation 3 sweeps; its marker follows its start
    assert report.anchors_removed == [1]
    assert _anchors(kit) == [2, 3]  # the predecessor's interval ends at 3's marker, after the sweep started


def test_failed_sweep_removes_no_anchor(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    _base(kit)
    t0 = datetime.now(UTC) - timedelta(hours=1)
    _anchor(kit, 1, run_id="run-1", marker_at=t0)
    [orphan] = _orphans_at(kit, 1, t0 + timedelta(minutes=5))
    _anchor(kit, 2, run_id="run-s", marker_at=t0 + timedelta(minutes=20))
    real_delete = kit.storage.delete

    def failing(root: Root, key: str, *, missing_ok: bool = False) -> None:
        if key == orphan.path:
            raise OSError("the disk went away")
        real_delete(root, key, missing_ok=missing_ok)

    monkeypatch.setattr(kit.storage, "delete", failing)
    with pytest.raises(OSError, match="disk"):
        _sweep_started(kit, t0 + timedelta(minutes=30))
    assert _anchors(kit) == [1, 2]


def test_sweep_stops_when_a_doomed_key_is_in_current(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec T3-SPEC-21: a kept-set defect that would delete a file the published version names is a hard stop,
    even when a surviving anchor's interval covers that file."""
    keys = _base(kit)
    live = keys[3]
    t0 = datetime.now(UTC) - timedelta(hours=1)
    _anchor(kit, 1, run_id="run-1", marker_at=t0)
    _stamp(kit, live, t0 + timedelta(minutes=5))
    real_kept, real_tracked = cleanup.kept_file_set, cleanup.tracked_keys

    def kept_without(*args: Any, **kwargs: Any) -> frozenset[CanonicalKey]:
        return real_kept(*args, **kwargs) - {live}

    def tracked_without(*args: Any, **kwargs: Any) -> frozenset[CanonicalKey]:
        return real_tracked(*args, **kwargs) - {live}

    monkeypatch.setattr(cleanup, "kept_file_set", kept_without)
    monkeypatch.setattr(cleanup, "tracked_keys", tracked_without)
    with pytest.raises(GuardStopError, match="CURRENT"):
        _sweep_started(kit, t0 + timedelta(minutes=30))
    assert _exists(kit, live)
