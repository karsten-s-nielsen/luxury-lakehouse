"""Cleanup across many runs (spec §10.3, §10.7, §10.8, §29.10, §29.11; §23.6 "Linear multi-run cleanup",
"Visibility change moves files")."""

from __future__ import annotations

from pathlib import Path

from lakekit import PLAYERS, LakeKit, content_sum, lake_roots, move_now

from lakehouse.catalog.cleanup import CleanupReport
from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.moves import delete_moved_leftovers, read_moves
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.restore import restore
from lakehouse.errors import IntegrityError
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

BASE = 25


def _base(kit: LakeKit) -> dict[int, CanonicalKey]:
    kit.day(0)
    cat = kit.catalog("run-0")
    keys = {m: kit.add_match(cat, m, visibility="public", frames=3, offset=m).key for m in range(1, BASE + 1)}
    kit.commit(cat)
    cat.close()
    return keys


def _full_run(kit: LakeKit, day: float, run_id: str) -> CleanupReport:
    """A run: the run-start delete of moved leftovers, then the cleanup and the final commit."""
    kit.day(day)
    cat = kit.catalog(run_id)
    try:
        manifest = frozenset(read_manifest(kit.storage, read_current(kit.storage).seq).canonical_keys)
        delete_moved_leftovers(kit.storage, read_moves(cat.conn), current_manifest=manifest)
        return kit.cleanup(cat)
    finally:
        cat.close()


def _current_reads(kit: LakeKit) -> None:
    named = read_manifest(kit.storage, read_current(kit.storage).seq).canonical_keys
    assert all(kit.storage.exists(key.root, key.path) for key in named)


def test_linear_multirun_cleanup(kit: LakeKit) -> None:
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    kit.replace_match(cat, 1, visibility="public", frames=3, offset=400)
    kit.commit(cat)
    cat.close()
    replaced = keys[1]
    deleted_on: float | None = None
    for day in range(1, 11):
        report = _full_run(kit, day + 0.5, f"run-{day}")
        assert report.swept == []  # the sweep never deletes the replaced file: DuckLake tracks it
        assert report.moved_not_found == []
        if replaced in report.ducklake_deleted:
            deleted_on = day + 0.5
        _current_reads(kit)
    assert deleted_on is not None
    assert deleted_on > 8  # deleted once it left both retentions
    assert not kit.storage.exists("lake", replaced.path)
    assert kit.read_match(1) == (3, content_sum(3, 400))


def test_private_to_public_with_replace_then_restore(kit: LakeKit) -> None:
    """§29.11, the A P-21 case: a private match becomes public and changes content in one run."""
    _base(kit)
    kit.day(0)
    cat = kit.catalog("run-0b")
    kit.add_match(cat, 70, visibility="private", frames=4, offset=10)
    kit.commit(cat)  # version 2: match 70 private
    cat.close()
    kit.day(1)
    cat = kit.catalog("run-1")
    before_run = read_current(kit.storage).seq
    move_now(cat, kit.storage, 70, "lake")
    kit.replace_match(cat, 70, visibility="public", frames=4, offset=90)
    kit.commit(cat)
    cat.close()
    for day in (1.5, 2.5, 3.5):
        _full_run(kit, day, f"run-{day}")
    record = restore(kit.storage, target_seq=before_run, operator="ops")
    for key in read_manifest(kit.storage, record.new_seq).canonical_keys:
        assert kit.storage.exists(key.root, key.path)
    assert kit.read_match(70) == (4, content_sum(4, 10))
    assert kit.read_match(5) == (3, content_sum(3, 5))


def test_public_private_public_survives_runs(kit: LakeKit) -> None:
    """§29.10, the A-SPEC-47 / B14 case: public, private, public with unchanged content."""
    _base(kit)
    cat = kit.catalog("run-0b")
    move_now(cat, kit.storage, 3, "restricted")
    move_now(cat, kit.storage, 3, "lake")
    kit.commit(cat)
    cat.close()
    for day in range(1, 11):
        _full_run(kit, day + 0.5, f"run-{day}")
        _current_reads(kit)
        assert kit.read_match(3) == (3, content_sum(3, 3))


def _move_flow(
    kit: LakeKit, *, delete_by_hand: bool
) -> tuple[dict[int, CanonicalKey], list[tuple[float, CleanupReport | IntegrityError]]]:
    """Day 0: match 1 moves private and match 2 is replaced; then daily runs. With `delete_by_hand`, match 2's
    replaced file is deleted by hand right away, so DuckLake later lists a missing key that no move record names."""
    keys = _base(kit)
    cat = kit.catalog("run-0b")
    move_now(cat, kit.storage, 1, "restricted")
    kit.replace_match(cat, 2, visibility="public", frames=3, offset=33)
    kit.commit(cat)
    cat.close()
    if delete_by_hand:
        kit.storage.delete("lake", keys[2].path)
    outcomes: list[tuple[float, CleanupReport | IntegrityError]] = []
    for day in range(1, 11):
        try:
            outcomes.append((day + 0.5, _full_run(kit, day + 0.5, f"run-{day}")))
        except IntegrityError as exc:
            outcomes.append((day + 0.5, exc))
            break
    return keys, outcomes


def test_move_flow_cleanup(kit: LakeKit, tmp_path: Path) -> None:
    """§23.6 "Visibility change moves files", cleanup clauses."""
    keys, accepted = _move_flow(kit, delete_by_hand=False)
    assert all(isinstance(outcome, CleanupReport) for _, outcome in accepted)  # no guard stop, no failure
    named_on = [day for day, outcome in accepted if isinstance(outcome, CleanupReport) and outcome.moved_not_found]
    assert named_on, "DuckLake's list never named the moved key"
    first = named_on[0]
    first_report = dict(accepted)[first]
    assert isinstance(first_report, CleanupReport)
    assert first_report.moved_not_found == [keys[1]]  # the moved key's "not found" is accepted
    assert kit.read_match(1) == (3, content_sum(3, 1))

    other = LakeKit(
        base=tmp_path / "b", roots=lake_roots(tmp_path / "b"), storage=LocalStorage(lake_roots(tmp_path / "b"))
    )
    init_lake(ResolvedRoots(roots=other.roots, source="cli"), bootstrap_file=tmp_path / "b" / "boot.json")
    _, outcomes = _move_flow(other, delete_by_hand=True)
    # Option (b): the unrelated hand-deleted key that no move record names is counted in missing_treated_as_deleted,
    # not a fatal IntegrityError, so every run completes (spec D3.1 + Crash-safety).
    assert all(isinstance(outcome, CleanupReport) for _, outcome in outcomes)
    counted = [day for day, o in outcomes if isinstance(o, CleanupReport) and o.missing_treated_as_deleted]
    assert counted, "the hand-deleted unrelated key was never counted in missing_treated_as_deleted"
    assert PLAYERS
