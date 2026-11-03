"""`lakehouse verify` (spec §10.4, §10.8; plan Tasks 9 and 26): every catalog version in storage matches its manifest,
every file a version names exists, and `CURRENT` names a stored version with its size and hash."""

from __future__ import annotations

from lakekit import PLAYERS, LakeKit

from lakehouse.catalog.publish import read_current, version_key
from lakehouse.catalog.verify import verify_lake


def _two_versions(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, 8001, visibility="public", frames=2, offset=0)
    cat.commit("run_end")
    kit.add_match(cat, 8002, visibility="private", frames=1, offset=0)
    cat.commit("run_end")
    cat.close()


def test_a_sound_lake_verifies(kit: LakeKit) -> None:
    _two_versions(kit)
    report = verify_lake(kit.storage)
    assert report.problems == []
    assert report.versions == 2 and report.files >= 2
    assert report.current_seq == read_current(kit.storage).seq


def test_a_missing_file_is_named(kit: LakeKit) -> None:
    _two_versions(kit)
    [gone] = kit.objects("restricted", f"data/bronze/{PLAYERS}/8002-")
    kit.storage.delete("restricted", gone)
    report = verify_lake(kit.storage)
    assert len(report.problems) == 1 and gone in report.problems[0] and "version 2" in report.problems[0]


def test_a_changed_version_object_is_named(kit: LakeKit) -> None:
    _two_versions(kit)
    data = bytearray(kit.storage.get("lake", version_key(1)))
    data[-1] ^= 0xFF
    kit.storage.delete("lake", version_key(1))
    kit.storage.put("lake", version_key(1), bytes(data))
    report = verify_lake(kit.storage)
    assert report.problems == ["version 1: the catalog file does not match the size and SHA-256 in its manifest"]


def test_an_empty_lake_has_no_current(kit: LakeKit) -> None:
    report = verify_lake(kit.storage)
    assert report.problems == ["no CURRENT: the lake has no published version yet"]
