"""The version fence of `WorkingCatalog.commit` and the catalog size limit (T6 spec D2, plan Task 2).

A version or manifest object that already exists stops the commit through `_fence_existing`: a lost lease or a moved
`CURRENT` raises `LeaseLostError` (another writer published); an existing object that neither explains raises
`IntegrityError` (CLI `failed`, exit 1), never the benign `ConflictError` (`skipped`, exit 3). On HF the version upload
is a single part, so a catalog larger than the storage's single-part limit is refused before the upload.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pytest
from lakekit import LakeKit, StubLease

from lakehouse import cli
from lakehouse.catalog.publish import manifest_key, read_current, version_key
from lakehouse.errors import IntegrityError, LeaseLostError, StorageLimitError
from lakehouse.storage.local import LocalStorage


def _publish(kit: LakeKit, run_id: str) -> None:
    cat = kit.catalog(run_id)
    cat.conn.execute("CREATE TABLE IF NOT EXISTS lake.main.t (x INTEGER)")
    cat.commit("run_end")
    cat.close()


@dataclass
class _LoseOnSecondCheck(StubLease):
    """Holds the lease at the commit's first check, and has lost it by the fence's check."""

    def assert_held(self) -> None:
        self.checks += 1
        if self.checks >= 2:
            raise LeaseLostError("stub: lease lost after the commit started")


def _next_seq(kit: LakeKit) -> int:
    return read_current(kit.storage).seq + 1


def test_existing_version_with_lost_lease_raises_lease_lost(kit: LakeKit) -> None:
    _publish(kit, "run-0")
    cat = kit.catalog("run-1", lease=_LoseOnSecondCheck(run_id="run-1"))
    kit.storage.put("lake", version_key(_next_seq(kit)), b"another writer's version")
    with pytest.raises(LeaseLostError, match="lease lost"):
        cat.commit("run_end")
    cat.close()


def test_existing_version_with_moved_current_raises_lease_lost(kit: LakeKit) -> None:
    _publish(kit, "run-0")
    cat = kit.catalog("run-1")
    _publish(kit, "run-other")  # CURRENT moves and takes the next sequence
    with pytest.raises(LeaseLostError, match="CURRENT changed"):
        cat.commit("run_end")
    cat.close()


def test_unexplained_existing_version_is_an_integrity_error(kit: LakeKit) -> None:
    _publish(kit, "run-0")
    cat = kit.catalog("run-1")
    seq = _next_seq(kit)
    kit.storage.put("lake", version_key(seq), b"an object nothing explains")
    with pytest.raises(IntegrityError, match=f"version {seq} already exists"):
        cat.commit("run_end")
    cat.close()


def test_existing_manifest_takes_the_same_fence(kit: LakeKit) -> None:
    _publish(kit, "run-0")
    cat = kit.catalog("run-1")
    seq = _next_seq(kit)
    kit.storage.put("lake", manifest_key(seq), b"{}")
    with pytest.raises(IntegrityError, match="already exists"):
        cat.commit("run_end")
    cat.close()


def test_integrity_error_exits_1(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def handler(ctx: object) -> int:
        raise IntegrityError("version 7 already exists and neither a lost lease nor a moved CURRENT explains it")

    class _Parser:
        def parse_args(self, argv: list[str]) -> argparse.Namespace:
            return argparse.Namespace(command="run", handler=handler)

    monkeypatch.setattr(cli, "_parser", _Parser)
    assert cli.main([]) == 1
    assert '"status": "failed"' in capsys.readouterr().out


class _Limited(LocalStorage):
    @property
    def single_part_limit(self) -> int | None:
        return 1024  # far below any catalog file


def test_commit_refuses_a_catalog_above_the_limit(kit: LakeKit) -> None:
    _publish(kit, "run-0")
    kit.storage = _Limited(kit.roots)
    cat = kit.catalog("run-1")
    seq = _next_seq(kit)
    with pytest.raises(StorageLimitError, match="1024"):
        cat.commit("run_end")
    assert not kit.storage.exists("lake", version_key(seq)), "nothing may be uploaded above the limit"
    cat.close()


def test_local_storage_has_no_limit(tmp_path: Path, kit: LakeKit) -> None:
    assert kit.storage.single_part_limit is None
