"""Working catalog, publication and CURRENT (spec §10.3 step 5, §10.4, §10.6, §10.8)."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.catalog import publish
from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import (
    CURRENT_KEY,
    CurrentPointer,
    manifest_key,
    publish_current,
    read_current,
    roots_record,
    version_key,
)
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import Settings, load_defaults
from lakehouse.errors import IntegrityError, LeaseLostError, RootMismatchError
from lakehouse.storage.keys import Root, RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake


@dataclass
class StubLease:
    """A lease whose `assert_held` passes or fails on demand (the stale-holder and lost-lease cases)."""

    generation: int = 1
    run_id: str = "run-1"
    held: bool = True
    checks: int = 0

    def assert_held(self) -> None:
        self.checks += 1
        if not self.held:
            raise LeaseLostError("stub: lease lost")


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _lake(base: Path) -> LocalStorage:
    init_lake(ResolvedRoots(roots=_roots(base), source="cli"), bootstrap_file=base / "boot.json")
    return LocalStorage(_roots(base))


def _settings() -> Settings:
    return load_defaults("laptop")


def _open(storage: LocalStorage, lease: StubLease | None = None, work: Path | None = None) -> WorkingCatalog:
    return WorkingCatalog.open(storage, _settings(), lease or StubLease(), work_dir=work)


def _seeded(tmp_path: Path) -> LocalStorage:
    """A lake whose CURRENT names version 1, with a native table of 5,000 rows."""
    storage = _lake(tmp_path)
    cat = _open(storage)
    cat.conn.execute("CREATE TABLE lake.main.t AS SELECT range AS i FROM range(5000)")
    cat.commit("run_end")
    cat.close()
    return storage


def test_commit_publishes_version_manifest_and_current(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    lease = StubLease()
    cat = _open(storage, lease)
    cat.conn.execute("CREATE TABLE lake.main.t AS SELECT range AS i FROM range(5000)")
    info = cat.commit("in_run")
    assert (info.seq, info.kind, str(info.key)) == (1, "in_run", "lake:catalog/v/1.ducklake")
    assert storage.exists("lake", "catalog/v/1.ducklake")
    assert storage.exists("lake", "catalog/v/1.files.json")
    current = read_current(storage)
    version = storage.get("lake", version_key(1))
    assert (current.seq, current.size, current.sha256) == (1, len(version), hashlib.sha256(version).hexdigest())
    assert (info.size, info.sha256) == (current.size, current.sha256)
    assert current.parent_seq is None
    assert current.lease_generation == 1
    assert current.storage_roots == roots_record(storage.roots)
    manifest = read_manifest(storage, 1)
    assert (manifest.seq, manifest.kind, manifest.run_id) == (1, "in_run", "run-1")
    # restore verifies a target version and detects a relocation from its manifest (Task 11)
    assert (manifest.version_size, manifest.version_sha256) == (current.size, current.sha256)
    assert manifest.storage_roots == roots_record(storage.roots)
    assert len(manifest.keys) == 1
    assert str(manifest.keys[0]).startswith("lake:data/main/t/")
    assert lease.checks >= 2  # before the version and again before CURRENT
    cat.conn.execute("INSERT INTO lake.t SELECT 1")  # re-attached after the commit
    second = cat.commit("run_end")
    assert second.seq == 2
    assert read_current(storage).parent_seq == 1
    cat.close()


def test_first_current_on_empty_lake_once(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    roots = roots_record(storage.roots)
    first = CurrentPointer.build(
        seq=1, version_key=version_key(1), size=1, sha256="a", parent_seq=None, lease_generation=1, storage_roots=roots
    )
    second = CurrentPointer.build(
        seq=2, version_key=version_key(2), size=1, sha256="b", parent_seq=None, lease_generation=1, storage_roots=roots
    )
    published = publish_current(storage, first, expected=None)
    assert published.tag is not None
    with pytest.raises(LeaseLostError):
        publish_current(storage, second, expected=None)
    assert read_current(storage).seq == 1


def test_current_self_checksum_detects_torn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _seeded(tmp_path)
    monkeypatch.setattr(publish, "_RETRY_WAIT_S", 0.0)
    document = json.loads(storage.get("lake", CURRENT_KEY))
    document["size"] += 1  # a torn or tampered CURRENT: the self checksum no longer matches
    storage.put("lake", CURRENT_KEY, json.dumps(document).encode())
    reads: list[str] = []
    real_get = storage.get_versioned

    def counting(root: str, key: str) -> tuple[bytes, str]:
        reads.append(key)
        return real_get("lake", key)

    monkeypatch.setattr(storage, "get_versioned", counting)
    with pytest.raises(IntegrityError, match="3 tries"):
        read_current(storage)
    assert reads == [CURRENT_KEY] * 3
    storage.put("lake", CURRENT_KEY, b"{half a json")
    with pytest.raises(IntegrityError):
        read_current(storage)


def test_working_copy_from_current_not_leftover(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    (work / "working.ducklake").write_bytes(b"a stale working copy left by an earlier run")
    cat = _open(storage, work=work)
    assert cat.conn.execute("SELECT count(*), sum(i) FROM lake.t").fetchone() == (5000, sum(range(5000)))
    assert cat.next_seq == 2
    cat.close()


def test_commit_requires_lease(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    lease = StubLease()
    cat = _open(storage, lease)
    cat.conn.execute("INSERT INTO lake.t SELECT 1")
    lease.held = False
    before = sorted(i.key for i in storage.list("lake", "catalog/"))
    with pytest.raises(LeaseLostError):
        cat.commit("in_run")
    assert sorted(i.key for i in storage.list("lake", "catalog/")) == before
    assert read_current(storage).seq == 1
    cat.close()


def test_current_cas_rejects_stale_writer(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    first = _open(storage, StubLease(run_id="run-a"), work=tmp_path / "wa")
    stale = _open(storage, StubLease(run_id="run-b"), work=tmp_path / "wb")
    stale_view = stale.current
    first.conn.execute("INSERT INTO lake.t SELECT 1")
    published = first.commit("run_end")
    version_bytes = storage.get("lake", version_key(published.seq))
    stale.conn.execute("INSERT INTO lake.t SELECT 2")
    with pytest.raises(LeaseLostError):
        stale.commit("run_end")
    # the compare-and-swap itself: a pointer based on the old CURRENT is refused
    assert stale_view is not None
    late = CurrentPointer.build(
        seq=9,
        version_key=version_key(9),
        size=1,
        sha256="x",
        parent_seq=stale_view.seq,
        lease_generation=1,
        storage_roots=stale_view.storage_roots,
    )
    with pytest.raises(LeaseLostError):
        publish_current(storage, late, expected=stale_view)
    current = read_current(storage)
    assert current.seq == published.seq
    assert storage.get("lake", version_key(published.seq)) == version_bytes
    assert hashlib.sha256(version_bytes).hexdigest() == current.sha256
    first.close()
    stale.close()


def test_version_never_overwritten(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    # T6 spec D2: an existing version that neither a lost lease nor a moved CURRENT explains is an integrity event
    # (IntegrityError, CLI exit 1), no longer the benign ConflictError (CLI skipped, exit 3)
    for held, error in ((True, IntegrityError), (False, LeaseLostError)):
        lease = StubLease()
        cat = _open(storage, lease, work=tmp_path / f"w-{held}")
        seq = cat.next_seq
        crafted = f"crafted version {seq}".encode()
        storage.put("lake", version_key(seq), crafted)
        cat.conn.execute("INSERT INTO lake.t SELECT 1")

        def lose_on_second_check(lease: StubLease = lease, held: bool = held) -> None:
            lease.checks += 1
            if not held and lease.checks > 1:
                raise LeaseLostError("stub: lease lost after the first check")

        lease.assert_held = lose_on_second_check  # type: ignore[method-assign]
        with pytest.raises(error, match="already exists" if held else "lease lost"):
            cat.commit("in_run")
        assert storage.get("lake", version_key(seq)) == crafted
        assert read_current(storage).seq == 1
        assert cat.next_seq == seq + 1  # the sequence is used from its upload attempt on (Reviewer B T3-IMPL-05)
        cat.close()
        storage.delete("lake", version_key(seq))


def test_roots_mismatch_with_current(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    current = read_current(storage)
    other = {"lake": str(tmp_path / "elsewhere"), "restricted": str(tmp_path / "elsewhere-restricted")}
    moved = CurrentPointer.build(
        seq=current.seq,
        version_key=current.version_key,
        size=current.size,
        sha256=current.sha256,
        parent_seq=current.parent_seq,
        lease_generation=current.lease_generation,
        storage_roots=other,
    )
    publish_current(storage, moved, expected=current)
    with pytest.raises(RootMismatchError, match="elsewhere"):
        _open(storage)
    with pytest.raises(RootMismatchError, match="elsewhere"), CatalogReader(storage).session():
        pass


def test_leftover_version_skipped(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    storage.put("lake", version_key(2), b"leftover of a crashed run")
    cat = _open(storage)
    cat.conn.execute("INSERT INTO lake.t SELECT 1")
    assert cat.commit("run_end").seq == 3
    assert read_current(storage).seq == 3
    assert storage.get("lake", version_key(2)) == b"leftover of a crashed run"
    assert not storage.exists("lake", manifest_key(2))
    cat.close()


def test_published_copy_attaches_with_same_snapshots(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    cat = _open(storage)
    cat.conn.execute("INSERT INTO lake.t SELECT range FROM range(5000, 6000)")
    before = cat.conn.execute(
        "SELECT snapshot_id, schema_version FROM ducklake_snapshots('lake') ORDER BY 1"
    ).fetchall()
    cat.commit("run_end")
    cat.close()
    with CatalogReader(storage).session() as conn:
        after = conn.execute("SELECT snapshot_id, schema_version FROM ducklake_snapshots('lake') ORDER BY 1").fetchall()
        assert conn.execute("SELECT count(*), sum(i) FROM lake.t").fetchone() == (6000, sum(range(6000)))
    assert after == before


def test_detach_leaves_no_wal(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    cat = _open(storage, work=tmp_path / "w")
    cat.conn.execute("INSERT INTO lake.t SELECT range FROM range(10)")
    path = cat.detach()
    assert path == tmp_path / "w" / "working.ducklake"
    assert sorted(p.name for p in path.parent.iterdir()) == ["working.ducklake"]
    cat.attach()
    assert cat.conn.execute("SELECT count(*) FROM lake.t").fetchone() == (5010,)
    cat.close()


def _crash_mid_run(base: str, work: str) -> None:
    storage = LocalStorage(_roots(Path(base)))
    cat = WorkingCatalog.open(storage, _settings(), StubLease(run_id="crashed"), work_dir=Path(work))
    cat.conn.execute("INSERT INTO lake.t SELECT range FROM range(100)")
    os._exit(0)  # a crash: no detach, so the working file and its WAL stay on disk


def test_crashed_working_copy_and_wal_ignored(tmp_path: Path) -> None:
    storage = _seeded(tmp_path)
    work = tmp_path / "work"
    ctx = multiprocessing.get_context("spawn")
    child = ctx.Process(target=_crash_mid_run, args=(str(tmp_path), str(work)))
    child.start()
    child.join(timeout=60)
    assert child.exitcode == 0
    assert (work / "working.ducklake.wal").exists(), "the crash must leave a WAL for this test to mean anything"
    cat = _open(storage, work=work)
    assert cat.conn.execute("SELECT count(*), sum(i) FROM lake.t").fetchone() == (5000, sum(range(5000)))
    cat.close()


class _InterleavedWrite(LocalStorage):
    """Another writer replaces CURRENT right after this writer's compare-and-swap returned (T6 spec D3: the race the
    gateway's non-atomic If-Match allows)."""

    def __init__(self, roots: RootMap, other: bytes) -> None:
        super().__init__(roots)
        self.other = other

    def replace_if_match(self, root: Root, key: str, data: bytes, *, expected_tag: str) -> str:
        tag = super().replace_if_match(root, key, data, expected_tag=expected_tag)
        if key == CURRENT_KEY:
            self.put(root, key, self.other)
        return tag


def _pointers(storage: LocalStorage) -> tuple[CurrentPointer, CurrentPointer, CurrentPointer]:
    roots = roots_record(storage.roots)
    first = CurrentPointer.build(
        seq=1, version_key=version_key(1), size=1, sha256="a", parent_seq=None, lease_generation=1, storage_roots=roots
    )
    mine = CurrentPointer.build(
        seq=2, version_key=version_key(2), size=1, sha256="b", parent_seq=1, lease_generation=1, storage_roots=roots
    )
    theirs = CurrentPointer.build(
        seq=2, version_key=version_key(2), size=1, sha256="c", parent_seq=1, lease_generation=2, storage_roots=roots
    )
    return first, mine, theirs


def test_replace_reads_current_back(tmp_path: Path) -> None:
    """T6 spec D3: after the compare-and-swap, the writer reads CURRENT back; another writer's pointer there raises
    LeaseLostError (never a silent lost update)."""
    plain = _lake(tmp_path)
    first, mine, theirs = _pointers(plain)
    expected = publish_current(plain, first, expected=None)
    storage = _InterleavedWrite(plain.roots, theirs.to_bytes())
    with pytest.raises(LeaseLostError, match="read back differs"):
        publish_current(storage, mine, expected=expected)


def test_read_back_matches_returns_the_read_tag(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    first, mine, _ = _pointers(storage)
    expected = publish_current(storage, first, expected=None)
    published = publish_current(storage, mine, expected=expected)
    assert published.tag == storage.get_versioned("lake", CURRENT_KEY)[1]


def test_read_back_mismatch_exits_1(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def handler(ctx: object) -> int:
        raise LeaseLostError("CURRENT read back differs from the pointer this run wrote: another writer published")

    class _Parser:
        def parse_args(self, argv: list[str]) -> argparse.Namespace:
            return argparse.Namespace(command="run", handler=handler)

    monkeypatch.setattr(cli, "_parser", _Parser)
    assert cli.main([]) == 1
    assert '"status": "failed"' in capsys.readouterr().out
