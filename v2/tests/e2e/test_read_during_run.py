"""Read during a run (spec §23.6 "Read during a run", "Reader across catalog commits").

Process A holds the lease and the working copy and makes 5 catalog commits. This test process (B) reads through
`CatalogReader` between and during them. Every query must return the rows of one consistent version, or raise.
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import IntegrityError
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.lease import Lease
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

COMMITS = 5
ROWS_PER_COMMIT = 200


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _expected(commits: int) -> tuple[int, int]:
    rows = commits * ROWS_PER_COMMIT
    return rows, sum(range(rows))


def _writer(base: str, ready: Event, done: Event) -> None:
    storage = LocalStorage(_roots(Path(base)))
    lease = Lease.acquire(storage, holder_id="process-a", run_id="run-a")
    if lease is None:
        raise RuntimeError("the writer did not get the lease")
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), lease, work_dir=Path(base) / "work")
    cat.conn.execute("CREATE TABLE lake.main.t (i BIGINT)")
    cat.commit("in_run")
    ready.set()
    for commit in range(COMMITS):
        start = commit * ROWS_PER_COMMIT
        cat.conn.execute("INSERT INTO lake.t SELECT range FROM range(?, ?)", [start, start + ROWS_PER_COMMIT])
        cat.commit("in_run" if commit < COMMITS - 1 else "run_end")
    cat.close()
    lease.release()
    done.set()


def test_reader_sees_only_consistent_versions(tmp_path: Path) -> None:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "boot.json")
    ctx = multiprocessing.get_context("spawn")
    ready, done = ctx.Event(), ctx.Event()
    writer = ctx.Process(target=_writer, args=(str(tmp_path), ready, done))
    writer.start()
    try:
        assert ready.wait(timeout=60), "the writer did not publish its first version"
        valid = {_expected(n) for n in range(COMMITS + 1)}
        reader = CatalogReader(LocalStorage(_roots(tmp_path)))
        seen: list[tuple[int, int]] = []
        loud: list[str] = []
        while True:
            finished = done.is_set()
            try:
                with reader.session() as conn:
                    row = conn.execute("SELECT count(*), coalesce(sum(i), 0) FROM lake.t").fetchone()
                assert row is not None
                seen.append((int(row[0]), int(row[1])))
            except (IntegrityError, duckdb.IOException, PermissionError) as exc:  # loud failures are allowed
                loud.append(f"{type(exc).__name__}: {exc}")
            if finished:
                break
        reader.close()
    finally:
        writer.join(timeout=60)
    assert writer.exitcode == 0
    inconsistent = [pair for pair in seen if pair not in valid]
    assert inconsistent == [], f"reads returned rows of no published version: {inconsistent[:5]}"
    assert seen, f"no read succeeded; loud failures: {loud[:5]}"
    assert seen[-1] == _expected(COMMITS)
    assert reader.attach_count >= 2
