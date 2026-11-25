"""The hf:// reader scans a table of many registered data files (hf_job tier; 2026-11-24, ADR-001 amendment).

DuckDB 2.0.0 deadlocks a DuckLake scan of many data files when the catalog itself is read over hf://; with a local
catalog the same scan completes. `CatalogReader` therefore attaches a local, checked copy of a remote version. This
test registers more files than this host's default `async_threads` (bronze-style: absolute hf:// URIs through
`ducklake_add_data_files`, as `bronze/writer.py` does), then reads the table with the Space's storage (HF token only)
in a child process. A child that does not return within `READ_LIMIT_S` is killed (`subprocess.run(timeout=)`), so a
regression fails the test and the Job still ends.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb
import pytest
from hfkit import RUN, reset_lease

from lakehouse.bronze.writer import reader_uri
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.lease import Lease

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]

TABLE = "ops_many_files"
ROWS_PER_FILE = 1000
READ_LIMIT_S = 300  # the fixed scan takes seconds; a deadlocked scan never returns

# The reader half, as the Ops Space reads: the lake roots from the environment, the HF token only (no gateway).
_CHILD = """
import json, os, sys
from pydantic import SecretStr
from lakehouse.catalog.reader import CatalogReader
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.roots import HF_TOKEN_ENV, default_bootstrap_file, resolve_roots

resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=os.environ,
                         bootstrap_file=default_bootstrap_file(os.environ))
space = HfStorage(resolved.roots, hf_token=SecretStr(os.environ[HF_TOKEN_ENV]), s3_key=None)
table = sys.argv[1]
reader = CatalogReader(space)
with reader.session() as conn:
    listed = conn.execute(f"SELECT count(*) FROM ducklake_list_files('lake', '{table}')").fetchone()[0]
    count, total = conn.execute(f"SELECT count(*), sum(i) FROM lake.main.{table}").fetchone()
reader.close()
print(json.dumps([int(listed), int(count), int(total)]))
"""


def _default_async_threads() -> int:
    row = duckdb.connect().execute("SELECT current_setting('async_threads')").fetchone()
    assert row is not None
    return int(row[0])


def _write(storage: HfStorage, work: Path, files: int) -> None:
    """One commit: `TABLE` with `files` registered data files, each stored under the session's purged prefix."""
    lease = Lease.acquire(storage, holder_id="hf-many-files", run_id=f"many-{uuid.uuid4().hex[:8]}")
    assert lease is not None, "the writer could not take the lease"
    lease.start_heartbeat()
    try:
        cat = WorkingCatalog.open(storage, load_defaults("hf"), lease, work_dir=work / "catalog")
        try:
            cat.conn.execute(f"DROP TABLE IF EXISTS lake.main.{TABLE}")
            cat.conn.execute(f"CREATE TABLE lake.main.{TABLE} (i BIGINT)")
            for n in range(files):
                local = work / f"f{n}.parquet"
                cat.conn.execute(
                    f"COPY (SELECT range AS i FROM range({n * ROWS_PER_FILE}, {(n + 1) * ROWS_PER_FILE})) "  # noqa: S608
                    f"TO '{local.as_posix()}' (FORMAT parquet)"
                )
                key = CanonicalKey("lake", f"data/_live/{RUN}/many-files/f{n}.parquet")
                storage.put("lake", key.path, local)
                cat.conn.execute("CALL ducklake_add_data_files('lake', ?, ?)", [TABLE, reader_uri(storage, key)])
            cat.commit("run_end")
        finally:
            cat.close()
    finally:
        lease.release()


def _drop(storage: HfStorage, work: Path) -> None:
    """Drop `TABLE` in a new commit, so CURRENT names no file that the session purge deletes."""
    reset_lease(storage)
    lease = Lease.acquire(storage, holder_id="hf-many-files", run_id=f"drop-{uuid.uuid4().hex[:8]}")
    assert lease is not None, "the cleanup could not take the lease"
    try:
        cat = WorkingCatalog.open(storage, load_defaults("hf"), lease, work_dir=work / "drop")
        try:
            cat.conn.execute(f"DROP TABLE IF EXISTS lake.main.{TABLE}")
            cat.commit("run_end")
        finally:
            cat.close()
    finally:
        lease.release()


def test_reader_scans_more_registered_files_than_async_threads(test_lake: HfStorage, tmp_path: Path) -> None:
    files = _default_async_threads() + 4
    total = files * ROWS_PER_FILE
    reset_lease(test_lake)
    _write(test_lake, tmp_path, files)
    try:
        try:
            done = subprocess.run(  # noqa: S603 - this interpreter, a constant script, a constant table name
                [sys.executable, "-c", _CHILD, TABLE],
                env=os.environ,
                capture_output=True,
                text=True,
                timeout=READ_LIMIT_S,
                check=False,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"the hf:// read of {files} data files did not return in {READ_LIMIT_S} s; child killed")
        assert done.returncode == 0, f"the reader child failed: {done.stderr[-2000:]}"
        listed, count, summed = json.loads(done.stdout.strip().splitlines()[-1])
    finally:
        _drop(test_lake, tmp_path)
    assert listed >= files, f"the table has {listed} data files, fewer than the {files} the test needs (non-vacuity)"
    assert (count, summed) == (total, sum(range(total)))
