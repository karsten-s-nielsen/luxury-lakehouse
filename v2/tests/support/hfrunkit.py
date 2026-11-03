"""The shared ingestion scenario for the HF run tests (plan Task 33 item 15), and its catalog assertions.

One `run_ingestion` on whatever storage it is given: a public match and a private match, read by the real SkillCorner
parser from the repository's fixture match (the private one re-keyed to a second native id), and dbt. It is run on a
local lake by `tests/e2e/test_hf_run_scenario_local.py` (so the logic is proved in CI) and on the throwaway HF test
lake by `tests/hf/test_run_hf_live.py` (so the HF write path is proved). A run builds the dbt dimensions
(contract-enforced tables, with real player rows), inserts a run-history row, writes and registers a tracking file,
and stores DuckLake-native files (the dbt tables exceed the inlining limit); the private match's bronze goes to the
restricted root. The real parser replaced a fake one on 2026-10-31 (T3 Task 12): the fake emitted no players and every
native write stayed inlined, so the hf_space reader's `dim_players` check and the sweep's path-form check could never
pass.
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hfkit import BARRIER_ARMED
from runkit import NOW, FakeAdapter, active, entry, save_settings

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.catalog.cleanup import run_cleanup
from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.runs.dbt import run_dbt
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion
from lakehouse.storage.base import Storage
from lakehouse.storage.lease import Lease

__all__ = [
    "BARRIER_BUILD_TABLE",
    "PRIVATE_MATCH",
    "PUBLIC_MATCH",
    "TRACKING_TABLE",
    "assert_run_built",
    "paced_commits",
    "run_scenario",
    "sweep_scenario",
]

BARRIER_BUILD_TABLE = "ops_barrier"  # the paced writer's table; the reader watches its row count move across commits

PROVIDER = "skillcorner"
MODULE = "skillcorner_public"
PUBLIC_MATCH = "1899585"  # the repository's SkillCorner fixture match (open data)
PRIVATE_MATCH = "2899585"  # the same fixture, re-keyed to a second native id
TRACKING_TABLE: BronzeTable = bronze_table(f"{PROVIDER}_tracking_players")
_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "skillcorner"
_FIXTURE_ID = re.compile(rb"(?<![0-9])1899585(?![0-9])")


def _match_files(native: str) -> dict[str, bytes]:
    """The fixture's match file (players, teams, pitch) and tracking slice, with the match id re-keyed to `native`."""
    files = {
        "match": (_FIXTURES / "match_1899585.json").read_bytes(),
        "tracking": (_FIXTURES / "tracking_1899585_slice.jsonl").read_bytes(),
    }
    return {artifact: _FIXTURE_ID.sub(native.encode(), data) for artifact, data in files.items()}


def run_scenario(storage: Storage, *, work_dir: Path) -> RunResult:
    """Save the config and run one ingestion of a public and a private match, with dbt."""
    save_settings(storage, [active(MODULE)])
    natives = {PUBLIC_MATCH: "public", PRIVATE_MATCH: "private"}
    files = {native: _match_files(native) for native in natives}
    adapter = FakeAdapter(
        MODULE,
        PROVIDER,
        "public",
        [
            entry(MODULE, native, visibility=vis, signals=dict.fromkeys(files[native], "s1"), competition=("61", "95"))
            for native, vis in natives.items()
        ],
        {(native, artifact): data for native, parts in files.items() for artifact, data in parts.items()},
    )
    return run_ingestion(
        storage=storage,
        trigger="schedule",
        adapters={MODULE: adapter},
        parsers={PROVIDER: SkillCornerParser()},
        now=lambda: NOW,
        work_dir=work_dir,
        dbt=run_dbt,
    )


def assert_run_built(storage: Storage, result: RunResult) -> dict[str, Any]:
    """The backend-agnostic checks of a scenario run; returns the registered tracking file's `ops_match_files` row,
    so the caller can assert its `read_uri` (an `hf://` URI on HF, a local path on a local lake)."""
    assert result.status == "ok", result.errors
    reader = CatalogReader(storage)
    try:
        with reader.session() as conn:
            [kind] = conn.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_catalog = 'lake' AND table_name = 'dim_players'"
            ).fetchall()
            assert kind[0] == "BASE TABLE", "dbt must build dim_players as a contract-enforced table"
            runs = [row for row in read_table(conn, "ops_runs") if row["run_id"] == result.run_id]
            assert runs and runs[0]["status"] == "ok", "the run inserts an ok run-history row"
            files = read_table(conn, "ops_match_files")
    finally:
        reader.close()
    tracking = [row for row in files if str(row["bronze_table"]).endswith("_tracking_players")]
    assert tracking, "the run registers a tracking file"
    assert any(row["rows"] > 0 for row in tracking), "the tracking file has rows"
    return tracking[0]


def paced_commits(
    storage: Storage, *, count: int, sleep_s: float, work_dir: Path, armed_value: bytes
) -> tuple[int, int, int]:
    """Take the lease and make `count` paced range-insert commits into `ops_barrier`, so a concurrent reader samples
    several consistent CURRENT sequences (plan Task 33 step 3: the overlap is guaranteed, not left to timing).

    `ops_barrier` is dropped and recreated empty and that CURRENT is published FIRST; only then is `armed_value` (the
    reader's campaign nonce) written to `BARRIER_ARMED`, so the reader never samples a reused lake's residual rows.
    Returns `(final_seq, final_row_count, working_snapshots)`: the published CURRENT sequence, the number of rows the
    table holds at the end (the reader's valid set is `(k, sum(range(k)))` for k in 1..count), and the snapshot count
    of the working copy, which a reader attaching the published version from hf:// must match.
    """
    lease = Lease.acquire(storage, holder_id="hf-paced", run_id=f"paced-{uuid.uuid4().hex[:8]}")
    assert lease is not None, "the paced writer could not take the lease"
    lease.start_heartbeat()
    try:
        cat = WorkingCatalog.open(storage, load_defaults("hf"), lease, work_dir=work_dir)
        try:
            # Start each campaign from an EMPTY ops_barrier and publish that CURRENT before arming the barrier: the
            # test lake is reused, so it may hold rows from an earlier campaign. Only after the empty state is the
            # published CURRENT does the writer set BARRIER_ARMED, so the reader never samples residual rows.
            cat.conn.execute(f"DROP TABLE IF EXISTS lake.main.{BARRIER_BUILD_TABLE}")
            cat.conn.execute(f"CREATE TABLE lake.main.{BARRIER_BUILD_TABLE} (i BIGINT)")
            cat.commit("in_run")
            storage.put("lake", BARRIER_ARMED, armed_value)
            for n in range(count):
                if n:
                    time.sleep(sleep_s)
                cat.conn.execute(f"INSERT INTO lake.main.{BARRIER_BUILD_TABLE} VALUES (?)", [n])  # noqa: S608 - constant name
                cat.commit("in_run" if n < count - 1 else "run_end")
            snaps = cat.conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
            working = int(snaps[0]) if snaps is not None else 0
        finally:
            cat.close()
    finally:
        lease.release()
    return read_current(storage).seq, count, working


_META_SCHEMA = "__ducklake_metadata_lake"  # the DuckLake metadata schema of the `lake` attachment (catalog.cleanup)


def _catalog_path_forms(conn: Any) -> set[str]:
    """The §23.6 path forms the lake's catalog actually holds: a DuckLake-written RELATIVE data file, an
    `hf://`-registered data file, and an `s3://` `data_path`. T33-20 asserts all three are present, so the sweep's
    path-form matching is genuinely exercised (not trivially green on a single-form lake)."""
    forms: set[str] = set()
    data_path = conn.execute(
        f"SELECT value FROM {_META_SCHEMA}.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL"  # noqa: S608
    ).fetchone()
    if data_path is not None and str(data_path[0]).startswith("s3://"):
        forms.add("s3_data_path")
    for path, is_relative in conn.execute(
        f"SELECT path, path_is_relative FROM {_META_SCHEMA}.ducklake_data_file"  # noqa: S608
    ).fetchall():
        if is_relative:
            forms.add("relative")
        elif str(path).startswith("hf://"):
            forms.add("hf")
    return forms


def sweep_scenario(storage: Storage, *, work_dir: Path) -> dict[str, Any]:
    """On an already-populated lake, put an orphan older than the lease start and one younger, run the sweep
    (`run_cleanup`, large-sweep allowed), and return the result for the caller to assert the age fence and that no
    kept-manifest file was swept (plan Task 33 item 15, §23.6 "Sweep with all path forms").

    The 1.1 s gaps put the old orphan, the lease generation and the young orphan in distinct storage seconds, because
    the gateway's `LastModified` has second precision.
    """
    run_id = f"sweep-{uuid.uuid4().hex[:8]}"
    old_key = f"data/_live/{run_id}/old.parquet"
    young_key = f"data/_live/{run_id}/young.parquet"
    storage.put("lake", old_key, b"old-orphan")
    time.sleep(1.1)
    lease = Lease.acquire(storage, holder_id="hf-sweep", run_id=run_id)
    assert lease is not None, "the sweep could not take the lease"
    lease.start_heartbeat()
    try:
        time.sleep(1.1)
        storage.put("lake", young_key, b"young-orphan")  # after the lease start: inside the age fence
        settings = load_defaults("hf")
        kept_keys = list(read_manifest(storage, read_current(storage).seq).canonical_keys)
        cat = WorkingCatalog.open(storage, settings, lease, work_dir=work_dir)
        path_forms: set[str] = set()
        try:
            report = run_cleanup(
                cat, storage, settings=settings, lease=lease, allow_large_sweep=True, now=datetime.now(UTC)
            )
            cat.commit("run_end")
            path_forms = _catalog_path_forms(cat.conn)  # the §23.6 forms present in the catalog (T33-20)
        finally:
            cat.close()
        # The data-loss guard: every CURRENT-manifest file must still physically exist after the sweep. This is an
        # independent check (storage.exists), not the canonical-key-set intersection that run_cleanup already
        # enforces, so it catches a listing whose path form mis-maps to another key and deletes a live file.
        kept_survived = all(storage.exists(key.root, key.path) for key in kept_keys)
    finally:
        lease.release()
    swept = {str(key) for key in report.swept}
    return {
        "old_key": f"lake:{old_key}",
        "young_key": f"lake:{young_key}",
        "swept": swept,
        "kept": {str(key) for key in kept_keys},
        "kept_survived": kept_survived,
        "young_exists": storage.exists("lake", young_key),
        "path_forms": sorted(path_forms),  # §23.6 forms present in the catalog (T33-20)
    }
