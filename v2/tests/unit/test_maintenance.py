"""Run-end maintenance keeps every non-bronze DuckLake table at one data file (T12 spec D3).

Each insert into an ops table writes one data file and partial deletes add delete files, which the merge alone never
clears. `maintain_tables` rewrites files with deletes (threshold 0.0, owner decision) and merges the small files in
time slices within a budget, after a fail-closed guard on the bronze tables' `auto_compact = false`. Replaced files
leave only through the existing guarded cleanup.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from lakekit import LakeKit, StubLease, lake_roots

from lakehouse.bronze.schemas import bronze_table
from lakehouse.bronze.writers import select_listing_writer
from lakehouse.catalog import maintenance as maintenance_module
from lakehouse.catalog.maintenance import maintain_tables
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

KEY = "3" * 64
MARKER = "maintenancemarker"
BRONZE = bronze_table("skillcorner_pining_matches")
_MODES = {"register": {}, "native": {ENCRYPTION_KEY_ENV: KEY}}


def _catalog(base: Path, mode: str) -> WorkingCatalog:
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    return WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env=_MODES[mode])


def _ops_files(cat: WorkingCatalog, count: int, *, table: str = "ops_probe", start: int = 0) -> None:
    """`count` inserts of 20 rows each: one data file per insert on both paths (above the inlining limit of 10)."""
    cat.conn.execute(f"CREATE TABLE IF NOT EXISTS lake.main.{table} (k BIGINT, note VARCHAR)")
    for i in range(start, start + count):
        cat.conn.execute(f"INSERT INTO lake.main.{table} SELECT ? * 100 + range, '{MARKER}' FROM range(20)", [i])  # noqa: S608 - test constants


def _files(cat: WorkingCatalog, table: str) -> tuple[int, int]:
    row = cat.conn.execute(
        f"SELECT count(*), count(delete_file) FROM ducklake_list_files('lake', '{table}')"  # noqa: S608 - test constants
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def _content(cat: WorkingCatalog, table: str) -> tuple[Any, ...]:
    row = cat.conn.execute(f"SELECT count(*), sum(hash(k, note)) FROM lake.main.{table}").fetchone()  # noqa: S608 - test constants
    assert row is not None
    return tuple(row)


def _bronze(cat: WorkingCatalog, base: Path, key: int) -> None:
    records = pa.table({"id": pa.array([key], pa.int64()), "visibility": ["public"]})
    writer = select_listing_writer(cat, cat.storage)
    writer.write(
        BRONZE,
        records,
        key=key,
        visibility_column="visibility",
        source_visibility="public",
        content_version=f"v{key}",
        out_dir=base / f"out-{key}",
    )
    writer.swap(BRONZE, key, also=lambda: None)


def _bronze_files(cat: WorkingCatalog) -> set[str]:
    return {
        str(r[0])
        for r in cat.conn.execute(f"SELECT data_file FROM ducklake_list_files('lake', '{BRONZE}')").fetchall()  # noqa: S608 - test constant
    }


@pytest.fixture
def native(tmp_path: Path) -> Iterator[WorkingCatalog]:
    cat = _catalog(tmp_path, "native")
    yield cat
    cat.close()


def test_guard_stops_before_any_maintenance(native: WorkingCatalog) -> None:
    native.conn.execute("CREATE TABLE lake.main.skillcorner_tracking_players (k BIGINT)")
    native.conn.execute(
        "COMMENT ON TABLE lake.main.skillcorner_tracking_players IS 'lakehouse:bronze'"
    )  # no auto_compact
    _ops_files(native, 20)
    with pytest.raises(GuardStopError, match="skillcorner_tracking_players"):
        maintain_tables(native, budget_s=None)
    assert _files(native, "ops_probe") == (20, 0), "nothing is merged when the guard stops"


def test_small_clean_table_is_left_alone(native: WorkingCatalog) -> None:
    _ops_files(native, 16)
    report = maintain_tables(native, budget_s=None)
    assert [t.table for t in report.tables] == []
    assert _files(native, "ops_probe") == (16, 0)


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_ops_table_becomes_one_file(tmp_path: Path, mode: str) -> None:
    cat = _catalog(tmp_path, mode)
    try:
        for key in (1, 2):
            _bronze(cat, tmp_path, key)
        bronze_before = _bronze_files(cat)
        _ops_files(cat, 100)
        before = _content(cat, "ops_probe")
        report = maintain_tables(cat, budget_s=None)
        assert _files(cat, "ops_probe") == (1, 0)
        assert _content(cat, "ops_probe") == before
        assert _bronze_files(cat) == bronze_before, "bronze files are never rewritten or merged"
        [entry] = [t for t in report.tables if t.table == "ops_probe"]
        assert (entry.files_before, entry.files_after) == (100, 1)
        if mode == "native":
            data = tmp_path / "lake" / "data"
            assert all(MARKER.encode() not in p.read_bytes() for p in data.rglob("*.parquet")), "cleartext on disk"
    finally:
        cat.close()


def test_partial_deletes_end_each_run_at_one_file(native: WorkingCatalog) -> None:
    for run in range(5):
        _ops_files(native, 10, start=run * 10)
        native.conn.execute("UPDATE lake.main.ops_probe SET note = 'kept' WHERE k % 7 = 0")  # the lineage `kept` write
        native.conn.execute("DELETE FROM lake.main.ops_probe WHERE k % 11 = 0")  # a history replace
        before = _content(native, "ops_probe")
        maintain_tables(native, budget_s=None)
        assert _files(native, "ops_probe") == (1, 0), f"run {run}"
        assert _content(native, "ops_probe") == before


def test_slices_bound_each_merge_call(native: WorkingCatalog, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maintenance_module, "MERGE_SLICE_FILES", 10)
    merged: list[int] = []
    real = maintenance_module._merge  # pyright: ignore[reportPrivateUsage] - the per-call seam under test

    def spy(cat: WorkingCatalog, table: str, window: Any) -> int:
        count = real(cat, table, window)
        merged.append(count)
        return count

    monkeypatch.setattr(maintenance_module, "_merge", spy)
    _ops_files(native, 35)
    report = maintain_tables(native, budget_s=None)
    assert _files(native, "ops_probe") == (1, 0)
    assert merged and all(count <= 10 for count in merged), merged
    [entry] = report.tables
    assert entry.slices == len(merged) >= 4


def test_budget_spent_stops_new_slices(native: WorkingCatalog, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maintenance_module, "MERGE_SLICE_FILES", 10)
    ticks = iter(range(0, 10**6, 1000))  # every reading of the clock is 1,000 s after the one before
    _ops_files(native, 35)
    report = maintain_tables(native, budget_s=2500, monotonic=lambda: float(next(ticks)))
    assert report.budget_spent and report.left == ("ops_probe",)
    files, _ = _files(native, "ops_probe")
    assert 1 < files < 35, "some slices ran, then the budget stopped new ones"
    maintain_tables(native, budget_s=None)
    assert _files(native, "ops_probe") == (1, 0), "the next call continues"


def test_replaced_files_leave_only_through_the_cleanup(kit: LakeKit) -> None:
    cat = kit.catalog()
    _ops_files(cat, 30)
    old = {
        str(r[0]) for r in cat.conn.execute("SELECT data_file FROM ducklake_list_files('lake', 'ops_probe')").fetchall()
    }
    kit.commit(cat)
    maintain_tables(cat, budget_s=None)
    kit.commit(cat)
    data = kit.base / "lake" / "data"
    on_disk = {p.name for p in data.rglob("*.parquet")}
    assert {Path(f).name for f in old} <= on_disk, "the maintenance deletes nothing"
    kit.day(kit.settings.retention.snapshot_days + 2)
    kit.cleanup(cat)  # snapshot expiry, the kept-version guard and DuckLake's guarded list (no GuardStopError)
    left = {p.name for p in data.rglob("*.parquet")}
    assert not ({Path(f).name for f in old} & left), "the existing cleanup removed the replaced files"
    cat.close()
