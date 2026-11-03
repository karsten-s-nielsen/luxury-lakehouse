"""The §10.2 file format and one file per table on the native path (T3 spec D6, decisions 18 and 19; plan round 3
Task 9c). A writing attach makes sure of the catalog's DuckLake options `parquet_compression = zstd`,
`parquet_compression_level = 3` and `parquet_version = 2`; the spike measured that they give zstd level 3,
`DELTA_BINARY_PACKED` on the integer columns and dictionary encoding elsewhere."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
from lakekit import StubLease, lake_roots

from lakehouse.catalog.duck import PARQUET_OPTIONS_STORED, connect_duckdb, encryption_options, sql_text
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.local import LocalStorage

KEY = "f" * 64
ENV = {"LAKEHOUSE_ENCRYPTION_KEY": KEY}
MARKER = "T3CLEARTEXTMARKER"


def _storage(tmp_path: Path) -> LocalStorage:
    for name in ("lake", "lake-restricted"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    return LocalStorage(lake_roots(tmp_path))


def _open(storage: LocalStorage, work: Path) -> WorkingCatalog:
    return WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=work, env=ENV)


def _options(cat: WorkingCatalog) -> dict[str, str]:
    rows = cat.conn.execute("SELECT option_name, value FROM ducklake_options('lake')").fetchall()
    return {str(name): str(value) for name, value in rows}


def _snapshots(cat: WorkingCatalog) -> int:
    row = cat.conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
    assert row is not None
    return int(row[0])


def test_writing_attach_sets_the_parquet_options_on_a_new_catalog(tmp_path: Path) -> None:
    cat = _open(_storage(tmp_path), tmp_path / "w")
    try:
        assert {name: _options(cat).get(name) for name in PARQUET_OPTIONS_STORED} == PARQUET_OPTIONS_STORED
    finally:
        cat.close()


def test_writing_attach_sets_the_parquet_options_on_an_existing_catalog(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    first = _open(storage, tmp_path / "w1")
    first.commit("run_end")
    first.close()
    second = _open(storage, tmp_path / "w2")  # a catalog published before T3 gets the options at its next attach
    try:
        assert {name: _options(second).get(name) for name in PARQUET_OPTIONS_STORED} == PARQUET_OPTIONS_STORED
    finally:
        second.close()


def test_reattach_with_options_set_changes_nothing(tmp_path: Path) -> None:
    cat = _open(_storage(tmp_path), tmp_path / "w")
    try:
        before = _snapshots(cat)
        cat.detach()
        cat.attach()
        assert _snapshots(cat) == before
    finally:
        cat.close()


def test_native_writes_one_file_per_table(tmp_path: Path) -> None:
    """A regression pin (the spike showed it already holds): 8,437,208 rows in 421 chunks, as a parser delivers a
    gradientsports-scale tracking table, become exactly one encrypted data file."""
    rows, chunks = 8_437_208, 421
    per = -(-rows // chunks)
    batches: list[pa.RecordBatch] = []
    for start in range(0, rows, per):
        n = min(per, rows - start)
        batches.append(
            pa.record_batch(
                {
                    "match_key": pa.array([7] * n, pa.int64()),
                    "frame": pa.array(range(start, start + n), pa.int32()),
                    "label": pa.array([MARKER] * n),
                }
            )
        )
    table = pa.Table.from_batches(batches)
    assert table.column(0).num_chunks == chunks
    data = tmp_path / "data"
    conn = connect_duckdb(env=os.environ, caps=None)
    opts = [f"DATA_PATH {sql_text(data.as_posix() + '/')}", "DATA_INLINING_ROW_LIMIT 0", *encryption_options(KEY)]
    conn.execute(f"ATTACH {sql_text('ducklake:' + (tmp_path / 'cat.ducklake').as_posix())} AS lake ({', '.join(opts)})")
    conn.execute("CREATE TABLE lake.tracking (match_key BIGINT, frame INTEGER, label VARCHAR)")
    conn.register("arrow_in", table)
    conn.execute("INSERT INTO lake.tracking SELECT * FROM arrow_in")
    conn.unregister("arrow_in")
    conn.execute("DETACH lake")
    files = sorted(data.rglob("*.parquet"))
    assert len(files) == 1
    assert MARKER.encode() not in files[0].read_bytes()  # still encrypted
