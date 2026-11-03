"""Catalog compaction on the detached catalog file and the keyed plain open (T3 spec D7, decisions 9 and 17; plan round
3 Task 10). The rewrite copies every metadata table into a fresh file (`COPY FROM DATABASE`), verifies rows, checksums
and schema, and only then replaces the old file; it never touches a data file. The keyed plain open maps only a wrong
key to `ConfigError` (a corrupt file raises the same DuckDB class with another message)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
from lakekit import StubLease, lake_roots

from lakehouse.catalog import compact as compact_module
from lakehouse.catalog.compact import COMPACT_MIN_BYTES, compact_catalog_file
from lakehouse.catalog.duck import keyed_plain_attach
from lakehouse.catalog.publish import version_key
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import CheckpointConfig, load_defaults
from lakehouse.errors import ConfigError
from lakehouse.runs.checkpoint import CheckpointPolicy
from lakehouse.storage.local import LocalStorage

KEY = "9" * 64
WRONG = "8" * 64


def _storage(base: Path) -> LocalStorage:
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    return LocalStorage(lake_roots(base))


def _catalog(base: Path, key: str | None, *, rows: int = 2000) -> tuple[LocalStorage, WorkingCatalog]:
    storage = _storage(base)
    env = {"LAKEHOUSE_ENCRYPTION_KEY": key} if key is not None else {}
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env=env)
    cat.conn.execute("CREATE TABLE lake.main.ops_probe (seq INTEGER, label VARCHAR)")
    for start in range(0, rows, 100):  # many small transactions fragment the catalog file
        cat.conn.execute(
            "INSERT INTO lake.main.ops_probe SELECT range, 'x' || range FROM range(?, ?)", [start, start + 100]
        )
    return storage, cat


def _plain_open(path: Path, key: str | None) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    if key is None:
        conn.execute(f"ATTACH '{path.as_posix()}' AS m")
    else:
        keyed_plain_attach(conn, path, alias="m", key=key)
    return conn


def _snapshot_of(path: Path, key: str | None) -> tuple[list[Any], dict[str, Any]]:
    conn = _plain_open(path, key)
    try:
        schema = sorted(
            conn.execute(
                "SELECT table_name, column_name, data_type, column_default, is_nullable FROM duckdb_columns() "
                "WHERE database_name = 'm'"
            ).fetchall()
        )
        content = {
            table: conn.execute(f'SELECT count(*), coalesce(sum(hash(t)), 0) FROM m.main."{table}" AS t').fetchone()  # noqa: S608 - names from the catalog
            for (table,) in conn.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'm'").fetchall()
        }
    finally:
        conn.close()
    return schema, content


@pytest.fixture
def no_threshold(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", 0)
    yield


@pytest.mark.parametrize("key", [None, KEY], ids=["plain", "keyed"])
def test_compaction_preserves_tables_rows_checksums_and_schema(
    tmp_path: Path, key: str | None, no_threshold: None
) -> None:
    _, cat = _catalog(tmp_path, key)
    path = cat.detach()
    before = _snapshot_of(path, key)
    result = compact_catalog_file(path, key=key)
    assert (result.compacted, result.failed_check) == (True, None)
    assert _snapshot_of(path, key) == before
    cat.close()


@pytest.mark.parametrize("key", [None, KEY], ids=["plain", "keyed"])
def test_compacted_catalog_reopens_with_the_ducklake_attach(
    tmp_path: Path, key: str | None, no_threshold: None
) -> None:
    _, cat = _catalog(tmp_path, key)
    path = cat.detach()
    assert compact_catalog_file(path, key=key).compacted
    cat.attach()
    row = cat.conn.execute("SELECT count(*) FROM lake.main.ops_probe").fetchone()
    assert row is not None and row[0] == 2000
    cat.close()


def test_compaction_touches_no_data_file(tmp_path: Path, no_threshold: None) -> None:
    _, cat = _catalog(tmp_path, KEY)
    path = cat.detach()
    data = tmp_path / "lake" / "data"
    before = {p: p.read_bytes() for p in data.rglob("*") if p.is_file()}
    assert compact_catalog_file(path, key=KEY).compacted
    assert {p: p.read_bytes() for p in data.rglob("*") if p.is_file()} == before
    cat.close()


def test_wrong_key_raises_config_error_without_the_key(tmp_path: Path) -> None:
    _, cat = _catalog(tmp_path, KEY, rows=100)
    path = cat.detach()
    conn = duckdb.connect()
    with pytest.raises(ConfigError) as raised:
        keyed_plain_attach(conn, path, alias="m", key=WRONG)
    assert WRONG not in str(raised.value) and KEY not in str(raised.value)
    cat.close()


def test_corrupt_file_keeps_its_own_error(tmp_path: Path) -> None:
    _, cat = _catalog(tmp_path, KEY, rows=100)
    path = cat.detach()
    bad = tmp_path / "corrupt.ducklake"
    data = bytearray(path.read_bytes())
    data[4096:8192] = b"\x00" * 4096  # a database header: the right key, a broken file
    bad.write_bytes(bytes(data))
    with pytest.raises(duckdb.Error) as raised:
        keyed_plain_attach(duckdb.connect(), bad, alias="m", key=KEY)
    assert not isinstance(raised.value, ConfigError)
    assert "Wrong encryption key" not in str(raised.value)
    cat.close()


class _Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.text: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.text.append(f"{record.getMessage()} {record.__dict__}")


def test_key_never_logged(tmp_path: Path, no_threshold: None) -> None:
    handler = _Collect()
    root = logging.getLogger("lakehouse")
    root.addHandler(handler)
    old = root.level
    root.setLevel(logging.DEBUG)
    try:
        _, cat = _catalog(tmp_path, KEY)
        path = cat.detach()
        compact_catalog_file(path, key=KEY)
        cat.attach()
        cat.commit("run_end")
        cat.close()
    finally:
        root.removeHandler(handler)
        root.setLevel(old)
    assert handler.text, "nothing was logged"
    assert not any(KEY in line for line in handler.text)


def test_keyed_plain_open_sets_temp_file_encryption(tmp_path: Path) -> None:
    _, cat = _catalog(tmp_path, KEY, rows=100)
    path = cat.detach()
    conn = duckdb.connect()
    keyed_plain_attach(conn, path, alias="m", key=KEY)
    row = conn.execute("SELECT current_setting('temp_file_encryption')").fetchone()
    assert row is not None and row[0] is True
    conn.close()
    cat.close()


def test_commit_compacts_at_or_above_threshold_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, cat = _catalog(tmp_path, KEY)
    size = cat.detach().stat().st_size
    cat.attach()
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", size + 1)
    below = cat.commit("in_run")
    assert below.compaction is not None and not below.compaction.compacted
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", 1)
    above = cat.commit("in_run")
    assert above.compaction is not None and above.compaction.compacted
    assert COMPACT_MIN_BYTES == 8 * 1024 * 1024
    cat.close()


def test_schema_mismatch_keeps_the_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_threshold: None
) -> None:
    """Rows and checksums agree but the fresh file's schema differs: the rewrite is discarded, the original stays."""
    _, cat = _catalog(tmp_path, KEY)
    path = cat.detach()
    original = path.read_bytes()
    real: Callable[[duckdb.DuckDBPyConnection, str], dict[str, list[tuple[Any, ...]]]] = vars(compact_module)["_schema"]

    def drifting(conn: duckdb.DuckDBPyConnection, alias: str) -> dict[str, list[tuple[Any, ...]]]:
        found = real(conn, alias)
        return {**found, "columns": [("forced",)]} if alias == "dst" else found

    monkeypatch.setattr(compact_module, "_schema", drifting)
    result = compact_catalog_file(path, key=KEY)
    assert (result.compacted, result.failed_check) == (False, "schema")
    assert path.read_bytes() == original
    assert not path.with_name(path.name + ".compact").exists()
    cat.close()


def test_verification_failure_publishes_original_with_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_threshold: None
) -> None:
    storage, cat = _catalog(tmp_path, KEY)
    calls = {"n": 0}
    real: Callable[[duckdb.DuckDBPyConnection, str], dict[str, Any]] = vars(compact_module)["_content"]

    def drifting(conn: duckdb.DuckDBPyConnection, alias: str) -> dict[str, Any]:
        calls["n"] += 1
        found = real(conn, alias)
        return found if calls["n"] % 2 else {**found, "forced": (1, 1)}  # the fresh copy's checksum differs

    monkeypatch.setattr(compact_module, "_content", drifting)
    original = cat.detach().read_bytes()
    cat.attach()
    sink: list[dict[str, Any]] = []

    def errors(**kwargs: Any) -> None:
        sink.append(kwargs)

    policy = CheckpointPolicy(
        cat,
        CheckpointConfig(),
        retention=load_defaults("laptop").retention,
        run_id="run-1",
        now=lambda: datetime.now(UTC),
        errors=errors,
        storage=storage,
    )
    record = policy.now("move")
    assert storage.get("lake", version_key(record.seq)) == original  # the uncompacted catalog was published
    [warning] = [call for call in sink if call["kind"] == "catalog_compaction"]
    assert "checksum" in warning["message"]
    cat.close()
