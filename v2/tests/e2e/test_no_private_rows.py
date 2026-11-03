"""Restricted root placement, last clause (spec §9, §23.6 "Restricted root placement"; plan Task 23).

A full run with one private match and one public match, resolution and dbt included. Every file that DuckDB itself
wrote into the lake root belongs to a table of the allowlist `{dim_*, *_links, ops_*, lineage_*}` (from
`ducklake_list_files` of every table, from DuckLake's inlined data, and from the storage listing of `data/main/`),
and no such file holds a row-level column on a row of the private match.
"""

from __future__ import annotations

import fnmatch
from pathlib import Path

import duckdb
import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.ids.keys import match_key
from lakehouse.runs.dbt import NATIVE_TABLE_ALLOWLIST, allowed_native_table
from lakehouse.runs.run import run_ingestion
from lakehouse.services.authz import Principal, Role, scoped_reads
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV

PRIVATE, PUBLIC = "3812", "3813"
ROW_LEVEL = {"frame", "x_mm", "y_mm", "x_cm", "y_cm", "line"}  # the §11.5 core names and the fake event payload
_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
# A long, unique marker: a short id like "3812" would collide by chance in a multi-MB encrypted file.
_PRIVATE_MARKER = b"MARKER-PRIVATE-7f3a9c2e-DO-NOT-LEAK-THIS-ROW"
_FAKE_EVENTS = "gradientsports_fake_events"


def _allowed(name: str) -> bool:
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in NATIVE_TABLE_ALLOWLIST)


def test_no_private_rows_in_lake_native_files(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("gradientsports_pining")])
    entries = [
        entry("gradientsports_pining", PRIVATE, visibility="private", competition=("38", "2022")),
        entry("gradientsports_pining", PUBLIC, visibility="public", competition=("38", "2022")),
    ]
    files = {(n, "events"): f"line {n}\nsecond {n}".encode() for n in (PRIVATE, PUBLIC)}
    adapter = FakeAdapter("gradientsports_pining", "gradientsports", "pining", entries, files)
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"gradientsports_pining": adapter},
        parsers={"gradientsports": FakeParser("gradientsports")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )
    assert result.status == "ok", result.errors
    private_key = match_key("gradientsports", PRIVATE)
    lake_root = Path(kit.roots.lake.local_path or "").resolve()
    native_prefix = (lake_root / "data" / "main").as_posix()
    # 1. the storage listing of the native data path
    listed = [info.key for info in kit.storage.list("lake", "data/main/")]
    assert all(_allowed(key.split("/")[2]) for key in listed), listed
    with CatalogReader(kit.storage).session() as conn:
        tables = [
            name
            for (name,) in conn.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main'"
            ).fetchall()
        ]
        native_files: list[str] = []
        for table in tables:
            for (path,) in conn.execute("SELECT data_file FROM ducklake_list_files('lake', ?)", [table]).fetchall():
                resolved = Path(str(path)).resolve().as_posix()
                if resolved.startswith(native_prefix):
                    # 2. a file DuckDB wrote belongs to an allowlisted table
                    assert _allowed(table), (table, resolved)
                    native_files.append(resolved)
        # 3. data that DuckLake inlined into the catalog belongs to an allowlisted table
        inlined_tables = conn.execute(
            "SELECT DISTINCT t.table_name, i.table_name FROM __ducklake_metadata_lake.ducklake_inlined_data_tables i "
            "JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = i.table_id"
        ).fetchall()
        with_rows: set[str] = set()
        for table, inlined in inlined_tables:
            count = conn.execute(f'SELECT count(*) FROM __ducklake_metadata_lake."{inlined}"').fetchone()  # noqa: S608 - a catalog name
            if count is not None and count[0] > 0:
                with_rows.add(str(table))
        assert sorted(name for name in with_rows if not _allowed(name)) == []
    # 4. no native file holds a row-level column on a row of the private match
    reader = duckdb.connect()
    for path in native_files:
        columns = {c for (c,) in reader.execute("SELECT name FROM parquet_schema(?)", [path]).fetchall()}
        if "match_key" in columns and columns & ROW_LEVEL:
            found = reader.execute("SELECT count(*) FROM read_parquet(?) WHERE match_key = ?", [path, private_key])
            assert found.fetchone() == (0,), path
    # the private match's row-level bronze lives in the restricted root only
    restricted = [info.key for info in kit.storage.list("restricted", "data/bronze/")]
    assert any(key.rsplit("/", 1)[-1].startswith(f"{private_key}-") for key in restricted)
    assert not any(
        info.key.rsplit("/", 1)[-1].startswith(f"{private_key}-") for info in kit.storage.list("lake", "data/bronze/")
    )


def test_no_private_rows_encrypted_local(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """The oracle, forked for encrypted-local (spec §9, §23.6; plan T14). Here visibility is the `_visibility`
    column, not a second root, so the checks change: (a) no private marker survives in cleartext anywhere under the
    single lake root (bronze, catalog, raw are all encrypted); (b) the one row-level gold model is a view, not a
    materialized public table; (c) the read seam drops the private match's rows for an anonymous caller; and the
    §23.6 native-table check allows bronze as native (`allowed_native_table(..., encrypted_local=True)`)."""
    monkeypatch.setenv(ENCRYPTION_KEY_ENV, _KEY)
    save_settings(kit.storage, [active("gradientsports_pining")])
    entries = [
        entry("gradientsports_pining", PRIVATE, visibility="private", competition=("38", "2022")),
        entry("gradientsports_pining", PUBLIC, visibility="public", competition=("38", "2022")),
    ]
    files = {(PRIVATE, "events"): _PRIVATE_MARKER + b"\n", (PUBLIC, "events"): b"public event line\n"}
    adapter = FakeAdapter("gradientsports_pining", "gradientsports", "pining", entries, files)
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"gradientsports_pining": adapter},
        parsers={"gradientsports": FakeParser("gradientsports")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )
    assert result.status == "ok", result.errors
    private_key = match_key("gradientsports", PRIVATE)
    lake_root = Path(kit.roots.lake.local_path or "").resolve()

    # (a) the private row-level marker never appears in cleartext under the single lake root
    offenders = [str(path) for path in lake_root.rglob("*") if path.is_file() and _PRIVATE_MARKER in path.read_bytes()]
    assert offenders == [], offenders

    reader = CatalogReader(kit.storage, env={ENCRYPTION_KEY_ENV: _KEY})
    try:
        with reader.session() as conn:
            # §23.6 adapted: every native lake-root table is allowlisted, or a bronze table (native by design here)
            for name, comment in conn.execute(
                "SELECT table_name, comment FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main'"
            ).fetchall():
                assert allowed_native_table(str(name), comment, encrypted_local=True), name
            # (b) the one row-level gold model is a view, never a materialized public table
            kind = conn.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_catalog = 'lake' AND table_name = 'tracking_cross_provider'"
            ).fetchone()
            assert kind is None or kind[0] == "VIEW", kind
            # (c) the private match's bronze rows carry _visibility='private'
            private_rows = [
                {"match_key": int(mk), "_visibility": str(vis)}
                for mk, vis in conn.execute(
                    f'SELECT match_key, _visibility FROM lake.main."{_FAKE_EVENTS}" WHERE match_key = ?',  # noqa: S608 - constant
                    [private_key],
                ).fetchall()
            ]
    finally:
        reader.close()
    assert private_rows and all(row["_visibility"] == "private" for row in private_rows)
    anon = scoped_reads(None, Principal(None, frozenset({Role.ANONYMOUS})))
    operator = scoped_reads(None, Principal("op", frozenset({Role.OPERATOR})))
    assert anon.visible(private_rows) == [], "an anonymous caller sees no private row"
    assert len(operator.visible(private_rows)) == len(private_rows), "an operator sees them all"
