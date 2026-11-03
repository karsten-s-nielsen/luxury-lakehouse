"""One insert per ops table per write (T3 spec D6, decision 18; plan round 3 Task 9c).

`executemany` ran one `INSERT` per row, and on the encrypted-local path (`DATA_INLINING_ROW_LIMIT 0`) each `INSERT`
stores a data file: the Mac Mini catalog held 560,582 one-row `ops_copy_fields` files. A write now inserts its rows
with one statement, so it stores at most one file per ops table it touches."""

from __future__ import annotations

from pathlib import Path

import pytest
from lakekit import LakeKit, StubLease, lake_roots
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.duck import insert_rows
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.resolve import tables as resolve_tables
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.local import LocalStorage

KEY = {"LAKEHOUSE_ENCRYPTION_KEY": "e" * 64}

_FILES_PER_SNAPSHOT = """
SELECT t.table_name, max(n) FROM (
    SELECT table_id, begin_snapshot, count(*) AS n FROM __ducklake_metadata_lake.ducklake_data_file
    GROUP BY table_id, begin_snapshot
) AS f JOIN __ducklake_metadata_lake.ducklake_table AS t USING (table_id)
WHERE t.table_name LIKE 'ops_%' GROUP BY t.table_name
"""


def _keyed(tmp_path: Path) -> WorkingCatalog:
    for name in ("lake", "lake-restricted"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    return WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env=KEY)


def _files_of(cat: WorkingCatalog, table: str) -> int:
    row = cat.conn.execute(
        "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file AS f "
        "JOIN __ducklake_metadata_lake.ducklake_table AS t USING (table_id) WHERE t.table_name = ?",
        [table],
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_match_write_stores_one_file_per_ops_table(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    natives = ["J03WPY", "J03WMX"]
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={
            "idsse_public": FakeAdapter(
                "idsse_public", "idsse", "public", [entry("idsse_public", n) for n in natives], files
            )
        },
        parsers={"idsse": FakeParser("idsse")},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        env=KEY,
        dbt=None,
    )
    keyed = WorkingCatalog.open(
        kit.storage, kit.settings, StubLease(run_id="run-read"), work_dir=kit.base / "r", env=KEY
    )
    try:
        per_snapshot = dict(keyed.conn.execute(_FILES_PER_SNAPSHOT).fetchall())
    finally:
        keyed.close()
    assert per_snapshot, "the run wrote no ops table"
    assert max(per_snapshot.values()) == 1, per_snapshot  # ops_copy_fields included: one file per write


def test_insert_rows_stores_one_file_for_many_rows(tmp_path: Path) -> None:
    cat = _keyed(tmp_path)
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe (match_key BIGINT, field VARCHAR, nonnull BOOLEAN)")
        insert_rows(
            cat.conn, "ops_probe", ("match_key", "field", "nonnull"), [[1, f"f{i}", i % 2 == 0] for i in range(300)]
        )
        assert _files_of(cat, "ops_probe") == 1
    finally:
        cat.close()


def _value(kind: str, index: int) -> object:
    base = kind.split()[0]
    if base == "VARCHAR":
        return f"v{index}"
    if base in ("BIGINT", "INTEGER"):
        return index
    if base == "DOUBLE":
        return 0.5
    if base == "BOOLEAN":
        return True
    if base == "TIMESTAMPTZ":
        return NOW
    raise AssertionError(f"no test value for {kind}")


def test_resolution_insert_stores_one_file_per_table(tmp_path: Path) -> None:
    cat = _keyed(tmp_path)
    try:
        table = "gold_player_links"
        columns = resolve_tables.TABLES[table]
        rows = [{name: _value(kind, index) for name, kind in columns} for index in range(50)]
        resolve_tables.insert(cat.conn, table, rows)
        assert _files_of(cat, table) == 1
    finally:
        cat.close()


@pytest.mark.parametrize("count", [0, 1, 1000])
def test_insert_rows_keeps_content_and_order(kit: LakeKit, count: int) -> None:
    cat = kit.catalog()
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe (seq INTEGER, label VARCHAR)")
        rows = [[index, f"row-{index}"] for index in range(count)]
        insert_rows(cat.conn, "ops_probe", ("seq", "label"), rows)
        back = cat.conn.execute("SELECT seq, label FROM lake.main.ops_probe ORDER BY seq").fetchall()
        assert [list(row) for row in back] == rows
    finally:
        cat.close()


def test_insert_rows_refuses_a_bad_identifier(kit: LakeKit) -> None:
    cat = kit.catalog()
    try:
        with pytest.raises(ValueError, match="identifier"):
            insert_rows(cat.conn, "ops_probe; DROP TABLE x", ("seq",), [[1]])
        with pytest.raises(ValueError, match="identifier"):
            insert_rows(cat.conn, "ops_probe", ("seq) VALUES (1); --",), [[1]])
    finally:
        cat.close()


def test_register_ops_write_over_the_inline_limit_is_one_file(kit: LakeKit) -> None:
    cat = kit.catalog()  # unencrypted: the default inlining limit (10 rows) applies
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe (seq INTEGER)")
        insert_rows(cat.conn, "ops_probe", ("seq",), [[index] for index in range(50)])
        assert _files_of(cat, "ops_probe") == 1
    finally:
        cat.close()
