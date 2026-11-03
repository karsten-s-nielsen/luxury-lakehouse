"""S1 Task 9: the `_visibility` column on encrypted-local bronze and the atomic visibility flip (plan T9).

On an encrypted-local lake the restricted copy is a column, not a second root. A native write sets `_visibility`
from the match's source flag; `MatchFiles.move` flips it, `ops_matches.visibility` and lineage together in one
published version, so `_visibility` always equals `ops_matches.visibility`. HF and unencrypted-local keep the
`place_root` two-root model and carry no `_visibility` column.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from lakekit import PLAYERS, LakeKit, StubLease, lake_roots, players
from runkit import checkpoints_for

from lakehouse.bronze.writers import VISIBILITY_COLUMN, LocalNativeWriter, Put
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.lineage.model import LineageRow, write_lineage
from lakehouse.raw.archive import RawArchive
from lakehouse.runs.history import MatchRow, read_table, replace_match
from lakehouse.runs.match_files import MatchFiles
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
Vis = Literal["public", "private"]


def _storage(base: Path) -> LocalStorage:
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(lake_roots(base))


def _match_row(match_key: int, visibility: Vis) -> MatchRow:
    return MatchRow(
        match_key=match_key,
        provider="skillcorner",
        native_match_id=str(match_key),
        visibility=visibility,
        kept_channel="pining",
        kept_reason="kept",
        competition_id=None,
        competition_name=None,
        season_id=None,
        season_name=None,
        has_360_frames=None,
        run_id="run-1",
        updated_at=_T,
    )


def _lineage_row(match_key: int, visibility: Vis) -> LineageRow:
    return LineageRow(
        match_key=match_key,
        provider="skillcorner",
        channel="pining",
        channel_id="c1",
        artifact="tracking",
        change_signal=None,
        delivery_sha256="d",
        content_sha256="c",
        wrapper="none",
        first_seen=_T,
        last_listed=_T,
        visibility=visibility,
        kept=True,
        reason="kept",
    )


def _write_native_match(
    cat: WorkingCatalog, storage: LocalStorage, base: Path, match_key: int, visibility: Vis
) -> None:
    """Native-write one match and seed its `ops_matches` + lineage rows, as the run path does, then publish."""
    writer = LocalNativeWriter(cat, storage)
    put = Put(PLAYERS, players(match_key, 5, 0), is_tracking=True, schema=None, source_hash=None)
    row = writer.write_table(
        put,
        match_key=match_key,
        visibility=visibility,
        content_version="v1",
        run_id="run-1",
        out_dir=base / "out",
        ingested_at=_T,
    )
    writer.swap(match_key, [row], set(), also=lambda: None)
    replace_match(cat.conn, _match_row(match_key, visibility), [], [])
    write_lineage(cat.conn, [_lineage_row(match_key, visibility)])
    cat.commit("run_end")


def _visibilities(cat: WorkingCatalog, match_key: int) -> list[str]:
    rows = cat.conn.execute(
        f'SELECT {VISIBILITY_COLUMN} FROM lake.main."{PLAYERS}" WHERE match_key = ?',  # noqa: S608 - constant column
        [match_key],
    ).fetchall()
    return [str(r[0]) for r in rows]


def test_native_write_sets_visibility_from_the_source_flag(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    try:
        _write_native_match(cat, storage, tmp_path, 100, "public")
        assert _visibilities(cat, 100) == ["public"] * 5
        assert read_table(cat.conn, "ops_matches")[0]["visibility"] == "public"
    finally:
        cat.close()


def test_flip_updates_column_ops_matches_and_lineage_as_one_version(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    try:
        _write_native_match(cat, storage, tmp_path, 100, "public")
        mover = MatchFiles(
            cat,
            storage,
            RawArchive(storage, enabled=False),
            "run-1",
            lambda: _T,
            checkpoints=checkpoints_for(cat, storage),
        )
        mover.move(100, "private", mover.ops_matches()[100])

        column = set(_visibilities(cat, 100))
        ops = read_table(cat.conn, "ops_matches")[0]["visibility"]
        lineage = cat.conn.execute(
            "SELECT DISTINCT visibility FROM lake.main.ops_lineage WHERE match_key = 100"
        ).fetchall()
        assert column == {"private"}, "the bronze _visibility rows flip to private"
        assert ops == "private", "ops_matches is the authoritative flag"
        assert column == {ops}, "the integrity invariant: _visibility equals ops_matches.visibility"
        assert [str(r[0]) for r in lineage] == ["private"], "kept lineage follows the flip"
    finally:
        cat.close()


def test_unencrypted_bronze_carries_no_visibility_column(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    kit = LakeKit(tmp_path, lake_roots(tmp_path), storage)
    cat = kit.catalog()  # keyless: the add_data_files two-root path
    try:
        kit.add_match(cat, 200, visibility="public", frames=5, offset=0)
        kit.commit(cat)
        columns = [
            str(r[0])
            for r in cat.conn.execute(
                "SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' "
                "AND schema_name = 'main' AND table_name = ?",
                [PLAYERS],
            ).fetchall()
        ]
        assert VISIBILITY_COLUMN not in columns, "unencrypted-local keeps today's bronze schema"
    finally:
        cat.close()
