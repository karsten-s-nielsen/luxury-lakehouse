"""S1 coupled block: cleanup leaves native bronze intact on an encrypted-local lake (plan T8/T12 follow-through).

On an encrypted-local lake bronze is native, so DuckLake manages its parquet files. `run_cleanup` must not delete a
tracked native bronze file: it runs on the keyed `cat.conn` (no plain-DuckDB open), `_tracked_refs` includes the
native data files (current and ended rows), and native-file expiry is left to DuckLake's own cleanup. This test
proves a match's native bronze still reads back after a cleanup, so the file-based sweep does not strip it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lakekit import PLAYERS, StubLease, lake_roots, players

from lakehouse.bronze.writers import LocalNativeWriter, Put
from lakehouse.catalog.cleanup import run_cleanup
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.runs.history import MatchRow, replace_match
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_ENV = {ENCRYPTION_KEY_ENV: _KEY}


def _match_row() -> MatchRow:
    return MatchRow(
        match_key=100,
        provider="skillcorner",
        native_match_id="100",
        visibility="public",
        kept_channel="pining",
        kept_reason="kept",
        competition_id=None,
        competition_name=None,
        season_id=None,
        season_name=None,
        has_360_frames=None,
        run_id="r",
        updated_at=_T,
    )


def test_cleanup_keeps_native_bronze_on_an_encrypted_local_lake(tmp_path: Path) -> None:
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    settings = load_defaults("laptop")
    lease = StubLease(run_id="r")
    cat = WorkingCatalog.open(storage, settings, lease, work_dir=tmp_path / "w", env=_ENV)
    writer = LocalNativeWriter(cat, storage)
    put = Put(PLAYERS, players(100, 5, 0), is_tracking=True, schema=None, source_hash=None)
    row = writer.write_table(
        put,
        match_key=100,
        visibility="public",
        content_version="v1",
        run_id="r",
        out_dir=tmp_path / "o",
        ingested_at=_T,
    )
    writer.swap(100, [row], set(), also=lambda: replace_match(cat.conn, _match_row(), [], []))
    cat.commit("run_end")

    run_cleanup(cat, storage, settings=settings, lease=lease, allow_large_sweep=False, now=datetime.now(UTC))
    cat.commit("run_end")
    cat.close()

    reader = CatalogReader(storage, env=_ENV)
    try:
        with reader.session() as conn:
            count = conn.execute(f'SELECT count(*) FROM lake.main."{PLAYERS}" WHERE match_key = 100').fetchone()  # noqa: S608 - constant
    finally:
        reader.close()
    assert count is not None and count[0] == 5, "cleanup must not strip a tracked native bronze file"
