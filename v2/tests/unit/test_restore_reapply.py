"""S1 Task 12: restore re-applies the latest visibility from the ledger on an encrypted-local lake (plan T12).

On an encrypted-local lake a visibility flip is a `_visibility` + `ops_matches` update plus a ledger record, not a
file move. So `restore` to a version from BEFORE a public->private flip must read the latest visibility per match
from the ledger in the pre-restore `CURRENT` and re-apply it, else the restored point would read public and leak
the match. The restore carries the full ledger forward, as it carries move records.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from lakekit import PLAYERS, StubLease, lake_roots, players
from runkit import checkpoints_for

from lakehouse.bronze.writers import LocalNativeWriter, Put
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.restore import restore
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
_ENV = {ENCRYPTION_KEY_ENV: _KEY}


def _match_row(visibility: Literal["public", "private"]) -> MatchRow:
    return MatchRow(
        match_key=100,
        provider="skillcorner",
        native_match_id="100",
        visibility=visibility,
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


def _lineage(visibility: Literal["public", "private"]) -> LineageRow:
    return LineageRow(
        match_key=100,
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


def test_restore_before_a_flip_re_applies_the_private_visibility(tmp_path: Path) -> None:
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    settings = load_defaults("laptop")

    # v1: the match is written public.
    cat = WorkingCatalog.open(storage, settings, StubLease(run_id="r"), work_dir=tmp_path / "w1", env=_ENV)
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
    writer.swap(100, [row], set(), also=lambda: replace_match(cat.conn, _match_row("public"), [], []))
    write_lineage(cat.conn, [_lineage("public")])
    v1 = cat.commit("run_end").seq

    # v2 (CURRENT): the match is flipped to private — a column + ledger update, no file move.
    mover = MatchFiles(
        cat, storage, RawArchive(storage, enabled=False), "r", lambda: _T, checkpoints=checkpoints_for(cat, storage)
    )
    mover.move(100, "private", mover.ops_matches()[100])
    cat.close()

    # restore to v1: the re-apply must bring the match back to private, not the v1 public.
    restore(storage, target_seq=v1, operator="karsten", settings=settings, env=_ENV, work_dir=tmp_path / "wr")

    reader = CatalogReader(storage, env=_ENV)
    try:
        with reader.session() as conn:
            column = {
                str(v)
                for (v,) in conn.execute(
                    f'SELECT DISTINCT _visibility FROM lake.main."{PLAYERS}" WHERE match_key = 100'  # noqa: S608 - constant
                ).fetchall()
            }
            ops = {r["match_key"]: r["visibility"] for r in read_table(conn, "ops_matches")}[100]
    finally:
        reader.close()
    assert column == {"private"}, "restore re-applied the ledger's latest (private) to the bronze rows"
    assert ops == "private", "restore re-applied the ledger's latest (private) to ops_matches"
