"""S1 Task 9: the append-only visibility-change ledger on an encrypted-local lake (plan T9, consumed by T12).

A flip records one `ops_visibility_changes` row (`match_key`, `new_visibility`, `at`, `run_id`; no file keys). The
ledger is append-only, so a second flip appends a second row. T12 restore reads the latest `new_visibility` per
match from this ledger in the pre-restore `CURRENT` and re-applies it after a data rollback.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from lakekit import PLAYERS, StubLease, lake_roots, players
from runkit import checkpoints_for

from lakehouse.bronze.writers import LocalNativeWriter, Put
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.lineage.model import LineageRow, write_lineage
from lakehouse.raw.archive import RawArchive
from lakehouse.runs.history import TABLES, MatchRow, read_table, replace_match
from lakehouse.runs.match_files import MatchFiles
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
Vis = Literal["public", "private"]


def _keyed_catalog(base: Path) -> tuple[LocalStorage, WorkingCatalog]:
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    writer = LocalNativeWriter(cat, storage)
    put = Put(PLAYERS, players(100, 5, 0), is_tracking=True, schema=None, source_hash=None)
    row = writer.write_table(
        put,
        match_key=100,
        visibility="public",
        content_version="v1",
        run_id="run-1",
        out_dir=base / "out",
        ingested_at=_T,
    )
    writer.swap(100, [row], set(), also=lambda: None)
    replace_match(
        cat.conn,
        MatchRow(
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
            run_id="run-1",
            updated_at=_T,
        ),
        [],
        [],
    )
    write_lineage(
        cat.conn,
        [
            LineageRow(
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
                visibility="public",
                kept=True,
                reason="kept",
            )
        ],
    )
    cat.commit("run_end")
    return storage, cat


def test_a_flip_appends_one_ledger_record_with_no_file_keys(tmp_path: Path) -> None:
    storage, cat = _keyed_catalog(tmp_path)
    try:
        mover = MatchFiles(
            cat,
            storage,
            RawArchive(storage, enabled=False),
            "run-1",
            lambda: _T,
            checkpoints=checkpoints_for(cat, storage),
        )
        mover.move(100, "private", mover.ops_matches()[100])

        ledger = read_table(cat.conn, "ops_visibility_changes")
        assert ledger == [{"match_key": 100, "new_visibility": "private", "changed_at": _T, "run_id": "run-1"}]
        # the ledger carries no file keys: its columns are exactly match_key, new_visibility, changed_at, run_id
        assert [name for name, _ in TABLES["ops_visibility_changes"]] == [
            "match_key",
            "new_visibility",
            "changed_at",
            "run_id",
        ]
    finally:
        cat.close()


def test_the_ledger_is_append_only_across_flips(tmp_path: Path) -> None:
    storage, cat = _keyed_catalog(tmp_path)
    try:
        mover = MatchFiles(
            cat,
            storage,
            RawArchive(storage, enabled=False),
            "run-1",
            lambda: _T,
            checkpoints=checkpoints_for(cat, storage),
        )
        mover.move(100, "private", mover.ops_matches()[100])
        mover.move(100, "public", mover.ops_matches()[100])

        # append-only: two flips leave two rows (an update-in-place would leave one)
        ledger = read_table(cat.conn, "ops_visibility_changes")
        assert len(ledger) == 2
        assert sorted(r["new_visibility"] for r in ledger) == ["private", "public"]
    finally:
        cat.close()
