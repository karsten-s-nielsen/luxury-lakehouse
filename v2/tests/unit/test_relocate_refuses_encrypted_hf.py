"""S1 Task 13: relocate refuses an encrypted-local lake (plan T13).

An encrypted-local lake cannot relocate. An hf:// destination is never allowed (HF cannot hold the lakehouse key;
`add_data_files` rejects an encrypted parquet), and a new local root needs the deferred key-aware path rewrite. The
refusal reads only `CURRENT.encrypted` (the pointer is plaintext), so it fires before any keyed open — this test
exercises it with a local destination, which the hf case shares (`before.encrypted` guards both).
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest
from lakekit import PLAYERS, StubLease, lake_roots, players

from lakehouse.bronze.writers import LocalNativeWriter, Put
from lakehouse.catalog.relocate import relocate
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_ENV = {ENCRYPTION_KEY_ENV: _KEY}


def test_relocate_refuses_an_encrypted_local_lake(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "lake").mkdir(parents=True, exist_ok=True)
    (source / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(source))
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(run_id="r"), work_dir=source / "w", env=_ENV)
    writer = LocalNativeWriter(cat, storage)
    put = Put(PLAYERS, players(100, 3, 0), is_tracking=True, schema=None, source_hash=None)
    row = writer.write_table(
        put,
        match_key=100,
        visibility="public",
        content_version="v1",
        run_id="r",
        out_dir=source / "o",
        ingested_at=_T,
    )
    writer.swap(100, [row], set(), also=lambda: None)
    cat.commit("run_end")  # CURRENT.encrypted is now True
    cat.close()

    new_base = tmp_path / "new-drive"
    new_base.mkdir()
    for name in ("lake", "lake-restricted"):
        shutil.move(source / name, new_base / name)
    moved = LocalStorage(lake_roots(new_base))

    with pytest.raises(GuardStopError, match="encrypted"):
        relocate(moved, old_roots=storage.roots, new_roots=moved.roots, env=_ENV)
