"""The `MatchFiles` collaborator (M1 extraction, plan Task 2): `ops_matches`, `files_of`, and `move` — a match's
file registry follows its files to the new root on a visibility change."""

from __future__ import annotations

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, checkpoints_for, entry, save_settings

from lakehouse.ids.keys import match_key
from lakehouse.raw.archive import RawArchive
from lakehouse.runs.history import read_table
from lakehouse.runs.match_files import MatchFiles
from lakehouse.runs.run import run_ingestion

KEY = match_key("idsse", "J03WPY")


def _ingest_public(kit: LakeKit) -> None:
    adapter = FakeAdapter(
        "idsse_public", "idsse", "public", [entry("idsse_public", "J03WPY")], {("J03WPY", "events"): b"a\nb\n"}
    )
    save_settings(kit.storage, [active("idsse_public")])
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"idsse_public": adapter},  # type: ignore[dict-item]
        parsers={"idsse": FakeParser("idsse")},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )
    assert result.status == "ok", result.errors


def test_move_follows_new_file_ids(kit: LakeKit) -> None:
    _ingest_public(kit)
    cat = kit.catalog()
    files = MatchFiles(
        cat,
        kit.storage,
        RawArchive(kit.storage, enabled=False),
        "run-move",
        lambda: NOW,
        checkpoints=checkpoints_for(cat, kit.storage),
    )
    row = {m["match_key"]: m for m in read_table(cat.conn, "ops_matches")}[KEY]
    assert {f.root for f in files.files_of(KEY)} == {"lake"}

    files.move(KEY, "private", row)

    moved = files.files_of(KEY)
    assert {f.root for f in moved} == {"restricted"}  # the registry followed the copies to the restricted root
    seen = cat.conn.execute(
        "SELECT DISTINCT visibility FROM lake.main.ops_matches WHERE match_key = ?", [KEY]
    ).fetchall()
    assert [row[0] for row in seen] == ["private"]
