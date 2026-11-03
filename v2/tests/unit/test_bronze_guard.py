"""S1 Task 10: the bronze integrity guard, wired at the production write/commit seam (plan T10).

`assert_bronze_integrity` dispatches by mode: an unencrypted-local or HF lake must hold no DuckLake-native bronze
file (its rows could mix visibilities under one file); an encrypted-local lake must keep the `_visibility`
invariant (present, non-null, and match-scoped rows equal to `ops_matches`). Since T3 the checkpoint policy calls it
before every publish that can carry a bronze write (spec D1), so a break stops the run before it is published. These
tests pin the dispatch, each failure mode, the messages, and the wiring.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from lakekit import PLAYERS, LakeKit, StubLease, lake_roots, players
from runkit import NOW, FakeAdapter, FakeParser, active, checkpoints_for, entry

import lakehouse.runs.checkpoint as checkpoint_module
from lakehouse.bronze.writers import LocalNativeWriter, Put, assert_bronze_integrity
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError
from lakehouse.ids.keys import match_key
from lakehouse.parsers.base import ParsedMatch
from lakehouse.raw.archive import Delivery, RawArchive
from lakehouse.runs._common import ModuleContext
from lakehouse.runs.history import MatchRow, replace_match
from lakehouse.runs.match_files import MatchFiles
from lakehouse.runs.match_writer import MatchWriter
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
KEY = match_key("idsse", "J03WPY")


def _noop_errors(
    *,
    kind: str,
    message: str,
    severity: str,
    module_run_id: str | None = None,
    module_id: str | None = None,
    native: str | None = None,
) -> None:
    pass


# --- the production-path wiring (keyless harness) ------------------------------------------------------------------


def _pieces() -> tuple[ModuleContext, Any, dict[str, Delivery], ParsedMatch]:
    files = {("J03WPY", "events"): b"events-x\nevents-y\n"}
    adapter = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY", signals={"events": "s1"}, visibility="public")],
        files,
    )
    parser = FakeParser("idsse")
    listed = adapter.entries[0]
    deliveries = {"events": adapter.fetch(listed, "events")}
    parsed = parser.parse(listed, deliveries, run_id="r")
    context = ModuleContext(
        module=active("idsse_public"),
        adapter=adapter,  # type: ignore[arg-type]
        parser=parser,  # type: ignore[arg-type]
        module_run_id="r.idsse_public",
    )
    return context, listed, deliveries, parsed


def _writer(kit: LakeKit, cat: WorkingCatalog) -> MatchWriter:
    """A match writer whose publishes go through a real checkpoint policy that publishes on every unit (M = 1)."""
    checkpoints = checkpoints_for(cat, kit.storage, matches=1)
    files = MatchFiles(
        cat, kit.storage, RawArchive(kit.storage, enabled=False), "r", lambda: NOW, checkpoints=checkpoints
    )
    return MatchWriter(cat, kit.storage, kit.base / "w", "r", lambda: NOW, files, _noop_errors, checkpoints=checkpoints)


def test_match_write_publish_invokes_the_guard(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard runs on the writer's catalog before the publish that carries the match write (T3 spec D1)."""
    cat = kit.catalog()
    writer = _writer(kit, cat)
    seen: list[int] = []

    def _spy(catalog: WorkingCatalog) -> None:
        seen.append(id(catalog))

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", _spy)
    context, listed, deliveries, parsed = _pieces()
    writer.write(KEY, context, listed, deliveries, parsed, writer.choose(KEY, "public", listed, parsed, []), None)
    assert seen == [id(cat)], "the publish of a match write must run assert_bronze_integrity on its catalog"
    cat.close()


def test_guard_runs_before_commit_so_a_failure_never_publishes(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    """S1-IMPL-19: the guard runs BEFORE `cat.commit` in the write seam, so a broken write never becomes a
    published version. A failing guard must stop the write before the commit is reached."""
    cat = kit.catalog()
    writer = _writer(kit, cat)

    def _raise(_catalog: WorkingCatalog) -> None:
        raise GuardStopError("injected: broken bronze")

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", _raise)
    commits: list[str] = []

    def _spy_commit(kind: str) -> SimpleNamespace:
        commits.append(kind)
        return SimpleNamespace(seq=99)

    monkeypatch.setattr(cat, "commit", _spy_commit)

    context, listed, deliveries, parsed = _pieces()
    with pytest.raises(GuardStopError):
        writer.write(KEY, context, listed, deliveries, parsed, writer.choose(KEY, "public", listed, parsed, []), None)
    assert commits == [], "a failing guard stops the write before commit; no version is published"
    cat.close()


def test_guard_unencrypted_rejects_a_native_bronze_file(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, 1, visibility="public", frames=3, offset=0)  # the add_data_files path: guard passes
    try:
        assert_bronze_integrity(cat)  # no native file yet
        cat.conn.execute(
            f'INSERT INTO lake.main."{PLAYERS}" '  # noqa: S608 - constant test table name
            "SELECT 9, 1, range, 11, range, range, true FROM range(5000)"
        )
        with pytest.raises(GuardStopError):
            assert_bronze_integrity(cat)
    finally:
        cat.close()


# --- the encrypted-local failure modes -----------------------------------------------------------------------------


def _keyed_with_match(tmp_path: Path) -> WorkingCatalog:
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={"LAKEHOUSE_ENCRYPTION_KEY": _KEY}
    )
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
            run_id="r",
            updated_at=_T,
        ),
        [],
        [],
    )
    cat.commit("run_end")
    return cat


def test_guard_encrypted_before_the_first_match_write(tmp_path: Path) -> None:
    """A lake whose bronze holds rows but no match was written yet (a first run that lists sources and selects no
    match) has no `ops_matches`: no row can disagree with it, so the guard passes; a null `_visibility` still stops.
    Found by the T3 real-data diagnostic (2026-11-01): the run-end publish raised `CatalogException: Table with name
    ops_matches does not exist!`."""
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(tmp_path))
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={"LAKEHOUSE_ENCRYPTION_KEY": _KEY}
    )
    try:
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
        writer.swap(100, [row], set(), also=lambda: None)
        listed = cat.conn.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
        tables = {str(name) for (name,) in listed}
        assert "ops_matches" not in tables, "the scenario needs a lake without ops_matches"
        assert_bronze_integrity(cat)
        cat.conn.execute(f'UPDATE lake.main."{PLAYERS}" SET _visibility = NULL WHERE frame = 0')  # noqa: S608 - constant
        with pytest.raises(GuardStopError, match="null"):
            assert_bronze_integrity(cat)
    finally:
        cat.close()


def test_guard_encrypted_rejects_a_null_visibility_row(tmp_path: Path) -> None:
    cat = _keyed_with_match(tmp_path)
    try:
        assert_bronze_integrity(cat)  # the written lake is sound
        cat.conn.execute(f'UPDATE lake.main."{PLAYERS}" SET _visibility = NULL WHERE frame = 0')  # noqa: S608 - constant
        with pytest.raises(GuardStopError, match="null"):
            assert_bronze_integrity(cat)
    finally:
        cat.close()


def test_guard_encrypted_rejects_a_visibility_mismatch(tmp_path: Path) -> None:
    cat = _keyed_with_match(tmp_path)
    try:
        assert_bronze_integrity(cat)
        # flip ops_matches alone, leaving the bronze _visibility at public: the row now disagrees
        cat.conn.execute("UPDATE lake.main.ops_matches SET visibility = 'private' WHERE match_key = 100")
        with pytest.raises(GuardStopError, match="disagrees") as raised:
            assert_bronze_integrity(cat)
        assert PLAYERS in str(raised.value)
        assert "match_key 100" in str(raised.value)  # the operator sees which match broke the guard (T3 spec D1)
    finally:
        cat.close()
