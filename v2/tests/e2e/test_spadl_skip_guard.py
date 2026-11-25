"""The SPADL steps skip a match whose exact input key is unchanged (owner rule 2026-11-06; calc spec "Idempotent per
(match_key, ...) with a skip guard, like v1").

One run ingests the five providers' fixtures with `compute=DEFAULT_COMPUTE_STEPS`. Each test then runs the five SPADL
steps again on a working copy of that lake (never published, so each test starts from the same lake) with a recording
writer. The key covers the step's per-match reads (hashed inside DuckDB), the canonical pointers of the match's own
match, player and team keys, the SPADL code and the library versions. It must not cover the pointers of other matches.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from computekit import PARSERS, fixture_adapters
from lakekit import LakeKit, lake_roots
from runkit import NOW, active, save_settings

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.compute import base
from lakehouse.compute import input_key as ik
from lakehouse.compute import spadl_actions as sa
from lakehouse.compute.registry import DEFAULT_COMPUTE_STEPS
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

_MODULES = ("statsbomb_public", "idsse_public", "skillcorner_public", "wyscout_public", "gradientsports_public")
_STEPS = (
    sa.SPADL_ACTIONS_STATSBOMB,
    sa.SPADL_ACTIONS_WYSCOUT,
    sa.SPADL_ACTIONS_IDSSE,
    sa.SPADL_ACTIONS_SKILLCORNER,
    sa.SPADL_ACTIONS_GRADIENTSPORTS,
)


class _RecordingWriter:
    """Records each match a SPADL step writes; writes nothing."""

    def __init__(self) -> None:
        self.writes: list[int] = []

    def write_table(self, put: Any, *, match_key: int, **_: Any) -> tuple[str, int]:
        return ("written", match_key)

    def swap(self, match_key: int, parts: Any, tables: Any, *, also: Any) -> None:
        self.writes.append(match_key)
        also()


class _RecordingConn:
    """Delegates to a DuckDB connection and records every SQL text it executes."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        self.executed.append(sql)
        return self.inner.execute(sql, params) if params is not None else self.inner.execute(sql)


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> LakeKit:
    root: Path = tmp_path_factory.mktemp("spadl-skip")
    roots = lake_roots(root)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=root / "boot.json")
    kit = LakeKit(base=root, roots=roots, storage=LocalStorage(roots))
    save_settings(kit.storage, [active(m) for m in _MODULES])  # type: ignore[arg-type]
    adapters = fixture_adapters()
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={m: adapters[m] for m in _MODULES},  # type: ignore[index]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
        compute=DEFAULT_COMPUTE_STEPS,
    )
    assert result.status == "ok", [f"{e.kind}: {e.message}" for e in result.errors]
    return kit


@pytest.fixture
def working(lake: LakeKit) -> Iterator[WorkingCatalog]:
    cat = lake.catalog(run_id="run-skip")
    try:
        yield cat
    finally:
        cat.close()


@pytest.fixture
def writer(monkeypatch: pytest.MonkeyPatch) -> _RecordingWriter:
    recording = _RecordingWriter()

    def select(*_a: object, **_k: object) -> _RecordingWriter:
        return recording

    monkeypatch.setattr(base, "select_writer", select)
    return recording


def _run_spadl(cat: Any) -> list[tuple[str, int, int]]:
    """Run the five SPADL steps; return (step, matches visited, failed) per step. Errors fail the test."""
    errors: list[dict[str, Any]] = []
    outcomes: list[tuple[str, int, int]] = []

    def record(**kw: Any) -> None:
        errors.append(kw)

    for step in _STEPS:
        outcome = step(cat, run_id="run-skip", caps=cast(Any, None), now=lambda: NOW, error=record)
        outcomes.append((step.name, outcome.matches, outcome.failed_matches))
    assert errors == []
    return outcomes


def _written(cat: WorkingCatalog) -> set[int]:
    """The matches that hold `spadl_actions` rows. The statsbomb fixture holds only `Starting XI` events, so its match
    converts to no actions (`EMPTY`) and has no rows."""
    return {int(r[0]) for r in cat.conn.execute("SELECT DISTINCT match_key FROM lake.main.spadl_actions").fetchall()}


def _surrogate(cat: WorkingCatalog, table: str) -> int:
    row = cat.conn.execute(f"SELECT min(match_key) FROM lake.main.{table}").fetchone()  # noqa: S608 - constant names
    assert row is not None and row[0] is not None
    return int(row[0])


def test_every_spadl_step_visits_its_fixture_match(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    outcomes = _run_spadl(working)
    assert [(name, n) for name, n, _ in outcomes] == [(step.name, 1) for step in _STEPS]


def test_an_unchanged_rerun_writes_no_spadl_match(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    _run_spadl(working)
    assert writer.writes == []


def test_a_changed_event_rewrites_only_its_match(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    mk = _surrogate(working, "wyscout_events")
    working.conn.execute(
        "UPDATE lake.main.wyscout_events SET event_sec = event_sec + 1 "
        "WHERE id = (SELECT min(id) FROM lake.main.wyscout_events WHERE event_sec IS NOT NULL)"
    )
    _run_spadl(working)
    assert writer.writes == [mk]


def test_a_changed_match_row_rewrites_its_match(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    mk = _surrogate(working, "skillcorner_dynamic_events")
    working.conn.execute("UPDATE lake.main.skillcorner_matches SET pitch_length = pitch_length + 1")
    _run_spadl(working)
    assert writer.writes == [mk]


def test_a_pointer_of_the_match_player_rewrites_its_match(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    mk = _surrogate(working, "gradientsports_events")
    working.conn.execute(
        "UPDATE lake.main.ops_canonical_pointers SET canonical_key = canonical_key + 1 "
        "WHERE entity = 'player' AND record_key = (SELECT min(player_key) FROM lake.main.gradientsports_events)"
    )
    _run_spadl(working)
    assert writer.writes == [mk]


def test_a_pointer_of_another_match_rewrites_nothing(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    working.conn.execute(
        "INSERT INTO lake.main.ops_canonical_pointers BY NAME "
        "SELECT * REPLACE (-12345 AS record_key, 'not-in-any-fixture' AS native_id) "
        "FROM lake.main.ops_canonical_pointers WHERE entity = 'player' LIMIT 1"
    )
    _run_spadl(working)
    assert writer.writes == []


def test_a_code_change_rewrites_every_spadl_match(
    working: WorkingCatalog, writer: _RecordingWriter, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = ik.code_digest

    def edited(modules: Sequence[str]) -> str:
        return real(modules) + "-edited"

    monkeypatch.setattr(ik, "code_digest", edited)
    _run_spadl(working)
    assert sorted(writer.writes) == sorted(_written(working))
    assert len(writer.writes) == len(_STEPS) - 1  # every provider but the statsbomb `Starting XI` fixture


def test_spadl_steps_issue_only_declared_reads(working: WorkingCatalog, writer: _RecordingWriter) -> None:
    """Make every match stale, then record every SQL text: each is a declared read, its in-DuckDB hash form, or a
    declared control read."""
    working.conn.execute("UPDATE lake.main.spadl_actions SET _source_hash = 'stale'")
    recording = _RecordingConn(working.conn)
    fake: Any = type("_Cat", (), {"conn": recording, "storage": working.storage})()
    _run_spadl(fake)
    reads = [read for step in _STEPS for read in step.reads]
    declared = {r.sql for r in reads} | {ik.sums_sql(r) for r in reads} | set(ik.CONTROL_READS)
    undeclared = set(recording.executed) - declared
    assert recording.executed and not undeclared, f"undeclared reads: {undeclared}"
    assert sorted(writer.writes) == sorted(_written(working))
