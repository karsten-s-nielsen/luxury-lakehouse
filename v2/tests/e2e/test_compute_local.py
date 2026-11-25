"""The calc layer end to end on a seeded local lake (spec D2/D3/D9; plan Task 11).

A gradientsports-only ingestion run carries `compute=REGISTERED_COMPUTE_STEPS`, so `finish` runs the full layered seam
(full dbt build -> layer-1 compute: spadl_actions + pitch_control + shape_graphs -> `+fct_actions` build -> layer-2
compute: line_breaking -> `+fct_tracking_metrics` build). The gradientsports fixture is the one provider with BOTH
flattened events and tracking frames, so a single-provider run exercises every compute and the mart.

This is the regression net for the catalog-glue ComputeStep bodies (the pure scorer cores are unit-tested per
`tests/unit/test_*_compute.py`; the match-iterating steps + the layered-seam wiring are only exercised here).
"""

from __future__ import annotations

import bz2
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import duckdb
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, ListingAdapter, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute.registry import REGISTERED_COMPUTE_STEPS
from lakehouse.config.model import ModuleId
from lakehouse.ids.native import Provider
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.runs.run import RunResult, run_ingestion

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PARSERS: dict[Provider, Any] = {
    "gradientsports": GradientSportsParser(),
    "idsse": IdsseParser(),
    "skillcorner": SkillCornerParser(),
}


def _read(folder: str, name: str) -> bytes:
    return (FIXTURES / folder / name).read_bytes()


def _adapter(
    module: ModuleId,
    provider: Provider,
    native: str,
    files: Mapping[str, bytes],
    *,
    competition: tuple[str, str],
    listings: Mapping[str, bytes],
) -> FakeAdapter:
    listed = entry(module, native, signals=dict.fromkeys(files, "s1"), competition=competition, has_360=None)  # type: ignore[arg-type]
    raw = {(native, artifact): data for artifact, data in files.items()}
    return ListingAdapter(module, provider, "public", [listed], raw, listings=listings)


def _gradientsports_adapter() -> FakeAdapter:
    files = {
        "metadata": _read("gradientsports", "metadata_9001.json"),
        "roster": _read("gradientsports", "roster_9001.json"),
        "events": _read("gradientsports", "events_9001.json"),
        "tracking": bz2.compress(_read("gradientsports", "tracking_9001.jsonl")),  # as the Drive folder delivers it
    }
    listings = {
        "competitions": _read("gradientsports", "competitions.csv"),
        "players": _read("gradientsports", "players.csv"),
    }
    return _adapter(
        "gradientsports_public", "gradientsports", "9001", files, competition=("77", "2030"), listings=listings
    )


def _idsse_adapter() -> FakeAdapter:
    files = {
        "metadata": _read("idsse", "matchinformation_J03WPY.xml"),
        "events": _read("idsse", "events_J03WPY_slice.xml"),
        "positions": _read("idsse", "positions_J03WPY_slice.xml"),
    }
    return _adapter(
        "idsse_public", "idsse", "J03WPY", files, competition=("DFL-COM-000002", "DFL-SEA-0001K6"), listings={}
    )


def _skillcorner_adapter() -> FakeAdapter:
    files = {
        "match": _read("skillcorner", "match_1899585.json"),
        "tracking": _read("skillcorner", "tracking_1899585_slice.jsonl"),
        "dynamic_events": _read("skillcorner", "dynamic_events_1899585_slice.csv"),
        "phases_of_play": _read("skillcorner", "phases_of_play_1899585_slice.csv"),
    }
    listings = {"matches": _read("skillcorner", "matches.json")}
    return _adapter("skillcorner_public", "skillcorner", "1899585", files, competition=("61", "95"), listings=listings)


def _run(kit: LakeKit, adapters: Mapping[ModuleId, FakeAdapter]) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters=adapters,  # type: ignore[arg-type]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
        compute=REGISTERED_COMPUTE_STEPS,  # the excluded pitch_control + shape_graphs code stays tested (spec D10)
    )


def _count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    # A compute that writes no rows never creates its bronze table (the staging view's `lake_table_present` guard then
    # returns empty-but-typed), so a count on an absent table is 0, not an error. (The gold `fct_*` are VIEWS, which
    # duckdb_tables() does not list, so catch the catalog error rather than pre-checking a table list.)
    try:
        found = conn.execute(f"SELECT count(*) FROM lake.main.{table}").fetchone()  # noqa: S608 - constant table names
    except duckdb.CatalogException:
        return 0
    assert found is not None
    return int(found[0])


def test_compute_populates_the_tracking_metrics_mart(kit: LakeKit) -> None:
    gs = _gradientsports_adapter()
    save_settings(kit.storage, [active("gradientsports_public")])
    result = _run(kit, {gs.module_id: gs})
    assert result.status == "ok", "\n".join(f"{e.kind}: {e.message}" for e in result.errors)
    tables = (
        "gradientsports_events",
        "fct_tracking_frames",
        "spadl_actions",
        "fct_actions",
        "pitch_control_values",
        "shape_graph_labels",
        "line_breaking_results",
        "fct_tracking_metrics",
    )
    with CatalogReader(kit.storage).session() as conn:
        counts = {t: _count(conn, t) for t in tables}
        controlled = conn.execute(
            "SELECT count(*) FROM lake.main.fct_tracking_metrics WHERE mean_controlled_fraction IS NOT NULL"
        ).fetchone()
    # The GS fixture exercises the whole forward DAG: events -> SPADL actions -> fct_actions, and tracking frames ->
    # pitch_control -> the mart. (shape_graph_labels and line_breaking_results may be 0: the fixture's tracking slice is
    # sparse/collinear, so the Delaunay shape graph and a located line-break need not occur; their scorer cores + the
    # mart aggregations are unit-tested in tests/unit.)
    for core in ("spadl_actions", "fct_actions", "pitch_control_values", "fct_tracking_metrics"):
        assert counts[core] > 0, counts
    assert controlled is not None and controlled[0] > 0, counts  # pitch_control reached the mart for some player


def test_idsse_compute_converts_dfl_events_to_spadl(kit: LakeKit) -> None:
    # idsse (DFL) is the plan's higher-risk adapter: this exercises the sportec converter + the per-type actor coalesce
    # + the frame-derived period/time on a real DFL fixture. A clean run (status ok) with spadl_actions + fct_actions
    # rows proves the idsse events converted, validated and canonicalized end to end (spec D9 / Task 4b). The GS SPADL
    # step is a no-op here (no gradientsports_events), proving the per-provider presence guard.
    idsse = _idsse_adapter()
    save_settings(kit.storage, [active("idsse_public")])
    result = _run(kit, {idsse.module_id: idsse})
    assert result.status == "ok", "\n".join(f"{e.kind}: {e.message}" for e in result.errors)
    with CatalogReader(kit.storage).session() as conn:
        actions = _count(conn, "spadl_actions")
        fct_actions = _count(conn, "fct_actions")
    assert actions > 0, "idsse produced no SPADL actions"
    assert fct_actions > 0


def test_skillcorner_compute_converts_dynamic_events_to_spadl(kit: LakeKit) -> None:
    # skillcorner is the inference-based adapter: this exercises spadl.skillcorner over the real dynamic_events fixture
    # (coords mm->m reshaping + the match-metadata dict + skillcorner-native canonicalization). A clean run with
    # spadl_actions + fct_actions rows proves the conversion end to end (spec D9 / Task 4b).
    sc = _skillcorner_adapter()
    save_settings(kit.storage, [active("skillcorner_public")])
    result = _run(kit, {sc.module_id: sc})
    assert result.status == "ok", "\n".join(f"{e.kind}: {e.message}" for e in result.errors)
    with CatalogReader(kit.storage).session() as conn:
        actions = _count(conn, "spadl_actions")
        fct_actions = _count(conn, "fct_actions")
    assert actions > 0, "skillcorner produced no SPADL actions"
    assert fct_actions > 0


# --- perf plan Task 5: the one-build driver's oracle and its declared-read guard (perf spec §4, §7.3, §7.4) ---------

_AUDIT = {"_ingested_at", "_run_id", "_source_hash", "match_key"}


class _RecordingConn:
    """Delegates to a DuckDB connection and records every SQL text it executes."""

    def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
        self.inner = inner
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        self.executed.append(sql)
        return self.inner.execute(sql, params) if params is not None else self.inner.execute(sql)


class _NoWriter:
    def write_table(self, put: Any, *, match_key: int, **_: Any) -> tuple[str, int]:
        return ("written", match_key)

    def swap(self, match_key: int, parts: Any, tables: Any, *, also: Any) -> None:
        also()


def _comparable(table: Any, keys: list[str]) -> Any:
    frame: Any = table.to_pandas() if hasattr(table, "to_pandas") else table
    frame = frame[[c for c in frame.columns if c not in _AUDIT]]
    return frame.sort_values(keys).reset_index(drop=True)


def test_group_output_equals_the_pure_scorers(kit: LakeKit) -> None:
    """The S oracle (perf spec §7.3) on the seeded lake: the one-build driver's bronze rows equal the pure scorers
    called directly on `build_match_frame` for every seeded match (§4 comparator). Task 9 runs it on real matches."""
    from lakehouse.compute.frames import build_match_frame
    from lakehouse.compute.input_key import HOME_TEAM_READ, match_record_keys
    from lakehouse.compute.pitch_control import home_and_away, pitch_control_values
    from lakehouse.compute.shape_graphs import shape_graph_values

    gs = _gradientsports_adapter()
    save_settings(kit.storage, [active("gradientsports_public")])
    assert _run(kit, {gs.module_id: gs}).status == "ok"
    with CatalogReader(kit.storage).session() as conn:
        cat: Any = type("_RO", (), {"conn": conn})()
        matches = conn.execute(
            "SELECT DISTINCT canonical_match_key, provider FROM lake.main.fct_tracking_frames"
        ).fetchall()
        assert matches
        for mk, provider in matches:
            record_keys = match_record_keys(conn, int(mk))
            frame = build_match_frame(cat, int(mk), provider=str(provider), record_keys=record_keys)
            home_row = conn.execute(HOME_TEAM_READ.sql, HOME_TEAM_READ.params(int(mk), str(provider))).fetchone()
            assert home_row is not None
            home, away = home_and_away(frame, int(home_row[0]))
            pc_keys = ["canonical_match_key", "period", "frame", "canonical_team_key"]
            expected_pc = _comparable(pitch_control_values(frame, home_team_id=home, away_team_id=away), pc_keys)
            actual_pc = _comparable(
                conn.execute("SELECT * FROM lake.main.pitch_control_values WHERE canonical_match_key = ?", [mk]).df(),
                pc_keys,
            )
            assert expected_pc.astype(str).equals(actual_pc[expected_pc.columns].astype(str))
            sg_keys = ["canonical_match_key", "period", "frame", "canonical_player_key"]
            expected_sg = _comparable(shape_graph_values(frame), sg_keys)
            if _count(conn, "shape_graph_labels") or len(expected_sg):
                actual_sg = _comparable(
                    conn.execute("SELECT * FROM lake.main.shape_graph_labels WHERE canonical_match_key = ?", [mk]).df(),
                    sg_keys,
                )
                assert expected_sg.astype(str).equals(actual_sg[expected_sg.columns].astype(str))


def test_steps_issue_only_declared_reads(kit: LakeKit) -> None:
    """B HFTC-PLAN-03: run BOTH real tracking steps (made stale, so they build frames) through a recording connection;
    every SQL text is exactly one declared input read (or its in-DuckDB hash form) or one declared control read."""
    import dataclasses

    from lakehouse.compute import input_key as ik
    from lakehouse.compute import tracking_steps as ts
    from lakehouse.compute.frames import FRAME_INPUT_READS

    gs = _gradientsports_adapter()
    save_settings(kit.storage, [active("gradientsports_public")])
    assert _run(kit, {gs.module_id: gs}).status == "ok"
    cat = kit.catalog(run_id="run-reads")
    try:
        for table in ("pitch_control_values", "shape_graph_labels", "line_breaking_results"):
            if _count(cat.conn, table):
                cat.conn.execute(f"UPDATE lake.main.{table} SET _source_hash = 'stale'")  # noqa: S608 - constant names
        recording = _RecordingConn(cat.conn)
        fake: Any = type("_Cat", (), {"conn": recording, "storage": cat.storage})()
        writer: Any = _NoWriter()
        errors: list[dict[str, Any]] = []

        def record(**kw: Any) -> None:
            errors.append(kw)

        for step in (
            dataclasses.replace(ts.TRACKING_FRAME_METRICS, writer=writer),
            dataclasses.replace(ts.LINE_BREAKING, writer=writer),
        ):
            step(fake, run_id="run-reads", caps=cast(Any, None), now=lambda: NOW, error=record)
        reads = (*FRAME_INPUT_READS, ik.HOME_TEAM_READ, ik.ACTIONS_INPUT_READ)
        declared = {r.sql for r in reads} | {ik.sums_sql(r) for r in reads} | set(ik.CONTROL_READS)
        undeclared = set(recording.executed) - declared
        assert recording.executed and not undeclared, f"undeclared reads: {undeclared}"
        assert any(sql == FRAME_INPUT_READS[0].sql for sql in recording.executed), "no frame was built"
        assert errors == []
    finally:
        cat.close()
