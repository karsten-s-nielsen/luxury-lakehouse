"""The tracking step drivers: one frame build per match, exact skip keys, independent scorers (perf spec §7.3/§7.4;
plan Task 5).

The drivers run on an in-memory DuckDB with `lake` attached: plain tables stand in for the gold views, and a fake
bronze writer stores `(canonical_match_key, _source_hash)` per output table so the skip lookup sees prior writes. The
frame builder and the scorers are injected fakes that count their calls.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from lakehouse.compute import input_key as ik
from lakehouse.compute import tracking_steps as ts
from lakehouse.compute.pitch_control import PITCH_CONTROL_SCHEMA, home_and_away
from lakehouse.compute.shape_graphs import SHAPE_GRAPH_SCHEMA
from lakehouse.runs._common import FATAL

NOW = datetime(2026, 11, 6, tzinfo=UTC)
MK = 7
HOME, AWAY = 100, 200


class _Writer:
    """Stores what a real swap would leave behind: the match's rows' canonical key + `_source_hash`."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn
        self.writes: list[tuple[str, int]] = []
        self._pending: dict[int, tuple[str, str]] = {}

    def write_table(self, put: Any, *, match_key: int, **_: Any) -> tuple[str, int]:
        self._pending[match_key] = (str(put.table), put.source_hash)
        return ("written", match_key)

    def swap(self, match_key: int, parts: Any, tables: Any, *, also: Any) -> None:
        table, source_hash = self._pending.pop(match_key)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS lake.main.{table} (canonical_match_key BIGINT, _source_hash VARCHAR)"
        )
        self.conn.execute(f"DELETE FROM lake.main.{table} WHERE canonical_match_key = ?", [match_key])  # noqa: S608 - test constants, no outside input
        self.conn.execute(f"INSERT INTO lake.main.{table} VALUES (?, ?)", [match_key, source_hash])  # noqa: S608 - test constants, no outside input
        self.writes.append((table, match_key))
        also()


class _Counter:
    def __init__(self, result: Any = None, exc: BaseException | None = None) -> None:
        self.calls = 0
        self.kwargs: list[dict[str, Any]] = []
        self.result = result
        self.exc = exc

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        self.kwargs.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.result


def _pitch_rows() -> pa.Table:
    return pa.table(
        {
            "canonical_match_key": pa.array([MK, MK], pa.int64()),
            "period": pa.array([1, 1], pa.int32()),
            "frame": pa.array([0, 0], pa.int32()),
            "canonical_team_key": pa.array([HOME, AWAY], pa.int64()),
            "control_fraction": pa.array([0.6, 0.4], pa.float64()),
        },
        schema=PITCH_CONTROL_SCHEMA,
    )


def _shape_rows() -> pa.Table:
    return pa.table(
        {
            "canonical_match_key": pa.array([MK], pa.int64()),
            "period": pa.array([1], pa.int32()),
            "frame": pa.array([0], pa.int32()),
            "canonical_player_key": pa.array([2], pa.int64()),
            "position_label": pa.array(["RCB"], pa.string()),
        },
        schema=SHAPE_GRAPH_SCHEMA,
    )


def _frame() -> Any:
    import pandas as pd  # pyright: ignore[reportMissingTypeStubs]

    return pd.DataFrame(
        {"game_id": [MK, MK, MK], "team_id": [HOME, AWAY, None], "is_ball": [False, False, True], "frame_id": [0, 0, 0]}
    )


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    c.execute(
        "CREATE TABLE lake.main.fct_tracking_frames "
        "(canonical_match_key BIGINT, match_key BIGINT, provider VARCHAR, x DOUBLE)"
    )
    c.execute("INSERT INTO lake.main.fct_tracking_frames VALUES (7, 7, 'idsse', 1.0), (7, 7, 'idsse', 2.0)")
    c.execute(
        "CREATE TABLE lake.main.fct_tracking_ball (canonical_match_key BIGINT, match_key BIGINT, time_seconds DOUBLE)"
    )
    c.execute("INSERT INTO lake.main.fct_tracking_ball VALUES (7, 7, 0.0), (7, 7, 0.04)")
    c.execute(
        "CREATE TABLE lake.main.fct_match_params (canonical_match_key BIGINT, provider VARCHAR, home_team_key BIGINT)"
    )
    c.execute("INSERT INTO lake.main.fct_match_params VALUES (7, 'idsse', 100)")
    c.execute(
        "CREATE TABLE lake.main.fct_roster_gk "
        "(canonical_match_key BIGINT, canonical_team_key BIGINT, canonical_player_key BIGINT)"
    )
    c.execute("INSERT INTO lake.main.fct_roster_gk VALUES (7, 100, 1), (7, 200, 11)")
    c.execute("CREATE TABLE lake.main.fct_actions (canonical_match_key BIGINT, action_id INTEGER, time_seconds DOUBLE)")
    c.execute("INSERT INTO lake.main.fct_actions VALUES (7, 1, 0.5)")
    c.execute("CREATE TABLE lake.main.ops_matches (match_key BIGINT, visibility VARCHAR)")
    c.execute("INSERT INTO lake.main.ops_matches VALUES (7, 'public')")
    c.execute("CREATE TABLE lake.main.ops_canonical_pointers (entity VARCHAR, record_key BIGINT, canonical_key BIGINT)")
    yield c
    c.close()


def _cat(c: duckdb.DuckDBPyConnection) -> Any:
    return type("_Cat", (), {"conn": c, "storage": None})()


def _group(
    writer: _Writer, *, builder: _Counter | None = None, pitch: _Counter | None = None, shape: _Counter | None = None
) -> ts.TrackingFrameMetrics:
    return ts.TrackingFrameMetrics(
        frame_builder=builder or _Counter(_frame()),
        pitch_scorer=pitch or _Counter(_pitch_rows()),
        shape_scorer=shape or _Counter(_shape_rows()),
        writer=cast("Any", writer),
    )


def _run(group: Any, c: duckdb.DuckDBPyConnection, errors: list[dict[str, Any]]) -> Any:
    def record(**kw: Any) -> None:
        errors.append(kw)

    return group(_cat(c), run_id="run-ts", caps=cast("Any", None), now=lambda: NOW, error=record)


def test_group_reports_one_outcome_per_scorer_name(conn: duckdb.DuckDBPyConnection) -> None:
    errors: list[dict[str, Any]] = []
    outcomes = _run(_group(_Writer(conn)), conn, errors)
    assert [o.step for o in outcomes] == ["pitch_control", "shape_graphs"]
    assert [o.rows for o in outcomes] == [2, 1]
    assert errors == []


def test_unchanged_match_builds_no_frame(conn: duckdb.DuckDBPyConnection) -> None:
    writer, builder = _Writer(conn), _Counter(_frame())
    _run(_group(writer, builder=builder), conn, [])
    assert builder.calls == 1
    _run(_group(writer, builder=builder), conn, [])
    assert builder.calls == 1  # the second run skipped both scorers before any frame build


def test_only_the_stale_scorer_runs(conn: duckdb.DuckDBPyConnection) -> None:
    writer = _Writer(conn)
    _run(_group(writer), conn, [])
    conn.execute("UPDATE lake.main.shape_graph_labels SET _source_hash = 'stale'")
    builder, pitch, shape = _Counter(_frame()), _Counter(_pitch_rows()), _Counter(_shape_rows())
    _run(_group(writer, builder=builder, pitch=pitch, shape=shape), conn, [])
    assert (builder.calls, pitch.calls, shape.calls) == (1, 0, 1)


def test_one_scorer_failing_leaves_the_other_written(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE lake.main.shape_graph_labels (canonical_match_key BIGINT, _source_hash VARCHAR)")
    conn.execute("INSERT INTO lake.main.shape_graph_labels VALUES (7, 'previous')")
    writer, errors = _Writer(conn), list[dict[str, Any]]()
    outcomes = _run(_group(writer, shape=_Counter(exc=ValueError("boom"))), conn, errors)
    assert writer.writes == [("pitch_control_values", MK)]
    assert len(errors) == 1 and errors[0]["kind"] == "compute_failed"
    assert errors[0]["message"].startswith(f"shape_graphs: match_key={MK} failed: ValueError: boom")
    assert conn.execute("SELECT _source_hash FROM lake.main.shape_graph_labels").fetchall() == [("previous",)]
    assert [o.failed_matches for o in outcomes] == [0, 1]


def test_fatal_from_one_scorer_propagates_out_of_the_group(conn: duckdb.DuckDBPyConnection) -> None:
    fatal_type = FATAL[0]  # LeaseLostError: a control error, never recorded as a per-match failure
    with pytest.raises(fatal_type):
        _run(_group(_Writer(conn), pitch=_Counter(exc=fatal_type("lost"))), conn, [])


def test_home_away_logic_change_recomputes(conn: duckdb.DuckDBPyConnection) -> None:
    """The home/away rule lives in the digested pitch_control module, and the home team value is an input read.
    (The frame builder reads the whole `fct_match_params` row, so a home-team change re-keys shape_graphs too.)"""
    assert home_and_away.__module__ == "lakehouse.compute.pitch_control"
    assert "lakehouse.compute.pitch_control" in ts.PITCH_CODE_MODULES
    before = ts.match_keys(conn, MK, "idsse", ik.match_record_keys(conn, MK))
    conn.execute("UPDATE lake.main.fct_match_params SET home_team_key = 200")
    after = ts.match_keys(conn, MK, "idsse", ik.match_record_keys(conn, MK))
    assert after["pitch_control"] != before["pitch_control"]
    assert after["shape_graphs"] != before["shape_graphs"]


def test_home_and_away_picks_the_other_team() -> None:
    assert home_and_away(_frame(), HOME) == (HOME, AWAY)
    assert home_and_away(_frame(), AWAY) == (AWAY, HOME)


def test_driver_edit_changes_every_tracking_key() -> None:
    for modules in (ts.PITCH_CODE_MODULES, ts.SHAPE_CODE_MODULES, ts.LINE_BREAKING_CODE_MODULES):
        assert "lakehouse.compute.tracking_steps" in modules
        assert set(modules) <= set(ik.TRACKING_CODE_MODULES)
    assert "lakehouse.compute.tracking_steps" in ik.TRACKING_CODE_MODULES


def _line_breaking_rows() -> pa.Table:
    return pa.table(
        {
            "canonical_match_key": pa.array([MK], pa.int64()),
            "period_id": pa.array([1], pa.int32()),
            "action_id": pa.array([1], pa.int32()),
            "canonical_player_key": pa.array([2], pa.int64()),
            "line_break": pa.array([True], pa.bool_()),
            "lines_broken": pa.array([1], pa.int32()),
            "line_breaking_type": pa.array(["through"], pa.string()),
        }
    )


def test_line_breaking_recomputes_on_a_frame_change(conn: duckdb.DuckDBPyConnection) -> None:
    writer = _Writer(conn)
    scorer = _Counter(_line_breaking_rows())
    step = ts.LineBreaking(frame_builder=_Counter(_frame()), scorer=scorer, writer=cast("Any", writer))
    _run(step, conn, [])
    _run(step, conn, [])
    assert scorer.calls == 1  # unchanged inputs: skipped
    conn.execute("UPDATE lake.main.fct_tracking_ball SET time_seconds = 0.08 WHERE time_seconds = 0.04")
    _run(step, conn, [])
    assert scorer.calls == 2  # a frame-side change re-computes line_breaking (B r3 HFTC-SPEC-21)


def test_abandoned_match_keeps_previous_rows(conn: duckdb.DuckDBPyConnection) -> None:
    """Review Focus 3: a stale match whose scorer is abandoned at the hard deadline keeps its previous rows and its
    stale key, so the next run recomputes it."""
    from lakehouse.compute.control import ComputeAbandoned

    conn.execute("CREATE TABLE lake.main.pitch_control_values (canonical_match_key BIGINT, _source_hash VARCHAR)")
    conn.execute("INSERT INTO lake.main.pitch_control_values VALUES (7, 'previous')")
    conn.execute("CREATE TABLE lake.main.shape_graph_labels (canonical_match_key BIGINT, _source_hash VARCHAR)")
    conn.execute("INSERT INTO lake.main.shape_graph_labels VALUES (7, 'previous')")
    writer, errors = _Writer(conn), list[dict[str, Any]]()
    outcomes = _run(_group(writer, pitch=_Counter(exc=ComputeAbandoned("hard deadline"))), conn, errors)
    assert writer.writes == []  # an abandoned match writes nothing for any member
    assert conn.execute("SELECT _source_hash FROM lake.main.pitch_control_values").fetchall() == [("previous",)]
    assert conn.execute("SELECT _source_hash FROM lake.main.shape_graph_labels").fetchall() == [("previous",)]
    assert [o.deferred_matches for o in outcomes] == [1, 1]
    assert all(e["kind"] == "compute_deferred" for e in errors)
    pitch = _Counter(_pitch_rows())
    _run(_group(writer, pitch=pitch), conn, [])
    assert pitch.calls == 1  # the next run recomputes the stale match


class _RecordingConn:
    """Records every `(sql, params)` the driver issues and delegates to the real connection."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        self.calls.append((sql, params))
        return self._conn.execute(sql) if params is None else self._conn.execute(sql, params)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.mark.parametrize("which", ["line_breaking", "group"])
def test_key_and_build_get_the_same_record_keys(conn: duckdb.DuckDBPyConnection, which: str) -> None:
    """perf spec §7.4a item 6: the driver looks up the match's record keys ONCE and passes the same tuple to the key
    reads and to the frame build, so the key hashes exactly the rows the build reads."""
    conn.execute("INSERT INTO lake.main.ops_canonical_pointers VALUES ('match', 7, 7), ('match', 70, 7)")
    builder = _Counter(_frame())
    writer = _Writer(conn)
    step: Any = (
        ts.LineBreaking(frame_builder=builder, scorer=_Counter(_line_breaking_rows()), writer=cast("Any", writer))
        if which == "line_breaking"
        else _group(writer, builder=builder)
    )
    recording = _RecordingConn(conn)
    _run(step, cast("Any", recording), [])
    assert builder.calls == 1
    assert builder.kwargs[0]["record_keys"] == (7, 70)
    pushed = [params for sql, params in recording.calls if "list_contains" in sql]
    assert len(pushed) >= 2  # the frames and ball key reads (the build is the fake)
    assert all(params[-1] == [7, 70] for params in pushed)
    assert [sql for sql, _ in recording.calls].count(ik.MATCH_RECORD_KEYS_SQL) == 1
