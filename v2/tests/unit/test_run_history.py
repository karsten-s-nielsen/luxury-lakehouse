"""Run history and the progress object (spec §11.3, §11.4, §17.1, §17.4, §19; plan Task 21)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from lakekit import LakeKit

from lakehouse.errors import InexactValueError, UnmappedFieldError
from lakehouse.parsers.base import CoordinateSystem
from lakehouse.runs.history import (
    ErrorRow,
    MatchFile,
    MatchRow,
    MatchSource,
    MatchStep,
    ModuleRun,
    RunRow,
    error_message,
    read_table,
    replace_match,
    write_coordinate_systems,
    write_errors,
    write_module_run,
    write_run,
    write_steps,
)
from lakehouse.runs.progress import Progress, read_progress, write_progress

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


@pytest.fixture
def conn(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"ATTACH '{(tmp_path / 'lake.duckdb').as_posix()}' AS lake")
    return con


def test_run_and_module_rows_round_trip(conn: duckdb.DuckDBPyConnection) -> None:
    run = RunRow(
        run_id="r1",
        trigger="cli",
        status="ok",
        reason=None,
        started_at=NOW,
        ended_at=NOW,
        added=2,
        changed=1,
        unchanged=3,
        failed=0,
        skipped=0,
        excluded=1,
    )
    write_run(conn, run)
    write_module_run(
        conn,
        ModuleRun(
            module_run_id="r1.idsse_public",
            run_id="r1",
            module_id="idsse_public",
            status="ok",
            started_at=NOW,
            ended_at=NOW,
            added=2,
            changed=1,
            unchanged=3,
            failed=0,
            excluded=1,
        ),
    )
    assert read_table(conn, "ops_runs") == [run.as_row()]
    assert read_table(conn, "ops_module_runs")[0]["module_id"] == "idsse_public"


def test_steps_and_errors_have_stable_ids(conn: duckdb.DuckDBPyConnection) -> None:
    write_steps(
        conn,
        [
            MatchStep(
                step_id="r1.idsse_public.J03WPY",
                run_id="r1",
                module_run_id="r1.idsse_public",
                module_id="idsse_public",
                match_key=7,
                native_match_id="J03WPY",
                outcome="added",
                visibility="public",
                recorded_at=NOW,
            )
        ],
    )
    write_errors(
        conn,
        "r1",
        [
            ErrorRow(
                module_run_id="r1.idsse_public",
                module_id="idsse_public",
                native_match_id="J03WPY",
                kind="unmapped_field",
                severity="error",
                message="unmapped fields in events: x",
                recorded_at=NOW,
            ),
            ErrorRow(
                module_run_id=None,
                module_id="gradientsports_pining",
                native_match_id=None,
                kind="visibility_check_failed",
                severity="warning",
                message="HTTP 401",
                recorded_at=NOW,
            ),
        ],
    )
    errors = read_table(conn, "ops_errors")
    assert [e["error_id"] for e in errors] == ["r1.e1", "r1.e2"]
    assert read_table(conn, "ops_match_steps")[0]["step_id"] == "r1.idsse_public.J03WPY"


def test_replace_match_is_an_upsert(conn: duckdb.DuckDBPyConnection) -> None:
    def row(visibility: str) -> MatchRow:
        return MatchRow(
            match_key=7,
            provider="idsse",
            native_match_id="J03WPY",
            visibility=visibility,  # type: ignore[arg-type]
            kept_channel="public",
            kept_reason="only the public copy",
            competition_id="DFL-COM-000002",
            competition_name="2. Bundesliga",
            season_id="DFL-SEA-0001K6",
            season_name="2022/2023",
            has_360_frames=None,
            run_id="r1",
            updated_at=NOW,
        )

    sources = [
        MatchSource(match_key=7, module_id="idsse_public", channel="public", channel_id="J03WPY", last_listed=NOW)
    ]
    files = [
        MatchFile(
            match_key=7,
            provider="idsse",
            bronze_table="idsse_events",
            root="lake",
            path="data/bronze/idsse_events/7-1-" + "a" * 32 + ".parquet",
            rows=10,
            ingested_at=NOW,
            run_id="r1",
            source_hash="b" * 64,
        )
    ]
    replace_match(conn, row("public"), sources, files)
    replace_match(conn, row("private"), sources, files)
    assert [m["visibility"] for m in read_table(conn, "ops_matches")] == ["private"]
    assert len(read_table(conn, "ops_match_sources")) == 1
    assert len(read_table(conn, "ops_match_files")) == 1


def test_coordinate_systems_one_row_per_artifact(conn: duckdb.DuckDBPyConnection) -> None:
    system = CoordinateSystem(
        origin="centre",
        units="m",
        pitch_length=105.0,
        pitch_width=68.0,
        x_direction="x",
        y_direction="y",
        play_direction=None,
        source_ref="ref",
    )
    write_coordinate_systems(
        conn, match_key=7, provider="idsse", run_id="r1", systems={"positions": system, "events": system}
    )
    write_coordinate_systems(conn, match_key=7, provider="idsse", run_id="r2", systems={"positions": system})
    rows = read_table(conn, "ops_coordinate_systems")
    assert [(r["artifact"], r["run_id"]) for r in rows] == [("positions", "r2")]  # a re-parse replaces the match's rows


def test_error_message_of_a_private_match_holds_no_value() -> None:
    """Row-level data never enters a lake-root table (§9): a private match's message keeps the error class and,
    for unmapped fields, the field paths only."""
    inexact = InexactValueError("tracking: line 7: ball_data.x=1.2345: more than 3 decimals")
    assert error_message(inexact, visibility="public") == str(inexact)
    assert error_message(inexact, visibility="private") == "InexactValueError"
    unmapped = UnmappedFieldError("events", ["a.b", "c"])
    assert error_message(unmapped, visibility="private") == "UnmappedFieldError: events: a.b, c"


def test_progress_object(kit: LakeKit) -> None:
    storage = kit.storage
    progress = Progress(
        run_id="r1",
        trigger="cli",
        status="running",
        step="list",
        module_id="idsse_public",
        done=1,
        total=7,
        started_at=NOW,
        updated_at=NOW,
        message=None,
    )
    write_progress(storage, progress)
    assert storage.exists("lake", "runs/progress/r1.json")
    assert read_progress(storage, "r1") == progress
    write_progress(storage, progress.advance(step="parse", done=2, at=NOW))
    assert read_progress(storage, "r1").done == 2
