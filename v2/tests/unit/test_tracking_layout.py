"""The tuned tracking layout (spec §10.2, §11.5) and the per-match file contract (§29.10)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse.bronze import tracking
from lakehouse.bronze.arrow import column_types, read_parquet, schema_field
from lakehouse.bronze.schemas import TRACKING_FRAMES_CORE, TRACKING_PLAYERS_CORE, BronzeTable, bronze_table
from lakehouse.bronze.writer import parse_file_name, write_bronze_file, write_tracking_file

PLAYERS = bronze_table("skillcorner_tracking_players")
FRAMES = bronze_table("skillcorner_tracking_frames")
MATCH = -4203055498971744044  # surrogate keys are signed


def _core_fields(schema: pa.Schema) -> list[pa.Field[Any]]:
    return [pa.field(name, arrow_type) for name, arrow_type in column_types(schema).items()]


def _players(rows: list[tuple[int, int, int]], *, match_key: int = MATCH) -> pa.Table:
    """Rows of (period, player_key, frame), deliberately unsorted by the caller."""
    return pa.table(
        {
            "match_key": pa.array([match_key] * len(rows), pa.int64()),
            "period": pa.array([r[0] for r in rows], pa.int8()),
            "frame": pa.array([r[2] for r in rows], pa.int32()),
            "player_key": pa.array([r[1] for r in rows], pa.int64()),
            "x_mm": pa.array([r[2] * 3 for r in rows], pa.int32()),
            "y_mm": pa.array([-r[2] for r in rows], pa.int32()),
            "is_visible": pa.array([True] * len(rows), pa.bool_()),
            "speed_mm_s": pa.array([100] * len(rows), pa.int32()),  # a provider-specific extra column
        },
        schema=pa.schema([*_core_fields(TRACKING_PLAYERS_CORE), pa.field("speed_mm_s", pa.int32())]),
    )


def _frames(frames: list[tuple[int, int]]) -> pa.Table:
    return pa.table(
        {
            "match_key": pa.array([MATCH] * len(frames), pa.int64()),
            "period": pa.array([f[0] for f in frames], pa.int8()),
            "frame": pa.array([f[1] for f in frames], pa.int32()),
            "timestamp": pa.array([f[1] * 100_000 for f in frames], pa.duration("us")),
            "ball_x_mm": pa.array([f[1] for f in frames], pa.int32()),
            "ball_y_mm": pa.array([f[1] for f in frames], pa.int32()),
            "ball_z_mm": pa.array([None] * len(frames), pa.int32()),
            "ball_detected": pa.array([True] * len(frames), pa.bool_()),
        },
        schema=TRACKING_FRAMES_CORE,
    )


def _encodings(path: Path) -> dict[str, tuple[str, ...]]:
    group = pq.ParquetFile(path).metadata.row_group(0)
    return {group.column(i).path_in_schema: group.column(i).encodings for i in range(group.num_columns)}


def test_bronze_table_names() -> None:
    assert bronze_table("idsse_tracking_players") == BronzeTable("idsse_tracking_players")
    for bad in ("Idsse", "1abc", "a-b", "a/b", "a b", ""):
        with pytest.raises(ValueError):
            bronze_table(bad)


def test_tracking_encodings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []
    real: Any = pq.write_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - pyarrow-stubs gap, see bronze.arrow

    def spy(table: pa.Table, where: str | Path, **kwargs: Any) -> None:
        seen.append(kwargs)
        real(table, where, **kwargs)

    monkeypatch.setattr(pq, "write_table", spy)
    path = write_tracking_file(
        PLAYERS, _players([(1, 7, f) for f in range(50)]), match_key=MATCH, content_version="v1", out_dir=tmp_path
    )
    encodings = _encodings(path)
    for column in ("x_mm", "y_mm", "frame"):
        assert "DELTA_BINARY_PACKED" in encodings[column]
        assert "RLE_DICTIONARY" not in encodings[column]
    for column in ("player_key", "match_key", "period"):
        assert "RLE_DICTIONARY" in encodings[column]
    # Parquet has no dictionary for BOOLEAN: flags stay bit-packed, 1 bit per value
    assert "RLE_DICTIONARY" not in encodings["is_visible"]
    assert "DELTA_BINARY_PACKED" not in encodings["is_visible"]
    group = pq.ParquetFile(path).metadata.row_group(0)
    assert {group.column(i).compression for i in range(group.num_columns)} == {"ZSTD"}
    # the zstd level is not stored in Parquet metadata, so the writer's arguments are checked
    assert (seen[0]["compression"], seen[0]["compression_level"]) == ("zstd", 3)
    assert "BYTE_STREAM_SPLIT" not in str(seen[0])
    frames = write_tracking_file(
        FRAMES, _frames([(1, f) for f in range(20)]), match_key=MATCH, content_version="v1", out_dir=tmp_path
    )
    frame_encodings = _encodings(frames)
    for column in ("frame", "timestamp", "ball_x_mm", "ball_y_mm"):
        assert "DELTA_BINARY_PACKED" in frame_encodings[column]


def test_tracking_sorted(tmp_path: Path) -> None:
    rows = [(2, 9, 5), (1, 9, 3), (1, 4, 8), (2, 4, 1), (1, 4, 2), (1, 9, 1)]
    path = write_tracking_file(PLAYERS, _players(rows), match_key=MATCH, content_version="v1", out_dir=tmp_path)
    written = read_parquet(path)
    order = list(
        zip(written["period"].to_pylist(), written["player_key"].to_pylist(), written["frame"].to_pylist(), strict=True)
    )
    assert order == sorted((p, k, f) for p, k, f in rows)
    frames = write_tracking_file(
        FRAMES, _frames([(2, 3), (1, 9), (1, 2), (2, 1)]), match_key=MATCH, content_version="v1", out_dir=tmp_path
    )
    frame_order = list(
        zip(read_parquet(frames)["period"].to_pylist(), read_parquet(frames)["frame"].to_pylist(), strict=True)
    )
    assert frame_order == [(1, 2), (1, 9), (2, 1), (2, 3)]


def test_write_id_unique(tmp_path: Path) -> None:
    data = _players([(1, 7, 1), (1, 7, 2)])
    first = write_tracking_file(PLAYERS, data, match_key=MATCH, content_version="v1", out_dir=tmp_path)
    second = write_tracking_file(PLAYERS, data, match_key=MATCH, content_version="v1", out_dir=tmp_path)
    assert first != second
    assert first.parent == second.parent == tmp_path / PLAYERS
    one, two = parse_file_name(first.name), parse_file_name(second.name)
    assert (one.match_key, one.content_version) == (two.match_key, two.content_version) == (MATCH, "v1")
    assert one.write_id != two.write_id
    assert first.name.startswith(f"{MATCH}-v1-")


def test_rowcount_and_schema_checked(tmp_path: Path) -> None:
    wrong_type = _players([(1, 7, 1)]).set_column(5, "y_mm", pa.array([1.5], pa.float64()))
    missing = _players([(1, 7, 1)]).drop_columns(["is_visible"])
    empty = _players([])
    other_match = _players([(1, 7, 1)], match_key=MATCH + 1)
    for data, message in ((wrong_type, "y_mm"), (missing, "is_visible"), (empty, "no rows"), (other_match, "match")):
        with pytest.raises(ValueError, match=message):
            write_tracking_file(PLAYERS, data, match_key=MATCH, content_version="v1", out_dir=tmp_path)
    with pytest.raises(ValueError, match="tracking"):
        write_tracking_file(
            bronze_table("statsbomb_events"),
            _players([(1, 7, 1)]),
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path,
        )
    with pytest.raises(ValueError, match="content version"):
        write_tracking_file(PLAYERS, _players([(1, 7, 1)]), match_key=MATCH, content_version="v-1", out_dir=tmp_path)
    assert not tmp_path.exists() or list(tmp_path.rglob("*.parquet")) == []


def test_bronze_file_audit_columns(tmp_path: Path) -> None:
    schema = pa.schema([("match_key", pa.int64()), ("event_id", pa.string())])
    data = pa.table({"match_key": pa.array([MATCH, MATCH], pa.int64()), "event_id": ["a", "b"]}, schema=schema)
    when = dt.datetime(2026, 10, 26, 12, 0, tzinfo=dt.UTC)
    path = write_bronze_file(
        bronze_table("statsbomb_events"),
        data,
        schema=schema,
        match_key=MATCH,
        content_version="v1",
        out_dir=tmp_path,
        run_id="run-1",
        source_hash="ab" * 32,
        ingested_at=when,
    )
    written = read_parquet(path)
    assert written.column_names == ["match_key", "event_id", "_ingested_at", "_run_id", "_source_hash"]
    assert written["_ingested_at"].type == pa.timestamp("us", tz="UTC")
    assert set(written["_run_id"].to_pylist()) == {"run-1"}
    assert set(written["_source_hash"].to_pylist()) == {"ab" * 32}
    assert written["_ingested_at"].to_pylist() == [when, when]
    with pytest.raises(ValueError, match="schema"):
        write_bronze_file(
            bronze_table("statsbomb_events"),
            data.drop_columns(["event_id"]),
            schema=schema,
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path,
            run_id="run-1",
            source_hash="ab" * 32,
        )


def test_tracking_kinds() -> None:
    assert tracking.tracking_kind(PLAYERS) == "players"
    assert tracking.tracking_kind(FRAMES) == "frames"
    with pytest.raises(ValueError):
        tracking.tracking_kind(bronze_table("idsse_events"))


def test_bronze_file_accepts_parser_audit_columns(tmp_path: Path) -> None:
    """A parser that adds the audit columns itself (Task 15) is written as is, after a check of its values."""
    schema = pa.schema([("match_key", pa.int64()), ("event_id", pa.string())])
    when = dt.datetime(2026, 10, 26, 12, 0, tzinfo=dt.UTC)
    data = pa.table({"match_key": pa.array([MATCH], pa.int64()), "event_id": ["a"]}, schema=schema)
    audited = data.append_column(
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.array([when], pa.timestamp("us", tz="UTC")),
    )
    audited = audited.append_column(pa.field("_run_id", pa.string(), nullable=False), pa.array(["run-1"]))
    audited = audited.append_column(pa.field("_source_hash", pa.string(), nullable=False), pa.array(["ab" * 32]))
    path = write_bronze_file(
        bronze_table("statsbomb_events"),
        audited,
        schema=schema,
        match_key=MATCH,
        content_version="v1",
        out_dir=tmp_path,
        run_id="run-1",
        source_hash="ab" * 32,
    )
    written = read_parquet(path)
    assert written.column_names == ["match_key", "event_id", "_ingested_at", "_run_id", "_source_hash"]
    assert written["_ingested_at"].to_pylist() == [when]
    with pytest.raises(ValueError, match="_run_id"):
        write_bronze_file(
            bronze_table("statsbomb_events"),
            audited,
            schema=schema,
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path,
            run_id="run-2",
            source_hash="ab" * 32,
        )
    with pytest.raises(ValueError, match="_source_hash"):  # one hash given: every row must have it
        write_bronze_file(
            bronze_table("statsbomb_events"),
            audited,
            schema=schema,
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path,
            run_id="run-1",
            source_hash="cd" * 32,
        )
    # the match's delivery hashes: each row's hash must be one of them (a table from several deliveries)
    write_bronze_file(
        bronze_table("statsbomb_events"),
        audited,
        schema=schema,
        match_key=MATCH,
        content_version="v1",
        out_dir=tmp_path / "set",
        run_id="run-1",
        source_hash=frozenset({"ab" * 32, "cd" * 32}),
    )
    with pytest.raises(ValueError, match="_source_hash"):
        write_bronze_file(
            bronze_table("statsbomb_events"),
            audited,
            schema=schema,
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path / "other",
            run_id="run-1",
            source_hash=frozenset({"cd" * 32, "ef" * 32}),
        )
    with pytest.raises(ValueError, match="one source hash"):  # no parser audit columns: a set cannot be filled in
        write_bronze_file(
            bronze_table("statsbomb_events"),
            data,
            schema=schema,
            match_key=MATCH,
            content_version="v1",
            out_dir=tmp_path / "none",
            run_id="run-1",
            source_hash=frozenset({"ab" * 32, "cd" * 32}),
        )


def test_frames_period_may_be_null(tmp_path: Path) -> None:
    """A frame outside every period (SkillCorner pre-match and half-time frames, `period: null`) keeps a null
    period: nothing is invented (§12.6). Player rows always have a period."""
    frames_period = schema_field(TRACKING_FRAMES_CORE, "period")
    assert frames_period.nullable
    assert not schema_field(TRACKING_PLAYERS_CORE, "period").nullable
    data = _frames([(1, 5), (1, 2)])
    periods = pa.array([None, 1], pa.int8())
    data = data.set_column(data.schema.get_field_index("period"), frames_period, periods)
    path = write_tracking_file(FRAMES, data, match_key=MATCH, content_version="v1", out_dir=tmp_path)
    written = read_parquet(path)
    assert list(zip(written["period"].to_pylist(), written["frame"].to_pylist(), strict=True)) == [(1, 2), (None, 5)]


def test_frames_timestamp_is_microseconds() -> None:
    """GS gives the time since the period start with 6 decimals (`periodElapsedTime: 0.033041`), so the shared core
    holds microseconds; milliseconds (IDSSE, SkillCorner) fit it exactly."""
    assert column_types(TRACKING_FRAMES_CORE)["timestamp"] == pa.duration("us")
