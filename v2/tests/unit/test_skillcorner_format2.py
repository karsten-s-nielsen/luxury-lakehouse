"""SkillCorner pining owner-tier format 2 (pining ADR 0011): the same match as JSONL/CSV and as Parquet gives the same
canonical bronze (spec §12.5; plan Task 20). The Parquet fixtures were made from the public slices of 1899585 with
the pining producer's own transforms (`tests/fixtures/skillcorner/README.md`)."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from skillcornerkit import ENTRY, FIXTURES, delivery

from lakehouse.bronze.arrow import read_parquet_bytes
from lakehouse.errors import InexactValueError, IntegrityError, UnmappedFieldError
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.skillcorner import (
    DYNAMIC_EVENTS,
    MATCHES,
    PHYSICAL,
    TRACKING_FRAMES,
    TRACKING_PLAYERS,
    TRACKING_ROSTER,
    SkillCornerParser,
)
from lakehouse.raw.archive import Delivery

AUDIT = ("_ingested_at", "_run_id", "_source_hash")


def _canonical(table: pa.Table) -> list[dict[str, Any]]:
    return table.drop_columns([c for c in AUDIT if c in table.column_names]).to_pylist()


def _raw_delivery(artifact: str, raw: bytes) -> Delivery:
    base = delivery("match")
    return Delivery.received(
        provider="skillcorner",
        channel="pining",
        native_match_id=base.native_match_id,
        artifact=artifact,
        raw=raw,
        visibility="private",
        change_signal=None,
        fetched_at=base.fetched_at,
    )


def _parse(**deliveries: Delivery) -> ParsedMatch:
    return SkillCornerParser().parse(ENTRY, {"match": delivery("match"), **deliveries}, run_id="r")


def _parquet(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_tracking_parquet_equals_jsonl() -> None:
    jsonl = _parse(tracking=delivery("tracking"))
    parquet = _parse(tracking_parquet=_raw_delivery("tracking_parquet", _parquet("tracking_1899585_slice.parquet")))
    for name in (TRACKING_PLAYERS, TRACKING_FRAMES, TRACKING_ROSTER):
        assert _canonical(parquet.tables[name]) == _canonical(jsonl.tables[name]), name
    assert parquet.tables[TRACKING_FRAMES].num_rows == 182
    assert "tracking_parquet" in parquet.coordinate_systems


def test_events_parquet_equals_csv() -> None:
    csv = _parse(dynamic_events=delivery("dynamic_events"))
    parquet = _parse(
        dynamic_events_parquet=_raw_delivery("dynamic_events_parquet", _parquet("dynamic_events_1899585_slice.parquet"))
    )
    assert parquet.tables[DYNAMIC_EVENTS].column_names == csv.tables[DYNAMIC_EVENTS].column_names
    assert _canonical(parquet.tables[DYNAMIC_EVENTS]) == _canonical(csv.tables[DYNAMIC_EVENTS])


def _rewritten(name: str, change: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]) -> bytes:
    """The fixture with its rows changed, written back with pyarrow's inferred schema."""
    rows = change(read_parquet_bytes(_parquet(name)).to_pylist())
    buffer = io.BytesIO()
    writer: Any = pq.write_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs
    writer(pa.Table.from_pylist(rows), buffer)
    return buffer.getvalue()


def _with(key: str, value: object) -> Callable[[list[dict[str, Any]]], list[dict[str, Any]]]:
    return lambda rows: [{**row, key: value} for row in rows]


def test_tracking_parquet_unknown_column_fails() -> None:
    raw = _rewritten("tracking_1899585_slice.parquet", _with("speed", 1.0))
    with pytest.raises(UnmappedFieldError, match="speed"):
        _parse(tracking_parquet=_raw_delivery("tracking_parquet", raw))


def test_tracking_parquet_inexact_coordinate_fails() -> None:
    def change(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{**row, "ball_data": {**row["ball_data"], "x": 1.2345}} for row in rows]

    raw = _rewritten("tracking_1899585_slice.parquet", change)
    with pytest.raises(InexactValueError, match=r"ball_data\.x"):
        _parse(tracking_parquet=_raw_delivery("tracking_parquet", raw))


def test_events_parquet_unknown_column_fails() -> None:
    raw = _rewritten("dynamic_events_1899585_slice.parquet", _with("new_model", 0.5))
    with pytest.raises(UnmappedFieldError, match="new_model"):
        _parse(dynamic_events_parquet=_raw_delivery("dynamic_events_parquet", raw))


def test_owner_metadata_coach_objects() -> None:
    """The owner tier's `metadata.json` writes a coach as `{id, first_name, last_name}`; the public files write null."""
    payload = json.loads((FIXTURES / "match_1899585.json").read_bytes())
    payload["home_team_coach"] = {"id": 4711, "first_name": "", "last_name": "Example"}
    parsed = SkillCornerParser().parse(
        ENTRY, {"match": _raw_delivery("match", json.dumps(payload).encode())}, run_id="r"
    )
    row = parsed.tables[MATCHES].to_pylist()[0]
    assert (row["home_team_coach_id"], row["home_team_coach_first_name"], row["home_team_coach_last_name"]) == (
        4711,
        "",
        "Example",
    )
    assert (row["away_team_coach_id"], row["away_team_coach_last_name"]) == (None, None)


def test_physical_table() -> None:
    """Format 2 `physical.parquet`: one row per player, every column as its own typed column (owner decision)."""
    raw = _parquet("physical_1899585_synthetic.parquet")
    table = _parse(physical=_raw_delivery("physical", raw)).tables[PHYSICAL]
    source = read_parquet_bytes(raw).to_pylist()
    assert table.num_rows == len(source) == 3
    rows = table.to_pylist()
    assert [r["hsr_distance_full_all"] for r in rows] == [s["hsr_distance_full_all"] for s in source]
    assert rows[2]["timetosprint"] is None
    assert [r["physical_check_passed"] for r in rows] == [True, False, True]
    assert all(r["player_key"] is not None and r["team_key"] is not None for r in rows)
    assert {r["match_key"] for r in rows} == {rows[0]["match_key"]}


def test_physical_other_match_fails() -> None:
    raw = _rewritten("physical_1899585_synthetic.parquet", _with("match_id", 42))
    with pytest.raises(IntegrityError, match="42"):
        _parse(physical=_raw_delivery("physical", raw))


def test_physical_unknown_column_fails() -> None:
    raw = _rewritten("physical_1899585_synthetic.parquet", _with("new_metric", 1.0))
    with pytest.raises(UnmappedFieldError, match="new_metric"):
        _parse(physical=_raw_delivery("physical", raw))


def test_both_forms_of_one_artifact_fail() -> None:
    """A match with tracking as JSONL and as Parquet (or events as CSV and Parquet) has two truths; it fails."""
    tracking = _raw_delivery("tracking_parquet", _parquet("tracking_1899585_slice.parquet"))
    with pytest.raises(IntegrityError, match="JSONL and as Parquet"):
        _parse(tracking=delivery("tracking"), tracking_parquet=tracking)
    events = _raw_delivery("dynamic_events_parquet", _parquet("dynamic_events_1899585_slice.parquet"))
    with pytest.raises(IntegrityError, match="CSV and as Parquet"):
        _parse(dynamic_events=delivery("dynamic_events"), dynamic_events_parquet=events)


def test_tracking_parquet_null_struct_is_an_absent_object() -> None:
    """A null `possession` struct in the Parquet reads as the JSONL frame without `possession`: null columns."""

    def change(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{**row, "possession": None} if index == 0 else row for index, row in enumerate(rows)]

    raw = _rewritten("tracking_1899585_slice.parquet", change)
    frames = _parse(tracking_parquet=_raw_delivery("tracking_parquet", raw)).tables[TRACKING_FRAMES].to_pylist()
    assert (frames[0]["possession_player_key"], frames[0]["possession_group"]) == (None, None)
