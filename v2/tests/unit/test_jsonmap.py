"""Declarative JSON-to-Arrow mapping with exactness checks (spec §12.1, §12.2, §12.4)."""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from lakehouse.bronze.arrow import column_types
from lakehouse.errors import InexactValueError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.parsers.jsonmap import Field, covered_paths, loads_exact, mapped_paths, records_to_table

ID_NAME = pa.struct([("id", pa.int64()), ("name", pa.string())])
FIELDS = (
    Field("event_id", "id", pa.string()),
    Field("type_id", "type.id", pa.int64()),
    Field("type_name", "type.name", pa.string()),
    Field("location", "location", pa.list_(pa.float64())),
    Field("timestamp", "timestamp", pa.duration("ms"), convert="clock_ms"),
    Field("match_date", "match_date", pa.date32(), convert="date"),
    Field("kick_off", "kick_off", pa.time32("ms"), convert="time_ms"),
    Field("last_updated", "last_updated", pa.timestamp("us"), convert="timestamp"),
    Field(
        "freeze_frame",
        "shot.freeze_frame",
        pa.list_(pa.struct([("location", pa.list_(pa.float64())), ("player", ID_NAME), ("teammate", pa.bool_())])),
    ),
    Field(
        "positions",
        "positions",
        pa.list_(pa.struct([("position", pa.string()), ("from", pa.duration("s"))])),
        nested={"[].from": "clock_s"},
    ),
)


def test_covered_paths_follow_the_type() -> None:
    assert covered_paths(FIELDS[8]) == frozenset(
        {
            "shot.freeze_frame[].location",
            "shot.freeze_frame[].player",  # a null player object
            "shot.freeze_frame[].player.id",
            "shot.freeze_frame[].player.name",
            "shot.freeze_frame[].teammate",
        }
    )
    assert "location" in mapped_paths(FIELDS)
    assert "positions[].from" in mapped_paths(FIELDS)


def test_records_to_table_types_and_values() -> None:
    records = loads_exact(
        """[
      {"id": "a", "type": {"id": 30, "name": "Pass"}, "location": [60.5, 40.0], "timestamp": "00:12:34.567",
       "match_date": "2022-11-20", "kick_off": "19:00:00.000", "last_updated": "2023-04-26T23:49:58.956186",
       "shot": {"freeze_frame": [{"location": [100.1, 40.2], "player": {"id": 7, "name": "Näme"},
                                   "teammate": true}]},
       "positions": [{"position": "Left Back", "from": "105:30"}]},
      {"id": "b", "type": {"id": 42, "name": "Ball Receipt*"}}
    ]""".encode()
    )
    table = records_to_table(records, FIELDS, artifact="events")
    assert table.column_names == [f.column for f in FIELDS]
    row = table.to_pylist()[0]
    assert row["timestamp"] == dt.timedelta(minutes=12, seconds=34, milliseconds=567)
    assert row["match_date"] == dt.date(2022, 11, 20)
    assert row["kick_off"] == dt.time(19, 0)
    assert row["last_updated"] == dt.datetime(2023, 4, 26, 23, 49, 58, 956186)
    assert row["freeze_frame"][0]["player"] == {"id": 7, "name": "Näme"}
    assert row["positions"][0]["from"] == dt.timedelta(minutes=105, seconds=30)
    second = table.to_pylist()[1]
    assert second["location"] is None
    assert second["freeze_frame"] is None
    assert column_types(table.schema)["timestamp"] == pa.duration("ms")


def test_unmapped_field_fails_loudly() -> None:
    with pytest.raises(UnmappedFieldError) as caught:
        records_to_table([{"id": "a", "type": {"id": 1, "name": "x", "extra": 2}}], FIELDS, artifact="events")
    assert caught.value.field_paths == ["type.extra"]


def test_float_text_round_trip_checked() -> None:
    assert loads_exact(b"[89.81834545454545, 0.1]") == [89.81834545454545, 0.1]
    with pytest.raises(InexactValueError, match=r"0\.12345678901234567890123"):
        loads_exact(b"[0.12345678901234567890123]")


@pytest.mark.parametrize(
    ("convert", "text"),
    [
        ("clock_ms", "00:12:34.5"),
        ("clock_s", "5:3"),
        ("clock_s", "12:75"),
        ("clock_ms", "00:61:00.000"),
        ("date", "2022-1-2"),
        ("time_ms", "19:00"),
        ("timestamp", "2023-04-26T23:49:58.9561867"),
    ],
)
def test_converters_refuse_inexact_text(convert: str, text: str) -> None:
    field_type = {
        "clock_ms": pa.duration("ms"),
        "clock_s": pa.duration("s"),
        "date": pa.date32(),
        "time_ms": pa.time32("ms"),
        "timestamp": pa.timestamp("us"),
    }[convert]
    field = Field("value", "value", field_type, convert=convert)  # type: ignore[arg-type]
    with pytest.raises(InexactValueError):
        records_to_table([{"value": text}], (field,), artifact="x")


def test_timestamp_keeps_its_fraction_digits() -> None:
    field = Field("value", "value", pa.timestamp("us"), convert="timestamp")
    table = records_to_table(
        [{"value": "2024-09-28T20:46:38.893391"}, {"value": "2024-09-28T20:46:38"}], (field,), artifact="x"
    )
    assert table.column("value").to_pylist() == [
        dt.datetime(2024, 9, 28, 20, 46, 38, 893391),
        dt.datetime(2024, 9, 28, 20, 46, 38),
    ]


def test_type_mismatch_fails_loudly() -> None:
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError, TypeError)):
        records_to_table([{"id": "a", "type": {"id": "not an int", "name": "x"}}], FIELDS, artifact="events")


def test_clock_hms_and_utc_timestamps() -> None:
    fields = (
        Field("start", "start", pa.duration("s"), convert="clock_hms"),
        Field("at", "at", pa.timestamp("us", tz="UTC"), convert="timestamp_utc"),
    )
    table = records_to_table(
        [
            {"start": "00:45:03", "at": "2024-12-07T04:00:00Z"},
            {"start": "01:07:06", "at": "2026-04-01T13:19:03.072753Z"},
        ],
        fields,
        artifact="x",
    )
    assert table.column("start").to_pylist() == [
        dt.timedelta(minutes=45, seconds=3),
        dt.timedelta(hours=1, seconds=426),
    ]
    assert table.column("at").to_pylist() == [
        dt.datetime(2024, 12, 7, 4, tzinfo=dt.UTC),
        dt.datetime(2026, 4, 1, 13, 19, 3, 72753, tzinfo=dt.UTC),
    ]


@pytest.mark.parametrize(
    ("convert", "text"),
    [
        ("clock_hms", "0:45:03"),
        ("clock_hms", "00:61:00"),
        ("clock_hms", "00:45:3"),
        ("timestamp_utc", "2024-12-07T04:00:00"),
        ("timestamp_utc", "2024-12-07T04:00:00+00:00"),
        ("timestamp_utc", "2024-12-07T04:00:00.1234567Z"),
        ("timestamp_utc", "2024-02-30T04:00:00Z"),
    ],
)
def test_clock_hms_and_utc_timestamps_refuse_inexact_text(convert: str, text: str) -> None:
    arrow_type = pa.duration("s") if convert == "clock_hms" else pa.timestamp("us", tz="UTC")
    field = Field("value", "value", arrow_type, convert=convert)  # type: ignore[arg-type]
    with pytest.raises(InexactValueError):
        records_to_table([{"value": text}], (field,), artifact="x")


def test_null_struct_is_mapped() -> None:
    """A null where a struct column expects an object (`playing_time.total: null`) is a value of that column."""
    total = pa.struct([("start_frame", pa.int64()), ("end_frame", pa.int64())])
    field = Field("total", "playing_time.total", total)
    table = records_to_table(
        [{"playing_time": {"total": None}}, {"playing_time": {"total": {"start_frame": 10, "end_frame": 20}}}],
        (field,),
        artifact="x",
    )
    assert table.column("total").to_pylist() == [None, {"start_frame": 10, "end_frame": 20}]
    with pytest.raises(UnmappedFieldError, match=r"playing_time\.total\.extra"):
        records_to_table([{"playing_time": {"total": {"extra": 1}}}], (field,), artifact="x")


def test_text_int_text_and_space_utc_converters() -> None:
    """Wyscout traps: an id that is an int in one file and text in another, a count written as text, and a UTC time
    written with a space."""
    fields = (
        Field("area_id", "area_id", pa.string(), convert="text"),
        Field("goals", "goals", pa.int64(), convert="int_text"),
        Field("at", "at", pa.timestamp("us", tz="UTC"), convert="timestamp_utc_space"),
    )
    table = records_to_table(
        [{"area_id": 250, "goals": "2", "at": "2018-07-15 15:00:00"}, {"area_id": "0", "goals": "0", "at": None}],
        fields,
        artifact="x",
    )
    assert table.column("area_id").to_pylist() == ["250", "0"]
    assert table.column("goals").to_pylist() == [2, 0]
    assert table.column("at").to_pylist() == [dt.datetime(2018, 7, 15, 15, tzinfo=dt.UTC), None]


@pytest.mark.parametrize(
    ("convert", "value"),
    [
        ("text", 1.5),
        ("text", True),
        ("int_text", "07"),
        ("int_text", "1.0"),
        ("int_text", 3),
        ("timestamp_utc_space", "2018-07-15T15:00:00"),
        ("timestamp_utc_space", "2018-07-15 15:00"),
    ],
)
def test_text_int_text_and_space_utc_refuse(convert: str, value: object) -> None:
    arrow_type = {"text": pa.string(), "int_text": pa.int64(), "timestamp_utc_space": pa.timestamp("us", tz="UTC")}
    field = Field("value", "value", arrow_type[convert], convert=convert)  # type: ignore[arg-type]
    with pytest.raises(InexactValueError):
        records_to_table([{"value": value}], (field,), artifact="x")


def test_null_in_place_of_a_split_object_is_mapped() -> None:
    """A null where the source elsewhere writes an object whose fields are separate columns (`country: null` for
    `country.id` and `country.name`) is a null of those columns, not an unmapped field (commercial StatsBomb)."""
    fields = (
        Field("country_id", "team.country.id", pa.int64()),
        Field("country_name", "team.country.name", pa.string()),
    )
    table = records_to_table(
        [{"team": {"country": None}}, {"team": {"country": {"id": 7, "name": "X"}}}], fields, artifact="x"
    )
    assert table.column("country_id").to_pylist() == [None, 7]
    assert table.column("country_name").to_pylist() == [None, "X"]


def test_scalar_in_place_of_an_object_fails() -> None:
    """A scalar where an object is expected would silently become nulls; it fails instead."""
    fields = (Field("country_id", "team.country.id", pa.int64()),)
    with pytest.raises(UnexpectedPayloadError, match=r"team\.country"):
        records_to_table([{"team": {"country": "X"}}], fields, artifact="x")


def test_clock_ms_or_s_accepts_both_statsbomb_forms() -> None:
    """Open-data lineups write position times as `MM:SS`, the commercial deliveries as `HH:MM:SS.fff` (measured on
    the pining owner tier, 2026-10-27); both become the same millisecond duration, each form checked exactly."""
    field = Field("t", "t", pa.duration("ms"), convert="clock_ms_or_s")
    table = records_to_table(
        [{"t": "45:00"}, {"t": "00:45:00.000"}, {"t": "01:30:00.250"}, {"t": None}], (field,), artifact="x"
    )
    assert table.column("t").to_pylist() == [
        dt.timedelta(minutes=45),
        dt.timedelta(minutes=45),
        dt.timedelta(hours=1, minutes=30, milliseconds=250),
        None,
    ]
    for text in ("45:0", "0:45:00.000", "00:45:00.00"):
        with pytest.raises(InexactValueError):
            records_to_table([{"t": text}], (field,), artifact="x")


def test_timestamp_without_seconds_round_trips_in_its_own_form() -> None:
    """StatsBomb writes some `last_updated_360` values to the minute (`2023-06-27T13:46`; 9 of 291 open-data matches,
    measured on the pining owner tier, 2026-10-27); the value is exact, and a wrong form still fails."""
    field = Field("t", "t", pa.timestamp("us"), convert="timestamp")
    table = records_to_table([{"t": "2023-06-27T13:46"}, {"t": "2023-06-27T13:46:05.123"}], (field,), artifact="x")
    assert table.column("t").to_pylist() == [
        dt.datetime(2023, 6, 27, 13, 46),
        dt.datetime(2023, 6, 27, 13, 46, 5, 123000),
    ]
    for text in ("2023-06-27T13", "2023-06-27T1:46", "2023-06-27 13:46"):
        with pytest.raises((InexactValueError, ValueError)):
            records_to_table([{"t": text}], (field,), artifact="x")
