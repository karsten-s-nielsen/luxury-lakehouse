"""The declarative XML-to-Arrow mapping (spec §12.1, §12.2, §12.4; plan Task 16)."""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest
from defusedxml.ElementTree import fromstring

from lakehouse.bronze.arrow import column_types
from lakehouse.errors import InexactValueError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.parsers.xmlmap import XmlColumns, convert, element_values, snake, xml_field_paths

EVENT = (
    '<Event EventId="7" X-Position="0.57" EventTime="2022-08-26T18:33:34.933+02:00">'
    '<Play Team="DFL-CLU-000005" FlatCross="false"><Pass Direction="backPass"/></Play></Event>'
)


def test_snake_names() -> None:
    assert snake("X-PositionFromTracking") == "x_position_from_tracking"
    assert snake("DlProviderId") == "dl_provider_id"
    assert snake("BallPossession") == "ball_possession"
    assert snake("xG") == "x_g"
    assert snake("RefereeinRRA") == "refereein_rra"
    assert snake("N") == "n"


def test_converters_are_exact() -> None:
    assert convert("true", "bool") is True
    assert convert("false", "bool") is False
    assert convert("-12", "int") == -12
    assert convert("0.0433", "float") == 0.0433
    assert convert("0.57", "mm") == 570
    assert convert("hello", "str") == "hello"
    assert convert("2022-10-15T11:01:29.320+00:00", "ts") == (dt.datetime(2022, 10, 15, 11, 1, 29, 320000, dt.UTC), 0)
    assert convert("2022-08-26T18:33:34.933+02:00", "ts") == (dt.datetime(2022, 8, 26, 16, 33, 34, 933000, dt.UTC), 120)
    west = dt.datetime(2022, 11, 11, 21, 52, 44, 680000, dt.UTC)
    assert convert("2022-11-11T20:22:44.680-01:30", "ts") == (west, -90)


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        ("True", "bool"),
        ("1", "bool"),
        ("1.0", "int"),
        ("99999999999999999999", "int"),
        ("1e3", "float"),
        ("nan", "float"),
        ("0.1000000000000000055511151231257827", "float"),
        ("12.3456", "mm"),
        ("2022-10-15T11:01:29.3204+00:00", "ts"),
        ("2022-10-15T11:01:29.320", "ts"),
        ("2022-10-15 11:01:29.320+00:00", "ts"),
        ("2022-10-15T11:01:29.32+00:00", "ts"),
        ("2022-10-15T11:01:29.320-00:00", "ts"),  # parses as +00:00, so the text does not round-trip
    ],
)
def test_converters_refuse_inexact_text(text: str, kind: str) -> None:
    with pytest.raises(InexactValueError):
        convert(text, kind)  # type: ignore[arg-type]


def test_element_values_and_inventory() -> None:
    event = fromstring(EVENT)
    assert element_values(event) == {
        "@EventId": "7",
        "@X-Position": "0.57",
        "@EventTime": "2022-08-26T18:33:34.933+02:00",
        "Play": "",
        "Play@Team": "DFL-CLU-000005",
        "Play@FlatCross": "false",
        "Play/Pass": "",
        "Play/Pass@Direction": "backPass",
    }
    assert xml_field_paths(event, "Root") == frozenset(
        {
            "Root/Event",
            "Root/Event@EventId",
            "Root/Event@X-Position",
            "Root/Event@EventTime",
            "Root/Event/Play",
            "Root/Event/Play@Team",
            "Root/Event/Play@FlatCross",
            "Root/Event/Play/Pass",
            "Root/Event/Play/Pass@Direction",
        }
    )


def test_repeated_child_is_refused() -> None:
    with pytest.raises(UnexpectedPayloadError, match="Play"):
        element_values(fromstring('<Event><Play Team="a"/><Play Team="b"/></Event>'))


def test_text_content_is_inventoried() -> None:
    element = fromstring("<Event><Note>free text</Note></Event>")
    assert "Event/Note#text" in xml_field_paths(element, "")
    with pytest.raises(UnexpectedPayloadError, match="text"):
        element_values(element)


COLUMNS = XmlColumns(
    attributes={
        "@EventId": "int",
        "@X-Position": "mm",
        "@EventTime": "ts",
        "Play@Team": "str",
        "Play@FlatCross": "bool",
        "Play/Pass@Direction": "str",
        "Play/Cross@Side": "str",
    },
    presence=("Play/Pass", "Play/Cross"),
    elements=("Play",),
)


def test_schema_names_and_types() -> None:
    assert list(column_types(COLUMNS.schema()).items()) == [
        ("event_id", pa.int64()),
        ("x_position_mm", pa.int32()),
        ("event_time", pa.timestamp("ms", tz="UTC")),
        ("event_time_utc_offset_min", pa.int16()),
        ("play_team", pa.string()),
        ("play_flat_cross", pa.bool_()),
        ("play_pass_direction", pa.string()),
        ("play_cross_side", pa.string()),
        ("has_play_pass", pa.bool_()),
        ("has_play_cross", pa.bool_()),
    ]


def test_table_from_rows() -> None:
    rows = [element_values(fromstring(EVENT)), {"@EventId": "8"}]
    table = COLUMNS.table(rows, artifact="events")
    assert table.column("event_id").to_pylist() == [7, 8]
    assert table.column("x_position_mm").to_pylist() == [570, None]
    assert table.column("event_time_utc_offset_min").to_pylist() == [120, None]
    assert table.column("play_flat_cross").to_pylist() == [False, None]
    assert table.column("has_play_pass").to_pylist() == [True, False]
    assert table.column("has_play_cross").to_pylist() == [False, False]


def test_undeclared_path_fails_loudly() -> None:
    with pytest.raises(UnmappedFieldError, match=r"Play@Height"):
        COLUMNS.table([{"@EventId": "1", "Play": "", "Play@Height": "flat"}], artifact="events")
    with pytest.raises(UnmappedFieldError, match=r"Play/Shot"):
        COLUMNS.table([{"@EventId": "1", "Play": "", "Play/Shot": ""}], artifact="events")


def test_inexact_value_names_its_path() -> None:
    with pytest.raises(InexactValueError, match=r"@X-Position"):
        COLUMNS.table([{"@EventId": "1", "@X-Position": "1.2345"}], artifact="events")


def test_duplicate_column_names_are_refused() -> None:
    with pytest.raises(ValueError, match="play_team"):
        XmlColumns(attributes={"Play@Team": "str", "Play@team": "str"})
