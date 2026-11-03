"""The declarative CSV-to-Arrow mapping (spec §12.1, §12.2, §12.4; plan Task 17)."""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from lakehouse.bronze.arrow import column_types
from lakehouse.errors import InexactValueError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.parsers.csvmap import CsvColumns, convert_csv, convert_typed, csv_header

COLUMNS = CsvColumns(
    {
        "event_id": "str",
        "frame_start": "int",
        "time_start": "clock_ds",
        "x_start": "mm",
        "xthreat": "float",
        "penalty_area_start": "bool",
    }
)
DATA = (
    b"event_id,frame_start,time_start,x_start,xthreat,penalty_area_start\n"
    b"8_0,11,00:00.1,0.49,0.0007,False\n"
    b'"8_1, quoted",30,95:12.3,-20.78,,True\n'
)


def test_schema_names_and_types() -> None:
    assert list(column_types(COLUMNS.schema()).items()) == [
        ("event_id", pa.string()),
        ("frame_start", pa.int64()),
        ("time_start", pa.duration("ms")),
        ("x_start_mm", pa.int32()),
        ("xthreat", pa.float64()),
        ("penalty_area_start", pa.bool_()),
    ]


def test_table_from_csv() -> None:
    table = COLUMNS.table(DATA, artifact="dynamic_events")
    assert table.column("event_id").to_pylist() == ["8_0", "8_1, quoted"]
    assert table.column("frame_start").to_pylist() == [11, 30]
    assert table.column("time_start").to_pylist() == [
        dt.timedelta(milliseconds=100),
        dt.timedelta(minutes=95, seconds=12.3),
    ]
    assert table.column("x_start_mm").to_pylist() == [490, -20780]
    assert table.column("xthreat").to_pylist() == [0.0007, None]  # an empty field is a null
    assert table.column("penalty_area_start").to_pylist() == [False, True]
    assert csv_header(DATA) == frozenset(COLUMNS.kinds)


def test_unknown_column_fails_loudly() -> None:
    data = DATA.replace(b"penalty_area_start\n", b"penalty_area_start,new_metric\n")
    data = data.replace(b"False\n", b"False,1\n").replace(b"True\n", b"True,2\n")
    with pytest.raises(UnmappedFieldError, match="new_metric"):
        COLUMNS.table(data, artifact="dynamic_events")


def test_bad_rows_and_headers_fail() -> None:
    with pytest.raises(UnexpectedPayloadError, match="fields"):
        COLUMNS.table(DATA + b"9_0,1\n", artifact="dynamic_events")
    with pytest.raises(UnexpectedPayloadError, match="twice"):
        COLUMNS.table(b"event_id,event_id\na,b\n", artifact="dynamic_events")
    with pytest.raises(UnmappedFieldError):
        COLUMNS.table(b"\xef\xbb\xbf" + DATA, artifact="dynamic_events")  # a BOM makes the first name unknown


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        ("true", "bool"),
        ("1", "bool"),
        ("1.5", "int"),
        ("1e3", "float"),
        ("0.1234", "mm"),
        ("00:00.12", "clock_ds"),
        ("00:60.1", "clock_ds"),
        ("0:00.1", "clock_ds"),
    ],
)
def test_converters_refuse_inexact_text(text: str, kind: str) -> None:
    with pytest.raises(InexactValueError):
        convert_csv(text, kind)  # type: ignore[arg-type]


def test_duplicate_column_names_are_refused() -> None:
    with pytest.raises(ValueError, match="x_start_mm"):
        CsvColumns({"x_start": "mm", "x_start_mm": "int"})


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (float("nan"), "float", None),  # NaN is a missing value (Task 4)
        (float("nan"), "int", None),
        (2.0, "int", 2),
        (7, "str", "7"),
        (1.234, "mm", 1234),
        (12, "mm", 12000),
        (3, "float", 3.0),
        (True, "bool", True),
        ("01:02.3", "clock_ds", convert_csv("01:02.3", "clock_ds")),
    ],
)
def test_convert_typed_gives_what_the_text_gives(value: object, kind: str, expected: object) -> None:
    assert convert_typed(value, kind) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "kind"),
    [(2.5, "int"), (1.2345, "mm"), (1.5, "str"), (True, "int"), (False, "str"), (3, "clock_ds")],
)
def test_convert_typed_refuses_inexact_values(value: object, kind: str) -> None:
    with pytest.raises(InexactValueError):
        convert_typed(value, kind)  # type: ignore[arg-type]
