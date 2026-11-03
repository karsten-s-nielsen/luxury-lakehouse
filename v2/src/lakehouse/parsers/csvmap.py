"""Declarative CSV-to-Arrow mapping with exactness checks (spec §12.1, §12.2, §12.4).

A CSV file's field inventory is its header. Every header name must be a declared column; an undeclared name raises
`UnmappedFieldError`, a repeated name or a record with the wrong number of fields raises `UnexpectedPayloadError`.
An empty field is a null (CSV cannot tell an empty text from a missing value). Each kind checks the source text:
booleans are `True` or `False` (the SkillCorner spelling), integers are plain digits, floats round-trip through
float64, coordinates become exact `int32` millimetres (column name `<name>_mm`), and `clock_ds` is a `MM:SS.d` match
clock with tenths of a second.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import pyarrow as pa

from lakehouse.errors import InexactValueError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.exact import float_to_mm
from lakehouse.parsers.xmlmap import convert

__all__ = ["CsvColumns", "CsvKind", "convert_csv", "convert_typed", "csv_header"]

CsvKind = Literal["str", "bool", "int", "float", "mm", "clock_ds"]

_CLOCK_DS: Final = re.compile(r"^(\d{2,3}):(\d{2})\.(\d)$")
_ARROW: Final[dict[CsvKind, pa.DataType]] = {
    "str": pa.string(),
    "bool": pa.bool_(),
    "int": pa.int64(),
    "float": pa.float64(),
    "mm": pa.int32(),
    "clock_ds": pa.duration("ms"),
}


_KIND_TEXT: Final[dict[CsvKind, str]] = {
    "str": "as string.",
    "bool": "as bool (the source text is `True` or `False`).",
    "int": "as int64.",
    "float": "as float64; the source text round-trips exactly (spec §12.4).",
    "mm": "in metres in the source; stored as exact int32 millimetres (spec §10.2).",
    "clock_ds": "a `MM:SS.d` match clock; stored as a duration in milliseconds.",
}


def _clock_ds(text: str) -> dt.timedelta:
    match = _CLOCK_DS.match(text)
    if match is None:
        raise InexactValueError(f"the clock {text!r} is not in MM:SS.d form")
    minutes, seconds, tenths = (int(part) for part in match.groups())
    if seconds > 59:
        raise InexactValueError(f"the clock {text!r} is out of range")
    return dt.timedelta(minutes=minutes, seconds=seconds, milliseconds=100 * tenths)


def convert_csv(text: str, kind: CsvKind) -> object:
    """The typed value of one non-empty CSV field."""
    if kind == "bool":
        if text == "True":
            return True
        if text == "False":
            return False
        raise InexactValueError(f"{text!r} is not a boolean (True or False)")
    if kind == "clock_ds":
        return _clock_ds(text)
    return convert(text, kind)


def convert_typed(value: object, kind: CsvKind) -> object:
    """The typed value of one non-null value of a typed source (a Parquet form of the same file), with the same
    result as the CSV text would give: a float NaN is a null (Task 4), a float in an integer column must be integral,
    a coordinate float must be exact in millimetres, and a float or a boolean never stands for text."""
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, str):
        return convert_csv(value, kind)
    if kind == "bool" and isinstance(value, bool):
        return value
    if isinstance(value, bool):
        raise InexactValueError(f"{value!r} is a boolean, not {kind}")
    if kind == "str" and isinstance(value, int):
        return str(value)
    if kind == "int" and isinstance(value, int | float):
        if float(value) != int(value):
            raise InexactValueError(f"{value!r} is not an integer")
        return int(value)
    if kind == "float" and isinstance(value, int | float):
        return float(value)
    if kind == "mm" and isinstance(value, int | float):
        return float_to_mm(float(value), "float64")
    raise InexactValueError(f"{value!r} ({type(value).__name__}) cannot be read as {kind}")


def _column_name(name: str, kind: CsvKind) -> str:
    return f"{name}_mm" if kind == "mm" else name


def _rows(decoded: bytes, artifact: str) -> tuple[list[str], list[list[str]]]:
    try:
        text = decoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnexpectedPayloadError(f"{artifact} is not UTF-8 text: {exc}") from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if header is None:
        raise UnexpectedPayloadError(f"{artifact} has no header")
    seen: set[str] = set()
    for name in header:
        if name in seen:
            raise UnexpectedPayloadError(f"{artifact}: the column {name!r} appears twice in the header")
        seen.add(name)
    rows: list[list[str]] = []
    for number, row in enumerate(reader, start=2):
        if len(row) != len(header):
            raise UnexpectedPayloadError(
                f"{artifact}: record {number} has {len(row)} fields, the header has {len(header)}"
            )
        rows.append(row)
    return header, rows


def csv_header(decoded: bytes) -> frozenset[str]:
    """The field inventory of a CSV file: its header names."""
    return frozenset(_rows(decoded, "csv")[0])


@dataclass(frozen=True)
class CsvColumns:
    """The columns of a CSV artifact: source column name to kind, in source order."""

    kinds: Mapping[str, CsvKind]
    _names: tuple[str, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        names = [_column_name(name, kind) for name, kind in self.kinds.items()]
        seen: set[str] = set()
        for name in names:
            if name in seen:
                raise ValueError(f"two source columns map to the column {name}")
            seen.add(name)
        object.__setattr__(self, "_names", tuple(names))

    @property
    def names(self) -> tuple[str, ...]:
        return self._names

    def describe(self) -> list[tuple[str, str]]:
        """`(column, description)` for every column, in schema order (spec §12.8)."""
        return [
            (_column_name(name, kind), f"Source column `{name}`, {_KIND_TEXT[kind]}")
            for name, kind in self.kinds.items()
        ]

    def schema(self) -> pa.Schema:
        return pa.schema([pa.field(_column_name(name, kind), _ARROW[kind]) for name, kind in self.kinds.items()])

    def table(self, decoded: bytes, *, artifact: str) -> pa.Table:
        header, rows = _rows(decoded, artifact)
        unmapped = sorted(name for name in header if name not in self.kinds)
        if unmapped:
            raise UnmappedFieldError(artifact, unmapped)
        position = {name: index for index, name in enumerate(header)}
        arrays: list[pa.Array[Any]] = []
        for name, kind in self.kinds.items():
            index = position.get(name)
            values: list[object] = []
            for number, row in enumerate(rows, start=2):
                text = row[index] if index is not None else ""
                if text == "":
                    values.append(None)
                    continue
                try:
                    values.append(convert_csv(text, kind))
                except InexactValueError as exc:
                    raise InexactValueError(f"{artifact}: record {number}: {name}: {exc}") from None
            arrays.append(pa.array(values, type=_ARROW[kind]))
        return pa.Table.from_arrays(arrays, schema=self.schema())

    def arrow_table(self, source: pa.Table, *, artifact: str) -> pa.Table:
        """The same columns from a typed form of the file (for example its Parquet form): every source column must
        be declared, and every value converts as its CSV text would (`convert_typed`)."""
        unmapped = sorted(name for name in source.column_names if name not in self.kinds)
        if unmapped:
            raise UnmappedFieldError(artifact, unmapped)
        arrays: list[pa.Array[Any]] = []
        for name, kind in self.kinds.items():
            if name not in source.column_names:
                arrays.append(pa.array([None] * source.num_rows, type=_ARROW[kind]))
                continue
            values: list[object] = []
            for number, value in enumerate(source.column(name).to_pylist(), start=1):
                try:
                    values.append(None if value is None else convert_typed(value, kind))
                except InexactValueError as exc:
                    raise InexactValueError(f"{artifact}: row {number}: {name}: {exc}") from None
            arrays.append(pa.array(values, type=_ARROW[kind]))
        return pa.Table.from_arrays(arrays, schema=self.schema())
