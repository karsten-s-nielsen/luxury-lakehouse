"""Declarative JSON-to-Arrow mapping with exactness checks (spec §12.1, §12.2, §12.4).

Each bronze column is a `Field`: a source path, an Arrow type and a converter. The source paths a field covers follow
from its type, so a struct or a list of structs covers its sub-fields too. `records_to_table` first checks the
payload's field inventory against the covered paths (an unmapped field raises `UnmappedFieldError`, because Arrow
would drop an unknown struct key without a word), then converts every value. Every conversion has an exactness check:
`loads_exact` refuses a JSON number whose text a float64 cannot reproduce, and each text converter formats its
result back and compares it with the source text.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal, cast

import pyarrow as pa

from lakehouse.bronze.arrow import list_value_type, struct_children
from lakehouse.errors import InexactValueError, UnexpectedPayloadError
from lakehouse.exact import loads_exact
from lakehouse.parsers.inventory import check_mapped, json_field_paths

__all__ = [
    "Convert",
    "Field",
    "convert_text",
    "covered_paths",
    "describe_fields",
    "loads_exact",
    "mapped_paths",
    "records_to_table",
]

Convert = Literal[
    "plain",
    "date",
    "timestamp",
    "timestamp_utc",
    "timestamp_utc_space",
    "time_ms",
    "clock_ms",
    "clock_s",
    "clock_ms_or_s",
    "clock_hms",
    "text",
    "int_text",
]

_CLOCK_MS: Final = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{3})$")
_MINUTES: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
_CLOCK_S: Final = re.compile(r"^(\d{2,3}):(\d{2})$")
_CLOCK_HMS: Final = re.compile(r"^(\d{2}):(\d{2}):(\d{2})$")
_UTC_SPACE: Final = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
_INT_TEXT: Final = re.compile(r"^(0|-?[1-9]\d*)$")
_UTC: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$")
_FRACTION: Final = re.compile(r"\.(\d+)")


@dataclass(frozen=True)
class Field:
    """One bronze column: `path` in the payload (keys joined by `.`), its Arrow type, and its converter. `nested`
    gives converters for sub-paths inside a list or struct value, for example `{"[].from": "clock_s"}`."""

    column: str
    path: str
    type: pa.DataType
    convert: Convert = "plain"
    nested: Mapping[str, Convert] = field(default_factory=dict[str, Convert])


def _type_paths(prefix: str, arrow_type: pa.DataType) -> set[str]:
    if pa.types.is_struct(arrow_type):
        # a null in place of the object is a value of the struct column (a list item has no path of its own)
        found: set[str] = set() if prefix.endswith("[]") else {prefix}
        for name, child in struct_children(arrow_type):
            found |= _type_paths(f"{prefix}.{name}", child)
        return found
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        value_type = list_value_type(arrow_type)
        if pa.types.is_struct(value_type) or pa.types.is_list(value_type):
            return _type_paths(f"{prefix}[]", value_type)
        return {prefix}
    return {prefix}


def covered_paths(column: Field) -> frozenset[str]:
    """The payload field paths that this column holds (inventory syntax of `json_field_paths`)."""
    return frozenset(_type_paths(column.path, column.type))


def describe_fields(fields: Sequence[Field], *, prefix: str = "") -> list[tuple[str, str]]:
    """`(column, description)` for each field (spec §12.8); `prefix` is prepended to each source path."""
    docs: list[tuple[str, str]] = []
    for column in fields:
        arrow = str(column.type).replace("list<item: ", "list<").replace("element: ", "")
        text = f"Source field `{prefix}{column.path}`, as {arrow}."
        if column.convert != "plain":
            text += f" Converted from text ({column.convert}) with a round-trip check (spec §12.4)."
        docs.append((column.column, text))
    return docs


def mapped_paths(fields: Sequence[Field]) -> frozenset[str]:
    covered: set[str] = set()
    for column in fields:
        covered |= covered_paths(column)
    return frozenset(covered)


def _date(text: str) -> dt.date:
    value = dt.date.fromisoformat(text)
    if value.isoformat() != text:
        raise InexactValueError(f"the date {text!r} is not in YYYY-MM-DD form")
    return value


def _fraction_digits(text: str) -> int:
    match = _FRACTION.search(text)
    return len(match.group(1)) if match else 0


def _timestamp(text: str) -> dt.datetime:
    digits = _fraction_digits(text)
    if digits > 6:
        raise InexactValueError(f"the timestamp {text!r} has more than microsecond precision")
    value = dt.datetime.fromisoformat(text)
    if value.tzinfo is not None:
        raise InexactValueError(f"the timestamp {text!r} has a time zone; this column holds local times")
    if _MINUTES.match(text):  # to the minute, as StatsBomb writes some `last_updated_360` values
        back = value.strftime("%Y-%m-%dT%H:%M")
    else:
        back = value.strftime("%Y-%m-%dT%H:%M:%S") + (f".{value.microsecond:06d}"[: digits + 1] if digits else "")
    if back != text:
        raise InexactValueError(f"the timestamp {text!r} does not round-trip ({back!r})")
    return value


def _time_ms(text: str) -> dt.time:
    value = dt.time.fromisoformat(text)
    back = value.strftime("%H:%M:%S") + f".{value.microsecond // 1000:03d}"
    if back != text or value.microsecond % 1000:
        raise InexactValueError(f"the time {text!r} is not in HH:MM:SS.fff form")
    return value


def _clock_ms(text: str) -> dt.timedelta:
    match = _CLOCK_MS.match(text)
    if match is None:
        raise InexactValueError(f"the clock {text!r} is not in HH:MM:SS.fff form")
    hours, minutes, seconds, millis = (int(part) for part in match.groups())
    if minutes > 59 or seconds > 59:
        raise InexactValueError(f"the clock {text!r} is out of range")
    return dt.timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis)


def _clock_s(text: str) -> dt.timedelta:
    match = _CLOCK_S.match(text)
    if match is None:
        raise InexactValueError(f"the clock {text!r} is not in MM:SS form")
    minutes, seconds = (int(part) for part in match.groups())
    if seconds > 59:
        raise InexactValueError(f"the clock {text!r} is out of range")
    return dt.timedelta(minutes=minutes, seconds=seconds)


def _clock_ms_or_s(text: str) -> dt.timedelta:
    """`HH:MM:SS.fff` or `MM:SS`, each checked exactly in its own form (StatsBomb position times: commercial and
    open-data deliveries write the two forms)."""
    return _clock_ms(text) if _CLOCK_MS.match(text) else _clock_s(text)


def _clock_hms(text: str) -> dt.timedelta:
    match = _CLOCK_HMS.match(text)
    if match is None:
        raise InexactValueError(f"the clock {text!r} is not in HH:MM:SS form")
    hours, minutes, seconds = (int(part) for part in match.groups())
    if minutes > 59 or seconds > 59:
        raise InexactValueError(f"the clock {text!r} is out of range")
    return dt.timedelta(hours=hours, minutes=minutes, seconds=seconds)


def _timestamp_utc(text: str) -> dt.datetime:
    """An ISO 8601 UTC time written with `Z`, up to microseconds. The pattern fixes every digit position and
    `fromisoformat` refuses an invalid date or time, so the stored instant holds exactly the written digits."""
    if not _UTC.match(text):
        raise InexactValueError(f"the time {text!r} is not an ISO 8601 UTC time ending in Z")
    return dt.datetime.fromisoformat(text).astimezone(dt.UTC)


def _timestamp_utc_space(text: str) -> dt.datetime:
    """A UTC time written `YYYY-MM-DD hh:mm:ss` (Wyscout `dateutc`)."""
    if not _UTC_SPACE.match(text):
        raise InexactValueError(f"the time {text!r} is not in YYYY-MM-DD hh:mm:ss form")
    return dt.datetime.fromisoformat(text.replace(" ", "T")).replace(tzinfo=dt.UTC)


def _int_text(text: str) -> int:
    """An integer written as text; leading zeros and signs other than one minus are refused (no exact round trip)."""
    if not _INT_TEXT.match(text):
        raise InexactValueError(f"{text!r} is not an integer written as text")
    return int(text)


_CONVERTERS: Final[dict[Convert, Any]] = {
    "date": _date,
    "timestamp": _timestamp,
    "time_ms": _time_ms,
    "clock_ms": _clock_ms,
    "clock_s": _clock_s,
    "clock_ms_or_s": _clock_ms_or_s,
    "clock_hms": _clock_hms,
    "timestamp_utc": _timestamp_utc,
    "timestamp_utc_space": _timestamp_utc_space,
    "int_text": _int_text,
}


def _convert(value: object, convert: Convert) -> object:
    if value is None or convert == "plain":
        return value
    if convert == "text":  # an id that is an int in one file and text in another
        if isinstance(value, bool) or not isinstance(value, int | str):
            raise InexactValueError(f"a text value must be an int or a string, not {type(value).__name__}")
        return str(value)
    if not isinstance(value, str):
        raise InexactValueError(f"a {convert} value must be text, not {type(value).__name__}")
    try:
        return _CONVERTERS[convert](value)
    except InexactValueError:
        raise
    except ValueError as exc:
        raise InexactValueError(f"the {convert} text {value!r} is malformed: {exc}") from exc


def convert_text(value: object, convert: Convert) -> object:
    """One value through a converter (`plain` passes it through); the public form of the column converters."""
    return _convert(value, convert)


def _convert_nested(value: object, path: str, convert: Convert) -> object:
    """Apply `convert` at `path` (`[]` for list items, `.key` for keys) inside a nested value."""
    if value is None:
        return None
    if not path:
        return _convert(value, convert)
    if path.startswith("[]"):
        if not isinstance(value, list):
            return value
        return [_convert_nested(item, path[2:], convert) for item in cast("list[object]", value)]
    if path.startswith("."):
        key, _, rest = path[1:].partition(".")
        bracket = key.find("[")
        if bracket >= 0:
            key, rest = key[:bracket], key[bracket:] + ("." + rest if rest else "")
        elif rest:
            rest = "." + rest
        if not isinstance(value, Mapping):
            return value
        mapping = dict(cast("Mapping[str, object]", value))
        if key in mapping:
            mapping[key] = _convert_nested(mapping[key], rest, convert)
        return mapping
    raise ValueError(f"invalid nested path {path!r}: it must start with '[]' or '.'")


def _get(record: Mapping[str, object], path: str) -> object:
    """The value at a dotted path; a missing key or a null object on the way gives null, and a scalar on the way
    fails (it would otherwise read as nulls)."""
    value: object = record
    keys = path.split(".")
    for depth, key in enumerate(keys):
        if value is None:
            return None
        if not isinstance(value, Mapping):
            where = ".".join(keys[:depth])
            raise UnexpectedPayloadError(f"{where} holds {type(value).__name__}, not the object of {path}")
        mapping: Mapping[str, object] = value  # pyright: ignore[reportUnknownVariableType] - JSON objects
        value = mapping.get(key)
    return value


def records_to_table(records: Sequence[Mapping[str, object]], fields: Sequence[Field], *, artifact: str) -> pa.Table:
    """A table with one column per field, after the inventory check. Missing values are nulls."""
    check_mapped(artifact, json_field_paths(list(records)), mapped_paths(fields))
    arrays: list[pa.Array[Any]] = []
    for column in fields:
        values: list[object] = []
        for record in records:
            value = _convert(_get(record, column.path), column.convert)
            for sub_path, convert in column.nested.items():
                value = _convert_nested(value, sub_path, convert)
            values.append(value)
        arrays.append(pa.array(values, type=column.type))
    return pa.Table.from_arrays(arrays, names=[column.column for column in fields])
