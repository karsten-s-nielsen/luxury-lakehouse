"""Declarative XML-to-Arrow mapping with exactness checks (spec §12.1, §12.2, §12.4).

A row is one XML element. Its fields are relative paths: `@EventId` is an attribute of the row element, `Play@Team`
an attribute of its `Play` child, and `Play/Pass` the presence of a grandchild. Each attribute path has a kind, and
the column name follows from the path (`Play/Pass@Direction` becomes `play_pass_direction`), so the mapping is
declared once and cannot drift from the column names. A path that the mapping does not declare raises
`UnmappedFieldError`; an element that repeats under one parent, or that holds text, raises `UnexpectedPayloadError`,
because one row column cannot hold it.

Every kind has an exactness check on the source text: booleans are `true` or `false`, integers are plain digits,
floats must round-trip through float64, coordinates become exact `int32` millimetres (Task 4 rules), and timestamps
keep both the instant (UTC, milliseconds) and the UTC offset that the source wrote, so the text can be rebuilt.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final, Literal

import pyarrow as pa

from lakehouse.errors import InexactValueError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.exact import text_to_mm

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element  # the type only; parsing uses defusedxml

__all__ = [
    "XmlColumns",
    "XmlKind",
    "attribute_column",
    "convert",
    "element_values",
    "presence_column",
    "snake",
    "xml_field_paths",
]

XmlKind = Literal["str", "bool", "int", "float", "mm", "ts"]

_CAMEL: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_INT: Final = re.compile(r"^-?\d+$")
_FLOAT: Final = re.compile(r"^-?\d+(\.\d+)?$")
_TS: Final = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})([+-]\d{2}:\d{2})$")
_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1

_ARROW: Final[dict[XmlKind, pa.DataType]] = {
    "str": pa.string(),
    "bool": pa.bool_(),
    "int": pa.int64(),
    "float": pa.float64(),
    "mm": pa.int32(),
}
_KIND_TEXT: Final[dict[XmlKind, str]] = {
    "str": "as string.",
    "bool": "as bool (the source text is `true` or `false`).",
    "int": "as int64.",
    "float": "as float64; the source text round-trips exactly (spec §12.4).",
    "mm": "in metres in the source; stored as exact int32 millimetres (spec §10.2).",
    "ts": "an ISO 8601 time with a UTC offset; stored as the UTC instant in milliseconds.",
}
_TS_TYPE: Final = pa.timestamp("ms", tz="UTC")
_OFFSET_TYPE: Final = pa.int16()


def snake(name: str) -> str:
    """`X-PositionFromTracking` becomes `x_position_from_tracking`; `xG` becomes `x_g`."""
    return _CAMEL.sub("_", name.replace("-", "_")).lower()


def _path_name(elements: str, attribute: str) -> str:
    parts = [snake(part) for part in elements.split("/") if part]
    if attribute:
        parts.append(snake(attribute))
    return "_".join(parts)


def attribute_column(path: str, kind: XmlKind) -> tuple[str, ...]:
    """The column names of a relative attribute path: one, or two for a timestamp (instant and UTC offset)."""
    elements, _, attribute = path.partition("@")
    if not attribute:
        raise ValueError(f"{path!r} is not an attribute path (it has no '@')")
    name = _path_name(elements, attribute)
    if kind == "mm":
        return (f"{name}_mm",)
    if kind == "ts":
        return (name, f"{name}_utc_offset_min")
    return (name,)


def presence_column(path: str) -> str:
    """The boolean column that says whether a relative element path is present in a row."""
    return f"has_{_path_name(path, '')}"


def _bool(text: str) -> bool:
    if text == "true":
        return True
    if text == "false":
        return False
    raise InexactValueError(f"{text!r} is not a boolean (true or false)")


def _int(text: str) -> int:
    if not _INT.match(text):
        raise InexactValueError(f"{text!r} is not an integer")
    value = int(text)
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise InexactValueError(f"{text!r} is out of the int64 range")
    return value


def _float(text: str) -> float:
    if not _FLOAT.match(text):
        raise InexactValueError(f"{text!r} is not a plain decimal number")
    value = float(text)
    if Decimal(repr(value)) != Decimal(text):
        raise InexactValueError(f"{text!r} has more digits than a float64 holds")
    return value


def _timestamp(text: str) -> tuple[dt.datetime, int]:
    match = _TS.match(text)
    if match is None:
        raise InexactValueError(f"{text!r} is not an ISO 8601 time with milliseconds and a UTC offset")
    try:
        value = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise InexactValueError(f"{text!r} is not a valid time: {exc}") from exc
    offset = value.utcoffset()
    if offset is None:  # the pattern requires an offset; this guards the type only
        raise InexactValueError(f"{text!r} has no UTC offset")
    minutes = int(offset.total_seconds()) // 60
    sign, hours, rest = ("-" if minutes < 0 else "+"), abs(minutes) // 60, abs(minutes) % 60
    back = f"{value:%Y-%m-%dT%H:%M:%S}.{value.microsecond // 1000:03d}{sign}{hours:02d}:{rest:02d}"
    if back != text or value.microsecond % 1000:
        raise InexactValueError(f"{text!r} does not round-trip ({back!r})")
    return value.astimezone(dt.UTC), minutes


def convert(text: str, kind: XmlKind) -> object:
    """The typed value of one attribute text; `ts` gives `(instant in UTC, UTC offset in minutes)`."""
    if kind == "str":
        return text
    if kind == "bool":
        return _bool(text)
    if kind == "int":
        return _int(text)
    if kind == "float":
        return _float(text)
    if kind == "mm":
        return text_to_mm(text)
    return _timestamp(text)


def _has_text(value: str | None) -> bool:
    return value is not None and value.strip() != ""


def xml_field_paths(element: Element, prefix: str) -> frozenset[str]:
    """Every element and attribute path under `element`, absolute (`<prefix>/<tag>@<attribute>`). Text content is
    the field `<path>#text`, so free text can never be dropped silently."""
    found: set[str] = set()

    def walk(node: Element, parent: str) -> None:
        here = f"{parent}/{node.tag}" if parent else node.tag
        found.add(here)
        found.update(f"{here}@{name}" for name in node.attrib)
        if _has_text(node.text) or any(_has_text(child.tail) for child in node):
            found.add(f"{here}#text")
        for child in node:
            walk(child, here)

    walk(element, prefix)
    return frozenset(found)


def element_values(element: Element) -> dict[str, str]:
    """The attribute texts of `element` and its descendants by relative path, plus `""` for each descendant
    element. A repeated child tag or text content raises `UnexpectedPayloadError`."""
    values: dict[str, str] = {}

    def walk(node: Element, here: str) -> None:
        if _has_text(node.text) or any(_has_text(child.tail) for child in node):
            raise UnexpectedPayloadError(f"<{node.tag}> holds text, which no column maps")
        for name, text in node.attrib.items():
            values[f"{here}@{name}"] = text
        seen: set[str] = set()
        for child in node:
            if child.tag in seen:
                raise UnexpectedPayloadError(f"<{node.tag}> has more than one <{child.tag}> child; one row holds one")
            seen.add(child.tag)
            path = f"{here}/{child.tag}" if here else child.tag
            values[path] = ""
            walk(child, path)

    walk(element, "")
    return values


@dataclass(frozen=True)
class XmlColumns:
    """The columns of one kind of row. `attributes` maps relative attribute paths to kinds, `presence` lists the
    relative element paths that get a `has_<path>` column, and `elements` lists element paths that are mapped
    without a column (for example the event type element, whose tag is a column of its own)."""

    attributes: Mapping[str, XmlKind]
    presence: tuple[str, ...] = ()
    elements: tuple[str, ...] = ()
    _names: tuple[str, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        names: list[str] = []
        for path, kind in self.attributes.items():
            names.extend(attribute_column(path, kind))
        names.extend(presence_column(path) for path in self.presence)
        seen: set[str] = set()
        for name in names:
            if name in seen:
                raise ValueError(f"two source paths map to the column {name}")
            seen.add(name)
        object.__setattr__(self, "_names", tuple(names))

    @property
    def names(self) -> tuple[str, ...]:
        return self._names

    def describe(self, base: str) -> list[tuple[str, str]]:
        """`(column, description)` for every column, in schema order; `base` is the absolute path of the row
        element, so each description names the full source path (spec §12.8)."""
        docs: list[tuple[str, str]] = []
        for path, kind in self.attributes.items():
            source = f"`{base}{path}`" if path.startswith("@") else f"`{base}/{path}`"
            names = attribute_column(path, kind)
            docs.append((names[0], f"Source attribute {source}, {_KIND_TEXT[kind]}"))
            if kind == "ts":
                docs.append((names[1], f"The UTC offset in minutes that {source} was written with."))
        docs.extend(
            (presence_column(path), f"Whether the element `{base}/{path}` is present in the row.")
            for path in self.presence
        )
        return docs

    def mapped(self) -> frozenset[str]:
        """The relative paths that this mapping holds."""
        return frozenset(self.attributes) | frozenset(self.presence) | frozenset(self.elements)

    def schema(self) -> pa.Schema:
        fields: list[pa.Field[Any]] = []
        for path, kind in self.attributes.items():
            names = attribute_column(path, kind)
            if kind == "ts":
                fields.extend([pa.field(names[0], _TS_TYPE), pa.field(names[1], _OFFSET_TYPE)])
            else:
                fields.append(pa.field(names[0], _ARROW[kind]))
        fields.extend(pa.field(presence_column(path), pa.bool_()) for path in self.presence)
        return pa.schema(fields)

    def table(self, rows: Sequence[Mapping[str, str]], *, artifact: str) -> pa.Table:
        """One table row per input row (relative path to text), after the inventory check. Missing values are nulls."""
        mapped = self.mapped()
        unmapped = sorted({path for row in rows for path in row if path not in mapped})
        if unmapped:
            raise UnmappedFieldError(artifact, unmapped)
        arrays: list[pa.Array[Any]] = []
        for path, kind in self.attributes.items():
            values: list[object] = []
            for row in rows:
                text = row.get(path)
                if text is None:
                    values.append(None)
                    continue
                try:
                    values.append(convert(text, kind))
                except InexactValueError as exc:
                    raise InexactValueError(f"{artifact}: {path}: {exc}") from None
            if kind == "ts":
                pairs: list[tuple[dt.datetime, int] | None] = values  # type: ignore[assignment]
                arrays.append(pa.array([p[0] if p else None for p in pairs], type=_TS_TYPE))
                arrays.append(pa.array([p[1] if p else None for p in pairs], type=_OFFSET_TYPE))
            else:
                arrays.append(pa.array(values, type=_ARROW[kind]))
        for path in self.presence:
            arrays.append(pa.array([path in row for row in rows], type=pa.bool_()))
        return pa.Table.from_arrays(arrays, schema=self.schema())
