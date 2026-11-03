"""Field inventories: every raw field path of a payload, checked against the bronze columns (spec §12).

A JSON path joins object keys with `.` and marks a list level with `[]`, so `freeze_frame[].location` is the
`location` of each element of `freeze_frame`. A list of scalars is one field (`location`). The inventory of a payload
must be a subset of the parser's mapped columns; anything else raises `UnmappedFieldError`, so a new source field
can never be dropped silently.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from lakehouse.errors import UnmappedFieldError

__all__ = ["check_mapped", "json_field_paths", "unmapped_fields"]


def _walk(value: object, prefix: str, out: set[str]) -> None:
    if isinstance(value, Mapping):
        for key, item in cast("Mapping[str, object]", value).items():
            _walk(item, f"{prefix}.{key}" if prefix else str(key), out)
    elif isinstance(value, list):
        items = cast("Sequence[object]", value)
        nested: list[object] = [item for item in items if isinstance(item, (Mapping, list))]
        if not nested:
            if prefix:
                out.add(prefix)  # a list of scalars (for example a location) is one field
            return
        for item in nested:
            _walk(item, f"{prefix}[]" if prefix else "[]", out)
    elif prefix:
        out.add(prefix)


def json_field_paths(payload: object) -> frozenset[str]:
    """Every field path of a parsed JSON payload; a top-level list is walked element by element."""
    found: set[str] = set()
    if isinstance(payload, list):
        for item in cast("Sequence[object]", payload):
            _walk(item, "", found)
    else:
        _walk(payload, "", found)
    return frozenset(found)


def _list_marks(path: str) -> list[int]:
    """The position of every `[]` in a path."""
    marks: list[int] = []
    start = path.find("[]")
    while start >= 0:
        marks.append(start)
        start = path.find("[]", start + 2)
    return marks


def unmapped_fields(inventory: frozenset[str], columns: frozenset[str]) -> list[str]:
    """The inventory fields that no bronze column maps, sorted. An empty list reports its own path; it is mapped
    when a column maps the sub-fields of its items (`cards` by `cards[].time`). A null in place of an object reports
    the object's path; it is mapped when columns map the object's fields (`country` by `country.id`); a scalar there
    fails when the columns are read (`jsonmap`)."""
    item_lists = {column[:index] for column in columns for index in _list_marks(column)}
    objects = {column[:index] for column in columns for index, char in enumerate(column) if char == "."}
    return sorted(path for path in inventory - columns if path not in item_lists and path not in objects)


def check_mapped(artifact: str, inventory: frozenset[str], columns: frozenset[str]) -> None:
    """Raise `UnmappedFieldError` naming every inventory field that no bronze column maps."""
    unmapped = unmapped_fields(inventory, columns)
    if unmapped:
        raise UnmappedFieldError(artifact, unmapped)
