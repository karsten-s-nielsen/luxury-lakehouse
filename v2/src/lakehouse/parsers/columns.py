"""Key and audit columns that every provider parser appends (spec §11.2, §14)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Final

import pyarrow as pa

from lakehouse.bronze.arrow import column_types
from lakehouse.bronze.schemas import AUDIT_COLUMNS

__all__ = ["append_columns", "constant", "key_array", "with_audit"]

_I64: Final = pa.int64()


def append_columns(table: pa.Table, columns: Mapping[str, pa.Array[Any]]) -> pa.Table:
    """The table with the columns appended; a name that the table already has fails."""
    for name, values in columns.items():
        if name in table.column_names:
            raise ValueError(f"the table already has a column {name!r}")
        table = table.append_column(name, values)
    return table


def constant(value: object, rows: int, arrow_type: pa.DataType) -> pa.Array[Any]:
    return pa.array([value] * rows, type=arrow_type)


def key_array(values: Sequence[object], make: Callable[[Any], int | None]) -> pa.Array[Any]:
    """A surrogate key per value; a missing value gets a null key."""
    return pa.array([None if value is None else make(value) for value in values], type=_I64)


def with_audit(table: pa.Table, *, run_id: str, source_hash: str, at: datetime) -> pa.Table:
    """Append `_ingested_at`, `_run_id` and `_source_hash` (the `content_sha256` of the delivery, §13.1)."""
    rows = table.num_rows
    values = {"_ingested_at": at, "_run_id": run_id, "_source_hash": source_hash}
    for name, arrow_type in column_types(AUDIT_COLUMNS).items():
        table = table.append_column(
            pa.field(name, arrow_type, nullable=False), constant(values[name], rows, arrow_type)
        )
    return table
