"""Typed seams over the pyarrow calls whose pyarrow-stubs 20 types are incomplete for pyarrow 26.

The stubs leave `Schema` fields, `Table.sort_by` and `parquet.write_table` partially unknown, and they type
`write_table(use_dictionary=...)` as `bool`, although pyarrow takes a list of column names. Each ignore below names
that gap. Everything else in v2 calls these functions, so the gaps stay in one place.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa
import pyarrow.parquet as pq

__all__ = [
    "SortOrder",
    "column_types",
    "list_value_type",
    "read_parquet",
    "read_parquet_bytes",
    "schema_field",
    "sort_table",
    "struct_children",
    "write_parquet",
]

SortOrder = Literal["ascending", "descending"]


def schema_field(schema: pa.Schema, name: str) -> pa.Field[Any]:
    """One field of a schema, with its nullability."""
    found: Any = schema.field(name)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: Field[Unknown]
    return cast("pa.Field[Any]", found)


def column_types(schema: pa.Schema) -> dict[str, pa.DataType]:
    """Each column name with its arrow type."""
    result: dict[str, pa.DataType] = {}
    for name in schema.names:
        field: Any = schema.field(name)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: Field[Unknown]
        result[name] = field.type
    return result


def struct_children(arrow_type: pa.DataType) -> list[tuple[str, pa.DataType]]:
    """The fields of a struct type, as (name, type) pairs."""
    struct: Any = arrow_type
    return [(str(struct.field(i).name), struct.field(i).type) for i in range(int(struct.num_fields))]


def list_value_type(arrow_type: pa.DataType) -> pa.DataType:
    """The item type of a list type."""
    list_type: Any = arrow_type
    value: pa.DataType = list_type.value_type
    return value


def sort_table(table: pa.Table, keys: list[tuple[str, SortOrder]]) -> pa.Table:
    sorter: Any = table.sort_by  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: sort_by partially unknown
    return sorter(keys)


def write_parquet(
    table: pa.Table,
    path: Path,
    *,
    compression_level: int,
    use_dictionary: bool | list[str] = True,
    column_encoding: dict[str, str] | None = None,
) -> None:
    """`parquet.write_table` with zstd and statistics; `use_dictionary` may name columns."""
    writer: Any = pq.write_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: use_dictionary typed as bool
    writer(
        table,
        path,
        compression="zstd",
        compression_level=compression_level,
        use_dictionary=use_dictionary,
        column_encoding=column_encoding,
        write_statistics=True,
    )


def read_parquet(path: Path) -> pa.Table:
    reader: Any = pq.read_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: read_table partially unknown
    table: pa.Table = reader(path)
    return table


def read_parquet_bytes(data: bytes) -> pa.Table:
    """A Parquet file held in memory (a delivery's bytes)."""
    reader: Any = pq.read_table  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] - stubs: read_table partially unknown
    table: pa.Table = reader(pa.BufferReader(data))
    return table
