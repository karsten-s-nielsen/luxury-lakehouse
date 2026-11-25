"""Bronze stores no duration type; time is an explicit-unit integer (no-duration spec §2, §4 guards 1, 2, 4).

Guard 1 (declared): every parser field declaration, every CSV column set, the shared bronze schemas and the compute
bronze schemas hold no duration type, nested types included. Guard 2: every time-kind column carries its unit suffix,
and the code-built tracking time columns are declared. Guard 4: the tracking encoding lists name real core columns.
"""

# pyarrow's stubs leave nested DataType members untyped; relax the unknown-family rules (see compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterator

import pyarrow as pa
import pytest

import lakehouse.parsers as parsers_pkg
from lakehouse.bronze import schemas as bronze_schemas
from lakehouse.bronze import tracking
from lakehouse.compute.line_breaking import LINE_BREAKING_SCHEMA
from lakehouse.compute.pitch_control import PITCH_CONTROL_SCHEMA
from lakehouse.compute.shape_graphs import SHAPE_GRAPH_SCHEMA
from lakehouse.compute.spadl_actions import SPADL_ACTIONS_SCHEMA
from lakehouse.parsers import csvmap, jsonmap
from lakehouse.parsers.csvmap import CsvColumns
from lakehouse.parsers.jsonmap import TIME_KINDS, Field


def _durations(arrow_type: pa.DataType, path: str) -> list[str]:
    if pa.types.is_duration(arrow_type):
        return [path]
    if pa.types.is_struct(arrow_type):
        return [
            hit
            for i in range(arrow_type.num_fields)
            for hit in _durations(arrow_type.field(i).type, f"{path}.{arrow_type.field(i).name}")
        ]
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _durations(arrow_type.value_type, f"{path}[]")
    return []


def _parser_modules() -> Iterator[object]:
    for info in pkgutil.iter_modules(parsers_pkg.__path__):
        yield importlib.import_module(f"lakehouse.parsers.{info.name}")


def _declared_fields() -> list[tuple[str, Field]]:
    found: list[tuple[str, Field]] = []
    for module in _parser_modules():
        for name, value in vars(module).items():
            if isinstance(value, tuple) and value and all(isinstance(item, Field) for item in value):
                found += [(f"{module.__name__}.{name}", item) for item in value]  # type: ignore[attr-defined]
    return found


def _declared_csv() -> list[tuple[str, CsvColumns]]:
    return [
        (f"{module.__name__}.{name}", value)  # type: ignore[attr-defined]
        for module in _parser_modules()
        for name, value in vars(module).items()
        if isinstance(value, CsvColumns)
    ]


def test_no_duration_type_in_any_bronze_schema() -> None:
    hits: list[str] = []
    for where, column in _declared_fields():
        hits += _durations(column.type, f"{where}:{column.column}")
    for where, columns in _declared_csv():
        hits += [h for f in columns.schema() for h in _durations(f.type, f"{where}:{f.name}")]
    for name in dir(bronze_schemas):
        value = getattr(bronze_schemas, name)
        if isinstance(value, pa.Schema):
            hits += [h for f in value for h in _durations(f.type, f"schemas.{name}:{f.name}")]
    for name, schema in bronze_schemas.TRACKING_FRAMES_EXTRA.items():
        hits += [h for f in schema for h in _durations(f.type, f"TRACKING_FRAMES_EXTRA[{name}]:{f.name}")]
    for name, schema in (
        ("SPADL_ACTIONS_SCHEMA", SPADL_ACTIONS_SCHEMA),
        ("PITCH_CONTROL_SCHEMA", PITCH_CONTROL_SCHEMA),
        ("SHAPE_GRAPH_SCHEMA", SHAPE_GRAPH_SCHEMA),
        ("LINE_BREAKING_SCHEMA", LINE_BREAKING_SCHEMA),
    ):
        hits += [h for f in schema for h in _durations(f.type, f"{name}:{f.name}")]
    assert _declared_fields() and _declared_csv(), "the declaration scan found nothing"
    assert not hits, f"declared duration types: {hits}"


def test_time_kind_columns_carry_their_unit_suffix() -> None:
    bad: list[str] = []
    for where, column in _declared_fields():
        if column.convert in TIME_KINDS and not column.column.endswith(TIME_KINDS[column.convert]):
            bad.append(f"{where}:{column.column} ({column.convert})")
        for sub_path, convert in column.nested.items():
            if convert in TIME_KINDS:
                leaf = jsonmap.nested_output_name(sub_path, convert)
                if not leaf.endswith(TIME_KINDS[convert]):
                    bad.append(f"{where}:{column.column}{sub_path} -> {leaf}")
                assert leaf in _struct_names(column.type), f"{where}:{column.column} has no nested field {leaf}"
    for where, columns in _declared_csv():
        for name in columns.schema().names:
            kind = columns.kind_of(name)
            if kind in TIME_KINDS and not name.endswith(TIME_KINDS[kind]):
                bad.append(f"{where}:{name} ({kind})")
    assert not bad, f"time columns without their unit suffix: {bad}"


def _struct_names(arrow_type: pa.DataType) -> set[str]:
    names: set[str] = set()
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _struct_names(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        for i in range(arrow_type.num_fields):
            names.add(arrow_type.field(i).name)
            names |= _struct_names(arrow_type.field(i).type)
    return names


def test_code_built_tracking_time_columns_are_declared() -> None:
    core = bronze_schemas.TRACKING_FRAMES_CORE
    assert core.field("timestamp_us").type == pa.int64()
    assert "timestamp" not in core.names
    extra = bronze_schemas.TRACKING_FRAMES_EXTRA["skillcorner"]
    assert extra.field("match_clock_ms").type == pa.int64()
    without = pa.schema(list(core))
    with pytest.raises(ValueError, match="match_clock_ms"):
        tracking.check_tracking_schema("frames", without, provider="skillcorner")
    wrong = pa.schema([*core, pa.field("match_clock_ms", pa.float64())])
    with pytest.raises(ValueError, match="match_clock_ms"):
        tracking.check_tracking_schema("frames", wrong, provider="skillcorner")
    tracking.check_tracking_schema("frames", pa.schema([*core, *extra]), provider="skillcorner")
    tracking.check_tracking_schema("frames", core, provider="idsse")  # idsse declares no extension


def test_tracking_encoding_lists_name_core_columns() -> None:
    for kind in ("players", "frames"):
        names = set(tracking._CORE[kind].names)  # pyright: ignore[reportPrivateUsage]
        for listing in (tracking._SORT, tracking._DELTA, tracking._DICTIONARY):  # pyright: ignore[reportPrivateUsage]
            missing = set(listing[kind]) - names
            assert not missing, f"{kind}: encoding names that are not core columns: {missing}"


def test_time_values_keep_their_precision() -> None:
    assert jsonmap._convert("00:00:02.250", "clock_ms") == 2250  # pyright: ignore[reportPrivateUsage]
    assert jsonmap._convert("01:02:03", "clock_hms") == 3723  # pyright: ignore[reportPrivateUsage]
    assert jsonmap._convert("12:05", "clock_s") == 725  # pyright: ignore[reportPrivateUsage]
    assert jsonmap._convert("12:05", "clock_ms_or_s") == 725_000  # pyright: ignore[reportPrivateUsage]
    assert jsonmap._convert("00:12:05.500", "clock_ms_or_s") == 725_500  # pyright: ignore[reportPrivateUsage]
    assert csvmap.convert_csv("01:02.3", "clock_ds") == 62_300


def test_null_time_stays_null() -> None:
    assert jsonmap._convert(None, "clock_ms") is None  # pyright: ignore[reportPrivateUsage]
    table = CsvColumns({"a": "int", "time_end": "clock_ds"}).table(b"a,time_end\n1,\n", artifact="t")
    assert table.column("time_end_ms").type == pa.int64()
    assert table.column("time_end_ms").to_pylist() == [None]  # a blank clock is NULL, never 0
