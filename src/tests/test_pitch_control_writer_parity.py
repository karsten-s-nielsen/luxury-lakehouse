"""ADR-002 §4 writer/DDL parity guard for the pitch-control pipeline.

PR 7 widens ``src/ingestion/pitch_control_batch.py`` to emit
``data_source STRING`` + ``match_key BIGINT`` natively, collapsing the PR 6
prefix-CASE bridge in ``stg_pitch_control__values`` to a passthrough.

``_RESULTS_SCHEMA`` is the canonical bronze DDL string consumed by
``ensure_table`` to create ``bronze.pitch_control_values``;
``_PITCH_CONTROL_BRONZE_COLS`` is the writer-side column tuple consumed by
``test_bronze_live_schema.py`` to assert live-table coverage. The
``output_schema`` StructType is the per-group UDF return shape handed to
``applyInPandas``. Drift between any of the three would silently break the
``replaceWhere`` MERGE on the live Delta table — the same failure-class that
caused four DEFCON outages during the PR 6 cycle (see
test_defcon_schema_parity.py for the canonical reference application).
"""

from __future__ import annotations

import re

import pytest

_DDL_TYPE_TO_SPARK_NAME = {
    "STRING": "string",
    "BIGINT": "long",
    "INT": "integer",
    "DOUBLE": "double",
    "FLOAT": "float",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}


def _parse_ddl(ddl: str) -> dict[str, str]:
    """Return ``{col_name: spark_type_name}`` from a CREATE-TABLE-style DDL."""
    out: dict[str, str] = {}
    for raw in ddl.split(","):
        tok = raw.strip()
        if not tok:
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+([A-Z]+)\b", tok)
        if not m:
            raise AssertionError(f"unparseable DDL fragment: {tok!r}")
        col, ddl_type = m.group(1), m.group(2)
        if ddl_type not in _DDL_TYPE_TO_SPARK_NAME:
            raise AssertionError(f"unknown DDL type {ddl_type!r} for column {col!r}")
        out[col] = _DDL_TYPE_TO_SPARK_NAME[ddl_type]
    return out


def _build_pitch_control_output_schema():  # type: ignore[no-untyped-def]
    """Replay the ``output_schema`` definition from src/ingestion/pitch_control_batch.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("tracking_id", StringType(), nullable=False),
            StructField("match_id", StringType(), nullable=False),
            StructField("data_source", StringType(), nullable=True),
            StructField("match_key", LongType(), nullable=True),
            StructField("pitch_control_value", DoubleType(), nullable=False),
        ]
    )


class TestPitchControlWriterDdlParity:
    """src/ingestion/pitch_control_batch.py output_schema must match _RESULTS_SCHEMA DDL."""

    def test_output_schema_columns_match_results_ddl(self) -> None:
        from ingestion import pitch_control_batch

        ddl = _parse_ddl(pitch_control_batch._RESULTS_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_pitch_control_output_schema().fields}

        # Every writer column must exist in the DDL (DDL also has _ingested_at,
        # populated by write_delta_table itself).
        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"pitch_control writer emits columns absent from _RESULTS_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, (
            f"pitch_control writer/DDL type drift {mismatched}. "
            "Either widen the DDL or narrow the StructType."
        )

    def test_bronze_cols_constant_matches_results_schema(self) -> None:
        """_PITCH_CONTROL_BRONZE_COLS must enumerate every column in _RESULTS_SCHEMA."""
        from ingestion import pitch_control_batch

        ddl_cols = set(_parse_ddl(pitch_control_batch._RESULTS_SCHEMA).keys())
        const_cols = set(pitch_control_batch._PITCH_CONTROL_BRONZE_COLS)
        assert ddl_cols == const_cols, (
            f"_PITCH_CONTROL_BRONZE_COLS / _RESULTS_SCHEMA drift. "
            f"In DDL only: {ddl_cols - const_cols}; in const only: {const_cols - ddl_cols}."
        )

    def test_pr7_widening_present(self) -> None:
        """PR 7 schema widening: bronze contract must include data_source + match_key."""
        from ingestion import pitch_control_batch

        ddl = _parse_ddl(pitch_control_batch._RESULTS_SCHEMA)
        assert ddl.get("data_source") == "string", (
            "PR 7 (ADR-011) requires data_source STRING in bronze.pitch_control_values "
            "to collapse stg_pitch_control__values' PR 6 prefix-CASE bridge."
        )
        assert ddl.get("match_key") == "long", (
            "PR 7 (ADR-011) requires match_key BIGINT in bronze.pitch_control_values "
            "to collapse the PR 6 prefix-CASE-then-dim_matches-JOIN bridge."
        )
