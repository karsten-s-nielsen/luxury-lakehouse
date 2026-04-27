"""ADR-002 §4 writer/DDL parity guard for the PAUSA pipeline.

PR 7 (ADR-013 second application) retargets ``src/ingestion/pausa.py`` from
direct gold-write to bronze-write. ``_RESULTS_SCHEMA`` is the canonical bronze
DDL string consumed by ``ensure_table`` to create ``bronze.pausa_values``;
``output_schema`` is the StructType handed to ``applyInPandas`` for the per-
group UDF return shape. Drift between the two would silently break the
``replaceWhere`` MERGE on the live Delta table — the same failure-class that
caused four DEFCON outages during the PR 6 cycle (see ADR-002 §4 +
test_defcon_schema_parity.py for the canonical reference application).

The test below parses ``_RESULTS_SCHEMA`` and asserts every writer column
agrees with the table contract, mirroring the DEFCON parity guard pattern.
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


def _build_pausa_output_schema():  # type: ignore[no-untyped-def]
    """Replay the ``output_schema`` definition from src/ingestion/pausa.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    IntegerType = pyspark_types.IntegerType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("pass_id", StringType(), nullable=False),
            StructField("match_id", StringType(), nullable=False),
            StructField("player_id", StringType(), nullable=True),
            StructField("team", StringType(), nullable=True),
            StructField("period", IntegerType(), nullable=True),
            StructField("timestamp_seconds", DoubleType(), nullable=True),
            StructField("frame_id", IntegerType(), nullable=True),
            StructField("temporal_judgment", DoubleType(), nullable=False),
            StructField("spatial_selection", DoubleType(), nullable=False),
            StructField("pausa_score", DoubleType(), nullable=False),
            StructField("actual_obso", DoubleType(), nullable=True),
            StructField("peak_obso", DoubleType(), nullable=True),
            StructField("optimal_obso", DoubleType(), nullable=True),
            StructField("receiver_x", DoubleType(), nullable=True),
            StructField("receiver_y", DoubleType(), nullable=True),
        ]
    )


class TestPausaWriterDdlParity:
    """src/ingestion/pausa.py output_schema must match _RESULTS_SCHEMA DDL."""

    def test_output_schema_columns_match_results_ddl(self) -> None:
        from ingestion import pausa

        ddl = _parse_ddl(pausa._RESULTS_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_pausa_output_schema().fields}

        # Every writer column must exist in the DDL (the DDL also has _ingested_at,
        # which the writer doesn't emit — write_delta_table populates it).
        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"pausa writer emits columns absent from _RESULTS_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, (
            f"pausa writer/DDL type drift {mismatched}. Either widen the DDL or narrow the StructType."
        )

    def test_writer_targets_bronze_schema(self) -> None:
        """ADR-013 second application: writer emits to bronze, not gold."""
        from ingestion import pausa

        assert pausa._BRONZE_SCHEMA == "bronze", (
            f"PR 7 (ADR-013) requires src/ingestion/pausa.py to target bronze; "
            f"got _BRONZE_SCHEMA={pausa._BRONZE_SCHEMA!r}. The dbt-built mart "
            f"fct_pausa_values is the gold-layer target."
        )
        assert pausa._TABLE_NAME == "pausa_values", (
            f"PR 7 (ADR-013) renames the bronze raw table to 'pausa_values'; got _TABLE_NAME={pausa._TABLE_NAME!r}."
        )
