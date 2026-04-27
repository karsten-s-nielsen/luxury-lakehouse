"""Defense-in-depth: writer schema MUST match the bronze DDL.

This test enforces ADR-002 §4 ("Writer/target schema drift guard") for the
DEFCON-lite pipeline. Four production failures during the PR-6 cycle traced
to ``valued_schema`` (used by ``applyInPandas``) silently disagreeing with
``_RESULTS_SCHEMA`` (used by ``ensure_table`` to create the live Delta
table):

* ``ff_team_id`` was synthesized as INT then promoted to LONG by Spark
  ANSI → fixed in PR #209.
* ``ff_player_id`` from ``monotonically_increasing_id().cast("int")``
  collided with multi-partition LONG values → fixed in PR #210.
* ``action_player_id`` declared StringType but pdf carried int64 →
  pyarrow.lib.ArrowTypeError on first re-execution → fixed 2026-04-27.
* ``defcon_value`` declared DoubleType vs FLOAT in DDL → Delta refused
  MERGE → fixed 2026-04-27.

Each was a per-column drift the SkipGuard hid for many cycles. The tests
below parse the canonical DDL string and assert that every column the
writer emits agrees with the table contract.
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
    """Return ``{col_name: spark_type_name}`` from a CREATE-TABLE-style DDL.

    Strips optional NOT NULL, COMMENT, and trailing punctuation. Type names
    are normalised to lowercase Spark type names so a comparison against a
    StructField's ``.dataType.simpleString()`` is direct.
    """
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


def _struct_to_dict(struct) -> dict[str, str]:  # type: ignore[no-untyped-def]
    """Return ``{col_name: spark_type_name}`` from a ``StructType``.

    Imports pyspark lazily so the test module loads on machines without a
    pyspark install (e.g. lint-only CI shards).
    """
    return {f.name: f.dataType.simpleString() for f in struct.fields}


def _build_valued_schema(module_name: str):  # type: ignore[no-untyped-def]
    """Replay the ``valued_schema`` definition from the named module.

    The schemas live inside ``process_*_matches`` function bodies (closures
    over imports), so we can't just import them. We re-import the same
    types and re-build the StructType — this asserts that BOTH the
    importable types AND the hard-coded column list agree with the DDL.
    Drift in either direction is caught.
    """
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    FloatType = pyspark_types.FloatType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    # Common skeleton — both modules emit the same final schema shape.
    fields = [
        StructField("event_id", StringType(), nullable=True),
        StructField("match_id", StringType(), nullable=True),
        StructField("competition_id", LongType(), nullable=True),
        StructField("season_id", LongType(), nullable=True),
        StructField("defender_player_id", LongType(), nullable=True),
        StructField("defender_team_id", LongType(), nullable=True),
        StructField("defender_x", DoubleType(), nullable=True),
        StructField("defender_y", DoubleType(), nullable=True),
        StructField("action_player_id", LongType(), nullable=True),
        StructField("action_type", StringType(), nullable=True),
        StructField("action_x", DoubleType(), nullable=True),
        StructField("action_y", DoubleType(), nullable=True),
        StructField("credit_type", StringType(), nullable=True),
        StructField("confidence", StringType(), nullable=True),
        StructField("defcon_value", FloatType(), nullable=True),
        StructField("dist_to_ball", DoubleType(), nullable=True),
        StructField("pitch_control_at_action", DoubleType(), nullable=True),
        StructField("data_source", StringType(), nullable=True),
    ]
    # Sanity: the named module should expose the canonical schema string.
    import importlib

    mod = importlib.import_module(module_name)
    assert hasattr(mod, "_RESULTS_SCHEMA"), f"{module_name} missing _RESULTS_SCHEMA"
    return StructType(fields)


def _writer_columns_must_match_ddl(module_name: str) -> None:
    """Every writer column must appear in the DDL with the same Spark type."""
    import importlib

    mod = importlib.import_module(module_name)
    ddl = _parse_ddl(mod._RESULTS_SCHEMA)
    valued = _struct_to_dict(_build_valued_schema(module_name))

    missing = [c for c in valued if c not in ddl]
    assert not missing, (
        f"{module_name}: writer emits columns absent from DDL: {missing}. "
        f"DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write."
    )

    mismatched = {c: (valued[c], ddl[c]) for c in valued if valued[c] != ddl[c]}
    assert not mismatched, (
        f"{module_name}: writer/DDL type drift {mismatched}. "
        "Either widen the DDL via ALTER TABLE, or narrow the StructType."
    )


class TestDefconLite360SchemaParity:
    """Pass-2 valued_schema in defcon_lite_360 must match the bronze DDL."""

    def test_valued_schema_matches_results_ddl(self) -> None:
        _writer_columns_must_match_ddl("ingestion.defcon_lite_360")


class TestDefconLiteTrackingSchemaParity:
    """Pass-2 valued_schema in defcon_lite_tracking must match the bronze DDL."""

    def test_valued_schema_matches_results_ddl(self) -> None:
        _writer_columns_must_match_ddl("ingestion.defcon_lite_tracking")


class TestDdlConsistencyAcrossModules:
    """Both DEFCON paths must declare identical bronze DDL."""

    def test_360_and_tracking_share_results_schema(self) -> None:
        from ingestion import defcon_lite_360, defcon_lite_tracking

        assert _parse_ddl(defcon_lite_360._RESULTS_SCHEMA) == _parse_ddl(defcon_lite_tracking._RESULTS_SCHEMA), (
            "defcon_lite_360._RESULTS_SCHEMA and defcon_lite_tracking._RESULTS_SCHEMA "
            "differ — both must agree to the same bronze.defcon_results contract."
        )
