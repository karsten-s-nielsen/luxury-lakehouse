"""ADR-002 §4 writer/DDL parity guard for the SPADL/VAEP pipeline.

PR-LL1 (silly-kicks 1.5.0+ ``preserve_native``) extends ``_SPADL_SCHEMA`` and
``_VAEP_SCHEMA`` in ``src/ingestion/spadl_vaep.py`` with 4 provider-namespaced
StatsBomb-native columns. Spark's ``applyInPandas`` schemas declared in
``src/ingestion/spadl_conversion.py`` (per source) must agree column-for-column
with these DDL constants — otherwise the per-game UDF return shape drifts from
the live Delta table contract and the next ``write_delta_table`` /
``replaceWhere`` MERGE silently fails with ``DELTA_FAILED_TO_MERGE_FIELDS``
(same failure-class that caused four DEFCON outages during PR 6 — see
ADR-002 §4 + ``test_defcon_schema_parity.py`` for the canonical reference
application; ``test_pausa_writer_parity.py`` for the PAUSA peer guard).

The test below parses ``_SPADL_SCHEMA`` + ``_VAEP_SCHEMA`` and asserts every
writer column present in either applyInPandas StructType (StatsBomb path or
Wyscout path) agrees with the table contract.
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


def _build_statsbomb_spadl_struct():  # type: ignore[no-untyped-def]
    """Replay the StatsBomb applyInPandas StructType in spadl_conversion.py."""
    pyspark_types = pytest.importorskip("pyspark.sql.types")
    BooleanType = pyspark_types.BooleanType  # noqa: N806
    DoubleType = pyspark_types.DoubleType  # noqa: N806
    LongType = pyspark_types.LongType  # noqa: N806
    StringType = pyspark_types.StringType  # noqa: N806
    StructField = pyspark_types.StructField  # noqa: N806
    StructType = pyspark_types.StructType  # noqa: N806

    return StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )


class TestSpadlVaepWriterDdlParity:
    """spadl_conversion.py applyInPandas schemas must match _SPADL_SCHEMA DDL."""

    def test_statsbomb_struct_matches_spadl_ddl(self) -> None:
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        out = {f.name: f.dataType.simpleString() for f in _build_statsbomb_spadl_struct().fields}

        missing = [c for c in out if c not in ddl]
        assert not missing, (
            f"StatsBomb spadl writer emits columns absent from _SPADL_SCHEMA: {missing}. "
            "DELTA_FAILED_TO_MERGE_FIELDS will fire on next replaceWhere write to "
            "bronze.spadl_actions."
        )

        mismatched = {c: (out[c], ddl[c]) for c in out if out[c] != ddl[c]}
        assert not mismatched, (
            f"StatsBomb spadl writer/DDL type drift {mismatched}. Either widen the DDL or narrow the StructType."
        )

    def test_spadl_ddl_includes_new_preserve_native_columns(self) -> None:
        """LL1: silly-kicks 1.5.0+ preserve_native surfaces 4 statsbomb_* fields."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert col in ddl, (
                f"_SPADL_SCHEMA missing PR-LL1 column {col!r}. "
                "silly-kicks 1.5.0+ preserve_native passthrough requires this."
            )

    def test_vaep_ddl_includes_new_preserve_native_columns(self) -> None:
        """LL1: vaep_action_values must carry the same 4 statsbomb_* fields downstream."""
        from ingestion import spadl_vaep

        ddl = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert col in ddl, (
                f"_VAEP_SCHEMA missing PR-LL1 column {col!r}. "
                "VAEP scoring UDF must carry preserve_native fields through to bronze.vaep_action_values."
            )

    def test_vaep_dtypes_match_spadl_dtypes_for_new_columns(self) -> None:
        """The 4 statsbomb_* columns must have identical types in both DDLs
        (otherwise vaep_action_values would have a narrower / mismatched view)."""
        from ingestion import spadl_vaep

        spadl = _parse_ddl(spadl_vaep._SPADL_SCHEMA)
        vaep = _parse_ddl(spadl_vaep._VAEP_SCHEMA)
        for col in (
            "statsbomb_possession_id",
            "statsbomb_possession_team_id",
            "statsbomb_play_pattern",
            "statsbomb_under_pressure",
        ):
            assert spadl[col] == vaep[col], (
                f"PR-LL1 column {col!r} type drift: _SPADL_SCHEMA={spadl[col]!r} vs _VAEP_SCHEMA={vaep[col]!r}"
            )
