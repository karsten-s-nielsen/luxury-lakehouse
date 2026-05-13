"""TC-1 — Bronze DDL ↔ UDF output column-set parity test.

Same pattern as test_spadl_vaep_writer_parity.py: parse the DDL string,
compare columns against the StructType used by applyInPandas.
"""

from __future__ import annotations

import re

_DDL_TYPE_TO_SPARK_NAME = {
    "STRING": "string",
    "BIGINT": "bigint",
    "INT": "int",
    "DOUBLE": "double",
    "FLOAT": "float",
    "TIMESTAMP": "timestamp",
    "BOOLEAN": "boolean",
}


def _parse_ddl(ddl: str) -> dict[str, str]:
    """Return {col_name: spark_type_name} from a CREATE-TABLE-style DDL."""
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


class TestTrackingContextSchemaParity:
    """Bronze DDL constant must match UDF output schema."""

    def test_ddl_columns_match_result_columns(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS, _TRACKING_CONTEXT_DDL

        ddl_cols = set(_parse_ddl(_TRACKING_CONTEXT_DDL).keys())
        result_cols = set(_RESULT_COLUMNS)
        assert ddl_cols == result_cols, (
            f"DDL vs _RESULT_COLUMNS mismatch.\n"
            f"  In DDL only: {ddl_cols - result_cols}\n"
            f"  In _RESULT_COLUMNS only: {result_cols - ddl_cols}"
        )

    def test_ddl_has_no_duplicates(self) -> None:
        from ingestion.tracking_context import _TRACKING_CONTEXT_DDL

        cols = [tok.strip().split()[0] for tok in _TRACKING_CONTEXT_DDL.split(",") if tok.strip()]
        seen: set[str] = set()
        dupes = [c for c in cols if c in seen or seen.add(c)]  # type: ignore[func-returns-value]
        assert not dupes, f"Duplicate columns in DDL: {dupes}"

    def test_column_count(self) -> None:
        from ingestion.tracking_context import _RESULT_COLUMNS

        assert len(_RESULT_COLUMNS) == 83, f"Expected 83 columns, got {len(_RESULT_COLUMNS)}"
