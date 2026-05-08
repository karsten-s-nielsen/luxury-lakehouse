"""Live data-quality checks for fct_pausa_values (ADR-013).

Validates mart row count, JOIN coverage, and md5 surrogate format on the
live warehouse. Static structural tests (file existence, contract, recipe)
live in src/tests/test_pausa_adr013_static.py and run in python-ci.

Requires live Databricks SQL warehouse. Skips when credentials are absent.
"""

from __future__ import annotations

import importlib
import os
import re
from typing import Any

import pytest

_databricks_sql_mod: Any
try:
    _databricks_sql_mod = importlib.import_module("databricks.sql")
except ImportError:  # pragma: no cover
    _databricks_sql_mod = None

_requires_databricks = pytest.mark.skipif(
    _databricks_sql_mod is None
    or not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="databricks-sql-connector not installed or DATABRICKS_* env vars not set",
)
_MD5_HEX_RE = re.compile(r"^[0-9a-f]{32}$")
_MART_VS_STG_FLOOR = 0.95


@_requires_databricks
class TestFctPausaValuesLive:
    """Live-warehouse assertions — skip when Databricks env vars are absent."""

    @pytest.fixture(scope="class")
    def conn(self) -> object:  # type: ignore[override]
        assert _databricks_sql_mod is not None
        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        c = _databricks_sql_mod.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        try:
            yield c
        finally:
            c.close()

    def test_mart_row_count_matches_or_exceeds_floor(self, conn: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        try:
            cur.execute("SELECT count(*) FROM soccer_analytics.bronze.pausa_values")
            (bronze_rows,) = cur.fetchone()
        except Exception as exc:
            pytest.skip(f"bronze.pausa_values not built yet: {exc}")
            return

        if bronze_rows == 0:
            pytest.skip("bronze.pausa_values is empty — no rows to verify against")

        try:
            cur.execute(
                "SELECT "
                " (SELECT count(*) FROM soccer_analytics.dev_silver.stg_pausa__values) AS stg, "
                " (SELECT count(*) FROM soccer_analytics.dev_gold.fct_pausa_values) AS mart"
            )
        except Exception as exc:
            pytest.skip(f"stg_pausa__values or fct_pausa_values not built: {exc}")
            return
        stg, mart = cur.fetchone()
        assert mart > 0, (
            f"fct_pausa_values has 0 rows from {stg} staging rows. "
            "This is the PR 7 ship-day silent JOIN-cardinality bug — "
            "stg_pausa__values is not computing the surrogate that matches "
            "fct_passes.pass_id."
        )
        coverage = mart / stg if stg else 0.0
        assert coverage >= _MART_VS_STG_FLOOR, (
            f"fct_pausa_values coverage {coverage:.2%} (mart={mart} / stg={stg}) below floor {_MART_VS_STG_FLOOR:.0%}."
        )

    def test_mart_pass_id_is_md5_hex_surrogate(self, conn: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        try:
            cur.execute("SELECT pass_id FROM soccer_analytics.dev_gold.fct_pausa_values LIMIT 100")
        except Exception as exc:
            pytest.skip(f"fct_pausa_values not built: {exc}")
            return
        rows = [r[0] for r in cur.fetchall()]
        if not rows:
            pytest.skip("fct_pausa_values is empty — covered by sibling test")
            return
        for pid in rows:
            assert _MD5_HEX_RE.match(pid), (
                f"pass_id {pid!r} is not a 32-char lowercase md5 hex — staging "
                "is emitting the native form, not the dbt_utils surrogate."
            )
