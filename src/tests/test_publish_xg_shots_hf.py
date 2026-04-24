"""Unit tests for scripts.publish_xg_shots_hf — SQL-shape regression guards."""

from __future__ import annotations

from pathlib import Path

_SCRIPT = Path(__file__).resolve().parent.parent.parent / "scripts" / "publish_xg_shots_hf.py"


class TestTryCastGuard:
    """Finding D regression (PR 4b, 2026-04-23).

    The publish SQL joins dim_matches whose native_match_id is STRING. Spark
    cast-pushdown pushes the BIGINT cast into the dim scan; on IDSSE / Metrica
    rows (alphanumeric native IDs) a plain `CAST(native_match_id AS BIGINT)`
    aborts the whole query. `try_cast` returns NULL on unparseable values and
    is the cast-pushdown-safe form. Reference: memory
    `reference_try_cast_spark_pushdown.md`.
    """

    def test_shots_sql_uses_try_cast(self) -> None:
        content = _SCRIPT.read_text().lower()
        assert "try_cast(dm.native_match_id" in content, (
            "publish_xg_shots_hf.py must use try_cast(dm.native_match_id ...); "
            "plain CAST breaks on IDSSE/Metrica rows per "
            "reference_try_cast_spark_pushdown.md (Finding D)."
        )

    def test_shots_sql_does_not_use_plain_cast_on_native_match_id(self) -> None:
        import re

        content = _SCRIPT.read_text().lower()
        # Plain CAST (not preceded by `try_`) applied to dm.native_match_id.
        plain_cast = re.compile(r"(?<!try_)cast\(dm\.native_match_id\s+as\s+bigint\)")
        assert not plain_cast.search(content), (
            "publish_xg_shots_hf.py must NOT use plain CAST on dm.native_match_id — regression guard for Finding D."
        )
