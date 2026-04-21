"""Bronze-coverage test for Wyscout: sources.yml must document the live schema.

Same pattern as StatsBomb — snapshot the live bronze schema via
``DESCRIBE TABLE`` and assert sources.yml is in sync.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from coverage_utils import (
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
    )

_TEST_DIR = Path(__file__).parent
_FIXTURES = _TEST_DIR / "fixtures"
_SNAPSHOT_PATH = _FIXTURES / "wyscout_bronze_schema_snapshot.json"
_SOURCES_YML_PATH = _TEST_DIR.parent.parent / "dbt_project" / "models" / "staging" / "wyscout" / "_wyscout__sources.yml"

EXCLUDED_BRONZE_COLS: dict[str, str] = {}


@pytest.fixture(scope="module")
def _snapshot() -> dict:
    return load_attr_enumeration(_SNAPSHOT_PATH)


class TestWyscoutBronzeCoverage:
    """Every bronze column must be documented in _wyscout__sources.yml."""

    def test_snapshot_has_expected_tables(self, _snapshot: dict) -> None:
        expected = {"wyscout_events", "wyscout_matches", "wyscout_players"}
        assert set(_snapshot["tables"].keys()) == expected

    @pytest.mark.parametrize(
        "table_name",
        ["wyscout_events", "wyscout_matches", "wyscout_players"],
    )
    def test_every_bronze_col_documented(self, _snapshot: dict, table_name: str) -> None:
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        missing = snapshot_cols - sources_cols - set(EXCLUDED_BRONZE_COLS.keys())
        assert not missing, (
            f"[{table_name}] {len(missing)} bronze col(s) in DESCRIBE TABLE "
            f"snapshot but not documented in sources.yml:\n  {sorted(missing)}"
        )

    def test_no_phantom_sources_yml_cols(self, _snapshot: dict) -> None:
        for table_name, cols in _snapshot["tables"].items():
            snapshot_cols = {c["name"] for c in cols}
            sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
            phantom = sources_cols - snapshot_cols
            assert not phantom, (
                f"[{table_name}] sources.yml documents {len(phantom)} col(s) "
                f"that DON'T exist in the live bronze schema snapshot:\n  {sorted(phantom)}"
            )
