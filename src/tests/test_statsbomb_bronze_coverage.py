"""Bronze-coverage test for StatsBomb: sources.yml must document the live schema.

Unlike IDSSE/SkillCorner/Metrica where we can run the bronze parser on a
synthetic input, StatsBomb's "parser" is statsbombpy over the network —
not test-isolable. Instead we snapshot the LIVE bronze schema via
``DESCRIBE TABLE`` (checked into
``src/tests/fixtures/statsbomb_bronze_schema_snapshot.json``) and assert
``_statsbomb__sources.yml`` documents every column.

**Refresh procedure:** when statsbombpy picks up new fields, or the
ingestion code adds derived cols, re-run DESCRIBE TABLE on the 5 StatsBomb
bronze tables and update the snapshot. The test then tells you which
sources.yml entries to add.
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
_SNAPSHOT_PATH = _FIXTURES / "statsbomb_bronze_schema_snapshot.json"
_SOURCES_YML_PATH = (
    _TEST_DIR.parent.parent / "dbt_project" / "models" / "staging" / "statsbomb" / "_statsbomb__sources.yml"
)

# Bronze cols we intentionally don't document yet (empty: we document all).
# Add entries here with a reason if a statsbombpy field is deliberately
# omitted from bronze sources.yml.
EXCLUDED_BRONZE_COLS: dict[str, str] = {}


@pytest.fixture(scope="module")
def _snapshot() -> dict:
    return load_attr_enumeration(_SNAPSHOT_PATH)


class TestStatsbombBronzeCoverage:
    """Every bronze column must be documented in _statsbomb__sources.yml."""

    def test_snapshot_has_expected_tables(self, _snapshot: dict) -> None:
        """Guardrail: the 5 StatsBomb bronze tables are in the snapshot."""
        expected = {
            "statsbomb_events",
            "statsbomb_matches",
            "statsbomb_lineups",
            "statsbomb_360",
            "statsbomb_competitions",
        }
        assert set(_snapshot["tables"].keys()) == expected

    @pytest.mark.parametrize(
        "table_name",
        [
            "statsbomb_events",
            "statsbomb_matches",
            "statsbomb_lineups",
            "statsbomb_360",
            "statsbomb_competitions",
        ],
    )
    def test_every_bronze_col_documented(self, _snapshot: dict, table_name: str) -> None:
        """Every col from the live schema snapshot must be in sources.yml.

        Failing means either (a) statsbombpy surfaced new fields and sources.yml
        is stale, or (b) the snapshot was refreshed without a sources.yml
        update. Remedy: update sources.yml + run this test + commit both.
        """
        snapshot_cols = {c["name"] for c in _snapshot["tables"][table_name]}
        sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
        missing = snapshot_cols - sources_cols - set(EXCLUDED_BRONZE_COLS.keys())
        assert not missing, (
            f"[{table_name}] {len(missing)} bronze col(s) in DESCRIBE TABLE "
            f"snapshot but not documented in sources.yml:\n  {sorted(missing)}"
        )

    def test_no_phantom_sources_yml_cols(self, _snapshot: dict) -> None:
        """sources.yml mustn't document cols that don't exist in live bronze."""
        for table_name, cols in _snapshot["tables"].items():
            snapshot_cols = {c["name"] for c in cols}
            sources_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML_PATH, table_name)
            phantom = sources_cols - snapshot_cols
            assert not phantom, (
                f"[{table_name}] sources.yml documents {len(phantom)} col(s) "
                f"that DON'T exist in the live bronze schema snapshot:\n  {sorted(phantom)}"
            )
