"""Bronze-coverage test for SkillCorner: every kloppy-exposed field lands in bronze.

Retro-validation of task #2 in the PR 1.5 cycle (the SkillCorner bronze
enrichment). Enforces the bronze-completeness principle for SkillCorner
going forward: when kloppy surfaces new fields or SkillCorner upgrades
their schema, the fixture + parser must update together.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    from coverage_utils import (
        assert_source_covered_by_bronze,
        load_attr_enumeration,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        assert_source_covered_by_bronze,
        load_attr_enumeration,
    )

from ingestion.skillcorner import _dataset_to_rows

# Reuse the mock-builder from the main SkillCorner test suite.
try:
    from test_skillcorner import _build_mock_dataset
except ImportError:  # pragma: no cover
    from tests.test_skillcorner import _build_mock_dataset  # type: ignore[no-redef]


_TEST_DIR = Path(__file__).parent
_FIXTURES = _TEST_DIR / "fixtures"
_ENUMERATION_PATH = _FIXTURES / "skillcorner_kloppy_attr_enumeration.json"


@pytest.fixture(scope="module")
def _enumeration() -> dict:
    return load_attr_enumeration(_ENUMERATION_PATH)


@pytest.fixture(scope="module")
def _actual_bronze_cols() -> set[str]:
    """Run the SkillCorner bronze parser on the existing mock fixture."""
    fixture = json.loads((_FIXTURES / "skillcorner_sample.json").read_text(encoding="utf-8"))
    dataset = _build_mock_dataset(fixture)
    rows = _dataset_to_rows(dataset, "1925299")
    assert rows, "mock dataset produced no rows"
    return {k for row in rows for k in row.keys()}


class TestSkillCornerBronzeCoverage:
    """Every kloppy-exposed SkillCorner field must land in the bronze row dict."""

    def test_enumeration_structure(self, _enumeration: dict) -> None:
        """Guardrail: fixture has the expected top-level keys."""
        assert "source_to_bronze" in _enumeration
        assert "excluded_source_fields" in _enumeration
        assert len(_enumeration["source_to_bronze"]) >= 19  # current snapshot

    def test_no_duplicate_bronze_mappings(self, _enumeration: dict) -> None:
        """Different source fields must map to distinct bronze columns."""
        mappings = _enumeration["source_to_bronze"]
        rev: dict[str, list[str]] = {}
        for src, bronze in mappings.items():
            rev.setdefault(bronze, []).append(src)
        collisions = {b: srcs for b, srcs in rev.items() if len(srcs) > 1}
        assert not collisions, f"Bronze col collisions: {collisions}"

    def test_excluded_fields_have_reasons(self, _enumeration: dict) -> None:
        """Each excluded-source entry must explain WHY bronze doesn't emit it."""
        excluded = _enumeration["excluded_source_fields"]
        empty = [k for k, v in excluded.items() if not v]
        assert not empty, f"Excluded fields missing reasons: {empty}"

    def test_every_kloppy_field_lands_in_bronze(
        self,
        _enumeration: dict,
        _actual_bronze_cols: set[str],
    ) -> None:
        """Core coverage: every non-excluded kloppy field → a bronze column."""
        expected_bronze_cols = set(_enumeration["source_to_bronze"].values())
        assert_source_covered_by_bronze(
            expected_bronze_cols=expected_bronze_cols,
            actual_bronze_cols=_actual_bronze_cols,
            excluded={},  # exclusions are at the SOURCE-field layer, not the bronze layer
            name="SkillCorner",
        )
