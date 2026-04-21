"""Unit tests for coverage_utils — the shared bronze/staging coverage helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    # Pytest inserts src/tests/ onto sys.path via the package __init__.py,
    # so sibling modules import via the short name.
    from coverage_utils import (
        assert_bronze_preserved_by_staging,
        assert_source_covered_by_bronze,
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
        to_snake_case,
    )
except ImportError:  # pragma: no cover — fallback for namespace-package layout
    from tests.coverage_utils import (  # type: ignore[no-redef]
        assert_bronze_preserved_by_staging,
        assert_source_covered_by_bronze,
        load_attr_enumeration,
        load_bronze_cols_from_sources_yml,
        load_staging_cols_from_models_yml,
        to_snake_case,
    )


class TestToSnakeCase:
    """Verifies attr-name normalisation handles DFL + hyphenated + acronym cases."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("PlayAngle", "play_angle"),
            ("X-Position", "x_position"),
            ("X-PositionFromTracking", "x_position_from_tracking"),
            ("BallPossessionPhase", "ball_possession_phase"),
            ("VideoAssistantAction", "video_assistant_action"),
            ("xG", "x_g"),
            ("FlatCross", "flat_cross"),
            ("IsAccurate", "is_accurate"),
            ("already_snake", "already_snake"),
        ],
    )
    def test_normalisation(self, name: str, expected: str) -> None:
        assert to_snake_case(name) == expected


class TestLoadAttrEnumeration:
    def test_round_trip(self, tmp_path: Path) -> None:
        fixture = {"event_level_attrs": ["A", "B"], "first_child_types": {"X": {"attrs": ["p"]}}}
        p = tmp_path / "fx.json"
        p.write_text(json.dumps(fixture), encoding="utf-8")
        assert load_attr_enumeration(p) == fixture


class TestLoadBronzeColsFromSourcesYml:
    def _write(self, path: Path, content: str) -> None:
        path.write_text(content, encoding="utf-8")

    def test_reads_documented_cols(self, tmp_path: Path) -> None:
        yml = tmp_path / "sources.yml"
        self._write(
            yml,
            """
version: 2
sources:
  - name: p
    tables:
      - name: p_events
        columns:
          - name: match_id
            description: mid
          - name: event_type
            description: et
""",
        )
        cols = load_bronze_cols_from_sources_yml(yml, "p_events")
        assert cols == {"match_id", "event_type"}

    def test_missing_table_raises(self, tmp_path: Path) -> None:
        yml = tmp_path / "sources.yml"
        self._write(
            yml,
            """
version: 2
sources:
  - name: p
    tables:
      - name: other
        columns:
          - name: a
""",
        )
        with pytest.raises(KeyError, match="not found"):
            load_bronze_cols_from_sources_yml(yml, "p_events")


class TestLoadStagingColsFromModelsYml:
    def test_reads_model_cols(self, tmp_path: Path) -> None:
        yml = tmp_path / "models.yml"
        yml.write_text(
            """
version: 2
models:
  - name: stg_x__events
    columns:
      - name: match_id
        data_type: string
      - name: event_id
        data_type: string
""",
            encoding="utf-8",
        )
        cols = load_staging_cols_from_models_yml(yml, "stg_x__events")
        assert cols == {"match_id", "event_id"}


class TestAssertSourceCoveredByBronze:
    def test_passes_when_complete(self) -> None:
        assert_source_covered_by_bronze(
            expected_bronze_cols={"a", "b"},
            actual_bronze_cols={"a", "b", "c"},
            excluded={},
            name="Test",
        )

    def test_accepts_excluded_with_reason(self) -> None:
        assert_source_covered_by_bronze(
            expected_bronze_cols={"a", "b", "c"},
            actual_bronze_cols={"a", "b"},
            excluded={"c": "deprecated in provider schema 2026-01-15"},
            name="Test",
        )

    def test_rejects_missing_without_exclusion(self) -> None:
        with pytest.raises(AssertionError, match=r"missing 1 source-enumerated"):
            assert_source_covered_by_bronze(
                expected_bronze_cols={"a", "b", "c"},
                actual_bronze_cols={"a", "b"},
                excluded={},
                name="Test",
            )

    def test_rejects_empty_reason(self) -> None:
        with pytest.raises(AssertionError, match=r"missing reason text"):
            assert_source_covered_by_bronze(
                expected_bronze_cols={"a"},
                actual_bronze_cols={"a"},
                excluded={"other": ""},
                name="Test",
            )


class TestAssertBronzePreservedByStaging:
    def test_passes_when_all_preserved(self) -> None:
        assert_bronze_preserved_by_staging(
            bronze_cols={"a", "b"},
            staging_cols={"a", "b", "derived"},
            renames={},
            intentionally_dropped={},
            name="Test",
        )

    def test_applies_renames(self) -> None:
        assert_bronze_preserved_by_staging(
            bronze_cols={"ShirtNumber"},
            staging_cols={"jersey_number"},
            renames={"ShirtNumber": "jersey_number"},
            intentionally_dropped={},
            name="Test",
        )

    def test_accepts_intentionally_dropped(self) -> None:
        assert_bronze_preserved_by_staging(
            bronze_cols={"a", "legacy_col"},
            staging_cols={"a"},
            renames={},
            intentionally_dropped={"legacy_col": "replaced by a after 2026-02 refactor"},
            name="Test",
        )

    def test_rejects_missing_without_rename_or_drop(self) -> None:
        with pytest.raises(AssertionError, match=r"missing 1 bronze column"):
            assert_bronze_preserved_by_staging(
                bronze_cols={"a", "forgotten"},
                staging_cols={"a"},
                renames={},
                intentionally_dropped={},
                name="Test",
            )
