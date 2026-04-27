"""Bronze contract test for pitch_control_values (PR 6, ADR-011).

Asserts the writer's emitted column set matches the source-YAML's
documented column set. Drift detection: if pitch_control_batch.py adds a
column, the source YAML must be updated. If the YAML is updated without
the writer producing the column, the live bronze schema diverges silently.

This complements:
  - test_bronze_live_schema.py — writer constant ↔ live Delta schema.
  - test_staging_coverage.py — bronze sources.yml ↔ staging models.yml.

Together the three tests close the writer ↔ source-yml ↔ staging-yml ↔
live-Delta drift loop on the pitch-control bronze→staging chain.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src importable when running via `uv run pytest`
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

from ingestion.pitch_control_batch import _PITCH_CONTROL_BRONZE_COLS

try:
    from coverage_utils import load_bronze_cols_from_sources_yml
except ImportError:  # pragma: no cover
    from tests.coverage_utils import load_bronze_cols_from_sources_yml  # type: ignore[no-redef]

_DBT_STAGING = Path(__file__).resolve().parent.parent.parent / "dbt_project" / "models" / "staging"
_SOURCES_YML = _DBT_STAGING / "pitch_control" / "_pitch_control__sources.yml"


def test_writer_columns_match_source_yml() -> None:
    """pitch_control_batch._PITCH_CONTROL_BRONZE_COLS must match
    _pitch_control__sources.yml columns block.

    Drift detection — see test_staging_coverage.py for the broader pattern.
    """
    writer_cols = set(_PITCH_CONTROL_BRONZE_COLS)
    yml_cols = load_bronze_cols_from_sources_yml(_SOURCES_YML, "pitch_control_values")

    missing_in_yml = writer_cols - yml_cols
    assert not missing_in_yml, (
        f"Writer emits {len(missing_in_yml)} column(s) not documented in "
        f"_pitch_control__sources.yml: {sorted(missing_in_yml)}. "
        "Fix: add the column(s) to the YAML's tables[].columns block."
    )

    missing_in_writer = yml_cols - writer_cols
    assert not missing_in_writer, (
        f"_pitch_control__sources.yml documents {len(missing_in_writer)} column(s) "
        f"not emitted by the writer: {sorted(missing_in_writer)}. "
        "Fix: either remove from YAML or add to ingestion.pitch_control_batch._PITCH_CONTROL_BRONZE_COLS."
    )
