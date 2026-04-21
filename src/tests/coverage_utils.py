"""Shared helpers for bronze + staging field-coverage tests.

The bronze-completeness principle (`feedback_bronze_completeness_principle`)
states that every field emitted by a source provider must land as a column in
the bronze Delta table, and every bronze column must either be propagated
through the staging model or explicitly dropped with a reason. These helpers
enforce the principle via pytest.

Per-provider test pattern (bronze):

1. Maintain a JSON fixture at
   ``src/tests/fixtures/<provider>_attr_enumeration.json`` listing all known
   source attributes. The fixture is the source-of-truth snapshot of the
   provider's schema at a point in time — when the provider releases a
   schema change, regenerate the fixture and update the parser together.
2. Run the bronze parser on a minimal synthetic input that exercises every
   attribute path in the fixture.
3. Assert every source attribute appears as a bronze column (via
   snake-case normalization + provider-specific prefixing), except those
   in an explicit ``EXCLUDED_FIELDS`` allowlist with a per-field reason.

Per-provider test pattern (staging):

1. Load bronze cols from ``_<provider>__sources.yml``.
2. Load staging cols from ``_<provider>__models.yml``.
3. Assert every bronze col either appears in staging, is renamed per an
   explicit ``RENAMES`` map, or is in ``INTENTIONALLY_DROPPED`` with a reason.

The helpers deliberately avoid pyspark / dbt-runtime dependencies — coverage
tests run as fast pure-Python unit tests in CI.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

# Matches boundaries in CamelCase / PascalCase for snake_case conversion:
#   lower→Upper  ("PlayAngle" -> "Play_Angle")
#   Upper→Upper-before-lower  ("DFLObj" -> "DFL_Obj")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

# Repo root: parent of src/tests/ → parent of src/ → project root.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent


def to_snake_case(name: str) -> str:
    """Convert DFL-style CamelCase / hyphenated attribute names to snake_case.

    Examples::

        >>> to_snake_case("PlayAngle")
        'play_angle'
        >>> to_snake_case("X-Position")
        'x_position'
        >>> to_snake_case("X-PositionFromTracking")
        'x_position_from_tracking'
        >>> to_snake_case("BallPossessionPhase")
        'ball_possession_phase'
        >>> to_snake_case("VideoAssistantAction")
        'video_assistant_action'
        >>> to_snake_case("xG")
        'x_g'
    """
    s = name.replace("-", "_")
    return _CAMEL_BOUNDARY.sub("_", s).lower()


def load_attr_enumeration(fixture_path: Path) -> dict[str, Any]:
    """Load a provider's JSON attribute enumeration fixture.

    The fixture format is provider-specific but always JSON. Returns the
    parsed dict as-is; the caller interprets based on the provider schema.
    """
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def load_bronze_cols_from_sources_yml(yml_path: Path, table_name: str) -> set[str]:
    """Return the column names documented for a bronze table in its sources.yml.

    Only the columns listed in the yml doc are returned — this is the
    *contract* view of the bronze table. If the parser emits additional
    columns not in yml docs, the bronze-coverage test will flag them.

    Raises:
        KeyError: ``table_name`` is not found in any source of the yml.
    """
    data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    for source in data.get("sources", []):
        for table in source.get("tables", []):
            if table.get("name") == table_name:
                return {col["name"] for col in table.get("columns", [])}
    msg = f"Table '{table_name}' not found in {yml_path}"
    raise KeyError(msg)


def load_staging_cols_from_models_yml(yml_path: Path, model_name: str) -> set[str]:
    """Return the column names documented for a staging model in its _models.yml.

    Raises:
        KeyError: ``model_name`` is not found in any model of the yml.
    """
    data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    for model in data.get("models", []):
        if model.get("name") == model_name:
            return {col["name"] for col in model.get("columns", [])}
    msg = f"Model '{model_name}' not found in {yml_path}"
    raise KeyError(msg)


def assert_source_covered_by_bronze(
    expected_bronze_cols: set[str],
    actual_bronze_cols: set[str],
    excluded: dict[str, str],
    *,
    name: str,
) -> None:
    """Assert every expected bronze col is present, unless explicitly excluded.

    Args:
        expected_bronze_cols: The set of column names the bronze parser
            SHOULD emit, derived from the source-attribute enumeration
            fixture after applying the provider's naming convention
            (snake_case + prefix).
        actual_bronze_cols: The set of column names the bronze parser
            actually emits (usually ``set(row_dict.keys())`` for a synthetic
            input).
        excluded: Mapping of ``{expected_col_name: reason_string}`` for
            columns the provider intentionally does not emit to bronze.
            Each entry must have a non-empty reason.
        name: Provider name for the assertion error message (e.g. "IDSSE").

    Raises:
        AssertionError: with the list of missing columns + guidance on
            how to resolve (add to bronze parser or to EXCLUDED_FIELDS).
    """
    missing = expected_bronze_cols - actual_bronze_cols - set(excluded.keys())
    if missing:
        msg = (
            f"[{name}] Bronze parser missing {len(missing)} source-enumerated column(s):\n"
            f"  {sorted(missing)}\n"
            "Fix: either add the column in the bronze parser, or add the column\n"
            "name to EXCLUDED_FIELDS in the test with a non-empty reason string."
        )
        raise AssertionError(msg)
    empty_reasons = sorted(k for k, v in excluded.items() if not v)
    if empty_reasons:
        msg = (
            f"[{name}] EXCLUDED_FIELDS entries missing reason text:\n"
            f"  {empty_reasons}\n"
            "Each excluded field must have a non-empty reason explaining why "
            "the bronze parser does not emit it."
        )
        raise AssertionError(msg)


def assert_bronze_preserved_by_staging(
    bronze_cols: set[str],
    staging_cols: set[str],
    renames: dict[str, str],
    intentionally_dropped: dict[str, str],
    *,
    name: str,
) -> None:
    """Assert every bronze column is either in staging, renamed, or dropped.

    Args:
        bronze_cols: Columns documented in the bronze sources.yml.
        staging_cols: Columns documented in the staging _models.yml.
        renames: Mapping of ``{bronze_col: staging_col}`` for bronze columns
            that are renamed in staging (e.g. ``ShirtNumber`` →
            ``jersey_number``).
        intentionally_dropped: Mapping of ``{bronze_col: reason_string}``
            for bronze columns the staging layer deliberately drops.
        name: Provider name for the error message.

    Raises:
        AssertionError: with the list of missing columns + guidance.
    """
    # Apply renames on the set of bronze cols we expect to find downstream.
    # Cols in `intentionally_dropped` are not expected downstream.
    to_verify = bronze_cols - set(intentionally_dropped.keys())
    expected_staging_names = {renames.get(c, c) for c in to_verify}
    missing = expected_staging_names - staging_cols
    if missing:
        msg = (
            f"[{name}] Staging model missing {len(missing)} bronze column(s) "
            f"(not renamed, not dropped):\n"
            f"  {sorted(missing)}\n"
            "Fix: either carry the column through in the staging SQL, add the\n"
            "bronze col to RENAMES with its new staging name, or add it to\n"
            "INTENTIONALLY_DROPPED with a non-empty reason."
        )
        raise AssertionError(msg)
    empty_reasons = sorted(k for k, v in intentionally_dropped.items() if not v)
    if empty_reasons:
        msg = f"[{name}] INTENTIONALLY_DROPPED entries missing reason text:\n  {empty_reasons}"
        raise AssertionError(msg)
