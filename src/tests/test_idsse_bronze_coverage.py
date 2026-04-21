"""Bronze-coverage test for IDSSE: every DFL source attribute must land in bronze.

Drives the IDSSE ``_parse_events_xml`` rewrite (task #3 in the PR 1.5 cycle).
Until the rewrite completes, this test is EXPECTED to fail — the failure
message lists exactly which bronze columns are still missing.

Pattern (see ``coverage_utils.py`` docstring for full protocol):

  1. Load ``idsse_dfl_event_attr_enumeration.json`` — the ground-truth
     snapshot of every DFL XML attribute seen in the 7-match sample.
  2. Generate a synthetic XML that exercises every first-child tag with
     every documented attribute.
  3. Run ``_parse_events_xml`` on the synthetic XML; collect the union of
     row-dict keys across all produced rows — this is the "actual bronze
     column set".
  4. Compute the expected bronze column set from the enumeration via the
     naming convention below (event-level rename map + first-child prefix
     map + nested-child prefix map, all snake_cased).
  5. Assert expected ⊆ actual (modulo EXCLUDED_FIELDS with reason).
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import pytest

try:
    from coverage_utils import (
        assert_source_covered_by_bronze,
        load_attr_enumeration,
        to_snake_case,
    )
except ImportError:  # pragma: no cover
    from tests.coverage_utils import (  # type: ignore[no-redef]
        assert_source_covered_by_bronze,
        load_attr_enumeration,
        to_snake_case,
    )

from ingestion.idsse import (
    _EVENT_LEVEL_ATTR_MAP as EVENT_LEVEL_ATTR_MAP,
)
from ingestion.idsse import (
    _EVENT_TYPE_PREFIX as EVENT_TYPE_PREFIX,
)
from ingestion.idsse import (
    _NESTED_PREFIX_MAP as NESTED_PREFIX_MAP,
)
from ingestion.idsse import _parse_events_xml

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "idsse_dfl_event_attr_enumeration.json"
_LOGGER = logging.getLogger("test_idsse_bronze_coverage")

# Columns derived by the parser (not direct XML attrs). These are guaranteed
# to appear on every row but are NOT in the source-enumeration fixture.
# Listed here for documentation — they aren't asserted by the coverage test.
DERIVED_EVENT_LEVEL_COLS: frozenset[str] = frozenset(
    {"match_id", "event_type", "period", "timestamp_seconds", "player_id", "team"},
)

# Source attributes that are intentionally NOT propagated to bronze.
# Each entry must have a non-empty reason explaining the decision.
# When empty, the bronze parser must emit EVERY enumerated attribute.
EXCLUDED_FIELDS: dict[str, str] = {}


def _generate_synthetic_xml(enum: dict) -> str:
    """Build a synthetic DFL event XML exercising every first-child type.

    One ``<Event>`` per first-child tag in the fixture, populated with every
    documented attribute (placeholder values). Nested children are emitted
    with empty attrs if the fixture doesn't enumerate them — the goal is
    coverage of COLUMN NAMES, not values.
    """
    events: list[str] = []
    for i, tag in enumerate(sorted(enum["first_child_types"].keys())):
        info = enum["first_child_types"][tag]
        attrs = info["attrs"]
        attr_str = " ".join(f'{a}="v{i}_{j}"' for j, a in enumerate(attrs))

        nested_xml = ""
        for nested_tag, nested_info in enum.get("nested_children", {}).get(tag, {}).items():
            nested_attrs = nested_info["attrs"]
            nested_attr_str = " ".join(f'{a}="nv{i}_{k}"' for k, a in enumerate(nested_attrs))
            nested_xml += f"<{nested_tag} {nested_attr_str}/>"

        events.append(
            f'<Event MatchId="DFL-MAT-TEST" '
            f'EventId="E{i:03d}" '
            f'EventTime="2023-05-27T15:{i // 60:02d}:{i % 60:02d}.000+02:00" '
            f'StartFrame="{i}" EndFrame="{i + 10}" CalculatedFrame="{i + 5}" '
            f'CalculatedTimestamp="2023-05-27T15:{i // 60:02d}:{i % 60:02d}.500+02:00" '
            f'X-Position="52.5" Y-Position="34.0" '
            f'X-Source-Position="52.5" Y-Source-Position="34.0" '
            f'X-PositionFromTracking="50.0" Y-PositionFromTracking="30.0">'
            f"<{tag} {attr_str}>{nested_xml}</{tag}>"
            f"</Event>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<PutDataRequest RequestId="TEST" MessageTime="2023-05-27T15:00:00.000+02:00" '
        'TransmissionComplete="true" DataStatus="postmatch">\n' + "\n".join(events) + "\n</PutDataRequest>"
    )


def _expected_bronze_cols(enum: dict) -> set[str]:
    """Compute the set of bronze cols the parser MUST emit to satisfy coverage."""
    expected: set[str] = set()

    # Event-level: one bronze col per DFL Event attribute, via the rename map.
    for dfl_attr in enum["event_level_attrs"]:
        if dfl_attr not in EVENT_LEVEL_ATTR_MAP:
            msg = (
                f"Event-level attr {dfl_attr!r} is in the fixture but not in "
                "EVENT_LEVEL_ATTR_MAP. Add a mapping (or mark as excluded)."
            )
            raise AssertionError(msg)
        expected.add(EVENT_LEVEL_ATTR_MAP[dfl_attr])

    # First-child: each attr → {prefix}_{snake(attr)}
    for tag, info in enum["first_child_types"].items():
        if tag not in EVENT_TYPE_PREFIX:
            msg = f"First-child tag {tag!r} is in the fixture but not in EVENT_TYPE_PREFIX."
            raise AssertionError(msg)
        prefix = EVENT_TYPE_PREFIX[tag]
        for attr in info["attrs"]:
            expected.add(f"{prefix}_{to_snake_case(attr)}")

    # Nested: each attr → {nested_prefix}_{snake(attr)}
    for _parent, nested_map in enum.get("nested_children", {}).items():
        for nested_tag, nested_info in nested_map.items():
            if nested_tag not in NESTED_PREFIX_MAP:
                msg = f"Nested tag {nested_tag!r} is in the fixture but not in NESTED_PREFIX_MAP."
                raise AssertionError(msg)
            prefix = NESTED_PREFIX_MAP[nested_tag]
            for attr in nested_info["attrs"]:
                expected.add(f"{prefix}_{to_snake_case(attr)}")

    return expected


@pytest.fixture(scope="module")
def _enumeration() -> dict:
    return load_attr_enumeration(_FIXTURE_PATH)


@pytest.fixture(scope="module")
def _actual_bronze_cols(_enumeration: dict) -> set[str]:
    """Run the IDSSE bronze parser on a synthetic XML covering every event type."""
    xml_text = _generate_synthetic_xml(_enumeration)
    fd, path = tempfile.mkstemp(suffix=".xml")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(xml_text)
        # Empty player_team_map is fine: the test doesn't check team-label values,
        # only column presence. The parser gracefully handles unknown players.
        rows = _parse_events_xml(path, {}, "TEST", _LOGGER)
    finally:
        os.unlink(path)
    return {k for row in rows for k in row.keys()}


class TestIdsseBronzeCoverage:
    """Verify every DFL XML attribute lands in an IDSSE bronze column."""

    def test_fixture_has_expected_structure(self, _enumeration: dict) -> None:
        """Guardrails against fixture drift: verify top-level keys + counts.

        If this fails, the fixture JSON has been edited without updating the
        constants. Inspect the fixture, adjust EVENT_TYPE_PREFIX /
        NESTED_PREFIX_MAP / EVENT_LEVEL_ATTR_MAP in this test file as needed.
        """
        assert "event_level_attrs" in _enumeration
        assert "first_child_types" in _enumeration
        assert "nested_children" in _enumeration
        # Current snapshot: 13 event-level attrs, 32 first-child types.
        assert len(_enumeration["event_level_attrs"]) == 13
        assert len(_enumeration["first_child_types"]) == 32

    def test_every_first_child_tag_has_a_prefix(self, _enumeration: dict) -> None:
        """EVENT_TYPE_PREFIX must cover every first-child tag in the fixture."""
        fixture_tags = set(_enumeration["first_child_types"].keys())
        missing = fixture_tags - set(EVENT_TYPE_PREFIX.keys())
        assert not missing, (
            f"Fixture has {len(missing)} first-child tag(s) with no entry in EVENT_TYPE_PREFIX: {sorted(missing)}"
        )

    def test_every_nested_tag_has_a_prefix(self, _enumeration: dict) -> None:
        """NESTED_PREFIX_MAP must cover every nested tag in the fixture."""
        fixture_nested: set[str] = set()
        for nested_map in _enumeration.get("nested_children", {}).values():
            fixture_nested.update(nested_map.keys())
        missing = fixture_nested - set(NESTED_PREFIX_MAP.keys())
        assert not missing, (
            f"Fixture has {len(missing)} nested tag(s) with no entry in NESTED_PREFIX_MAP: {sorted(missing)}"
        )

    def test_prefixes_are_distinct(self) -> None:
        """Distinct first-child tags must have distinct prefixes to avoid col collisions."""
        rev: dict[str, list[str]] = {}
        for tag, prefix in EVENT_TYPE_PREFIX.items():
            rev.setdefault(prefix, []).append(tag)
        collisions = {p: tags for p, tags in rev.items() if len(tags) > 1}
        assert not collisions, f"Prefix collisions: {collisions}"

    def test_every_source_attr_lands_in_bronze(self, _enumeration: dict, _actual_bronze_cols: set[str]) -> None:
        """The core coverage assertion: every DFL attr → bronze column.

        Fails with a sorted list of missing cols until the IDSSE parser
        rewrite (task #3 in the PR 1.5 cycle) completes.
        """
        expected = _expected_bronze_cols(_enumeration)
        assert_source_covered_by_bronze(
            expected_bronze_cols=expected,
            actual_bronze_cols=_actual_bronze_cols,
            excluded=EXCLUDED_FIELDS,
            name="IDSSE",
        )
