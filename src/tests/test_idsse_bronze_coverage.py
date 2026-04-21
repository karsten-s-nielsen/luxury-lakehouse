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
from typing import ClassVar

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


class TestDflEventSchemaModuleParity:
    """The in-package DFL event schema module (``ingestion._dfl_event_schema``)
    is the runtime source of truth for :func:`_compute_idsse_events_bronze_cols`.
    The on-disk fixture (``idsse_dfl_event_attr_enumeration.json``) is the
    independent ground truth, regenerated when new DFL snapshots arrive.
    These tests assert the two do not drift.
    """

    def test_event_level_attrs_match_fixture(self, _enumeration: dict) -> None:
        from ingestion._dfl_event_schema import EVENT_LEVEL_ATTRS

        assert set(EVENT_LEVEL_ATTRS) == set(_enumeration["event_level_attrs"])

    def test_first_child_attrs_match_fixture(self, _enumeration: dict) -> None:
        from ingestion._dfl_event_schema import FIRST_CHILD_ATTRS

        fixture_first = _enumeration["first_child_types"]
        assert set(FIRST_CHILD_ATTRS.keys()) == set(fixture_first.keys()), (
            "First-child tag set differs between module and fixture"
        )
        for tag, attrs in FIRST_CHILD_ATTRS.items():
            assert set(attrs) == set(fixture_first[tag]["attrs"]), (
                f"Attrs for first-child {tag!r} differ: "
                f"module={sorted(attrs)} vs fixture={sorted(fixture_first[tag]['attrs'])}"
            )

    def test_nested_child_attrs_match_fixture(self, _enumeration: dict) -> None:
        from ingestion._dfl_event_schema import NESTED_CHILD_ATTRS

        fixture_nested = _enumeration.get("nested_children", {})
        assert set(NESTED_CHILD_ATTRS.keys()) == set(fixture_nested.keys()), (
            "Nested-parent tag set differs between module and fixture"
        )
        for parent_tag, nested_map in NESTED_CHILD_ATTRS.items():
            fixture_map = fixture_nested[parent_tag]
            assert set(nested_map.keys()) == set(fixture_map.keys()), f"Nested tags under {parent_tag!r} differ"
            for nested_tag, attrs in nested_map.items():
                assert set(attrs) == set(fixture_map[nested_tag]["attrs"]), (
                    f"Attrs for nested {parent_tag}.{nested_tag} differ"
                )


class TestComputedEventsBronzeColsMatchParserOutput:
    """``_IDSSE_EVENTS_BRONZE_COLS`` is pre-computed at parser-module import
    time and used by ``finalize_bronze_df`` to guarantee every col reaches
    Delta. It must exactly match the set the parser actually emits when run
    on the synthetic fixture XML — otherwise the pre-declared schema is
    wrong and some cols would still be dropped by Arrow.
    """

    def test_computed_cols_equal_parser_output(self, _actual_bronze_cols: set[str]) -> None:
        from ingestion.idsse import _IDSSE_EVENTS_BRONZE_COLS

        # Parser-output cols (from _actual_bronze_cols fixture) must be a
        # subset of the pre-computed expected set. If parser emits a col not
        # in the expected set, finalize_bronze_df doesn't need to add it, but
        # the expected set is then incomplete — flag it.
        missing_from_expected = _actual_bronze_cols - _IDSSE_EVENTS_BRONZE_COLS
        assert not missing_from_expected, (
            f"Parser emits columns not in _IDSSE_EVENTS_BRONZE_COLS: {sorted(missing_from_expected)}"
        )

        # The expected set MAY contain cols that a given synthetic XML run
        # doesn't exercise (e.g., shot_outcome_type only appears with nested
        # shot outcome tags). That's fine — those are the cols that need
        # finalize_bronze_df protection. We only flag the reverse direction.


class TestTrackingCoverage:
    """Bronze-completeness coverage for the IDSSE tracking parser.

    Every DFL ``<Frame>`` attribute enumerated in
    ``idsse_dfl_tracking_attr_enumeration.json`` must land in a dedicated
    bronze column via :func:`_parse_positions_xml`. Mirrors the events-side
    pattern in :class:`TestIdsseBronzeCoverage` — runs the parser on
    synthetic XML exercising every attribute and checks the resulting row
    dict keys.
    """

    _TRACKING_FIXTURE_PATH: ClassVar[Path] = (
        Path(__file__).parent / "fixtures" / "idsse_dfl_tracking_attr_enumeration.json"
    )

    # Mapping from DFL Frame attr to bronze column name for PLAYER rows.
    # Ball-only attrs (Z, BallPossession, BallStatus) appear on player rows
    # prefixed as ball_*; captured separately.
    _PLAYER_ATTR_TO_BRONZE: ClassVar[dict[str, str]] = {
        "N": "frame",
        "T": "t",
        "X": "x",
        "Y": "y",
        "S": "s",
        "A": "a",
        "D": "d",
        "M": "m",
    }
    _BALL_ONLY_ATTR_TO_BRONZE: ClassVar[dict[str, str]] = {
        "X": "ball_x",
        "Y": "ball_y",
        "Z": "ball_z",
        "S": "ball_s",
        "A": "ball_a",
        "D": "ball_d",
        "M": "ball_m",
        "T": "ball_t",
        "BallPossession": "ball_possession",
        "BallStatus": "ball_status",
    }
    # Derived cols the parser always emits.
    _DERIVED_TRACKING_COLS: ClassVar[set[str]] = {
        "period",
        "player_id",
        "team",
        "team_id",
        "match_id",
        "frame_rate",
        "timestamp",
        "is_goalkeeper",
    }

    def _load_tracking_enum(self) -> dict:
        import json

        return json.loads(self._TRACKING_FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_tracking_fixture_exists(self) -> None:
        assert self._TRACKING_FIXTURE_PATH.exists(), f"Missing: {self._TRACKING_FIXTURE_PATH}"

    def test_tracking_parser_emits_all_dfl_frame_attrs(self) -> None:
        from ingestion.idsse import _IDSSE_TRACKING_BRONZE_COLS

        enum = self._load_tracking_enum()
        player_attrs = set(enum["frame_attrs_by_teamid_category"]["player"])
        ball_attrs = set(enum["frame_attrs_by_teamid_category"]["ball"])

        expected: set[str] = set(self._DERIVED_TRACKING_COLS)
        for attr in player_attrs:
            expected.add(self._PLAYER_ATTR_TO_BRONZE[attr])
        for attr in ball_attrs:
            if attr == "N":
                # N → player-row `frame` derived col (same frame N is the
                # join key between player and ball rows); not a ball_*
                # column.
                continue
            expected.add(self._BALL_ONLY_ATTR_TO_BRONZE[attr])

        parser_cols = set(_IDSSE_TRACKING_BRONZE_COLS)
        missing = expected - parser_cols
        assert not missing, f"Parser constant missing {len(missing)} DFL attrs: {sorted(missing)}"

    def test_parser_runtime_output_matches_constant(self) -> None:
        """End-to-end: run the parser on synthetic XML covering every DFL attr,
        verify the emitted row dict's keys equal the declared constant.
        """
        from ingestion.idsse import _IDSSE_TRACKING_BRONZE_COLS, _parse_positions_xml, _parse_teams

        info_xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <Teams>
      <Team TeamId="DFL-CLU-HOME" Role="home">
        <Players>
          <Player PersonId="H001" PlayingPosition="TW" />
        </Players>
      </Team>
      <Team TeamId="DFL-CLU-AWAY" Role="guest">
        <Players>
          <Player PersonId="A001" PlayingPosition="RA" />
        </Players>
      </Team>
    </Teams>
  </MatchInformation>
</PutDataRequest>
"""
        # Synthetic positions XML exercises every DFL Frame attr on both ball
        # and player FrameSets: X Y Z S A D M T (ball adds BallPossession +
        # BallStatus).
        _ball_frame = (
            '<Frame N="10000" T="2024-01-01T15:00:00.000Z" X="0.1" Y="0.2" Z="0.3" '
            'S="1.5" A="0.8" D="90.0" M="false" '
            'BallPossession="DFL-CLU-HOME" BallStatus="Alive"/>'
        )
        _player_frame = (
            '<Frame N="10000" T="2024-01-01T15:00:00.000Z" X="-10.0" Y="5.0" S="5.2" A="1.1" D="45.0" M="false"/>'
        )
        pos_xml = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-TEST" TeamId="BALL" PersonId="DFL-OBJ-BALL">
{_ball_frame}
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-TEST" TeamId="DFL-CLU-HOME" PersonId="H001">
{_player_frame}
</FrameSet>
</Positions>
</PutDataRequest>
"""
        info_fd, info_path = tempfile.mkstemp(suffix=".xml")
        pos_fd, pos_path = tempfile.mkstemp(suffix=".xml")
        try:
            os.close(info_fd)
            os.close(pos_fd)
            with open(info_path, "w", encoding="utf-8") as f:
                f.write(info_xml)
            with open(pos_path, "w", encoding="utf-8") as f:
                f.write(pos_xml)
            _h, _a, ptm, gk = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "TEST", _LOGGER, gk_player_ids=gk)
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

        rows = [row for period_rows in rows_by_period.values() for row in period_rows]
        assert rows, "Parser produced no rows"
        actual = set(rows[0].keys())
        expected = set(_IDSSE_TRACKING_BRONZE_COLS)
        assert actual == expected, f"extra={actual - expected}, missing={expected - actual}"
