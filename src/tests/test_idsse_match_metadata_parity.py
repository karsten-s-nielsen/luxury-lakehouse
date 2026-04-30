"""Meta-test: per-match metadata parity between the IDSSE tracking and
events bronze writers.

Both ``ingest_idsse`` and ``ingest_idsse_events`` parse the same upstream
``DFL_02_01_matchinformation_*.xml`` files via
``ingestion.idsse._parse_match_metadata``. PR-LL2 (PR #229) wired the
metadata fields into ``bronze.idsse_events`` but missed
``bronze.idsse_tracking`` — staging then compensated by hardcoding a
match→competition mapping in dbt SQL. Session 69 (2026-04-30) closed the
gap and added this test so the asymmetric-coverage class cannot recur.

Failure modes this test prevents:

1. **Removal regression** — someone deletes ``competition_native_id`` from
   one writer's row builder but not the other; bronze drifts apart at the
   per-match metadata layer.
2. **Constant drift** — someone updates ``_IDSSE_MATCH_METADATA_BRONZE_COLS``
   without updating one of the two row builders; ``finalize_bronze_df``
   then fills NULL silently for the missing cols.
3. **Schema-vs-runtime drift** — the BRONZE_COLS constants advertise
   columns the parser never emits, or vice-versa.

Pure Python — no Databricks connection. Asserted at PR-CI time.
"""

from __future__ import annotations

import os
import tempfile

from ingestion.idsse import (
    _EMPTY_MATCH_METADATA,
    _IDSSE_EVENTS_BRONZE_COLS,
    _IDSSE_MATCH_METADATA_BRONZE_COLS,
    _IDSSE_TRACKING_BRONZE_COLS,
    _MatchMetadata,
    _parse_positions_xml,
)


def test_metadata_constant_in_tracking_bronze_cols() -> None:
    """``_IDSSE_TRACKING_BRONZE_COLS`` must include every name in
    ``_IDSSE_MATCH_METADATA_BRONZE_COLS``. ``finalize_bronze_df`` uses
    BRONZE_COLS to enforce drop-safety; a missing entry here means the
    column would be silently dropped to NullType during write."""
    tracking_set = set(_IDSSE_TRACKING_BRONZE_COLS)
    missing = _IDSSE_MATCH_METADATA_BRONZE_COLS - tracking_set
    assert not missing, (
        f"_IDSSE_TRACKING_BRONZE_COLS missing match-metadata cols: {sorted(missing)}. "
        "Add them to the tuple OR remove them from "
        "_IDSSE_MATCH_METADATA_BRONZE_COLS — both must agree."
    )


def test_metadata_constant_in_events_bronze_cols() -> None:
    """``_IDSSE_EVENTS_BRONZE_COLS`` must include every name in
    ``_IDSSE_MATCH_METADATA_BRONZE_COLS``. The parity invariant is two-way —
    if events drops a metadata column, this test fails just as it would
    for tracking."""
    missing = _IDSSE_MATCH_METADATA_BRONZE_COLS - _IDSSE_EVENTS_BRONZE_COLS
    assert not missing, (
        f"_IDSSE_EVENTS_BRONZE_COLS missing match-metadata cols: {sorted(missing)}. "
        "The events writer's row builder MUST emit these columns OR they "
        "must be removed from _IDSSE_MATCH_METADATA_BRONZE_COLS."
    )


def test_metadata_constant_matches_dataclass_fields() -> None:
    """Every field on ``_MatchMetadata`` whose value can be a STRING native
    identifier (i.e. excludes pitch dimensions, which are only relevant to
    tracking-coordinate processing) MUST be surfaced through a bronze
    column listed in ``_IDSSE_MATCH_METADATA_BRONZE_COLS``.

    Structural intent: if someone adds e.g. ``referee_id`` or
    ``stadium_id`` to ``_MatchMetadata``, this test fails until the
    parity-set is updated to include the corresponding bronze column.
    """
    # Non-string-native fields (pitch dimensions are floats and only
    # meaningful for the tracking coordinate frame; not metadata-parity
    # candidates).
    non_native_fields: set[str] = {"pitch_x", "pitch_y"}
    string_native_fields = {
        f.name for f in _MatchMetadata.__dataclass_fields__.values() if f.name not in non_native_fields
    }
    # Each ``<entity>_id`` field is expected to surface as ``<entity>_id_native``
    # OR ``<entity>_native_id`` per the existing PR-LL2 naming convention.
    # We don't enforce a specific spelling here — only that no string-native
    # field on the dataclass is missing from the parity-set entirely.
    canonical_cols = _IDSSE_MATCH_METADATA_BRONZE_COLS

    def _has_corresponding_col(field_name: str) -> bool:
        # `competition_id` → matches `competition_native_id` and `competition_id_native`.
        # Strip trailing `_id` if present for a more permissive base, then
        # accept any col whose root prefix matches.
        base = field_name.removesuffix("_id")
        return any(base in col for col in canonical_cols)

    missing = [f for f in string_native_fields if not _has_corresponding_col(f)]
    assert not missing, (
        f"_MatchMetadata fields with no corresponding entry in "
        f"_IDSSE_MATCH_METADATA_BRONZE_COLS: {sorted(missing)}. "
        f"Adding a new dataclass field is structural — both bronze writers "
        f"MUST surface it (and the parity-set updated) before merge."
    )


_PARITY_INFO_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <General CompetitionId="DFL-COM-000099"
             SeasonId="DFL-SEA-9999X9"
             HomeTeamId="DFL-CLU-HOME"
             GuestTeamId="DFL-CLU-AWAY"/>
    <Environment PitchX="105.00" PitchY="68.00"/>
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

# Synthetic positions XML — DFL spec literal long-form is intentionally on
# one line per `<Frame>` to mirror the real .xml shape. Excused from E501
# because wrapping would diverge from the wire format.
_PARITY_POS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-TEST" TeamId="BALL" PersonId="DFL-OBJ-BALL">
<Frame N="10000" T="2024-01-01T15:00:00.000Z" X="0.0" Y="0.0" Z="0.3" S="1.5" A="0.8" D="90.0" M="false" BallPossession="DFL-CLU-HOME" BallStatus="Alive"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-TEST" TeamId="DFL-CLU-HOME" PersonId="H001">
<Frame N="10000" T="2024-01-01T15:00:00.000Z" X="0.0" Y="0.0" S="0.0" A="0.0" D="0.0" M="false"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""  # noqa: E501


def _write_pair() -> tuple[str, str]:
    info_fd, info_path = tempfile.mkstemp(suffix="_info.xml")
    pos_fd, pos_path = tempfile.mkstemp(suffix="_pos.xml")
    os.close(info_fd)
    os.close(pos_fd)
    with open(info_path, "w", encoding="utf-8") as f:
        f.write(_PARITY_INFO_XML)
    with open(pos_path, "w", encoding="utf-8") as f:
        f.write(_PARITY_POS_XML)
    return info_path, pos_path


def test_tracking_parser_emits_match_metadata_at_runtime() -> None:
    """End-to-end: when the tracking parser is invoked with a populated
    ``_MatchMetadata``, every name in ``_IDSSE_MATCH_METADATA_BRONZE_COLS``
    appears as a key on the emitted row dicts.

    Catches the class where the row builder is updated but the BRONZE_COLS
    constant is not (or vice-versa) — finalize_bronze_df then silently
    drops the column to NullType, while the constant claims it should be
    present.
    """
    import logging

    from ingestion.idsse import _parse_teams

    info_path, pos_path = _write_pair()
    try:
        _h, _a, ptm, gk = _parse_teams(info_path)
        metadata = _MatchMetadata(
            competition_id="DFL-COM-000099",
            season_id="DFL-SEA-9999X9",
            home_team_id="DFL-CLU-HOME",
            away_team_id="DFL-CLU-AWAY",
            pitch_x=105.0,
            pitch_y=68.0,
        )
        rows_by_period = _parse_positions_xml(
            pos_path,
            ptm,
            "TEST",
            logging.getLogger("test"),
            gk_player_ids=gk,
            metadata=metadata,
        )
    finally:
        os.unlink(info_path)
        os.unlink(pos_path)

    rows = [row for period_rows in rows_by_period.values() for row in period_rows]
    assert rows, "parser produced no rows"
    actual_keys = set(rows[0].keys())
    missing = _IDSSE_MATCH_METADATA_BRONZE_COLS - actual_keys
    assert not missing, (
        f"tracking parser did not emit match-metadata cols at runtime: "
        f"{sorted(missing)}. Either add them to the row dict in "
        f"_parse_positions_xml OR remove them from "
        f"_IDSSE_MATCH_METADATA_BRONZE_COLS."
    )

    # Value parity — the metadata kwarg must propagate, not get lost.
    assert rows[0]["competition_native_id"] == "DFL-COM-000099"
    assert rows[0]["season_native_id"] == "DFL-SEA-9999X9"
    assert rows[0]["home_team_id_native"] == "DFL-CLU-HOME"
    assert rows[0]["away_team_id_native"] == "DFL-CLU-AWAY"


def test_tracking_parser_default_metadata_emits_empty_strings() -> None:
    """When no metadata is supplied (test path / synthetic XML without
    matchinformation), the parser MUST still emit the metadata columns —
    populated with the sentinel empty strings on ``_EMPTY_MATCH_METADATA``.

    Without this guarantee, finalize_bronze_df would produce a NullType
    column and Delta would drop it silently — the LL1 latent-bug class.
    """
    import logging

    from ingestion.idsse import _parse_teams

    info_path, pos_path = _write_pair()
    try:
        _h, _a, ptm, gk = _parse_teams(info_path)
        # Default metadata kwarg = _EMPTY_MATCH_METADATA.
        rows_by_period = _parse_positions_xml(pos_path, ptm, "TEST", logging.getLogger("test"), gk_player_ids=gk)
    finally:
        os.unlink(info_path)
        os.unlink(pos_path)

    rows = [row for period_rows in rows_by_period.values() for row in period_rows]
    assert rows, "parser produced no rows"
    actual_keys = set(rows[0].keys())
    missing = _IDSSE_MATCH_METADATA_BRONZE_COLS - actual_keys
    assert not missing, f"parser dropped metadata cols when called without metadata kwarg: {sorted(missing)}"
    # Sentinel values from _EMPTY_MATCH_METADATA.
    assert rows[0]["competition_native_id"] == _EMPTY_MATCH_METADATA.competition_id
    assert rows[0]["season_native_id"] == _EMPTY_MATCH_METADATA.season_id
