"""Meta-test: bronze.tracking_player_metadata writer schema parity.

Background: 2026-04-24 migration `2026-04-24-add-metrica-is-anonymized.sql`
added `is_anonymized BOOLEAN` to `bronze.tracking_player_metadata`. PR-Cycle-B
(2026-05-01) discovered that `extract_tracking_metadata` (the writer) was
full-overwriting the bronze table WITHOUT including this column in its
schema constant or its emitted row dicts. Every daily-job run silently
dropped the migration-added column, causing scheduled `Bronze Live Schema`
CI checks to fail with "is_anonymized absent from bronze.tracking_player_metadata".

This is the same writer/migration drift class as ADR-002 §4 (cost-hook
schema drift). The test asserts:

1. `_RESULTS_SCHEMA` SQL DDL declares `is_anonymized BOOLEAN`.
2. IDSSE row builder emits `is_anonymized` on every row dict (False —
   IDSSE carries real player identity).

Pure Python — no Databricks connection. Asserted at PR-CI time, not at
nightly Bronze Live Schema scheduled run.
"""

from __future__ import annotations

import logging
import os
import tempfile

from ingestion.tracking_metadata import (
    _IDSSE_MATCH_IDS,
    _MATCH_COMPETITION,
    _RESULTS_SCHEMA,
    _extract_idsse_metadata,
)


def test_results_schema_declares_is_anonymized() -> None:
    """`_RESULTS_SCHEMA` DDL string must declare `is_anonymized BOOLEAN`.

    Catches the case where someone adds the column to the row dict but
    forgets the schema constant — Spark would then either reject the
    write (DataFrame schema mismatch) or silently drop the column on
    overwrite, depending on Delta's mergeSchema setting.
    """
    assert "is_anonymized" in _RESULTS_SCHEMA, (
        f"_RESULTS_SCHEMA missing is_anonymized column. "
        f"DDL: {_RESULTS_SCHEMA!r}. "
        f"Migration scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql "
        f"added the column to bronze.tracking_player_metadata; the writer "
        f"full-overwrites the table, so the column must also be in the "
        f"writer's _RESULTS_SCHEMA."
    )
    assert "BOOLEAN" in _RESULTS_SCHEMA.upper(), (
        f"_RESULTS_SCHEMA contains 'is_anonymized' but no BOOLEAN type. DDL: {_RESULTS_SCHEMA!r}"
    )


_PARITY_INFO_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <General HomeTeamName="HomeTeam" GuestTeamName="GuestTeam"/>
    <Teams>
      <Team TeamId="DFL-CLU-HOME" Role="home" TeamName="HomeTeam">
        <Players>
          <Player PersonId="H001" Shortname="Player One" ShirtNumber="10"/>
        </Players>
      </Team>
      <Team TeamId="DFL-CLU-AWAY" Role="guest" TeamName="GuestTeam">
        <Players>
          <Player PersonId="A001" Shortname="Player Two" ShirtNumber="11"/>
        </Players>
      </Team>
    </Teams>
  </MatchInformation>
</PutDataRequest>
"""


def test_idsse_extractor_emits_is_anonymized() -> None:
    """`_extract_idsse_metadata` must emit `is_anonymized=False` on every row.

    IDSSE / Bundesliga DFL data carries real player identity (DFL-OBJ-PLY
    person IDs). The flag is explicitly False — True would conflate IDSSE
    with anonymized providers like Metrica.
    """
    mid = _IDSSE_MATCH_IDS[0]
    comp = _MATCH_COMPETITION[mid]
    info_filename = f"DFL_02_01_matchinformation_{comp}_DFL-MAT-{mid}.xml"

    with tempfile.TemporaryDirectory() as tmpdir:
        info_path = os.path.join(tmpdir, info_filename)
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(_PARITY_INFO_XML)
        rows = _extract_idsse_metadata(tmpdir, logging.getLogger("test"))

    assert rows, "parser produced no rows from synthetic IDSSE XML"
    for row in rows:
        assert "is_anonymized" in row, (
            f"is_anonymized missing from IDSSE row dict: {row!r}. "
            f'Add `"is_anonymized": False` to the rows.append({{...}}) '
            f"call in _extract_idsse_metadata."
        )
        assert row["is_anonymized"] is False, (
            f"IDSSE is_anonymized={row['is_anonymized']!r}, expected False "
            f"(IDSSE carries real player identity, not anonymized)."
        )
