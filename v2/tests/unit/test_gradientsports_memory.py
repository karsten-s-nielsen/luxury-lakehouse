"""The Gradient Sports tracking scan holds no avoidable copies (audit T11, owner decision 2026-11-01).

A real match decodes to about 1.1 GB of JSON lines and 10-11 million player rows. The scan used to hold a second
full copy of the text (`decoded.split(b"\\n")`, 1.2 GB) and one Python string object per player row for three columns
with a handful of distinct values (about 1.6 GB). It now iterates the lines in place and keeps one object per distinct
string.
"""

from __future__ import annotations

import tracemalloc
from typing import Any

from gskit import fixture_bytes

from lakehouse.parsers import gradientsports as gs
from lakehouse.parsers.jsonmap import records_to_table

_scan: Any = vars(gs)["_scan"]  # the scan is the unit under test
_list_payload: Any = vars(gs)["_list_payload"]
REPEAT = 400  # the fixture's frames, repeated (duplicate frames are kept, so the payload stays valid)


def _maps() -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    roster = records_to_table(_list_payload(fixture_bytes("roster"), "roster"), gs.ROSTER_FIELDS, artifact="roster")
    jerseys = {
        (str(team), str(shirt)): str(pid)
        for team, shirt, pid in zip(
            roster.column("team_id").to_pylist(),
            roster.column("shirt_number").to_pylist(),
            roster.column("player_id").to_pylist(),
            strict=True,
        )
    }
    meta = records_to_table(
        _list_payload(fixture_bytes("metadata"), "metadata"), gs.METADATA_FIELDS, artifact="metadata"
    )
    first = meta.to_pylist()[0]
    return jerseys, {"home": str(first["home_team_id"]), "away": str(first["away_team_id"])}


def _payload(padding: int = 0) -> bytes:
    """The fixture's frames repeated; `padding` JSON-legal spaces after each line's first brace make the text large
    against what the scan keeps, so a copy of the lines stands out from the scan's own structures."""
    pad = b"{" + b" " * padding
    lines = [line.replace(b"{", pad, 1) for line in fixture_bytes("tracking").splitlines() if line.strip()]
    return b"\n".join(lines * REPEAT) + b"\n"


def test_repeated_strings_are_stored_once() -> None:
    jerseys, teams = _maps()
    scan = _scan(_payload(), collect=True, jerseys=jerseys, teams=teams)
    for column in (scan.p_jersey, scan.p_confidence, scan.p_visibility):
        assert len(column) > 1000
        assert len({id(value) for value in column}) == len(set(column)), "one object per distinct value"


def test_the_scan_holds_no_copy_of_the_lines() -> None:
    jerseys, teams = _maps()
    payload = _payload(padding=20_000)
    tracemalloc.start()
    try:
        _scan(payload, collect=True, jerseys=jerseys, teams=teams)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    # a list of every line holds a full copy (at least 1.0 times the payload); the scan's own rows stay far below
    assert peak < 0.5 * len(payload), f"the scan peaked at {peak / len(payload):.2f} times the payload"
