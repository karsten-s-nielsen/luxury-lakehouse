"""Guard 1 over PARSED fixtures: no bronze table any parser produces holds a duration type (no-duration spec §4).

Every provider's fixture deliveries are parsed (the same kits the bronze-doc contract uses), and every produced
table's schema is walked recursively through struct and list types. A computed column cannot escape this guard.
"""

# pyarrow's stubs leave nested DataType members untyped; relax the unknown-family rules (see compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from gskit import parse_with as parse_gs
from idssekit import parse_with as parse_idsse
from skillcornerkit import parse_with as parse_skillcorner

from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.raw.archive import Delivery

ROOT = Path(__file__).resolve().parents[2]
STATSBOMB = ROOT / "tests" / "fixtures" / "statsbomb"


def durations_in(arrow_type: pa.DataType, path: str) -> list[str]:
    """Every path inside `arrow_type` whose type is a duration (struct fields and list items walked)."""
    if pa.types.is_duration(arrow_type):
        return [path]
    if pa.types.is_struct(arrow_type):
        found: list[str] = []
        for index in range(arrow_type.num_fields):
            child = arrow_type.field(index)
            found += durations_in(child.type, f"{path}.{child.name}")
        return found
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return durations_in(arrow_type.value_type, f"{path}[]")
    return []


def schema_durations(name: str, schema: pa.Schema) -> list[str]:
    return [hit for field in schema for hit in durations_in(field.type, f"{name}.{field.name}")]


def _statsbomb_tables() -> dict[str, pa.Table]:
    def delivery(artifact: str, file: str) -> Delivery:
        return Delivery.received(
            provider="statsbomb",
            channel="public",
            native_match_id="9100001",
            artifact=artifact,
            raw=(STATSBOMB / file).read_bytes(),
            visibility="public",
            change_signal=None,
            fetched_at=datetime.now(UTC),
        )

    entry = SourceEntry(
        module_id="statsbomb_public",
        native_match_id="9100001",
        competition_id="43",
        competition_name="x",
        season_id="106",
        season_name="y",
        visibility="public",
        artifacts={"events": ArtifactRef(key="e", change_signal=None, expected_sha256=None, size=None)},
        listed_at=datetime.now(UTC),
    )
    parsed = StatsBombParser().parse(
        entry,
        {
            "events": delivery("events", "events_9100001.json"),
            "lineups": delivery("lineups", "lineups_9100001.json"),
            "three_sixty": delivery("three_sixty", "three_sixty_9100001.json"),
            "metadata": delivery("metadata", "commercial_metadata_9100001.json"),
        },
        run_id="r",
    )
    return {str(name): table for name, table in parsed.tables.items()}


def test_no_duration_type_in_parsed_fixture_tables() -> None:
    tables: dict[str, pa.Table] = {}
    tables.update(_statsbomb_tables())
    for kit in (parse_gs, parse_idsse, parse_skillcorner):
        tables.update({str(name): table for name, table in kit().tables.items()})
    hits = [hit for name, table in tables.items() for hit in schema_durations(name, table.schema)]
    assert tables, "no parsed tables"
    assert not hits, f"bronze tables with a duration type: {hits}"
