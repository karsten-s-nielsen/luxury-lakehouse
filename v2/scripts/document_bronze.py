"""Write the dbt source documentation of a provider's bronze tables from its parser's field declarations (§12.8).

Usage: uv run python scripts/document_bronze.py statsbomb|idsse|skillcorner|gradientsports|wyscout|pining
The contract test `tests/contract/test_bronze_docs.py` fails when a documented column list and the parsed table drift.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import pyarrow as pa
import yaml

from lakehouse.parsers import gradientsports, idsse, pining, skillcorner, statsbomb, wyscout
from lakehouse.parsers.jsonmap import Field

ROOT: Final = Path(__file__).resolve().parents[1]

KEY_COLUMNS: Final = {
    "provider": "The provider name (`statsbomb`).",
    "native_match_id": "The canonical native match id (Task 2).",
    "match_key": "Surrogate match key: xxhash64 of `provider|native_match_id`, seed 42, signed (spec §14).",
    "player_key": "Surrogate player key of `player_id` (spec §14); null when the event has no player.",
    "team_key": "Surrogate team key of `team_id` (spec §14).",
    "possession_team_key": "Surrogate team key of `possession_team_id` (spec §14).",
    "competition_key": "Surrogate competition key of `competition_id` (spec §14).",
    "season_key": "Surrogate season key of `competition_id:season_id` (spec §14, Task 2).",
    "home_team_key": "Surrogate team key of `home_team_id` (spec §14).",
    "away_team_key": "Surrogate team key of `away_team_id` (spec §14).",
    "_ingested_at": "When the run parsed the delivery (UTC).",
    "_run_id": "The run that wrote the row.",
    "_source_hash": "The content_sha256 of the delivered file (spec §13.1).",
}


def _type_text(arrow_type: pa.DataType) -> str:
    return str(arrow_type).replace("list<item: ", "list<").replace("element: ", "")


def _column_docs(fields: Sequence[Field], extra: Sequence[str], *, units: str) -> list[dict[str, str]]:
    docs: list[dict[str, str]] = []
    for field in fields:
        text = f"Source field `{field.path}`, as {_type_text(field.type)}."
        if "location" in field.path or field.path == "visible_area":
            text += f" Pitch coordinates in {units}, as delivered (spec §11.4)."
        if field.convert != "plain":
            text += f" Converted from text ({field.convert}) with a round-trip check (spec §12.4)."
        docs.append({"name": field.column, "description": text})
    docs.extend({"name": name, "description": KEY_COLUMNS[name]} for name in extra)
    return docs


def statsbomb_tables() -> list[dict[str, object]]:
    audit = ["_ingested_at", "_run_id", "_source_hash"]
    match_keys = ["provider", "native_match_id", "match_key"]
    listing_keys = ["provider", "competition_key", "season_key"]
    specs: list[tuple[str, str, Sequence[Field], list[str]]] = [
        (
            statsbomb.EVENTS,
            "One row per event of a match (events/<match_id>.json).",
            statsbomb.EVENT_FIELDS,
            [*match_keys, "player_key", "team_key", "possession_team_key", *audit],
        ),
        (
            statsbomb.LINEUPS,
            "One row per player per team of a match (lineups/<match_id>.json).",
            statsbomb.LINEUP_FIELDS,
            [*match_keys, "player_key", "team_key", *audit],
        ),
        (
            statsbomb.LINEUP_TEAMS,
            "One row per team of a match's lineups file, with its team-level events and formations (pining owner "
            "tier; null in open data).",
            statsbomb.LINEUP_TEAM_FIELDS,
            [*match_keys, "team_key", *audit],
        ),
        (
            statsbomb.THREE_SIXTY,
            "One row per 360 frame of a match (three-sixty/<match_id>.json).",
            statsbomb.THREE_SIXTY_FIELDS,
            [*match_keys, *audit],
        ),
        (
            statsbomb.MATCH_METADATA,
            "One row per match from the pining `metadata` artifact (one match object in feed shape, spec §7.2).",
            statsbomb.METADATA_FIELDS,
            [*match_keys, "competition_key", "season_key", "home_team_key", "away_team_key", *audit],
        ),
        (
            statsbomb.MATCHES,
            "One row per match of a competition-season listing (matches/<c>/<s>.json).",
            statsbomb.MATCH_FIELDS,
            [*listing_keys, "match_key", "home_team_key", "away_team_key", *audit],
        ),
        (
            statsbomb.COMPETITIONS,
            "One row per competition-season (competitions.json).",
            statsbomb.COMPETITION_FIELDS,
            [*listing_keys, *audit],
        ),
    ]
    return [
        {"name": name, "description": description, "columns": _column_docs(fields, extra, units="yards")}
        for name, description, fields, extra in specs
    ]


def _tables(docs: Sequence[tuple[str, str, Sequence[tuple[str, str]]]]) -> list[dict[str, object]]:
    return [
        {
            "name": name,
            "description": description,
            "columns": [{"name": column, "description": text} for column, text in columns],
        }
        for name, description, columns in docs
    ]


_GENERATORS: Final = {
    "statsbomb": statsbomb_tables,
    "idsse": lambda: _tables(idsse.bronze_docs()),
    "skillcorner": lambda: _tables(skillcorner.bronze_docs()),
    "gradientsports": lambda: _tables(gradientsports.bronze_docs()),
    "wyscout": lambda: _tables(wyscout.bronze_docs()),
    "pining": lambda: _tables(pining.bronze_docs()),
}


def write(provider: str) -> Path:
    if provider not in _GENERATORS:
        raise SystemExit(f"no documentation generator for {provider!r} yet")
    target = ROOT / "dbt" / "models" / "staging" / provider / f"_{provider}__sources.yml"
    target.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "version": 2,
        "sources": [
            {
                "name": f"bronze_{provider}",
                "description": f"Bronze tables of {provider}, written by the v2 parser. Generated by "
                "scripts/document_bronze.py from the parser's field declarations; do not edit by hand.",
                "database": "lake",  # the DuckLake attach of the run's dbt profile
                "schema": "main",
                "tables": _GENERATORS[provider](),
            }
        ],
    }
    text = yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=120)
    target.write_bytes(text.encode("utf-8"))  # LF on every OS, as .gitattributes stores it
    return target


if __name__ == "__main__":
    print(write(sys.argv[1] if len(sys.argv) > 1 else "statsbomb"))
