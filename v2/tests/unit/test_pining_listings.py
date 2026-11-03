"""The pining match and player lists as source-level listings (spec §10.5, §12; plan Task 20)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse.bronze.listings import split_listing
from lakehouse.errors import UnexpectedPayloadError, UnmappedFieldError
from lakehouse.ids.keys import match_key
from lakehouse.parsers.pining import LISTING_TABLES, PiningListingParser

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "pining"


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _listing(provider: str, kind: str, raw: bytes) -> Any:
    return PiningListingParser(provider).listing(kind, raw, run_id="r", source_hash="h" * 64)  # type: ignore[arg-type]


def test_match_list_rows_and_keys() -> None:
    raw = _raw("idsse_matches_public.json")
    table = _listing("idsse", "matches", raw)
    source = json.loads(raw)["matches"]
    assert table.num_rows == 7
    rows = table.to_pylist()
    assert [r["channel_match_id"] for r in rows] == [e["id"] for e in source]
    assert rows[0]["native_match_id"] == "J03WMX"
    assert rows[0]["match_key"] == match_key("idsse", "J03WMX")
    assert rows[0]["updated_at"] == datetime(2026, 10, 27, 1, 53, 24, tzinfo=UTC)
    assert rows[0]["source_licence"] == "CC-BY 4.0"
    assert {a["key"]: a["filename"] for a in rows[0]["artifacts"]} == source[0]["artifacts"]
    meta = {m["key"]: (m["sha256"], m["size"]) for m in rows[0]["artifact_meta"]}
    assert meta == {k: (v["sha256"], v["size"]) for k, v in source[0]["artifact_meta"].items()}
    assert set(table.column("_source_hash").to_pylist()) == {"h" * 64}


def test_skillcorner_id_prefixed_artifact_keys_are_values() -> None:
    """The legacy SkillCorner keys carry the match id; the key is a value, so it is no field (one column per field)."""
    parser = PiningListingParser("skillcorner")
    raw = _raw("skillcorner_matches_public.json")
    assert parser.field_inventory("matches", raw) <= parser.bronze_columns("matches")
    table = _listing("skillcorner", "matches", raw)
    assert table.num_rows == 20


def test_unknown_entry_field_fails() -> None:
    payload = json.loads(_raw("idsse_matches_public.json"))
    payload["matches"][0]["weather"] = "rain"
    with pytest.raises(UnmappedFieldError, match="weather"):
        _listing("idsse", "matches", json.dumps(payload).encode())


REVISION = "4340d274572876239c154c90bc507a9b3250a656"  # the SkillCorner opendata commit that pining pins (pining #79)


@pytest.mark.parametrize(
    ("kind", "name"), [("matches", "skillcorner_matches_public.json"), ("players", "skillcorner_players_public.json")]
)
def test_source_revision_is_a_column(kind: str, name: str) -> None:
    """Audit T9: pining's `_SourceMeta.revision` (the upstream git commit, on match and player records alike) is a
    column, not an unmapped field; a record without it reads null."""
    payload = json.loads(_raw(name))
    payload[kind][0]["source"]["revision"] = REVISION
    table = _listing("skillcorner", kind, json.dumps(payload).encode())
    assert table.column("source_revision").to_pylist()[:2] == [REVISION, None]


def test_listing_must_name_the_provider() -> None:
    with pytest.raises(UnexpectedPayloadError, match="names provider 'idsse', not 'skillcorner'"):
        _listing("skillcorner", "matches", _raw("idsse_matches_public.json"))


def test_missing_visibility_is_public_never_null() -> None:
    """Task 10 ruling: a pining list never emits a null flag; a missing flag means public (API reference)."""
    payload = json.loads(_raw("idsse_matches_public.json"))
    del payload["matches"][0]["visibility"]
    payload["matches"][1]["visibility"] = "private"
    table = _listing("idsse", "matches", json.dumps(payload).encode())
    assert table.column("visibility").to_pylist()[:2] == ["public", "private"]
    parts = split_listing(table, visibility_column="visibility", source_visibility="private")
    assert parts["restricted"].num_rows == 1
    assert parts["lake"].num_rows == 6


def test_player_list_public_skillcorner() -> None:
    raw = _raw("skillcorner_players_public.json")
    table = _listing("skillcorner", "players", raw)
    source = json.loads(raw)["players"]
    assert table.num_rows == len(source)
    first = table.to_pylist()[0]
    assert (first["player_id"], first["first_name"], first["visibility"]) == (
        source[0]["id"],
        source[0]["firstName"],
        "public",
    )
    assert first["player_key"] is not None
    no_dob = next(r for r, s in zip(table.to_pylist(), source, strict=True) if "dob" not in s)
    assert no_dob["dob"] is None  # absent, not null, in the source: a null here invents nothing


def test_listing_tables_named_per_provider() -> None:
    assert LISTING_TABLES[("statsbomb", "players")] == "statsbomb_pining_players"
    assert LISTING_TABLES[("idsse", "matches")] == "idsse_pining_matches"
    assert len(LISTING_TABLES) == 8
