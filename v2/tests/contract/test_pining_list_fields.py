"""§23.6 "Pining list fields" (§29.1): for every pining fixture match, the list entry's competition and season equal
the values the provider parser reads from the match's own metadata artifact (plan Task 20)."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.net.http import Expect, HttpResult
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.raw.archive import Delivery
from lakehouse.runs.list_fields import list_fields_mismatch
from lakehouse.sources.pining import PiningAdapter, PiningToken

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
TOKEN = PiningToken.of("owner-token-value-for-tests", "owner")  # pragma: allowlist secret - a test value


class _Listing:
    def __init__(self, provider: str, entries: list[dict[str, Any]]) -> None:
        self.body = json.dumps({"provider": provider, "matches": entries}).encode()

    def get(self, url: str, *, headers: Any = None, max_bytes: int, expect: Expect) -> HttpResult:
        return HttpResult(status=200, body=self.body, headers={}, final_url=url)


def _entries(provider: str, entries: list[dict[str, Any]]) -> list[SourceEntry]:
    module = ModuleConfig(module_id=f"{provider}_pining", active=True, filter=[AllClause()])  # type: ignore[arg-type]
    adapter = PiningAdapter(
        provider,  # type: ignore[arg-type]
        http=_Listing(provider, entries),
        base_url="https://pining.example.test/v1",
        token=TOKEN,
        module=module,
        clock=lambda: NOW,
    )
    return list(adapter.list_entries())


def _delivery(provider: str, artifact: str, raw: bytes) -> Delivery:
    return Delivery.received(
        provider=provider,  # type: ignore[arg-type]
        channel="pining",
        native_match_id="x",
        artifact=artifact,
        raw=raw,
        visibility="public",
        change_signal=None,
        fetched_at=NOW,
    )


def _idsse() -> list[tuple[SourceEntry, ParsedMatch]]:
    recorded = json.loads((ROOT / "pining" / "idsse_matches_public.json").read_bytes())["matches"]
    out: list[tuple[SourceEntry, ParsedMatch]] = []
    for entry in _entries("idsse", recorded):
        raw = (ROOT / "idsse" / f"matchinformation_{entry.native_match_id}.xml").read_bytes()
        out.append((entry, IdsseParser().parse(entry, {"metadata": _delivery("idsse", "metadata", raw)}, run_id="r")))
    return out


def _skillcorner() -> list[tuple[SourceEntry, ParsedMatch]]:
    recorded = json.loads((ROOT / "pining" / "skillcorner_matches_public.json").read_bytes())["matches"]
    out: list[tuple[SourceEntry, ParsedMatch]] = []
    for entry in _entries("skillcorner", recorded):
        path = ROOT / "skillcorner" / f"match_{entry.native_match_id}.json"
        if path.exists():
            delivery = _delivery("skillcorner", "match", path.read_bytes())
            out.append((entry, SkillCornerParser().parse(entry, {"match": delivery}, run_id="r")))
    return out


def _gradientsports() -> list[tuple[SourceEntry, ParsedMatch]]:
    raw = (ROOT / "gradientsports" / "metadata_9001.json").read_bytes()
    meta = json.loads(raw)[0]
    listed = {
        "id": "9001",
        "artifacts": {"metadata": "metadata.json"},
        "visibility": "private",
        "updated_at": "2026-10-27T00:00:00Z",
        "competition_id": str(meta["competition"]["id"]),
        "competition_name": meta["competition"]["name"],
        "season_name": meta["season"],
    }
    entry = _entries("gradientsports", [listed])[0]
    roster = (ROOT / "gradientsports" / "roster_9001.json").read_bytes()
    deliveries = {
        "metadata": _delivery("gradientsports", "metadata", raw),
        "roster": _delivery("gradientsports", "roster", roster),
    }
    return [(entry, GradientSportsParser().parse(entry, deliveries, run_id="r"))]


def _statsbomb() -> list[tuple[SourceEntry, ParsedMatch]]:
    raw = (ROOT / "statsbomb" / "commercial_metadata_9100001.json").read_bytes()
    meta = json.loads(raw)
    listed = {
        "id": "9100001",
        "artifacts": {"events": "events.json.gz", "metadata": "metadata.json"},
        "visibility": "private",
        "updated_at": "2026-10-27T00:00:00Z",
        "competition_id": str(meta["competition"]["competition_id"]),
        "competition_name": meta["competition"]["competition_name"],
        "season_id": str(meta["season"]["season_id"]),
        "season_name": meta["season"]["season_name"],
    }
    entry = _entries("statsbomb", [listed])[0]
    events = (ROOT / "statsbomb" / "commercial_events_9100001.json").read_bytes()
    deliveries = {
        "events": _delivery("statsbomb", "events", events),
        "metadata": _delivery("statsbomb", "metadata", raw),
    }
    return [(entry, StatsBombParser().parse(entry, deliveries, run_id="r"))]


CASES = {"idsse": _idsse, "skillcorner": _skillcorner, "gradientsports": _gradientsports, "statsbomb": _statsbomb}


@pytest.mark.parametrize("provider", sorted(CASES))
def test_list_fields_equal_metadata(provider: str) -> None:
    pairs = CASES[provider]()
    assert pairs
    for entry, parsed in pairs:
        assert list_fields_mismatch(entry, parsed.names) == [], entry.native_match_id


def test_a_mismatch_is_reported_and_an_absent_field_is_not() -> None:
    entry, parsed = _idsse()[0]
    changed = dataclasses.replace(entry, season_name="2099/2100", competition_id=None)
    assert list_fields_mismatch(changed, parsed.names) == [
        "season_name: the list says '2099/2100', the metadata '2022/2023'"
    ]


def test_names_without_one_competition_row_is_a_mismatch() -> None:
    entry, parsed = _idsse()[0]
    empty = dataclasses.replace(parsed.names, competitions=parsed.names.competitions.slice(0, 0))
    assert list_fields_mismatch(entry, empty) == ["the metadata names 0 competitions"]
