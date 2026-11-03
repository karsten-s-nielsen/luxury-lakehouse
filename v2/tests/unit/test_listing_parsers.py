"""The listing protocol of the provider parsers (spec §10.5; plan Task 21): the table of each listing kind, the
per-record visibility column, and the rows."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse.parsers.base import ListingParser
from lakehouse.parsers.gradientsports import COMPETITIONS as GS_COMPETITIONS
from lakehouse.parsers.gradientsports import PLAYER_LIST, GradientSportsParser
from lakehouse.parsers.pining import PiningListingParser
from lakehouse.parsers.skillcorner import MATCH_LIST, SkillCornerParser
from lakehouse.parsers.statsbomb import COMPETITIONS, MATCHES, StatsBombParser
from lakehouse.parsers.wyscout import LISTING_TABLES, WyscoutParser

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
CASES = [
    (StatsBombParser(), "competitions", "statsbomb/competitions.json", COMPETITIONS),
    (StatsBombParser(), "matches/43/106", "statsbomb/matches_43_106.json", MATCHES),
    (SkillCornerParser(), "matches", "skillcorner/matches.json", MATCH_LIST),
    (GradientSportsParser(), "competitions", "gradientsports/competitions.csv", GS_COMPETITIONS),
    (GradientSportsParser(), "players", "gradientsports/players.csv", PLAYER_LIST),
    (WyscoutParser(), "teams", "wyscout/teams.json", LISTING_TABLES["teams"]),
    (WyscoutParser(), "tag_names", "wyscout/tags2name.csv", LISTING_TABLES["tag_names"]),
]


@pytest.mark.parametrize(("parser", "kind", "name", "table"), CASES)
def test_listing_protocol(parser: object, kind: str, name: str, table: str) -> None:
    assert isinstance(parser, ListingParser)
    assert parser.listing_table(kind) == table
    assert parser.listing_visibility(kind) is None  # a public source: the source's visibility
    rows = parser.listing(kind, (FIXTURES / name).read_bytes(), run_id="r", source_hash="h" * 64)
    assert rows.num_rows > 0
    assert set(rows.column("_source_hash").to_pylist()) == {"h" * 64}


@pytest.mark.parametrize("parser", [StatsBombParser(), SkillCornerParser(), GradientSportsParser(), WyscoutParser()])
def test_unknown_listing_kind_fails(parser: ListingParser) -> None:
    with pytest.raises(ValueError, match="listing"):
        parser.listing_table("no_such_listing")


def test_pining_listing_parser_conforms() -> None:
    parser = PiningListingParser("idsse")
    assert isinstance(parser, ListingParser)
    assert parser.listing_visibility("players") == "visibility"
    assert parser.listing_table("matches") == "idsse_pining_matches"
