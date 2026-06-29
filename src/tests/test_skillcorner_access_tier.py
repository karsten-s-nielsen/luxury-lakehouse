"""SkillCorner match-info carries raw visibility + derived access_tier (spec §6.2 / C1)."""

from __future__ import annotations

import json

from ingestion.skillcorner_matches import parse_match_json

_MATCH = {
    "home_team": {"id": 1, "name": "A"},
    "away_team": {"id": 2, "name": "B"},
    "competition_edition": {"competition": {"id": 10, "name": "C"}, "season": {"id": 20, "name": "S"}},
    "players": [{"id": 100, "team_id": 1}],
}


def test_parse_stamps_public_visibility_and_tier() -> None:
    df = parse_match_json(json.dumps(_MATCH), match_id="1886347", visibility="public")
    assert (df["visibility"] == "public").all()
    assert (df["access_tier"] == "public").all()


def test_parse_stamps_private_to_restricted() -> None:
    df = parse_match_json(json.dumps(_MATCH), match_id="1886347", visibility="private")
    assert (df["visibility"] == "private").all()
    assert (df["access_tier"] == "restricted").all()
