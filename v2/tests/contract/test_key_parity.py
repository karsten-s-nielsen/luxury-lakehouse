"""Key parity with v1 (spec §14.1, §23.6 "Key parity").

The fixture holds known v1 keys read from the v1 Lakebase dims at Gate G-A, public-source ids only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from lakehouse.ids.keys import competition_key, match_key, player_key, surrogate_key, team_key

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "keys" / "v1_parity.json"


def _rows() -> list[dict[str, Any]]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["rows"]


def test_fixture_covers_every_entity_with_null_cases() -> None:
    rows = _rows()
    for entity in ("match", "player", "team", "competition"):
        assert sum(1 for row in rows if row["entity"] == entity and row["native_id"] is not None) >= 3
    for entity in ("player", "team", "competition"):
        assert any(row["entity"] == entity and row["native_id"] is None for row in rows)
    assert all(row["source"] for row in rows)


@pytest.mark.parametrize("row", _rows(), ids=lambda row: f"{row['entity']}-{row['provider']}-{row['native_id']}")
def test_v1_parity_values(row: dict[str, Any]) -> None:
    assert surrogate_key(row["provider"], row["native_id"]) == row["key"]


def test_named_keys_use_the_same_formula() -> None:
    assert player_key("statsbomb", "4355") == surrogate_key("statsbomb", "4355")
    assert team_key("skillcorner", "4177") == surrogate_key("skillcorner", "4177")
    assert competition_key("idsse", "DFL-COM-000002") == surrogate_key("idsse", "DFL-COM-000002")
    assert player_key("statsbomb", None) is None


def test_known_check_value() -> None:
    assert match_key("idsse", "J03WMX") == 4203055498971744044


def test_match_key_rejects_empty() -> None:
    with pytest.raises(ValueError):
        match_key("idsse", "")


def test_unknown_provider_rejected() -> None:
    with pytest.raises(ValueError, match="metrica"):
        surrogate_key("metrica", "Sample_Game_1")  # type: ignore[arg-type]
