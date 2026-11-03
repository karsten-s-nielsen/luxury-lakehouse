"""Native id format contracts (spec §7.3, §14.4; v1 ADR-018), ported from v1 src/tests/test_format_contract.py.

Every value that flows into a (provider, native_id) key has one canonical generator. v1's Metrica cases and its two
checks that parse v1 dbt model files are not ported: Metrica is not a v2 provider, and v2 has no such model files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.ids.keys import season_key
from lakehouse.ids.native import (
    PROVIDER_RANK,
    NativeCompetitionId,
    NativeMatchId,
    NativePlayerId,
    NativeTeamId,
    gradientsports_native_competition_id,
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
    idsse_native_competition_id,
    idsse_native_match_id,
    idsse_native_player_id,
    idsse_native_team_id,
    native_season_id,
    skillcorner_native_competition_id,
    skillcorner_native_match_id,
    skillcorner_native_player_id,
    skillcorner_native_team_id,
    statsbomb_native_competition_id,
    statsbomb_native_match_id,
    statsbomb_native_player_id,
    statsbomb_native_team_id,
    wyscout_native_competition_id,
    wyscout_native_match_id,
    wyscout_native_player_id,
    wyscout_native_team_id,
)

INVENTORY = Path(__file__).resolve().parents[1] / "fixtures" / "channels" / "inventory.json"


def _inventory() -> dict[str, Any]:
    return json.loads(INVENTORY.read_text(encoding="utf-8"))["pairs"]


# --- ported from v1: IDSSE -------------------------------------------------------------------------------------


def test_idsse_match_id_format() -> None:
    assert idsse_native_match_id("J03WMX") == "J03WMX"
    assert re.match(r"^[A-Z0-9]+$", idsse_native_match_id("J03WN1"))


def test_idsse_match_id_rejects_prefix() -> None:
    with pytest.raises(ValueError, match="invalid IDSSE match id"):
        idsse_native_match_id("idsse_J03WMX")


def test_idsse_competition_id_format() -> None:
    assert idsse_native_competition_id("DFL-COM-000001") == "DFL-COM-000001"
    assert re.match(r"^DFL-COM-[A-Z0-9]+$", idsse_native_competition_id("DFL-COM-000002"))


def test_idsse_player_and_team_ids() -> None:
    assert idsse_native_player_id("DFL-OBJ-002G1Q") == "DFL-OBJ-002G1Q"
    assert idsse_native_team_id("DFL-CLU-000002") == "DFL-CLU-000002"
    with pytest.raises(ValueError):
        idsse_native_player_id("not-a-dfl-id")
    with pytest.raises(ValueError):
        idsse_native_team_id("not-a-clu-id")


# --- ported from v1: StatsBomb and Wyscout ---------------------------------------------------------------------


def test_statsbomb_ids() -> None:
    assert statsbomb_native_match_id(3788741) == "3788741"
    assert statsbomb_native_player_id(3009) == "3009"
    assert statsbomb_native_team_id(217) == "217"
    assert statsbomb_native_competition_id(43) == "43"
    with pytest.raises(ValueError):
        statsbomb_native_player_id(0)
    with pytest.raises(ValueError):
        statsbomb_native_match_id(-5)


def test_wyscout_ids() -> None:
    assert wyscout_native_match_id(2057954) == "2057954"
    assert wyscout_native_player_id(25413) == "25413"
    assert wyscout_native_team_id(1610) == "1610"
    assert wyscout_native_competition_id(28) == "28"
    with pytest.raises(ValueError):
        wyscout_native_player_id(-1)


# --- ported from v1: SkillCorner and Gradient Sports -----------------------------------------------------------


@pytest.mark.parametrize(
    ("generator", "label"),
    [
        (skillcorner_native_match_id, "SkillCorner match id"),
        (skillcorner_native_player_id, "SkillCorner player id"),
        (skillcorner_native_team_id, "SkillCorner team id"),
        (skillcorner_native_competition_id, "SkillCorner competition id"),
        (gradientsports_native_match_id, "Gradient Sports match id"),
        (gradientsports_native_player_id, "Gradient Sports player id"),
        (gradientsports_native_team_id, "Gradient Sports team id"),
        (gradientsports_native_competition_id, "Gradient Sports competition id"),
    ],
)
def test_numeric_string_generators(generator: Any, label: str) -> None:
    assert generator("1886347") == "1886347"
    assert generator(1886347) == "1886347"
    for bad in ("", "abc123", "x_1886347"):
        with pytest.raises(ValueError, match=f"invalid {label}"):
            generator(bad)


def test_named_tuples() -> None:
    assert NativeMatchId.skillcorner("1886347") == NativeMatchId(provider="skillcorner", value="1886347")
    assert NativePlayerId.gradientsports("38673") == NativePlayerId(provider="gradientsports", value="38673")
    assert NativeTeamId.statsbomb(914) == NativeTeamId(provider="statsbomb", value="914")
    assert NativeCompetitionId.idsse("DFL-COM-000001").value == "DFL-COM-000001"
    assert NativeCompetitionId.gradientsports("38").value == "38"
    assert NativeCompetitionId.statsbomb(43).value == "43"


def test_provider_rank() -> None:
    assert PROVIDER_RANK == ("statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports")


# --- v2 additions: season grain (§14.4) ------------------------------------------------------------------------


def test_season_grain() -> None:
    # StatsBomb season 281 (2023/2024) under five competitions in competitions.json, read 2026-10-26.
    competitions = ["9", "37", "131", "135", "182"]
    keys = {season_key("statsbomb", native_season_id("statsbomb", c, "281")) for c in competitions}
    assert len(keys) == 5
    assert native_season_id("statsbomb", "9", "281") == "9:281"


def test_native_season_id_rejects_separators() -> None:
    for competition, season in (("9|1", "281"), ("9", "2:81"), ("9:1", "281"), ("9", "28|1")):
        with pytest.raises(ValueError):
            native_season_id("statsbomb", competition, season)


# --- v2 additions: channel mappings (§7.3) ---------------------------------------------------------------------


def test_channel_mapping_idsse() -> None:
    assert canonical_native_match_id("idsse", "pining", "DFL-MAT-J03WMX") == "J03WMX"
    assert canonical_native_match_id("idsse", "public", "J03WMX") == "J03WMX"


def test_channel_mapping_skillcorner_identity() -> None:
    assert canonical_native_match_id("skillcorner", "pining", "1899585") == "1899585"
    assert canonical_native_match_id("skillcorner", "public", "1899585") == "1899585"


def test_channel_mapping_statsbomb() -> None:
    for sample in _inventory()["statsbomb"]["public"]["samples"]:
        canonical = canonical_native_match_id("statsbomb", "public", sample)
        assert canonical == str(sample)
        assert canonical_native_match_id("statsbomb", "pining", str(sample)) == canonical


def test_channel_mapping_gradientsports() -> None:
    for sample in _inventory()["gradientsports"]["public"]["samples"]:
        assert canonical_native_match_id("gradientsports", "pining", sample) == sample
        assert canonical_native_match_id("gradientsports", "public", int(sample)) == sample


def test_channel_mapping_rejects_unknown_channel_forms() -> None:
    with pytest.raises(ValueError):
        canonical_native_match_id("statsbomb", "pining", "SB-3788741")
    with pytest.raises(ValueError):
        canonical_native_match_id("idsse", "pining", "DFL-XXX-J03WMX")
    with pytest.raises(ValueError, match="wyscout"):
        canonical_native_match_id("wyscout", "pining", "2057954")


def test_inventory_covers_every_pair() -> None:
    pairs = _inventory()
    assert set(pairs) == {"statsbomb", "idsse", "skillcorner", "gradientsports"}
    for name, pair in pairs.items():
        assert pair["overlap_evidence"], name
        assert len(pair["public"]["samples"]) >= 3, name
        assert pair["public"]["evidence"] and pair["pining"]["evidence"], name
        assert pair["canonical"], name
        assert pair["pining"]["samples"] or pair["pining"]["private_id_pattern"], name
