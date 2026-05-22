"""Cross-table format-contract tests (ADR-018).

Each test asserts that a value emitted by a bronze writer matches the
format expected by the dim staging that consumes it. Catches the bug
class where bronze writer + dim staging are each correct in isolation
but drift apart at the JOIN boundary.

Test naming: ``test_<source>_<entity>_format_matches_dim``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import ClassVar

import pytest
import yaml

from shared.identifiers import (
    NativeMatchId,
    NativePlayerId,
    NativeTeamId,
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
    idsse_native_competition_id,
    idsse_native_match_id,
    idsse_native_player_id,
    idsse_native_team_id,
    metrica_native_competition_id,
    metrica_native_match_id,
    metrica_native_player_id,
    metrica_native_season_id,
    metrica_native_team_id,
    skillcorner_native_match_id,
    skillcorner_native_player_id,
    skillcorner_native_team_id,
    statsbomb_native_player_id,
    statsbomb_native_team_id,
    wyscout_native_player_id,
    wyscout_native_team_id,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Bronze writer ↔ dim staging format equality
# ---------------------------------------------------------------------------


class TestIdsseFormatContract:
    """ADR-018 contract: idsse.py bronze writer output format matches
    stg_idsse__matches's expected format."""

    def test_idsse_match_id_format_matches_dim(self) -> None:
        """bronze.idsse_events.match_id format == dim_matches.native_match_id format.

        The dim staging is ``regexp_replace(prefixed_match_id, '^idsse_', '')``
        which expects bare DFL MatchId. Our generator produces bare too.
        """
        assert idsse_native_match_id("J03WMX") == "J03WMX"
        # Format regex parity:
        assert re.match(r"^[A-Z0-9]+$", idsse_native_match_id("J03WN1"))

    def test_idsse_competition_id_format_matches_dim(self) -> None:
        # dim_competitions.idsse_competitions filters where competition_id
        # is not null; format DFL-COM-XXXXXX.
        assert idsse_native_competition_id("DFL-COM-000001") == "DFL-COM-000001"
        assert re.match(r"^DFL-COM-[A-Z0-9]+$", idsse_native_competition_id("DFL-COM-000002"))


class TestMetricaFormatContract:
    """ADR-018 contract: metrica_events.py bronze writer output matches
    stg_metrica__team_players's ``concat('metrica_', match_id, '_', side)`` format."""

    def test_metrica_match_id_format_matches_dim(self) -> None:
        assert metrica_native_match_id("Sample_Game_1") == "Sample_Game_1"

    def test_metrica_team_id_format_matches_dim(self) -> None:
        """dim_teams.metrica_anon_teams emits ``concat('metrica_', match_id, '_', side)``
        where side is 'home'/'away' lowercase. Our generator MUST produce identical strings."""
        for match_id in ("Sample_Game_1", "Sample_Game_2", "Sample_Game_3"):
            for side in ("home", "away"):
                bronze_format = metrica_native_team_id(match_id, side)  # type: ignore[arg-type]
                dim_format = f"metrica_{match_id}_{side}"
                assert bronze_format == dim_format

    def test_metrica_competition_id_format_matches_dim(self) -> None:
        # dim_competitions.metrica_competitions.native_competition_id = 'metrica-sample'
        assert metrica_native_competition_id() == "metrica-sample"

    def test_metrica_season_id_format_matches_dim(self) -> None:
        assert metrica_native_season_id() == "metrica-open-2017"


# ---------------------------------------------------------------------------
# dim_matches CTE passthrough check (Bug #4)
# ---------------------------------------------------------------------------


class TestDimMatchesMetricaPassthrough:
    """Bug #4: dim_matches.sql Metrica CTE must pass competition_id through
    instead of hardcoding NULL. Verified via SQL parsing of the model file."""

    def test_metrica_cte_passes_competition_id(self) -> None:
        """``dim_matches.sql`` ``metrica_matches`` CTE must reference ``competition_id``
        from staging instead of hardcoding NULL."""
        dim_matches_sql = (_REPO_ROOT / "dbt_project" / "models" / "marts" / "dim_matches.sql").read_text()
        # Find the metrica_matches CTE block
        match = re.search(
            r"metrica_matches as \(\s*(?:--[^\n]*\n\s*)*select(.*?)from \{\{ ref\('stg_metrica__matches'\) \}\}",
            dim_matches_sql,
            re.DOTALL,
        )
        assert match is not None, "metrica_matches CTE not found in dim_matches.sql"
        cte_body = match.group(1)
        # Must NOT hardcode null competition_id (Bug #4 surface).
        assert "cast(null as string)           as competition_id" not in cte_body, (
            "Bug #4: dim_matches.sql metrica_matches CTE still hardcodes NULL competition_id; "
            "should pass through staging's 'metrica-sample'."
        )
        # Must reference competition_id (the passthrough form):
        assert "competition_id" in cte_body, "metrica_matches CTE must reference competition_id"


# ---------------------------------------------------------------------------
# Mart-level not_null filter mirror check (Bug #5)
# ---------------------------------------------------------------------------


class TestMartLevelNotNullFilters:
    """Bug #5: PR #228 added where: filter at staging only; mart-level mirror
    on fct_action_values not_null tests was missed. PR-LL3 S1 tightens
    team_key/player_key to include 'idsse' (dim coverage verified).

    Legacy BIGINT cols (player_id, team_id) + VAEP values stay SB+WS only.
    Kimball surrogates (team_key, player_key) are SB+WS+IDSSE (Metrica
    excluded pending dim_players anonymous-ID coverage verification)."""

    # Legacy BIGINT + VAEP: inherently NULL for IDSSE/Metrica
    _SB_WS_ONLY_COLUMNS: ClassVar[list[str]] = [
        "player_id",
        "team_id",
        "vaep_value",
        "offensive_value",
        "defensive_value",
    ]

    # Kimball surrogates: IDSSE now resolves via player_id_native/team_id_native
    _SB_WS_IDSSE_COLUMNS: ClassVar[list[str]] = [
        "team_key",
        "player_key",
    ]

    @pytest.mark.parametrize("col_name", _SB_WS_ONLY_COLUMNS)
    def test_mart_not_null_filter_sb_ws_only(self, col_name: str) -> None:
        """Legacy BIGINT / VAEP not_null tests scoped to SB+WS only."""
        where_clause = self._get_not_null_where(col_name)
        assert "statsbomb" in where_clause and "wyscout" in where_clause, (
            f"{col_name!r} not_null where: filter must scope to "
            f"data_source IN ('statsbomb', 'wyscout'), got: {where_clause!r}"
        )
        assert "idsse" not in where_clause, f"{col_name!r}: legacy BIGINT/VAEP not_null should NOT include 'idsse'"

    @pytest.mark.parametrize("col_name", _SB_WS_IDSSE_COLUMNS)
    def test_mart_not_null_filter_sb_ws_idsse(self, col_name: str) -> None:
        """Kimball surrogate not_null tests include IDSSE (PR-LL3 S1)."""
        where_clause = self._get_not_null_where(col_name)
        assert "statsbomb" in where_clause and "wyscout" in where_clause and "idsse" in where_clause, (
            f"{col_name!r} not_null where: filter must scope to "
            f"data_source IN ('statsbomb', 'wyscout', 'idsse'), got: {where_clause!r}"
        )

    @staticmethod
    def _get_not_null_where(col_name: str) -> str:
        models_yml = _REPO_ROOT / "dbt_project" / "models" / "marts" / "_marts__models.yml"
        data = yaml.safe_load(models_yml.read_text())
        fct_av = next(m for m in data["models"] if m["name"] == "fct_action_values")
        col = next((c for c in fct_av["columns"] if c["name"] == col_name), None)
        assert col is not None, f"column {col_name!r} not found in fct_action_values"
        tests = col.get("data_tests", [])
        not_null_entry = None
        for t in tests:
            if t == "not_null" or (isinstance(t, dict) and "not_null" in t):
                not_null_entry = t
                break
        assert not_null_entry is not None, f"{col_name!r} on fct_action_values has no not_null test"
        assert isinstance(not_null_entry, dict), f"{col_name!r} not_null must be a dict with where: filter"
        cfg = not_null_entry["not_null"].get("config", {})
        return cfg.get("where", "")


# ---------------------------------------------------------------------------
# Player ID format contracts (PR-LL3 S2, ADR-018)
# ---------------------------------------------------------------------------


class TestPlayerIdFormatContract:
    """ADR-018: player_id_native generators produce the format that
    dim_players.native_player_id expects per source."""

    def test_statsbomb_native_player_id_valid(self) -> None:
        assert statsbomb_native_player_id(3009) == "3009"

    def test_statsbomb_native_player_id_rejects_zero(self) -> None:
        with pytest.raises(ValueError):
            statsbomb_native_player_id(0)

    def test_wyscout_native_player_id_valid(self) -> None:
        assert wyscout_native_player_id(25413) == "25413"

    def test_wyscout_native_player_id_rejects_negative(self) -> None:
        with pytest.raises(ValueError):
            wyscout_native_player_id(-1)

    def test_idsse_native_player_id_valid(self) -> None:
        assert idsse_native_player_id("DFL-OBJ-002G1Q") == "DFL-OBJ-002G1Q"

    def test_idsse_native_player_id_rejects_bad_format(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_player_id("not-a-dfl-id")

    def test_metrica_native_player_id_valid(self) -> None:
        assert metrica_native_player_id("Player11") == "Player11"

    def test_metrica_native_player_id_rejects_bad_format(self) -> None:
        with pytest.raises(ValueError):
            metrica_native_player_id("BadFormat")


# ---------------------------------------------------------------------------
# Team ID format contracts (PR-LL3 S7, ADR-018)
# ---------------------------------------------------------------------------


class TestTeamIdFormatContract:
    """ADR-018: team_id generators produce the format that
    dim_teams.native_team_id expects per source."""

    def test_statsbomb_native_team_id_valid(self) -> None:
        assert statsbomb_native_team_id(217) == "217"

    def test_wyscout_native_team_id_valid(self) -> None:
        assert wyscout_native_team_id(1610) == "1610"

    def test_idsse_native_team_id_valid(self) -> None:
        assert idsse_native_team_id("DFL-CLU-000002") == "DFL-CLU-000002"

    def test_idsse_native_team_id_rejects_bad_format(self) -> None:
        with pytest.raises(ValueError):
            idsse_native_team_id("not-a-clu-id")


# ---------------------------------------------------------------------------
# SkillCorner — ADR-018 format contracts
# ---------------------------------------------------------------------------


class TestSkillCornerFormatContract:
    def test_skillcorner_match_id_from_string(self) -> None:
        assert skillcorner_native_match_id("1886347") == "1886347"

    def test_skillcorner_match_id_from_int(self) -> None:
        assert skillcorner_native_match_id(1886347) == "1886347"

    def test_skillcorner_match_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("skillcorner_1886347")

    def test_skillcorner_match_id_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("")

    def test_skillcorner_match_id_rejects_alpha(self) -> None:
        with pytest.raises(ValueError, match="invalid SkillCorner match id"):
            skillcorner_native_match_id("abc123")


class TestSkillCornerPlayerIdFormatContract:
    def test_skillcorner_native_player_id_valid(self) -> None:
        assert skillcorner_native_player_id(38673) == "38673"

    def test_skillcorner_native_player_id_string(self) -> None:
        assert skillcorner_native_player_id("38673") == "38673"

    def test_skillcorner_native_player_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            skillcorner_native_player_id("player_38673")


class TestSkillCornerTeamIdFormatContract:
    def test_skillcorner_native_team_id_valid(self) -> None:
        assert skillcorner_native_team_id(4177) == "4177"

    def test_skillcorner_native_team_id_string(self) -> None:
        assert skillcorner_native_team_id("4177") == "4177"

    def test_skillcorner_native_team_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            skillcorner_native_team_id("team_4177")


class TestSkillCornerNamedTuples:
    def test_native_match_id_skillcorner(self) -> None:
        nid = NativeMatchId.skillcorner("1886347")
        assert nid.provider == "skillcorner"
        assert nid.value == "1886347"

    def test_native_player_id_skillcorner(self) -> None:
        nid = NativePlayerId.skillcorner("38673")
        assert nid.provider == "skillcorner"
        assert nid.value == "38673"

    def test_native_team_id_skillcorner(self) -> None:
        nid = NativeTeamId.skillcorner("4177")
        assert nid.provider == "skillcorner"
        assert nid.value == "4177"


# ---------------------------------------------------------------------------
# Gradient Sports -- ADR-018 format contracts
# ---------------------------------------------------------------------------


class TestGradientSportsFormatContract:
    def test_gradientsports_match_id_from_string(self) -> None:
        assert gradientsports_native_match_id("10502") == "10502"

    def test_gradientsports_match_id_from_int(self) -> None:
        assert gradientsports_native_match_id(10502) == "10502"

    def test_gradientsports_match_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("gs_10502")

    def test_gradientsports_match_id_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("")

    def test_gradientsports_match_id_rejects_alpha(self) -> None:
        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            gradientsports_native_match_id("abc123")


class TestGradientSportsPlayerIdFormatContract:
    def test_gradientsports_native_player_id_valid(self) -> None:
        assert gradientsports_native_player_id(38673) == "38673"

    def test_gradientsports_native_player_id_string(self) -> None:
        assert gradientsports_native_player_id("38673") == "38673"

    def test_gradientsports_native_player_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            gradientsports_native_player_id("player_38673")


class TestGradientSportsTeamIdFormatContract:
    def test_gradientsports_native_team_id_valid(self) -> None:
        assert gradientsports_native_team_id(4177) == "4177"

    def test_gradientsports_native_team_id_string(self) -> None:
        assert gradientsports_native_team_id("4177") == "4177"

    def test_gradientsports_native_team_id_rejects_prefix(self) -> None:
        with pytest.raises(ValueError):
            gradientsports_native_team_id("team_4177")


class TestGradientSportsNamedTuples:
    def test_native_match_id_gradientsports(self) -> None:
        nid = NativeMatchId.gradientsports("10502")
        assert nid.provider == "gradientsports"
        assert nid.value == "10502"

    def test_native_player_id_gradientsports(self) -> None:
        nid = NativePlayerId.gradientsports("38673")
        assert nid.provider == "gradientsports"
        assert nid.value == "38673"

    def test_native_team_id_gradientsports(self) -> None:
        nid = NativeTeamId.gradientsports("4177")
        assert nid.provider == "gradientsports"
        assert nid.value == "4177"


class TestNativeCompetitionIdWrappers:
    def test_native_competition_id_idsse(self) -> None:
        from shared.identifiers import NativeCompetitionId

        nid = NativeCompetitionId.idsse("DFL-COM-000001")
        assert nid.provider == "idsse"
        assert nid.value == "DFL-COM-000001"

    def test_native_competition_id_metrica(self) -> None:
        from shared.identifiers import NativeCompetitionId

        nid = NativeCompetitionId.metrica()
        assert nid.provider == "metrica"
        assert nid.value == "metrica-sample"

    def test_native_competition_id_gradientsports(self) -> None:
        from shared.identifiers import NativeCompetitionId

        nid = NativeCompetitionId.gradientsports("38")
        assert nid.provider == "gradientsports"
        assert nid.value == "38"


class TestGradientSportsCompetitionIdFormatContract:
    def test_gradientsports_native_competition_id_valid_string(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id

        assert gradientsports_native_competition_id("38") == "38"

    def test_gradientsports_native_competition_id_valid_int(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id

        assert gradientsports_native_competition_id(38) == "38"

    def test_gradientsports_native_competition_id_rejects_alpha(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id

        with pytest.raises(ValueError, match="invalid Gradient Sports competition id"):
            gradientsports_native_competition_id("abc")

    def test_gradientsports_native_competition_id_rejects_empty(self) -> None:
        from shared.identifiers import gradientsports_native_competition_id

        with pytest.raises(ValueError, match="invalid Gradient Sports competition id"):
            gradientsports_native_competition_id("")
