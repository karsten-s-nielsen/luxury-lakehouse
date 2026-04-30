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
    idsse_native_competition_id,
    idsse_native_match_id,
    metrica_native_competition_id,
    metrica_native_match_id,
    metrica_native_season_id,
    metrica_native_team_id,
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
    on the 5 fct_action_values not_null tests was missed."""

    _DEFERRED_COLUMNS: ClassVar[list[str]] = [
        "player_id",
        "team_id",
        "vaep_value",
        "offensive_value",
        "defensive_value",
    ]

    @pytest.mark.parametrize("col_name", _DEFERRED_COLUMNS)
    def test_mart_not_null_filter_present(self, col_name: str) -> None:
        """``_marts__models.yml`` must scope the deferred not_null tests on
        ``fct_action_values`` to ``data_source IN ('statsbomb', 'wyscout')``
        pending PR-LL3 player Kimball mapping."""
        models_yml = _REPO_ROOT / "dbt_project" / "models" / "marts" / "_marts__models.yml"
        data = yaml.safe_load(models_yml.read_text())
        fct_av = next(m for m in data["models"] if m["name"] == "fct_action_values")
        col = next((c for c in fct_av["columns"] if c["name"] == col_name), None)
        assert col is not None, f"column {col_name!r} not found in fct_action_values"
        tests = col.get("data_tests", [])
        # Find the not_null test entry — may be string 'not_null' or dict {'not_null': {...}}
        not_null_entry = None
        for t in tests:
            if t == "not_null" or (isinstance(t, dict) and "not_null" in t):
                not_null_entry = t
                break
        assert not_null_entry is not None, f"Bug #5: {col_name!r} on fct_action_values has no not_null test"
        # Bug #5 fix: not_null must be wrapped in a dict with config.where filter.
        assert isinstance(not_null_entry, dict), (
            f"Bug #5: {col_name!r} not_null must be a dict with where: filter "
            f"`data_source IN ('statsbomb', 'wyscout')` pending PR-LL3"
        )
        cfg = not_null_entry["not_null"].get("config", {})
        where_clause = cfg.get("where", "")
        assert "statsbomb" in where_clause and "wyscout" in where_clause, (
            f"Bug #5: {col_name!r} not_null where: filter must scope to "
            f"data_source IN ('statsbomb', 'wyscout'), got: {where_clause!r}"
        )
