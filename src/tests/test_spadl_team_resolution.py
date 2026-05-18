"""Unit tests for team_id hash resolution in tracking-provider SPADL converters.

Validates that the hash_native_id_to_bigint function produces correct, consistent,
and distinct team_id values from team_id_native strings — the core invariant that
VAEP scoring depends on (same-team equality comparison).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from ingestion.spadl_adapter import UNKNOWN_TEAM_SENTINEL, hash_native_id_to_bigint

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Realistic native team IDs per provider
_IDSSE_HOME = "DFL-CLU-000008"
_IDSSE_AWAY = "DFL-CLU-00000G"
_METRICA_HOME = "metrica_Sample_Game_1_home"
_METRICA_AWAY = "metrica_Sample_Game_1_away"
_SKILLCORNER_HOME = "1805"
_SKILLCORNER_AWAY = "1806"


# ---------------------------------------------------------------------------
# Helper: replicates the production team_id resolution pattern
# ---------------------------------------------------------------------------


def _apply_team_id_hash(actions: pd.DataFrame, match_id_str: str) -> pd.DataFrame:
    """Replicate the team_id resolution logic applied in spadl_conversion.py.

    WARNING: This is a test-local copy of the production pattern. If the
    production logic in _make_idsse_spadl_udf / _make_metrica_spadl_udf /
    _make_skillcorner_spadl_udf changes, this helper must be updated too.
    The structural regression guard in test_spadl_vaep.py catches divergence
    by asserting the NULL-fill pattern is absent from production code.
    """
    _logger = logging.getLogger(__name__)
    null_team_mask = actions["team_id_native"].isna()
    if null_team_mask.any():
        _logger.warning(
            "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). Filling with sentinel hash.",
            null_team_mask.sum(),
            match_id_str,
            actions.loc[null_team_mask, "type_id"].unique().tolist(),
        )
        actions.loc[null_team_mask, "team_id_native"] = UNKNOWN_TEAM_SENTINEL
    actions["team_id"] = actions["team_id_native"].map(hash_native_id_to_bigint).astype("Int64")
    return actions


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTeamIdPopulated:
    """team_id is non-NULL and correctly hashed for each provider."""

    def test_team_id_populated_for_idsse_actions(self) -> None:
        """IDSSE DFL-CLU-* native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_IDSSE_HOME, _IDSSE_AWAY, _IDSSE_HOME, _IDSSE_AWAY],
                    dtype="string",
                ),
                "type_id": [0, 0, 1, 8],
            }
        )
        result = _apply_team_id_hash(actions, "J03WMX")
        assert result["team_id"].notna().all()
        # Same native → same hash
        home_ids = result.loc[result["team_id_native"] == _IDSSE_HOME, "team_id"]
        assert home_ids.nunique() == 1

    def test_team_id_populated_for_metrica_actions(self) -> None:
        """Metrica synthetic native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_METRICA_HOME, _METRICA_AWAY, _METRICA_HOME],
                    dtype="string",
                ),
                "type_id": [0, 0, 11],
            }
        )
        result = _apply_team_id_hash(actions, "Sample_Game_1")
        assert result["team_id"].notna().all()

    def test_team_id_populated_for_skillcorner_actions(self) -> None:
        """SkillCorner numeric-string native IDs produce non-NULL team_id hashes."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_SKILLCORNER_HOME, _SKILLCORNER_AWAY, _SKILLCORNER_HOME],
                    dtype="string",
                ),
                "type_id": [0, 1, 11],
            }
        )
        result = _apply_team_id_hash(actions, "1234567")
        assert result["team_id"].notna().all()


class TestSentinelFill:
    """NULL team_id_native rows get sentinel hash instead of crash."""

    def test_team_id_null_native_fills_sentinel(self, caplog: pytest.LogCaptureFixture) -> None:
        """NULL team_id_native rows get deterministic sentinel hash with warning."""
        actions = pd.DataFrame(
            {
                "team_id_native": pd.array(
                    [_IDSSE_HOME, pd.NA, _IDSSE_AWAY, pd.NA],
                    dtype="string",
                ),
                "type_id": [0, 4, 0, 4],  # type_id=4 = freekick_short
            }
        )
        with caplog.at_level(logging.WARNING):
            result = _apply_team_id_hash(actions, "J03WMX")

        # No NULL team_id in output
        assert result["team_id"].notna().all()

        # Sentinel hash is deterministic
        sentinel_hash = hash_native_id_to_bigint(UNKNOWN_TEAM_SENTINEL)
        sentinel_rows = result.loc[result["team_id"] == sentinel_hash]
        assert len(sentinel_rows) == 2

        # Sentinel differs from both real team hashes
        home_hash = hash_native_id_to_bigint(_IDSSE_HOME)
        away_hash = hash_native_id_to_bigint(_IDSSE_AWAY)
        assert sentinel_hash != home_hash
        assert sentinel_hash != away_hash

        # Warning logged with match context
        assert "NULL team_id_native in 2 rows" in caplog.text
        assert "J03WMX" in caplog.text
        assert "4" in caplog.text  # type_id in warning


class TestHashProperties:
    """Hash function produces correct equality semantics for VAEP."""

    def test_two_teams_produce_distinct_hashes(self) -> None:
        """Different team_id_native values produce different team_id hashes."""
        pairs = [
            (_IDSSE_HOME, _IDSSE_AWAY),
            (_METRICA_HOME, _METRICA_AWAY),
            (_SKILLCORNER_HOME, _SKILLCORNER_AWAY),
        ]
        for native_a, native_b in pairs:
            assert hash_native_id_to_bigint(native_a) != hash_native_id_to_bigint(native_b), (
                f"collision: {native_a} == {native_b}"
            )

    def test_hash_is_deterministic(self) -> None:
        """Same team_id_native always produces the same team_id hash."""
        for native in [_IDSSE_HOME, _METRICA_AWAY, _SKILLCORNER_HOME, UNKNOWN_TEAM_SENTINEL]:
            h1 = hash_native_id_to_bigint(native)
            h2 = hash_native_id_to_bigint(native)
            assert h1 == h2, f"non-deterministic hash for {native}"

    def test_hash_is_positive_bigint(self) -> None:
        """Hash values are positive integers that fit in a BIGINT column."""
        for native in [_IDSSE_HOME, _METRICA_AWAY, _SKILLCORNER_HOME, UNKNOWN_TEAM_SENTINEL]:
            h = hash_native_id_to_bigint(native)
            assert isinstance(h, int)
            assert h > 0
            assert h < 2**63  # fits in signed BIGINT
