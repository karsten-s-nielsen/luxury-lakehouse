"""Integration tests: VAEP scoring on tracking-provider data with hashed team_id.

Validates the full pipeline: team_id hash → SPADL features → VAEP scoring.
Uses test-trained XGBoost models (no MLflow/UC dependency) to verify that
non-NULL team_id produces non-NULL VAEP values.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd
import pytest

from ingestion.spadl_adapter import hash_native_id_to_bigint

# ---------------------------------------------------------------------------
# Fixture: realistic SPADL actions for two teams
# ---------------------------------------------------------------------------

_HOME_NATIVE = "DFL-CLU-000008"
_AWAY_NATIVE = "DFL-CLU-00000G"
_HOME_TEAM_ID = hash_native_id_to_bigint(_HOME_NATIVE)
_AWAY_TEAM_ID = hash_native_id_to_bigint(_AWAY_NATIVE)
_MATCH_ID = hash_native_id_to_bigint("J03WMX")


def _build_two_team_fixture(n_actions: int = 100) -> pd.DataFrame:
    """Build a realistic SPADL DataFrame with two teams and valid sequencing.

    Produces alternating possessions with proper time progression, action types,
    and all columns required by the VAEP scoring UDF (including native/enrichment
    columns that are NULL for tracking providers).
    """
    rng = np.random.default_rng(42)

    # Alternating team possession (blocks of 5-10 actions)
    team_ids = []
    current_team = _HOME_TEAM_ID
    i = 0
    while i < n_actions:
        block_size = rng.integers(5, 11)
        team_ids.extend([current_team] * min(block_size, n_actions - i))
        i += block_size
        current_team = _AWAY_TEAM_ID if current_team == _HOME_TEAM_ID else _HOME_TEAM_ID

    # Action types: pass=0, dribble=1, cross=2, shot=11, tackle=8, interception=9
    type_ids = rng.choice([0, 0, 0, 1, 1, 2, 8, 9, 11], size=n_actions)
    # Results: success=1, fail=0
    result_ids = rng.choice([0, 1], size=n_actions, p=[0.3, 0.7])
    # Body parts: foot=0, head=1
    bodypart_ids = rng.choice([0, 0, 0, 1], size=n_actions)

    # Time progression: monotonic within each period (split at 60%)
    n_p1 = int(n_actions * 0.6)
    n_p2 = n_actions - n_p1
    period_ids = np.array([1] * n_p1 + [2] * n_p2)
    time_secs_p1 = np.sort(rng.uniform(0, 2700, size=n_p1))
    time_secs_p2 = np.sort(rng.uniform(2700, 5400, size=n_p2))
    time_seconds = np.concatenate([time_secs_p1, time_secs_p2])

    # Coordinates on 105x68 pitch
    start_x = rng.uniform(0, 105, size=n_actions)
    start_y = rng.uniform(0, 68, size=n_actions)
    end_x = start_x + rng.uniform(-10, 10, size=n_actions)
    end_y = start_y + rng.uniform(-5, 5, size=n_actions)
    end_x = np.clip(end_x, 0, 105)
    end_y = np.clip(end_y, 0, 68)

    df = pd.DataFrame(
        {
            "game_id": _MATCH_ID,
            "match_id": _MATCH_ID,
            "original_event_id": range(n_actions),
            "period_id": period_ids,
            "time_seconds": time_seconds,
            "team_id": pd.array(team_ids[:n_actions], dtype="Int64"),
            "player_id": pd.array(rng.integers(1000, 2000, size=n_actions).tolist(), dtype="Int64"),
            "start_x": start_x,
            "start_y": start_y,
            "end_x": end_x,
            "end_y": end_y,
            "type_id": type_ids,
            "result_id": result_ids,
            "bodypart_id": bodypart_ids,
            "action_id": range(n_actions),
            "competition_id": pd.array([1] * n_actions, dtype="Int64"),
            "season_id": pd.array([2025] * n_actions, dtype="Int64"),
            "data_source": "idsse",
        }
    )

    # Add all columns the UDF output projection expects (NULL for tracking providers)
    _null_string_cols = [
        "statsbomb_possession_id",
        "statsbomb_possession_team_id",
        "statsbomb_play_pattern",
        "statsbomb_under_pressure",
        "possession_id_heuristic",
        "gk_role",
        "gk_was_distributing",
        "gk_was_engaged",
        "gk_actions_in_possession",
        "defending_gk_player_id",
        "team_id_native",
        "home_team_id_native",
        "competition_native_id",
        "season_native_id",
        "match_id_native",
        "player_id_native",
        "tackle_winner_player_id_native",
        "tackle_winner_player_key",
        "tackle_winner_team_id_native",
        "tackle_winner_team_key",
        "tackle_loser_player_id_native",
        "tackle_loser_player_key",
        "tackle_loser_team_id_native",
        "tackle_loser_team_key",
    ]
    for col in _null_string_cols:
        df[col] = pd.NA

    # silly-kicks 4.13.0 (sk ADR-018): is_synthetic is a non-NULL bool (False on
    # genuine observed actions — all of this fixture), not part of the NA fill.
    df["is_synthetic"] = False

    # silly-kicks 4.21.0/4.22.0 (ADR-048): result_source + restart-coordinate enrichment —
    # present on every post-migration bronze row (the UDF's output reindex selects strictly).
    df["result_source"] = pd.NA
    for col in ("enriched_start_x", "enriched_start_y", "enriched_end_x", "enriched_end_y"):
        df[col] = float("nan")
    for col in ("start_coord_source", "end_coord_source"):
        df[col] = pd.NA
    for col in ("start_coord_confidence", "end_coord_confidence"):
        df[col] = float("nan")

    # Populate team_id_native (realistic for tracking providers)
    df["team_id_native"] = df["team_id"].map({_HOME_TEAM_ID: _HOME_NATIVE, _AWAY_TEAM_ID: _AWAY_NATIVE})

    return df


# ---------------------------------------------------------------------------
# Test-trained model helper
# ---------------------------------------------------------------------------


def _train_test_models(x: pd.DataFrame) -> tuple[bytearray, bytearray]:
    """Train trivial XGBoost classifiers on fixture features and return raw bytes.

    Returns (scores_raw, concedes_raw) suitable for _make_scoring_udf.
    """
    from xgboost import XGBClassifier

    n = len(x)
    rng = np.random.default_rng(99)

    # Binary labels (random — we test non-NULL output, not model accuracy)
    y_scores = rng.integers(0, 2, size=n)
    y_concedes = rng.integers(0, 2, size=n)

    model_scores = XGBClassifier(n_estimators=2, max_depth=2, random_state=42)
    model_scores.fit(x, y_scores)

    model_concedes = XGBClassifier(n_estimators=2, max_depth=2, random_state=42)
    model_concedes.fit(x, y_concedes)

    return (
        model_scores.get_booster().save_raw("json"),
        model_concedes.get_booster().save_raw("json"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestVaepNonNullForTrackingProviders:
    """VAEP scoring produces non-NULL values when team_id is properly hashed."""

    def test_vaep_non_null_for_two_team_fixture(self) -> None:
        """Full pipeline: hashed team_id → features → VAEP values are non-NULL."""
        pytest.importorskip("silly_kicks")

        import silly_kicks.spadl as spadl
        import silly_kicks.vaep.features as fs

        actions = _build_two_team_fixture(100)
        named = spadl.add_names(actions)

        # Extract features (same pipeline as production)
        nb_prev = 3
        gamestates = fs.gamestates(named, nb_prev_actions=nb_prev)
        feature_fns = [
            fs.actiontype_onehot,
            fs.result_onehot,
            fs.bodypart_onehot,
            fs.time,
            fs.startlocation,
            fs.endlocation,
            fs.startpolar,
            fs.endpolar,
            fs.movement,
            fs.team,
            fs.time_delta,
        ]
        x = pd.concat([fn(gamestates) for fn in feature_fns], axis=1)

        # Train test models on this feature set
        scores_raw, concedes_raw = _train_test_models(x)

        # Score using the production UDF constructor
        from ingestion.spadl_vaep import _make_scoring_udf

        scoring_udf: Callable[..., pd.DataFrame] = _make_scoring_udf(scores_raw, concedes_raw)  # type: ignore[assignment]
        result = scoring_udf(actions)

        # VAEP values should be non-NULL for all but boundary actions
        # (last nb_prev actions per period cannot have full game states)
        vaep_cols = ["offensive_value", "defensive_value", "vaep_value"]
        for col in vaep_cols:
            assert col in result.columns, f"Missing column: {col}"

        null_count = result["vaep_value"].isna().sum()
        # At most nb_prev NaN per period boundary (2 periods x nb_prev = 6 max)
        n_periods = actions["period_id"].nunique()
        max_expected_nan = n_periods * nb_prev
        assert null_count <= max_expected_nan, (
            f"Too many NULL VAEP values: {null_count} > {max_expected_nan} "
            f"(expected at most {nb_prev} per period boundary)"
        )

    def test_vaep_raises_on_null_team_id(self) -> None:
        """VAEP scoring raises RuntimeError when team_id is NULL (defense-in-depth)."""
        pytest.importorskip("silly_kicks")
        pytest.importorskip("xgboost")

        import silly_kicks.spadl as spadl
        import silly_kicks.vaep.features as fs

        # Build fixture with NULL team_id (the broken state we're preventing)
        actions = _build_two_team_fixture(50)
        actions["team_id"] = pd.array([pd.NA] * len(actions), dtype="Int64")

        named = spadl.add_names(actions)
        gamestates = fs.gamestates(named, nb_prev_actions=3)
        feature_fns = [
            fs.actiontype_onehot,
            fs.result_onehot,
            fs.bodypart_onehot,
            fs.time,
            fs.startlocation,
            fs.endlocation,
            fs.startpolar,
            fs.endpolar,
            fs.movement,
            fs.team,
            fs.time_delta,
        ]
        x = pd.concat([fn(gamestates) for fn in feature_fns], axis=1)
        scores_raw, concedes_raw = _train_test_models(x)

        from ingestion.spadl_vaep import _make_scoring_udf

        scoring_udf: Callable[..., pd.DataFrame] = _make_scoring_udf(scores_raw, concedes_raw)  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="NULL team_id"):
            scoring_udf(actions)
