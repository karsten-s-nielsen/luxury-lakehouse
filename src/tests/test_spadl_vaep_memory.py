"""tracemalloc smoke test for SPADL/VAEP scoring UDF (OPT-3 sub-item b).

Verified safe: max group = 3,236 rows/match (~1 MB), already grouped by
(match_id, data_source). This test provides a regression guard -- if a
future change inflates per-group memory, the test catches it before
production OOM on the 800 MB serverless cap.

Threshold derivation: measured actual peak, set at 2x measured baseline.
"""

from __future__ import annotations

import tracemalloc

import numpy as np
import pandas as pd
import pytest


def _build_synthetic_spadl_group(n_rows: int = 2711, *, random_state: int = 42) -> pd.DataFrame:
    """Build a synthetic SPADL DataFrame at p99 group size.

    Must include all columns the scoring UDF reads: the full set from
    ``_SPADL_SCHEMA`` minus ``_ingested_at`` (not passed through applyInPandas).
    """
    rng = np.random.default_rng(random_state)

    action_types = list(range(22))  # silly_kicks action type IDs
    result_ids = [0, 1]  # fail/success
    bodypart_ids = [0, 1, 2]  # foot, head, other

    return pd.DataFrame(
        {
            "game_id": np.int64(1001),
            "match_id": np.int64(1001),
            "original_event_id": [f"evt_{i}" for i in range(n_rows)],
            "period_id": rng.choice([1, 2], n_rows).astype(np.int64),
            "time_seconds": np.sort(rng.uniform(0, 5400, n_rows)),
            "team_id": rng.choice([10, 20], n_rows).astype(np.int64),
            "player_id": rng.choice(range(100, 122), n_rows).astype(np.int64),
            "start_x": rng.uniform(0, 105, n_rows),
            "start_y": rng.uniform(0, 68, n_rows),
            "end_x": rng.uniform(0, 105, n_rows),
            "end_y": rng.uniform(0, 68, n_rows),
            "type_id": rng.choice(action_types, n_rows).astype(np.int64),
            "result_id": rng.choice(result_ids, n_rows).astype(np.int64),
            "bodypart_id": rng.choice(bodypart_ids, n_rows).astype(np.int64),
            "action_id": rng.integers(0, 100000, n_rows).astype(np.int64),
            "competition_id": np.int64(11),
            "season_id": np.int64(90),
            "data_source": "statsbomb",
            # StatsBomb-native fields (NULL for non-StatsBomb, but we test StatsBomb path)
            "statsbomb_possession_id": rng.integers(1, 200, n_rows).astype(np.int64),
            "statsbomb_possession_team_id": rng.choice([10, 20], n_rows).astype(np.int64),
            "statsbomb_play_pattern": rng.choice(["Regular Play", "From Corner", "From Free Kick"], n_rows),
            "statsbomb_under_pressure": rng.choice(np.array([True, False, None], dtype=object), n_rows),
            # Enrichment columns
            "possession_id_heuristic": rng.integers(1, 200, n_rows).astype(np.int64),
            "gk_role": rng.choice(np.array(["goalkeeper", None], dtype=object), n_rows),
            "gk_was_distributing": rng.choice(np.array([True, False, None], dtype=object), n_rows),
            "gk_was_engaged": rng.choice(np.array([True, False, None], dtype=object), n_rows),
            "gk_actions_in_possession": rng.integers(0, 5, n_rows).astype(np.int64),
            "defending_gk_player_id": rng.choice(np.array([100, 110, None], dtype=object), n_rows),
            # Native string identifiers
            "team_id_native": rng.choice(["10", "20"], n_rows),
            "home_team_id_native": "10",
            "competition_native_id": "11",
            "season_native_id": "90",
            "match_id_native": "1001",
            "player_id_native": [str(rng.integers(100, 122)) for _ in range(n_rows)],
            # Tackle qualifier columns (NULL for StatsBomb)
            "tackle_winner_player_id_native": None,
            "tackle_winner_player_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_winner_team_id_native": None,
            "tackle_winner_team_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_loser_player_id_native": None,
            "tackle_loser_player_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            "tackle_loser_team_id_native": None,
            "tackle_loser_team_key": pd.array([None] * n_rows, dtype=pd.Int64Dtype()),
            # silly-kicks 4.13.0 (sk ADR-018): is_synthetic provenance flag. False on
            # genuine observed actions (all of this synthetic StatsBomb fixture).
            "is_synthetic": np.zeros(n_rows, dtype=bool),
        }
    )


@pytest.fixture(scope="module")
def vaep_model_bytes() -> tuple[bytes, bytes]:
    """Train tiny XGBClassifier models on synthetic SPADL data.

    Uses the real silly_kicks feature pipeline to ensure feature count
    alignment with what the production UDF expects.
    """
    import silly_kicks.spadl as spadl
    import silly_kicks.vaep.features as fs
    import silly_kicks.vaep.labels as labels
    from xgboost import XGBClassifier

    # Build a small synthetic game with enough actions for gamestates
    pdf = _build_synthetic_spadl_group(200, random_state=0)
    named = spadl.add_names(pdf)

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
    y = labels.scores(named, nr_actions=10)
    y_concedes = labels.concedes(named, nr_actions=10)

    # Align lengths (labels may be shorter)
    min_len = min(len(x), len(y), len(y_concedes))
    x = x.iloc[:min_len]
    y = y.iloc[:min_len]
    y_concedes = y_concedes.iloc[:min_len]

    m_scores = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_scores.fit(x, y.values.ravel())
    scores_raw = m_scores.get_booster().save_raw("json")

    m_concedes = XGBClassifier(n_estimators=5, max_depth=2, random_state=42)
    m_concedes.fit(x, y_concedes.values.ravel())
    concedes_raw = m_concedes.get_booster().save_raw("json")

    return bytes(scores_raw), bytes(concedes_raw)


class TestSyntheticGroupColumnParity:
    """Synthetic builder must match _SPADL_SCHEMA (minus _ingested_at)."""

    def test_columns_match_spadl_schema(self) -> None:
        import re

        from ingestion.spadl_vaep import _SPADL_SCHEMA

        expected = {m.group(1) for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]+", _SPADL_SCHEMA)} - {
            "_ingested_at"
        }
        actual = set(_build_synthetic_spadl_group(10).columns)
        assert actual == expected, (
            f"Synthetic builder drifted from _SPADL_SCHEMA.\n"
            f"  Missing: {expected - actual}\n"
            f"  Extra: {actual - expected}"
        )


class TestSpadlVaepMemory:
    """Peak memory of VAEP scoring UDF must stay under threshold."""

    def test_peak_memory_at_p99_group_size(self, vaep_model_bytes: tuple[bytes, bytes]) -> None:
        """Run UDF body at p99 group size (2,711 rows) under tracemalloc."""
        scores_raw, concedes_raw = vaep_model_bytes

        from ingestion.spadl_vaep import _make_scoring_udf

        scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)

        pdf = _build_synthetic_spadl_group(2711, random_state=42)

        tracemalloc.start()

        _ = scoring_udf(pdf)  # type: ignore[operator]

        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)
        # Print so the executor can read the measured value for threshold tuning
        print(f"\n  VAEP UDF peak memory: {peak_mb:.1f} MB at 2,711 rows")

        # Measured baseline: 8.7 MB (2026-05-20, p99=2711 rows, XGBClassifier n_est=5)
        # Threshold: 2x baseline = 18 MB
        threshold_mb = 18.0

        assert peak_mb < threshold_mb, (
            f"VAEP scoring UDF peak memory {peak_mb:.1f} MB exceeds "
            f"threshold {threshold_mb:.1f} MB at p99 group size (2,711 rows). "
            f"The 800 MB serverless cap is shared with Spark overhead, Python "
            f"runtime, and model cache -- per-UDF budget must leave room."
        )
