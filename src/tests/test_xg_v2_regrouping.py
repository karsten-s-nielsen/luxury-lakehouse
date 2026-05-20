"""Regrouping correctness tests for xG v2 (OPT-3 sub-item c).

Verifies that the groupBy key is ``match_key`` (bounded at 25-50 shots/group)
and that the temp-table materialization hack has been removed. Also tests
UDF output preservation of ``competition_id`` across per-match groups.
"""

from __future__ import annotations

import inspect

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Source-code structural guards
# ---------------------------------------------------------------------------


class TestXgV2SourceCodeGuards:
    """Source-code assertions that prevent regression to competition_id grouping."""

    def test_load_shots_includes_match_key(self) -> None:
        """_load_shots_with_context SQL must SELECT s.match_key."""
        from ingestion.xg_model_v2 import _load_shots_with_context

        source = inspect.getsource(_load_shots_with_context)
        assert "s.match_key" in source, (
            "_load_shots_with_context must include s.match_key in the SELECT list "
            "so that groupBy('match_key') has a column to group on."
        )

    def test_groupby_uses_match_key(self) -> None:
        """run_pipeline must groupBy('match_key'), not 'competition_id'."""
        from ingestion.xg_model_v2 import run_pipeline

        source = inspect.getsource(run_pipeline)
        assert 'groupBy("match_key")' in source, "run_pipeline must use groupBy('match_key') for bounded group sizing."
        assert 'groupBy("competition_id")' not in source, (
            "run_pipeline must NOT use groupBy('competition_id') — "
            "competition 11 has 21,186 shots in one group (OOM risk)."
        )

    def test_no_temp_table_reference(self) -> None:
        """run_pipeline must not reference _xg_v2_scored_temp."""
        from ingestion.xg_model_v2 import run_pipeline

        source = inspect.getsource(run_pipeline)
        assert "_xg_v2_scored_temp" not in source, (
            "Temp table materialization was a workaround for per-competition "
            "DAG re-execution. With match_key grouping + single bulk write, "
            "the temp table is unnecessary."
        )

    def test_docstring_says_match_key(self) -> None:
        """Module docstring must reference match_key, not competition_id grouping."""
        from ingestion import xg_model_v2

        docstring = xg_model_v2.__doc__ or ""
        assert "match_key" in docstring, "Module docstring must mention 'match_key' as the grouping key."
        assert "grouped by `competition_id`" not in docstring, (
            "Module docstring still says 'grouped by competition_id' — stale."
        )


# ---------------------------------------------------------------------------
# UDF correctness: competition_id preserved across per-match groups
# ---------------------------------------------------------------------------


def _make_synthetic_shots(n: int = 500, *, random_state: int = 42) -> pd.DataFrame:
    """Create realistic synthetic shot data for testing.

    Mirrors ``test_xg_model_v2._make_synthetic_shots`` but adds ``match_key``
    and ``shot_id`` columns needed for regrouping tests.
    """
    rng = np.random.default_rng(random_state)

    body_parts = ["Right Foot", "Left Foot", "Head"]
    techniques = ["Normal", "Volley", "Half Volley", "Overhead Kick"]
    shot_types = ["Open Play", "Free Kick", "Penalty", "Corner"]
    play_patterns: list[str | None] = ["Regular Play", "From Corner", "From Free Kick", None]

    distance = rng.uniform(5, 50, n)
    angle = rng.uniform(0.05, 1.5, n)
    base_prob = np.clip(0.3 - 0.005 * distance + 0.1 * angle, 0.02, 0.95)
    is_goal = rng.binomial(1, base_prob)

    return pd.DataFrame(
        {
            "shot_id": [f"shot_{i}" for i in range(n)],
            "competition_id": rng.choice([11, 2, 7], n),
            "match_key": rng.choice([1001, 1002, 1003, 2001, 2002, 2003], n),
            "player_id": rng.integers(1000, 9999, n),
            "team_id": rng.choice([10, 20, 30, 40], n),
            "distance_to_goal": distance,
            "shot_angle": angle,
            "location_x": rng.uniform(90, 120, n),
            "location_y": rng.uniform(10, 70, n),
            "end_location_x": rng.uniform(118, 121, n),
            "end_location_y": rng.uniform(30, 50, n),
            "period": rng.choice([1, 2], n),
            "minute": rng.integers(0, 90, n),
            "is_first_time": rng.choice(np.array([True, False, None], dtype=object), n),
            "shot_body_part": rng.choice(body_parts, n),
            "shot_technique": rng.choice(techniques, n),
            "shot_type": rng.choice(shot_types, n),
            "play_pattern": rng.choice(np.array(play_patterns, dtype=object), n),
            "is_goal": is_goal,
            "data_source": ["statsbomb"] * n,
            "shot_freeze_frame": [None] * n,
        }
    )


class TestUdfPreservesCompetitionId:
    """applyInPandas with groupBy('match_key') must preserve competition_id."""

    def test_competition_id_preserved_per_shot(self) -> None:
        """Every shot's competition_id must survive the UDF round-trip."""
        from analytics.xg_model import (
            XGModelConfig,
            build_features,
            serialize_xgboost_model,
            train_xgboost_model,
        )

        # Train a tiny XGBoost model for the UDF
        train_shots = _make_synthetic_shots(100, random_state=0)
        config = XGModelConfig()
        x, y = build_features(train_shots, config)
        model = train_xgboost_model(x, y, config)
        xgboost_bytes = serialize_xgboost_model(model)

        # Build dummy v2 weights using the production serializer
        import json

        from analytics.set_encoder import SetEncoderConfig, serialize_set_encoder_weights
        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        cc = next(iter(model.calibrated_classifiers_))
        xgb_features = list(cc.estimator.get_booster().feature_names)  # type: ignore[union-attr]
        tabular_dim = len(xgb_features)

        enc_config = SetEncoderConfig()
        rng_w = np.random.default_rng(42)
        weights_dict: dict[str, np.ndarray] = {
            "encoder_fc1_weight": rng_w.standard_normal((enc_config.encoder_hidden, enc_config.player_feature_dim)),
            "encoder_fc1_bias": rng_w.standard_normal(enc_config.encoder_hidden),
            "encoder_fc2_weight": rng_w.standard_normal((enc_config.context_dim, enc_config.encoder_hidden)),
            "encoder_fc2_bias": rng_w.standard_normal(enc_config.context_dim),
        }
        pred_input_dim = tabular_dim + enc_config.context_dim
        weights_dict.update(
            {
                "pred_fc1_weight": rng_w.standard_normal((enc_config.pred_hidden_1, pred_input_dim)),
                "pred_fc1_bias": rng_w.standard_normal(enc_config.pred_hidden_1),
                "pred_fc2_weight": rng_w.standard_normal((enc_config.pred_hidden_2, enc_config.pred_hidden_1)),
                "pred_fc2_bias": rng_w.standard_normal(enc_config.pred_hidden_2),
                "pred_fc3_weight": rng_w.standard_normal((1, enc_config.pred_hidden_2)),
                "pred_fc3_bias": rng_w.standard_normal(1),
            }
        )
        weight_bytes = serialize_set_encoder_weights(weights_dict)
        envelope = json.loads(weight_bytes.decode("utf-8"))
        envelope["feature_names"] = [f"feat_{i}" for i in range(tabular_dim)]
        envelope["tabular_dim"] = tabular_dim
        v2_weights_bytes = json.dumps(envelope).encode("utf-8")

        scoring_udf = _make_v2_scoring_udf(v2_weights_bytes, xgboost_bytes)

        # Build a test DataFrame: 3 competitions, 2 matches each, ~5 shots/match
        test_shots = _make_synthetic_shots(30, random_state=99)
        # Assign deterministic match_key -> competition_id mapping
        test_shots["match_key"] = [1001, 1002, 2001, 2002, 3001, 3002] * 5
        test_shots["competition_id"] = test_shots["match_key"].map(
            {1001: 11, 1002: 11, 2001: 2, 2002: 2, 3001: 7, 3002: 7}
        )

        # Simulate what groupBy("match_key").applyInPandas does:
        # call the UDF once per match_key group
        results = []
        for _mk, group in test_shots.groupby("match_key"):
            result = scoring_udf(group)
            results.append(result)
        output = pd.concat(results, ignore_index=True)

        # Assert: output row count == input row count
        assert len(output) == len(test_shots), f"Row count mismatch: input={len(test_shots)}, output={len(output)}"

        # Assert: every competition_id from input appears in output
        assert set(output["competition_id"]) == set(test_shots["competition_id"]), (
            "competition_id values in output don't match input"
        )

        # Assert: no duplicate shot_ids (uniqueness preserved)
        assert output["shot_id"].is_unique, "Duplicate shot_ids in output"

        # Assert: competition_id values match per shot_id
        input_map = dict(zip(test_shots["shot_id"], test_shots["competition_id"], strict=True))
        output_map = dict(zip(output["shot_id"], output["competition_id"], strict=True))
        for sid in input_map:
            assert input_map[sid] == output_map[sid], (
                f"competition_id mismatch for shot {sid}: input={input_map[sid]}, output={output_map[sid]}"
            )
