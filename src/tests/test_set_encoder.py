"""Tests for the pure NumPy set encoder analytics module."""

from __future__ import annotations

import json

import numpy as np
import numpy.testing as npt

from analytics.set_encoder import (
    SetEncoderConfig,
    deserialize_set_encoder_weights,
    encode_player_set,
    predict_xg,
    predict_xg_with_uncertainty,
    serialize_set_encoder_weights,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_random_weights(
    config: SetEncoderConfig,
    tabular_dim: int = 13,
    seed: int = 42,
) -> dict[str, np.ndarray]:
    """Create random weight matrices matching the set encoder architecture."""
    rng = np.random.default_rng(seed)

    return {
        "encoder_fc1_weight": rng.standard_normal((config.encoder_hidden, config.player_feature_dim)),
        "encoder_fc1_bias": rng.standard_normal((config.encoder_hidden,)),
        "encoder_fc2_weight": rng.standard_normal((config.context_dim, config.encoder_hidden)),
        "encoder_fc2_bias": rng.standard_normal((config.context_dim,)),
        "pred_fc1_weight": rng.standard_normal((config.pred_hidden_1, config.context_dim + tabular_dim)),
        "pred_fc1_bias": rng.standard_normal((config.pred_hidden_1,)),
        "pred_fc2_weight": rng.standard_normal((config.pred_hidden_2, config.pred_hidden_1)),
        "pred_fc2_bias": rng.standard_normal((config.pred_hidden_2,)),
        "pred_fc3_weight": rng.standard_normal((1, config.pred_hidden_2)),
        "pred_fc3_bias": rng.standard_normal((1,)),
    }


def _make_player_features(n_players: int, seed: int = 123) -> np.ndarray:
    """Create synthetic player features: [x_norm, y_norm, is_keeper, is_teammate]."""
    rng = np.random.default_rng(seed)
    features = np.zeros((n_players, 4), dtype=np.float64)
    features[:, 0] = rng.uniform(0, 1, n_players)  # x_norm
    features[:, 1] = rng.uniform(0, 1, n_players)  # y_norm
    features[:, 2] = 0.0  # is_keeper (set first player as keeper below)
    features[:, 3] = rng.choice([0.0, 1.0], n_players)  # is_teammate
    if n_players > 0:
        features[0, 2] = 1.0  # first player is keeper
    return features


# ---------------------------------------------------------------------------
# TestEncodePlayerSet
# ---------------------------------------------------------------------------


class TestEncodePlayerSet:
    def test_empty_set_returns_zeros(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        empty_features = np.zeros((0, 4), dtype=np.float64)

        context = encode_player_set(empty_features, weights, config=config)

        assert context.shape == (config.context_dim,)
        npt.assert_allclose(context, np.zeros(config.context_dim))

    def test_single_player(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        player = _make_player_features(1)

        context = encode_player_set(player, weights, config=config)

        assert context.shape == (config.context_dim,)
        # At least some activations should be non-zero with random weights
        assert np.any(context != 0.0)

    def test_permutation_invariance(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        players = _make_player_features(5, seed=99)

        context_original = encode_player_set(players, weights, config=config)

        # Shuffle player order
        rng = np.random.default_rng(777)
        shuffled_idx = rng.permutation(5)
        players_shuffled = players[shuffled_idx]

        context_shuffled = encode_player_set(players_shuffled, weights, config=config)

        npt.assert_allclose(context_original, context_shuffled, atol=1e-10)

    def test_variable_size_input(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config)

        context_3 = encode_player_set(_make_player_features(3), weights, config=config)
        context_10 = encode_player_set(_make_player_features(10), weights, config=config)

        # Both should produce the same output shape
        assert context_3.shape == (config.context_dim,)
        assert context_10.shape == (config.context_dim,)


# ---------------------------------------------------------------------------
# TestPredictXG
# ---------------------------------------------------------------------------


class TestPredictXG:
    def test_output_range(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)
        tabular = np.random.default_rng(42).standard_normal(13)
        context = np.random.default_rng(43).standard_normal(config.context_dim)

        result = predict_xg(tabular, context, weights, config=config)

        assert 0.0 <= result <= 1.0

    def test_deterministic_without_dropout(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)
        tabular = np.random.default_rng(42).standard_normal(13)
        context = np.random.default_rng(43).standard_normal(config.context_dim)

        result_1 = predict_xg(tabular, context, weights, config=config)
        result_2 = predict_xg(tabular, context, weights, config=config)

        assert result_1 == result_2


# ---------------------------------------------------------------------------
# TestMCDropoutUncertainty
# ---------------------------------------------------------------------------


class TestMCDropoutUncertainty:
    def test_returns_four_values(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)
        tabular = np.random.default_rng(42).standard_normal(13)
        context = np.random.default_rng(43).standard_normal(config.context_dim)

        result = predict_xg_with_uncertainty(tabular, context, weights, config=config)

        assert len(result) == 4
        mean, std, ci_lower, ci_upper = result
        assert 0.0 <= mean <= 1.0
        assert std >= 0.0
        assert 0.0 <= ci_lower <= 1.0
        assert 0.0 <= ci_upper <= 1.0

    def test_ci_contains_mean(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)
        tabular = np.random.default_rng(42).standard_normal(13)
        context = np.random.default_rng(43).standard_normal(config.context_dim)

        mean, _std, ci_lower, ci_upper = predict_xg_with_uncertainty(tabular, context, weights, config=config)

        assert ci_lower <= mean <= ci_upper

    def test_reproducible_with_seed(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)
        tabular = np.random.default_rng(42).standard_normal(13)
        context = np.random.default_rng(43).standard_normal(config.context_dim)

        result_1 = predict_xg_with_uncertainty(tabular, context, weights, config=config, random_state=99)
        result_2 = predict_xg_with_uncertainty(tabular, context, weights, config=config, random_state=99)

        assert result_1 == result_2


# ---------------------------------------------------------------------------
# TestSerialization
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_roundtrip(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)

        serialized = serialize_set_encoder_weights(weights)
        restored = deserialize_set_encoder_weights(serialized)

        assert set(restored.keys()) == set(weights.keys())
        for key in weights:
            npt.assert_array_equal(restored[key], weights[key])

    def test_no_pickle_in_bytes(self) -> None:
        config = SetEncoderConfig()
        weights = _make_random_weights(config, tabular_dim=13)

        serialized = serialize_set_encoder_weights(weights)

        # Pickle protocol magic bytes must not appear
        assert b"\x80\x05" not in serialized
        # Should be valid JSON starting with '{'
        assert serialized[:1] == b"{"
        parsed = json.loads(serialized.decode("utf-8"))
        assert parsed["model_type"] == "set_encoder_xg_v2"
