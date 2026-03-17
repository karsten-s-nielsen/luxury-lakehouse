"""Pure NumPy set encoder for freeze-frame context in xG v2.

Training uses PyTorch (scripts/train_xg_v2_hf.py on HF Jobs).
Inference uses pure NumPy — no PyTorch, no ONNX, zero new dependencies.

Architecture: Deep Sets (Zaheer et al. 2017)
    per-player MLP → sum aggregation → prediction MLP with MC dropout

References:
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Gal, Y. & Ghahramani, Z. (2016). "Dropout as a Bayesian Approximation." ICML.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SetEncoderConfig:
    """Immutable configuration for the set encoder architecture."""

    player_feature_dim: int = 4
    encoder_hidden: int = 32
    context_dim: int = 16
    pred_hidden_1: int = 64
    pred_hidden_2: int = 32
    dropout_p: float = 0.1
    n_mc_samples: int = 50


# ---------------------------------------------------------------------------
# Activation functions
# ---------------------------------------------------------------------------


def _relu(x: npt.NDArray[np.floating[Any]]) -> npt.NDArray[np.floating[Any]]:
    """ReLU activation: max(x, 0)."""
    return np.maximum(x, 0)  # type: ignore[return-value]


def _sigmoid(x: npt.NDArray[np.floating[Any]]) -> npt.NDArray[np.floating[Any]]:
    """Numerically stable sigmoid: 1 / (1 + exp(-clip(x, -50, 50)))."""
    x_clipped = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x_clipped))  # type: ignore[return-value]


def _linear(
    x: npt.NDArray[np.floating[Any]],
    weight: npt.NDArray[np.floating[Any]],
    bias: npt.NDArray[np.floating[Any]],
) -> npt.NDArray[np.floating[Any]]:
    """Linear layer: x @ weight.T + bias."""
    return x @ weight.T + bias  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Set encoder (per-player MLP → sum aggregation)
# ---------------------------------------------------------------------------


def encode_player_set(
    player_features: npt.NDArray[np.floating[Any]],
    weights: dict[str, npt.NDArray[np.floating[Any]]],
    *,
    config: SetEncoderConfig | None = None,
) -> npt.NDArray[np.floating[Any]]:
    """Encode a variable-size set of players into a fixed-size context vector.

    Architecture:
        Linear(4→32) → ReLU → Linear(32→16) → ReLU → sum aggregation

    Args:
        player_features: Array of shape (N_players, 4) with columns
            [x_norm, y_norm, is_keeper, is_teammate].
        weights: Weight dict with encoder_fc1_*, encoder_fc2_* keys.
        config: Encoder configuration. Uses defaults if None.

    Returns:
        Context vector of shape (context_dim,). Zero vector if input is empty.
    """
    if config is None:
        config = SetEncoderConfig()

    # Handle empty player set → zero context vector
    if player_features.shape[0] == 0:
        return np.zeros(config.context_dim, dtype=np.float64)

    # Per-player MLP: Linear(4→32) → ReLU → Linear(32→16) → ReLU
    h = _relu(_linear(player_features, weights["encoder_fc1_weight"], weights["encoder_fc1_bias"]))
    h = _relu(_linear(h, weights["encoder_fc2_weight"], weights["encoder_fc2_bias"]))

    # Sum aggregation (permutation invariant)
    context: npt.NDArray[np.floating[Any]] = np.sum(h, axis=0)  # type: ignore[assignment]
    return context


# ---------------------------------------------------------------------------
# Prediction MLP with optional MC dropout
# ---------------------------------------------------------------------------


def predict_xg(
    tabular_features: npt.NDArray[np.floating[Any]],
    context_vector: npt.NDArray[np.floating[Any]],
    weights: dict[str, npt.NDArray[np.floating[Any]]],
    *,
    dropout_mask: npt.NDArray[np.floating[Any]] | None = None,
    config: SetEncoderConfig | None = None,
) -> float:
    """Run the prediction MLP to produce an xG probability.

    Architecture:
        Concat(tabular, context) → Linear(→64) → ReLU → Dropout
        → Linear(→32) → ReLU → Dropout → Linear(→1) → Sigmoid

    Args:
        tabular_features: 1-D array of tabular shot features.
        context_vector: Context vector from ``encode_player_set``.
        weights: Weight dict with pred_fc1_*, pred_fc2_*, pred_fc3_* keys.
        dropout_mask: Optional binary mask for MC dropout. When provided,
            hidden activations are multiplied by the mask and scaled by
            1/(1-p) (inverted dropout). First ``pred_hidden_1`` elements
            mask layer 1; next ``pred_hidden_2`` mask layer 2.
        config: Encoder configuration. Uses defaults if None.

    Returns:
        xG probability in [0, 1].
    """
    if config is None:
        config = SetEncoderConfig()

    # Concatenate tabular features with context vector
    x = np.concatenate([tabular_features, context_vector])

    # Layer 1: Linear → ReLU → (optional dropout)
    h1 = _relu(_linear(x.reshape(1, -1), weights["pred_fc1_weight"], weights["pred_fc1_bias"])).ravel()
    if dropout_mask is not None:
        mask_1 = dropout_mask[: config.pred_hidden_1]
        h1 = h1 * mask_1 / (1.0 - config.dropout_p)

    # Layer 2: Linear → ReLU → (optional dropout)
    h2 = _relu(_linear(h1.reshape(1, -1), weights["pred_fc2_weight"], weights["pred_fc2_bias"])).ravel()
    if dropout_mask is not None:
        mask_2 = dropout_mask[config.pred_hidden_1 : config.pred_hidden_1 + config.pred_hidden_2]
        h2 = h2 * mask_2 / (1.0 - config.dropout_p)

    # Output layer: Linear → Sigmoid
    logit = _linear(h2.reshape(1, -1), weights["pred_fc3_weight"], weights["pred_fc3_bias"]).ravel()
    prob = _sigmoid(logit)

    return float(prob[0])


# ---------------------------------------------------------------------------
# MC dropout uncertainty estimation
# ---------------------------------------------------------------------------


def predict_xg_with_uncertainty(
    tabular_features: npt.NDArray[np.floating[Any]],
    context_vector: npt.NDArray[np.floating[Any]],
    weights: dict[str, npt.NDArray[np.floating[Any]]],
    *,
    config: SetEncoderConfig | None = None,
    random_state: int = 42,
) -> tuple[float, float, float, float]:
    """MC dropout inference for xG with uncertainty estimates.

    Runs ``config.n_mc_samples`` forward passes with random dropout masks
    to approximate Bayesian posterior (Gal & Ghahramani 2016).

    Args:
        tabular_features: 1-D array of tabular shot features.
        context_vector: Context vector from ``encode_player_set``.
        weights: Weight dict with all prediction layer keys.
        config: Encoder configuration. Uses defaults if None.
        random_state: Seed for reproducible dropout masks.

    Returns:
        Tuple of (mean, std, ci_lower, ci_upper) where CI is 95%
        (mean +/- 1.96*std), clipped to [0, 1].
    """
    if config is None:
        config = SetEncoderConfig()

    rng = np.random.default_rng(random_state)
    total_mask_dim = config.pred_hidden_1 + config.pred_hidden_2

    predictions = np.empty(config.n_mc_samples, dtype=np.float64)
    for i in range(config.n_mc_samples):
        # Generate Bernoulli dropout mask (1 = keep, 0 = drop)
        mask = rng.binomial(1, 1.0 - config.dropout_p, size=total_mask_dim).astype(np.float64)
        predictions[i] = predict_xg(
            tabular_features,
            context_vector,
            weights,
            dropout_mask=mask,
            config=config,
        )

    mean = float(np.mean(predictions))
    std = float(np.std(predictions))
    ci_lower = float(np.clip(mean - 1.96 * std, 0.0, 1.0))
    ci_upper = float(np.clip(mean + 1.96 * std, 0.0, 1.0))

    return mean, std, ci_lower, ci_upper


# ---------------------------------------------------------------------------
# Serialization (JSON + base64, no pickle)
# ---------------------------------------------------------------------------


def serialize_set_encoder_weights(weights: dict[str, npt.NDArray[np.floating[Any]]]) -> bytes:
    """Serialize set encoder weights to JSON bytes with base64-encoded arrays.

    Envelope structure:
    - ``model_type``: ``"set_encoder_xg_v2"``
    - ``weights``: dict mapping layer name to ``{"data": base64_str, "shape": list, "dtype": str}``

    No pickle is used (banned by project security policy).
    """
    serialized_weights: dict[str, dict[str, Any]] = {}
    for key, arr in weights.items():
        arr_bytes = arr.astype(np.float64).tobytes()
        serialized_weights[key] = {
            "data": base64.b64encode(arr_bytes).decode("ascii"),
            "shape": list(arr.shape),
            "dtype": "float64",
        }

    envelope: dict[str, Any] = {
        "model_type": "set_encoder_xg_v2",
        "weights": serialized_weights,
    }

    return json.dumps(envelope).encode("utf-8")


def deserialize_set_encoder_weights(data: bytes) -> dict[str, npt.NDArray[np.floating[Any]]]:
    """Deserialize JSON bytes back to a weight dict.

    Each array is reconstructed via ``np.frombuffer`` with ``.copy()``
    to ensure the result is writeable.

    No pickle is used (banned by project security policy).
    """
    envelope = json.loads(data.decode("utf-8"))
    serialized_weights: dict[str, dict[str, Any]] = envelope["weights"]

    weights: dict[str, npt.NDArray[np.floating[Any]]] = {}
    for key, meta in serialized_weights.items():
        raw = base64.b64decode(meta["data"])
        arr = np.frombuffer(raw, dtype=np.float64).copy()
        weights[key] = arr.reshape(meta["shape"])

    return weights
