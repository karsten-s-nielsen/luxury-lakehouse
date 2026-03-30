"""Football2Vec tokenizer, training, and inference.

Implements the player embedding pipeline from Theiner et al. (2022):
  1. Tokenize match events into spatial action tokens (e.g. "pass_6_4")
  2. Train Doc2Vec (Le & Mikolov 2014) on per-player-match token sequences
  3. Infer fixed-length player embedding vectors

Uses the unified SPADL 23-type action vocabulary from ``fct_action_values``.
Coordinates are in the SPADL 105x68 meter system.

References:
  - Theiner et al. (2022) "Football2Vec" — spatial tokenization + Doc2Vec
  - Le & Mikolov (2014) "Distributed Representations of Sentences and Documents"
  - Decroos et al. (2019) "Actions Speak Louder than Goals" — SPADL action types
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Any, cast

import pandas as pd
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SPADL 23-type action vocabulary (from fct_action_values)
# ---------------------------------------------------------------------------

SPADL_ACTION_TYPES: frozenset[str] = frozenset(
    {
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "foul",
        "tackle",
        "interception",
        "shot",
        "shot_penalty",
        "shot_freekick",
        "keeper_save",
        "keeper_claim",
        "keeper_punch",
        "keeper_pick_up",
        "clearance",
        "bad_touch",
        "non_action",
        "dribble",
        "goalkick",
    }
)


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TokenizerConfig:
    """Grid dimensions and pitch parameters for spatial tokenization."""

    grid_cols: int = 12
    grid_rows: int = 8
    pitch_length: float = 105.0
    pitch_width: float = 68.0


@dataclass(frozen=True)
class TrainingConfig:
    """Doc2Vec hyperparameters for player embedding training."""

    vector_size: int = 32
    window: int = 5
    min_count: int = 2
    epochs: int = 20
    dm: int = 1


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------


def tokenize_event(
    event: dict[str, Any],
    config: TokenizerConfig | None = None,
) -> str | None:
    """Convert a single SPADL action to a spatial grid token.

    Token format: ``{action_type}_{grid_x}_{grid_y}``

    Args:
        event: Dict with keys ``action_type``, ``start_x``, ``start_y``.
            Coordinates in SPADL 105x68 meter system.
        config: Optional tokenizer config (default: 12x8 grid on 105x68).

    Returns:
        Token string, or None if coordinates are missing/invalid.
    """
    cfg = config or TokenizerConfig()

    x_val = event.get("start_x")
    y_val = event.get("start_y")
    if x_val is None or y_val is None:
        return None
    if isinstance(x_val, float) and math.isnan(x_val):
        return None
    if isinstance(y_val, float) and math.isnan(y_val):
        return None

    cell_w = cfg.pitch_length / cfg.grid_cols
    cell_h = cfg.pitch_width / cfg.grid_rows
    gx = min(int(float(x_val) / cell_w), cfg.grid_cols - 1)
    gy = min(int(float(y_val) / cell_h), cfg.grid_rows - 1)

    action = event.get("action_type", "non_action")
    return f"{action}_{gx}_{gy}"


def tokenize_match_events(
    df: pd.DataFrame,
    config: TokenizerConfig | None = None,
) -> dict[tuple[str, str], list[str]]:
    """Tokenize a DataFrame of SPADL actions into per-player-match token sequences.

    Args:
        df: DataFrame with columns ``canonical_player_id``, ``match_id``,
            ``action_type``, ``start_x``, ``start_y``, ``event_index``.
        config: Optional tokenizer config (default: 12x8 grid on 105x68).

    Returns:
        Dict mapping (canonical_player_id, match_id) to list of token strings,
        ordered by event_index. Player-match pairs with zero valid tokens are excluded.
    """
    if df.empty:
        return {}

    cfg = config or TokenizerConfig()

    # Sort by event_index for correct temporal ordering
    sorted_df = df.sort_values("event_index")

    result: dict[tuple[str, str], list[str]] = {}

    for record in sorted_df.to_dict("records"):
        event: dict[str, Any] = {
            "action_type": record["action_type"],
            "start_x": record["start_x"],
            "start_y": record["start_y"],
        }

        event_token = tokenize_event(event, cfg)
        if event_token is None:
            continue

        key = (str(record["canonical_player_id"]), str(record["match_id"]))
        if key not in result:
            result[key] = []
        result[key].append(event_token)

    return result


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------


def train_model(
    sequences: dict[tuple[str, str], list[str]],
    config: TrainingConfig,
) -> Doc2Vec:
    """Train a Doc2Vec model on player-match token sequences.

    Args:
        sequences: Dict mapping (player_id, match_id) → token list.
        config: Doc2Vec hyperparameters.

    Returns:
        Trained gensim Doc2Vec model.
    """
    tagged_docs = [
        TaggedDocument(words=tokens, tags=[f"{player_id}_{match_id}"])
        for (player_id, match_id), tokens in sequences.items()
    ]

    model = Doc2Vec(
        vector_size=config.vector_size,
        window=config.window,
        min_count=config.min_count,
        epochs=config.epochs,
        dm=config.dm,
        workers=1,  # Deterministic for reproducibility
    )

    model.build_vocab(tagged_docs)
    model.train(tagged_docs, total_examples=model.corpus_count, epochs=model.epochs)

    logger.info(
        "Trained Doc2Vec model: %d documents, %d vocab tokens, vector_size=%d",
        len(tagged_docs),
        len(model.wv),
        config.vector_size,
    )

    return model


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------


def infer_vectors(
    model: Doc2Vec,
    sequences: dict[tuple[str, str], list[str]],
    epochs: int = 20,
) -> dict[tuple[str, str], list[float]]:
    """Infer embedding vectors for player-match token sequences.

    Args:
        model: Trained Doc2Vec model.
        sequences: Dict mapping (player_id, match_id) → token list.
        epochs: Number of inference epochs (default 20).

    Returns:
        Dict mapping (player_id, match_id) → list of float embedding values.
    """
    vectors: dict[tuple[str, str], list[float]] = {}

    for key, tokens in sequences.items():
        vec = model.infer_vector(tokens, epochs=epochs)
        vectors[key] = [float(v) for v in vec]

    return vectors


# ---------------------------------------------------------------------------
# MLflow pyfunc wrapper
# ---------------------------------------------------------------------------


class Football2VecModel:
    """MLflow custom pyfunc wrapper for Football2Vec inference.

    Wraps a trained Doc2Vec model and tokenizer config for serving via
    MLflow's pyfunc interface. On Databricks, this enables model registration
    and batch scoring via spark_udf().

    Note: Inherits from mlflow.pyfunc.PythonModel at runtime on Databricks.
    Locally, the class stands alone for testability without mlflow installed.
    """

    model: Doc2Vec
    tokenizer_config: TokenizerConfig

    def load_context(self, context: Any) -> None:
        """Load model artifacts from MLflow context.

        Args:
            context: MLflow PythonModelContext with artifacts dict.
        """
        model_dir: str = context.artifacts["model_dir"]
        self.model = cast(Doc2Vec, Doc2Vec.load(os.path.join(model_dir, "player2vec.model")))
        config_path = os.path.join(model_dir, "tokenizer_config.json")
        with open(config_path) as f:
            config_data = json.load(f)
        self.tokenizer_config = TokenizerConfig(**config_data) if config_data else TokenizerConfig()

    def predict(self, context: Any, model_input: pd.DataFrame) -> pd.DataFrame:
        """Infer embedding vectors from token sequences.

        Args:
            context: MLflow PythonModelContext (unused in predict).
            model_input: DataFrame with "tokens" column (list of token lists).

        Returns:
            DataFrame with "vector" column containing list[float] embeddings.
        """
        vectors = [self.model.infer_vector(tokens, epochs=20).tolist() for tokens in model_input["tokens"]]
        return pd.DataFrame({"vector": vectors})
