"""Football2Vec tokenizer, training, and inference.

Implements the player embedding pipeline from Theiner et al. (2022):
  1. Tokenize match events into spatial action tokens (e.g. "pass_6_4")
  2. Train Doc2Vec (Le & Mikolov 2014) on per-player-match token sequences
  3. Infer fixed-length player embedding vectors

Supports both StatsBomb and Wyscout event schemas via unified tokenizer
with source-specific event type mappings.

References:
  - Theiner et al. (2022) "Football2Vec" — spatial tokenization + Doc2Vec
  - Le & Mikolov (2014) "Distributed Representations of Sentences and Documents"
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
# StatsBomb event type mapping
# ---------------------------------------------------------------------------

_STATSBOMB_EVENT_MAP: dict[str, str] = {
    "Pass": "pass",
    "Shot": "shot",
    "Carry": "carry",
    "Duel": "duel",
    "Interception": "interception",
    "Foul Committed": "foul",
    "Clearance": "clearance",
    "Dribble": "take_on",
    "Goalkeeper": "goalkeeper",
}

# ---------------------------------------------------------------------------
# Wyscout event type mapping
# ---------------------------------------------------------------------------

_WYSCOUT_EVENT_MAP: dict[str, str] = {
    "Pass": "pass",
    "Shot": "shot",
    "Duel": "duel",
    "Foul": "foul",
    "Goalkeeper leaving line": "goalkeeper",
}

_WYSCOUT_OTHERS_SUB_MAP: dict[str, str] = {
    "Interception": "interception",
    "Acceleration": "take_on",
    "Touch": "throw_in",
}


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TokenizerConfig:
    """Grid dimensions and pitch parameters for spatial tokenization."""

    grid_cols: int = 12
    grid_rows: int = 8
    pitch_length: float = 120.0
    pitch_width: float = 80.0


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


def _resolve_statsbomb_action(event: dict[str, Any]) -> str:
    """Resolve a StatsBomb event to an action type string."""
    event_type = event.get("event_type", "")

    if event_type == "Pass":
        if event.get("pass_cross"):
            return "cross"
        play_pattern = event.get("play_pattern")
        if play_pattern == "From Corner":
            return "corner"
        if play_pattern == "From Throw In":
            return "throw_in"
        return "pass"

    return _STATSBOMB_EVENT_MAP.get(event_type, "other")


def _resolve_wyscout_action(event: dict[str, Any]) -> str:
    """Resolve a Wyscout event to an action type string."""
    event_type = event.get("event_type", "")
    sub_event_type = event.get("sub_event_type") or ""

    if event_type == "Pass":
        if sub_event_type == "Cross":
            return "cross"
        return "pass"

    if event_type == "Free Kick":
        if "clearance" in sub_event_type.lower():
            return "clearance"
        return "free_kick"

    if event_type == "Others":
        return _WYSCOUT_OTHERS_SUB_MAP.get(sub_event_type, "other")

    return _WYSCOUT_EVENT_MAP.get(event_type, "other")


def tokenize_event(event: dict[str, Any], config: TokenizerConfig) -> str | None:
    """Tokenize a single event into a spatial action token.

    Args:
        event: Dict with keys event_type, x, y, data_source, and optional
            play_pattern, pass_cross (StatsBomb) or sub_event_type (Wyscout).
        config: Grid and pitch dimension configuration.

    Returns:
        Token string like "pass_6_4", or None if coordinates are missing/NaN.
    """
    x = event.get("x")
    y = event.get("y")

    # Skip events with missing or NaN coordinates
    if x is None or y is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    if isinstance(y, float) and math.isnan(y):
        return None

    # Map to grid cell
    cell_width = config.pitch_length / config.grid_cols
    cell_height = config.pitch_width / config.grid_rows
    grid_x = min(int(x / cell_width), config.grid_cols - 1)
    grid_y = min(int(y / cell_height), config.grid_rows - 1)

    # Resolve action type based on data source
    data_source = event.get("data_source", "")
    if data_source == "statsbomb":
        action = _resolve_statsbomb_action(event)
    elif data_source == "wyscout":
        action = _resolve_wyscout_action(event)
    else:
        action = "other"

    return f"{action}_{grid_x}_{grid_y}"


def tokenize_match_events(
    df: pd.DataFrame,
    config: TokenizerConfig,
) -> dict[tuple[str, str], list[str]]:
    """Tokenize a DataFrame of events into per-player-match token sequences.

    Args:
        df: DataFrame with columns canonical_player_id, match_id, event_type,
            x, y, event_index, data_source, play_pattern, pass_cross, sub_event_type.
        config: Grid and pitch dimension configuration.

    Returns:
        Dict mapping (canonical_player_id, match_id) → list of token strings,
        ordered by event_index. Player-match pairs with zero valid tokens are excluded.
    """
    if df.empty:
        return {}

    # Sort by event_index for correct temporal ordering
    sorted_df = df.sort_values("event_index")

    result: dict[tuple[str, str], list[str]] = {}

    for record in sorted_df.to_dict("records"):
        event: dict[str, Any] = {
            "event_type": record["event_type"],
            "x": record["x"],
            "y": record["y"],
            "data_source": record["data_source"],
            "play_pattern": record.get("play_pattern"),
            "pass_cross": record.get("pass_cross"),
            "sub_event_type": record.get("sub_event_type"),
        }

        event_token = tokenize_event(event, config)
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
