"""VAEP model training helpers.

Provides feature-extraction and XGBoost model-training functions used by
the HF Jobs training script (``scripts/train_vaep_model_hf.py``) and
retained for local experimentation.

Production training runs on HF Jobs; the Databricks inference pipeline
(``spadl_vaep.py``) loads pre-trained models from the MLflow registry
and does NOT import from this module.

Reference: Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019).
"Actions Speak Louder than Goals: Valuing Player Actions in Soccer."
KDD 2019.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import silly_kicks.spadl as spadl
import silly_kicks.vaep.features as fs
import silly_kicks.vaep.labels as labels
from xgboost import XGBClassifier

from ingestion.spadl_vaep import _NB_PREV_ACTIONS, _get_feature_fns

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------


def extract_features_for_games(
    actions: pd.DataFrame,
    game_ids: Any,
    log: logging.Logger | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract VAEP features and labels for a subset of games.

    Returns ``(X, Y_scores, Y_concedes)`` for the specified *game_ids*.
    Uses pre-built game groups to avoid O(n*m) boolean mask filtering.

    Args:
        actions: SPADL-format DataFrame with at least ``game_id`` and
            the standard SPADL columns.
        game_ids: Iterable of game IDs to process.
        log: Optional logger (falls back to module-level logger).

    Returns:
        A 3-tuple of DataFrames.  All three are empty when no valid
        games could be processed.
    """
    _log = log or logger
    named = spadl.add_names(actions)  # type: ignore[arg-type]
    all_x: list[pd.DataFrame] = []
    all_y_scores: list[pd.DataFrame] = []
    all_y_concedes: list[pd.DataFrame] = []

    # Pre-build game index (CLAUDE.md: no boolean mask filter inside loops)
    _game_groups: dict[Any, pd.DataFrame] = dict(iter(named.groupby("game_id")))

    for game_id in game_ids:
        game_actions = _game_groups.get(game_id, pd.DataFrame()).reset_index(drop=True)
        if len(game_actions) < 2:
            continue
        try:
            gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)  # type: ignore[arg-type]
            x_game = pd.concat([fn(gamestates) for fn in _get_feature_fns()], axis=1)
            y_scores = labels.scores(game_actions, nr_actions=10)  # type: ignore[arg-type]
            y_concedes = labels.concedes(game_actions, nr_actions=10)  # type: ignore[arg-type]
            all_x.append(x_game)
            all_y_scores.append(y_scores)
            all_y_concedes.append(y_concedes)
        except Exception as exc:
            msg = f"VAEP feature extraction failed for game_id={game_id}"
            raise RuntimeError(msg) from exc

    if not all_x:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    return (
        pd.concat(all_x, ignore_index=True),
        pd.concat(all_y_scores, ignore_index=True),
        pd.concat(all_y_concedes, ignore_index=True),
    )


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------


def train_vaep_models(
    x: pd.DataFrame,
    y_scores: pd.DataFrame,
    y_concedes: pd.DataFrame,
    log: logging.Logger | None = None,
) -> tuple[XGBClassifier, XGBClassifier]:
    """Train two XGBoost classifiers for P(scoring) and P(conceding).

    Args:
        x: Feature matrix (output of :func:`extract_features_for_games`).
        y_scores: Binary labels — 1 if scoring occurred within 10 actions.
        y_concedes: Binary labels — 1 if conceding occurred within 10 actions.
        log: Optional logger (falls back to module-level logger).

    Returns:
        ``(model_scores, model_concedes)`` — fitted XGBClassifier pair.
    """
    _log = log or logger

    model_scores = XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
    )
    model_concedes = XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
    )

    _log.info("Training VAEP scoring model on %d samples", len(x))
    model_scores.fit(x, y_scores["scores"])

    _log.info("Training VAEP conceding model on %d samples", len(x))
    model_concedes.fit(x, y_concedes["concedes"])

    return model_scores, model_concedes
