# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.49-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "psutil>=5.9",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train VAEP models (scores + concedes) on HuggingFace Jobs (CPU).

Downloads SPADL action data from HF Hub, extracts features via
silly-kicks, trains two XGBClassifier models (P(scoring) and
P(conceding)), logs to MLflow, and pushes weights to HF Hub.

This is a standalone PEP 723 script that runs on HF Jobs. The project wheel
is installed for workflow card support; training logic is inlined.

Reference: Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019).
"Actions Speak Louder than Goals: Valuing Player Actions in Soccer."
Proceedings of the 25th ACM SIGKDD International Conference on Knowledge
Discovery & Data Mining.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_vaep_model_hf.py \
        --flavor cpu-basic --timeout 60m \
        --secrets HF_TOKEN=$HF_TOKEN \
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
        --env DATABRICKS_HOST=$DATABRICKS_HOST
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any

import numpy as np
import pandas as pd
import psutil
import silly_kicks.spadl as spadl
import silly_kicks.spadl.config as spadlcfg
import silly_kicks.vaep.features as fs
import silly_kicks.vaep.labels as labels
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from ingestion.artifact_deploy import require_mlflow_env, set_and_verify_mlflow_champion
from ingestion.hf_jobs_cost import HF_RATE_CPU_BASIC, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from shared.constants import mlflow_model_uri
from workflows import workflow

# Validated HF Jobs flavor — single source of truth, asserted against
# scripts/sk3_mig_b_retrain.py:_FLAVOR_MAP at CI time. Escalated from
# cpu-basic per spec §1.6 (cpu-basic OOMed at 4323/5404 games on the SK3-MIG
# data scale; psutil instrumentation in main() reports RSS for next-cycle
# review).
VALIDATED_HF_FLAVOR: str = "cpu-xl"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP
# 723 deps silently overrides the wheel's transitive pin; explicit pins are an
# active footgun, not a safety net (verified empirically 2026-05-04).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 34, 0)


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to train."
        )


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
SPADL_DATASET = f"{HF_ORG}/spadl-vaep-action-values"
# PRIVATE companion repo carrying the license-gated partitions (ADR-049). Name + the
# expectation of WHICH providers live there derive from ingestion.hf_publish (the wheel
# is a PEP 723 dependency above) — single source of truth with the publisher's split, so
# the training corpus can never silently diverge from the publish policy again
# (Champions v10-and-earlier trained WITHOUT gradientsports because the trainer
# inherited the old SQL-side publishing filter unnoticed).
from ingestion.hf_publish import RESTRICTED_HF_PROVIDERS, restricted_repo_id  # noqa: E402

RESTRICTED_DATASET = restricted_repo_id(SPADL_DATASET)
MODEL_REPO = f"{HF_ORG}/vaep-model"

# VAEP feature extraction (matches src/ingestion/spadl_vaep.py)
_FEATURE_FNS: list[Any] = [
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
_NB_PREV_ACTIONS = 3

# XGBoost hyperparameters (same as src/ingestion/spadl_vaep.py)
N_ESTIMATORS = 100
MAX_DEPTH = 3
LEARNING_RATE = 0.1
TEST_SIZE = 0.2
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Column mapping: HF dataset -> SPADL format
# ---------------------------------------------------------------------------

# Reverse-lookup dicts: string name -> integer ID (pre-built by silly-kicks)
_ACTIONTYPE_TO_ID: dict[str, int] = spadlcfg.actiontype_id
_RESULT_TO_ID: dict[str, int] = spadlcfg.result_id
_BODYPART_TO_ID: dict[str, int] = spadlcfg.bodypart_id


def _convert_hf_to_spadl(df: pd.DataFrame) -> pd.DataFrame:
    """Convert HF dataset columns to SPADL format.

    The HF dataset has string-typed columns (action_type, action_result,
    bodypart) and uses different column names (match_id, period) than
    SPADL expects (game_id, period_id, type_id, result_id,
    bodypart_id). This function:

    1. Renames columns to SPADL standard names.
    2. Maps string action names to integer IDs.
    3. Ensures all required SPADL columns are present.
    """
    out = df.copy()

    # Rename HF columns -> SPADL columns
    out = out.rename(
        columns={
            "match_id": "game_id",
            "period": "period_id",
        }
    )

    # Map string names to integer IDs
    out["type_id"] = out["action_type"].map(_ACTIONTYPE_TO_ID).fillna(0).astype(int)
    out["result_id"] = out["action_result"].map(_RESULT_TO_ID).fillna(0).astype(int)
    out["bodypart_id"] = out["bodypart"].map(_BODYPART_TO_ID).fillna(0).astype(int)

    # Ensure numeric types for coordinate columns
    for col in ["start_x", "start_y", "end_x", "end_y", "time_seconds"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype("float64")

    for col in ["game_id", "team_id", "player_id", "period_id"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")

    return out


# ---------------------------------------------------------------------------
# Feature extraction (mirrors src/ingestion/spadl_vaep.py)
# ---------------------------------------------------------------------------


def extract_features_for_games(
    actions: pd.DataFrame,
    game_ids: list[Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract features and labels for a subset of games.

    Returns (X, Y_scores, Y_concedes) for the specified games.
    Uses pre-built game groups to avoid O(n*m) boolean mask filtering.
    """
    # add_names() adds type_name, result_name, bodypart_name from IDs
    named = spadl.add_names(actions)  # type: ignore[arg-type]
    all_x: list[pd.DataFrame] = []
    all_y_scores: list[pd.DataFrame] = []
    all_y_concedes: list[pd.DataFrame] = []

    # Pre-build game index (CLAUDE.md: no boolean mask filter inside loops)
    game_groups: dict[Any, pd.DataFrame] = dict(iter(named.groupby("game_id")))

    # Per spec §2.6: log resident-set high-water marks every 100 games so the
    # operator can size the next cycle's flavor escalation if cpu-large OOMs.
    process = psutil.Process()
    rss_high_water_gb = 0.0

    n_processed = 0
    n_failed = 0
    for i, game_id in enumerate(game_ids):
        if i % 100 == 0:
            rss_gb = process.memory_info().rss / 1e9
            rss_high_water_gb = max(rss_high_water_gb, rss_gb)
            logger.info(
                "feature_extraction game=%d/%d rss=%.2fGB hwm=%.2fGB",
                i,
                len(game_ids),
                rss_gb,
                rss_high_water_gb,
            )
        game_actions = game_groups.get(game_id, pd.DataFrame()).reset_index(drop=True)
        if len(game_actions) < 2:
            continue
        try:
            gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)  # type: ignore[arg-type]
            x_game = pd.concat([fn(gamestates) for fn in _FEATURE_FNS], axis=1)
            y_scores = labels.scores(game_actions, nr_actions=10)  # type: ignore[arg-type]
            y_concedes = labels.concedes(game_actions, nr_actions=10)  # type: ignore[arg-type]
            all_x.append(x_game)
            all_y_scores.append(y_scores)
            all_y_concedes.append(y_concedes)
            n_processed += 1
        except Exception:
            n_failed += 1
            logger.warning("Failed feature extraction for game %s", game_id, exc_info=True)

    logger.info("Feature extraction: %d games processed, %d failed", n_processed, n_failed)

    if not all_x:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    return (
        pd.concat(all_x, ignore_index=True),
        pd.concat(all_y_scores, ignore_index=True),
        pd.concat(all_y_concedes, ignore_index=True),
    )


# ---------------------------------------------------------------------------
# Serialization (JSON envelope with base64-encoded XGBoost boosters)
# ---------------------------------------------------------------------------


def serialize_vaep_models(
    model_scores: XGBClassifier,
    model_concedes: XGBClassifier,
) -> bytes:
    """Serialize both VAEP models to a single JSON envelope (no pickle).

    Each model's booster is serialized to JSON format and base64-encoded.
    """
    envelope = {
        "model_type": "vaep_xgboost_v1",
        "scores_booster_b64": base64.b64encode(model_scores.get_booster().save_raw("json")).decode("ascii"),
        "concedes_booster_b64": base64.b64encode(model_concedes.get_booster().save_raw("json")).decode("ascii"),
        "n_features": int(model_scores.n_features_in_),
        "nb_prev_actions": _NB_PREV_ACTIONS,
    }
    return json.dumps(envelope, indent=2).encode("utf-8")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-vaep", phase="training")
def main() -> None:
    """Download SPADL actions, train VAEP models, log to MLflow, push to HF Hub."""
    _assert_silly_kicks_min()
    # ADR-012 §4: fail loud on missing MLflow/Databricks env BEFORE the
    # expensive HF download + training, so registration can never be silently
    # skipped (the old `if tracking_uri:` gate, removed below).
    require_mlflow_env()

    from huggingface_hub import HfApi, get_token, hf_hub_download

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-vaep",
        phase="training",
        rate_usd_per_hour=HF_RATE_CPU_BASIC,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()

    # ------------------------------------------------------------------
    # 1. Load SPADL data from HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Loading SPADL actions from HF Hub ===")

    # Find parquet files — prefer data.parquet (HF viewer canonical files),
    # skip part-* files which may be duplicates
    all_items = list(api.list_repo_tree(SPADL_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
    # Fall back: if no data.parquet found, use all parquet files
    if not parquet_files:
        parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {SPADL_DATASET}")

    logger.info("Downloading %d parquet files...", len(parquet_files))
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SPADL_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        # Extract data_source from Hive partition path if not in columns
        if "data_source" not in df.columns and "data_source=" in pf:
            ds = pf.split("data_source=")[1].split("/")[0]
            df["data_source"] = ds
        dfs.append(df)
        logger.info("  %s: %s rows", pf, f"{len(df):,}")

    all_actions = pd.concat(dfs, ignore_index=True)

    # ------------------------------------------------------------------
    # 1b. Restricted-corpus supplement — PRIVATE HF dataset (ADR-049)
    # ------------------------------------------------------------------
    # License-gated providers are absent from the PUBLIC dataset but 100% in
    # the training corpus: the publisher writes their partitions to the
    # PRIVATE companion repo, keeping full publish->version->train lineage
    # (commit hash recorded below, same as the public repo). The expectation
    # derives from the SAME constant the publisher splits on:
    #   - RESTRICTED_HF_PROVIDERS non-empty -> partitions for those providers
    #     are REQUIRED (fail-loud: a silent skip is exactly how Champions
    #     v10-and-earlier trained WITHOUT gradientsports unnoticed);
    #   - empty -> the full corpus is public; skip with a log line.
    restricted_commit_hash = "unrequired-empty-policy"
    if RESTRICTED_HF_PROVIDERS:
        logger.info(
            "=== Loading restricted partitions %s from %s ===",
            sorted(RESTRICTED_HF_PROVIDERS),
            RESTRICTED_DATASET,
        )
        restricted_items = list(api.list_repo_tree(RESTRICTED_DATASET, repo_type="dataset", recursive=True))
        restricted_files = [f.path for f in restricted_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
        if not restricted_files:
            raise RuntimeError(
                f"No data.parquet partitions in {RESTRICTED_DATASET} but the policy expects "
                f"{sorted(RESTRICTED_HF_PROVIDERS)} — refusing a silently-shrunk training corpus. "
                "Run publish_spadl_vaep_hf.py first."
            )
        gs_dfs: list[pd.DataFrame] = []
        for pf in restricted_files:
            local = hf_hub_download(RESTRICTED_DATASET, pf, repo_type="dataset", token=hf_token)
            df = pd.read_parquet(local)
            if "data_source" not in df.columns and "data_source=" in pf:
                df["data_source"] = pf.split("data_source=")[1].split("/")[0]
            gs_dfs.append(df)
            logger.info("  %s: %s rows", pf, f"{len(df):,}")
        restricted_actions = pd.concat(gs_dfs, ignore_index=True)
        if restricted_actions.empty:
            raise RuntimeError(f"{RESTRICTED_DATASET} partitions are empty — refusing a shrunk training corpus.")
        missing = RESTRICTED_HF_PROVIDERS - set(restricted_actions["data_source"].unique())
        if missing:
            raise RuntimeError(f"Restricted dataset lacks expected provider partition(s): {sorted(missing)}")
        restricted_commit_hash = api.repo_info(RESTRICTED_DATASET, repo_type="dataset").sha or "unknown"
        logger.info(
            "Restricted supplement: %s rows (commit %s)", f"{len(restricted_actions):,}", restricted_commit_hash
        )
        all_actions = pd.concat([all_actions, restricted_actions], ignore_index=True)
    else:
        logger.info("RESTRICTED_HF_PROVIDERS is empty — full corpus is public; no restricted supplement.")

    # Deduplicate in case of overlapping exports
    if "action_value_id" in all_actions.columns:
        before = len(all_actions)
        all_actions = all_actions.drop_duplicates(subset=["action_value_id"])
        if len(all_actions) < before:
            logger.info("Deduplicated: %s -> %s rows", f"{before:,}", f"{len(all_actions):,}")

    logger.info("Total actions loaded: %s", f"{len(all_actions):,}")

    # Capture dataset commit hash for E5 versioning
    dataset_info = api.repo_info(SPADL_DATASET, repo_type="dataset")
    dataset_commit_hash = dataset_info.sha or "unknown"
    logger.info("Dataset commit hash: %s", dataset_commit_hash)

    # ------------------------------------------------------------------
    # 2. Convert HF dataset columns to SPADL format
    # ------------------------------------------------------------------
    logger.info("=== Converting to SPADL format ===")
    actions_spadl = _convert_hf_to_spadl(all_actions)

    # Validate required columns exist
    required_cols = {
        "game_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "type_id",
        "result_id",
        "bodypart_id",
    }
    missing = required_cols - set(actions_spadl.columns)
    if missing:
        raise RuntimeError(f"Missing required SPADL columns after conversion: {missing}")

    logger.info("SPADL actions: %s rows, %d columns", f"{len(actions_spadl):,}", len(actions_spadl.columns))

    # ------------------------------------------------------------------
    # 3. Train/test split by competition (80/20, stratified)
    # ------------------------------------------------------------------
    logger.info("=== Splitting data ===")
    game_ids = actions_spadl["game_id"].unique().tolist()

    # Build game -> competition mapping for the stratified split. Stratify on the
    # Kimball surrogate ``competition_key`` (ADR-011), NOT the legacy numeric
    # ``competition_id``: the legacy column is NULL for non-numeric provider IDs
    # (idsse / metrica / skillcorner), which put pd.NA into the stratify array and
    # crashed ``train_test_split`` (boolean value of NA is ambiguous). The Kimball
    # surrogate is non-NULL for ALL providers, so the trainer can use every
    # provider's SPADL. Competition is only a split-balancing label here, never a
    # model feature. NA-safe int cast is defense-in-depth (a SB/WS row will never
    # be NULL, but a future provider with a sparse key should degrade, not crash).
    _comp_col = "competition_key" if "competition_key" in actions_spadl.columns else "competition_id"
    game_comp = (
        actions_spadl[["game_id", _comp_col]].drop_duplicates(subset=["game_id"]).set_index("game_id")[_comp_col]
    )
    game_comp = pd.to_numeric(game_comp, errors="coerce").fillna(0).astype("int64")
    comp_labels = [int(game_comp.get(gid, 0)) for gid in game_ids]

    # Stratified split requires each class to have >= 2 members.
    # Merge rare competition_ids into a shared bucket. Use threshold
    # high enough that the merged bucket itself has >= 2 members.
    from collections import Counter

    comp_counts = Counter(comp_labels)
    min_for_stratify = max(2, int(1 / TEST_SIZE) + 1)  # need >=1 in each split
    comp_labels_safe = [c if comp_counts[c] >= min_for_stratify else -1 for c in comp_labels]

    # If the merged bucket is still too small, fall back to no stratification
    merged_counts = Counter(comp_labels_safe)
    can_stratify = all(v >= 2 for v in merged_counts.values())

    rng = np.random.default_rng(RANDOM_STATE)
    shuffled_indices = rng.permutation(len(game_ids))
    game_ids_arr = np.array(game_ids)[shuffled_indices]
    comp_labels_arr = np.array(comp_labels_safe)[shuffled_indices]

    train_games, test_games = train_test_split(
        game_ids_arr,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=comp_labels_arr if can_stratify else None,
    )

    logger.info(
        "Split: %d train games, %d test games (of %d total)",
        len(train_games),
        len(test_games),
        len(game_ids),
    )

    # ------------------------------------------------------------------
    # 4. Extract features
    # ------------------------------------------------------------------
    logger.info("=== Extracting features (train) ===")
    x_train, y_scores_train, y_concedes_train = extract_features_for_games(
        actions_spadl,
        train_games.tolist(),
    )
    if x_train.empty:
        raise RuntimeError("No training features extracted — aborting")

    logger.info("Train features: %s rows x %d cols", f"{len(x_train):,}", x_train.shape[1])

    logger.info("=== Extracting features (test) ===")
    x_test, y_scores_test, y_concedes_test = extract_features_for_games(
        actions_spadl,
        test_games.tolist(),
    )
    if x_test.empty:
        raise RuntimeError("No test features extracted — aborting")

    logger.info("Test features: %s rows x %d cols", f"{len(x_test):,}", x_test.shape[1])

    # ------------------------------------------------------------------
    # 5. Train models
    # ------------------------------------------------------------------
    logger.info("=== Training VAEP models ===")

    model_scores = XGBClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        learning_rate=LEARNING_RATE,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model_concedes = XGBClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        learning_rate=LEARNING_RATE,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    logger.info("Training scores model on %s samples...", f"{len(x_train):,}")
    model_scores.fit(x_train, y_scores_train["scores"])

    logger.info("Training concedes model on %s samples...", f"{len(x_train):,}")
    model_concedes.fit(x_train, y_concedes_train["concedes"])

    logger.info("Models trained successfully")

    # ------------------------------------------------------------------
    # 6. Evaluate
    # ------------------------------------------------------------------
    logger.info("=== Evaluating ===")

    scores_proba = model_scores.predict_proba(x_test)[:, 1]
    concedes_proba = model_concedes.predict_proba(x_test)[:, 1]

    scores_metrics = {
        "brier_score": float(brier_score_loss(y_scores_test["scores"], scores_proba)),
        "log_loss": float(log_loss(y_scores_test["scores"], scores_proba)),
        "roc_auc": float(roc_auc_score(y_scores_test["scores"], scores_proba)),
    }
    concedes_metrics = {
        "brier_score": float(brier_score_loss(y_concedes_test["concedes"], concedes_proba)),
        "log_loss": float(log_loss(y_concedes_test["concedes"], concedes_proba)),
        "roc_auc": float(roc_auc_score(y_concedes_test["concedes"], concedes_proba)),
    }

    logger.info("Scores model:   %s", {k: f"{v:.4f}" for k, v in scores_metrics.items()})
    logger.info("Concedes model: %s", {k: f"{v:.4f}" for k, v in concedes_metrics.items()})

    # ------------------------------------------------------------------
    # 7. Log to MLflow (remote tracking URI)
    # ------------------------------------------------------------------
    # ADR-012 §4: registration is unconditional (require_mlflow_env() proved the
    # env present at entry). Read via subscript so a missing value fails loud.
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow
    import mlflow.pyfunc

    logger.info("=== Logging to MLflow (%s) ===", tracking_uri)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/vaep_model")

    # Pyfunc wrapper that stores both VAEP models for @Champion loading
    class _VaepPyfuncWrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
        def __init__(self, scores: XGBClassifier, concedes: XGBClassifier) -> None:
            self.scores_model = scores
            self.concedes_model = concedes

        def predict(self, context: Any, model_input: pd.DataFrame) -> np.ndarray:
            return self.scores_model.predict_proba(model_input)[:, 1]

    wrapper = _VaepPyfuncWrapper(model_scores, model_concedes)
    input_example = x_test.head(1)

    _vaep_fqn = mlflow_model_uri("soccer_analytics", "dev_gold", "vaep_model")
    with mlflow.start_run(run_name="vaep_model_hf_jobs"):
        mlflow.log_params(
            {
                "n_estimators": N_ESTIMATORS,
                "max_depth": MAX_DEPTH,
                "learning_rate": LEARNING_RATE,
                "nb_prev_actions": _NB_PREV_ACTIONS,
                "n_train_games": len(train_games),
                "n_test_games": len(test_games),
                "n_train_samples": len(x_train),
                "n_test_samples": len(x_test),
                "n_features": x_train.shape[1],
                "training_env": "hf_jobs_cpu",
                "hf_dataset_commit": dataset_commit_hash,
                "hf_restricted_dataset_commit": restricted_commit_hash,
            }
        )
        for name, value in scores_metrics.items():
            mlflow.log_metric(f"scores_{name}", value)
        for name, value in concedes_metrics.items():
            mlflow.log_metric(f"concedes_{name}", value)

        mlflow.pyfunc.log_model(
            python_model=wrapper,
            artifact_path="vaep_model",
            registered_model_name=_vaep_fqn,
            input_example=input_example,
        )

        run_id = mlflow.active_run().info.run_id

    # ADR-012 §4 zombie-alias guard: round-trip the @Champion alias read.
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=_vaep_fqn, run_id=run_id)

    logger.info("MLflow logging complete")

    # ------------------------------------------------------------------
    # 8. Serialize and publish to HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Publishing to HF Hub ===")

    model_bytes = serialize_vaep_models(model_scores, model_concedes)

    metrics_payload: dict[str, object] = {
        "scores": scores_metrics,
        "concedes": concedes_metrics,
        "config": {
            "n_estimators": N_ESTIMATORS,
            "max_depth": MAX_DEPTH,
            "learning_rate": LEARNING_RATE,
            "nb_prev_actions": _NB_PREV_ACTIONS,
            "n_features": x_train.shape[1],
            "feature_names": list(x_train.columns),
            "n_train_games": len(train_games),
            "n_test_games": len(test_games),
            "n_train_samples": len(x_train),
            "n_test_samples": len(x_test),
            "hf_dataset_commit": dataset_commit_hash,
        },
    }
    metrics_payload = recorder.complete(metrics_payload, row_count=len(x_train) + len(x_test))

    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    api.upload_file(
        path_or_fileobj=model_bytes,
        path_in_repo="vaep_model.json",
        repo_id=MODEL_REPO,
        token=hf_token,
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=MODEL_REPO,
        token=hf_token,
    )

    # PR 4c: upload model card alongside weights.
    readme_result = upload_hf_readme(
        repo_id=MODEL_REPO,
        readme_path=get_hf_card_path("vaep-model.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info(
        "Uploaded model card: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )

    logger.info("Published: https://huggingface.co/%s", MODEL_REPO)
    logger.info("Model: %s bytes", f"{len(model_bytes):,}")
    logger.info("VAEP model training complete!")


if __name__ == "__main__":
    main()
