# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.73-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
# ]
# ///
"""Train ScoutGPT decoder (autoregressive + VAEP auxiliary loss) on HF Jobs A10G GPU.

Player-conditioned causal GPT over SPADL possession episodes. The focal player
conditioning token at position 0 enables counterfactual substitution evaluation.

References:
    Hong, S. et al. (2025). "ScoutGPT: A Player-Conditioned GPT for Soccer."
        arXiv:2512.17266.
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals." KDD.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_scoutgpt_hf.py \\
        --flavor l40sx1 --timeout 180m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$WAREHOUSE_ID \\
        --env DATASET_PINNED_SHA=$DATASET_SHA \\
        -- --variant=rope --output-repo-suffix=-variant-rope
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
from analytics.scoutgpt_training import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_EPOCHS,
    DEFAULT_LR,
    DEFAULT_PATIENCE,
    WEIGHT_DECAY,
    ScoutGPTDataset,
    build_datasets,
    evaluate_and_report,
    load_training_data,
    load_training_data_sql,
    stratified_split,
    train_loop,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from shared.constants import mlflow_model_uri
from workflows import workflow

# Validated HF Jobs flavor — single source of truth, asserted against
# scripts/sk3_mig_b_retrain.py:_FLAVOR_MAP at CI time.
VALIDATED_HF_FLAVOR: str = "l40sx1"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP
# 723 deps silently overrides the wheel's transitive pin; explicit pins are an
# active footgun, not a safety net (verified empirically 2026-05-04).
_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 7, 0)


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to train."
        )


logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "scoutgpt"


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    hf_token: str,
    metrics: dict[str, Any],
    model_repo: str,
) -> None:
    """Save model weights, config, and metrics to HF Hub.

    Uploads:
    - ``stage1/model.safetensors``
    - ``stage1/config.json``
    - ``metrics.json``
    """
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save

    api = HfApi(token=hf_token)
    api.create_repo(model_repo, exist_ok=True, repo_type="model", token=hf_token)

    with tempfile.TemporaryDirectory() as td:
        model_path = os.path.join(td, "model.safetensors")
        _save(model.state_dict(), model_path)

        config_dict = asdict(config)
        config_path = os.path.join(td, "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_dict, f, indent=2)

        for name, path in [("model.safetensors", model_path), ("config.json", config_path)]:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=f"stage1/{name}",
                repo_id=model_repo,
                repo_type="model",
                token=hf_token,
            )
        logger.info("Checkpoint uploaded to %s/stage1/", model_repo)

    api.upload_file(
        path_or_fileobj=json.dumps(metrics, indent=2, default=str).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=model_repo,
        repo_type="model",
        token=hf_token,
    )
    logger.info("metrics.json uploaded to %s", model_repo)

    # PR 4c: upload model card alongside weights. The card filename matches
    # the HF repo basename (the helper's convention), so canonical runs push
    # ``scoutgpt.md`` and variant runs (e.g. ``scoutgpt-variant-rope``) push
    # the matching in-repo variant card.
    card_basename = model_repo.rsplit("/", 1)[-1] + ".md"
    readme_result = upload_hf_readme(
        repo_id=model_repo,
        readme_path=get_hf_card_path(card_basename, kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info(
        "Uploaded model card: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )


def _save_checkpoint_local(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    metrics: dict[str, Any],
    local_output_dir: Path,
) -> None:
    """Save model weights, config, and metrics to a local directory.

    Writes:
    - ``{local_output_dir}/stage1/model.pt`` (torch.save — avoids the safetensors
      runtime dep which is only in the HF Jobs PEP 723 header, not the project venv)
    - ``{local_output_dir}/stage1/config.json``
    - ``{local_output_dir}/metrics.json``
    """
    stage1_dir = local_output_dir / "stage1"
    stage1_dir.mkdir(parents=True, exist_ok=True)

    torch.save(model.state_dict(), str(stage1_dir / "model.pt"))
    (stage1_dir / "config.json").write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    (local_output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    logger.info("Checkpoint + metrics written locally to %s", local_output_dir)


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------


def _log_mlflow(
    config: ScoutGPTConfig,
    history: dict[str, list[float]],
    metrics: dict[str, Any],
    model: ScoutGPTDecoder,
    args: argparse.Namespace,
    dataset_commit: str,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not uri:
        logger.info("MLFLOW_TRACKING_URI not set — skipping MLflow logging")
        return
    import mlflow

    fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("/soccer_analytics/scoutgpt")
    with mlflow.start_run(run_name="scoutgpt_stage1_hf_jobs"):
        mlflow.log_params(
            {
                "architecture": "causal_decoder_transformer",
                "vocab_size": config.vocab_size,
                "hidden_dim": config.hidden_dim,
                "num_layers": config.num_layers,
                "num_heads": config.num_heads,
                "dropout": config.dropout,
                "max_seq_len": config.max_seq_len,
                "num_players": config.num_players,
                "spatial_mlp_dim": config.spatial_mlp_dim,
                "vaep_loss_weight": config.vaep_loss_weight,
                "batch_size": args.batch_size,
                "max_epochs": args.epochs,
                "actual_epochs": len(history["train_loss"]),
                "learning_rate": args.lr,
                "weight_decay": WEIGHT_DECAY,
                "patience": args.patience,
                "n_train": n_train,
                "n_val": n_val,
                "n_test": n_test,
                "n_parameters": sum(p.numel() for p in model.parameters()),
                "training_env": "hf_jobs_l40s",
                "dataset_commit": dataset_commit,
            }
        )
        for name, val in metrics.items():
            if isinstance(val, (int, float)):
                mlflow.log_metric(name, val)
        for key, vals in history.items():
            for i, val in enumerate(vals):
                mlflow.log_metric(key, val, step=i)

        class _Wrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(mi))

        mlflow.pyfunc.log_model(
            python_model=_Wrapper(),
            artifact_path="scoutgpt_model",
            registered_model_name=fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{fqn}'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias(name=fqn, alias="Champion", version=latest.version)
        logger.info("MLflow complete (version=%s, run=%s)", latest.version, run_id)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the shared argparse parser used by both HF Jobs and local entry points."""
    parser = argparse.ArgumentParser(description="Train ScoutGPT on HF Jobs GPU or local hardware")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    parser.add_argument(
        "--variant",
        type=str,
        required=True,
        choices=("learnable", "rope"),
        help="ScoutGPTConfig.position_embedding value — which A/B variant to train.",
    )
    parser.add_argument(
        "--output-repo-suffix",
        type=str,
        default="",
        help=(
            "Suffix for output HF repo name. Destination = "
            "luxury-lakehouse/scoutgpt{suffix}. Empty writes to canonical production repo; "
            "use e.g. '-variant-rope' for sibling-repo A/B runs. Ignored in --local-mode."
        ),
    )
    # ScoutGPTConfig overrides (optional). None -> use config default.
    parser.add_argument(
        "--conditioning-type",
        type=str,
        default=None,
        choices=[
            "additive",
            "cross_attention",
            "film",
            "gated",
            "fourier_cross_attention",
            "swiglu",
        ],
        help="ScoutGPTConfig.conditioning_type override. None -> use config default (additive).",
    )
    parser.add_argument(
        "--hidden-dim",
        type=int,
        default=None,
        help="ScoutGPTConfig.hidden_dim override. None -> use config default (256).",
    )
    parser.add_argument(
        "--num-layers",
        type=int,
        default=None,
        help="ScoutGPTConfig.num_layers override. None -> use config default (6).",
    )
    parser.add_argument(
        "--num-heads",
        type=int,
        default=None,
        help="ScoutGPTConfig.num_heads override. None -> use config default (8).",
    )
    # Local-mode knobs
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help=(
            "Skip HFJobsCostRecorder, MLflow logging, and HF Hub upload. "
            "Write checkpoint + metrics to --local-output-dir instead. "
            "Used by scripts/run_fourier_scoutgpt_ab.py for local hardware runs."
        ),
    )
    parser.add_argument(
        "--local-output-dir",
        type=str,
        default=None,
        help="Required when --local-mode is set: directory for checkpoint + metrics.json.",
    )
    parser.add_argument(
        "--max-episodes",
        type=int,
        default=None,
        help=(
            "Truncate the loaded dataset to the first N episodes BEFORE stratified_split. "
            "Used for smoke tests (e.g. --max-episodes 1000). None -> full dataset."
        ),
    )
    return parser


def _build_scoutgpt_config(args: argparse.Namespace) -> ScoutGPTConfig:
    """Build a ScoutGPTConfig from CLI overrides, with None → config default semantics."""
    cfg_overrides: dict[str, Any] = {"position_embedding": args.variant}
    if args.conditioning_type is not None:
        cfg_overrides["conditioning_type"] = args.conditioning_type
    if args.hidden_dim is not None:
        cfg_overrides["hidden_dim"] = args.hidden_dim
    if args.num_layers is not None:
        cfg_overrides["num_layers"] = args.num_layers
    if args.num_heads is not None:
        cfg_overrides["num_heads"] = args.num_heads
    return ScoutGPTConfig(**cfg_overrides)


def _run_training_core(
    args: argparse.Namespace,
    hf_token: str,
    device: torch.device,
) -> tuple[
    ScoutGPTDecoder,
    ScoutGPTConfig,
    dict[str, list[float]],
    dict[str, Any],
    dict[str, Any],
    str,
    int,
    int,
    int,
    int,
]:
    """Shared training + evaluation pipeline used by both HF Jobs and local entry points.

    Returns (model, config, history, eval_metrics, metrics_core, dataset_commit, n_train, n_val, n_test, row_count).
    `metrics_core` is the pre-recorder-complete dict (no cost telemetry yet).
    """
    dataset_revision = os.environ.get("DATASET_PINNED_SHA") or None

    db_host = os.environ.get("DATABRICKS_HOST", "")
    if db_host:
        data, _player_id_map, dataset_commit = load_training_data_sql(
            db_host.replace("https://", "").replace("http://", "").rstrip("/"),
            os.environ["DATABRICKS_TOKEN"],
            os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
        )
    else:
        data, _player_id_map, dataset_commit = load_training_data(
            hf_token,
            TRAINING_DATASET,
            revision=dataset_revision,
        )
    logger.info("Loaded %d episodes (commit=%s)", len(data), dataset_commit)

    if args.max_episodes is not None and args.max_episodes < len(data):
        data = data.head(args.max_episodes).reset_index(drop=True)
        logger.info("Truncated to %d episodes for smoke/subset run", len(data))

    parsed = build_datasets(data)
    (all_atypes, all_sxs, all_sys, all_exs, all_eys, all_res, all_vaeps, all_tds, all_pidxs, all_comp_ids) = parsed

    train_df, val_df, test_df = stratified_split(data)
    ti = train_df.index.tolist()
    vi = val_df.index.tolist()
    tei = test_df.index.tolist()
    logger.info("Split: train=%d val=%d test=%d", len(ti), len(vi), len(tei))

    config = _build_scoutgpt_config(args)
    logger.info("Config: %s", config)

    train_ds = ScoutGPTDataset(
        [all_atypes[i] for i in ti],
        [all_sxs[i] for i in ti],
        [all_sys[i] for i in ti],
        [all_exs[i] for i in ti],
        [all_eys[i] for i in ti],
        [all_res[i] for i in ti],
        [all_vaeps[i] for i in ti],
        [all_tds[i] for i in ti],
        [all_pidxs[i] for i in ti],
        competition_ids=[all_comp_ids[i] for i in ti],
    )
    val_ds = ScoutGPTDataset(
        [all_atypes[i] for i in vi],
        [all_sxs[i] for i in vi],
        [all_sys[i] for i in vi],
        [all_exs[i] for i in vi],
        [all_eys[i] for i in vi],
        [all_res[i] for i in vi],
        [all_vaeps[i] for i in vi],
        [all_tds[i] for i in vi],
        [all_pidxs[i] for i in vi],
        competition_ids=[all_comp_ids[i] for i in vi],
    )
    test_ds = ScoutGPTDataset(
        [all_atypes[i] for i in tei],
        [all_sxs[i] for i in tei],
        [all_sys[i] for i in tei],
        [all_exs[i] for i in tei],
        [all_eys[i] for i in tei],
        [all_res[i] for i in tei],
        [all_vaeps[i] for i in tei],
        [all_tds[i] for i in tei],
        [all_pidxs[i] for i in tei],
        competition_ids=[all_comp_ids[i] for i in tei],
    )

    model, history = train_loop(
        train_ds,
        val_ds,
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )

    test_data = data.iloc[tei].reset_index(drop=True)
    train_data = data.iloc[ti].reset_index(drop=True)
    eval_metrics = evaluate_and_report(
        model,
        test_ds,
        train_data,
        test_data,
        device,
        history,
        config,
        args.batch_size,
    )

    metrics_core: dict[str, Any] = {
        "dataset_commit": dataset_commit,
        "n_train": len(ti),
        "n_val": len(vi),
        "n_test": len(tei),
        "config": asdict(config),
        **eval_metrics,
    }

    return model, config, history, eval_metrics, metrics_core, dataset_commit, len(ti), len(vi), len(tei), len(data)


def main_local() -> None:
    """Local-mode entry point — NOT decorated with @workflow.

    Bypasses HFJobsCostRecorder, MLflow, HF Hub upload, and (critically) the
    @workflow decorator's observability hooks that would otherwise try to
    write to Databricks Delta tables from a non-Databricks environment.
    """
    args = _build_arg_parser().parse_args()
    if not args.local_mode:
        msg = "main_local() invoked without --local-mode"
        raise RuntimeError(msg)
    if args.local_output_dir is None:
        msg = "--local-output-dir is required when --local-mode is set"
        raise ValueError(msg)
    local_output_dir = Path(args.local_output_dir)

    from huggingface_hub import get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        msg = "HF_TOKEN required for dataset streaming even in local mode"
        raise RuntimeError(msg)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Local mode: device=%s output_dir=%s", device, local_output_dir)
    t0 = time.time()

    model, config, _history, _eval_metrics, metrics_core, *_ = _run_training_core(args, hf_token, device)

    wall_clock_seconds = time.time() - t0
    metrics = {
        **metrics_core,
        "local_mode": True,
        "wall_clock_seconds": wall_clock_seconds,
        "wall_clock_minutes": wall_clock_seconds / 60.0,
    }
    _save_checkpoint_local(model, config, metrics, local_output_dir)
    logger.info("Local ScoutGPT training complete in %.1fs", wall_clock_seconds)


@workflow("wf-scoutgpt", phase="training")
def main() -> None:
    """Train ScoutGPT (HF Jobs path): player-conditioned autoregressive decoder over SPADL episodes."""
    _assert_silly_kicks_min()

    args = _build_arg_parser().parse_args()

    if args.local_mode:
        msg = (
            "main() is the HF Jobs entry point and does not support --local-mode. "
            "Invoke main_local() via the __main__ dispatch instead."
        )
        raise RuntimeError(msg)

    model_repo = f"{HF_ORG}/scoutgpt{args.output_repo_suffix}"

    from huggingface_hub import HfApi, get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    # Pre-create the output model repo so HFJobsCostRecorder can write cost
    # telemetry from t=0. Without this, the recorder hits 404 until
    # _save_checkpoint runs at end-of-training and creates the repo itself.
    HfApi(token=hf_token).create_repo(model_repo, exist_ok=True, repo_type="model", token=hf_token)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-scoutgpt",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=model_repo,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    t0 = time.time()

    try:
        model, config, history, eval_metrics, metrics_core, dataset_commit, n_train, n_val, n_test, row_count = (
            _run_training_core(args, hf_token, device)
        )

        metrics = recorder.complete(metrics_core, row_count=row_count)
        _save_checkpoint(model, config, hf_token, metrics, model_repo)
        _log_mlflow(
            config,
            history,
            eval_metrics,
            model,
            args,
            dataset_commit,
            n_train,
            n_val,
            n_test,
        )

    except Exception as exc:
        recorder.fail(exc)
        raise

    logger.info("ScoutGPT training complete in %.1fs", time.time() - t0)


if __name__ == "__main__":
    # Dispatch between HF Jobs entry (main(), decorated with @workflow) and
    # local-mode entry (main_local(), undecorated) based on --local-mode.
    import sys

    if "--local-mode" in sys.argv:
        main_local()
    else:
        main()
