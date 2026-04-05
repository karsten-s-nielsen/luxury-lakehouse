"""ScoutGPT target evaluator — trains a model from candidate config and returns fitness metrics."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

_log = logging.getLogger(__name__)

# Reduced evaluation budget for evolution (vs. full training)
_EVOLVE_COUNTERFACTUAL_EPISODES = 200
_EVOLVE_COUNTERFACTUAL_PLAYERS = 50


def train_and_evaluate(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
) -> dict[str, float]:
    """Build model from candidate config, train, return evaluation metrics.

    This function is called by the LocalCudaBackend (and future backends).
    It imports torch lazily so the evolve package can be imported without torch.

    Args:
        candidate_config: Hyperparameter dict containing both model architecture keys
            and training hyperparams (``learning_rate``, ``batch_size``, ``dataset``).
        device: Torch device string (e.g. ``"cuda:0"`` or ``"cpu"``).
        epochs: Maximum training epochs (early stopping may terminate sooner).
        seed: Random seed — reserved for future deterministic evaluation; not yet
            threaded through all PyTorch samplers.

    Returns:
        Dict of scalar fitness metrics (all float-castable):
        ``spearman_rho``, ``rho_std``, ``top1_accuracy``, ``val_loss``,
        ``param_count``, ``training_time_seconds``, ``epochs_trained``.
    """
    import torch

    from analytics.scoutgpt_decoder import ScoutGPTConfig
    from analytics.scoutgpt_training import (
        ScoutGPTDataset,
        build_action_type_frequencies,
        build_datasets,
        evaluate_counterfactual_ranking,
        load_training_data,
        stratified_split,
        train_loop,
    )

    torch_device = torch.device(device)
    start_time = time.monotonic()

    # --- Extract training hyperparams (not part of ScoutGPTConfig) ---
    lr: float = candidate_config.get("learning_rate", 1e-4)
    batch_size: int = candidate_config.get("batch_size", 256)

    # --- Build ScoutGPTConfig from model architecture keys ---
    config_keys = {
        "hidden_dim",
        "num_layers",
        "num_heads",
        "dropout",
        "max_seq_len",
        "num_players",
        "spatial_mlp_dim",
        "vaep_loss_weight",
        "conditioning_type",
    }
    model_kwargs = {k: v for k, v in candidate_config.items() if k in config_keys}
    config = ScoutGPTConfig(**model_kwargs)

    # --- Load dataset and parse all episodes up front ---
    hf_token = os.environ.get("HF_TOKEN", "")
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/scoutgpt-training-data")
    data, _player_map, _sha = load_training_data(hf_token=hf_token, dataset_repo=dataset_repo)

    # build_datasets parses the full DataFrame into per-field lists once.
    # The split is done on indices so we avoid re-parsing per split.
    parsed = build_datasets(data)
    (all_atypes, all_sxs, all_sys, all_exs, all_eys, all_res, all_vaeps, all_tds, all_pidxs, all_comp_ids) = parsed

    train_df, val_df, test_df = stratified_split(data)
    ti = train_df.index.tolist()
    vi = val_df.index.tolist()
    tei = test_df.index.tolist()

    _log.info("Dataset split: train=%d val=%d test=%d", len(ti), len(vi), len(tei))

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
        max_seq_len=config.max_seq_len,
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
        max_seq_len=config.max_seq_len,
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
        max_seq_len=config.max_seq_len,
        competition_ids=[all_comp_ids[i] for i in tei],
    )

    # --- Train ---
    model, history = train_loop(
        train_ds=train_ds,
        val_ds=val_ds,
        config=config,
        device=torch_device,
        epochs=epochs,
        batch_size=batch_size,
        lr=lr,
        patience=max(3, epochs // 2),
    )

    # --- Evaluate ---
    model.eval()
    top1 = history["val_top1_accuracy"][-1] if history["val_top1_accuracy"] else 0.0

    action_freqs = build_action_type_frequencies(all_atypes=all_atypes, all_pidxs=all_pidxs, indices=ti)
    cf_results = evaluate_counterfactual_ranking(
        model=model,
        test_ds=test_ds,
        device=torch_device,
        num_episodes=_EVOLVE_COUNTERFACTUAL_EPISODES,
        num_players=_EVOLVE_COUNTERFACTUAL_PLAYERS,
        action_type_frequencies=action_freqs,
    )

    elapsed = time.monotonic() - start_time
    param_count = sum(p.numel() for p in model.parameters())

    metrics: dict[str, float] = {
        "spearman_rho": cf_results["mean_spearman_rho"],
        "rho_std": cf_results["rho_std"],
        "top1_accuracy": top1,
        "val_loss": history["val_loss"][-1] if history["val_loss"] else float("inf"),
        "param_count": float(param_count),
        "training_time_seconds": elapsed,
        "epochs_trained": float(len(history["val_loss"])),
    }

    _log.info(
        "ScoutGPT candidate: rho=%.4f, top1=%.4f, params=%d, time=%.1fs",
        metrics["spearman_rho"],
        metrics["top1_accuracy"],
        param_count,
        elapsed,
    )

    # Clean up GPU memory before returning so the next candidate starts clean
    del model, train_ds, val_ds, test_ds
    if torch_device.type == "cuda":
        torch.cuda.empty_cache()

    return metrics


__all__ = ["train_and_evaluate"]
