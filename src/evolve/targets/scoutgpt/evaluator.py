"""ScoutGPT target evaluator — trains a model from candidate config and returns fitness metrics."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger(__name__)

# Reduced evaluation budget for evolution (vs. full training)
_EVOLVE_COUNTERFACTUAL_EPISODES = 200
_EVOLVE_COUNTERFACTUAL_PLAYERS = 50

# ---------------------------------------------------------------------------
# Module-level dataset cache — loaded once, reused across all candidates.
# The underlying lists are read-only during training; each candidate gets
# its own ScoutGPTDataset wrapper that references the shared data.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CachedData:
    """Immutable container for parsed + split dataset data."""

    parsed: tuple[list[Any], ...]
    train_indices: list[int]
    val_indices: list[int]
    test_indices: list[int]
    action_freqs: dict[int, dict[int, float]]


_dataset_cache: dict[str, _CachedData] = {}
_cache_lock = threading.Lock()


def _load_or_cache(dataset_repo: str, hf_token: str) -> _CachedData:
    """Load and parse the dataset, caching by repo name.

    On the first call, downloads from HF Hub, parses all episodes, computes
    the stratified split, and builds action type frequencies.  Subsequent
    calls return the cached result immediately.

    Thread-safe: a lock prevents concurrent threads from loading the
    dataset simultaneously (each load takes ~28 minutes).
    """
    with _cache_lock:
        if dataset_repo in _dataset_cache:
            _log.info("Using cached dataset for %s", dataset_repo)
            return _dataset_cache[dataset_repo]

        from analytics.scoutgpt_training import (
            build_action_type_frequencies,
            build_datasets,
            load_training_data,
            stratified_split,
        )

        data, _player_map, _sha = load_training_data(hf_token=hf_token, dataset_repo=dataset_repo)
        parsed = build_datasets(data)

        train_df, val_df, test_df = stratified_split(data)
        ti = train_df.index.tolist()
        vi = val_df.index.tolist()
        tei = test_df.index.tolist()

        _log.info("Dataset split: train=%d val=%d test=%d", len(ti), len(vi), len(tei))

        action_freqs = build_action_type_frequencies(all_atypes=parsed[0], all_pidxs=parsed[8], indices=ti)

        cached = _CachedData(
            parsed=parsed,
            train_indices=ti,
            val_indices=vi,
            test_indices=tei,
            action_freqs=action_freqs,
        )
        _dataset_cache[dataset_repo] = cached
        return cached


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
        evaluate_counterfactual_ranking,
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

    # --- Load dataset (cached across candidates) ---
    hf_token = os.environ.get("HF_TOKEN", "")
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/scoutgpt-training-data")
    cached = _load_or_cache(dataset_repo, hf_token)

    # Slice cached parsed fields into per-split datasets.
    def _slice(indices: list[int]) -> tuple[list[Any], ...]:
        return tuple([field[i] for i in indices] for field in cached.parsed)

    def _make_dataset(sliced: tuple[list[Any], ...]) -> ScoutGPTDataset:
        return ScoutGPTDataset(*sliced[:9], max_seq_len=config.max_seq_len, competition_ids=sliced[9])

    train_ds = _make_dataset(_slice(cached.train_indices))
    val_ds = _make_dataset(_slice(cached.val_indices))
    test_ds = _make_dataset(_slice(cached.test_indices))

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

    cf_results = evaluate_counterfactual_ranking(
        model=model,
        test_ds=test_ds,
        device=torch_device,
        num_episodes=_EVOLVE_COUNTERFACTUAL_EPISODES,
        num_players=_EVOLVE_COUNTERFACTUAL_PLAYERS,
        action_type_frequencies=cached.action_freqs,
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
