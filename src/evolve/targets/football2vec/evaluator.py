"""Football2Vec target evaluator — trains a candidate from config, returns fitness metrics.

Self-contained MLM training loop (no MLflow, no HF Hub publishing, no checkpoint writing).
Module-level dataset cache shared across all candidate evaluations in this process.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level dataset cache — load once, reuse across all candidates.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CachedData:
    """Immutable container for parsed action sequences + train/val indices."""

    aids: list[list[int]]
    xs: list[list[float]]
    ys: list[list[float]]
    train_indices: list[int]
    val_indices: list[int]


_dataset_cache: dict[str, _CachedData] = {}
_cache_lock = threading.Lock()


def _load_or_cache(dataset_repo: str, hf_token: str) -> _CachedData:
    """Load and parse the dataset, caching by repo name.

    On the first call, downloads from HF Hub, parses all sequences, and computes
    the stratified split. Subsequent calls return the cached result immediately.
    Thread-safe: a lock prevents concurrent threads from loading the dataset
    simultaneously (each load takes ~30s for 114K rows).
    """
    with _cache_lock:
        if dataset_repo in _dataset_cache:
            _log.info("Using cached dataset for %s", dataset_repo)
            return _dataset_cache[dataset_repo]

        from ingestion.football2vec_v2_training import (
            load_training_data,
            parse_actions,
            stratified_split,
        )

        data, _commit = load_training_data(hf_token, dataset_repo)
        aids_all, xs_all, ys_all = parse_actions(data["actions"])
        train_df, val_df, _test_df = stratified_split(data)
        ti = train_df.index.tolist()
        vi = val_df.index.tolist()
        _log.info("Dataset split: train=%d val=%d", len(ti), len(vi))

        cached = _CachedData(
            aids=aids_all,
            xs=xs_all,
            ys=ys_all,
            train_indices=ti,
            val_indices=vi,
        )
        _dataset_cache[dataset_repo] = cached
        return cached


# ---------------------------------------------------------------------------
# Self-contained MLM train + eval loop (no checkpoints, no MLflow, no publishing)
# ---------------------------------------------------------------------------


def _train_eval_one_candidate(
    config_obj: Any,  # Football2VecConfig
    train_ds: Any,  # Football2VecDataset
    val_ds: Any,
    device: Any,  # torch.device
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> dict[str, Any]:
    """Run one candidate's training and return metrics."""
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader

    from analytics.football2vec_transformer import Football2VecEncoder
    from ingestion.football2vec_v2_training import (
        VOCAB_SIZE,
        WARMUP_FRACTION,
        WEIGHT_DECAY,
        get_cosine_schedule_with_warmup,
    )

    model = Football2VecEncoder(config_obj).to(device)
    # Expand vocab embedding to include MASK + PAD tokens (matches scripts/train_football2vec_v2.py).
    expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim).to(device)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded

    # Windows + pin_memory from non-main thread is slow/flaky; use 0 workers there.
    # See memory: project_evolve_openevolve_overhead.md.
    num_workers = 0 if os.name == "nt" else 2
    tl = DataLoader(
        train_ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=(device.type == "cuda"),
    )
    vl = DataLoader(
        val_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        pin_memory=(device.type == "cuda"),
    )
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = max(1, len(tl) * epochs)
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val_loss = float("inf")
    best_val_acc = 0.0
    patience_ctr = 0
    epochs_run = 0

    for epoch in range(epochs):
        model.train()
        for batch in tl:
            optimizer.zero_grad()
            logits = model.mlm_forward(
                batch["action_ids"].to(device),
                batch["x_coords"].to(device),
                batch["y_coords"].to(device),
                batch["attention_mask"].to(device),
            )
            loss = criterion(logits.view(-1, config_obj.vocab_size), batch["labels"].to(device).view(-1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()

        # Validation
        model.eval()
        v_loss = 0.0
        correct = 0
        masked = 0
        nb = 0
        with torch.no_grad():
            for b in vl:
                logits = model.mlm_forward(
                    b["action_ids"].to(device),
                    b["x_coords"].to(device),
                    b["y_coords"].to(device),
                    b["attention_mask"].to(device),
                )
                labels = b["labels"].to(device)
                v_loss += criterion(logits.view(-1, config_obj.vocab_size), labels.view(-1)).item()
                nb += 1
                mask = labels != -100
                if mask.any():
                    correct += (logits.argmax(dim=-1)[mask] == labels[mask]).sum().item()
                    masked += mask.sum().item()
        v_loss /= max(nb, 1)
        v_acc = correct / max(masked, 1)
        epochs_run = epoch + 1
        _log.info("epoch %d/%d — val_loss=%.4f val_acc=%.4f", epochs_run, epochs, v_loss, v_acc)

        # Track max accuracy independently of min loss.
        # Early-stopping triggers on val_loss (stable signal); val_accuracy is the
        # reported fitness metric and may peak at a different epoch.
        if v_acc > best_val_acc:
            best_val_acc = v_acc
        if v_loss < best_val_loss:
            best_val_loss = v_loss
            patience_ctr = 0
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                _log.info("early stopping at epoch %d", epochs_run)
                break

    param_count = sum(p.numel() for p in model.parameters())
    return {
        "val_accuracy": best_val_acc,
        "val_loss": best_val_loss,
        "param_count": float(param_count),
        "epochs_trained": float(epochs_run),
    }


# ---------------------------------------------------------------------------
# Public API — train_and_evaluate (called by ComputeBackend implementations)
# ---------------------------------------------------------------------------


def train_and_evaluate(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, Any]:
    """Build model from candidate config, train, return evaluation metrics.

    Returns:
        Dict of scalar fitness metrics (all float-castable):
        ``val_accuracy``, ``val_loss``, ``param_count``, ``training_time_seconds``,
        ``epochs_trained``. On error, returns ``{"val_accuracy": 0.0, "error": 1.0,
        "_error_text": <traceback>}``.
    """
    import torch

    from analytics.football2vec_transformer import Football2VecConfig
    from ingestion.football2vec_v2_training import Football2VecDataset

    torch_device = torch.device(device)
    start = time.monotonic()
    _log.info("Football2Vec candidate starting (device=%s, epochs=%d, seed=%d)", device, epochs, seed)

    # Reproducibility (best-effort — DataLoader workers may still introduce variance).
    torch.manual_seed(seed)

    # Extract training hyperparams (not part of Football2VecConfig)
    lr: float = candidate_config.get("learning_rate", 1e-4)
    batch_size: int = candidate_config.get("batch_size", 256)

    # Build Football2VecConfig from candidate config (architecture keys only).
    # mask_prob is intentionally NOT in this set — it is consumed by
    # Football2VecDataset (dataset masking), not by Football2VecConfig/Encoder.
    # Keeping it here would silently double-handle the value if a future
    # Football2VecEncoder change starts reading cfg.mask_prob.
    config_keys = {
        "vocab_size",
        "hidden_dim",
        "num_layers",
        "num_heads",
        "dropout",
        "max_seq_len",
        "spatial_mlp_dim",
        "pooling_type",
        "spatial_injection",
        "position_embedding",
    }
    model_kwargs = {k: v for k, v in candidate_config.items() if k in config_keys}
    config_obj = Football2VecConfig(**model_kwargs)

    # Load dataset (cached across candidates)
    hf_token = os.environ.get("HF_TOKEN", "")
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/football2vec-training-data")
    cached = _load_or_cache(dataset_repo, hf_token)

    # Build per-split Football2VecDataset using mask_prob from candidate
    mask_prob: float = candidate_config.get("mask_prob", 0.15)
    train_ds = Football2VecDataset(
        [cached.aids[i] for i in cached.train_indices],
        [cached.xs[i] for i in cached.train_indices],
        [cached.ys[i] for i in cached.train_indices],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
    )
    val_ds = Football2VecDataset(
        [cached.aids[i] for i in cached.val_indices],
        [cached.xs[i] for i in cached.val_indices],
        [cached.ys[i] for i in cached.val_indices],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
    )

    try:
        metrics = _train_eval_one_candidate(
            config_obj=config_obj,
            train_ds=train_ds,
            val_ds=val_ds,
            device=torch_device,
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            patience=max(2, epochs // 2),
        )
        metrics["training_time_seconds"] = time.monotonic() - start
        _log.info(
            "Football2Vec candidate done: val_acc=%.4f val_loss=%.4f time=%.1fs",
            metrics["val_accuracy"],
            metrics["val_loss"],
            metrics["training_time_seconds"],
        )
    except (torch.cuda.OutOfMemoryError, RuntimeError, ValueError) as exc:
        _log.warning("Football2Vec candidate failed (OOM or runtime error), score 0: %s", exc)
        metrics = {
            "val_accuracy": 0.0,
            "val_loss": float("inf"),
            "param_count": 0.0,
            "epochs_trained": 0.0,
            "training_time_seconds": time.monotonic() - start,
            "error": 1.0,
            "_error_text": traceback.format_exc(),
        }

    # GPU memory hygiene
    if torch_device.type == "cuda":
        torch.cuda.empty_cache()

    return metrics


__all__ = ["train_and_evaluate"]
