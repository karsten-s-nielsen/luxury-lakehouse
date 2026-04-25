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
from pathlib import Path
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
    # Resolve token via huggingface_hub's standard chain: HF_TOKEN env ->
    # ~/.cache/huggingface/token file -> None. Phase 1c (2026-04-23) debug:
    # non-interactive SSH can have HF_TOKEN unset even when the remote has
    # a valid file token. A bare ``os.environ.get(..., "")`` returns "" and
    # downstream passes it as ``token=""`` to hf_hub_download, which builds
    # an illegal ``Bearer `` header and httpx rejects with
    # LocalProtocolError. get_token() resolves from the file cache
    # transparently, matching HfApi()'s default behaviour.
    from huggingface_hub import get_token

    hf_token = get_token() or ""
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


# ---------------------------------------------------------------------------
# Stage-2 adversarial fine-tuning — EV2 infrastructure.
# ---------------------------------------------------------------------------


_stage1_cache: dict[tuple[str, str], dict[str, Any]] = {}
_stage1_lock = threading.Lock()


def _load_or_cache_stage1_encoder(
    model_repo: str,
    commit_sha: str,
    config_obj: Any,  # Football2VecConfig
    device: Any,  # torch.device
    hf_token: str,
) -> Any:
    """Load the stage-1 encoder from a pinned HF Hub revision, cache per process.

    Keyed by (model_repo, commit_sha). Weights are ~500 MB — re-download per
    candidate evaluation is wasteful; the cache saves it.
    """
    import torch
    import torch.nn as nn
    from huggingface_hub import hf_hub_download
    from safetensors.torch import load_file as _load

    from analytics.football2vec_transformer import Football2VecEncoder
    from ingestion.football2vec_v2_training import VOCAB_SIZE

    key = (model_repo, commit_sha)
    with _stage1_lock:
        cached = _stage1_cache.get(key)
        if cached is not None:
            _log.info("Using cached stage-1 encoder for %s @ %s", model_repo, commit_sha[:8])
            model = Football2VecEncoder(config_obj)
            expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim)
            with torch.no_grad():
                expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
            model.token_embedding = expanded
            model.load_state_dict(cached["state_dict"])
            return model.to(device)

        local = hf_hub_download(
            model_repo,
            "stage1/model.safetensors",
            repo_type="model",
            token=hf_token,
            revision=commit_sha,
        )
        state = _load(local, device="cpu")
        _stage1_cache[key] = {"state_dict": state}

        model = Football2VecEncoder(config_obj)
        expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim)
        with torch.no_grad():
            expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
        model.token_embedding = expanded
        model.load_state_dict(state)
        return model.to(device)


def _apply_program_adversary(
    program_path: str,
    hidden_dim: int,
    num_competitions: int,
    device: Any,  # torch.device
) -> Any:  # nn.Module
    """Exec a Phase 1 seed under restricted globals and return a DynamicAdversary module.

    Seeds follow the two-function pattern (same as ScoutGPT L2):

    - ``custom_layers(hidden_dim, num_competitions)`` returns a ``dict[str, nn.Module]``
      of adversary submodules. Must include a ``"grl"`` key (per-epoch lambda injection
      hook).
    - ``custom_embed(self, encoder_output, attention_mask)`` returns logits of shape
      ``(B, num_competitions)``. Despite the name, this is the **adversary forward**
      (the ``custom_embed`` function name is reused from the validator which hardcodes it).

    We build a ``_DynamicAdversary`` wrapper that registers the layers as children
    and delegates ``forward`` to ``custom_embed``.

    Raises ValueError if the seed lacks either function or the layers dict lacks ``"grl"``.
    """
    import torch
    import torch.nn as nn

    from evolve.targets.scoutgpt.building_blocks import (
        AdaLNZero,
        AdaptiveBandwidth,
        CompetitiveGate,
        CrossLayer,
        GradientReversal,
        HyperLinear,
        KANLayer,
        MoERouter,
        RatioGate,
    )

    source = Path(program_path).read_text(encoding="utf-8")
    restricted_globals: dict[str, Any] = {
        "torch": torch,
        "math": __import__("math"),
        "MoERouter": MoERouter,
        "HyperLinear": HyperLinear,
        "KANLayer": KANLayer,
        "AdaLNZero": AdaLNZero,
        "CrossLayer": CrossLayer,
        "CompetitiveGate": CompetitiveGate,
        "GradientReversal": GradientReversal,
        "AdaptiveBandwidth": AdaptiveBandwidth,
        "RatioGate": RatioGate,
        "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102 — see ADR-001  # nosemgrep: python.lang.security.audit.exec-detected.exec-detected

    layers_fn = restricted_globals.get("custom_layers")
    forward_fn = restricted_globals.get("custom_embed")
    if layers_fn is None:
        msg = f"seed {program_path} has no custom_layers() function"
        raise ValueError(msg)
    if forward_fn is None:
        msg = f"seed {program_path} has no custom_embed() function"
        raise ValueError(msg)

    layers_dict = layers_fn(hidden_dim, num_competitions)
    if not isinstance(layers_dict, dict):
        msg = f"seed {program_path} custom_layers must return dict, got {type(layers_dict).__name__}"
        raise ValueError(msg)
    if "grl" not in layers_dict:
        msg = f"seed {program_path} custom_layers dict must include 'grl' key for per-epoch lambda injection"
        raise ValueError(msg)

    class _DynamicAdversary(nn.Module):
        """Evaluator-built wrapper: registers layers + delegates forward to custom_embed."""

        def __init__(self) -> None:
            super().__init__()
            for name, mod in layers_dict.items():
                self.register_module(name, mod)

        def forward(self, encoder_output: Any, attention_mask: Any) -> Any:
            return forward_fn(self, encoder_output, attention_mask)

    return _DynamicAdversary().to(device)


def train_and_evaluate_stage2(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, Any]:
    """Stage-2 adversarial fine-tuning evaluator.

    Loads the pinned stage-1 encoder, builds the adversary (injected seed OR
    registry lookup), runs the refactored ``_train_stage2_loop``, and returns
    the metrics dict the harvest orchestrator consumes.

    Required ``candidate_config`` keys (beyond stage-1 architecture keys):
      ``stage1_model_repo``, ``stage1_commit_sha``, ``dataset``,
      ``adversary_architecture``, ``lambda_schedule_shape``, ``lambda_max``,
      ``lambda_warmup_epochs``. Optional ``L_0_reference`` (float): when set,
      the returned ``mlm_score`` and ``fitness`` are computed; otherwise they
      are NaN (baseline run populates L_0 for subsequent seeds).

    Returns a dict with keys: val_mlm_loss, val_adv_accuracy, num_competitions,
    chance, leakage, debias_score, mlm_score, fitness, param_count,
    training_time_seconds, epochs_trained.
    """
    import sys

    import torch

    from analytics.football2vec_adversary import AdversaryConfig, build_adversary, lambda_schedule
    from analytics.football2vec_transformer import Football2VecConfig
    from ingestion.football2vec_v2_training import (
        Football2VecDataset,
        load_training_data,
        parse_actions,
        stratified_split,
    )

    torch_device = torch.device(device)
    start = time.monotonic()
    _log.info("Stage-2 candidate starting (device=%s, epochs=%d, seed=%d)", device, epochs, seed)
    torch.manual_seed(seed)

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

    # Use huggingface_hub's env -> file -> None resolution (see _load_or_cache
    # for the Phase 1c debug rationale).
    from huggingface_hub import get_token

    hf_token = get_token() or ""
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/football2vec-training-data")
    data, _commit = load_training_data(hf_token, dataset_repo)
    aids_all, xs_all, ys_all = parse_actions(data["actions"])
    ucomp = sorted(data["competition_id"].unique().tolist())
    c2i: dict[int, int] = {int(c): i for i, c in enumerate(ucomp)}
    cl = [c2i[int(c)] for c in data["competition_id"].values]
    num_competitions = len(ucomp)

    train_df, val_df, _test_df = stratified_split(data)
    ti, vi = train_df.index.tolist(), val_df.index.tolist()

    batch_size: int = int(candidate_config.get("batch_size", 256))
    lr: float = float(candidate_config.get("learning_rate", 3e-4))
    mask_prob: float = float(candidate_config.get("mask_prob", 0.22))
    patience: int = max(3, epochs // 2)

    train_ds = Football2VecDataset(
        [aids_all[i] for i in ti],
        [xs_all[i] for i in ti],
        [ys_all[i] for i in ti],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
        competition_ids=[cl[i] for i in ti],
    )
    val_ds = Football2VecDataset(
        [aids_all[i] for i in vi],
        [xs_all[i] for i in vi],
        [ys_all[i] for i in vi],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
        competition_ids=[cl[i] for i in vi],
    )

    stage1_repo: str = candidate_config.get("stage1_model_repo", "luxury-lakehouse/football2vec-v2")
    stage1_sha: str = str(candidate_config.get("stage1_commit_sha", "main"))
    encoder = _load_or_cache_stage1_encoder(stage1_repo, stage1_sha, config_obj, torch_device, hf_token)

    adv_cfg = AdversaryConfig(
        architecture=candidate_config.get("adversary_architecture", "linear"),
        lambda_schedule_shape=candidate_config.get("lambda_schedule_shape", "linear"),
        lambda_max=float(candidate_config.get("lambda_max", 0.2)),
        lambda_warmup_epochs=int(candidate_config.get("lambda_warmup_epochs", 5)),
    )
    if program_path is not None:
        adversary = _apply_program_adversary(program_path, config_obj.hidden_dim, num_competitions, torch_device)
    else:
        adversary = build_adversary(adv_cfg, config_obj.hidden_dim, num_competitions).to(torch_device)

    def schedule_fn(epoch: int, total_epochs: int) -> float:
        return lambda_schedule(adv_cfg, epoch, total_epochs)

    sys.path.insert(0, "scripts")
    try:
        from train_football2vec_v2 import _train_stage2_loop
    finally:
        sys.path.pop(0)

    try:
        _encoder, _adversary, history = _train_stage2_loop(
            encoder,
            train_ds,
            val_ds,
            num_competitions,
            config_obj,
            torch_device,
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            patience=patience,
            adversary_module=adversary,
            lambda_schedule_fn=schedule_fn,
        )

        val_mlm_loss_final = history["val_mlm_loss"][-1] if history["val_mlm_loss"] else float("inf")
        val_adv_acc_final = history["val_adv_accuracy"][-1] if history["val_adv_accuracy"] else 0.0
        chance = 1.0 / num_competitions
        leakage = max(0.0, (val_adv_acc_final - chance) / max(1.0 - chance, 1e-9))
        debias_score = 1.0 - leakage

        l_0_reference = candidate_config.get("L_0_reference")
        mlm_score: float = float("nan")
        fitness: float = float("nan")
        if l_0_reference is not None and val_mlm_loss_final > 0:
            mlm_score = min(1.0, float(l_0_reference) / val_mlm_loss_final)
            fitness = 0.4 * mlm_score + 0.6 * debias_score

        param_count = sum(p.numel() for p in encoder.parameters()) + sum(p.numel() for p in adversary.parameters())

        elapsed = time.monotonic() - start
        metrics: dict[str, Any] = {
            "val_mlm_loss": val_mlm_loss_final,
            "val_adv_accuracy": val_adv_acc_final,
            "num_competitions": float(num_competitions),
            "chance": chance,
            "leakage": leakage,
            "debias_score": debias_score,
            "mlm_score": mlm_score,
            "fitness": fitness,
            "param_count": float(param_count),
            "training_time_seconds": elapsed,
            "epochs_trained": float(len(history.get("val_mlm_loss", []))),
        }
        _log.info(
            "Stage-2 candidate done: val_mlm=%.4f val_adv_acc=%.4f leak=%.3f time=%.1fs",
            val_mlm_loss_final,
            val_adv_acc_final,
            leakage,
            elapsed,
        )
    # Broadened from (OutOfMemoryError, RuntimeError, ValueError) after
    # Phase 1c/1d (2026-04-23) silent failures: httpx.LocalProtocolError
    # from empty HF_TOKEN fell outside the original tuple, the exception
    # escaped, remote_worker silently exited non-zero, and the orchestrator
    # logged fail_metrics with NO _error_text. Catching Exception guarantees
    # the full traceback lands in _error_text for post-mortem debugging.
    except Exception as exc:
        _log.warning("Stage-2 candidate failed: %s", exc)
        metrics = {
            "val_mlm_loss": float("inf"),
            "val_adv_accuracy": 0.0,
            "debias_score": 0.0,
            "mlm_score": 0.0,
            "fitness": 0.0,
            "param_count": 0.0,
            "training_time_seconds": time.monotonic() - start,
            "epochs_trained": 0.0,
            "error": 1.0,
            "_error_text": traceback.format_exc(),
        }

    if torch_device.type == "cuda":
        torch.cuda.empty_cache()
    return metrics


__all__ = ["_apply_program_adversary", "train_and_evaluate", "train_and_evaluate_stage2"]
