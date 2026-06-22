# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.52-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train Football2Vec v2 transformer (MLM + adversarial debiasing) on HF Jobs A10G GPU.

Stage 1 (MLM): Learn contextual player embeddings by predicting masked SPADL action types.
Stage 2 (Adversarial): Fine-tune with gradient reversal (Ganin et al. 2016) for competition debiasing.

References:
    Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks."
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals." KDD.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_football2vec_v2.py --stage 1 \\
        --flavor l40sx1 --timeout 120m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from analytics.football2vec_transformer import Football2VecConfig, Football2VecEncoder
from ingestion.artifact_deploy import require_mlflow_env, set_and_verify_mlflow_champion
from ingestion.football2vec_v2_training import (
    ADVERSARIAL_LAMBDA_MAX,
    ADVERSARIAL_WARMUP_EPOCHS,
    VOCAB_SIZE,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    Football2VecDataset,
    get_cosine_schedule_with_warmup,
    load_training_data_sql,
    parse_actions,
    stratified_split,
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
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 34, 0)


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
TRAINING_DATASET = f"{HF_ORG}/football2vec-training-data"
MODEL_REPO = f"{HF_ORG}/football2vec-v2"
EMBEDDINGS_DATASET = f"{HF_ORG}/football2vec-statsbomb-wyscout"

MASK_TOKEN_ID = VOCAB_SIZE
PAD_TOKEN_ID = VOCAB_SIZE + 1
DEFAULT_EPOCHS = 30
DEFAULT_BATCH_SIZE = 256
# 3e-4 after EV1 iter-15 promotion (HF Jobs L40S val_acc_15ep=0.5850, +1.6 pp
# vs the prior 1e-4 baseline of 0.569). See docs/evolve/ev1-football2vec/SUMMARY.md.
DEFAULT_LR = 3e-4
DEFAULT_PATIENCE = 5

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "football2vec_v2"


# ---------------------------------------------------------------------------
# Training loops
# ---------------------------------------------------------------------------


def _train_stage1_loop(
    train_ds: Football2VecDataset,
    val_ds: Football2VecDataset,
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2VecEncoder, dict[str, list[float]]]:
    model = Football2VecEncoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))
    expanded = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim).to(device)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded

    tl = DataLoader(
        train_ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    vl = DataLoader(
        val_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(tl) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val = float("inf")
    patience_ctr = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {"train_loss": [], "val_loss": [], "val_accuracy": []}

    for epoch in range(epochs):
        t0 = time.time()
        model.train()
        total_loss = 0.0
        nb = 0
        for batch in tl:
            optimizer.zero_grad()
            logits = model.mlm_forward(
                batch["action_ids"].to(device),
                batch["x_coords"].to(device),
                batch["y_coords"].to(device),
                batch["attention_mask"].to(device),
            )
            loss = criterion(logits.view(-1, config.vocab_size), batch["labels"].to(device).view(-1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()
            total_loss += loss.item()
            nb += 1

        avg_loss = total_loss / max(nb, 1)
        vl_loss, vl_acc = _eval_mlm(model, vl, criterion, config, device)
        history["train_loss"].append(avg_loss)
        history["val_loss"].append(vl_loss)
        history["val_accuracy"].append(vl_acc)
        logger.info(
            "Epoch %d/%d — loss=%.4f val_loss=%.4f acc=%.4f (%.1fs)",
            epoch + 1,
            epochs,
            avg_loss,
            vl_loss,
            vl_acc,
            time.time() - t0,
        )
        if vl_loss < best_val:
            best_val = vl_loss
            patience_ctr = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break
    if best_state:
        model.load_state_dict(best_state)
    return model, history


def _eval_mlm(
    model: Football2VecEncoder,
    vl: DataLoader[dict[str, torch.Tensor]],
    criterion: nn.CrossEntropyLoss,
    config: Football2VecConfig,
    device: torch.device,
) -> tuple[float, float]:
    model.eval()
    total_loss = 0.0
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
            total_loss += criterion(logits.view(-1, config.vocab_size), labels.view(-1)).item()
            nb += 1
            mask = labels != -100
            if mask.any():
                correct += (logits.argmax(dim=-1)[mask] == labels[mask]).sum().item()
                masked += mask.sum().item()
    return total_loss / max(nb, 1), correct / max(masked, 1)


def _extend_mask_for_cls(attention_mask: torch.Tensor, pooling_type: str) -> torch.Tensor:
    """Extend attention_mask with a True column at position 0 for CLS pooling.

    The encoder prepends a CLS token inside _encode when pooling_type="cls", which
    expands sequence length by 1. The adversary receives the full encoder output
    (B, S+1, hidden_dim) and must receive a mask of matching shape (B, S+1).
    """
    if pooling_type != "cls":
        return attention_mask
    cls_col = torch.ones(attention_mask.size(0), 1, dtype=torch.bool, device=attention_mask.device)
    return torch.cat([cls_col, attention_mask], dim=1)


def _default_lambda_schedule(epoch: int, total_epochs: int) -> float:
    """Production linear ramp schedule — unchanged from pre-refactor behavior."""
    del total_epochs
    return ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)


def _train_stage2_loop(
    model: Football2VecEncoder,
    train_ds: Football2VecDataset,
    val_ds: Football2VecDataset,
    num_comp: int,
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
    adversary_module: nn.Module | None = None,
    lambda_schedule_fn: Callable[[int, int], float] | None = None,
) -> tuple[Football2VecEncoder, nn.Module, dict[str, list[float]]]:
    """Stage-2 adversarial fine-tuning loop.

    Args:
        model: Pre-trained Football2VecEncoder from stage-1.
        train_ds, val_ds: MLM datasets with competition_ids populated.
        num_comp: Number of competition classes.
        config: Football2VecConfig for hidden_dim / vocab_size lookups.
        device: Target device.
        epochs, batch_size, lr, patience: Standard training hyperparameters.
        adversary_module: Optional injected adversary. If None, defaults to
            ``LinearAdversaryHead(hidden_dim, num_comp)`` which is byte-equivalent
            to the pre-refactor ``TeamClassifierHead`` under the CLS-pool convention.
            Must accept ``(encoder_output, attention_mask)`` and expose ``.grl.lambda_val``.
        lambda_schedule_fn: Optional injected schedule of signature
            ``(epoch, total_epochs) -> float``. If None, reproduces the linear ramp
            from 0 to ADVERSARIAL_LAMBDA_MAX over ADVERSARIAL_WARMUP_EPOCHS epochs.
    """
    from analytics.football2vec_adversary import LinearAdversaryHead

    model = model.to(device)
    if adversary_module is None:
        adversary: nn.Module = LinearAdversaryHead(config.hidden_dim, num_comp).to(device)
    else:
        adversary = adversary_module.to(device)
    schedule_fn: Callable[[int, int], float] = (
        lambda_schedule_fn if lambda_schedule_fn is not None else _default_lambda_schedule
    )

    tl = DataLoader(
        train_ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    vl = DataLoader(
        val_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    all_p = list(model.parameters()) + list(adversary.parameters())
    optimizer = torch.optim.AdamW(all_p, lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(tl) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    mlm_crit = nn.CrossEntropyLoss(ignore_index=-100)
    adv_crit = nn.CrossEntropyLoss()
    best_combined = float("inf")
    patience_ctr = 0
    best_enc: dict[str, Any] = {}
    best_adv: dict[str, Any] = {}
    hist: dict[str, list[float]] = {
        "train_mlm_loss": [],
        "train_adv_loss": [],
        "train_combined_loss": [],
        "val_mlm_loss": [],
        "val_adv_accuracy": [],
        "val_combined_loss": [],
        "lambda_val": [],
    }

    for epoch in range(epochs):
        t0 = time.time()
        lam = schedule_fn(epoch, epochs)
        adversary.grl.lambda_val = lam  # type: ignore[attr-defined]
        model.train()
        adversary.train()
        t_mlm = 0.0
        t_adv = 0.0
        nb = 0
        for b in tl:
            aids = b["action_ids"].to(device)
            xs = b["x_coords"].to(device)
            ys = b["y_coords"].to(device)
            am = b["attention_mask"].to(device)
            optimizer.zero_grad()
            mlm_loss = mlm_crit(
                model.mlm_forward(aids, xs, ys, am).view(-1, config.vocab_size), b["labels"].to(device).view(-1)
            )
            # Second forward pass through _encode — byte-equivalent RNG to pre-refactor
            # (which had an equivalent _encode call inside model.forward / model()).
            encoded = model._encode(aids, xs, ys, am)
            extended_mask = _extend_mask_for_cls(am, model.config.pooling_type)
            adv_loss = adv_crit(adversary(encoded, extended_mask), b["competition_id"].to(device))
            (mlm_loss + lam * adv_loss).backward()
            torch.nn.utils.clip_grad_norm_(all_p, max_norm=1.0)
            optimizer.step()
            scheduler.step()
            t_mlm += mlm_loss.item()
            t_adv += adv_loss.item()
            nb += 1

        a_mlm = t_mlm / max(nb, 1)
        a_adv = t_adv / max(nb, 1)
        v_mlm, v_adv_acc = _eval_stage2(model, adversary, vl, mlm_crit, config, device)
        v_comb = v_mlm + lam * a_adv
        hist["train_mlm_loss"].append(a_mlm)
        hist["train_adv_loss"].append(a_adv)
        hist["train_combined_loss"].append(a_mlm + lam * a_adv)
        hist["val_mlm_loss"].append(v_mlm)
        hist["val_adv_accuracy"].append(v_adv_acc)
        hist["val_combined_loss"].append(v_comb)
        hist["lambda_val"].append(lam)
        logger.info(
            "Epoch %d/%d — mlm=%.4f adv=%.4f val_mlm=%.4f val_adv_acc=%.4f lam=%.3f (%.1fs)",
            epoch + 1,
            epochs,
            a_mlm,
            a_adv,
            v_mlm,
            v_adv_acc,
            lam,
            time.time() - t0,
        )
        if v_comb < best_combined:
            best_combined = v_comb
            patience_ctr = 0
            best_enc = {k: v.clone() for k, v in model.state_dict().items()}
            best_adv = {k: v.clone() for k, v in adversary.state_dict().items()}
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break
    if best_enc:
        model.load_state_dict(best_enc)
        adversary.load_state_dict(best_adv)
    return model, adversary, hist


def _eval_stage2(
    model: Football2VecEncoder,
    adv: nn.Module,
    vl: DataLoader[dict[str, torch.Tensor]],
    crit: nn.CrossEntropyLoss,
    config: Football2VecConfig,
    device: torch.device,
) -> tuple[float, float]:
    model.eval()
    adv.eval()
    t_mlm = 0.0
    correct = 0
    total = 0
    nb = 0
    with torch.no_grad():
        for b in vl:
            aids = b["action_ids"].to(device)
            xs = b["x_coords"].to(device)
            ys = b["y_coords"].to(device)
            am = b["attention_mask"].to(device)
            t_mlm += crit(
                model.mlm_forward(aids, xs, ys, am).view(-1, config.vocab_size), b["labels"].to(device).view(-1)
            ).item()
            comp = b["competition_id"].to(device)
            encoded = model._encode(aids, xs, ys, am)
            extended_mask = _extend_mask_for_cls(am, model.config.pooling_type)
            correct += (adv(encoded, extended_mask).argmax(dim=-1) == comp).sum().item()
            total += comp.size(0)
            nb += 1
    return t_mlm / max(nb, 1), correct / max(total, 1)


# ---------------------------------------------------------------------------
# Embedding inference + Model I/O + MLflow
# ---------------------------------------------------------------------------


def _gen_embeddings(
    model: Football2VecEncoder,
    data: pd.DataFrame,
    aids: list[list[int]],
    xs: list[list[float]],
    ys: list[list[float]],
    device: torch.device,
) -> pd.DataFrame:
    model.eval()
    ds = Football2VecDataset(aids, xs, ys, mlm=False)
    loader = DataLoader(
        ds, batch_size=512, shuffle=False, num_workers=4, pin_memory=device.type == "cuda", persistent_workers=True
    )
    embs: list[np.ndarray] = []
    with torch.no_grad():
        for b in loader:
            embs.append(
                model(
                    b["action_ids"].to(device),
                    b["x_coords"].to(device),
                    b["y_coords"].to(device),
                    b["attention_mask"].to(device),
                )
                .cpu()
                .numpy()
            )
    arr = np.concatenate(embs, axis=0)
    return pd.DataFrame(
        {
            "canonical_player_id": data["canonical_player_id"].values,
            "match_id": data["match_id"].values,
            "behavioral_vector": [arr[i].tolist() for i in range(len(arr))],
        }
    )


def _save_ckpt(
    model: Football2VecEncoder,
    config: Football2VecConfig,
    stage: str,
    hf_token: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save

    api = HfApi(token=hf_token)
    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)
    with tempfile.TemporaryDirectory() as td:
        sp = os.path.join(td, "model.safetensors")
        _save(model.state_dict(), sp)
        cd = asdict(config)
        cd.update(
            {"_expanded_vocab_size": VOCAB_SIZE + 2, "_mask_token_id": MASK_TOKEN_ID, "_pad_token_id": PAD_TOKEN_ID}
        )
        cp = os.path.join(td, "config.json")
        with open(cp, "w", encoding="utf-8") as f:
            json.dump(cd, f, indent=2)
        for name, path in [("model.safetensors", sp), ("config.json", cp)]:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=f"{stage}/{name}",
                repo_id=MODEL_REPO,
                repo_type="model",
                token=hf_token,
            )
    if metrics:
        api.upload_file(
            path_or_fileobj=json.dumps(metrics, indent=2).encode("utf-8"),
            path_in_repo="metrics.json",
            repo_id=MODEL_REPO,
            repo_type="model",
            token=hf_token,
        )

    # PR 4c: upload model card alongside weights (idempotent per-stage call).
    readme_result = upload_hf_readme(
        repo_id=MODEL_REPO,
        readme_path=get_hf_card_path("football2vec-v2-model-card.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    print(f"  Uploaded model card: {readme_result['commit_url']} (sha256={readme_result['sha256'][:8]})")


def _load_stage1(config: Football2VecConfig, device: torch.device, hf_token: str) -> Football2VecEncoder:
    from huggingface_hub import hf_hub_download
    from safetensors.torch import load_file as _load

    model = Football2VecEncoder(config)
    expanded = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded
    local = hf_hub_download(MODEL_REPO, "stage1/model.safetensors", repo_type="model", token=hf_token)
    model.load_state_dict(_load(local, device=str(device)))
    return model.to(device)


def _publish_emb(df: pd.DataFrame, hf_token: str, stage: str) -> None:
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(EMBEDDINGS_DATASET, exist_ok=True, repo_type="dataset", token=hf_token)
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "embeddings_v2.parquet")
        df.to_parquet(p, index=False)
        api.upload_file(
            path_or_fileobj=p,
            path_in_repo="data/embeddings_v2.parquet",
            repo_id=EMBEDDINGS_DATASET,
            repo_type="dataset",
            token=hf_token,
            commit_message=f"Update v2 embeddings ({stage})",
        )

    # PR 4c: upload dataset card alongside embeddings.
    readme_result = upload_hf_readme(
        repo_id=EMBEDDINGS_DATASET,
        readme_path=get_hf_card_path("football2vec-statsbomb-wyscout.md", kind="dataset"),
        hf_token=hf_token,
    )
    print(f"  Uploaded embeddings card: {readme_result['commit_url']} (sha256={readme_result['sha256'][:8]})")


def _log_mlflow(
    stage: str,
    config: Football2VecConfig,
    history: dict[str, list[float]],
    metrics: dict[str, Any],
    model: Football2VecEncoder,
    args: argparse.Namespace,
    dc: str,
    nt: int,
    nv: int,
    nte: int,
) -> None:
    # ADR-012 §4: registration is unconditional (require_mlflow_env() at main()
    # entry proved the env present). Read via subscript so a missing value raises.
    uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("/soccer_analytics/football2vec_v2")
    with mlflow.start_run(run_name=f"football2vec_v2_{stage}_hf_jobs"):
        mlflow.log_params(
            {
                "stage": stage,
                "architecture": "encoder_only_transformer",
                "vocab_size": config.vocab_size,
                "hidden_dim": config.hidden_dim,
                "num_layers": config.num_layers,
                "num_heads": config.num_heads,
                "dropout": config.dropout,
                "max_seq_len": config.max_seq_len,
                "mask_prob": config.mask_prob,
                "spatial_mlp_dim": config.spatial_mlp_dim,
                "batch_size": args.batch_size,
                "max_epochs": args.epochs,
                "actual_epochs": len(history.get("train_loss", history.get("train_mlm_loss", []))),
                "learning_rate": args.lr,
                "weight_decay": WEIGHT_DECAY,
                "patience": args.patience,
                "n_train": nt,
                "n_val": nv,
                "n_test": nte,
                "n_parameters": sum(p.numel() for p in model.parameters()),
                "training_env": "hf_jobs_l40s",
                "dataset_commit": dc,
            }
        )
        if stage == "stage2":
            mlflow.log_params(
                {
                    "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
                    "adversarial_warmup_epochs": ADVERSARIAL_WARMUP_EPOCHS,
                    "adversary_target": "competition_id",
                }
            )
        for n, v in metrics.items():
            if isinstance(v, (int, float)):
                mlflow.log_metric(n, v)
        for key, vals in history.items():
            for i, val in enumerate(vals):
                mlflow.log_metric(key, val, step=i)

        class _W(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(mi))

        mlflow.pyfunc.log_model(
            python_model=_W(),
            artifact_path="football2vec_v2_model",
            registered_model_name=fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id
    # ADR-012 §4 zombie-alias guard: round-trip the @Champion alias read.
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=fqn, run_id=run_id)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-v2", phase="training")
def main() -> None:
    """Train Football2Vec v2: Stage 1 (MLM) or Stage 2 (adversarial debiasing)."""
    _assert_silly_kicks_min()
    require_mlflow_env()  # ADR-012 §4: fail loud before training if registration env is missing.

    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", type=int, choices=[1, 2], default=1)
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    args = parser.parse_args()

    from huggingface_hub import get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")
    recorder = HFJobsCostRecorder(
        workflow_id="wf-football2vec-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    t0 = time.time()
    try:
        if args.stage == 1:
            _run_stage1(args, hf_token, device, recorder)
        else:
            _run_stage2(args, hf_token, device, recorder)
    except Exception as exc:
        recorder.fail(exc)
        raise
    logger.info("Football2Vec v2 Stage %d complete in %.1fs", args.stage, time.time() - t0)


def _run_stage1(args: argparse.Namespace, hf_token: str, device: torch.device, recorder: HFJobsCostRecorder) -> None:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    data, dc = load_training_data_sql(host, os.environ["DATABRICKS_TOKEN"], os.environ["DATABRICKS_SQL_WAREHOUSE_ID"])
    aids_all, xs_all, ys_all = parse_actions(data["actions"])
    train_df, val_df, test_df = stratified_split(data)
    ti, vi, tei = train_df.index.tolist(), val_df.index.tolist(), test_df.index.tolist()
    logger.info("Split: train=%d val=%d test=%d", len(ti), len(vi), len(tei))

    config = Football2VecConfig()
    model, history = _train_stage1_loop(
        Football2VecDataset([aids_all[i] for i in ti], [xs_all[i] for i in ti], [ys_all[i] for i in ti], mlm=True),
        Football2VecDataset([aids_all[i] for i in vi], [xs_all[i] for i in vi], [ys_all[i] for i in vi], mlm=True),
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )
    test_ds = Football2VecDataset(
        [aids_all[i] for i in tei], [xs_all[i] for i in tei], [ys_all[i] for i in tei], mlm=True
    )
    tl, ta = _eval_mlm(
        model,
        DataLoader(test_ds, batch_size=args.batch_size, shuffle=False, num_workers=2),
        nn.CrossEntropyLoss(ignore_index=-100),
        config,
        device,
    )
    logger.info("Test — loss=%.4f acc=%.4f", tl, ta)
    emb_df = _gen_embeddings(model, data, aids_all, xs_all, ys_all, device)
    metrics: dict[str, Any] = {
        "stage": "stage1",
        "test_loss": tl,
        "test_accuracy": ta,
        "actual_epochs": len(history["train_loss"]),
        "n_train": len(ti),
        "n_val": len(vi),
        "n_test": len(tei),
        "n_embeddings": len(emb_df),
        "embedding_dim": config.hidden_dim,
        "dataset_commit": dc,
        "config": asdict(config),
    }
    metrics = recorder.complete(metrics, row_count=len(emb_df))
    _save_ckpt(model, config, "stage1", hf_token, metrics=metrics)
    _log_mlflow(
        "stage1", config, history, {"test_loss": tl, "test_accuracy": ta}, model, args, dc, len(ti), len(vi), len(tei)
    )


def _run_stage2(args: argparse.Namespace, hf_token: str, device: torch.device, recorder: HFJobsCostRecorder) -> None:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    data, dc = load_training_data_sql(host, os.environ["DATABRICKS_TOKEN"], os.environ["DATABRICKS_SQL_WAREHOUSE_ID"])
    aids_all, xs_all, ys_all = parse_actions(data["actions"])
    ucomp = sorted(data["competition_id"].unique().tolist())
    c2i: dict[int, int] = {c: i for i, c in enumerate(ucomp)}
    cl = [c2i[int(c)] for c in data["competition_id"].values]
    config = Football2VecConfig()
    model = _load_stage1(config, device, hf_token)
    train_df, val_df, test_df = stratified_split(data)
    ti, vi, tei = train_df.index.tolist(), val_df.index.tolist(), test_df.index.tolist()

    model, adversary, history = _train_stage2_loop(
        model,
        Football2VecDataset(
            [aids_all[i] for i in ti],
            [xs_all[i] for i in ti],
            [ys_all[i] for i in ti],
            mlm=True,
            competition_ids=[cl[i] for i in ti],
        ),
        Football2VecDataset(
            [aids_all[i] for i in vi],
            [xs_all[i] for i in vi],
            [ys_all[i] for i in vi],
            mlm=True,
            competition_ids=[cl[i] for i in vi],
        ),
        len(ucomp),
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )
    test_ds = Football2VecDataset(
        [aids_all[i] for i in tei],
        [xs_all[i] for i in tei],
        [ys_all[i] for i in tei],
        mlm=True,
        competition_ids=[cl[i] for i in tei],
    )
    t_mlm, t_adv_acc = _eval_stage2(
        model,
        adversary,
        DataLoader(test_ds, batch_size=args.batch_size, shuffle=False, num_workers=2),
        nn.CrossEntropyLoss(ignore_index=-100),
        config,
        device,
    )
    chance = 1.0 / len(ucomp)
    logger.info("Test — mlm_loss=%.4f adv_acc=%.4f chance=%.4f", t_mlm, t_adv_acc, chance)
    emb_df = _gen_embeddings(model, data, aids_all, xs_all, ys_all, device)
    metrics: dict[str, Any] = {
        "stage": "stage2",
        "test_mlm_loss": t_mlm,
        "test_adv_accuracy": t_adv_acc,
        "chance_level": chance,
        "num_competitions": len(ucomp),
        "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
        "actual_epochs": len(history["train_mlm_loss"]),
        "n_train": len(ti),
        "n_val": len(vi),
        "n_test": len(tei),
        "n_embeddings": len(emb_df),
        "embedding_dim": config.hidden_dim,
        "dataset_commit": dc,
        "config": asdict(config),
    }
    metrics = recorder.complete(metrics, row_count=len(emb_df))
    _save_ckpt(model, config, "stage2", hf_token, metrics=metrics)
    _publish_emb(emb_df, hf_token, "stage2")
    _log_mlflow(
        "stage2",
        config,
        history,
        {"test_mlm_loss": t_mlm, "test_adv_accuracy": t_adv_acc, "chance_level": chance},
        model,
        args,
        dc,
        len(ti),
        len(vi),
        len(tei),
    )


if __name__ == "__main__":
    main()
