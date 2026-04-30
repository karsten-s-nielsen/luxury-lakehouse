# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.26-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
#     "openevolve>=0.2.0",
# ]
# ///
"""Harvest evaluation of ScoutGPT L2 seeds — evaluates each unshipped seed
program against the additive baseline under identical 15-epoch fidelity
on the current dataset. Produces a combined results.json + per-variant
metrics.json uploaded to luxury-lakehouse/scoutgpt-l2-harvest.

All variants use the shared L2 seed config (hidden_dim=192, num_layers=3,
num_heads=6) for fair comparison — NOT the production ScoutGPT defaults
(hidden_dim=256, num_layers=6, num_heads=8). The seeds themselves were
designed around this smaller config; the baseline additive run uses the
same config so the comparison is isolating the conditioning mechanism,
not the capacity.

Usage (HF Jobs L40S):
    hf jobs uv run scripts/evaluate_scoutgpt_l2_seeds.py \\
        --flavor l40sx1 --timeout 360m \\
        --secrets HF_TOKEN=$HF_TOKEN

Fitness formula per src/evolve/targets/scoutgpt/config.yaml:
    0.7 * spearman_rho + 0.3 * top1_accuracy
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import torch
from huggingface_hub import HfApi

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
RESULTS_REPO = f"{HF_ORG}/scoutgpt-l2-harvest"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"

# Shared config mirroring the L2 seed programs' declared hyperparameters
# (fourier_cross_attention, hybrid_gated_attention, orthogonal_cross_attention,
# swiglu_conditioning all declare identical configs). Baseline additive run
# uses the same config so the comparison isolates the conditioning mechanism.
_SHARED_CONFIG: dict[str, Any] = {
    "hidden_dim": 192,
    "num_layers": 3,
    "num_heads": 6,
    "dropout": 0.15,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.32,
    "learning_rate": 2e-4,
    "weight_decay": 0.01,
    "batch_size": 384,
    "dataset": TRAINING_DATASET,
}

_EPOCHS = 15
_SEED = 42
_FITNESS_W_RHO = 0.7
_FITNESS_W_TOP1 = 0.3

# L2 seeds to evaluate — located under src/evolve/targets/scoutgpt/seed_programs/.
# "additive" is the baseline (no program_path); the 4 L2 seeds get their code
# exec'd into the model via _apply_program.
_VARIANTS: list[tuple[str, str | None]] = [
    ("additive", None),
    ("fourier_cross_attention", "fourier_cross_attention.py"),
    ("hybrid_gated_attention", "hybrid_gated_attention.py"),
    ("orthogonal_cross_attention", "orthogonal_cross_attention.py"),
    ("swiglu_conditioning", "swiglu_conditioning.py"),
]


def _fitness(metrics: dict[str, Any]) -> float:
    rho = float(metrics.get("spearman_rho", 0.0))
    top1 = float(metrics.get("top1_accuracy", 0.0))
    return _FITNESS_W_RHO * rho + _FITNESS_W_TOP1 * top1


def _seed_program_path(rel: str) -> str:
    """Resolve seed file path. The wheel bundles these under
    evolve/targets/scoutgpt/seed_programs/."""
    import evolve.targets.scoutgpt.seed_programs as pkg

    pkg_dir = Path(pkg.__file__).parent
    path = pkg_dir / rel
    if not path.exists():
        msg = f"seed program not found: {path}"
        raise FileNotFoundError(msg)
    return str(path)


def _run_variant(variant: str, program_rel: str | None, device: str) -> dict[str, Any]:
    """Train + evaluate one variant, return its metrics dict."""
    from evolve.targets.scoutgpt.evaluator import train_and_evaluate

    candidate_config = dict(_SHARED_CONFIG)
    if program_rel is None:
        candidate_config["conditioning_type"] = "additive"
        program_path = None
    else:
        # Override to "additive" so the unused cross_attention layers aren't
        # allocated alongside the custom_layers from _apply_program.
        candidate_config["conditioning_type"] = "additive"
        program_path = _seed_program_path(program_rel)

    logger.info("=== Evaluating variant=%s program=%s ===", variant, program_rel)
    t0 = time.monotonic()
    metrics = train_and_evaluate(
        candidate_config=candidate_config,
        device=device,
        epochs=_EPOCHS,
        seed=_SEED,
        program_path=program_path,
    )
    elapsed = time.monotonic() - t0
    metrics["variant"] = variant
    metrics["program_path"] = program_rel or "<none>"
    metrics["fitness"] = _fitness(metrics)
    metrics["wall_clock_seconds"] = elapsed
    logger.info(
        "variant=%s fitness=%.4f (rho=%.4f, top1=%.4f) elapsed=%.1fs",
        variant,
        metrics["fitness"],
        metrics.get("spearman_rho", 0.0),
        metrics.get("top1_accuracy", 0.0),
        elapsed,
    )
    return metrics


def _upload_json(api: HfApi, hf_token: str, obj: Any, path_in_repo: str) -> None:
    data = json.dumps(obj, indent=2, default=str).encode("utf-8")
    api.upload_file(
        path_or_fileobj=data,
        path_in_repo=path_in_repo,
        repo_id=RESULTS_REPO,
        repo_type="model",
        token=hf_token,
    )
    logger.info("Uploaded %s -> %s", path_in_repo, RESULTS_REPO)


def main() -> None:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        msg = "HF_TOKEN required"
        raise RuntimeError(msg)

    api = HfApi(token=hf_token)
    api.create_repo(RESULTS_REPO, exist_ok=True, repo_type="model", token=hf_token)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Device: %s", device)

    results: list[dict[str, Any]] = []
    for variant, program_rel in _VARIANTS:
        try:
            metrics = _run_variant(variant, program_rel, device)
        except Exception as exc:
            logger.exception("Variant %s failed", variant)
            metrics = {
                "variant": variant,
                "program_path": program_rel or "<none>",
                "error": str(exc),
                "fitness": 0.0,
            }
        results.append(metrics)
        # Per-variant upload immediately so partial results survive crashes
        _upload_json(api, hf_token, metrics, f"{variant}/metrics.json")

    # Sort by fitness desc for the combined view
    results_sorted = sorted(results, key=lambda r: -r.get("fitness", 0.0))
    combined = {
        "dataset": TRAINING_DATASET,
        "shared_config": _SHARED_CONFIG,
        "epochs": _EPOCHS,
        "seed": _SEED,
        "fitness_formula": f"{_FITNESS_W_RHO} * spearman_rho + {_FITNESS_W_TOP1} * top1_accuracy",
        "variants": results_sorted,
    }
    _upload_json(api, hf_token, combined, "results.json")

    # PR 4c: upload model card alongside harvest results.
    readme_result = upload_hf_readme(
        repo_id=RESULTS_REPO,
        readme_path=get_hf_card_path("scoutgpt-l2-harvest.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info(
        "Uploaded harvest card: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )

    logger.info("Harvest complete. Variants evaluated: %d", len(results))
    for r in results_sorted:
        logger.info(
            "  %s: fitness=%.4f rho=%.4f top1=%.4f",
            r.get("variant"),
            r.get("fitness", 0.0),
            r.get("spearman_rho", 0.0),
            r.get("top1_accuracy", 0.0),
        )

    # Non-zero exit if any variant errored — surface failure to the caller
    if any("error" in r for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
