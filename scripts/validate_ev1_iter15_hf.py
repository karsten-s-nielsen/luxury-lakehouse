# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.29-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "datasets>=3.0",
#     "torch>=2.0",
#     "huggingface-hub>=1.5.0",
#     "openevolve>=0.2.0",
#     "scikit-learn>=1.3",
# ]
# ///
"""Throwaway: validate EV1 iter-15 config on HF Jobs L40S at 15-epoch fidelity.

Uses evolve.targets.football2vec.evaluator.train_and_evaluate -- same MLM train +
eval loop the local RTX 5070 Ti run used, stripped of HF Hub publishing and
MLflow (we don't want to overwrite the production football2vec-v2 model).

Reproduction target: local run produced val_acc_15ep = 0.5865. A cloud L40S
run in the same val_acc band (within statistical noise) would confirm the
iter-15 config is a real improvement over the 0.569 baseline and is safe to
promote to Football2VecConfig defaults.

Usage:
    hf jobs uv run scripts/_validate_ev1_iter15_hf.py \\
        --flavor l40sx1 --timeout 30m --secrets HF_TOKEN=$HF_TOKEN
"""

from __future__ import annotations

import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("validate_iter15")

from evolve.targets.football2vec.evaluator import train_and_evaluate  # noqa: E402

ITER15_CONFIG = {
    "hidden_dim": 192,
    "num_layers": 4,
    "num_heads": 6,
    "dropout": 0.1,
    "mask_prob": 0.22,
    "spatial_mlp_dim": 64,
    "pooling_type": "cls",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 3e-4,
    "batch_size": 256,
}

LOCAL_VAL_ACC_15EP = 0.5865  # produced on RTX 5070 Ti, 2026-04-19
BASELINE_VAL_ACC_15EP = 0.569  # PR #124 production baseline


def main() -> None:
    if not os.environ.get("HF_TOKEN"):
        log.error("HF_TOKEN not set -- aborting.")
        sys.exit(1)

    log.info("Training iter-15 config on HF Jobs L40S at 15 epochs.")
    log.info("Target: val_acc_15ep ~= %.4f (reproducing local)", LOCAL_VAL_ACC_15EP)
    log.info("Baseline (PR #124 Football2Vec v2 defaults): val_acc_15ep ~= %.4f", BASELINE_VAL_ACC_15EP)

    result = train_and_evaluate(
        candidate_config=ITER15_CONFIG,
        device="cuda:0",
        epochs=15,
        seed=42,
    )

    log.info("=" * 70)
    log.info("RESULT:")
    log.info(json.dumps(result, indent=2))
    log.info("=" * 70)
    val_acc = float(result.get("val_accuracy", 0.0))
    delta_vs_local = val_acc - LOCAL_VAL_ACC_15EP
    delta_vs_baseline = val_acc - BASELINE_VAL_ACC_15EP
    log.info(
        "val_acc_15ep = %.4f  (delta vs local %.4f: %+.4f pp, vs baseline %.4f: %+.4f pp)",
        val_acc,
        LOCAL_VAL_ACC_15EP,
        (val_acc - LOCAL_VAL_ACC_15EP) * 100,
        BASELINE_VAL_ACC_15EP,
        (val_acc - BASELINE_VAL_ACC_15EP) * 100,
    )
    if delta_vs_local < -0.01:
        log.warning("VAL_ACC BELOW LOCAL REPRODUCTION BAND -- INVESTIGATE before promoting defaults")
    elif delta_vs_baseline < 0:
        log.warning("VAL_ACC BELOW BASELINE -- do NOT promote defaults")
    else:
        log.info("val_acc reproduces local result within noise band; safe to promote")


if __name__ == "__main__":
    main()
