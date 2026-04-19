# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ git+https://github.com/karsten-s-nielsen/luxury-lakehouse.git@evolve/football2vec-l1-sweep",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "datasets>=3.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "huggingface-hub>=1.5.0",
# ]
# ///
"""Validate EV1 iter-11 winning config at 15 epochs on HF Jobs L40S.

Runs the iter-11 config (discovered in the EV1 sweep at 5-epoch fidelity) through
the football2vec evolve target's self-contained MLM training loop for 15 epochs —
matching the production retrain fidelity used for the documented baseline.

**Purpose**: decide whether the +0.05 pp / -34% param gain found at 5 epochs holds
at 15 epochs before promoting the iter-11 config to `Football2VecConfig` defaults.

**Baseline reference**: wheel 0.3.3 defaults trained for 15 epochs on L40S reached
val_accuracy ~0.569 (per session 43 retrain summary, commit cd063c6 / PR #124). This
validation compares the iter-11 config at the same fidelity.

**Notable methodology caveat**: iter-11's learning_rate=5e-4 is 5x the baseline's
1e-4. With cosine warmup + 10% warmup fraction, peak lr lasts ~1.5 epochs at 15 epochs
(vs ~0.5 epochs at 5). If the evolved config diverges early, that's the 5-epoch
signal not translating to 15-epoch training — a valid finding.

Usage (HF Jobs CLI):
    hf jobs uv run --flavor l40sx1 \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        scripts/validate_ev1_iter11.py

Dependency pin: the PEP 723 header pins luxury-lakehouse to this branch's git ref,
because the iter-11 config relies on the three new architectural enums
(pooling_type, spatial_injection, position_embedding) which are NOT yet in the
released wheel 0.3.3 — they land with the EV1 PR.
"""

from __future__ import annotations

import json
import os
import sys

# iter-11 winning config — source: results/evolve/football2vec/20260418T232631Z/
# Discovered at iter 11 of the EV1 sweep; LLM-mutated from the wider.py seed.
# Held val_accuracy=0.5693 at 5 epochs with 1,295,255 params (-34% vs wider seed).
ITER_11_CONFIG = {
    "hidden_dim": 128,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.2,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "film",
    "position_embedding": "sinusoidal",
    "learning_rate": 5e-4,
    "batch_size": 256,
    "dataset": "luxury-lakehouse/football2vec-training-data",
}

# Baseline from session 43 retrain (defaults at the time of commit cd063c6).
# Defaults used the OLD Football2VecConfig (no enum fields — equivalent to all
# enums at their current default values: mean/additive/learnable).
BASELINE_15_EPOCH_VAL_ACCURACY = 0.569
BASELINE_PARAM_COUNT_APPROX = 1_295_640


def main() -> None:
    if not os.environ.get("HF_TOKEN"):
        print(json.dumps({"status": "error", "reason": "HF_TOKEN env var not set"}), flush=True)
        sys.exit(1)

    from evolve.targets.football2vec.evaluator import train_and_evaluate

    print(json.dumps({"status": "starting", "epochs": 15, "config": ITER_11_CONFIG}), flush=True)

    result = train_and_evaluate(
        candidate_config=ITER_11_CONFIG,
        device="cuda:0",
        epochs=15,
        seed=42,
    )

    iter11_val_acc = float(result.get("val_accuracy", 0.0))
    iter11_params = float(result.get("param_count", 0.0))
    delta_pp_acc = (iter11_val_acc - BASELINE_15_EPOCH_VAL_ACCURACY) * 100
    delta_pct_params = (iter11_params - BASELINE_PARAM_COUNT_APPROX) / BASELINE_PARAM_COUNT_APPROX * 100

    if iter11_val_acc >= BASELINE_15_EPOCH_VAL_ACCURACY:
        verdict = "real_win_promote"
    elif iter11_val_acc >= 0.560:
        verdict = "parsimony_tradeoff_decide_by_preference"
    else:
        verdict = "five_epoch_signal_was_noise_do_not_promote"

    summary = {
        "status": "done",
        "iter_11_15epoch": {
            "val_accuracy": iter11_val_acc,
            "val_loss": float(result.get("val_loss", 0.0)),
            "param_count": iter11_params,
            "epochs_trained": float(result.get("epochs_trained", 0.0)),
            "training_time_seconds": float(result.get("training_time_seconds", 0.0)),
        },
        "baseline_15epoch": {
            "val_accuracy": BASELINE_15_EPOCH_VAL_ACCURACY,
            "param_count_approx": BASELINE_PARAM_COUNT_APPROX,
            "source": "session 43 retrain, commit cd063c6 / PR #124",
        },
        "delta_pp_val_accuracy": round(delta_pp_acc, 3),
        "delta_pct_param_count": round(delta_pct_params, 2),
        "verdict": verdict,
    }
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
