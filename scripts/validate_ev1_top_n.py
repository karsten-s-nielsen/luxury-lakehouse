"""Validate the top-N EV1 candidates at 15-epoch fidelity.

Reads all program JSON files from the final EV1 checkpoint, sorts by 5-epoch
`val_accuracy`, and re-trains the top 10 at 15 epochs on the local GPU. Reports
a leaderboard sorted by 15-epoch val_accuracy so we can verify that iter-11
(already validated at 15 epochs) still wins at full fidelity, rather than being
an artifact of the 5-epoch signal-compression effect observed in the EV1 POC.

Cached: iter-11's 15-epoch result is reused (from scripts/validate_ev1_iter11.py)
rather than re-run. All other top-10 candidates get a fresh 15-epoch training run.

Usage (local, overnight):

    nohup uv run python scripts/validate_ev1_top_n.py \\
        > results/evolve/football2vec/validate_top_n_15ep.log 2>&1 &

Expected wall-clock: ~9 candidates x ~35 min = ~5.5 hours on the RTX 5070 Ti.
Cost: $0 (local). Each candidate runs serially; the module-level dataset cache in
`evolve.targets.football2vec.evaluator` is reused across all candidates (loaded
once from HF Hub on the first candidate, ~30s).

Run fidelity matches production: 15 epochs, patience=`max(2, epochs//2)` = 7,
weight_decay=0.01, cosine warmup (10%). Same as
`scripts/validate_ev1_iter11.py` for iter-11 specifically.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# Root logger must be configured BEFORE importing evaluator so its INFO epoch
# messages are visible (lesson from scripts/validate_ev1_iter11.py, which omitted
# this and silenced progress output for 35 min).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

from evolve.targets.football2vec.evaluator import train_and_evaluate  # noqa: E402

CHECKPOINT_DIR = Path("results/evolve/football2vec/20260418T232631Z/checkpoints/checkpoint_50/programs")
ITER_11_ID = "d7f0076e-060b-41c3-8d67-ccc1dde0e4de"
ITER_11_15EP_VAL_ACC = 0.5824087841701933  # from scripts/validate_ev1_iter11.py local run 2026-04-19
ITER_11_15EP_VAL_LOSS = 1.005638665623135
ITER_11_15EP_PARAM_COUNT = 1_295_255.0
ITER_11_15EP_TRAINING_TIME = 2078.562
TOP_N = 10


def _extract_config(code: str) -> dict[str, Any]:
    """Extract the `config` dict literal from a candidate program's source string."""
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "config":
                    value_source = ast.get_source_segment(code, node.value)
                    if value_source is None:
                        msg = "Cannot extract config source"
                        raise ValueError(msg)
                    raw = ast.literal_eval(value_source)
                    if not isinstance(raw, dict):
                        msg = f"config is not a dict: {type(raw).__name__}"
                        raise ValueError(msg)
                    return raw  # type: ignore[return-value]
    msg = "No 'config' assignment in candidate source"
    raise ValueError(msg)


def _load_candidates() -> list[dict[str, Any]]:
    """Load all candidate programs from the final checkpoint, filter to valid ones."""
    if not CHECKPOINT_DIR.is_dir():
        msg = f"Checkpoint dir not found: {CHECKPOINT_DIR}"
        raise FileNotFoundError(msg)
    candidates: list[dict[str, Any]] = []
    for p in CHECKPOINT_DIR.glob("*.json"):
        try:
            raw = p.read_text(encoding="utf-8")
        except OSError:
            logger.warning("could not read %s", p.name)
            continue
        if not raw.strip():
            logger.warning("skipping empty program file %s", p.name)
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("skipping malformed program file %s", p.name)
            continue
        metrics = data.get("metrics") or {}
        val_acc_5ep = float(metrics.get("val_accuracy", 0.0))
        if val_acc_5ep <= 0.0 or metrics.get("error", 0) == 1.0:
            continue  # skip failed candidates
        candidates.append(
            {
                "id": data["id"],
                "iteration_found": int(data.get("iteration_found", -1)),
                "val_acc_5ep": val_acc_5ep,
                "val_loss_5ep": float(metrics.get("val_loss", float("inf"))),
                "param_count_5ep": float(metrics.get("param_count", 0.0)),
                "code": data["code"],
            }
        )
    candidates.sort(key=lambda c: c["val_acc_5ep"], reverse=True)
    return candidates


def _validate_one(candidate: dict[str, Any], ordinal: int, total: int) -> dict[str, Any]:
    """Run 15-epoch training for one candidate and return a leaderboard entry."""
    prog_id = candidate["id"]
    iter_n = candidate["iteration_found"]
    val_5ep = candidate["val_acc_5ep"]

    if prog_id == ITER_11_ID:
        logger.info(
            "[%d/%d] iter=%d id=%s val_acc_5ep=%.4f val_acc_15ep=%.4f (cached from prior run)",
            ordinal,
            total,
            iter_n,
            prog_id,
            val_5ep,
            ITER_11_15EP_VAL_ACC,
        )
        return {
            "id": prog_id,
            "iteration_found": iter_n,
            "val_acc_5ep": val_5ep,
            "val_acc_15ep": ITER_11_15EP_VAL_ACC,
            "val_loss_15ep": ITER_11_15EP_VAL_LOSS,
            "param_count": ITER_11_15EP_PARAM_COUNT,
            "training_time_seconds_15ep": ITER_11_15EP_TRAINING_TIME,
            "source": "cached_from_prior_validation",
        }

    try:
        config = _extract_config(candidate["code"])
    except Exception:
        logger.exception("[%d/%d] iter=%d id=%s config-extract failed; skipping", ordinal, total, iter_n, prog_id)
        return {
            "id": prog_id,
            "iteration_found": iter_n,
            "val_acc_5ep": val_5ep,
            "val_acc_15ep": 0.0,
            "source": "config_extract_error",
        }

    logger.info(
        "[%d/%d] iter=%d id=%s val_acc_5ep=%.4f — training at 15 epochs",
        ordinal,
        total,
        iter_n,
        prog_id,
        val_5ep,
    )
    result = train_and_evaluate(
        candidate_config=config,
        device="cuda:0",
        epochs=15,
        seed=42,
    )
    val_15ep = float(result.get("val_accuracy", 0.0))
    logger.info(
        "[%d/%d] iter=%d id=%s  ->  val_acc_15ep=%.4f  time=%.1fs",
        ordinal,
        total,
        iter_n,
        prog_id,
        val_15ep,
        result.get("training_time_seconds", 0.0),
    )
    return {
        "id": prog_id,
        "iteration_found": iter_n,
        "val_acc_5ep": val_5ep,
        "val_acc_15ep": val_15ep,
        "val_loss_15ep": float(result.get("val_loss", float("inf"))),
        "param_count": float(result.get("param_count", 0.0)),
        "epochs_trained_15ep": float(result.get("epochs_trained", 0.0)),
        "training_time_seconds_15ep": float(result.get("training_time_seconds", 0.0)),
        "config": config,
        "source": "validated",
    }


def main() -> None:
    if not os.environ.get("HF_TOKEN"):
        print(json.dumps({"status": "error", "reason": "HF_TOKEN env var not set"}), flush=True)
        sys.exit(1)

    all_candidates = _load_candidates()
    logger.info("Loaded %d valid candidates from %s", len(all_candidates), CHECKPOINT_DIR)

    top_n = all_candidates[:TOP_N]
    logger.info("Evaluating top %d candidates at 15-epoch fidelity", len(top_n))
    for i, c in enumerate(top_n, 1):
        logger.info("  [%d] iter=%d id=%s val_acc_5ep=%.4f", i, c["iteration_found"], c["id"], c["val_acc_5ep"])

    leaderboard: list[dict[str, Any]] = []
    for i, candidate in enumerate(top_n, 1):
        try:
            entry = _validate_one(candidate, i, len(top_n))
        except Exception as exc:
            logger.exception("[%d/%d] candidate crashed; continuing with next", i, len(top_n))
            entry = {
                "id": candidate["id"],
                "iteration_found": candidate["iteration_found"],
                "val_acc_5ep": candidate["val_acc_5ep"],
                "val_acc_15ep": 0.0,
                "error": repr(exc),
                "source": "training_error",
            }
        leaderboard.append(entry)

    # Final report: sort by 15-epoch val_accuracy
    leaderboard.sort(key=lambda x: x.get("val_acc_15ep", 0.0), reverse=True)

    logger.info("=" * 80)
    logger.info("FINAL LEADERBOARD (sorted by val_acc_15ep)")
    logger.info(
        "%-4s %-8s %-36s %-12s %-12s %-12s %s",
        "rank",
        "iter",
        "id",
        "val_5ep",
        "val_15ep",
        "params",
        "source",
    )
    for rank, entry in enumerate(leaderboard, 1):
        logger.info(
            "%-4d %-8d %-36s %-12.4f %-12.4f %-12.0f %s",
            rank,
            entry.get("iteration_found", -1),
            entry["id"],
            entry["val_acc_5ep"],
            entry.get("val_acc_15ep", 0.0),
            entry.get("param_count", 0.0),
            entry.get("source", "?"),
        )

    print(json.dumps({"status": "done", "leaderboard": leaderboard}, indent=2), flush=True)


if __name__ == "__main__":
    main()
