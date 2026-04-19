"""Re-train EV1 rope-variant candidates at 15-epoch fidelity with the proper RoPE primitive.

Context: the initial EV1 top-N validation ran before ``src/analytics/rope.py`` +
``src/analytics/rotary_attention.py`` existed. The rope branch in
``Football2VecEncoder`` at that point added a tiled sine pattern to the input
embeddings — an approximation that was not real RoPE and that the model could
exploit as a shortcut, producing inflated 15-epoch val_accuracy (0.7514,
0.7173) that did not reflect a genuine improvement.

This script re-runs only the rope-configured candidates from the same
checkpoint with the proper RoPE implementation now wired into the encoder, so
their honest 15-epoch numbers can replace the artifact results in the
corrected SUMMARY.md leaderboard.

Usage (local, overnight):

    nohup uv run python scripts/validate_ev1_rope_only.py \\
        > results/evolve/football2vec/validate_rope_only_15ep.log 2>&1 &

Expected wall-clock: 2 candidates x ~25-30 min = ~1 hour on the RTX 5070 Ti.
Cost: $0 (local). Module-level dataset cache in
``evolve.targets.football2vec.evaluator`` is reused across candidates.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

from evolve.targets.football2vec.evaluator import train_and_evaluate  # noqa: E402

CHECKPOINT_DIR = Path("results/evolve/football2vec/20260418T232631Z/checkpoints/checkpoint_50/programs")

# Limit to the rope candidates that appeared in the published top-10 at 5-epoch fidelity —
# the ones whose artifact 15-epoch numbers (0.7514, 0.7173) need to be replaced in the
# corrected SUMMARY.md leaderboard. Other rope candidates were below rank 10 and not
# published. Scope tight on purpose; set to None to re-run every rope candidate.
TARGET_ITERATIONS: set[int] | None = {1, 16}


def _extract_config(code: str) -> dict[str, Any]:
    """Extract the ``config`` dict literal from a candidate program's source string."""
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


def _load_rope_candidates() -> list[dict[str, Any]]:
    """Load candidate programs, filter to those with position_embedding='rope'."""
    if not CHECKPOINT_DIR.is_dir():
        msg = f"Checkpoint dir not found: {CHECKPOINT_DIR}"
        raise FileNotFoundError(msg)
    rope_candidates: list[dict[str, Any]] = []
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
            continue
        try:
            config = _extract_config(data["code"])
        except (SyntaxError, ValueError, TypeError):
            logger.exception("skipping %s — config-extract failed", data.get("id", "?"))
            continue
        if config.get("position_embedding") != "rope":
            continue
        iter_found = int(data.get("iteration_found", -1))
        if TARGET_ITERATIONS is not None and iter_found not in TARGET_ITERATIONS:
            continue
        rope_candidates.append(
            {
                "id": data["id"],
                "iteration_found": int(data.get("iteration_found", -1)),
                "val_acc_5ep": val_acc_5ep,
                "val_loss_5ep": float(metrics.get("val_loss", float("inf"))),
                "param_count_5ep": float(metrics.get("param_count", 0.0)),
                "config": config,
            }
        )
    rope_candidates.sort(key=lambda c: c["iteration_found"])
    return rope_candidates


def _validate_one(candidate: dict[str, Any], ordinal: int, total: int) -> dict[str, Any]:
    """Run 15-epoch training for one rope candidate and return a leaderboard entry."""
    prog_id = candidate["id"]
    iter_n = candidate["iteration_found"]
    val_5ep = candidate["val_acc_5ep"]
    config = candidate["config"]

    logger.info(
        "[%d/%d] iter=%d id=%s rope-config val_acc_5ep=%.4f — training at 15 epochs with proper RoPE",
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
        "source": "validated_with_proper_rope",
    }


def main() -> None:
    if not os.environ.get("HF_TOKEN"):
        print(json.dumps({"status": "error", "reason": "HF_TOKEN env var not set"}), flush=True)
        sys.exit(1)

    rope_candidates = _load_rope_candidates()
    logger.info("Loaded %d rope-variant candidates from %s", len(rope_candidates), CHECKPOINT_DIR)
    for i, c in enumerate(rope_candidates, 1):
        logger.info("  [%d] iter=%d id=%s val_acc_5ep=%.4f", i, c["iteration_found"], c["id"], c["val_acc_5ep"])

    if not rope_candidates:
        logger.warning("no rope candidates to validate; nothing to do")
        print(json.dumps({"status": "done", "leaderboard": []}, indent=2), flush=True)
        return

    leaderboard: list[dict[str, Any]] = []
    for i, candidate in enumerate(rope_candidates, 1):
        try:
            entry = _validate_one(candidate, i, len(rope_candidates))
        except Exception as exc:
            logger.exception("[%d/%d] candidate crashed; continuing with next", i, len(rope_candidates))
            entry = {
                "id": candidate["id"],
                "iteration_found": candidate["iteration_found"],
                "val_acc_5ep": candidate["val_acc_5ep"],
                "val_acc_15ep": 0.0,
                "error": repr(exc),
                "source": "training_error",
            }
        leaderboard.append(entry)

    leaderboard.sort(key=lambda x: x.get("val_acc_15ep", 0.0), reverse=True)

    logger.info("=" * 80)
    logger.info("ROPE-ONLY LEADERBOARD (sorted by val_acc_15ep, proper RoPE)")
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
