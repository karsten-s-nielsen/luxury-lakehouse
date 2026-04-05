"""Remote worker — runs a candidate evaluation and prints JSON metrics to stdout.

This module is invoked on a remote machine by :class:`evolve.backends.remote_ssh.RemoteSSHBackend`::

    python -m evolve.remote_worker candidate.py cuda:0 5 42 scoutgpt

It loads the candidate config from the given Python file, imports the
target evaluator, runs ``train_and_evaluate``, and writes the resulting
metrics dict as a single JSON line to stdout.  All other output (logs,
warnings) goes to stderr so it does not interfere with JSON parsing on
the caller side.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import sys
import types
from typing import Any

_log = logging.getLogger(__name__)


def _load_candidate_config(candidate_path: str) -> dict[str, Any]:
    """Load ``config`` from a Python file written by the SSH backend.

    The file is expected to contain a single assignment:
    ``config = {...}``
    """
    spec = importlib.util.spec_from_file_location("_candidate", candidate_path)
    if spec is None or spec.loader is None:
        msg = f"Cannot load candidate config from {candidate_path}"
        raise ImportError(msg)

    module = types.ModuleType("_candidate")
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    config: dict[str, Any] = module.config  # type: ignore[attr-defined]
    return config


def main() -> None:
    """Entry point for remote candidate evaluation.

    Arguments (positional):
        1. candidate_path — path to a Python file containing ``config = {...}``
        2. device — PyTorch device string (default ``cuda:0``)
        3. epochs — number of training epochs (default ``5``)
        4. seed — random seed (default ``42``)
        5. target — target name under ``evolve.targets`` (default ``scoutgpt``)
    """
    if len(sys.argv) < 2:
        print("Usage: python -m evolve.remote_worker <candidate.py> [device] [epochs] [seed] [target]", file=sys.stderr)
        sys.exit(1)

    candidate_path = sys.argv[1]
    device = sys.argv[2] if len(sys.argv) > 2 else "cuda:0"
    epochs = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    seed = int(sys.argv[4]) if len(sys.argv) > 4 else 42
    target = sys.argv[5] if len(sys.argv) > 5 else "scoutgpt"

    # Redirect logging to stderr so stdout stays clean for JSON output.
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(name)s %(message)s")

    _log.info("Loading candidate config from %s", candidate_path)
    config = _load_candidate_config(candidate_path)

    _log.info("Running %s evaluator (device=%s, epochs=%d, seed=%d)", target, device, epochs, seed)
    target_module = importlib.import_module(f"evolve.targets.{target}.evaluator")
    metrics: dict[str, float] = target_module.train_and_evaluate(
        candidate_config=config,
        device=device,
        epochs=epochs,
        seed=seed,
    )

    # Single JSON line to stdout — the SSH caller parses this.
    print(json.dumps(metrics))


if __name__ == "__main__":
    main()
