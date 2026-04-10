"""Benchmark HF Jobs GPU flavors for evolve candidate training.

Submits the same candidate config to all specified GPU flavors in parallel,
polls until complete, and reports runtime + cost comparison.

Usage::

    uv run python scripts/benchmark_hf_jobs.py
"""

from __future__ import annotations

import base64
import json
import logging
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from huggingface_hub import HfApi, get_token
from huggingface_hub._jobs_api import JobStage

from shared.wheel import WHEEL_BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Benchmark config
# ---------------------------------------------------------------------------

# Cross-attention seed — the best-performing seed from Stage 1.
CANDIDATE_CONFIG: dict = {
    "conditioning_type": "cross_attention",
    "hidden_dim": 256,
    "num_layers": 4,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 3e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
    "dataset": "luxury-lakehouse/scoutgpt-training-data",
}

EPOCHS = 3
SEED = 42
TARGET = "scoutgpt"
TIMEOUT = "2h"

# GPU flavors to benchmark (name, $/hr from live API).
FLAVORS: list[tuple[str, float]] = [
    ("l4x1", 0.80),
    ("a10g-large", 1.50),
    ("l40sx1", 1.80),
    ("a100-large", 2.50),
    ("h200", 5.00),
]

POLL_INTERVAL = 30  # seconds (conservative to avoid HF API rate limits)

# ---------------------------------------------------------------------------
# Worker script (runs on HF Jobs)
# ---------------------------------------------------------------------------

WORKER_SCRIPT = f'''\
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse[analytics,training] @ {WHEEL_BASE_URL}",
# ]
# ///

"""HF Jobs benchmark worker — trains a candidate and prints JSON metrics."""

import base64
import importlib
import json
import logging
import os
import sys

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(name)s %(message)s")
_log = logging.getLogger("benchmark_worker")


def main() -> None:
    config_b64 = os.environ.get("EVOLVE_CANDIDATE_CONFIG", "")
    if not config_b64:
        print(json.dumps({{"error": 1.0, "reason": "missing config"}}))
        sys.exit(0)

    candidate_config = json.loads(base64.b64decode(config_b64).decode())
    device = os.environ.get("EVOLVE_DEVICE", "cuda:0")
    epochs = int(os.environ.get("EVOLVE_EPOCHS", "3"))
    seed = int(os.environ.get("EVOLVE_SEED", "42"))
    target = os.environ.get("EVOLVE_TARGET", "scoutgpt")

    _log.info("Benchmark: %s evaluator (device=%s, epochs=%d)", target, device, epochs)

    target_module = importlib.import_module(f"evolve.targets.{{target}}.evaluator")
    metrics = target_module.train_and_evaluate(
        candidate_config=candidate_config,
        device=device,
        epochs=epochs,
        seed=seed,
    )

    # Compute combined_score using the same weights as EvolveEvaluator
    rho_weight = float(os.environ.get("EVOLVE_RHO_WEIGHT", "0.7"))
    top1_weight = float(os.environ.get("EVOLVE_TOP1_WEIGHT", "0.3"))
    rho = metrics.get("spearman_rho", 0.0)
    top1 = metrics.get("top1_accuracy", 0.0)
    metrics["combined_score"] = rho * rho_weight + top1 * top1_weight

    print(json.dumps(metrics))


if __name__ == "__main__":
    main()
'''


# ---------------------------------------------------------------------------
# Job management
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkJob:
    """Tracks a submitted benchmark job."""

    flavor: str
    cost_per_hr: float
    job_id: str
    submit_time: float
    end_time: float | None = None
    metrics: dict | None = None
    status: str = "RUNNING"


def submit_all(api: HfApi, namespace: str, script_path: str) -> list[BenchmarkJob]:
    """Submit one job per flavor, all in parallel."""
    config_b64 = base64.b64encode(json.dumps(CANDIDATE_CONFIG).encode()).decode()
    token = get_token() or ""
    jobs: list[BenchmarkJob] = []

    for flavor, cost_hr in FLAVORS:
        _log.info("Submitting benchmark job: %s ($%.2f/hr)", flavor, cost_hr)
        try:
            info = api.run_uv_job(
                script=script_path,
                env={
                    "EVOLVE_CANDIDATE_CONFIG": config_b64,
                    "EVOLVE_DEVICE": "cuda:0",
                    "EVOLVE_EPOCHS": str(EPOCHS),
                    "EVOLVE_SEED": str(SEED),
                    "EVOLVE_TARGET": TARGET,
                },
                secrets={"HF_TOKEN": token},
                flavor=flavor,
                timeout=TIMEOUT,
                namespace=namespace,
            )
            jobs.append(
                BenchmarkJob(
                    flavor=flavor,
                    cost_per_hr=cost_hr,
                    job_id=info.id,
                    submit_time=time.monotonic(),
                )
            )
            _log.info("  -> job_id=%s", info.id)
        except Exception:
            _log.exception("Failed to submit job for %s", flavor)

    return jobs


def poll_all(api: HfApi, jobs: list[BenchmarkJob], namespace: str) -> None:
    """Poll all jobs until every one reaches a terminal state."""
    pending = {j.job_id: j for j in jobs if j.status == "RUNNING"}

    while pending:
        for job_id, job in list(pending.items()):
            try:
                info = api.inspect_job(job_id=job_id, namespace=namespace)
                stage = info.status.stage

                if stage == JobStage.COMPLETED:
                    job.end_time = time.monotonic()
                    job.status = "COMPLETED"
                    job.metrics = parse_metrics(api, job_id, namespace)
                    elapsed = job.end_time - job.submit_time
                    _log.info(
                        "%s COMPLETED in %.0fs (score=%.4f)",
                        job.flavor,
                        elapsed,
                        job.metrics.get("combined_score", 0),
                    )
                    del pending[job_id]
                elif stage in (JobStage.ERROR, JobStage.CANCELED, JobStage.DELETED):
                    job.end_time = time.monotonic()
                    job.status = stage.value
                    msg = info.status.message or "unknown"
                    _log.error("%s FAILED: %s — %s", job.flavor, stage.value, msg)
                    del pending[job_id]
            except Exception:
                _log.warning("Error polling %s (%s)", job.flavor, job_id, exc_info=True)

        if pending:
            remaining = ", ".join(j.flavor for j in pending.values())
            _log.info("Waiting on %d jobs: %s", len(pending), remaining)
            time.sleep(POLL_INTERVAL)


def parse_metrics(api: HfApi, job_id: str, namespace: str) -> dict:
    """Extract JSON metrics from job logs."""
    logs = list(api.fetch_job_logs(job_id=job_id, namespace=namespace))
    for line in reversed(logs):
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                data = json.loads(stripped)
                if "training_time_seconds" in data or "combined_score" in data:
                    return data
            except json.JSONDecodeError:
                continue
    return {"error": 1.0, "reason": "no metrics in logs"}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_report(jobs: list[BenchmarkJob]) -> None:
    """Print the benchmark comparison table."""
    print("\n" + "=" * 90)
    print("HF Jobs GPU Benchmark — ScoutGPT cross_attention, 3 epochs, 894K episodes")
    print("=" * 90)
    print(
        f"{'Flavor':<14} {'GPU':<10} {'Status':<10} {'Wall (s)':<10} "
        f"{'Train (s)':<10} {'$/hr':<8} {'Cost ($)':<10} {'Score':<8}"
    )
    print("-" * 90)

    for job in sorted(jobs, key=lambda j: j.cost_per_hr):
        wall = (job.end_time - job.submit_time) if job.end_time else 0
        train = job.metrics.get("training_time_seconds", 0) if job.metrics else 0
        cost = (wall / 3600) * job.cost_per_hr
        score = job.metrics.get("combined_score", 0) if job.metrics else 0

        print(
            f"{job.flavor:<14} {'—':<10} {job.status:<10} {wall:<10.0f} "
            f"{train:<10.0f} ${job.cost_per_hr:<7.2f} ${cost:<9.4f} {score:<8.4f}"
        )

    print("-" * 90)

    # Best value calculation
    completed = [j for j in jobs if j.status == "COMPLETED" and j.end_time]
    if completed:
        best_value = min(
            completed,
            key=lambda j: ((j.end_time - j.submit_time) / 3600) * j.cost_per_hr,  # type: ignore[operator]
        )
        best_wall = (best_value.end_time - best_value.submit_time) if best_value.end_time else 0  # type: ignore[operator]
        best_cost = (best_wall / 3600) * best_value.cost_per_hr
        print(f"\nBest value: {best_value.flavor} — ${best_cost:.4f}/candidate, {best_wall:.0f}s wall time")

        fastest = min(completed, key=lambda j: (j.end_time or 0) - j.submit_time)
        fast_wall = (fastest.end_time - fastest.submit_time) if fastest.end_time else 0  # type: ignore[operator]
        fast_cost = (fast_wall / 3600) * fastest.cost_per_hr
        print(f"Fastest:    {fastest.flavor} — ${fast_cost:.4f}/candidate, {fast_wall:.0f}s wall time")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    api = HfApi()

    # Use explicit namespace to avoid whoami rate limits.
    namespace = "karstenskyt"
    _log.info("Using namespace: %s", namespace)

    # Write worker script to a temp file (run_uv_job expects a file path)
    script_dir = Path(tempfile.mkdtemp(prefix="evolve_benchmark_"))
    script_path = script_dir / "benchmark_worker.py"
    script_path.write_text(WORKER_SCRIPT, encoding="utf-8")
    _log.info("Worker script written to %s", script_path)

    # Submit all benchmark jobs
    jobs = submit_all(api, namespace, str(script_path))
    if not jobs:
        _log.error("No jobs submitted — aborting")
        return

    _log.info("Submitted %d benchmark jobs, polling...", len(jobs))

    # Poll until all complete
    poll_all(api, jobs, namespace)

    # Report
    print_report(jobs)

    # Save raw results
    results = []
    for job in jobs:
        wall = (job.end_time - job.submit_time) if job.end_time else 0
        results.append(
            {
                "flavor": job.flavor,
                "job_id": job.job_id,
                "status": job.status,
                "wall_seconds": wall,
                "cost_per_hr": job.cost_per_hr,
                "cost_total": (wall / 3600) * job.cost_per_hr,
                "metrics": job.metrics,
            }
        )

    out_path = "results/evolve/hf_jobs_benchmark.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    _log.info("Raw results saved to %s", out_path)


if __name__ == "__main__":
    main()
