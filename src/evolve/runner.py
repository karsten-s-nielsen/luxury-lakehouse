"""CLI entry point for the Evolve engine.

Wires together configuration, compute backends, seed evaluation, and
OpenEvolve's evolutionary loop into a single ``evolve`` command.

Usage::

    uv run evolve --target scoutgpt
    uv run evolve --target scoutgpt --backend remote_ssh --iterations 200
    uv run evolve --target scoutgpt --config path/to/custom.yaml --resume
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from evolve.backends import create_backend
from evolve.config import EvolveConfig
from evolve.evaluator import EvolveEvaluator

_log = logging.getLogger(__name__)

_TARGETS_DIR = Path(__file__).parent / "targets"


# ---------------------------------------------------------------------------
# Seed evaluation
# ---------------------------------------------------------------------------


def _discover_seed_programs(target: str) -> list[Path]:
    """Return all seed program ``.py`` files for *target*, excluding __init__.py."""
    seed_dir = _TARGETS_DIR / target / "seed_programs"
    if not seed_dir.is_dir():
        msg = f"Seed programs directory not found: {seed_dir}"
        raise FileNotFoundError(msg)
    programs = sorted(p for p in seed_dir.glob("*.py") if p.name != "__init__.py")
    if not programs:
        msg = f"No seed programs found in {seed_dir}"
        raise FileNotFoundError(msg)
    return programs


def _evaluate_seeds(
    evaluator: EvolveEvaluator,
    seed_programs: list[Path],
    results_dir: Path,
    max_parallel: int = 1,
) -> tuple[Path, dict[str, float]]:
    """Evaluate all seed programs and return the best one.

    When *max_parallel* > 1 and multiple compute backends are available,
    seed evaluations are dispatched concurrently via a thread pool.  The
    :class:`BackendPool` is thread-safe, so each thread acquires the next
    idle backend automatically.

    Saves individual seed results as JSON files under ``results_dir/seed_results/``.

    Args:
        evaluator: The configured evaluator instance.
        seed_programs: List of paths to seed ``.py`` files.
        results_dir: Timestamped results directory for this run.
        max_parallel: Maximum concurrent evaluations (matches backend count).

    Returns:
        Tuple of (best_seed_path, best_metrics).

    Raises:
        RuntimeError: If no seed produces a valid (non-zero) combined score.
    """
    seed_results_dir = results_dir / "seed_results"
    seed_results_dir.mkdir(parents=True, exist_ok=True)

    def _eval_one(program: Path) -> tuple[Path, dict[str, float]]:
        _log.info("Evaluating seed program: %s", program.name)
        metrics = evaluator.evaluate(str(program))
        score = metrics.get("combined_score", 0.0)

        result_file = seed_results_dir / f"{program.stem}.json"
        result_file.write_text(json.dumps({"program": program.name, "metrics": metrics}, indent=2))
        _log.info("Seed %s: combined_score=%.4f", program.name, score)
        return program, metrics

    workers = min(max_parallel, len(seed_programs))
    _log.info("Evaluating %d seeds with %d parallel workers", len(seed_programs), workers)

    best_path: Path | None = None
    best_metrics: dict[str, float] = {}
    best_score = float("-inf")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_eval_one, p): p for p in seed_programs}
        for future in concurrent.futures.as_completed(futures):
            program, metrics = future.result()
            score = metrics.get("combined_score", 0.0)
            if score > best_score:
                best_score = score
                best_path = program
                best_metrics = metrics

    if best_path is None or best_score <= 0.0:
        msg = "No seed program produced a valid (non-zero) combined score"
        raise RuntimeError(msg)

    _log.info("Best seed: %s (combined_score=%.4f)", best_path.name, best_score)
    return best_path, best_metrics


# ---------------------------------------------------------------------------
# OpenEvolve integration
# ---------------------------------------------------------------------------


def _translate_to_openevolve_config(config: EvolveConfig) -> dict[str, Any]:
    """Translate our EvolveConfig to the nested dict expected by ``openevolve.Config.from_dict``.

    OpenEvolve uses nested sections: ``database`` (population/islands),
    ``evaluator`` (parallelism/timeout), ``llm`` (models/temperature).
    """
    evo = config.evolution
    llm = config.llm

    oe_config: dict[str, Any] = {
        "max_iterations": evo.iterations,
        "diff_based_evolution": evo.diff_based,
        "early_stopping_patience": evo.early_stopping_patience,
        "database": {
            "population_size": evo.population_size,
            "num_islands": evo.num_islands,
            "migration_interval": evo.migration_interval,
        },
        "evaluator": {
            "parallel_evaluations": evo.parallel_evaluations,
            "timeout": config.evaluation.timeout_seconds,
        },
    }

    if llm.models:
        oe_config["llm"] = {
            "models": [
                {
                    "name": m.name,
                    "weight": m.weight,
                    "api_base": m.api_base,
                    "api_key": f"${{{m.api_key_env}}}",
                }
                for m in llm.models
            ],
            "temperature": llm.temperature,
            "max_tokens": llm.max_tokens,
        }

    return oe_config


def _set_api_keys(config: EvolveConfig) -> None:
    """Verify that required API key environment variables are set.

    Logs a warning for any missing keys. Does not set them — they must
    already be present in the environment.
    """
    seen: set[str] = set()
    for model in config.llm.models:
        env_var = model.api_key_env
        if env_var in seen:
            continue
        seen.add(env_var)
        if not os.environ.get(env_var):
            _log.warning("Environment variable %s is not set (required by model %s)", env_var, model.name)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="evolve",
        description="LLM-guided evolutionary architecture search for ML training pipelines.",
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Target domain name (e.g. 'scoutgpt'). Must have a directory under evolve/targets/.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a YAML config file. Defaults to evolve/targets/<target>/config.yaml.",
    )
    parser.add_argument(
        "--backend",
        default=None,
        help="Override the compute backend type (local_cuda, docker, hf_jobs, remote_ssh).",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Override the compute device (e.g. 'cuda:0', 'cuda:1').",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Override the number of evolution iterations.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the latest results directory for this target.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for the Evolve engine."""
    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s","name":"%(name)s","level":"%(levelname)s","message":"%(message)s"}',
    )

    parser = _build_parser()
    args = parser.parse_args(argv)

    # ---- Load config ------------------------------------------------
    target: str = args.target
    config_path: Path = args.config or (_TARGETS_DIR / target / "config.yaml")
    if not config_path.exists():
        parser.error(f"Config file not found: {config_path}")

    _log.info("Loading config from %s", config_path)
    config = EvolveConfig.from_yaml(config_path)

    # ---- Apply CLI overrides ----------------------------------------
    if args.backend:
        config.backend.type = args.backend
    if args.device:
        config.backend.device = args.device
    if args.iterations:
        config.evolution.iterations = args.iterations

    # ---- Create backend and verify ----------------------------------
    backend = create_backend(config.backend, timeout=config.evaluation.timeout_seconds)
    if not backend.available():
        _log.error("Backend '%s' is not available. Check hardware and drivers.", config.backend.type)
        raise SystemExit(1)
    _log.info("Backend '%s' is available", config.backend.type)

    # Warn if parallel_evaluations exceeds the number of backends — extra
    # threads will block waiting for an idle backend, wasting thread resources.
    backend_count = len([t.strip() for t in config.backend.type.split(",")])
    if config.evolution.parallel_evaluations > backend_count:
        _log.warning(
            "parallel_evaluations (%d) exceeds backend count (%d) — %d thread(s) will block waiting",
            config.evolution.parallel_evaluations,
            backend_count,
            config.evolution.parallel_evaluations - backend_count,
        )

    # ---- Set up results directory -----------------------------------
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results_dir = Path("results/evolve") / target / timestamp
    results_dir.mkdir(parents=True, exist_ok=True)
    _log.info("Results directory: %s", results_dir)

    # Save config snapshot
    config_snapshot_path = results_dir / "config.yaml"
    config_snapshot_path.write_text(yaml.dump(config.model_dump(), default_flow_style=False, sort_keys=False))

    # ---- Evaluate seed programs -------------------------------------
    evaluator = EvolveEvaluator(
        backend=backend,
        target=target,
        eval_config=config.evaluation,
        fitness_config=config.fitness,
    )

    seed_programs = _discover_seed_programs(target)
    _log.info("Found %d seed programs", len(seed_programs))

    best_seed, seed_metrics = _evaluate_seeds(
        evaluator, seed_programs, results_dir, max_parallel=config.evolution.parallel_evaluations
    )
    _log.info("Best seed: %s -> %s", best_seed.name, json.dumps(seed_metrics, indent=2))

    # ---- Run evolutionary search ------------------------------------
    try:
        import openevolve
    except ImportError:
        _log.error("openevolve is not installed. Install with: uv pip install 'luxury-lakehouse[evolve]'")
        raise SystemExit(1)  # noqa: B904

    _set_api_keys(config)
    oe_config = _translate_to_openevolve_config(config)

    _log.info(
        "Starting evolution: %d iterations, population=%d, islands=%d",
        config.evolution.iterations,
        config.evolution.population_size,
        config.evolution.num_islands,
    )

    oe_cfg = openevolve.Config.from_dict(oe_config)
    best_result = openevolve.run_evolution(
        initial_program=str(best_seed),
        evaluator=evaluator.evaluate,
        config=oe_cfg,
        iterations=config.evolution.iterations,
        output_dir=str(results_dir),
    )

    # ---- Save best result -------------------------------------------
    best_code = best_result.best_code or ""
    if best_code:
        dest = results_dir / "best_program.py"
        dest.write_text(best_code)
        _log.info("Best program saved to %s", dest)

    best_metrics_out: dict[str, Any] = best_result.metrics or {}
    metrics_path = results_dir / "metrics.json"
    metrics_path.write_text(json.dumps(best_metrics_out, indent=2))
    _log.info("Best metrics saved to %s", metrics_path)

    primary = config.fitness.primary
    _log.info(
        "Evolution complete. Best %s: %.4f (score=%.4f)",
        primary,
        best_metrics_out.get(primary, 0.0),
        best_result.best_score,
    )


if __name__ == "__main__":
    main()
