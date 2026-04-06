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
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from evolve.backends import create_backend
from evolve.config import EvalConfig, EvolveConfig
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


def _eval_fingerprint(eval_config: EvalConfig) -> str:
    """Deterministic hash of evaluation parameters that affect seed results.

    Used to detect stale cached seed results after config changes.
    """
    key = f"{eval_config.epochs}:{eval_config.seed}:{eval_config.dataset}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _load_cached_seeds(
    seed_results_dir: Path,
    seed_programs: list[Path],
    fingerprint: str,
) -> dict[str, dict[str, float]]:
    """Load valid cached seed results, skipping stale or missing ones.

    Returns a mapping of ``{program_stem: metrics}`` for seeds whose
    cached result has a matching fingerprint.  Seeds with missing,
    corrupt, or stale result files are excluded (and will be evaluated).
    """
    cached: dict[str, dict[str, float]] = {}
    for program in seed_programs:
        result_file = seed_results_dir / f"{program.stem}.json"
        if not result_file.exists():
            continue
        try:
            data = json.loads(result_file.read_text())
            if data.get("fingerprint") != fingerprint:
                _log.info("Stale seed cache for %s (fingerprint mismatch), will re-evaluate", program.name)
                continue
            metrics = data["metrics"]
            if metrics.get("combined_score", 0.0) > 0.0:
                cached[program.stem] = metrics
                score = metrics["combined_score"]
                _log.info("Loaded cached seed: %s (combined_score=%.4f)", program.name, score)
        except (json.JSONDecodeError, KeyError):
            _log.warning("Corrupt seed cache for %s, will re-evaluate", program.name)
    return cached


def _evaluate_seeds(
    evaluator: EvolveEvaluator,
    seed_programs: list[Path],
    results_dir: Path,
    eval_config: EvalConfig,
    max_parallel: int = 1,
    cached_seeds: dict[str, dict[str, float]] | None = None,
) -> tuple[Path, dict[str, float]]:
    """Evaluate all seed programs and return the best one.

    When *max_parallel* > 1 and multiple compute backends are available,
    seed evaluations are dispatched concurrently via a thread pool.  The
    :class:`BackendPool` is thread-safe, so each thread acquires the next
    idle backend automatically.

    Seeds with valid cached results (via *cached_seeds*) are skipped.
    Saves individual seed results as JSON files under ``results_dir/seed_results/``.

    Args:
        evaluator: The configured evaluator instance.
        seed_programs: List of paths to seed ``.py`` files.
        results_dir: Timestamped results directory for this run.
        eval_config: Evaluation config (used for fingerprinting).
        max_parallel: Maximum concurrent evaluations (matches backend count).
        cached_seeds: Pre-loaded seed results from a previous run, keyed by
            program stem.  Seeds present here are skipped.

    Returns:
        Tuple of (best_seed_path, best_metrics).

    Raises:
        RuntimeError: If no seed produces a valid (non-zero) combined score.
    """
    seed_results_dir = results_dir / "seed_results"
    seed_results_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = _eval_fingerprint(eval_config)

    cached = cached_seeds or {}
    to_evaluate = [p for p in seed_programs if p.stem not in cached]

    if cached:
        _log.info(
            "Resuming seeds: %d cached, %d to evaluate",
            len(cached),
            len(to_evaluate),
        )

    def _eval_one(program: Path) -> tuple[Path, dict[str, float]]:
        _log.info("Evaluating seed program: %s", program.name)
        metrics = evaluator.evaluate(str(program))
        score = metrics.get("combined_score", 0.0)

        result_file = seed_results_dir / f"{program.stem}.json"
        result_file.write_text(
            json.dumps({"program": program.name, "fingerprint": fingerprint, "metrics": metrics}, indent=2)
        )
        _log.info("Seed %s: combined_score=%.4f", program.name, score)
        return program, metrics

    # Start with cached results
    best_path: Path | None = None
    best_metrics: dict[str, float] = {}
    best_score = float("-inf")

    for program in seed_programs:
        if program.stem in cached:
            score = cached[program.stem].get("combined_score", 0.0)
            if score > best_score:
                best_score = score
                best_path = program
                best_metrics = cached[program.stem]

    # Evaluate remaining seeds
    if to_evaluate:
        workers = min(max_parallel, len(to_evaluate))
        _log.info("Evaluating %d seeds with %d parallel workers", len(to_evaluate), workers)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_eval_one, p): p for p in to_evaluate}
            for future in concurrent.futures.as_completed(futures):
                program, metrics = future.result()
                score = metrics.get("combined_score", 0.0)
                if score > best_score:
                    best_score = score
                    best_path = program
                    best_metrics = metrics
    else:
        _log.info("All %d seeds loaded from cache — skipping evaluation", len(cached))

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


_EVALUATOR_SCRIPT = '''\
"""Standalone evaluator loaded by OpenEvolve via importlib.

Fully self-contained: constructs its own EvolveEvaluator from a JSON
config file written alongside this script.  This is necessary because
OpenEvolve's ``process_parallel`` spawns worker processes via
``ProcessPoolExecutor`` — each worker gets a fresh Python interpreter
where in-process globals (like module-level references) are ``None``.

The JSON config file is written by ``_write_evaluator_script()`` in
``evolve.runner`` and contains all parameters needed to reconstruct
the backend, evaluator, and fitness config.
"""

import json
import logging
from pathlib import Path

_log = logging.getLogger(__name__)

# Lazy singleton — constructed once per process on first evaluate() call.
_evaluator = None


def _get_evaluator():
    global _evaluator
    if _evaluator is not None:
        return _evaluator

    config_path = Path(__file__).with_name("_openevolve_evaluator_config.json")
    cfg = json.loads(config_path.read_text())

    from evolve.backends import create_backend
    from evolve.config import BackendConfig, EvalConfig, FitnessConfig
    from evolve.evaluator import EvolveEvaluator

    backend_config = BackendConfig(**cfg["backend"])
    backend = create_backend(backend_config, timeout=cfg["timeout_seconds"])
    eval_config = EvalConfig(**cfg["eval_config"])
    fitness_config = FitnessConfig(**cfg["fitness_config"])

    _evaluator = EvolveEvaluator(
        backend=backend,
        target=cfg["target"],
        eval_config=eval_config,
        fitness_config=fitness_config,
    )
    _log.info("Evaluator constructed in worker process (pid=%d)", __import__("os").getpid())
    return _evaluator


def evaluate(program_path: str) -> dict:
    """Entry point called by OpenEvolve for each candidate."""
    return _get_evaluator().evaluate(program_path)
'''


def _write_evaluator_script(results_dir: Path, config: EvolveConfig) -> Path:
    """Write a standalone evaluator ``.py`` and its config JSON.

    The script and config are placed in ``results_dir`` so they persist
    with the run artifacts for debugging.  OpenEvolve accepts a file
    path as the *evaluator* argument to :func:`run_evolution`.
    """
    # Write the config JSON that the script loads at init time
    eval_cfg = {
        "target": config.target,
        "timeout_seconds": config.evaluation.timeout_seconds,
        "backend": config.backend.model_dump(),
        "eval_config": config.evaluation.model_dump(),
        "fitness_config": config.fitness.model_dump(),
    }
    config_path = results_dir / "_openevolve_evaluator_config.json"
    config_path.write_text(json.dumps(eval_cfg, indent=2))

    # Write the evaluator script
    script_path = results_dir / "_openevolve_evaluator.py"
    script_path.write_text(_EVALUATOR_SCRIPT, encoding="utf-8")
    return script_path


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
    # OpenEvolve logs emoji (arrows, checkmarks) that crash Windows cp1252 console.
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

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
    seed_programs = _discover_seed_programs(target)
    cached_seeds: dict[str, dict[str, float]] = {}

    if args.resume:
        # Scan results directories newest-to-oldest, use the first one
        # that has any valid cached seeds (skips killed/incomplete runs).
        target_results = Path("results/evolve") / target
        if target_results.is_dir():
            fingerprint = _eval_fingerprint(config.evaluation)
            for resume_dir in sorted(target_results.iterdir(), reverse=True):
                seed_results_dir = resume_dir / "seed_results"
                if not seed_results_dir.is_dir():
                    continue
                cached_seeds = _load_cached_seeds(seed_results_dir, seed_programs, fingerprint)
                if cached_seeds:
                    _log.info(
                        "Resuming from %s: %d/%d seeds cached",
                        resume_dir.name,
                        len(cached_seeds),
                        len(seed_programs),
                    )
                    break
            else:
                _log.warning("No valid cached seeds for target '%s' — running fresh", target)
        else:
            _log.warning("No results directory for target '%s' — running fresh", target)

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

    _log.info("Found %d seed programs", len(seed_programs))

    best_seed, seed_metrics = _evaluate_seeds(
        evaluator,
        seed_programs,
        results_dir,
        eval_config=config.evaluation,
        max_parallel=config.evolution.parallel_evaluations,
        cached_seeds=cached_seeds,
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

    evaluator_path = _write_evaluator_script(results_dir, config)
    _log.info("Evaluator script: %s", evaluator_path)

    best_result = openevolve.run_evolution(
        initial_program=str(best_seed),
        evaluator=str(evaluator_path),
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
