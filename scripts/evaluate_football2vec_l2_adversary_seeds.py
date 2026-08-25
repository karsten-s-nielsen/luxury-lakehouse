# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.105-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "datasets>=3.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
#     "openevolve>=0.2.0",
# ]
# ///
"""EV2 Phase 1 orchestrator — harvest of 6 adversary seed programs + linear baseline.

Dispatches variants across the local pool (AI-PC + Media-PC + DGX Spark) via
``evolve.backends.pool.BackendPool`` + ``ThreadPoolExecutor`` — the same long-term
reusable pattern EV1 uses in ``src/evolve/targets/football2vec/config.yaml`` and
the ScoutGPT cycles use.

Dispatch target name: ``football2vec_stage2`` — a thin alias module at
``src/evolve/targets/football2vec_stage2/evaluator.py`` binds ``train_and_evaluate``
to the stage-2 entry point so ``BackendPool.train(target="football2vec_stage2", ...)``
works without modifying the backend abstractions.

Remote deployment: before dispatch, the orchestrator tar-pipes ``src/``, ``scripts/``,
and ``pyproject.toml`` to each remote workspace (``/home/<user>/evolve-workspace``).
The remote Python is invoked via ``RemoteSshBackend`` with
``python_path='PYTHONPATH=./src /path/to/venv/python'`` so the deployed branch source
shadows any installed wheel. Pattern established in session 53 cross-attention A/B
(see ``feedback_orchestration_lessons.md`` rules 6 and b).

Baseline (``linear``) runs first to capture ``L_0`` for downstream fitness computation;
the remaining 6 seeds are then dispatched concurrently across all healthy backends.

Per-variant ``metrics.json`` is uploaded to ``luxury-lakehouse/football2vec-l2-harvest``
as each variant completes (partial-crash survival). Combined ``results.json`` mirrors
to ``docs/evolve/ev2-football2vec-l2-adversarial/results.json`` at the end.

Usage:

    uv run python scripts/evaluate_football2vec_l2_adversary_seeds.py \\
        --stage1-sha <pinned sha> --dataset-sha <pinned sha>

    # Exclude a backend (e.g. DGX Spark offline):
    #   --hosts ai,media
    # Force single-machine sequential (debugging):
    #   --hosts ai --force-sequential
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
RESULTS_REPO = f"{HF_ORG}/football2vec-l2-harvest"
TRAINING_DATASET = f"{HF_ORG}/football2vec-training-data"
STAGE1_MODEL_REPO = f"{HF_ORG}/football2vec-v2"

# Evolve pin-drift discipline (§2.12): freeze dataset version during
# architecture experiments. Bump via: scripts/bump_evolve_pin.py
PINNED_DATASET_REPO: str = TRAINING_DATASET
PINNED_DATASET_SHA: str = "PLACEHOLDER_UNTIL_PHASE_9"
PINNED_REASON: str = "Post-SK3-MIG-B Phase 9; bump via scripts/bump_evolve_pin.py"
_TARGET = "football2vec_stage2"

# Pinned shared config — mirrors the Phase 1 spec section. Reproducibility
# anchors (stage1_commit_sha, dataset_sha) are injected at dispatch time.
_SHARED_CONFIG: dict[str, Any] = {
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
    "adversary_architecture": "linear",
    "lambda_schedule_shape": "linear",
    "lambda_max": 0.2,
    "lambda_warmup_epochs": 5,
    "dataset": TRAINING_DATASET,
    "stage1_model_repo": STAGE1_MODEL_REPO,
}

_EPOCHS = 30
_SEED = 42
_FITNESS_W_MLM = 0.4
_FITNESS_W_DEBIAS = 0.6

# (variant_name, seed_program_filename_or_None)
_VARIANTS: list[tuple[str, str | None]] = [
    ("linear", None),
    ("deep_mlp_2layer", "deep_mlp_2layer.py"),
    ("deep_mlp_3layer", "deep_mlp_3layer.py"),
    ("cross_attention_adversary", "cross_attention_adversary.py"),
    ("attention_pool_head", "attention_pool_head.py"),
    ("residual_mlp", "residual_mlp.py"),
    ("dual_head_ensemble", "dual_head_ensemble.py"),
]

# Remote host config. The venv_python paths point at each remote's torch-enabled
# environment (Media-PC: torch 2.12 nightly cu128 for Blackwell sm_120; DGX Spark:
# torch 2.11 cu130 for GB10). Remote workspaces are dedicated to EV2 and do not
# collide with any EV1 / ScoutGPT workspace on the same machine.
#
# ``timeout_seconds`` is the per-variant training budget for that backend, tuned
# to the machine's measured epoch speed times the expected 16-epoch early-stop
# cap, with a 2x safety factor. Phase 1b hit the global 900s default on DGX
# Spark (904s elapsed before kill) — per-backend tuning prevents that class of
# false-positive kill while still failing fast on true hangs.
_REMOTE_HOSTS: dict[str, dict[str, Any]] = {
    "media": {
        "host": "super@192.168.68.70",
        "remote_dir": "/home/super/evolve-workspace",
        "venv_python": "/home/super/venv-fourier/bin/python",
        "timeout_seconds": 10800,  # 3h: 5070 Ti at ~5.5 min/epoch x 16 = 88 min x 2 safety
    },
    "spark": {
        "host": "karsten@192.168.68.73",
        "remote_dir": "/home/karsten/evolve-workspace",
        "venv_python": "/home/karsten/Development/evolve-env/bin/python",
        "timeout_seconds": 21600,  # 6h: GB10 at ~11 min/epoch x 16 = 176 min x 2 safety
    },
}


def _seed_program_path(rel: str) -> str:
    """Resolve a seed file path relative to the wheel-bundled package."""
    import evolve.targets.football2vec.seed_programs_stage2 as pkg

    pkg_dir = Path(pkg.__file__).parent
    path = pkg_dir / rel
    if not path.exists():
        msg = f"seed program not found: {path}"
        raise FileNotFoundError(msg)
    return str(path)


def _deploy_to_remote(host: str, remote_dir: str, timeout: int = 120) -> None:
    """Tar-pipe local src/, scripts/, pyproject.toml to the remote workspace.

    Uses the session-53-established pattern from ``feedback_orchestration_lessons.md``
    rule 6: tight tar-pipe scope (avoids pulling terraform provider caches that
    would fill WSL disks). Deployment is idempotent — extract overwrites existing
    files in the remote workspace.

    Raises ``subprocess.CalledProcessError`` on any deploy failure so the
    orchestrator can skip an unhealthy backend before dispatching candidates.
    """
    logger.info("Deploying source to %s:%s", host, remote_dir)
    # Ensure remote workspace exists before streaming the tarball.
    mkdir_cmd = ["ssh", "-o", "ConnectTimeout=10", host, f"mkdir -p {remote_dir}"]
    subprocess.run(mkdir_cmd, check=True, timeout=30)  # noqa: S603

    # Pipe tar from stdout to ssh into tar on remote. `tar -C remote_dir xzf -` extracts
    # in the remote workspace. Scope is intentionally narrow per rule 6.
    tar_cmd = ["tar", "czf", "-", "src", "scripts", "pyproject.toml"]
    ssh_cmd = ["ssh", "-o", "ConnectTimeout=10", host, f"cd {remote_dir} && tar xzf -"]
    tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE)  # noqa: S603
    try:
        ssh_proc = subprocess.Popen(ssh_cmd, stdin=tar_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
        if tar_proc.stdout is None:
            msg = "tar subprocess failed to open stdout pipe"
            raise RuntimeError(msg)
        tar_proc.stdout.close()  # allow SIGPIPE on tar if ssh exits
        stdout, stderr = ssh_proc.communicate(timeout=timeout)
        tar_proc.wait(timeout=10)
        if ssh_proc.returncode != 0:
            raise subprocess.CalledProcessError(ssh_proc.returncode, ssh_cmd, output=stdout, stderr=stderr)
    finally:
        if tar_proc.poll() is None:
            tar_proc.terminate()
    logger.info("Deploy complete: %s", host)


# Every module the stage-2 evaluator transitively imports when running on a
# remote worker. The smoke test imports this exact list so a misprovisioned
# venv fails loudly BEFORE any candidate is dispatched (rather than crashing
# mid-training like the initial EV2 Phase 1 attempt did with safetensors).
# Note: mlflow is NOT needed — scripts/train_football2vec_v2.py imports it lazily
# inside _log_mlflow, which the evaluator never calls.
_REMOTE_REQUIRED_IMPORTS: tuple[str, ...] = (
    "torch",
    "safetensors",
    "huggingface_hub",
    "datasets",
    "pandas",
    "numpy",
    "pyarrow",
    "sklearn.model_selection",
    "scipy",
    # evolve.evaluator imports openevolve.evaluation_result at module load —
    # a venv without openevolve passes the original smoke test but crashes
    # ~2s into dispatch with ModuleNotFoundError. Phase 1b Media-PC lost 3
    # variants to this; guarding pre-dispatch saves ~6 seconds of dispatch
    # per missing-module failure and produces a precise install hint.
    "openevolve",
)


def _smoke_test_remote(host: str, venv_python: str) -> tuple[bool, str]:
    """Verify the remote venv has torch+CUDA, every module the stage-2
    evaluator imports, AND a working HF Hub auth credential. Returns
    (ok, detail). On failure, detail names the first failing check
    (missing module / CUDA / HF auth) so the caller can log a precise
    reason and the operator can fix exactly what's broken.
    """
    import_probe = "; ".join(f"import {m}" for m in _REMOTE_REQUIRED_IMPORTS)
    # HF Hub auth check. Phase 1c (2026-04-23) uncovered that a remote venv
    # can pass every import check yet crash at dispatch the moment the
    # evaluator tries to download Stage-1 weights, because non-interactive
    # SSH sessions inherit a different environment than interactive ones and
    # HF_TOKEN may be absent or expired. huggingface_hub then passes an empty
    # (or stale) token to httpx, which rejects "Bearer " as an illegal header
    # value or returns 401. whoami() exercises the exact token-resolution path
    # (env var -> ~/.cache/huggingface/token -> ~/.huggingface/token) that the
    # evaluator hits, and returns a user name on success. An empty name means
    # auth failed.
    #
    # IMPORTANT: all Python string literals below use DOUBLE quotes, never
    # single quotes. The full probe is wrapped in SINGLE quotes on the remote
    # bash side (``python -c '<probe>'``); any ' inside the probe would close
    # the outer quote prematurely and break shell parsing. Phase 1d (2026-04-23)
    # verified this: single quotes inside the probe made both Media-PC and DGX
    # Spark skip at smoke test with a "bash: -c: line 1: ..." parse error.
    # Double quotes inside the probe pass through literally by the outer
    # single-quote bash parse and Python accepts them as string delimiters.
    # Non-ASCII characters (em-dash etc.) are also avoided for locale safety.
    hf_auth_probe = (
        "from huggingface_hub import HfApi; "
        '_name = HfApi().whoami().get("name", ""); '
        'assert _name, "hf_auth: HfApi().whoami() returned no user name or invalid token"'
    )
    # Check order: imports -> CUDA -> HF auth. Missing modules are reported first
    # because they are the cheapest to fix (pip install); CUDA second because
    # it indicates hardware misconfig; HF auth last because it requires token
    # provisioning. The CUDA assert message uses double quotes for the same
    # quoting reason as hf_auth_probe above.
    probe = f'{import_probe}; import torch; assert torch.cuda.is_available(), "cuda unavailable"; {hf_auth_probe}'
    cmd = [
        "ssh",
        "-o",
        "ConnectTimeout=10",
        host,
        f"{venv_python} -c '{probe}' 2>&1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)  # noqa: S603
    except subprocess.TimeoutExpired:
        return False, "smoke test timed out (SSH or Python unresponsive)"
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout).strip()
        # Extract the first ModuleNotFoundError / AssertionError message if present.
        first_issue = stderr.split("\n")[-1][:200]
        return False, first_issue
    return True, "ok"


def _verify_remote_entrypoint(host: str, venv_python: str, remote_dir: str) -> tuple[bool, str]:
    """Post-deploy check that exercises the exact import chain the dispatched
    worker runs: ``evolve.evaluator`` + ``evolve.remote_worker`` with
    ``PYTHONPATH=./src``. Catches problems the venv-only smoke test misses —
    stale ``evolve/__init__.py`` referencing a removed module, source-vs-wheel
    conflicts, or new transitive deps in the deployed branch that the remote
    venv does not carry yet. Must run AFTER ``_deploy_to_remote`` so ``./src``
    is populated on the remote.
    """
    probe = "from evolve.evaluator import EvolveEvaluator; from evolve.remote_worker import main"
    cmd = [
        "ssh",
        "-o",
        "ConnectTimeout=10",
        host,
        f"cd {remote_dir} && env PYTHONPATH=./src {venv_python} -c '{probe}' 2>&1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)  # noqa: S603
    except subprocess.TimeoutExpired:
        return False, "entrypoint verification timed out (SSH or Python unresponsive)"
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout).strip()
        first_issue = stderr.split("\n")[-1][:300]
        return False, first_issue
    return True, "ok"


def _build_pool(selected_hosts: set[str]) -> Any:
    """Build a BackendPool containing every selected host that passes smoke-test.

    Returns a ``BackendPool`` instance. Raises ``RuntimeError`` if no backends are
    healthy — the orchestrator aborts rather than silently degrading.
    """
    from evolve.backends.local_cuda import LocalCudaBackend
    from evolve.backends.pool import BackendPool
    from evolve.backends.remote_ssh import RemoteSSHBackend

    backends: list[Any] = []
    if "ai" in selected_hosts:
        backends.append(LocalCudaBackend(device="cuda:0"))
        logger.info("Pool: adding LocalCudaBackend (AI-PC, cuda:0)")

    for alias in ("media", "spark"):
        if alias not in selected_hosts:
            continue
        cfg = _REMOTE_HOSTS[alias]
        host = cfg["host"]
        venv_python = cfg["venv_python"]
        remote_dir = cfg["remote_dir"]
        timeout_seconds = int(cfg["timeout_seconds"])
        ok, detail = _smoke_test_remote(host, venv_python)
        if not ok:
            logger.warning(
                "Skipping %s (%s) — smoke test failed: %s. "
                "Install missing packages on the remote venv (%s) and re-run. "
                "Expected modules: %s",
                alias,
                host,
                detail,
                venv_python,
                ", ".join(_REMOTE_REQUIRED_IMPORTS),
            )
            continue
        # Deploy source before the backend is allowed into the pool.
        _deploy_to_remote(host, remote_dir)
        # Post-deploy entrypoint verification — catches issues the venv-only smoke
        # test cannot (e.g. a sibling module referenced by evolve.__init__ that
        # was renamed in a recent commit, or a new openevolve-version API change).
        ok, detail = _verify_remote_entrypoint(host, venv_python, remote_dir)
        if not ok:
            logger.warning(
                "Skipping %s (%s) — entrypoint verification failed post-deploy: %s. "
                "This is a branch-specific failure (smoke test passed but deployed "
                "source cannot import). Inspect src/evolve/__init__.py and its "
                "transitive imports for new deps not in the remote venv.",
                alias,
                host,
                detail,
            )
            continue
        # PYTHONPATH=./src ensures the deployed branch source takes precedence over
        # any installed wheel. We MUST prefix with `env` because RemoteSSHBackend
        # wraps python_path with `stdbuf -oL -eL` which execs its first argument
        # directly (no shell) — passing `PYTHONPATH=./src` as that argument would
        # try to execute a file literally named "PYTHONPATH=./src". The `env`
        # executable properly interprets the assignment before execing python.
        # Pattern matches the session 53 A/B workflow
        # (feedback_orchestration_lessons.md rule b).
        wrapped_python = f"env PYTHONPATH=./src {venv_python}"
        user, host_only = host.split("@", 1)
        backends.append(
            RemoteSSHBackend(
                host=host_only,
                user=user,
                remote_dir=remote_dir,
                python_path=wrapped_python,
                device="cuda:0",
                timeout=timeout_seconds,
            )
        )
        logger.info("Pool: adding RemoteSSHBackend(%s, timeout=%ds)", host, timeout_seconds)

    if not backends:
        msg = "no healthy backends — aborting dispatch"
        raise RuntimeError(msg)

    return BackendPool(backends)


def _make_candidate_config(
    stage1_sha: str,
    dataset_sha: str,
    l_0_reference: float | None,
) -> dict[str, Any]:
    cfg = dict(_SHARED_CONFIG)
    cfg["stage1_commit_sha"] = stage1_sha
    cfg["_dataset_sha_pinned"] = dataset_sha  # recorded for provenance
    if l_0_reference is not None:
        cfg["L_0_reference"] = l_0_reference
    return cfg


def _run_variant_via_pool(
    pool: Any,
    variant: str,
    program_rel: str | None,
    stage1_sha: str,
    dataset_sha: str,
    l_0_reference: float | None,
) -> dict[str, Any]:
    """Dispatch one variant through BackendPool (any free backend runs it)."""
    candidate_config = _make_candidate_config(stage1_sha, dataset_sha, l_0_reference)
    program_path = _seed_program_path(program_rel) if program_rel is not None else None
    logger.info("=== Dispatching variant=%s program=%s ===", variant, program_rel)
    t0 = time.monotonic()
    metrics = pool.train(
        candidate_config=candidate_config,
        target=_TARGET,
        epochs=_EPOCHS,
        seed=_SEED,
        program_path=program_path,
    )
    elapsed = time.monotonic() - t0
    metrics = dict(metrics)
    metrics["variant"] = variant
    metrics["program_path"] = program_rel or "<baseline>"
    metrics["wall_clock_seconds"] = elapsed
    logger.info(
        "variant=%s val_mlm=%.4f val_adv_acc=%.4f debias=%.3f mlm=%.3f fitness=%.3f elapsed=%.1fs",
        variant,
        metrics.get("val_mlm_loss", float("inf")),
        metrics.get("val_adv_accuracy", 0.0),
        metrics.get("debias_score", 0.0),
        metrics.get("mlm_score", 0.0),
        metrics.get("fitness", 0.0),
        elapsed,
    )
    return metrics


def _upload_json(api: Any, hf_token: str, obj: Any, path_in_repo: str) -> None:
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage1-sha",
        required=True,
        help="HF Hub commit SHA of luxury-lakehouse/football2vec-v2 to pin",
    )
    parser.add_argument(
        "--dataset-sha",
        default=PINNED_DATASET_SHA,
        help="HF Hub commit SHA of luxury-lakehouse/football2vec-training-data to pin (default: PINNED_DATASET_SHA)",
    )
    parser.add_argument(
        "--hosts",
        default="ai,media,spark",
        help="Comma-separated subset of {ai,media,spark}. Default: all three.",
    )
    parser.add_argument(
        "--force-sequential",
        action="store_true",
        help="Disable ThreadPoolExecutor (run variants one at a time). Debugging only.",
    )
    parser.add_argument(
        "--l0-override",
        type=float,
        default=None,
        help=(
            "Skip the baseline (linear) run and use the provided L_0 value as the "
            "reference MLM loss for fitness calculation. Used to resume a harvest "
            "after a partial failure without re-running the 1.5-2 hour baseline."
        ),
    )
    parser.add_argument(
        "--variants",
        default=None,
        help=(
            "Comma-separated subset of variant names to run (excluding 'linear' which "
            "is controlled by --l0-override). Default: all 6 seeds. "
            "Valid names: deep_mlp_2layer, deep_mlp_3layer, cross_attention_adversary, "
            "attention_pool_head, residual_mlp, dual_head_ensemble."
        ),
    )
    args = parser.parse_args()

    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        msg = "HF_TOKEN required"
        raise RuntimeError(msg)

    # Lazy imports: torch + HF + evolve (which pulls openevolve) only when main runs.
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(RESULTS_REPO, exist_ok=True, repo_type="model", token=hf_token)

    selected_hosts = set(args.hosts.split(","))
    logger.info(
        "Starting harvest — stage1_sha=%s dataset_sha=%s hosts=%s force_sequential=%s",
        args.stage1_sha[:8] if args.stage1_sha else "<unset>",
        args.dataset_sha[:8] if args.dataset_sha else "<unset>",
        sorted(selected_hosts),
        args.force_sequential,
    )

    pool = _build_pool(selected_hosts)

    results: list[dict[str, Any]] = []

    # Phase 1a — baseline FIRST (unless --l0-override skips it), sequentially, so L_0
    # is known before the remaining dispatch.
    if args.l0_override is not None:
        l_0 = float(args.l0_override)
        logger.info("Using --l0-override %.4f — skipping baseline 'linear' run", l_0)
    else:
        baseline_metrics = _run_variant_via_pool(
            pool, "linear", None, args.stage1_sha, args.dataset_sha, l_0_reference=None
        )
        l_0 = baseline_metrics.get("val_mlm_loss", float("inf"))
        baseline_metrics["mlm_score"] = 1.0
        debias = baseline_metrics.get("debias_score", 0.0)
        baseline_metrics["fitness"] = _FITNESS_W_MLM * 1.0 + _FITNESS_W_DEBIAS * debias
        _upload_json(api, hf_token, baseline_metrics, "linear/metrics.json")
        logger.info("Baseline complete: L_0 = %.4f, fitness = %.4f", l_0, baseline_metrics["fitness"])
        results.append(baseline_metrics)

    # Phase 1b — remaining seeds dispatched concurrently across backends via the pool.
    # --variants filter narrows the set (used to re-run the subset that failed in a
    # prior partial harvest without wasting compute on successful ones).
    all_seeds = [(name, rel) for name, rel in _VARIANTS if name != "linear"]
    if args.variants is not None:
        selected_variants = {v.strip() for v in args.variants.split(",") if v.strip()}
        unknown = selected_variants - {name for name, _ in all_seeds}
        if unknown:
            msg = f"--variants has unknown names: {sorted(unknown)}. Valid: {[n for n, _ in all_seeds]}"
            raise ValueError(msg)
        remaining = [(name, rel) for name, rel in all_seeds if name in selected_variants]
        logger.info(
            "--variants filter: running %d / %d seeds: %s",
            len(remaining),
            len(all_seeds),
            [n for n, _ in remaining],
        )
    else:
        remaining = all_seeds

    if args.force_sequential:
        for name, rel in remaining:
            try:
                metrics = _run_variant_via_pool(pool, name, rel, args.stage1_sha, args.dataset_sha, l_0)
            except Exception as exc:
                # Per-variant failure is isolated — upload the error row and keep going.
                logger.exception("Variant %s failed", name)
                metrics = {"variant": name, "program_path": rel, "error": str(exc), "fitness": 0.0}
            results.append(metrics)
            _upload_json(api, hf_token, metrics, f"{name}/metrics.json")
    else:
        # Max concurrency = number of pool backends (BackendPool serializes internally on a queue;
        # using more workers than backends would just queue on .get()).
        max_workers = len(pool._backends)
        logger.info("Dispatching %d remaining variants with max_workers=%d", len(remaining), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures: dict[Future[dict[str, Any]], tuple[str, str | None]] = {
                ex.submit(_run_variant_via_pool, pool, name, rel, args.stage1_sha, args.dataset_sha, l_0): (name, rel)
                for name, rel in remaining
            }
            for fut in futures:
                name, rel = futures[fut]
                try:
                    metrics = fut.result()
                except Exception as exc:
                    # Per-variant failure is isolated — upload the error row and keep going.
                    logger.exception("Variant %s failed", name)
                    metrics = {"variant": name, "program_path": rel, "error": str(exc), "fitness": 0.0}
                results.append(metrics)
                _upload_json(api, hf_token, metrics, f"{name}/metrics.json")

    results_sorted = sorted(results, key=lambda r: -r.get("fitness", 0.0))
    combined = {
        "dataset": TRAINING_DATASET,
        "dataset_sha_pinned": args.dataset_sha,
        "stage1_model_repo": STAGE1_MODEL_REPO,
        "stage1_sha_pinned": args.stage1_sha,
        "shared_config": _SHARED_CONFIG,
        "epochs": _EPOCHS,
        "seed": _SEED,
        "fitness_formula": f"{_FITNESS_W_MLM} * mlm_score + {_FITNESS_W_DEBIAS} * debias_score",
        "L_0": l_0,
        "pool_backends": [type(b).__name__ for b in pool._backends],
        "variants": results_sorted,
    }
    _upload_json(api, hf_token, combined, "results.json")
    mirror_path = Path("docs/evolve/ev2-football2vec-l2-adversarial/results.json")
    mirror_path.parent.mkdir(parents=True, exist_ok=True)
    mirror_path.write_text(json.dumps(combined, indent=2, default=str), encoding="utf-8")

    logger.info("Harvest complete — %d variants evaluated", len(results))
    if any("error" in r for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
