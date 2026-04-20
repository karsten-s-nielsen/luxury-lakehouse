"""RoPE-for-ScoutGPT A/B orchestration shim.

Submits two HF Jobs L40S runs (variant=learnable, variant=rope), pinned to the
same dataset SHA, waits for both, downloads metrics.json from each variant
sibling repo, and writes a combined SUMMARY.md.

Usage:
    uv run python scripts/run_rope_scoutgpt_ab.py \\
        [--dataset-sha <sha>] \\
        [--epochs 30] [--batch-size 256] [--lr 1e-4] [--patience 5]

Requires environment variables:
    HF_TOKEN
    MLFLOW_TRACKING_URI
    DATABRICKS_HOST
    DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, hf_hub_download

# `hf jobs uv run --detach` prints a human-friendly line like
# "View at: https://huggingface.co/jobs/<user>/<job_id>". The job id is the
# final path segment; extract it so `hf jobs ps` queries match.
_JOB_ID_URL_RE = re.compile(r"/jobs/[^/]+/([0-9a-f]+)\b")

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"
SUMMARY_DIR = Path("docs/evolve/rope-scoutgpt")
SUMMARY_PATH = SUMMARY_DIR / "SUMMARY.md"

VARIANTS = ("learnable", "rope")
# hf jobs ps / logs return these terminal states; poll until both runs land here.
_TERMINAL_STATES = {"COMPLETED", "SUCCEEDED", "FAILED", "CANCELED", "CANCELLED", "ERROR"}
_SUCCESS_STATES = {"COMPLETED", "SUCCEEDED"}


def _require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        msg = f"{name} environment variable is required"
        raise RuntimeError(msg)
    return value


def _resolve_dataset_sha(hf_token: str, explicit_sha: str | None) -> str:
    if explicit_sha:
        logger.info("Using explicit dataset SHA: %s", explicit_sha)
        return explicit_sha
    api = HfApi(token=hf_token)
    info = api.repo_info(repo_id=TRAINING_DATASET, repo_type="dataset")
    sha = info.sha or ""
    if not sha:
        msg = f"could not resolve current SHA for {TRAINING_DATASET}"
        raise RuntimeError(msg)
    logger.info("Resolved current dataset SHA: %s", sha)
    return sha


def _hf_cli() -> str:
    cli = shutil.which("hf")
    if not cli:
        msg = "`hf` CLI not found in PATH — install huggingface_hub[cli]"
        raise RuntimeError(msg)
    return cli


def _submit_job(variant: str, dataset_sha: str, args: argparse.Namespace) -> str:
    """Submit one HF Jobs L40S run; return the job id."""
    hf_cli = _hf_cli()

    cmd = [
        hf_cli,
        "jobs",
        "uv",
        "run",
        "--flavor",
        "l40sx1",
        "--timeout",
        "180m",
        "--secrets",
        f"HF_TOKEN={_require_env('HF_TOKEN')}",
        "--env",
        f"MLFLOW_TRACKING_URI={_require_env('MLFLOW_TRACKING_URI')}",
        "--env",
        f"DATABRICKS_HOST={_require_env('DATABRICKS_HOST')}",
        "--env",
        f"DATABRICKS_TOKEN={_require_env('DATABRICKS_TOKEN')}",
        "--env",
        f"DATASET_PINNED_SHA={dataset_sha}",
        "--detach",
        "scripts/train_scoutgpt_hf.py",
        "--",
        "--variant",
        variant,
        # `=` form required: `-variant-{name}` starts with `-`, argparse would
        # otherwise treat it as a flag and crash with "expected one argument".
        f"--output-repo-suffix=-variant-{variant}",
        "--epochs",
        str(args.epochs),
        "--batch-size",
        str(args.batch_size),
        "--lr",
        str(args.lr),
        "--patience",
        str(args.patience),
    ]
    logger.info("Submitting %s variant", variant)
    # Command list is entirely constructed from hard-coded args + env vars resolved
    # on this machine; no untrusted user input flows into argv. Shim is a developer-side
    # orchestration tool, not a surface that accepts network input.
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)  # noqa: S603
    combined = f"{proc.stdout}\n{proc.stderr}"
    match = _JOB_ID_URL_RE.search(combined)
    if not match:
        msg = f"could not extract job id from: stdout={proc.stdout!r} stderr={proc.stderr!r}"
        raise RuntimeError(msg)
    job_id = match.group(1)
    logger.info("Submitted %s variant — job id: %s", variant, job_id)
    return job_id


def _job_status(job_id: str) -> str:
    """Return the current status stage for a job id, via `hf jobs inspect`.

    `hf jobs ps` output can be truncated on large queues; `hf jobs inspect`
    is per-job and always returns the full row. The ``status`` field is a
    nested object ``{"stage": "RUNNING", "message": "..."}``; we return the
    uppercased ``stage`` string.
    """
    hf_cli = _hf_cli()
    # Hard-coded argv; see S603 justification on _submit_job.
    proc = subprocess.run(  # noqa: S603
        [hf_cli, "jobs", "inspect", job_id],
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        rows = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        msg = f"could not parse `hf jobs inspect {job_id}` output: {proc.stdout!r}"
        raise RuntimeError(msg) from exc
    if not rows:
        return "UNKNOWN"
    row = rows[0]
    status_field = row.get("status") or {}
    if isinstance(status_field, dict):
        return str(status_field.get("stage") or "UNKNOWN").upper()
    # Fallback for older hf CLI versions that returned a flat string.
    return str(status_field).upper()


def _wait_for_completion(job_ids: dict[str, str], poll_seconds: int = 60) -> dict[str, str]:
    """Block until every job reaches a terminal state; return final statuses."""
    statuses: dict[str, str] = {v: "PENDING" for v in job_ids}
    while True:
        for variant, job_id in job_ids.items():
            if statuses[variant] not in _TERMINAL_STATES:
                statuses[variant] = _job_status(job_id)
        logger.info("Statuses: %s", statuses)
        if all(s in _TERMINAL_STATES for s in statuses.values()):
            return statuses
        time.sleep(poll_seconds)


def _download_metrics(variant: str, hf_token: str) -> dict[str, Any]:
    repo_id = f"{HF_ORG}/scoutgpt-variant-{variant}"
    with tempfile.TemporaryDirectory() as td:
        local_path = hf_hub_download(
            repo_id=repo_id,
            filename="metrics.json",
            repo_type="model",
            token=hf_token,
            local_dir=td,
        )
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)


def _fmt_row(variant: str, m: dict[str, Any]) -> str:
    if not m:
        return f"| {variant} | — | — | — | — | — | — |"
    return (
        f"| {variant} | {m.get('test_top1_accuracy', 0.0):.4f} | "
        f"{m.get('test_top5_accuracy', 0.0):.4f} | "
        f"{m.get('mean_spearman_rho', 0.0):.4f} | "
        f"{m.get('cross_source_gap', 0.0):.4f} | "
        f"{m.get('actual_epochs', '?')} | "
        f"${m.get('estimated_cost_usd', 0.0):.2f} |"
    )


def _format_summary(
    dataset_sha: str,
    job_ids: dict[str, str],
    statuses: dict[str, str],
    metrics: dict[str, dict[str, Any]],
) -> str:
    rows = "\n".join(_fmt_row(v, metrics.get(v, {})) for v in VARIANTS)
    bucket_rows = "\n".join(
        f"| {v} | "
        + " | ".join(f"{metrics.get(v, {}).get(f'test_top1_accuracy_{q}', 0.0):.4f}" for q in ("q1", "q2", "q3", "q4"))
        + " |"
        for v in VARIANTS
    )
    baseline_rows = "\n".join(
        f"| {v} | {metrics.get(v, {}).get('baseline_most_frequent_accuracy', 0.0):.4f}"
        f" | {metrics.get(v, {}).get('baseline_bigram_accuracy', 0.0):.4f} |"
        for v in VARIANTS
    )
    job_lines = "".join(f"- {variant}: `{job_ids[variant]}` — {statuses[variant]}\n" for variant in VARIANTS)

    return (
        "# RoPE-for-ScoutGPT — A/B Summary\n\n"
        f"**Dataset SHA (pinned):** `{dataset_sha}`\n\n"
        "**HF Jobs:**\n"
        f"{job_lines}\n"
        "## Headline metrics\n\n"
        "| Variant | test_top1 | test_top5 | counterfactual_rho | cross_source_gap | epochs | cost |\n"
        "|---|---:|---:|---:|---:|---:|---:|\n"
        f"{rows}\n\n"
        "## Bucket accuracy by episode length\n\n"
        "| Variant | q1 | q2 | q3 | q4 |\n|---|---:|---:|---:|---:|\n"
        f"{bucket_rows}\n\n"
        "## Baselines (variant-invariant; sanity check)\n\n"
        "| Variant | most_frequent | bigram |\n|---|---:|---:|\n"
        f"{baseline_rows}\n\n"
        "## Recommendation\n\n"
        "_Filled in by the human reviewer after reading the metrics above._\n\n"
        "- Promote rope → separate approval-gated cycle\n"
        "- Defer (inconclusive) → schedule re-run pair at +$6\n"
        "- Reject rope (learnable wins or ties with lower complexity) → close cycle\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="RoPE-for-ScoutGPT A/B orchestration")
    parser.add_argument("--dataset-sha", default=None, help="Pin to this dataset SHA (else resolve current HEAD).")
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--patience", type=int, default=5)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()

    hf_token = _require_env("HF_TOKEN")
    _require_env("MLFLOW_TRACKING_URI")
    _require_env("DATABRICKS_HOST")
    _require_env("DATABRICKS_TOKEN")

    dataset_sha = _resolve_dataset_sha(hf_token, args.dataset_sha)

    job_ids = {v: _submit_job(v, dataset_sha, args) for v in VARIANTS}

    statuses = _wait_for_completion(job_ids, poll_seconds=args.poll_seconds)
    logger.info("All jobs reached terminal state: %s", statuses)

    metrics: dict[str, dict[str, Any]] = {}
    for variant in VARIANTS:
        if statuses[variant] in _SUCCESS_STATES:
            try:
                metrics[variant] = _download_metrics(variant, hf_token)
                logger.info("Downloaded metrics for %s", variant)
            except Exception as exc:  # noqa: BLE001 — shim, failure surfaced in SUMMARY as missing row
                logger.error("Could not download metrics for %s: %s", variant, exc)
                metrics[variant] = {}
        else:
            logger.warning("Variant %s did not succeed (%s) — skipping metrics", variant, statuses[variant])
            metrics[variant] = {}

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(
        _format_summary(dataset_sha, job_ids, statuses, metrics),
        encoding="utf-8",
    )
    logger.info("SUMMARY written to %s", SUMMARY_PATH)

    # Exit non-zero if any variant failed, so calling shells can detect.
    if any(s not in _SUCCESS_STATES for s in statuses.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
