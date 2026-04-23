#!/usr/bin/env python3
"""GH Actions helper: submit + poll a Databricks one-shot dbt run (PR 4a).

Usage:
    python scripts/trigger_dbt_job.py \
        --pr-number 42 --commit-sha abc1234 \
        --tarball /tmp/dbt_project.tar.gz --manifest /tmp/manifest_main.json \
        --select-arg "state:modified+" \
        --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" \
        --volume-prefix "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234"

Emits a JSON object on stdout with final result, e.g.:
    {"run_id": 12345, "life_cycle_state": "TERMINATED", "result_state": "SUCCESS",
     "run_page_url": "https://...", "output_volume_path": "/Volumes/..."}

Exit code: 0 on SUCCESS, 1 on FAILED/CANCELED/INTERNAL_ERROR.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_SHIM_VOLUME_PATH = "/Workspace/Shared/luxury-lakehouse-ci/run_dbt_in_databricks.py"
_POLL_INTERVAL_S = 15
_MAX_POLL_ATTEMPTS = 120  # 30 min total at 15s cadence
_IN_FLIGHT = frozenset({"PENDING", "RUNNING", "TERMINATING", "QUEUED"})


@dataclasses.dataclass(frozen=True)
class RunResult:
    life_cycle_state: str
    result_state: str | None
    run_page_url: str


def _workspace_client() -> WorkspaceClient:
    """Construct a Databricks WorkspaceClient from ambient environment.

    Resolution order is the SDK default: DATABRICKS_HOST + DATABRICKS_TOKEN
    env vars, then ~/.databrickscfg, then OIDC. In GH Actions the OIDC path
    is the intended one (DATABRICKS_CLIENT_ID + workload identity federation).
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def build_runs_submit_payload(
    *,
    pr_number: int,
    commit_sha: str,
    tarball_volume_path: str,
    manifest_volume_path: str,
    select_arg: str,
    output_volume_path: str,
) -> dict[str, Any]:
    """Build the /api/2.0/jobs/runs/submit payload.

    Uses serverless jobs compute with PERFORMANCE_OPTIMIZED target — the
    workspace rejects classic clusters ("Only serverless compute is supported").
    PERFORMANCE_OPTIMIZED keeps warm pools, so per-PR cold-start is near-zero.
    """
    return {
        "run_name": f"dbt-live-ci (PR #{pr_number}, {commit_sha})",
        "timeout_seconds": 1800,
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "client": "2",
                    "environment_version": "2",
                },
            }
        ],
        "tasks": [
            {
                "task_key": "dbt_build",
                "environment_key": "Default",
                "performance_target": "PERFORMANCE_OPTIMIZED",
                "spark_python_task": {
                    "python_file": _SHIM_VOLUME_PATH,
                    "parameters": [
                        "--tarball-path",
                        tarball_volume_path,
                        "--manifest-path",
                        manifest_volume_path,
                        "--select-arg",
                        select_arg,
                        "--output-path",
                        output_volume_path,
                    ],
                },
            }
        ],
    }


def upload_tarball(local_path: Path, volume_path: str) -> None:
    """Upload a local file to a UC Volume path via the Databricks Files API."""
    ws = _workspace_client()
    logger.info("Uploading %s (%d bytes) to %s", local_path, local_path.stat().st_size, volume_path)
    with local_path.open("rb") as f:
        ws.files.upload(volume_path, f, overwrite=True)


def submit_run(*, host: str, token: str, payload: dict[str, Any]) -> int:
    """POST to /api/2.0/jobs/runs/submit and return the new run_id."""
    host = host.rstrip("/").removeprefix("https://").removeprefix("http://")
    # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http -- scheme is hard-coded https:// literal; host scheme stripped above
    resp = requests.post(
        f"https://{host}/api/2.0/jobs/runs/submit",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=(10, 60),
        verify=True,
    )
    if resp.status_code >= 400:
        logger.error("runs/submit %d response body: %s", resp.status_code, resp.text[:1000])
    resp.raise_for_status()
    run_id = int(resp.json()["run_id"])
    logger.info("Submitted run_id=%d", run_id)
    return run_id


def poll_run(
    *,
    host: str,
    token: str,
    run_id: int,
    max_attempts: int = _MAX_POLL_ATTEMPTS,
    poll_interval_s: int = _POLL_INTERVAL_S,
) -> RunResult:
    """Poll /api/2.0/jobs/runs/get until terminal state or max_attempts."""
    host = host.rstrip("/").removeprefix("https://").removeprefix("http://")
    for attempt in range(max_attempts):
        # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http -- scheme is hard-coded https:// literal; host scheme stripped above
        resp = requests.get(
            f"https://{host}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=(10, 30),
            verify=True,
        )
        resp.raise_for_status()
        body = resp.json()
        life = body.get("state", {}).get("life_cycle_state", "")
        result = body.get("state", {}).get("result_state")
        url = body.get("run_page_url", "")
        logger.info(
            "run_id=%d attempt=%d life_cycle_state=%s result_state=%s",
            run_id,
            attempt,
            life,
            result,
        )
        if life not in _IN_FLIGHT:
            return RunResult(life_cycle_state=life, result_state=result, run_page_url=url)
        time.sleep(poll_interval_s)
    raise TimeoutError(f"run_id={run_id} did not reach terminal state after {max_attempts} polls")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Trigger a Databricks one-shot dbt run via OIDC.")
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--tarball", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--select-arg", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--volume-prefix", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    tarball_vp = f"{args.volume_prefix}/dbt_project.tar.gz"
    manifest_vp = f"{args.volume_prefix}/manifest_main.json"
    output_vp = f"{args.volume_prefix}/run_results.json"

    upload_tarball(args.tarball, tarball_vp)
    upload_tarball(args.manifest, manifest_vp)

    payload = build_runs_submit_payload(
        pr_number=args.pr_number,
        commit_sha=args.commit_sha,
        tarball_volume_path=tarball_vp,
        manifest_volume_path=manifest_vp,
        select_arg=args.select_arg,
        output_volume_path=output_vp,
    )

    run_id = submit_run(host=args.host, token=args.token, payload=payload)
    result = poll_run(host=args.host, token=args.token, run_id=run_id)

    out = {
        "run_id": run_id,
        "life_cycle_state": result.life_cycle_state,
        "result_state": result.result_state,
        "run_page_url": result.run_page_url,
        "output_volume_path": output_vp,
    }
    print(json.dumps(out))

    return 0 if result.result_state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
