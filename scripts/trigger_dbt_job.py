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
                # EXACTLY ONE of client / environment_version. Sending both worked from
                # 2026-04-26 until 2026-06-09; the 2026-06-10 Databricks rollout rejects the
                # pair at runs/submit with INVALID_PARAMETER_VALUE ("Only one of them must be
                # provided for serverless environments") — broke the daily dbt-live-ci.
                # environment_version is the current canonical field; client is its legacy alias.
                #
                # `dependencies` DECLARES dbt on the environment instead of pip-installing it
                # at runtime (ADR-046 lockstep, 2026-07-27). The old runtime install used a
                # RANGE, so the job resolved dbt 1.11.8 while the runner produced the manifest
                # with uv.lock's 1.11.12 — dbt rejected the newer WritableManifest and exited 2
                # nightly. Exact `==`, kept equal to uv.lock by
                # src/tests/test_ci_dbt_pin_parity.py.
                "spec": {
                    "environment_version": "2",
                    "dependencies": [
                        "dbt-core==1.11.12",
                        "dbt-databricks==1.12.2",
                    ],
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
    resp = requests.post(
        # nosemgrep: python.lang.security.audit.insecure-transport.requests.request-with-http.request-with-http
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
    """Poll until terminal state or max_attempts.

    Uses WorkspaceClient.jobs.get_run for the poll loop — the SDK handles
    OIDC token rotation transparently. Pre-PR-6-followup this used direct
    requests.get with a static Authorization header, which 403s after ~5
    min when the GitHub OIDC token expires (verified 2026-04-27 on PR
    #207's first live-build run: 19 polls succeeded, attempt 20 hit 403
    even though the underlying dbt job was still in flight and ultimately
    succeeded). The SDK's auth provider auto-refreshes tokens via the
    GitHub workload identity federation flow, so polls past the original
    OIDC TTL keep working.

    `host` and `token` are retained for backward compatibility with manual
    invocations (for ``DATABRICKS_AUTH_TYPE`` unset). When the OIDC env is
    detected (CI), the SDK constructor reads the federation config from
    env directly and ignores the static token.
    """
    import os

    from databricks.sdk import WorkspaceClient

    # Prefer ambient env (OIDC federation auto-refresh); fall back to
    # explicit (host, token) for manual invocations.
    if os.environ.get("DATABRICKS_AUTH_TYPE") == "github-oidc":
        ws = WorkspaceClient()
    else:
        ws_host = host.rstrip("/").removeprefix("https://").removeprefix("http://")
        ws = WorkspaceClient(host=f"https://{ws_host}", token=token)

    for attempt in range(max_attempts):
        run = ws.jobs.get_run(run_id=run_id)
        state = run.state
        life_enum = state.life_cycle_state if state else None
        result_enum = state.result_state if state else None
        # SDK enums expose .value (e.g. RunLifeCycleState.RUNNING.value == "RUNNING").
        life = life_enum.value if life_enum is not None else ""
        result = result_enum.value if result_enum is not None else None
        url = run.run_page_url or ""
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
