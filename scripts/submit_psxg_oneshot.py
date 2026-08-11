"""Submit a one-shot ``compute_psxg_tracking`` run on Databricks serverless.

The tracking-PSxG writer (``ingestion.compute_psxg_tracking``) reads on-target
tracking shots from gold ``fct_action_context`` (+ ``fct_action_values`` for the
goal label), scores them with the ``@Champion`` PSxG model loaded from a UC Volume,
fits an out-of-sample Platt recalibration, and writes ``bronze.psxg_tracking_predictions``
(ADR-013). It is a recompute task (run on a model change), not part of the daily DAG,
so this script submits it as a one-off ``jobs.submit`` calling the
``compute_psxg_tracking`` wheel entry point directly.

Runs AS the ingestion service principal by default so executor writes to the bronze
schema succeed (``--run-as-user`` falls back to the submitter identity).

Env vars: ``DATABRICKS_HOST``, ``DATABRICKS_TOKEN`` (SDK default auth chain).

Usage::

    uv run --extra sdk python scripts/submit_psxg_oneshot.py --model-version v2-ontarget
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from ingestion.databricks_auth import workspace_client

logger = logging.getLogger("psxg_oneshot")

# The ingestion SP — same identity the daily compute tasks run as, so executor
# writes to the bronze schema (and the model-weights Volume read) are authorized.
_INGESTION_SP_APPLICATION_ID = "008b207b-96a8-4d54-b185-a77479a55abe"

# Lean env: the wheel + scikit-learn (the Platt fit). numpy/pandas are on the
# serverless base; numpy is pinned to match the analytics env (avoids the base
# 1.23.5 downgrade). Pins mirror uv.lock.
_PSXG_DEPENDENCIES: tuple[str, ...] = (
    "scikit-learn==1.7.2",
    "numpy==1.26.4",
)

_DEFAULT_MODEL_PATH = "/Volumes/soccer_analytics/dev_gold/model_weights/psxg/psxg_model.json"
_POLL_SECONDS = 15


def _wheel_volume_path(catalog: str) -> str:
    """Deployed wheel path on the libs UC Volume (bronze schema, per catalog module)."""
    from shared.wheel import WHEEL_FILENAME

    return f"/Volumes/{catalog}/bronze/libs/{WHEEL_FILENAME}"


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Submit a one-shot compute_psxg_tracking serverless run.")
    p.add_argument("--catalog", default="soccer_analytics")
    p.add_argument("--gold-schema", default="dev_gold")
    p.add_argument("--bronze-schema", default="bronze")
    p.add_argument("--model-path", default=_DEFAULT_MODEL_PATH, help="UC Volume path to psxg_model.json")
    p.add_argument("--model-version", required=True, help="PSxG model version being scored (e.g. v2-ontarget)")
    p.add_argument("--wheel-path", default=None, help="Override the deployed wheel Volume path")
    p.add_argument("--timeout-seconds", type=int, default=1800, help="Databricks task timeout")
    p.add_argument("--timeout-min", type=int, default=40, help="Local poll timeout (minutes)")
    p.add_argument("--run-as-user", action="store_true", help="Run as the submitter instead of the ingestion SP")
    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    args = _build_arg_parser().parse_args()

    from databricks.sdk.service import compute, jobs

    w = workspace_client()
    wheel_path = args.wheel_path or _wheel_volume_path(args.catalog)
    run_as = None if args.run_as_user else jobs.JobRunAs(service_principal_name=_INGESTION_SP_APPLICATION_ID)
    identity = "submitter" if args.run_as_user else f"ingestion-SP({_INGESTION_SP_APPLICATION_ID})"

    wheel_params = [
        "--catalog",
        args.catalog,
        "--gold-schema",
        args.gold_schema,
        "--bronze-schema",
        args.bronze_schema,
        "--model-path",
        args.model_path,
        "--model-version",
        args.model_version,
    ]
    logger.info(
        "Submitting one-shot compute_psxg_tracking: model_version=%s model_path=%s run_as=%s wheel=%s",
        args.model_version,
        args.model_path,
        identity,
        wheel_path,
    )
    waiter = w.jobs.submit(
        run_name=f"psxg-oneshot-{args.model_version}",
        run_as=run_as,
        tasks=[
            jobs.SubmitTask(
                task_key="psxg_oneshot",
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="luxury_lakehouse",
                    entry_point="compute_psxg_tracking",
                    parameters=wheel_params,
                ),
                environment_key="psxg",
                timeout_seconds=args.timeout_seconds,
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="psxg",
                spec=compute.Environment(client="1", dependencies=[wheel_path, *_PSXG_DEPENDENCIES]),
            )
        ],
    )
    run_id = waiter.run_id
    logger.info("RUN_ID=%s  URL=%s/jobs/runs/%s", run_id, w.config.host, run_id)

    deadline = time.monotonic() + args.timeout_min * 60
    last = ""
    run = None
    while True:
        run = w.jobs.get_run(run_id=run_id)
        st = run.state
        lc = st.life_cycle_state.value if (st and st.life_cycle_state) else "?"
        rs = st.result_state.value if (st and st.result_state) else "-"
        cur = f"{lc}/{rs}"
        if cur != last:
            logger.info("state=%s", cur)
            last = cur
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        if time.monotonic() > deadline:
            logger.warning("Local poll timeout (%d min) — cancelling run %s", args.timeout_min, run_id)
            with __import__("contextlib").suppress(Exception):
                w.jobs.cancel_run(run_id=run_id)
            break
        time.sleep(_POLL_SECONDS)

    # Driver log tail (row counts + any error live here).
    task_run_id = (run.tasks[0].run_id if (run and run.tasks) else None) or run_id
    with __import__("contextlib").suppress(Exception):
        output = w.jobs.get_run_output(run_id=task_run_id)
        if output.logs:
            logger.info("---- driver log tail ----\n%s", "\n".join(output.logs.splitlines()[-40:]))

    result_state = run.state.result_state.value if (run and run.state and run.state.result_state) else "UNKNOWN"
    if result_state != "SUCCESS":
        sys.exit(f"compute_psxg_tracking run did not succeed: {result_state}")
    logger.info("compute_psxg_tracking one-shot SUCCESS (run_id=%s)", run_id)


if __name__ == "__main__":
    main()
