"""Submit ONE compute_action_context iteration for a single match on serverless.

Bypasses the preflight -> for_each fan-out entirely: a one-off ``jobs.submit``
that calls the ``compute_action_context`` wheel entry point directly with a
single ``--match-ids "provider:id"`` (the production driver path
``_process_tracking_match`` runs unchanged on Databricks serverless).

Two jobs in one:
  1. Tests the single-game path for a given provider (start with metrica, then
     the other providers in increasing per-game complexity).
  2. Surfaces the OPEN serverless ``applyInPandas`` hang via the merged
     ``ingestion.exec_visibility`` instrumentation (ADR-032): the driver
     PhaseHeartbeat prints elapsed/phase/row-count to the task log, and the
     executor writes env-fingerprint / per-batch / _ERROR markers + a
     faulthandler stack dump.

After the run terminates (or times out), this prints:
  - the driver task log tail (PhaseHeartbeat + AC1_FINGERPRINT + any error), and
  - every executor rendezvous marker's content from the UC Volume
    ``_staging/ac1_progress/<provider>_<match>_p<period>/`` (env fingerprint =
    the leading hang-hypothesis signal: numba threading layer, fork/spawn,
    versions, internet reachability).

Env vars: DATABRICKS_HOST, DATABRICKS_TOKEN (SDK default auth chain).

Usage::

    # one metrica game (period filter is optional; metrica is not period-split)
    uv run python scripts/submit_ac1_oneshot.py --match-ids metrica:Sample_Game_2

    # other providers later, increasing per-game complexity:
    uv run python scripts/submit_ac1_oneshot.py --match-ids skillcorner:2011166
    uv run python scripts/submit_ac1_oneshot.py --match-ids idsse:J03WN1:1   # one half
    uv run python scripts/submit_ac1_oneshot.py --match-ids gradientsports:10502
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logger = logging.getLogger("ac1_oneshot")

# Mirror terraform/modules/workflows/main.tf "analytics" environment spec exactly
# so the one-shot executor env matches the daily job's. Keep in sync.
_ANALYTICS_DEPENDENCIES: tuple[str, ...] = (
    "silly-kicks>=4.22.0,<5",
    "accessible-space>=2.0,<3",
    # numba: JITs silly-kicks pitch-control + ball-carrier kernels (else silent
    # numpy fallback). Added to the TF analytics env in PR #325; mirrored here.
    "numba>=0.59.0",
    "numpy<2.0",
    "xgboost-cpu==3.2.0",
    "rapidfuzz>=3.6.0",
    "unidecode>=1.3.0",
    "sparse-dot-topn>=1.1.0",
    "mlflow-skinny>=2.19.0",
    "mplsoccer>=1.1.3",
    "matplotlib>=3.8.0",
    "scipy>=1.11.0",
)

_POLL_SECONDS = 20
_LOG_TAIL_CHARS = 6000
_TRACKING_PROVIDERS = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})

# Ingestion service principal (application_id). The one-shot runs AS this SP so
# the production driver path executes with the same identity as the daily job —
# critically, the SP holds READ/WRITE VOLUME on bronze._staging, so the executor
# rendezvous markers (env-fingerprint / per-batch / _ERROR + faulthandler) can
# actually be written. An interactive user PAT lacks that grant, so a run
# submitted as the user produces NO markers (the leading hang-diagnostic signal).
# Requires the submitter to be a workspace admin or hold CAN_USE on the SP.
# See project memory ``ac1-serverless-hang-open`` (identity caveat).
_INGESTION_SP_APPLICATION_ID = "008b207b-96a8-4d54-b185-a77479a55abe"


def _wheel_volume_path(catalog: str) -> str:
    """Deployed wheel path on the libs UC Volume (bronze schema, per catalog module)."""
    from shared.wheel import WHEEL_FILENAME

    return f"/Volumes/{catalog}/bronze/libs/{WHEEL_FILENAME}"


def _parse_match_arg(raw: str) -> tuple[str, str, int | None]:
    """Parse 'provider:id' or 'provider:id:period' -> (provider, id, period|None)."""
    parts = raw.split(":")
    provider = parts[0]
    if len(parts) == 3 and parts[2].strip().isdigit():
        return provider, parts[1].strip(), int(parts[2].strip())
    return provider, ":".join(parts[1:]).strip(), None


def _rendezvous_dir(catalog: str, schema: str, provider: str, match_id: str, period: int | None) -> str:
    """Mirror ingestion.action_context._process_tracking_match's rendezvous path."""
    return f"/Volumes/{catalog}/{schema}/_staging/ac1_progress/{provider}_{match_id}_p{period}"


def _dump_markers(w: object, rendezvous_dir: str) -> None:
    """List + print every executor rendezvous marker's content (the hang signal)."""
    print(f"\n----- EXECUTOR MARKERS: {rendezvous_dir} -----")
    try:
        entries = list(w.files.list_directory_contents(rendezvous_dir))  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001 — diagnostic; dir may not exist if driver never created it
        print(f"  <could not list markers: {exc}>")
        return
    if not entries:
        print("  <no markers — executor never wrote (FUSE-write failed?) or driver never reached UDF dispatch>")
        return
    for entry in sorted(entries, key=lambda e: getattr(e, "path", "")):
        path = getattr(entry, "path", "")
        print(f"\n  === {path.rsplit('/', 1)[-1]} ===")
        try:
            resp = w.files.download(path)  # type: ignore[attr-defined]
            content = resp.contents.read().decode("utf-8", errors="replace")  # type: ignore[attr-defined]
            print("  " + content.replace("\n", "\n  "))
        except Exception as exc:  # noqa: BLE001 — diagnostic
            print(f"  <download failed: {exc}>")


def _dump_driver_log(w: object, task_run_id: int) -> None:
    print(f"\n----- DRIVER TASK LOG (task_run={task_run_id}) -----")
    try:
        out = w.jobs.get_run_output(run_id=task_run_id)  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001 — diagnostic
        print(f"  <get_run_output failed: {exc}>")
        return
    if out.error:
        print(f"  ERROR: {out.error}")
    if out.error_trace:
        print(f"  ERROR_TRACE (tail):\n{out.error_trace[-_LOG_TAIL_CHARS:]}")
    if out.logs:
        print(f"  LOGS (tail, truncated={out.logs_truncated}):\n{out.logs[-_LOG_TAIL_CHARS:]}")
    if not (out.error or out.error_trace or out.logs):
        print("  <no error / no logs returned>")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Submit one compute_action_context iteration on serverless.")
    p.add_argument("--match-ids", required=True, help="provider:id  or  provider:id:period")
    p.add_argument("--catalog", default="soccer_analytics")
    p.add_argument("--schema", default="bronze")
    p.add_argument("--timeout-min", type=int, default=40, help="Max minutes to poll before giving up (LOCAL)")
    p.add_argument("--wheel-path", default=None, help="Override deployed wheel UC Volume path")
    p.add_argument(
        "--ghost-gk-backend",
        default=None,
        help="Ghost-GK KDE backend for compute_action_context (one of scipy,vectorized,cpu-numba,fft,fft-cic; "
        "default fft-cic). Exact backends are slow — pair with --timeout-seconds. Ignored with --profile.",
    )
    p.add_argument(
        "--timeout-seconds",
        type=int,
        default=7200,
        help="REMOTE Databricks task timeout (jobs.SubmitTask.timeout_seconds). The for-each/oneshot path has "
        "no in-process watchdog, so this is the escape hatch for a slow exact-backend run.",
    )
    p.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Run the profile_action_context entry point instead of compute_action_context: "
            "single-process cProfile of the enrichment on the driver (no bronze write). The "
            "cumulative-time breakdown is written as a 'cprofile_summary' marker and printed "
            "below with the other rendezvous markers. Tracking providers only, one match."
        ),
    )
    p.add_argument(
        "--max-batches",
        type=int,
        default=60,
        help="With --profile: profile only the first N frame batches (representative "
        "sample; 0 = whole match, high fidelity but serial-slow). Ignored without --profile.",
    )
    p.add_argument(
        "--run-as-user",
        action="store_true",
        help=(
            "Run as the submitting user instead of the ingestion SP. NOTE: the user PAT "
            "lacks bronze._staging WRITE VOLUME, so executor markers will NOT be written "
            "(read the faulthandler dump via the Spark UI thread dump instead)."
        ),
    )
    args = p.parse_args()

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import compute, jobs

    provider, match_id, period = _parse_match_arg(args.match_ids)
    wheel_path = args.wheel_path or _wheel_volume_path(args.catalog)
    w = WorkspaceClient()

    # Run AS the ingestion SP by default so executor FUSE writes to the
    # bronze._staging rendezvous dir succeed (markers are the hang signal). The
    # --run-as-user escape hatch falls back to the submitter identity.
    run_as = None if args.run_as_user else jobs.JobRunAs(service_principal_name=_INGESTION_SP_APPLICATION_ID)
    identity = "submitter" if args.run_as_user else f"ingestion-SP({_INGESTION_SP_APPLICATION_ID})"

    logger.info(
        "Submitting one-shot: provider=%s match=%s period=%s run_as=%s wheel=%s",
        provider,
        match_id,
        period,
        identity,
        wheel_path,
    )
    entry_point = "profile_action_context" if args.profile else "compute_action_context"
    wheel_params = ["--catalog", args.catalog, "--schema", args.schema, "--match-ids", args.match_ids]
    if args.profile:
        wheel_params += ["--max-batches", str(args.max_batches)]
    elif args.ghost_gk_backend:
        # compute_action_context accepts --ghost-gk-backend; profile_action_context does not.
        wheel_params += ["--ghost-gk-backend", args.ghost_gk_backend]
    waiter = w.jobs.submit(
        run_name=f"ac1-{'profile' if args.profile else 'oneshot'}-{provider}-{match_id}",
        run_as=run_as,
        tasks=[
            jobs.SubmitTask(
                task_key="ac1_oneshot",
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="luxury_lakehouse",
                    entry_point=entry_point,
                    parameters=wheel_params,
                ),
                environment_key="analytics",
                timeout_seconds=args.timeout_seconds,
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="analytics",
                spec=compute.Environment(client="1", dependencies=[wheel_path, *_ANALYTICS_DEPENDENCIES]),
            )
        ],
    )
    run_id = waiter.run_id
    logger.info("RUN_ID=%s  URL=%s/jobs/runs/%s", run_id, w.config.host, run_id)

    deadline = time.monotonic() + args.timeout_min * 60
    last = ""
    timed_out = False
    while True:
        run = w.jobs.get_run(run_id=run_id)
        st = run.state
        lc = st.life_cycle_state.value if (st and st.life_cycle_state) else "?"
        rs = st.result_state.value if (st and st.result_state) else "-"
        cur = f"{lc}/{rs}"
        if cur != last:
            logger.info("[%6.0fs] state=%s", time.monotonic() - (deadline - args.timeout_min * 60), cur)
            last = cur
        if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        if time.monotonic() > deadline:
            logger.warning("Local poll timeout (%d min) — cancelling run %s", args.timeout_min, run_id)
            with __import__("contextlib").suppress(Exception):
                w.jobs.cancel_run(run_id=run_id)
            timed_out = True
            break
        time.sleep(_POLL_SECONDS)

    # Post-mortem: driver log + executor markers (the hang signal lives here).
    task_run_id = (run.tasks[0].run_id if run.tasks else None) or run_id
    _dump_driver_log(w, task_run_id)
    if provider in _TRACKING_PROVIDERS:
        _dump_markers(w, _rendezvous_dir(args.catalog, args.schema, provider, match_id, period))

    final = w.jobs.get_run(run_id=run_id).state
    final_rs = final.result_state.value if (final and final.result_state) else "-"
    logger.info(
        "DONE provider=%s match=%s final=%s%s", provider, match_id, final_rs, " (LOCAL-TIMEOUT)" if timed_out else ""
    )
    return 0 if final_rs == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
