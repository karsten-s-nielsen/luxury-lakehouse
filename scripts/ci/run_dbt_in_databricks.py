"""Shim executed inside a Databricks Job cluster for live dbt CI (PR 4a).

Invoked by spark_python_task with args:
    --tarball-path     UC Volume path to the dbt_project tarball
    --manifest-path    UC Volume path to the main-branch dbt manifest JSON
    --select-arg       dbt --select argument (e.g. 'state:modified+' or '+all')
    --output-path      UC Volume path where run_results.json will be uploaded

Flow:
    1. Download tarball + manifest from UC Volume to /tmp/.
    2. Extract tarball; manifest is copied into target-main/ alongside dbt_project/.
    3. (dbt is already present — declared on the job's serverless environment by
       scripts/trigger_dbt_job.py, version-locked to uv.lock.)
    4. cd dbt_project; dbt deps; dbt build --select <arg> --state target-main/ --profiles-dir .
    5. Upload target/run_results.json back to UC Volume.
    6. Exit with dbt's exit code.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_PROJECT_WAREHOUSE_NAME_PREFIX = "soccer-analytics-warehouse"

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# dbt is DECLARED on the job's serverless environment by scripts/trigger_dbt_job.py, not
# installed here. The former runtime `pip install` used a version RANGE, which resolved a
# different dbt than the runner used to write manifest_main.json — the WritableManifest
# schema mismatch that failed dbt-live-ci nightly from 2026-07-22 (ADR-046 lockstep).


def _workspace_client() -> WorkspaceClient:
    """Construct a Databricks WorkspaceClient using ambient runtime auth.

    Inside a Databricks job, the runtime provides implicit auth. No
    explicit credentials are needed.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dbt inside a Databricks Job for live CI.")
    parser.add_argument("--tarball-path", required=True, help="UC Volume path to dbt_project tarball")
    parser.add_argument("--manifest-path", required=True, help="UC Volume path to main-branch manifest JSON")
    parser.add_argument("--select-arg", required=True, help="dbt --select argument")
    parser.add_argument("--output-path", required=True, help="UC Volume path to upload run_results.json to")
    return parser.parse_args(argv)


def download_from_volume(volume_path: str, out_path: Path) -> None:
    """Download a file from UC Volume to the local cluster filesystem."""
    ws = _workspace_client()
    logger.info("Downloading %s to %s", volume_path, out_path)
    resp = ws.files.download(volume_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # SDK types resp.contents as BinaryIO | None. In practice on a successful
    # download it is always a stream with .read() returning bytes. Unit tests
    # may substitute raw bytes directly; accept both shapes.
    contents = resp.contents
    if contents is None:
        raise RuntimeError(f"Empty download response for {volume_path}")
    payload: bytes = contents.read() if hasattr(contents, "read") else contents  # type: ignore[assignment]

    with out_path.open("wb") as f:
        f.write(payload)


def extract_tarball(tarball: Path, extract_dir: Path) -> None:
    """Extract a .tar.gz into extract_dir."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s to %s", tarball, extract_dir)
    with tarfile.open(tarball, "r:gz") as tf:
        tf.extractall(extract_dir, filter="data")


def ensure_dbt_env_vars() -> None:
    """Populate DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH.

    dbt profiles.yml references these via env_var(). On a serverless job, they
    are not auto-injected, so we resolve them via WorkspaceClient (mirrors
    src/ingestion/dbt_runner._ensure_databricks_env_vars).
    """
    ws = _workspace_client()

    if not os.environ.get("DATABRICKS_HOST"):
        host = ws.config.host
        if not host:
            raise RuntimeError("Cannot resolve DATABRICKS_HOST: WorkspaceClient.config.host is empty")
        os.environ["DATABRICKS_HOST"] = host
        logger.info("Resolved DATABRICKS_HOST: %s", host)

    if not os.environ.get("DATABRICKS_TOKEN"):
        headers = ws.config.authenticate()
        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise RuntimeError(f"SDK auth header is not Bearer: {auth_header[:30]}...")
        os.environ["DATABRICKS_TOKEN"] = auth_header[len("Bearer ") :]
        logger.info("Resolved DATABRICKS_TOKEN from SDK auth provider")

    if not os.environ.get("DATABRICKS_HTTP_PATH"):
        warehouses = list(ws.warehouses.list())
        project_wh = next(
            (wh for wh in warehouses if wh.name and wh.name.startswith(_PROJECT_WAREHOUSE_NAME_PREFIX)),
            None,
        )
        if project_wh is None or not project_wh.id:
            raise RuntimeError(f"No SQL warehouse with name starting with {_PROJECT_WAREHOUSE_NAME_PREFIX!r}")
        http_path = f"/sql/1.0/warehouses/{project_wh.id}"
        os.environ["DATABRICKS_HTTP_PATH"] = http_path
        logger.info("Resolved DATABRICKS_HTTP_PATH: %s (warehouse %s)", http_path, project_wh.name)


def run_dbt(project_dir: Path, select_arg: str, manifest_main_dir: Path) -> int:
    """Run `dbt deps` + `dbt build --select <arg> --state <manifest_main_dir>`.

    Invoked as a MODULE (``python -m dbt.cli.main``), not as the ``dbt`` console script.
    The old form relied on ``install_dbt()`` putting ``dbt`` on PATH; with dbt now declared
    on the serverless environment there is no such guarantee, and a PATH miss would surface
    as a bare ``FileNotFoundError`` deep inside the job. ``sys.executable`` is also an
    absolute path, which is what ruff's S607 is about — hence no noqa needed.

    Returns dbt build's exit code (0 = success, 1/2 = warnings/errors).
    """
    logger.info("Running dbt deps in %s", project_dir)
    deps = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "deps", "--profiles-dir", "."],
        cwd=project_dir,
        check=False,
    )
    if deps.returncode != 0:
        logger.error("dbt deps failed with exit code %d", deps.returncode)
        return deps.returncode

    logger.info("Running dbt build --select %s --state %s", select_arg, manifest_main_dir)
    build = subprocess.run(  # noqa: S603 — select_arg validated via argparse; fixed command tokens otherwise
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--select",
            select_arg,
            "--state",
            str(manifest_main_dir),
            "--profiles-dir",
            ".",
        ],
        cwd=project_dir,
        check=False,
    )
    return build.returncode


def upload_output(local_path: Path, volume_path: str) -> None:
    """Upload the local run_results.json to UC Volume with overwrite=True."""
    ws = _workspace_client()
    logger.info("Uploading %s to %s", local_path, volume_path)
    with local_path.open("rb") as f:
        ws.files.upload(volume_path, f, overwrite=True)


def stage_dbt_workspace(extract_dir: Path, manifest_main: Path) -> tuple[Path, Path]:
    """Verify extraction shape and copy the main-branch manifest into target-main/.

    Returns ``(project_dir, target_main_dir)``. Split out of ``main`` so it is a
    discrete seam the end-to-end test can mock — otherwise a test that mocks
    only ``extract_tarball`` would still hit real filesystem work here.
    """
    project_dir = extract_dir / "dbt_project"
    if not project_dir.exists():
        raise RuntimeError(f"dbt_project/ not found in tarball (looked at {project_dir})")

    target_main = project_dir / "target-main"
    target_main.mkdir(parents=True, exist_ok=True)
    shutil.copy(manifest_main, target_main / "manifest.json")
    return project_dir, target_main


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    # Unique per-invocation workdir — serverless job containers can be recycled
    # across runs and leave prior /tmp state with permissions that the new
    # invocation can't clean up. tempfile.mkdtemp guarantees no cross-run
    # conflicts.
    import tempfile

    workdir = Path(tempfile.mkdtemp(prefix="dbt_live_ci_"))

    tarball = workdir / "dbt_project.tar.gz"
    manifest_main = workdir / "manifest_main.json"
    download_from_volume(args.tarball_path, tarball)
    download_from_volume(args.manifest_path, manifest_main)

    extract_dir = workdir / "extracted"
    extract_tarball(tarball, extract_dir)

    project_dir, target_main = stage_dbt_workspace(extract_dir, manifest_main)

    # No install step: dbt is declared on the job's serverless environment
    # (scripts/trigger_dbt_job.py), so it is already present and version-locked.
    ensure_dbt_env_vars()
    exit_code = run_dbt(project_dir, args.select_arg, target_main)

    run_results = project_dir / "target" / "run_results.json"
    if run_results.exists():
        upload_output(run_results, args.output_path)
    else:
        logger.warning("run_results.json not found at %s — uploading empty placeholder", run_results)
        empty = workdir / "empty_run_results.json"
        empty.write_text('{"results": [], "note": "dbt did not produce run_results.json"}')
        upload_output(empty, args.output_path)

    return exit_code


if __name__ == "__main__":
    # Databricks serverless job tasks wrap __main__ in an IPython context that
    # treats SystemExit as abnormal — even SystemExit(0) shows up as
    # "error: SystemExit: 0" in runs/get-output. Return-normally on success,
    # raise on non-zero so the task state reflects dbt's actual outcome.
    _rc = main()
    if _rc != 0:
        raise RuntimeError(f"dbt exited with code {_rc}")
