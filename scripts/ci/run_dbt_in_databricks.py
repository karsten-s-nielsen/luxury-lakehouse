"""Shim executed inside a Databricks Job cluster for live dbt CI (PR 4a).

Invoked by spark_python_task with args:
    --tarball-path     UC Volume path to the dbt_project tarball
    --manifest-path    UC Volume path to the main-branch dbt manifest JSON
    --select-arg       dbt --select argument (e.g. 'state:modified+' or '+all')
    --output-path      UC Volume path where run_results.json will be uploaded

Flow:
    1. Download tarball + manifest from UC Volume to /tmp/.
    2. Extract tarball; manifest is copied into target-main/ alongside dbt_project/.
    3. Pip-install dbt-core + dbt-databricks (version pinned per shim upload).
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

# dbt version — keep in sync with pyproject.toml [project.optional-dependencies].dbt.
_DBT_PIN = "dbt-core>=1.10.0,<1.12.0"
_DBT_DATABRICKS_PIN = "dbt-databricks>=1.10.0,<1.12.0"


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
            raise RuntimeError(
                f"No SQL warehouse with name starting with {_PROJECT_WAREHOUSE_NAME_PREFIX!r}"
            )
        http_path = f"/sql/1.0/warehouses/{project_wh.id}"
        os.environ["DATABRICKS_HTTP_PATH"] = http_path
        logger.info("Resolved DATABRICKS_HTTP_PATH: %s (warehouse %s)", http_path, project_wh.name)


def install_dbt() -> None:
    """Install dbt-core and dbt-databricks into the cluster's Python env."""
    logger.info("Installing dbt: %s + %s", _DBT_PIN, _DBT_DATABRICKS_PIN)
    subprocess.run(  # noqa: S603 — args list is a fixed module-level constant, no shell
        [sys.executable, "-m", "pip", "install", "--quiet", _DBT_PIN, _DBT_DATABRICKS_PIN],
        check=True,
    )


def run_dbt(project_dir: Path, select_arg: str, manifest_main_dir: Path) -> int:
    """Run `dbt deps` + `dbt build --select <arg> --state <manifest_main_dir>`.

    Returns dbt build's exit code (0 = success, 1/2 = warnings/errors).
    """
    logger.info("Running dbt deps in %s", project_dir)
    deps = subprocess.run(
        ["dbt", "deps", "--profiles-dir", "."],  # noqa: S607 — dbt is installed on PATH by install_dbt()
        cwd=project_dir,
        check=False,
    )
    if deps.returncode != 0:
        logger.error("dbt deps failed with exit code %d", deps.returncode)
        return deps.returncode

    logger.info("Running dbt build --select %s --state %s", select_arg, manifest_main_dir)
    build = subprocess.run(  # noqa: S603 — select_arg validated via argparse; fixed command tokens otherwise
        [  # noqa: S607 — dbt is installed on PATH by install_dbt()
            "dbt",
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

    install_dbt()
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
    sys.exit(main())
