#!/usr/bin/env python3
"""Upload the CI dbt shim to Workspace Files.

Destination: /Workspace/Shared/luxury-lakehouse-ci/run_dbt_in_databricks.py

Run automatically by ``.github/workflows/dbt-live-ci.yml`` immediately before it triggers
the Databricks job, so the deployed shim is always the one in the triggering commit.

It used to say "re-run when the shim changes" and nothing enforced that. The copy was
uploaded by hand on 2026-04-23 and drifted for three months: on 2026-07-28 the nightly was
still executing a retired ``install_dbt()`` that pip-installed dbt from a RANGE, resolving
1.11.8 against a manifest written by 1.11.12, and dbt rejected it with
``Field "macros" ... in WritableManifest has invalid value``. The repo file had been
correct for hours; the job had never seen it. Hence the CI step, and the sentinel
``test_dbt_live_ci_deploys_the_shim_before_triggering``.

Still safe to run by hand (idempotent, ``overwrite=True``, no arguments).

Workspace Files chosen over UC Volumes because serverless-job
spark_python_task resolution against UC Volume paths failed in PR 4a E2E
even with READ_VOLUME + WRITE_VOLUME + USE SCHEMA + USE CATALOG granted
to the OIDC SP; Workspace Files worked with just default /Shared ACLs.

Uses ambient Databricks auth (WorkspaceClient default resolution).
"""

from __future__ import annotations

import io
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from ingestion.databricks_auth import workspace_client

# PR-Cycle-B (2026-05-01): databricks-sdk is in the [sdk] optional extra.
# Lazy-import keeps this module importable without the extra installed.
if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat
else:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        WorkspaceClient = None  # type: ignore[assignment, misc]
        ImportFormat = None  # type: ignore[assignment, misc]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SHIM_WORKSPACE_DIR = "/Shared/luxury-lakehouse-ci"
_SHIM_WORKSPACE_PATH = f"{_SHIM_WORKSPACE_DIR}/run_dbt_in_databricks.py"
_LOCAL_SHIM = Path(__file__).parent / "ci" / "run_dbt_in_databricks.py"


def main() -> int:
    if not _LOCAL_SHIM.exists():
        logger.error("Local shim missing at %s", _LOCAL_SHIM)
        return 1

    ws = workspace_client()
    ws.workspace.mkdirs(_SHIM_WORKSPACE_DIR)
    logger.info("Uploading %s to %s", _LOCAL_SHIM, _SHIM_WORKSPACE_PATH)
    content = _LOCAL_SHIM.read_bytes()
    ws.workspace.upload(
        path=_SHIM_WORKSPACE_PATH,
        content=io.BytesIO(content),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    logger.info("Upload complete (%d bytes)", len(content))
    return 0


if __name__ == "__main__":
    sys.exit(main())
