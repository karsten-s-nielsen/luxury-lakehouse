#!/usr/bin/env python3
"""Upload the CI dbt shim to UC Volume. Re-run when the shim changes.

Destination: /Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py

Uses ambient Databricks auth (WorkspaceClient default resolution).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SHIM_VOLUME_PATH = "/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py"
_LOCAL_SHIM = Path(__file__).parent / "ci" / "run_dbt_in_databricks.py"


def main() -> int:
    if not _LOCAL_SHIM.exists():
        logger.error("Local shim missing at %s", _LOCAL_SHIM)
        return 1

    ws = WorkspaceClient()
    logger.info("Uploading %s to %s", _LOCAL_SHIM, _SHIM_VOLUME_PATH)
    with _LOCAL_SHIM.open("rb") as f:
        ws.files.upload(_SHIM_VOLUME_PATH, f, overwrite=True)
    logger.info("Upload complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
