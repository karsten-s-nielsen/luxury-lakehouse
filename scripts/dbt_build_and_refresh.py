#!/usr/bin/env python3
"""Run `dbt build` then `python -m ingestion.refresh_synced_tables --wait` atomically.

Canonical local dev flow for "rebuild gold tables and propagate to Lakebase".
If dbt fails, refresh is skipped. If refresh fails after dbt success, the
wrapper exits with the refresh exit code.

Usage:
    python scripts/dbt_build_and_refresh.py                       # full build
    python scripts/dbt_build_and_refresh.py --select tag:cost     # targeted
    python scripts/dbt_build_and_refresh.py --target prod         # forwarded
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DBT_PROJECT = _REPO_ROOT / "dbt_project"


def main() -> int:
    """Run dbt build, then refresh synced tables on success."""
    dbt_args = sys.argv[1:]
    print(f"==> Running: dbt build {' '.join(dbt_args)}", flush=True)

    dbt_result = subprocess.run(  # noqa: S603
        ["dbt", "build", *dbt_args],  # noqa: S607
        cwd=str(_DBT_PROJECT),
        check=False,
    )

    if dbt_result.returncode != 0:
        print(
            f"==> ERROR: dbt build failed (exit {dbt_result.returncode}). Skipping synced table refresh.",
            flush=True,
        )
        return dbt_result.returncode

    print("==> dbt build succeeded. Triggering synced table refresh (--wait)...", flush=True)

    refresh_result = subprocess.run(
        [sys.executable, "-m", "ingestion.refresh_synced_tables", "--wait"],
        check=False,
    )

    if refresh_result.returncode != 0:
        print(
            f"==> ERROR: refresh_synced_tables failed (exit {refresh_result.returncode}).",
            flush=True,
        )
    else:
        print("==> dbt build + synced table refresh complete.", flush=True)

    return refresh_result.returncode


if __name__ == "__main__":
    sys.exit(main())
