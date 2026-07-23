#!/usr/bin/env python3
"""Print a short-lived Databricks OAuth bearer token from a configured CLI profile.

Post-PAT-retirement (2026-07-21) PATs are dead. Local live/integration tests and local
dbt runs still need a valid bearer token: the databricks-sdk picks up ``DATABRICKS_TOKEN``,
and dbt's ``profiles.yml`` reads ``token: env_var('DATABRICKS_TOKEN')`` directly. This mints
a fresh OAuth U2M token (from the databricks CLI token cache) so both authenticate.

Usage (local live tests / dbt):

    export DATABRICKS_TOKEN=$(uv run --extra sdk python scripts/mint_databricks_oauth.py)
    uv run pytest src/tests/...          # live tests now authenticate via OAuth
    uv run --extra dbt dbt compile ...   # dbt reads the same DATABRICKS_TOKEN

Profile defaults to ``$DATABRICKS_CONFIG_PROFILE`` or ``OAUTH``; override with ``--profile``.
Prints ONLY the token to stdout (safe for ``$(...)`` capture); diagnostics go to stderr.
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a Databricks OAuth bearer token")
    parser.add_argument(
        "--profile",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE") or "OAUTH",
        help="databricks CLI profile to mint from (default: $DATABRICKS_CONFIG_PROFILE or OAUTH)",
    )
    args = parser.parse_args()

    # A stale DATABRICKS_TOKEN (e.g. a dead PAT) would shadow the OAuth profile and, combined
    # with an oauth client_id, trips the SDK's "more than one authorization method" guard.
    os.environ.pop("DATABRICKS_TOKEN", None)

    try:
        from databricks.sdk.core import Config
    except ImportError:
        print("databricks-sdk not installed; run with `uv run --extra sdk`", file=sys.stderr)
        return 1

    cfg = Config(profile=args.profile)
    token = cfg.authenticate().get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        print(f"profile '{args.profile}' produced no bearer token", file=sys.stderr)
        return 1
    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
