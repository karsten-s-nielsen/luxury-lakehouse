#!/usr/bin/env python3
"""Run Lakebase PostgreSQL grants for the Taipy app service principal.

Automates the manual psql step documented in lakebase_grants.sql.
Connects as the workspace admin (via PAT) and grants SELECT access
to the app service principal on dev_gold and observability schemas.

Usage:
    uv run python scripts/run_lakebase_grants.py
    uv run python scripts/run_lakebase_grants.py --verify  # check grants only

Environment:
    DATABRICKS_HOST          Workspace hostname (no https://)
    DATABRICKS_TOKEN         PAT for workspace admin
    DATABRICKS_HTTP_PATH     SQL warehouse path (for endpoint discovery)

Requires:
    psycopg2-binary, requests, databricks-sdk (all project dependencies)
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import uuid

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-5s  %(message)s")
logger = logging.getLogger(__name__)

# Service principal UUID for the Taipy app.
# Find via: terraform output -raw hf_app_sp_application_id
APP_SP_UUID = "1a1dbf08-df56-48de-b97a-276b2a4232d8"

# Lakebase project and endpoint identifiers (from Terraform)
ENDPOINT_NAME = "projects/soccer-analytics-dev/branches/production/endpoints/primary"

# Schemas to grant access to
SCHEMAS = ["dev_gold", "observability"]


def _get_lakebase_credential(host: str, token: str) -> tuple[str, str]:
    """Get a Lakebase PG credential via the REST API.

    Returns (jwt_token, pg_username).
    """
    resp = requests.post(
        f"https://{host}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    jwt = resp.json()["token"]
    payload = json.loads(base64.urlsafe_b64decode(jwt.split(".")[1] + "=="))
    return jwt, payload["sub"]


def _get_lakebase_dns(host: str, token: str) -> str:
    """Discover the Lakebase endpoint DNS from the API."""
    resp = requests.get(
        f"https://{host}/api/2.0/postgres/{ENDPOINT_NAME.rsplit('/endpoints/', 1)[0]}/endpoints",
        headers={"Authorization": f"Bearer {token}"},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    endpoints = resp.json().get("endpoints", [])
    if not endpoints:
        msg = "No Lakebase endpoints found"
        raise RuntimeError(msg)
    return endpoints[0]["status"]["hosts"]["host"]


def _run_grants(cur: psycopg2.extensions.cursor, sp_uuid: str) -> None:
    """Run GRANT statements for the service principal."""
    sp_quoted = f'"{sp_uuid}"'
    for schema in SCHEMAS:
        grants = [
            f"GRANT USAGE ON SCHEMA {schema} TO {sp_quoted}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {sp_quoted}",
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {sp_quoted}",
        ]
        for g in grants:
            cur.execute(g)
            logger.info("OK: %s", g[:80])


def _verify_grants(cur: psycopg2.extensions.cursor, sp_uuid: str) -> int:
    """Check existing grants for the service principal."""
    cur.execute(
        "SELECT table_schema, COUNT(*) FROM information_schema.role_table_grants "
        "WHERE grantee = %s GROUP BY table_schema ORDER BY 1",
        (sp_uuid,),
    )
    rows = cur.fetchall()
    total = 0
    for schema, count in rows:
        logger.info("  %s: %d table grants", schema, count)
        total += count
    return total


def main() -> None:
    """Run or verify Lakebase grants."""
    parser = argparse.ArgumentParser(description="Run Lakebase PG grants for the Taipy app SP")
    parser.add_argument("--verify", action="store_true", help="Check grants only, don't modify")
    parser.add_argument("--sp-uuid", default=APP_SP_UUID, help=f"Service principal UUID (default: {APP_SP_UUID})")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        logger.error("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        sys.exit(1)

    logger.info("Discovering Lakebase endpoint DNS...")
    dns = _get_lakebase_dns(host, token)
    logger.info("Lakebase DNS: %s", dns)

    logger.info("Obtaining admin credential via REST API...")
    jwt, pg_user = _get_lakebase_credential(host, token)
    logger.info("Connected as PG user: %s", pg_user)

    conn = psycopg2.connect(
        host=dns,
        port=5432,
        database="databricks_postgres",
        user=pg_user,
        password=jwt,
        sslmode="require",
        connect_timeout=10,
    )
    conn.autocommit = True
    cur = conn.cursor()

    if args.verify:
        logger.info("Verifying grants for SP %s:", args.sp_uuid)
        total = _verify_grants(cur, args.sp_uuid)
        if total == 0:
            logger.warning("No grants found — run without --verify to apply")
        else:
            logger.info("Total: %d grants", total)
    else:
        logger.info("Applying grants for SP %s...", args.sp_uuid)
        _run_grants(cur, args.sp_uuid)
        logger.info("Verifying...")
        total = _verify_grants(cur, args.sp_uuid)
        logger.info("Done. %d grants verified.", total)

    conn.close()


if __name__ == "__main__":
    main()
