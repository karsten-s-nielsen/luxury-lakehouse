#!/usr/bin/env python3
"""Apply (or verify) Lakebase PostgreSQL grants for the Taipy app SP.

Lakebase synced tables are recreated whenever their source dbt mart is
rebuilt with ``{{ config(materialized='table') }}`` (drop + create), or when
Unity Catalog ownership changes, or when they're recreated via the
Databricks UI. Grants on the previous PG table do NOT carry over — the
new table is owned by an internal Lakebase role (``databricks_writer_*``)
with zero SELECT grants to service principals.

Historically this module claimed that ``ALTER DEFAULT PRIVILEGES FOR ROLE
databricks_superuser`` would auto-grant future synced tables. That claim
is structurally impossible: synced tables are owned by
``databricks_writer_<instance_id>``, not by ``databricks_superuser``, so
no default-privilege rule scoped to ``databricks_superuser`` ever fires.
See ``docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md``.

This script is therefore the **canonical** mechanism for ensuring the
Taipy app SP can read synced tables. It must be re-run after any
synced-table recreation. The companion ``--verify`` mode is the drift
detector used as a pre-deploy gate in ``manage_space.py deploy``.

Usage:
    # Apply grants (SP application_id resolved live via the Databricks SDK):
    uv run python scripts/run_lakebase_grants.py

    # Apply with explicit SP UUID:
    uv run python scripts/run_lakebase_grants.py --sp-uuid <uuid>

    # Verify only — exits non-zero and prints a drift diff if anything's missing:
    uv run python scripts/run_lakebase_grants.py --verify

Environment:
    DATABRICKS_HOST        Workspace hostname (with or without https:// prefix)
    Credential             Either DATABRICKS_TOKEN (local dev), or
                           DATABRICKS_AUTH_TYPE=github-oidc plus
                           ACTIONS_ID_TOKEN_REQUEST_TOKEN/_URL (CI). Bearers are resolved
                           per call via ``auth_headers()``, never snapshotted at start-up
                           (ADR-071 amendment).
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

from ingestion.databricks_auth import auth_headers, has_databricks_auth, workspace_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-5s  %(message)s")
logger = logging.getLogger(__name__)

# Lakebase endpoint path — matches terraform output ``lakebase_endpoint_name``.
ENDPOINT_NAME = "projects/soccer-analytics-dev/branches/production/endpoints/primary"

# Schemas the Taipy app reads from.
SCHEMAS = ["dev_gold", "observability"]

# Taipy app SP — resolved LIVE from the Databricks workspace by display name,
# NOT from terraform. Terraform runs only in CI (GitHub), so a local app deploy
# has no initialized terraform; shelling out to `terraform output` there fails
# with "provider plugins not installed" even though the grants are healthy.
# The SP is provisioned by terraform/modules/service_principals `hf_app`
# (display_name = "luxury-lakehouse-hf-app-v2-${var.environment}"); the constant
# below is anti-drift-tested against that .tf in test_lakebase_grants_sp_resolution.
_HF_APP_SP_DISPLAY_NAME = "luxury-lakehouse-hf-app-v2-dev"
# Explicit override escape hatch. CI passes --sp-uuid (from the terraform-output
# GitHub var) and never reaches the SDK path; this env var is its local analogue.
_SP_APP_ID_ENV = "LAKEBASE_TAIPY_SP_APP_ID"


def _normalize_host(raw: str) -> str:
    """Strip ``https://`` prefix and trailing slash from ``DATABRICKS_HOST``.

    The Databricks CLI writes this env var with the prefix, but the REST
    helpers below construct ``https://{host}/api/...`` themselves, which
    double-prefixes if not normalized.
    """
    host = raw.strip()
    if host.startswith("https://"):
        host = host[len("https://") :]
    elif host.startswith("http://"):
        host = host[len("http://") :]
    return host.rstrip("/")


def _resolve_sp_application_id() -> str:
    """Resolve the Taipy app SP's ``application_id`` WITHOUT terraform.

    Terraform runs only in CI, so a local app deploy cannot ``terraform output``
    the id. Instead we read it from the source of truth — the live Databricks
    workspace — via the SDK, using the same ``DATABRICKS_HOST``/``TOKEN`` the
    deploy already needs. Resolution order:

    1. ``LAKEBASE_TAIPY_SP_APP_ID`` env override (local analogue of CI's
       ``--sp-uuid``).
    2. SDK lookup by the SP's terraform-provisioned display name.

    This is *more* drift-safe than the old ``terraform output`` (it reads the
    real workspace, not terraform's cached view). The application_id is never
    hardcoded; only the display name is, and that is anti-drift-tested against
    the terraform module. Raises if the SP can't be uniquely resolved.
    """
    override = os.environ.get(_SP_APP_ID_ENV)
    if override:
        logger.info("Using Taipy SP application_id from %s", _SP_APP_ID_ENV)
        return override.strip()

    ws = workspace_client()
    matches = [
        sp
        for sp in ws.service_principals.list(filter=f'displayName eq "{_HF_APP_SP_DISPLAY_NAME}"')
        if sp.display_name == _HF_APP_SP_DISPLAY_NAME and sp.application_id
    ]
    if len(matches) != 1:
        msg = (
            f"Expected exactly one service principal named {_HF_APP_SP_DISPLAY_NAME!r}; "
            f"found {len(matches)}. Pass --sp-uuid or set {_SP_APP_ID_ENV}."
        )
        raise RuntimeError(msg)
    app_id = matches[0].application_id
    logger.info("Resolved Taipy SP %r -> %s via Databricks SDK", _HF_APP_SP_DISPLAY_NAME, app_id)
    return str(app_id)


def _load_expected_synced_tables() -> list[tuple[str, str]]:
    """Return the authoritative list of ``(schema, table)`` tuples.

    Source: ``ingestion.refresh_synced_tables.SYNCED_TABLES`` — the same
    inventory the daily Databricks job iterates over. Using this single
    source guarantees the grants script and the refresh task agree about
    what "all synced tables" means.
    """
    # Lazy import — avoids a heavy import chain when the script is invoked
    # only for its CLI help.
    sys.path.insert(0, "src")
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    expected: list[tuple[str, str]] = []
    for config in SYNCED_TABLES:
        schema = config.schema_override or "dev_gold"
        expected.append((schema, config.name))
    return expected


def _get_lakebase_credential(host: str) -> tuple[str, str]:
    """Get a Lakebase PG credential via the REST API.

    Returns ``(jwt_token, pg_username)``.

    Resolves the bearer at the point of use rather than taking one as a parameter: under
    GitHub OIDC the SDK mints per request, so a caller-held token is a snapshot that can
    already be dead (ADR-071 amendment).
    """
    resp = requests.post(
        f"https://{host}/api/2.0/postgres/credentials",
        headers={**auth_headers(), "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    jwt = resp.json()["token"]
    payload = json.loads(base64.urlsafe_b64decode(jwt.split(".")[1] + "=="))
    return jwt, payload["sub"]


def _get_lakebase_dns(host: str) -> str:
    """Discover the Lakebase endpoint DNS from the API."""
    project_path = ENDPOINT_NAME.rsplit("/endpoints/", 1)[0]
    resp = requests.get(
        f"https://{host}/api/2.0/postgres/{project_path}/endpoints",
        headers=auth_headers(),
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    endpoints = resp.json().get("endpoints", [])
    if not endpoints:
        msg = "No Lakebase endpoints found"
        raise RuntimeError(msg)
    return endpoints[0]["status"]["hosts"]["host"]


def _apply_schema_grants(cur: psycopg2.extensions.cursor, sp_uuid: str) -> None:
    """Grant USAGE on schema and bulk SELECT on existing tables.

    The bulk ``GRANT SELECT ON ALL TABLES IN SCHEMA`` covers tables that
    existed at invocation time. For future tables we rely on re-running
    this script after sync-recreation (see module docstring + ADR-005).
    """
    sp_quoted = f'"{sp_uuid}"'
    for schema in SCHEMAS:
        for stmt in (
            f"GRANT USAGE ON SCHEMA {schema} TO {sp_quoted}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {sp_quoted}",
            # ALTER DEFAULT PRIVILEGES (without FOR ROLE) covers future tables
            # created by the CURRENT user. It does NOT fire for synced-table
            # recreations (those are owned by databricks_writer_<id>, which we
            # cannot target without role membership). Kept as defence-in-depth
            # for any ad-hoc tables a human admin might create in the schema.
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {sp_quoted}",
        ):
            cur.execute(stmt)
            logger.info("  OK: %s", stmt[:100])


def _verify_coverage(
    cur: psycopg2.extensions.cursor, sp_uuid: str, expected: list[tuple[str, str]]
) -> list[tuple[str, str]]:
    """Return the list of ``(schema, table)`` tuples missing SELECT for ``sp_uuid``.

    Empty list means full coverage (i.e. the grants gate would pass).
    """
    cur.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.role_table_grants
        WHERE grantee = %s AND privilege_type = 'SELECT'
          AND table_schema = ANY(%s)
        """,
        (sp_uuid, SCHEMAS),
    )
    have: set[tuple[str, str]] = {(s, t) for s, t in cur.fetchall()}
    return [(s, t) for s, t in expected if (s, t) not in have]


def connect_as_superuser() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection to Lakebase as ``databricks_superuser``.

    Uses ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` env vars to fetch a short-
    lived admin JWT via the Lakebase credential API, then connects to the
    primary endpoint with ``sslmode=require`` and ``autocommit=True``. Returns
    the open connection; caller is responsible for closing it.

    Raises:
        RuntimeError: if ``DATABRICKS_HOST`` or ``DATABRICKS_TOKEN`` is unset.

    This helper is exposed (non-underscore) specifically so tests and one-off
    scripts can reuse the proven connection pattern without duplicating the
    JWT-fetch logic. Not used by ``main()`` below, which keeps its
    step-by-step logs for operator clarity.
    """
    raw_host = os.environ.get("DATABRICKS_HOST")
    if not raw_host or not has_databricks_auth():
        raise RuntimeError(
            "DATABRICKS_HOST and a credential are required: either DATABRICKS_TOKEN, or "
            "DATABRICKS_AUTH_TYPE=github-oidc with ACTIONS_ID_TOKEN_REQUEST_* present."
        )
    host = _normalize_host(raw_host)
    dns = _get_lakebase_dns(host)
    jwt, pg_user = _get_lakebase_credential(host)
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
    return conn


def _existing_synced_tables(cur: psycopg2.extensions.cursor) -> set[tuple[str, str]]:
    """Return ``{(schema, table)}`` for every PG table/partitioned-table in SCHEMAS.

    Lakebase synced tables show up as ``relkind='p'`` (partitioned table).
    Regular tables (``'r'``) are included for defence-in-depth.
    """
    cur.execute(
        """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p')
        """,
        (SCHEMAS,),
    )
    return {(s, t) for s, t in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply or verify Lakebase grants for the Taipy app SP")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check coverage only; exit non-zero on drift. Pre-deploy gate mode.",
    )
    parser.add_argument(
        "--sp-uuid",
        help=(
            "Taipy app SP application ID. Defaults to a live Databricks SDK lookup by "
            "display name (or the LAKEBASE_TAIPY_SP_APP_ID env var). CI passes this explicitly."
        ),
    )
    args = parser.parse_args()

    raw_host = os.environ.get("DATABRICKS_HOST")
    if not raw_host or not has_databricks_auth():
        logger.error(
            "DATABRICKS_HOST and a credential are required: either DATABRICKS_TOKEN, or "
            "DATABRICKS_AUTH_TYPE=github-oidc with ACTIONS_ID_TOKEN_REQUEST_* present."
        )
        return 2
    host = _normalize_host(raw_host)

    sp_uuid = args.sp_uuid or _resolve_sp_application_id()
    logger.info("Target SP UUID: %s", sp_uuid)

    logger.info("Discovering Lakebase endpoint DNS...")
    dns = _get_lakebase_dns(host)
    logger.info("Lakebase DNS: %s", dns)

    logger.info("Obtaining admin credential via REST API...")
    jwt, pg_user = _get_lakebase_credential(host)
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

    expected = _load_expected_synced_tables()
    logger.info("Expected synced-table count: %d", len(expected))

    try:
        if args.verify:
            existing = _existing_synced_tables(cur)
            not_in_pg = [pair for pair in expected if pair not in existing]
            if not_in_pg:
                logger.warning(
                    "Expected %d synced tables; %d are not yet present in Lakebase: %s",
                    len(expected),
                    len(not_in_pg),
                    ", ".join(f"{s}.{t}" for s, t in not_in_pg),
                )
            verifiable = [pair for pair in expected if pair in existing]
            missing = _verify_coverage(cur, sp_uuid, verifiable)
            if missing:
                logger.error("DRIFT: SP %s is missing SELECT on %d synced table(s):", sp_uuid, len(missing))
                for schema, table in missing:
                    logger.error("  - %s.%s", schema, table)
                logger.error("Fix: run `uv run python scripts/run_lakebase_grants.py` (no --verify) and re-check.")
                return 1
            logger.info(
                "OK: SP %s has SELECT on all %d synced tables present in Lakebase.",
                sp_uuid,
                len(verifiable),
            )
            if not_in_pg:
                logger.warning(
                    "NOTE: %d expected synced table(s) not yet materialized in Lakebase — "
                    "re-run this verifier after the next refresh.",
                    len(not_in_pg),
                )
            return 0

        logger.info("Applying schema-level grants...")
        _apply_schema_grants(cur, sp_uuid)
        logger.info("Verifying per-table coverage...")
        existing = _existing_synced_tables(cur)
        verifiable = [pair for pair in expected if pair in existing]
        missing = _verify_coverage(cur, sp_uuid, verifiable)
        if missing:
            logger.error("Post-apply drift: %d tables still missing SELECT:", len(missing))
            for schema, table in missing:
                logger.error("  - %s.%s", schema, table)
            return 1
        logger.info(
            "Done. SP %s has SELECT on %d synced tables (of %d expected; %d not yet synced).",
            sp_uuid,
            len(verifiable),
            len(expected),
            len(expected) - len(verifiable),
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
