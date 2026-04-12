#!/usr/bin/env python3
"""Grant Lakebase synced table refresh permissions to service principals.

The Taipy app's `hf_app v2` SP and the ingestion job's `ingestion` SP both need
two distinct grants to call the synced table refresh API:

  1. CAN_USE on the Lakebase database project
       (enables GET /api/2.0/database/synced_tables/{full_name})
  2. CAN_RUN on each of the 34 backing pipelines
       (enables POST /api/2.0/pipelines/{pipeline_id}/updates)

These grants enable:
  - The daily Databricks job's `refresh_synced_tables` task (final stage,
    runs as the `ingestion` SP)
  - The Taipy admin endpoint POST /api/cache/clear?refresh_synced=1
    (the background subprocess runs as the `hf_app v2` SP)

Run this script:
  - After any new synced table is added (one-time per table)
  - After any synced table is recreated (UC recreate / schema change),
    because pipeline_ids may change
  - As part of `scripts/maintain_synced_tables.py` (Step 0)

Idempotent: granting an existing permission is a no-op. Reversible: --revoke
removes the grants via the same endpoints with `permission_level=None`.

Usage:
    uv run python scripts/grant_synced_table_permissions.py                # apply grants
    uv run python scripts/grant_synced_table_permissions.py --status       # show current ACLs only
    uv run python scripts/grant_synced_table_permissions.py --dry-run      # preview, no changes
    uv run python scripts/grant_synced_table_permissions.py --revoke       # remove grants (emergency)
    uv run python scripts/grant_synced_table_permissions.py --environment prod

Auth: uses `WorkspaceClient` — must run as a workspace admin (CAN_MANAGE on
the database project + IS_OWNER or CAN_MANAGE on each pipeline).

Verifying: Look for `Summary: 70 succeeded, 0 failed`. Then test from staging
via `curl -X POST https://luxury-lakehouse-staging.hf.space/api/cache/clear?refresh_synced=1
-H "Authorization: Bearer $HF_TOKEN"` and watch the staging logs for
`admin: background synced refresh completed (exit 0)`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

from ingestion.refresh_synced_tables import DEFAULT_CATALOG, DEFAULT_SCHEMA, SYNCED_TABLES, _get_auth_headers

_LOG_SOURCE = "grant_synced_table_permissions"

DATABASE_PROJECT_PERMISSION = "CAN_USE"
PIPELINE_PERMISSION = "CAN_RUN"

# SP display name patterns — `{env}` is substituted at runtime
HF_APP_SP_NAME_PATTERN = "luxury-lakehouse-hf-app-v2-{env}"
INGESTION_SP_NAME_PATTERN = "luxury-lakehouse-ingestion-{env}"


def _log(event: str, **kwargs: object) -> None:
    """Emit a structured JSON-line log to stdout."""
    record = {"source": _LOG_SOURCE, "event": event, **kwargs}
    print(json.dumps(record), flush=True)


def _host() -> str:
    raw = os.environ["DATABRICKS_HOST"]
    return raw if raw.startswith("https://") else f"https://{raw}"


def _resolve_sp_app_id(ws: WorkspaceClient, display_name: str) -> str:
    """Look up an SP application_id by display name. Raises if not found."""
    for sp in ws.service_principals.list():
        if sp.display_name == display_name:
            if not sp.application_id:
                msg = f"Service principal {display_name!r} has no application_id"
                raise RuntimeError(msg)
            return sp.application_id
    msg = f"Service principal {display_name!r} not found in workspace"
    raise RuntimeError(msg)


def _resolve_database_project_id(headers: dict[str, str], host: str) -> str:
    """Resolve the database project ID by inspecting one synced table's metadata.

    Avoids hardcoding the project ID — the synced table metadata response
    includes `effective_database_project_id`, which is stable across grants
    and is the canonical project the synced tables belong to.
    """
    sample_table = SYNCED_TABLES[0][0]  # first table in the list
    full = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{sample_table}"
    r = requests.get(f"{host}/api/2.0/database/synced_tables/{full}", headers=headers, timeout=(10, 30))
    r.raise_for_status()
    data = r.json()
    project_id = data.get("effective_database_project_id")
    if not project_id:
        msg = f"Synced table {full} has no effective_database_project_id in response"
        raise RuntimeError(msg)
    return project_id


def _patch_acl(host: str, headers: dict[str, str], path: str, sp_app_id: str, level: str | None) -> tuple[bool, str]:
    """PATCH a permissions endpoint. level=None revokes."""
    body = {"access_control_list": [{"service_principal_name": sp_app_id, "permission_level": level}]}
    r = requests.patch(f"{host}{path}", headers=headers, json=body, timeout=(10, 30))
    ok = 200 <= r.status_code < 300
    return ok, f"HTTP {r.status_code}{'' if ok else ' ' + r.text[:200]}"


def _enumerate_pipeline_ids(host: str, headers: dict[str, str]) -> list[tuple[str, str, str]]:
    """Resolve all 34 synced tables' backing pipeline_ids.

    Returns a list of (table_name, schema, pipeline_id). Tables that fail to
    resolve are reported via _log and excluded from the returned list.
    """
    resolved: list[tuple[str, str, str]] = []
    for table_name, schema_override in SYNCED_TABLES:
        schema = schema_override or DEFAULT_SCHEMA
        full = f"{DEFAULT_CATALOG}.{schema}.{table_name}"
        try:
            r = requests.get(f"{host}/api/2.0/database/synced_tables/{full}", headers=headers, timeout=(10, 30))
            r.raise_for_status()
            pipeline_id = r.json()["data_synchronization_status"]["pipeline_id"]
            resolved.append((table_name, schema, pipeline_id))
        except Exception as exc:
            _log("resolve_failed", table=table_name, error=str(exc)[:200])
    return resolved


def _show_status(host: str, headers: dict[str, str], project_id: str, sp_app_ids: list[tuple[str, str]]) -> None:
    """Print current ACLs for the database project and the first pipeline."""
    _log("status_check", project_id=project_id)

    # Database project
    r = requests.get(f"{host}/api/2.0/permissions/database-projects/{project_id}", headers=headers, timeout=(10, 30))
    if r.ok:
        sp_set = {app_id for _, app_id in sp_app_ids}
        for entry in r.json().get("access_control_list", []):
            principal = entry.get("user_name") or entry.get("group_name") or entry.get("service_principal_name") or "?"
            perms = [p.get("permission_level") for p in entry.get("all_permissions", [])]
            is_target = principal in sp_set
            _log(
                "project_acl_entry",
                principal=principal,
                permissions=perms,
                is_target_sp=is_target,
            )

    # First pipeline
    pipelines = _enumerate_pipeline_ids(host, headers)
    if pipelines:
        first_table, _, first_pid = pipelines[0]
        r = requests.get(f"{host}/api/2.0/permissions/pipelines/{first_pid}", headers=headers, timeout=(10, 30))
        if r.ok:
            for entry in r.json().get("access_control_list", []):
                principal = (
                    entry.get("user_name") or entry.get("group_name") or entry.get("service_principal_name") or "?"
                )
                perms = [p.get("permission_level") for p in entry.get("all_permissions", [])]
                _log(
                    "pipeline_acl_entry",
                    table=first_table,
                    pipeline_id=first_pid,
                    principal=principal,
                    permissions=perms,
                )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Grant Lakebase synced table refresh permissions to service principals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--grant", action="store_true", help="Apply grants (default)")
    mode_group.add_argument("--revoke", action="store_true", help="Remove grants")
    mode_group.add_argument("--status", action="store_true", help="Show current ACLs only (read-only)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without applying")
    parser.add_argument(
        "--environment",
        default="dev",
        help="Workspace environment for SP name resolution (default: dev)",
    )
    args = parser.parse_args()

    # Default mode is grant
    if not (args.grant or args.revoke or args.status):
        args.grant = True

    headers = _get_auth_headers()
    headers["Content-Type"] = "application/json"
    host = _host()
    ws = WorkspaceClient()

    mode = "status" if args.status else ("revoke" if args.revoke else "grant")
    _log("start", environment=args.environment, mode=mode, dry_run=args.dry_run)

    # Resolve SP application_ids by display name
    try:
        hf_app_name = HF_APP_SP_NAME_PATTERN.format(env=args.environment)
        ingestion_name = INGESTION_SP_NAME_PATTERN.format(env=args.environment)
        hf_app_sp = _resolve_sp_app_id(ws, hf_app_name)
        ingestion_sp = _resolve_sp_app_id(ws, ingestion_name)
    except RuntimeError as exc:
        _log("sp_resolution_failed", error=str(exc))
        return 1

    sp_targets: list[tuple[str, str]] = [
        (hf_app_name, hf_app_sp),
        (ingestion_name, ingestion_sp),
    ]
    _log("sps_resolved", sps={label: app_id for label, app_id in sp_targets})

    # Resolve database project ID
    try:
        project_id = _resolve_database_project_id(headers, host)
    except Exception as exc:
        _log("project_resolution_failed", error=str(exc))
        return 1
    _log("project_resolved", project_id=project_id)

    # --status: read-only, then exit
    if args.status:
        _show_status(host, headers, project_id, sp_targets)
        return 0

    revoke = args.revoke

    # 1. Database project grants
    project_path = f"/api/2.0/permissions/database-projects/{project_id}"
    project_results: list[tuple[str, bool, str]] = []
    for sp_label, sp_app_id in sp_targets:
        action_level = None if revoke else DATABASE_PROJECT_PERMISSION
        if args.dry_run:
            _log(
                "would_apply",
                target="database-project",
                sp_label=sp_label,
                sp_app_id=sp_app_id,
                permission=action_level or "REVOKE",
            )
            project_results.append((sp_label, True, "dry-run"))
        else:
            ok, msg = _patch_acl(host, headers, project_path, sp_app_id, action_level)
            _log(
                "project_grant",
                sp_label=sp_label,
                permission=action_level or "REVOKE",
                ok=ok,
                detail=msg,
            )
            project_results.append((sp_label, ok, msg))

    # 2. Resolve all pipeline IDs
    pipelines = _enumerate_pipeline_ids(host, headers)
    _log("pipelines_resolved", count=len(pipelines))

    # 3. Pipeline grants for each (sp, pipeline) pair
    t0 = time.monotonic()
    pipeline_results: list[tuple[str, str, bool, str]] = []
    for table_name, _schema, pipeline_id in pipelines:
        pipeline_path = f"/api/2.0/permissions/pipelines/{pipeline_id}"
        for sp_label, sp_app_id in sp_targets:
            action_level = None if revoke else PIPELINE_PERMISSION
            if args.dry_run:
                pipeline_results.append((table_name, sp_label, True, "dry-run"))
            else:
                ok, msg = _patch_acl(host, headers, pipeline_path, sp_app_id, action_level)
                pipeline_results.append((table_name, sp_label, ok, msg))
                if not ok:
                    _log(
                        "pipeline_grant_failed",
                        table=table_name,
                        sp_label=sp_label,
                        detail=msg,
                    )
    elapsed_s = round(time.monotonic() - t0, 2)

    # Summary
    project_failures = sum(1 for _, ok, _ in project_results if not ok)
    pipeline_failures = sum(1 for _, _, ok, _ in pipeline_results if not ok)
    total_grants = len(project_results) + len(pipeline_results)
    total_failures = project_failures + pipeline_failures

    _log(
        "complete",
        total_grants=total_grants,
        total_failures=total_failures,
        project_grants=len(project_results),
        pipeline_grants=len(pipeline_results),
        elapsed_s=elapsed_s,
        dry_run=args.dry_run,
    )
    return 0 if total_failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
