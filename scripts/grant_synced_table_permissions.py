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
removes the named principals via the same endpoints with permission_level=None.

Usage:
    uv run python scripts/grant_synced_table_permissions.py                # apply grants
    uv run python scripts/grant_synced_table_permissions.py --status       # show current ACLs only
    uv run python scripts/grant_synced_table_permissions.py --dry-run      # preview, no changes
    uv run python scripts/grant_synced_table_permissions.py --revoke       # remove grants (emergency)
    uv run python scripts/grant_synced_table_permissions.py --environment prod

Auth: uses WorkspaceClient — must run as an identity with CAN_MANAGE on the
database project + IS_OWNER or CAN_MANAGE on each pipeline.

Implementation note: uses the databricks-sdk throughout (no raw HTTP). The
Permissions API identifies database projects by their short name (e.g.
'soccer-analytics-dev'), NOT by the UID returned in synced-table metadata's
effective_database_project_id — this is resolved via ws.postgres.list_projects().
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from ingestion.refresh_synced_tables import DEFAULT_CATALOG, DEFAULT_SCHEMA, SYNCED_TABLES

_LOG_SOURCE = "grant_synced_table_permissions"

DATABASE_PROJECT_PERMISSION = PermissionLevel.CAN_USE
PIPELINE_PERMISSION = PermissionLevel.CAN_RUN

HF_APP_SP_NAME_PATTERN = "luxury-lakehouse-hf-app-v2-{env}"
INGESTION_SP_NAME_PATTERN = "luxury-lakehouse-ingestion-{env}"

_PROJECT_NAME_PREFIX = "projects/"


def _log(event: str, **kwargs: object) -> None:
    """Emit a structured JSON-line log to stdout."""
    record = {"source": _LOG_SOURCE, "event": event, **kwargs}
    print(json.dumps(record, default=str), flush=True)


def _level_label(level: PermissionLevel | None) -> str:
    """Render a PermissionLevel for logs as 'CAN_USE' not 'PermissionLevel.CAN_USE'."""
    if level is None:
        return "REVOKE"
    return level.value if hasattr(level, "value") else str(level)


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


def _resolve_database_project_name(ws: WorkspaceClient, synced_table_full_name: str) -> str:
    """Resolve the short project name (e.g. 'soccer-analytics-dev').

    The Permissions API identifies database-projects by short name, NOT by the
    UID synced-table metadata exposes as effective_database_project_id. This
    helper bridges the two via ws.postgres.list_projects().

    The synced-table metadata is fetched via ws.api_client.do('GET', ...)
    rather than the typed ws.database.get_synced_database_table() because the
    current databricks-sdk (v0.x) SyncedDatabaseTable schema does not expose
    effective_database_project_id, even though the underlying API returns it.

    Raises RuntimeError if:
      - the synced-table response has no effective_database_project_id, or
      - no Lakebase project has a matching uid, or
      - a matching project has an unexpected name format (lacks 'projects/' prefix).
    """
    raw = ws.api_client.do("GET", f"/api/2.0/database/synced_tables/{synced_table_full_name}")
    uid = raw.get("effective_database_project_id") if isinstance(raw, dict) else None
    if not uid:
        msg = f"Synced table {synced_table_full_name} has no effective_database_project_id"
        raise RuntimeError(msg)
    for project in ws.postgres.list_projects():
        if project.uid == uid:
            if not project.name or not project.name.startswith(_PROJECT_NAME_PREFIX):
                msg = f"Project uid={uid!r} has unexpected name {project.name!r} (expected 'projects/<slug>')"
                raise RuntimeError(msg)
            return project.name[len(_PROJECT_NAME_PREFIX) :]
    msg = f"No Lakebase project has uid={uid!r}; cannot resolve permissions-API short name"
    raise RuntimeError(msg)


def _enumerate_pipelines(ws: WorkspaceClient) -> list[tuple[str, str, str]]:
    """Resolve all synced tables' backing pipeline_ids via the SDK.

    Returns list of (table_name, schema, pipeline_id). Raises on resolve
    failure — no silent drops.
    """
    resolved: list[tuple[str, str, str]] = []
    for table_name, schema_override in SYNCED_TABLES:
        schema = schema_override or DEFAULT_SCHEMA
        full = f"{DEFAULT_CATALOG}.{schema}.{table_name}"
        meta = ws.database.get_synced_database_table(full)
        dss = getattr(meta, "data_synchronization_status", None)
        pid = getattr(dss, "pipeline_id", None) if dss else None
        if not pid:
            msg = f"Synced table {full} has no pipeline_id in data_synchronization_status"
            raise RuntimeError(msg)
        resolved.append((table_name, schema, pid))
    return resolved


def _principal_name(entry: object) -> str:
    return (
        getattr(entry, "user_name", None)
        or getattr(entry, "group_name", None)
        or getattr(entry, "service_principal_name", None)
        or "?"
    )


def _permission_levels(entry: object) -> list[str]:
    out: list[str] = []
    for p in getattr(entry, "all_permissions", None) or []:
        level = getattr(p, "permission_level", None)
        if level is None:
            continue
        out.append(level.value if hasattr(level, "value") else str(level))
    return out


def _show_project_acl(ws: WorkspaceClient, project_name: str, sp_app_ids: set[tuple[str, str]]) -> None:
    """Fetch and log the current ACL on the database project.

    Raises on SDK failure — no silent swallow. Callers that want best-effort
    behavior must wrap with a specific try/except at the call site.
    """
    acl = ws.permissions.get(request_object_type="database-projects", request_object_id=project_name)
    target_set = {app_id for _, app_id in sp_app_ids}
    for entry in getattr(acl, "access_control_list", None) or []:
        principal = _principal_name(entry)
        _log(
            "project_acl_entry",
            principal=principal,
            permissions=_permission_levels(entry),
            is_target_sp=principal in target_set,
        )


def _show_pipeline_acl(ws: WorkspaceClient, table: str, pipeline_id: str) -> None:
    acl = ws.permissions.get(request_object_type="pipelines", request_object_id=pipeline_id)
    for entry in getattr(acl, "access_control_list", None) or []:
        _log(
            "pipeline_acl_entry",
            table=table,
            pipeline_id=pipeline_id,
            principal=_principal_name(entry),
            permissions=_permission_levels(entry),
        )


def _patch_acl(
    ws: WorkspaceClient,
    object_type: str,
    object_id: str,
    sp_app_id: str,
    level: PermissionLevel | None,
) -> None:
    """Apply an additive ACL patch for a single principal.

    Uses ws.permissions.update (additive — patches the named principal in while
    leaving other principals untouched). ws.permissions.set would replace the
    entire ACL, which would wipe the admins group and the workspace users
    defaults.

    level=None means revoke (remove the named principal from the ACL).
    """
    acr = AccessControlRequest(service_principal_name=sp_app_id, permission_level=level)
    ws.permissions.update(
        request_object_type=object_type,
        request_object_id=object_id,
        access_control_list=[acr],
    )


def _apply_grants(
    ws: WorkspaceClient,
    project_name: str,
    pipelines: list[tuple[str, str, str]],
    sp_targets: Iterable[tuple[str, str]],
    *,
    revoke: bool,
    dry_run: bool,
) -> int:
    """Apply (or revoke) the full grant set. Returns number of operations."""
    project_level: PermissionLevel | None = None if revoke else DATABASE_PROJECT_PERMISSION
    pipeline_level: PermissionLevel | None = None if revoke else PIPELINE_PERMISSION
    total = 0
    for sp_label, sp_app_id in sp_targets:
        if dry_run:
            _log(
                "would_apply",
                target="database-project",
                sp_label=sp_label,
                sp_app_id=sp_app_id,
                permission=_level_label(project_level),
            )
        else:
            _patch_acl(ws, "database-projects", project_name, sp_app_id, project_level)
            _log("project_grant", sp_label=sp_label, permission=_level_label(project_level))
        total += 1
    for table, _schema, pid in pipelines:
        for sp_label, sp_app_id in sp_targets:
            if dry_run:
                _log(
                    "would_apply",
                    target="pipeline",
                    table=table,
                    sp_label=sp_label,
                    sp_app_id=sp_app_id,
                    permission=_level_label(pipeline_level),
                )
            else:
                _patch_acl(ws, "pipelines", pid, sp_app_id, pipeline_level)
            total += 1
    return total


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

    if not (args.grant or args.revoke or args.status):
        args.grant = True

    ws = WorkspaceClient()
    mode = "status" if args.status else ("revoke" if args.revoke else "grant")
    _log("start", environment=args.environment, mode=mode, dry_run=args.dry_run)

    hf_app_name = HF_APP_SP_NAME_PATTERN.format(env=args.environment)
    ingestion_name = INGESTION_SP_NAME_PATTERN.format(env=args.environment)
    hf_app_sp = _resolve_sp_app_id(ws, hf_app_name)
    ingestion_sp = _resolve_sp_app_id(ws, ingestion_name)
    sp_targets: set[tuple[str, str]] = {(hf_app_name, hf_app_sp), (ingestion_name, ingestion_sp)}
    _log("sps_resolved", sps={label: app_id for label, app_id in sp_targets})

    sample_full = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{SYNCED_TABLES[0][0]}"
    project_name = _resolve_database_project_name(ws, sample_full)
    _log("project_resolved", project_name=project_name)

    if args.status:
        _show_project_acl(ws, project_name, sp_targets)
        pipelines = _enumerate_pipelines(ws)
        # Sample one pipeline to avoid dumping 34 ACLs.
        if pipelines:
            table, _schema, pid = pipelines[0]
            _show_pipeline_acl(ws, table, pid)
        return 0

    pipelines = _enumerate_pipelines(ws)
    _log("pipelines_resolved", count=len(pipelines))

    t0 = time.monotonic()
    total = _apply_grants(ws, project_name, pipelines, sp_targets, revoke=args.revoke, dry_run=args.dry_run)
    elapsed_s = round(time.monotonic() - t0, 2)
    _log("complete", total_grants=total, elapsed_s=elapsed_s, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
