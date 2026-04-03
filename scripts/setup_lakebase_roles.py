#!/usr/bin/env python3
"""Manage Lakebase PostgreSQL roles for service principals.

Declarative role management: defines the desired PG roles and ensures they exist
on the Lakebase branch. Idempotent -- safe to re-run.

Usage:
    python scripts/setup_lakebase_roles.py [--verify] [--cleanup]

Options:
    --verify   List current roles and exit (no changes)
    --cleanup  Delete roles not in the desired state (orphaned NO_LOGIN roles)

Requires:
    - databricks-sdk >= 0.98.0 (w.postgres.create_role / list_roles / delete_role)
    - DATABRICKS_HOST and auth configured (PAT, OAuth, or CLI profile)
"""

from __future__ import annotations

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Role,
    RoleAuthMethod,
    RoleIdentityType,
    RoleRoleSpec,
)

# ---------------------------------------------------------------------------
# Configuration — desired PG roles
# ---------------------------------------------------------------------------

PROJECT = "projects/soccer-analytics-dev"
BRANCH = f"{PROJECT}/branches/production"

# Service principals that need Lakebase PG access.
# Map: SP application_id -> human-readable name
DESIRED_SP_ROLES: dict[str, str] = {
    # Auto-provisioned by Lakebase during synced table creation.
    # Listed here for completeness and verification.
    "be66af99-5296-4fd9-887a-c081bce38bfa": "luxury-lakehouse-ingestion-sp (auto-provisioned)",
    # OAuth M2M SP for HF Spaces Taipy app (v2).
    # Created programmatically via w.postgres.create_role().
    "1a1dbf08-df56-48de-b97a-276b2a4232d8": "luxury-lakehouse-hf-app-v2-dev",
}


# ---------------------------------------------------------------------------
# Role management
# ---------------------------------------------------------------------------


def list_roles(ws: WorkspaceClient) -> list[Role]:
    """List all PG roles on the production branch."""
    return list(ws.postgres.list_roles(BRANCH))


def verify_roles(ws: WorkspaceClient) -> None:
    """Print current roles and check against desired state."""
    roles = list_roles(ws)
    print(f"\nLakebase PG roles on {BRANCH}:")
    print(f"{'PG Role':<45} {'Auth':<25} {'Identity':<20} {'Status'}")
    print("-" * 110)

    existing_sp_roles: set[str] = set()

    for r in roles:
        pg_role = r.status.postgres_role if r.status else "?"
        auth = r.status.auth_method.value if r.status and r.status.auth_method else "?"
        identity = r.status.identity_type.value if r.status and r.status.identity_type else "?"

        # Determine status
        if identity == "SERVICE_PRINCIPAL":
            existing_sp_roles.add(pg_role)
            if pg_role in DESIRED_SP_ROLES:
                status = f"OK ({DESIRED_SP_ROLES[pg_role]})"
            else:
                status = "UNKNOWN (not in desired state)"
        elif identity == "USER":
            status = "OK (workspace user)"
        elif auth == "NO_LOGIN":
            status = "ORPHANED (candidate for cleanup)"
        else:
            status = "?"

        print(f"  {pg_role:<43} {auth:<25} {identity:<20} {status}")

    # Check for missing desired roles
    print()
    missing = set(DESIRED_SP_ROLES.keys()) - existing_sp_roles
    if missing:
        print("Missing desired SP roles:")
        for sp_id in missing:
            print(f"  {sp_id} — {DESIRED_SP_ROLES[sp_id]}")
    else:
        print("All desired SP roles are present.")


def ensure_roles(ws: WorkspaceClient) -> int:
    """Create any missing desired SP roles. Returns count of roles created."""
    roles = list_roles(ws)
    existing_sp_ids = {
        r.status.postgres_role
        for r in roles
        if r.status and r.status.identity_type == RoleIdentityType.SERVICE_PRINCIPAL
    }

    created = 0
    for sp_id, name in DESIRED_SP_ROLES.items():
        if sp_id in existing_sp_ids:
            print(f"  Role exists: {sp_id} ({name})")
            continue

        print(f"  Creating role: {sp_id} ({name})")
        try:
            ws.postgres.create_role(
                parent=BRANCH,
                role=Role(
                    spec=RoleRoleSpec(
                        auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                        identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                        postgres_role=sp_id,
                    )
                ),
            )
            print("    Created successfully")
            created += 1
        except Exception as e:
            print(f"    ERROR: {e}")

    return created


def cleanup_orphaned(ws: WorkspaceClient) -> int:
    """Delete NO_LOGIN roles (orphaned SPs with no auth). Returns count deleted."""
    roles = list_roles(ws)
    deleted = 0

    for r in roles:
        if r.status and r.status.auth_method == RoleAuthMethod.NO_LOGIN:
            pg_role = r.status.postgres_role or "?"
            print(f"  Deleting orphaned NO_LOGIN role: {pg_role} ({r.name})")
            try:
                ws.postgres.delete_role(name=r.name)
                print("    Deleted successfully")
                deleted += 1
            except Exception as e:
                print(f"    ERROR: {e}")

    if deleted == 0:
        print("  No orphaned roles found.")

    return deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Lakebase PG roles for service principals")
    parser.add_argument("--verify", action="store_true", help="List roles and exit (no changes)")
    parser.add_argument("--cleanup", action="store_true", help="Delete orphaned NO_LOGIN roles")
    args = parser.parse_args()

    ws = WorkspaceClient()

    if args.verify:
        verify_roles(ws)
        return

    print("Ensuring desired PG roles exist...")
    created = ensure_roles(ws)

    if args.cleanup:
        print("\nCleaning up orphaned roles...")
        deleted = cleanup_orphaned(ws)
        print(f"\nSummary: {created} created, {deleted} deleted")
    else:
        print(f"\nSummary: {created} created (use --cleanup to remove orphaned roles)")

    print()
    verify_roles(ws)


if __name__ == "__main__":
    main()
