#!/usr/bin/env python3
"""Manage Lakebase PostgreSQL roles for service principals.

Declarative role management: defines the desired PG roles (``DESIRED_SP_ROLES``)
and ensures they exist on the Lakebase branch. Idempotent -- safe to re-run.

This is a one-time fresh-install step AND the fix path when a service principal
cannot authenticate to Lakebase (``psycopg2 ... password authentication failed
for user '<app-id>'`` means the SP has no PG role here yet).

Two privilege tiers (see ``DesiredRole.superuser``):
    - plain grantee (default): the Taipy app SP, which only receives SELECT.
    - ``superuser=True``: the CI OIDC SP (terraform_ci), created as a member of
      ``databricks_superuser`` so ``lakebase-grants.yml`` can run GRANT /
      ALTER DEFAULT PRIVILEGES + ``connect_as_superuser()``. This replaces the
      retired admin PAT's superuser (workspace PATs were retired 2026-07-21;
      see ADR-071). Must be run once by an existing superuser (a workspace admin
      -- ``current_user`` is a ``databricks_superuser`` member).

Usage:
    python scripts/setup_lakebase_roles.py [--verify] [--cleanup]

Options:
    --verify   List current roles and exit (no changes)
    --cleanup  Delete roles not in the desired state (orphaned NO_LOGIN roles)

Requires:
    - databricks-sdk >= 0.98.0 (w.postgres.create_role / list_roles / delete_role)
    - DATABRICKS_HOST and OAuth auth configured. PATs were retired 2026-07-21;
      authenticate as a workspace admin via the OAuth CLI profile:
      ``databricks auth login --profile OAUTH`` then run with that profile
      (e.g. export a bearer via ``Config(profile="OAUTH")`` as DATABRICKS_TOKEN,
      or set DATABRICKS_CONFIG_PROFILE=OAUTH).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ingestion.databricks_auth import workspace_client

# PR-Cycle-B (2026-05-01): databricks-sdk is in the [sdk] optional extra.
# Lazy-import keeps this module importable without the extra installed.
if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.postgres import (
        Role,
        RoleAuthMethod,
        RoleIdentityType,
        RoleMembershipRole,
        RoleRoleSpec,
    )
else:
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.postgres import (
            Role,
            RoleAuthMethod,
            RoleIdentityType,
            RoleMembershipRole,
            RoleRoleSpec,
        )
    except ImportError:
        WorkspaceClient = None  # type: ignore[assignment, misc]
        Role = None  # type: ignore[assignment, misc]
        RoleAuthMethod = None  # type: ignore[assignment, misc]
        RoleIdentityType = None  # type: ignore[assignment, misc]
        RoleMembershipRole = None  # type: ignore[assignment, misc]
        RoleRoleSpec = None  # type: ignore[assignment, misc]

# ---------------------------------------------------------------------------
# Configuration — desired PG roles
# ---------------------------------------------------------------------------

PROJECT = "projects/soccer-analytics-dev"
BRANCH = f"{PROJECT}/branches/production"


@dataclass(frozen=True)
class DesiredRole:
    """A desired Lakebase PG role for a Databricks service principal.

    ``superuser=True`` creates the role as a member of ``databricks_superuser``
    (via ``RoleMembershipRole.DATABRICKS_SUPERUSER``). That membership is what
    grants ``CREATE ROLE`` / ``GRANT`` capability — the same privilege a human
    workspace admin has (``current_user`` is a ``databricks_superuser`` member).
    Only identities that must *administer* PG (run GRANT / connect_as_superuser)
    need it; plain grantees (the Taipy app SP) must NOT have it (least privilege).
    """

    name: str
    superuser: bool = False


# Service principals that need Lakebase PG access.
# NOTE: these application_ids are for the dev deployment. A fresh install in a
# different workspace resolves them from terraform outputs (see the comment on
# each entry) and updates this map accordingly.
DESIRED_SP_ROLES: dict[str, DesiredRole] = {
    # Auto-provisioned by Lakebase during synced table creation.
    # Listed here for completeness and verification.
    "be66af99-5296-4fd9-887a-c081bce38bfa": DesiredRole("luxury-lakehouse-ingestion-sp (auto-provisioned)"),
    # OAuth M2M SP for HF Spaces Taipy app (v2). Plain grantee of SELECT — NOT a superuser.
    # Created programmatically via w.postgres.create_role().
    "1a1dbf08-df56-48de-b97a-276b2a4232d8": DesiredRole("luxury-lakehouse-hf-app-v2-dev"),
    # CI OIDC SP = `terraform output -raw terraform_ci_sp_application_id`
    # (also GitHub repo var DATABRICKS_CLIENT_ID). lakebase-grants.yml authenticates
    # as this SP via GitHub OIDC and runs GRANT / ALTER DEFAULT PRIVILEGES +
    # connect_as_superuser(), which require databricks_superuser membership. This is
    # parity with the retired admin PAT (whose human owner is a superuser). See ADR-071.
    "521f5d6a-cfd4-4fe1-a5cb-d5b12e247276": DesiredRole(
        "luxury-lakehouse-terraform-ci-dev (CI OIDC — Lakebase superuser for grants)",
        superuser=True,
    ),
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
        pg_role = r.status.postgres_role if (r.status and r.status.postgres_role) else "?"
        auth = r.status.auth_method.value if r.status and r.status.auth_method else "?"
        identity = r.status.identity_type.value if r.status and r.status.identity_type else "?"

        # Determine status
        if identity == "SERVICE_PRINCIPAL":
            existing_sp_roles.add(pg_role)
            if pg_role in DESIRED_SP_ROLES:
                status = f"OK ({DESIRED_SP_ROLES[pg_role].name})"
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
            desired = DESIRED_SP_ROLES[sp_id]
            suffix = " [superuser]" if desired.superuser else ""
            print(f"  {sp_id} — {desired.name}{suffix}")
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
    for sp_id, desired in DESIRED_SP_ROLES.items():
        if sp_id in existing_sp_ids:
            # NOTE: create_role is skip-if-exists, so a role first created WITHOUT
            # superuser is not retroactively promoted here. To (re)grant superuser to
            # an existing role, delete it (--cleanup won't touch a live-login role) and
            # re-run, or grant membership out-of-band. Fresh installs create it correctly.
            suffix = " [superuser]" if desired.superuser else ""
            print(f"  Role exists: {sp_id} ({desired.name}){suffix}")
            continue

        membership = [RoleMembershipRole.DATABRICKS_SUPERUSER] if desired.superuser else None
        suffix = " as databricks_superuser member" if desired.superuser else ""
        print(f"  Creating role: {sp_id} ({desired.name}){suffix}")
        try:
            ws.postgres.create_role(
                parent=BRANCH,
                role=Role(
                    spec=RoleRoleSpec(
                        auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                        identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                        postgres_role=sp_id,
                        membership_roles=membership,
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
            if r.name is None:
                print(f"  Skipping orphaned role with no name: {pg_role}")
                continue
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

    ws = workspace_client()

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
