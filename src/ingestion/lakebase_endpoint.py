"""Resolve the Lakebase PostgreSQL endpoint DNS — shared host-derivation seam (ADR-041).

The Lakebase maintenance scripts (`scripts/create_indexes.py`, `scripts/run_lakebase_grants.py`)
discover the endpoint DNS from the Databricks REST API so CI needs no hand-set `LAKEBASE_HOST`
variable; the `LAKEBASE_HOST` env var is honoured ONLY as a local-dev override. This module is the
importable home of that contract, so the synced-table heal (`heal_synced_tables`) and the operator
delete (`scripts/delete_synced_table.py`) resolve the host the same way instead of hard-requiring the
env var — the inconsistency that broke the heal e2e config-gate. The two scripts above predate this
helper and keep their equivalent private copies; they can adopt this seam in a later refactor.

Import-safe offline: no module-level credential reads; the SDK client is injected.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

DEFAULT_ENDPOINT_NAME = "projects/soccer-analytics-dev/branches/production/endpoints/primary"


def _endpoint_dns(ep: dict[str, Any]) -> str | None:
    """Extract ``status.hosts.host`` (the public DNS) from one endpoint payload, or ``None``."""
    status = ep.get("status")
    if isinstance(status, dict):
        hosts = status.get("hosts")
        if isinstance(hosts, dict):
            val = hosts.get("host")
            if isinstance(val, str) and val:
                return val
    return None


def derive_lakebase_dns(
    ws: WorkspaceClient,
    *,
    endpoint_name: str = DEFAULT_ENDPOINT_NAME,
    override: str | None = None,
) -> str:
    """Return the Lakebase endpoint DNS.

    ``override`` (falling back to the ``LAKEBASE_HOST`` env var) wins for local dev. Otherwise the DNS
    is discovered via ``GET /api/2.0/postgres/<project>/endpoints`` and matched to ``endpoint_name``.
    Mirrors ``scripts/create_indexes.py:_get_lakebase_dns`` so CI requires no hand-set host var.
    """
    resolved = override if override is not None else os.environ.get("LAKEBASE_HOST")
    if resolved:
        return resolved

    host = (ws.config.host or "").rstrip("/")
    headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]
    project_path = endpoint_name.rsplit("/endpoints/", 1)[0]
    resp = requests.get(
        f"{host}/api/2.0/postgres/{project_path}/endpoints",
        headers=headers,
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    endpoints = resp.json().get("endpoints", [])
    if not endpoints:
        raise RuntimeError(f"No Lakebase endpoints found under {project_path}")

    # Common case — single endpoint per project in this setup.
    if len(endpoints) == 1:
        dns = _endpoint_dns(endpoints[0])
        if dns:
            return dns

    # Multi-endpoint — match by the suffix of endpoint_name.
    suffix = endpoint_name.rsplit("/endpoints/", 1)[1]
    for ep in endpoints:
        name = ep.get("name", "")
        if isinstance(name, str) and (name.endswith(f"/endpoints/{suffix}") or name == suffix):
            dns = _endpoint_dns(ep)
            if dns:
                return dns
    raise RuntimeError(f"Lakebase endpoint '{suffix}' not found among {len(endpoints)} endpoints under {project_path}")
