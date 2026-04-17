"""SEC4 item H: assert orphan PG role ``be66af99-...`` is absent from Lakebase.

This role was granted manually during the 2026-04-17 warm-tier incident and
documented in ADR-005 §Neutral (line 69) as pre-existing technical debt. The
SEC4 cycle removes it; this test guards against re-introduction.

The test connects to Lakebase via the same auth path as
``scripts/run_lakebase_grants.py`` (short-lived admin JWT). It is skipped
when Lakebase credentials are not available (CI without workspace creds,
most developer machines without DATABRICKS_HOST/TOKEN exported).

See:
- docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md
- docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md §Orphan PG role
- docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md §Neutral
"""

from __future__ import annotations

import os

import pytest

_ORPHAN_ROLE = "be66af99-5296-4fd9-887a-c081bce38bfa"


@pytest.mark.skipif(
    not os.getenv("DATABRICKS_HOST") or not os.getenv("DATABRICKS_TOKEN"),
    reason="Lakebase credentials not available (DATABRICKS_HOST / DATABRICKS_TOKEN unset)",
)
def test_orphan_pg_role_absent_from_pg_roles() -> None:
    """pg_roles must not contain the orphan UUID.

    Removed in SEC4 item H via ``REASSIGN OWNED`` + ``DROP OWNED`` + ``DROP ROLE``.
    Re-introduction would signal either (a) a manual grant being re-applied,
    or (b) a regression in the synced-table auto-provision path. Either case
    deserves investigation, not silent tolerance.
    """
    from scripts.run_lakebase_grants import connect_as_superuser

    conn = connect_as_superuser()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM pg_roles WHERE rolname = %s",
                (_ORPHAN_ROLE,),
            )
            row = cur.fetchone()
            count = row[0] if row else 0
    finally:
        conn.close()

    assert count == 0, (
        f"Orphan PG role {_ORPHAN_ROLE!r} still present in pg_roles — "
        f"SEC4 item H incomplete, or re-introduction occurred. Investigate "
        f"via scripts/run_lakebase_grants.py and ADR-005 §Neutral before "
        f"re-running the drop."
    )
