#!/usr/bin/env python3
"""PR-LL2 bronze migration: idempotent ALTER TABLE ADD COLUMNS for 4 tables.

Adds the LL2 + LL2 Path B columns to four bronze tables on Databricks Delta.
Pattern lifted from ``scripts/maintain_synced_tables.py`` (DESCRIBE TABLE
to find missing columns, then ALTER TABLE ADD COLUMNS only for the missing
subset). Safe to re-run — idempotent at the application layer (Databricks
serverless Delta does NOT support ``ADD COLUMN IF NOT EXISTS``).

Usage:
    uv run python scripts/migrate_bronze_for_pr_ll2.py            # apply
    uv run python scripts/migrate_bronze_for_pr_ll2.py --dry-run  # preview SQL only

Runs against the dev catalog/schema by default (override with --catalog/--schema).
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sql-connector>=4.0.0"]
# ///

from __future__ import annotations

import argparse
import logging
import os
import sys

from databricks import sql

logger = logging.getLogger("migrate_bronze_for_pr_ll2")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Target schemas — column-name → DDL-type mapping for each table.
# ---------------------------------------------------------------------------

# LL1 (silly-kicks 1.5.0+ preserve_native) added these to bronze.vaep_action_values
# but the corresponding ALTER on bronze.spadl_actions was missed — closing here.
_LL1_STATSBOMB_NATIVE_COLS: dict[str, str] = {
    "statsbomb_possession_id": "BIGINT",
    "statsbomb_possession_team_id": "BIGINT",
    "statsbomb_play_pattern": "STRING",
    "statsbomb_under_pressure": "BOOLEAN",
}

# LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments
# (silly-kicks 1.4.0+ helpers) + ``action_id`` surfacing.
_LL2_ENRICHMENT_COLS: dict[str, str] = {
    "possession_id_heuristic": "BIGINT",
    "gk_role": "STRING",
    "gk_was_distributing": "BOOLEAN",
    "gk_was_engaged": "BOOLEAN",
    "gk_actions_in_possession": "BIGINT",
    "defending_gk_player_id": "BIGINT",
}
_LL2_ACTION_ID_COL: dict[str, str] = {"action_id": "BIGINT"}

# LL2 Path B: native (string) provider identifiers for Kimball-aligned joins.
_LL2_PATH_B_SPADL_COLS: dict[str, str] = {
    "team_id_native": "STRING",
    "home_team_id_native": "STRING",
    "competition_native_id": "STRING",
    "season_native_id": "STRING",
    "match_id_native": "STRING",
}

# LL2 Path B for bronze events tables (idsse + metrica). away_team_id_native
# is exposed on these but NOT on spadl_actions (which only carries the acting
# team's id_native).
_LL2_PATH_B_EVENTS_COLS: dict[str, str] = {
    "competition_native_id": "STRING",
    "season_native_id": "STRING",
    "home_team_id_native": "STRING",
    "away_team_id_native": "STRING",
    "team_id_native": "STRING",
}


def _spadl_actions_target() -> dict[str, str]:
    """Target columns for bronze.spadl_actions (LL1 backfill + LL2 + Path B)."""
    return {
        **_LL1_STATSBOMB_NATIVE_COLS,
        **_LL2_ENRICHMENT_COLS,
        **_LL2_PATH_B_SPADL_COLS,
    }


def _vaep_action_values_target() -> dict[str, str]:
    """Target columns for bronze.vaep_action_values (LL2 + Path B; LL1 statsbomb_*
    cols already added by the PR-LL1 ALTER but kept here for idempotency safety)."""
    return {
        **_LL1_STATSBOMB_NATIVE_COLS,
        **_LL2_ACTION_ID_COL,
        **_LL2_ENRICHMENT_COLS,
        **_LL2_PATH_B_SPADL_COLS,
    }


def _idsse_events_target() -> dict[str, str]:
    """Target columns for bronze.idsse_events (LL2 Path B)."""
    return dict(_LL2_PATH_B_EVENTS_COLS)


def _metrica_events_target() -> dict[str, str]:
    """Target columns for bronze.metrica_events (LL2 Path B)."""
    return dict(_LL2_PATH_B_EVENTS_COLS)


_TARGETS: dict[str, dict[str, str]] = {
    "spadl_actions": _spadl_actions_target(),
    "vaep_action_values": _vaep_action_values_target(),
    "idsse_events": _idsse_events_target(),
    "metrica_events": _metrica_events_target(),
}


# ---------------------------------------------------------------------------
# Connection + ALTER logic
# ---------------------------------------------------------------------------


def _connect():  # type: ignore[no-untyped-def]
    """Open a Databricks SQL connection. MSYS double-slash on HTTP_PATH stripped."""
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    if http_path.startswith("//"):
        http_path = http_path[1:]
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _describe_columns(cur, fq_table: str) -> set[str]:  # type: ignore[no-untyped-def]
    """Return the set of column names present on the given fully-qualified Delta table."""
    cur.execute(f"DESCRIBE TABLE {fq_table}")
    cols: set[str] = set()
    for row in cur.fetchall():
        col_name = row[0]
        # DESCRIBE TABLE output starts with column rows then has metadata rows
        # like '# Partition Information'. Stop at the first '#'-prefixed row.
        if col_name.startswith("#") or not col_name:
            break
        cols.add(col_name)
    return cols


def _alter_add_columns(cur, fq_table: str, missing_cols: dict[str, str], dry_run: bool) -> None:  # type: ignore[no-untyped-def]
    """Emit one ALTER TABLE ADD COLUMNS for the missing subset."""
    if not missing_cols:
        return
    # Quoting columns/types is unnecessary for valid Spark identifiers; we
    # control both sides of the f-string. Bandit S608 false positive.
    spec = ", ".join(f"{col} {ddl_type}" for col, ddl_type in missing_cols.items())
    sql_text = f"ALTER TABLE {fq_table} ADD COLUMNS ({spec})"
    logger.info("SQL: %s", sql_text)
    if dry_run:
        return
    cur.execute(sql_text)


def _migrate_table(cur, catalog: str, schema: str, table: str, target: dict[str, str], dry_run: bool) -> bool:  # type: ignore[no-untyped-def]
    """Apply the LL2 ALTER for one bronze table.

    Returns True if any columns were added (or would be in dry-run mode).
    """
    fq = f"{catalog}.{schema}.{table}"
    try:
        existing = _describe_columns(cur, fq)
    except Exception as exc:  # noqa: BLE001 — Databricks SQL connector raises various
        # provider-specific exceptions (TABLE_OR_VIEW_NOT_FOUND, transient connection
        # errors, permission denied) that we want to skip+log so the migration
        # continues with other tables. The script is idempotent and re-runnable.
        logger.warning("Could not DESCRIBE %s: %s — skipping", fq, exc)
        return False

    missing = {col: ddl_type for col, ddl_type in target.items() if col not in existing}
    if not missing:
        logger.info("%s — already at target schema (no missing LL2 cols)", fq)
        return False

    logger.info("%s — adding %d missing columns: %s", fq, len(missing), sorted(missing))
    _alter_add_columns(cur, fq, missing, dry_run)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="PR-LL2 bronze ALTER migration (idempotent)")
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog name")
    parser.add_argument("--schema", default="bronze", help="Schema name (default: bronze)")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    logger.info(
        "PR-LL2 bronze migration: catalog=%s schema=%s dry_run=%s",
        args.catalog,
        args.schema,
        args.dry_run,
    )

    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            any_changed = False
            for table, target in _TARGETS.items():
                changed = _migrate_table(cur, args.catalog, args.schema, table, target, args.dry_run)
                any_changed = any_changed or changed
            if not any_changed:
                logger.info("All four target tables already at LL2 target schema — no-op")
        finally:
            cur.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
