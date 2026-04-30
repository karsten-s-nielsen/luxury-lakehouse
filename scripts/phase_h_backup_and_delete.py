#!/usr/bin/env python3
"""Phase H: DEEP CLONE backups + DESTRUCTIVE DELETE for PR-LL2 Path B close-out.

Steps:
  1. DEEP CLONE 4 bronze tables to *_pre_close_out_backup (24h retention by VACUUM policy)
  2. DESTRUCTIVE: DELETE all rows from bronze.idsse_events + bronze.metrica_events
  3. DESTRUCTIVE: DELETE wyscout/idsse/metrica rows from bronze.spadl_actions + bronze.vaep_action_values

Safe to re-run for backup creation (uses CREATE TABLE IF NOT EXISTS via DEEP CLONE
re-execution, which Databricks handles by failing if table exists — we catch).
DELETE phase is gated behind --apply-deletes flag.

Usage:
    uv run --with databricks-sql-connector python scripts/phase_h_backup_and_delete.py             # backup only
    uv run --with databricks-sql-connector python scripts/phase_h_backup_and_delete.py --apply-deletes  # + DELETE
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

logger = logging.getLogger("phase_h")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")

CATALOG = "soccer_analytics"
BRONZE = "bronze"

BACKUP_TABLES = [
    "idsse_events",
    "metrica_events",
    "spadl_actions",
    "vaep_action_values",
]

# Per-line `noqa: S608` below is a false-positive suppression — CATALOG and BRONZE
# are hardcoded module-level constants (`'soccer_analytics'` and `'bronze'`), not
# user input. SQL injection is not possible.
DELETE_STATEMENTS = [
    f"DELETE FROM {CATALOG}.{BRONZE}.idsse_events",  # noqa: S608
    f"DELETE FROM {CATALOG}.{BRONZE}.metrica_events",  # noqa: S608
    f"DELETE FROM {CATALOG}.{BRONZE}.spadl_actions WHERE data_source IN ('idsse', 'metrica', 'wyscout')",  # noqa: S608
    f"DELETE FROM {CATALOG}.{BRONZE}.vaep_action_values WHERE data_source IN ('idsse', 'metrica', 'wyscout')",  # noqa: S608
]


def _connect():  # type: ignore[no-untyped-def]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    if http_path.startswith("//"):
        http_path = http_path[1:]
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply-deletes", action="store_true", help="Also run the DESTRUCTIVE DELETE statements")
    args = parser.parse_args()

    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            # Phase H.1: pre-state row counts
            logger.info("=== Pre-state row counts ===")
            for t in BACKUP_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {CATALOG}.{BRONZE}.{t}")  # noqa: S608
                logger.info("  %s: %d rows", t, cur.fetchone()[0])

            # Phase H.2: DEEP CLONE backups
            logger.info("=== DEEP CLONE backups ===")
            for t in BACKUP_TABLES:
                bkp = f"{CATALOG}.{BRONZE}.{t}_pre_close_out_backup"
                src = f"{CATALOG}.{BRONZE}.{t}"
                try:
                    cur.execute(f"CREATE TABLE {bkp} DEEP CLONE {src}")
                    logger.info("  CLONED %s -> %s", src, bkp)
                except Exception as exc:
                    if "TABLE_OR_VIEW_ALREADY_EXISTS" in str(exc) or "ALREADY EXISTS" in str(exc).upper():
                        logger.info("  %s already exists — skipping (idempotent)", bkp)
                    else:
                        raise

            # Phase H.3: verify backups
            logger.info("=== Backup verification ===")
            for t in BACKUP_TABLES:
                bkp = f"{CATALOG}.{BRONZE}.{t}_pre_close_out_backup"
                cur.execute(f"SELECT COUNT(*) FROM {bkp}")  # noqa: S608
                logger.info("  %s: %d rows", bkp, cur.fetchone()[0])

            if not args.apply_deletes:
                logger.info("=== Backups complete; --apply-deletes NOT set, exiting before DELETE ===")
                return 0

            # Phase H.4: DESTRUCTIVE DELETE
            logger.info("=== DESTRUCTIVE DELETE (gated by --apply-deletes) ===")
            for stmt in DELETE_STATEMENTS:
                logger.info("SQL: %s", stmt)
                cur.execute(stmt)
                logger.info("  (committed)")

            # Phase H.5: post-DELETE row counts
            logger.info("=== Post-DELETE row counts ===")
            for t in BACKUP_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {CATALOG}.{BRONZE}.{t}")  # noqa: S608
                logger.info("  %s: %d rows (was the DELETE-target)", t, cur.fetchone()[0])

        finally:
            cur.close()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
