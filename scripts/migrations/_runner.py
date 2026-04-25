"""Reusable idempotent migration executor for scripts/migrations/*.sql.

Kimball PR 5a helper. Handles:
  - Comment-only lines (stripped before split)
  - Multi-statement files (split on ';')
  - Databricks's lack of ``ADD COLUMN IF NOT EXISTS`` → skip ALTER TABLE ADD COLUMNS
    when the target column already exists (DESCRIBE pre-check)

Usage:
    uv run --with databricks-sql-connector python scripts/migrations/_runner.py \
        scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import sys

_ADD_RE = re.compile(
    r"ALTER\s+TABLE\s+(\S+)\s+ADD\s+COLUMNS\s*\(\s*(\w+)\s+\w+",
    re.IGNORECASE,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("migration", type=pathlib.Path)
    args = parser.parse_args()

    from databricks import sql

    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cur = conn.cursor()

    def col_exists(table: str, col: str) -> bool:
        cur.execute(f"DESCRIBE TABLE {table}")
        return col in {r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")}

    raw = args.migration.read_text()
    code_only = "\n".join(ln for ln in raw.splitlines() if not ln.lstrip().startswith("--"))
    for stmt in code_only.split(";"):
        s = stmt.strip()
        if not s:
            continue
        head = s.splitlines()[0][:100]
        m = _ADD_RE.match(s)
        if m:
            table, col = m.group(1), m.group(2)
            if col_exists(table, col):
                print(f"Skipping (col exists): {head}")
                continue
        print(f"Executing: {head}")
        cur.execute(s)
        print("  OK")
    conn.close()
    print("Migration complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
