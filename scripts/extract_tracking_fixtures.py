"""Extract bronze tracking fixtures from Databricks for local integration tests.

One-time script — run manually when fixtures need updating.
Requires DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID env vars.

Extracts:
  - IDSSE J03WMX: period 1, first 120s of tracking + all actions + events
  - Metrica Sample_Game_1: full tracking + actions
  - Metrica Sample_Game_3: full tracking + actions (has "Player 22" space bug)
  - IDSSE J03WN1: actions only (has null team_id_native)

Usage:
    uv run python scripts/extract_tracking_fixtures.py
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

FIXTURE_DIR = Path("src/tests/fixtures/tracking_context")

# Maximum rows per query — Databricks SQL Statement API limit.
# IDSSE tracking is wide (16 cols) so stay conservative.
_MAX_ROWS_PER_CHUNK = 80_000

# ── Fixture definitions ──────────────────────────────────────────────
# Each entry: (filename, SQL query, description)
FIXTURES: list[tuple[str, str, str]] = [
    # --- IDSSE J03WMX ---
    (
        "idsse_J03WMX_p1_tracking.parquet",
        """
        SELECT match_id, period, frame, timestamp, x, y, s, ball_status,
               frame_rate, player_id, team_id, is_goalkeeper,
               ball_x, ball_y, ball_z, ball_s
        FROM soccer_analytics.bronze.idsse_tracking
        WHERE match_id = 'J03WMX' AND period = 1 AND timestamp <= 120.0
        ORDER BY frame, player_id
        """,
        "IDSSE tracking: J03WMX period 1, first 120s (~50K rows)",
    ),
    (
        "idsse_J03WMX_actions.parquet",
        """
        SELECT *
        FROM soccer_analytics.bronze.spadl_actions
        WHERE match_id_native = 'J03WMX' AND data_source = 'idsse'
        ORDER BY period_id, action_id
        """,
        "IDSSE SPADL actions: J03WMX (all periods, ~500 rows)",
    ),
    (
        "idsse_J03WMX_events.parquet",
        """
        SELECT *
        FROM soccer_analytics.bronze.idsse_events
        WHERE match_id = 'J03WMX'
        """,
        "IDSSE events: J03WMX (for home_team_id resolution)",
    ),
    # --- Metrica Sample_Game_1 ---
    (
        "metrica_game1_tracking.parquet",
        """
        SELECT match_id, period, frame, timestamp, frame_rate,
               gk_jersey_numbers, home_players, away_players,
               ball_x, ball_y
        FROM soccer_analytics.bronze.metrica_tracking
        WHERE match_id = 'Sample_Game_1' AND period = 1 AND timestamp <= 300.0
        ORDER BY period, frame
        """,
        "Metrica tracking: Sample_Game_1 period 1, first 5 min",
    ),
    (
        "metrica_game1_actions.parquet",
        """
        SELECT *
        FROM soccer_analytics.bronze.spadl_actions
        WHERE match_id_native = 'Sample_Game_1' AND data_source = 'metrica'
        ORDER BY period_id, action_id
        """,
        "Metrica SPADL actions: Sample_Game_1 (~1100 rows)",
    ),
    # --- Metrica Sample_Game_3 ---
    (
        "metrica_game3_tracking.parquet",
        """
        SELECT match_id, period, frame, timestamp, frame_rate,
               gk_jersey_numbers, home_players, away_players,
               ball_x, ball_y
        FROM soccer_analytics.bronze.metrica_tracking
        WHERE match_id = 'Sample_Game_3' AND period = 1 AND timestamp <= 300.0
        ORDER BY period, frame
        """,
        "Metrica tracking: Sample_Game_3 period 1, first 5 min (has 'Player 22' with space)",
    ),
    (
        "metrica_game3_actions.parquet",
        """
        SELECT *
        FROM soccer_analytics.bronze.spadl_actions
        WHERE match_id_native = 'Sample_Game_3' AND data_source = 'metrica'
        ORDER BY period_id, action_id
        """,
        "Metrica SPADL actions: Sample_Game_3 (~1100 rows)",
    ),
    # --- IDSSE J03WN1 (null-team bug) ---
    (
        "idsse_J03WN1_actions.parquet",
        """
        SELECT *
        FROM soccer_analytics.bronze.spadl_actions
        WHERE match_id_native = 'J03WN1' AND data_source = 'idsse'
        ORDER BY period_id, action_id
        """,
        "IDSSE SPADL actions: J03WN1 (has null team_id_native — Bug #8)",
    ),
]


def _execute_query_to_df(sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks SDK and return as DataFrame."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import Disposition, Format

    w = WorkspaceClient()

    logger.info("Executing: %s", sql.strip()[:100])
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql.strip(),
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    # Poll if PENDING
    while result.status and result.status.state and result.status.state.value in ("PENDING", "RUNNING"):
        logger.info("  ... waiting (state=%s)", result.status.state.value)
        time.sleep(3)
        result = w.statement_execution.get_statement(result.statement_id)

    if result.status and result.status.state and result.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"Query failed: {result.status}")

    # Build DataFrame from result
    columns = [col.name for col in result.manifest.schema.columns]
    rows = result.result.data_array if result.result and result.result.data_array else []
    df = pd.DataFrame(rows, columns=columns)

    # Type coercion from string arrays
    for col_meta in result.manifest.schema.columns:
        col_name = col_meta.name
        type_name = col_meta.type_text or ""
        if type_name in ("BIGINT", "INT", "LONG"):
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        elif type_name in ("DOUBLE", "FLOAT", "DECIMAL"):
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        elif type_name == "BOOLEAN":
            df[col_name] = df[col_name].map({"true": True, "false": False, None: None}).astype("boolean")
        elif type_name == "TIMESTAMP":
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce")

    return df


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Verify env vars
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_SQL_WAREHOUSE_ID"):
        if not os.environ.get(var):
            raise SystemExit(f"Missing env var: {var}")

    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    for filename, sql, desc in FIXTURES:
        logger.info("── %s ──", desc)
        path = FIXTURE_DIR / filename
        if path.exists():
            logger.info("SKIP (already exists): %s", path)
            existing = pd.read_parquet(path)
            logger.info("  %d rows, %.1f MB", len(existing), existing.memory_usage(deep=True).sum() / 1024 / 1024)
            continue

        df = _execute_query_to_df(sql, warehouse_id)
        df.to_parquet(path, index=False)
        mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info("Wrote %d rows (%.1f MB) to %s", len(df), mem_mb, path)
        total += len(df)

    logger.info("Done. %d total rows extracted.", total)

    # Summary
    logger.info("\n── Fixture Summary ──")
    for f in sorted(FIXTURE_DIR.glob("*.parquet")):
        df = pd.read_parquet(f)
        logger.info("  %s: %d rows, cols=%s", f.name, len(df), list(df.columns)[:5])


if __name__ == "__main__":
    main()
