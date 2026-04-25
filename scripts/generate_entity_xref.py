#!/usr/bin/env python3
# ruff: noqa: S608 — script constructs MERGE + VIEW from internal column lists (no user input).
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sql-connector>=3.0",
#     "rapidfuzz>=3.0",
#     "pandas>=2.0",
# ]
# ///
"""Generate cross-provider entity xref rows for bronze.{player,team}_xref_raw.

PR 5a (ADR-011). Fuzzy-match player + team names across StatsBomb, Wyscout,
and IDSSE (Metrica is anonymised → unreachable by name matching). Emits rows
at confidence ≥ 70 with the source_a < source_b ordering convention.

Idempotent: Delta MERGE INTO on the unique xref grain. Re-runs update
confidences in place without duplication.

Usage:
    uv run --with databricks-sql-connector --with rapidfuzz --with pandas \\
        python scripts/generate_entity_xref.py --dry-run
    uv run --with databricks-sql-connector --with rapidfuzz --with pandas \\
        python scripts/generate_entity_xref.py  # executes MERGE
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

import pandas as pd
from databricks import sql
from rapidfuzz import fuzz, process

CONFIDENCE_THRESHOLD = 70
DBX_CATALOG = "soccer_analytics"


def fuzzy_match_score(a: str, b: str) -> int:
    """Token-sort ratio — handles word-order variants (surname-first vs given-name-first)."""
    return int(fuzz.token_sort_ratio(a, b))


def emit_pair_ordered(
    source_a: str,
    id_a: str,
    source_b: str,
    id_b: str,
    confidence: int,
    match_layer: int,
    id_field_prefix: str = "player_id",
) -> dict[str, Any]:
    """Emit an xref row enforcing source_a < source_b lexicographically.

    id_field_prefix = 'player_id' for player xref rows,
                      'team_id'   for team xref rows.
    """
    if source_a > source_b:
        source_a, source_b = source_b, source_a
        id_a, id_b = id_b, id_a
    return {
        "source_a": source_a,
        f"{id_field_prefix}_a": id_a,
        "source_b": source_b,
        f"{id_field_prefix}_b": id_b,
        "confidence": confidence,
        "match_layer": match_layer,
        "resolution_type": "automated",
    }


def _connect():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def fetch_rosters(conn) -> dict[str, pd.DataFrame]:
    """Pull player + team rosters per provider from dev_silver staging.

    IMPORTANT: each roster is deduplicated on the native ID column (keep
    first-seen name variant). StatsBomb + Wyscout lineup data often have
    multiple name spellings per player_id across matches — SELECT DISTINCT
    on (player_id, player_name) produces duplicates we must collapse before
    fuzzy-matching, otherwise process.extractOne emits multiple xref rows
    for the same source_a player_id and violates the injectivity invariant.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT player_id, max(player_name) AS player_name
          FROM soccer_analytics.dev_silver.stg_statsbomb__lineups
         WHERE player_id IS NOT NULL AND player_name IS NOT NULL
         GROUP BY player_id
    """)
    sb_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_name"])

    cur.execute("""
        SELECT player_id, max(player_name) AS player_name
          FROM soccer_analytics.dev_silver.stg_wyscout__players
         WHERE player_id IS NOT NULL AND player_name IS NOT NULL
         GROUP BY player_id
    """)
    ws_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_name"])

    cur.execute("""
        SELECT player_id, max(player_display_name) AS player_display_name
          FROM soccer_analytics.dev_silver.stg_tracking__player_metadata
         WHERE provider = 'idsse'
           AND player_id IS NOT NULL
           AND player_display_name IS NOT NULL
         GROUP BY player_id
    """)
    idsse_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_display_name"])

    cur.execute("""
        SELECT DISTINCT team_id, team_name
          FROM soccer_analytics.dev_silver.stg_statsbomb__events
         WHERE team_id IS NOT NULL AND team_name IS NOT NULL
    """)
    sb_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    cur.execute("""
        SELECT DISTINCT team_id, team_name
          FROM soccer_analytics.dev_silver.stg_wyscout__teams
         WHERE team_id IS NOT NULL AND team_name IS NOT NULL
    """)
    ws_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    # IDSSE teams: DFL TeamId + display name come from separate views.
    # stg_idsse__home_away_teams maps (match_id, side) → real DFL TeamId;
    # stg_tracking__player_metadata maps (match_id, team_side) → display name.
    # Join across match_id + side to get (team_id, team_name).
    cur.execute("""
        WITH bridge AS (
            SELECT DISTINCT match_id, side, team_id
              FROM soccer_analytics.dev_silver.stg_idsse__home_away_teams
             WHERE team_id IS NOT NULL
        ),
        names AS (
            SELECT match_id, team_side, max(team_display_name) AS team_name
              FROM soccer_analytics.dev_silver.stg_tracking__player_metadata
             WHERE provider = 'idsse'
             GROUP BY match_id, team_side
        )
        SELECT b.team_id, max(n.team_name) AS team_name
          FROM bridge b
          LEFT JOIN names n
            ON n.match_id = concat('idsse_', b.match_id)
           AND n.team_side = b.side
         GROUP BY b.team_id
    """)
    idsse_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    return {
        "sb_players": sb_players,
        "ws_players": ws_players,
        "idsse_players": idsse_players,
        "sb_teams": sb_teams,
        "ws_teams": ws_teams,
        "idsse_teams": idsse_teams,
    }


def _match_roster_pair(
    a_df: pd.DataFrame,
    a_source: str,
    a_id_col: str,
    a_name_col: str,
    b_df: pd.DataFrame,
    b_source: str,
    b_id_col: str,
    b_name_col: str,
    id_field_prefix: str,
) -> list[dict[str, Any]]:
    """Best-match pairs from df_a → df_b at confidence ≥ 70.

    Uses ``rapidfuzz.process.extractOne`` which is C++-vectorised —
    ~100x faster than the equivalent nested Python loop on 10k+ rosters.
    """
    if a_df.empty or b_df.empty:
        return []

    b_names: list[str] = b_df[b_name_col].astype(str).tolist()
    b_ids: list[str] = b_df[b_id_col].astype(str).tolist()

    rows: list[dict[str, Any]] = []
    for _, a in a_df.iterrows():
        result = process.extractOne(
            str(a[a_name_col]),
            b_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=CONFIDENCE_THRESHOLD,
        )
        if result is None:
            continue
        _matched_name, score, idx = result
        rows.append(
            emit_pair_ordered(
                a_source,
                str(a[a_id_col]),
                b_source,
                b_ids[idx],
                int(score),
                1,
                id_field_prefix=id_field_prefix,
            )
        )
    return rows


def match_players(rosters: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """SB↔WS + SB↔IDSSE + WS↔IDSSE player pairs."""
    rows: list[dict[str, Any]] = []
    rows.extend(
        _match_roster_pair(
            rosters["sb_players"],
            "statsbomb",
            "player_id",
            "player_name",
            rosters["ws_players"],
            "wyscout",
            "player_id",
            "player_name",
            "player_id",
        )
    )
    rows.extend(
        _match_roster_pair(
            rosters["sb_players"],
            "statsbomb",
            "player_id",
            "player_name",
            rosters["idsse_players"],
            "idsse",
            "player_id",
            "player_display_name",
            "player_id",
        )
    )
    rows.extend(
        _match_roster_pair(
            rosters["ws_players"],
            "wyscout",
            "player_id",
            "player_name",
            rosters["idsse_players"],
            "idsse",
            "player_id",
            "player_display_name",
            "player_id",
        )
    )
    return rows


def match_teams(rosters: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(
        _match_roster_pair(
            rosters["sb_teams"],
            "statsbomb",
            "team_id",
            "team_name",
            rosters["ws_teams"],
            "wyscout",
            "team_id",
            "team_name",
            "team_id",
        )
    )
    rows.extend(
        _match_roster_pair(
            rosters["sb_teams"],
            "statsbomb",
            "team_id",
            "team_name",
            rosters["idsse_teams"],
            "idsse",
            "team_id",
            "team_name",
            "team_id",
        )
    )
    rows.extend(
        _match_roster_pair(
            rosters["ws_teams"],
            "wyscout",
            "team_id",
            "team_name",
            rosters["idsse_teams"],
            "idsse",
            "team_id",
            "team_name",
            "team_id",
        )
    )
    return rows


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def merge_xref(conn, table: str, rows: list[dict[str, Any]], key_cols: list[str]) -> None:
    """Delta MERGE on the unique xref grain."""
    if not rows:
        logging.info("No rows to merge into %s", table)
        return
    df = pd.DataFrame(rows)
    cur = conn.cursor()

    temp_view = f"_pr5a_xref_staging_{table.split('.')[-1]}"
    values_parts = []
    for _, row in df.iterrows():
        lit_parts = []
        for c in df.columns:
            v = row[c]
            if isinstance(v, (int, float)) and not pd.isna(v):
                lit_parts.append(str(v))
            else:
                lit_parts.append(_sql_literal(v))
        values_parts.append("(" + ", ".join(lit_parts) + ")")

    create_view_sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {temp_view} AS\n"
        f"SELECT * FROM VALUES\n" + ",\n".join(values_parts) + f"\nAS t({', '.join(df.columns)})"
    )
    cur.execute(create_view_sql)

    update_clause = ",\n    ".join(f"{c} = source.{c}" for c in df.columns if c not in key_cols)
    insert_cols = ", ".join([*list(df.columns), "_ingested_at"])
    insert_vals = ", ".join([*[f"source.{c}" for c in df.columns], "current_timestamp()"])

    merge_sql = f"""
        MERGE INTO {table} target
        USING {temp_view} source
           ON {" AND ".join(f"target.{k} = source.{k}" for k in key_cols)}
         WHEN MATCHED THEN UPDATE SET
            {update_clause},
            _ingested_at = current_timestamp()
         WHEN NOT MATCHED THEN INSERT ({insert_cols})
              VALUES ({insert_vals})
    """
    cur.execute(merge_sql)
    logging.info("Merged %d rows into %s", len(rows), table)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = _connect()
    try:
        rosters = fetch_rosters(conn)
        logging.info("Rosters: %s", {k: len(v) for k, v in rosters.items()})

        player_rows = match_players(rosters)
        team_rows = match_teams(rosters)

        logging.info("Player xref pairs at conf≥%d: %d", CONFIDENCE_THRESHOLD, len(player_rows))
        logging.info("Team xref pairs at conf≥%d: %d", CONFIDENCE_THRESHOLD, len(team_rows))

        if args.dry_run:
            print("--- Sample player xref rows ---")
            for r in player_rows[:5]:
                print(r)
            print("--- Sample team xref rows ---")
            for r in team_rows[:5]:
                print(r)
            return 0

        merge_xref(
            conn,
            f"{DBX_CATALOG}.bronze.player_xref_raw",
            player_rows,
            ["source_a", "player_id_a", "source_b", "player_id_b"],
        )
        merge_xref(
            conn,
            f"{DBX_CATALOG}.bronze.team_xref_raw",
            team_rows,
            ["source_a", "team_id_a", "source_b", "team_id_b"],
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
