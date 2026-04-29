#!/usr/bin/env python3
"""PR-LL2 post-deploy validation: assert all LL2 / Path B columns are populated.

Run AFTER the combined backfill (Phase 23) + dbt full-refresh (Phase 24) finish.
Catches the LL1 latent-bug class (where a column was declared in DDL but ended
up 100% NULL because of an applyInPandas StructType drop).

Usage:
    uv run python scripts/validate_pr_ll2_post_deploy.py
    uv run python scripts/validate_pr_ll2_post_deploy.py --catalog soccer_analytics

Exit codes:
    0 — all checks passed
    1 — at least one column had unexpectedly low non-NULL count
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

logger = logging.getLogger("validate_pr_ll2_post_deploy")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Validation contract: which columns must be populated for which sources.
# ---------------------------------------------------------------------------
#
# (column, expected_sources): the column should have at least 1 non-NULL row
# for each source listed. Sources NOT listed may legitimately be NULL.

# LL1 statsbomb_* columns: only populated for StatsBomb rows.
_LL1_STATSBOMB_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("statsbomb_possession_id", ["statsbomb"]),
    ("statsbomb_possession_team_id", ["statsbomb"]),
    ("statsbomb_play_pattern", ["statsbomb"]),
    ("statsbomb_under_pressure", ["statsbomb"]),
]

# LL2 enrichment columns: provider-agnostic — populated for ALL 4 sources.
# gk_role / defending_gk_player_id are NULL on non-shot/non-GK rows; the
# COUNT(*) WHERE col IS NOT NULL only needs to be > 0 (not == row count).
_LL2_ENRICHMENT_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("possession_id_heuristic", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("gk_role", ["statsbomb", "wyscout", "idsse", "metrica"]),
    # gk_was_distributing / gk_was_engaged default False (so non-NULL by
    # construction) — the validation works the same: > 0 non-NULL rows.
    ("gk_was_distributing", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("gk_was_engaged", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("gk_actions_in_possession", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("defending_gk_player_id", ["statsbomb", "wyscout", "idsse", "metrica"]),
]

# action_id surfaced from silly-kicks output (LL2 closes pre-LL2 100%-NULL).
_LL2_ACTION_ID_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("action_id", ["statsbomb", "wyscout", "idsse", "metrica"]),
]

# LL2 Path B: native string identifiers — populated for ALL sources.
_LL2_PATH_B_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("team_id_native", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("home_team_id_native", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("competition_native_id", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("season_native_id", ["statsbomb", "wyscout", "idsse", "metrica"]),
    ("match_id_native", ["statsbomb", "wyscout", "idsse", "metrica"]),
]

_ALL_VALIDATIONS: list[tuple[str, list[str]]] = (
    _LL1_STATSBOMB_VALIDATIONS + _LL2_ENRICHMENT_VALIDATIONS + _LL2_ACTION_ID_VALIDATIONS + _LL2_PATH_B_VALIDATIONS
)


def _connect():  # type: ignore[no-untyped-def]
    """Open a Databricks SQL connection. Strip MSYS double-slash prefix."""
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    if http_path.startswith("//"):
        http_path = http_path[1:]
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _validate_column(cur, fq_table: str, col: str, sources: list[str]) -> list[str]:  # type: ignore[no-untyped-def]
    """Return a list of failure messages (empty if all expected sources have non-NULL data)."""
    failures: list[str] = []
    for source in sources:
        cur.execute(
            f"SELECT COUNT(*) FROM {fq_table} "  # noqa: S608
            f"WHERE data_source = %(src)s AND {col} IS NOT NULL",
            {"src": source},
        )
        non_null_rows = cur.fetchone()[0]
        if non_null_rows > 0:
            logger.info("OK    %s.%s (data_source=%s): %d non-NULL rows", fq_table, col, source, non_null_rows)
        else:
            msg = f"FAIL  {fq_table}.{col} (data_source={source}): 0 non-NULL rows"
            logger.error(msg)
            failures.append(msg)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="PR-LL2 post-deploy validation")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument(
        "--table",
        default="vaep_action_values",
        help="Target table — default vaep_action_values (terminal in the SPADL/VAEP chain)",
    )
    args = parser.parse_args()

    fq_table = f"{args.catalog}.{args.schema}.{args.table}"
    logger.info("Validating LL2 / Path B columns on %s", fq_table)

    conn = _connect()
    all_failures: list[str] = []
    try:
        cur = conn.cursor()
        try:
            for col, sources in _ALL_VALIDATIONS:
                all_failures.extend(_validate_column(cur, fq_table, col, sources))
        finally:
            cur.close()
    finally:
        conn.close()

    if all_failures:
        logger.error("VALIDATION FAILED — %d column(s) had 0 non-NULL rows for an expected source:", len(all_failures))
        for f in all_failures:
            logger.error("  %s", f)
        return 1

    logger.info("VALIDATION PASSED — all LL2 / Path B columns populated for expected sources")
    return 0


if __name__ == "__main__":
    sys.exit(main())
