#!/usr/bin/env python3
"""Cross-table native-ID integrity validation (ADR-018 F3).

Asserts two invariants on bronze.spadl_actions / bronze.vaep_action_values:

1. **Column population**: every LL1/LL2 column declared in DDL has at
   least one non-NULL row for each expected data_source. Catches the
   LL1-class latent bug (column declared but 0 non-NULL rows due to
   StructType drop at applyInPandas boundary).

2. **JOIN-coverage**: every distinct value of bronze
   ``<entity>_native`` columns resolves to a row in the corresponding
   ``dim_<entity>``. Catches the PR-LL2-Path-B-class bug (bronze writer
   format and dim staging format drift apart, producing 100% NULL
   ``<entity>_key`` in fct_action_values at full mart build time).

This script runs:

- After the combined backfill + dbt full-refresh (post-deploy gate).
- In slim CI (pre-merge gate) via the wf-vaep-light flow's tag.

Usage:
    uv run python scripts/validate_native_id_integrity.py
    uv run python scripts/validate_native_id_integrity.py --catalog soccer_analytics

Exit codes:
    0 — all checks passed
    1 — at least one column had unexpectedly low non-NULL count, OR
        at least one JOIN-coverage probe found unmatched native IDs
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

logger = logging.getLogger("validate_native_id_integrity")
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

# PR-Cycle-A.4 (2026-04-30, ADR-018 alignment): silly-kicks 2.5.0 sportec
# tackle qualifier columns. ``<col>_native`` STRING + ``<col>_key`` BIGINT
# surrogate per LL2 Path B convention. Populated only on sportec (idsse)
# rows where the DFL XML's tackle_winner qualifier is present. Other
# sources NULL by design.
_PATH_B_CLOSE_OUT_TACKLE_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("tackle_winner_player_id_native", ["idsse"]),
    ("tackle_winner_player_key", ["idsse"]),
    ("tackle_winner_team_id_native", ["idsse"]),
    ("tackle_winner_team_key", ["idsse"]),
    ("tackle_loser_player_id_native", ["idsse"]),
    ("tackle_loser_player_key", ["idsse"]),
    ("tackle_loser_team_id_native", ["idsse"]),
    ("tackle_loser_team_key", ["idsse"]),
]

_ALL_COLUMN_VALIDATIONS: list[tuple[str, list[str]]] = (
    _LL1_STATSBOMB_VALIDATIONS
    + _LL2_ENRICHMENT_VALIDATIONS
    + _LL2_ACTION_ID_VALIDATIONS
    + _LL2_PATH_B_VALIDATIONS
    + _PATH_B_CLOSE_OUT_TACKLE_VALIDATIONS
)


# ---------------------------------------------------------------------------
# JOIN-coverage probes (ADR-018 F3): every distinct bronze native ID must
# resolve to a row in the corresponding dim_*. Returns rows ⇒ failure.
# ---------------------------------------------------------------------------
#
# Tuple shape: (label, source, bronze_col, dim_table, dim_native_col, dim_key_col).


def _join_probe(
    label: str,
    source: str,
    bronze_col: str,
    dim_table: str,
    dim_native: str,
    dim_key: str,
) -> tuple[str, str, str, str, str, str]:
    """Tuple constructor — keeps the probe list readable at <120 cols."""
    return (label, source, bronze_col, dim_table, dim_native, dim_key)


_NATIVE_JOIN_PROBES: list[tuple[str, str, str, str, str, str]] = [
    # match_id_native → dim_matches.native_match_id (4 sources)
    _join_probe("sb_match_join", "statsbomb", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    _join_probe("ws_match_join", "wyscout", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    _join_probe("idsse_match_join", "idsse", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    _join_probe("metrica_match_join", "metrica", "match_id_native", "dim_matches", "native_match_id", "match_key"),
    # team_id_native → dim_teams.native_team_id (4 sources)
    _join_probe("sb_team_join", "statsbomb", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    _join_probe("ws_team_join", "wyscout", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    _join_probe("idsse_team_join", "idsse", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    _join_probe("metrica_team_join", "metrica", "team_id_native", "dim_teams", "native_team_id", "team_key"),
    # competition_native_id → dim_competitions.native_competition_id (4 sources)
    _join_probe(
        "sb_comp_join",
        "statsbomb",
        "competition_native_id",
        "dim_competitions",
        "native_competition_id",
        "competition_key",
    ),
    _join_probe(
        "ws_comp_join",
        "wyscout",
        "competition_native_id",
        "dim_competitions",
        "native_competition_id",
        "competition_key",
    ),
    _join_probe(
        "idsse_comp_join",
        "idsse",
        "competition_native_id",
        "dim_competitions",
        "native_competition_id",
        "competition_key",
    ),
    _join_probe(
        "metrica_comp_join",
        "metrica",
        "competition_native_id",
        "dim_competitions",
        "native_competition_id",
        "competition_key",
    ),
]


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


def _validate_join_coverage(  # type: ignore[no-untyped-def]
    cur,
    catalog: str,
    bronze_schema: str,
    gold_schema: str,
    bronze_table: str,
    probe: tuple[str, str, str, str, str, str],
) -> list[str]:
    """Run a JOIN coverage probe; return failure messages.

    Asserts every distinct bronze.<bronze_table>.<bronze_col> for <source>
    resolves to a row in <gold_schema>.<dim_table>.<dim_native_col> on
    (provider, native_id) join. Tuple shape:
        (label, source, bronze_col, dim_table, dim_native_col, dim_key_col).
    """
    label, source, b_col, dim_table, d_native, d_key = probe
    fq_bronze = f"{catalog}.{bronze_schema}.{bronze_table}"
    fq_dim = f"{catalog}.{gold_schema}.{dim_table}"
    cur.execute(
        f"SELECT COUNT(DISTINCT b.{b_col}) FROM {fq_bronze} b "  # noqa: S608
        f"LEFT JOIN {fq_dim} d ON b.{b_col} = d.{d_native} AND b.data_source = d.provider "
        f"WHERE b.data_source = %(src)s AND b.{b_col} IS NOT NULL AND d.{d_key} IS NULL",
        {"src": source},
    )
    unmatched = cur.fetchone()[0]
    if unmatched > 0:
        msg = (
            f"FAIL  {label}: {unmatched} distinct bronze {b_col} values do not resolve in "
            f"{dim_table}.{d_native} (provider={source})"
        )
        logger.error(msg)
        return [msg]
    logger.info(
        "OK    %s: all distinct %s values resolve in %s",
        label,
        b_col,
        dim_table,
    )
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-table native-ID integrity validation (ADR-018)")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument(
        "--gold-schema",
        default="dev_gold",
        help="Gold schema housing dim_* tables (default: dev_gold)",
    )
    parser.add_argument(
        "--table",
        default="vaep_action_values",
        help="Bronze terminal table — default vaep_action_values",
    )
    parser.add_argument(
        "--skip-join-probes",
        action="store_true",
        help="Skip the JOIN-coverage probes (column-fill validations only)",
    )
    args = parser.parse_args()

    fq_table = f"{args.catalog}.{args.bronze_schema}.{args.table}"
    logger.info(
        "Validating native-ID integrity on %s (gold dims via %s.%s)",
        fq_table,
        args.catalog,
        args.gold_schema,
    )

    conn = _connect()
    all_failures: list[str] = []
    try:
        cur = conn.cursor()
        try:
            # Phase 1: column-fill validations.
            for col, sources in _ALL_COLUMN_VALIDATIONS:
                all_failures.extend(_validate_column(cur, fq_table, col, sources))

            # Phase 2: cross-table JOIN-coverage probes.
            if not args.skip_join_probes:
                # JOIN probes run against bronze.spadl_actions (the source of
                # truth for native cols; vaep_action_values is downstream and
                # carries the same values via UDF passthrough — same coverage).
                for probe in _NATIVE_JOIN_PROBES:
                    all_failures.extend(
                        _validate_join_coverage(
                            cur,
                            args.catalog,
                            args.bronze_schema,
                            args.gold_schema,
                            "spadl_actions",
                            probe,
                        )
                    )
        finally:
            cur.close()
    finally:
        conn.close()

    if all_failures:
        logger.error(
            "VALIDATION FAILED — %d failure(s):",
            len(all_failures),
        )
        for f in all_failures:
            logger.error("  %s", f)
        return 1

    logger.info("VALIDATION PASSED — all column fills + JOIN coverages green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
