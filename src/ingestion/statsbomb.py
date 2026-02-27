"""StatsBomb open-data ingestion into the Databricks bronze layer.

Traverses the StatsBomb API hierarchy via ``statsbombpy``:
  competitions → matches → events / lineups / 360 frames

Bronze tables produced:
  - statsbomb_competitions
  - statsbomb_matches
  - statsbomb_events
  - statsbomb_lineups
  - statsbomb_360

Nested JSON columns (dicts/lists) are serialized to JSON strings so that
downstream dbt staging models can parse them with Databricks SQL JSON functions.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
import statsbombpy.sb as sb

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    serialize_json_columns,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Competition ingestion
# ---------------------------------------------------------------------------


def ingest_competitions(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Fetch and write all StatsBomb competitions.

    Returns:
        The competitions pandas DataFrame (used to iterate matches).
    """
    logger.info("Fetching StatsBomb competitions")
    raw = sb.competitions()
    competitions_pdf: pd.DataFrame = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw
    competitions_pdf = serialize_json_columns(competitions_pdf)

    sdf = spark.createDataFrame(competitions_pdf)
    validate_dataframe(sdf, ["competition_id", "season_id"], "statsbomb_competitions", logger)
    write_delta_table(sdf, catalog, schema, "statsbomb_competitions", mode="overwrite", logger=logger)

    return competitions_pdf


# ---------------------------------------------------------------------------
# Match + detail ingestion (events, lineups, 360)
# ---------------------------------------------------------------------------


def _read_existing_match_ids(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> set[int]:
    """Return match IDs already present in a Delta table, or empty set if table doesn't exist."""
    full_table = f"{catalog}.{schema}.{table}"
    try:
        existing = spark.read.table(full_table).select("match_id").distinct().collect()
        return {row["match_id"] for row in existing}
    except Exception:
        logger.debug("Table %s not found or unreadable, starting fresh", full_table, exc_info=True)
        return set()


def _safe_fetch(
    fetch_fn: Any,
    *args: Any,
    logger: logging.Logger,
    label: str,
    **kwargs: Any,
) -> pd.DataFrame | None:
    """Call a statsbombpy fetch function, returning None on failure."""
    try:
        return fetch_fn(*args, **kwargs)
    except Exception:
        logger.exception("Failed to fetch %s for args=%s", label, args)
        return None


def ingest_matches_and_details(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competitions_pdf: pd.DataFrame,
    logger: logging.Logger,
) -> None:
    """For each competition/season: fetch matches, events, lineups, and 360 data.

    Uses partition-level overwrite keyed on ``competition_id`` and ``season_id``
    for idempotent incremental loading.
    """
    existing_event_match_ids = _read_existing_match_ids(spark, catalog, schema, "statsbomb_events", logger)

    unique_combos = competitions_pdf[["competition_id", "season_id"]].drop_duplicates()
    total = len(unique_combos)

    for idx, (_, row) in enumerate(unique_combos.iterrows()):
        comp_id = int(row["competition_id"])
        season_id = int(row["season_id"])
        logger.info("Processing competition %d, season %d (%d/%d)", comp_id, season_id, idx + 1, total)

        # --- Matches ---
        matches_pdf = _safe_fetch(
            sb.matches,
            competition_id=comp_id,
            season_id=season_id,
            logger=logger,
            label="matches",
        )
        if matches_pdf is None or matches_pdf.empty:
            logger.warning("No matches for competition=%d season=%d, skipping", comp_id, season_id)
            continue

        # Ensure competition_id and season_id columns exist for partitioning
        # (sb.matches() may return these as nested objects rather than flat columns)
        if "competition_id" not in matches_pdf.columns:
            matches_pdf["competition_id"] = comp_id
        if "season_id" not in matches_pdf.columns:
            matches_pdf["season_id"] = season_id

        matches_pdf = serialize_json_columns(matches_pdf)
        matches_sdf = spark.createDataFrame(matches_pdf)
        validate_dataframe(matches_sdf, ["match_id", "competition_id", "season_id"], "statsbomb_matches", logger)
        replace_expr = f"competition_id = {comp_id} AND season_id = {season_id}"
        write_delta_table(
            matches_sdf,
            catalog,
            schema,
            "statsbomb_matches",
            replace_where=replace_expr,
            logger=logger,
        )

        # Determine new match IDs for detail fetching
        all_match_ids: list[int] = matches_pdf["match_id"].tolist()
        new_match_ids = [mid for mid in all_match_ids if mid not in existing_event_match_ids]

        if not new_match_ids:
            logger.info("All %d matches already ingested for comp=%d season=%d", len(all_match_ids), comp_id, season_id)
            continue

        logger.info("Fetching details for %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

        # Accumulators for batch write
        events_batch: list[pd.DataFrame] = []
        lineups_batch: list[pd.DataFrame] = []
        frames_batch: list[pd.DataFrame] = []

        for match_id in new_match_ids:
            # Events
            events_pdf = _safe_fetch(sb.events, match_id=match_id, logger=logger, label="events")
            if events_pdf is not None and not events_pdf.empty:
                events_pdf["match_id"] = match_id
                events_pdf["competition_id"] = comp_id
                events_pdf["season_id"] = season_id
                events_batch.append(events_pdf)

            # Lineups
            lineups_raw = _safe_fetch(sb.lineups, match_id=match_id, logger=logger, label="lineups")
            if lineups_raw is not None:
                _process_lineups(lineups_raw, match_id, comp_id, season_id, lineups_batch)

            # 360 frames
            frames_pdf = _safe_fetch(sb.frames, match_id=match_id, logger=logger, label="360")
            if frames_pdf is not None and not frames_pdf.empty:
                frames_pdf["match_id"] = match_id
                frames_pdf["competition_id"] = comp_id
                frames_pdf["season_id"] = season_id
                frames_batch.append(frames_pdf)

        # Batch write per competition/season
        _write_batch(
            spark,
            catalog,
            schema,
            "statsbomb_events",
            events_batch,
            replace_expr,
            logger,
            required_columns=["event_id", "match_id", "type_name"],
        )
        _write_batch(
            spark,
            catalog,
            schema,
            "statsbomb_lineups",
            lineups_batch,
            replace_expr,
            logger,
            required_columns=["match_id", "team_name", "player_name"],
        )
        _write_batch(
            spark,
            catalog,
            schema,
            "statsbomb_360",
            frames_batch,
            replace_expr,
            logger,
            required_columns=["event_id", "match_id"],
        )


def _process_lineups(
    lineups_raw: Any,
    match_id: int,
    comp_id: int,
    season_id: int,
    lineups_batch: list[pd.DataFrame],
) -> None:
    """Process lineups response into a flat DataFrame and append to batch.

    ``sb.lineups()`` returns a dict keyed by team name, each value being a
    DataFrame of player entries.
    """
    if isinstance(lineups_raw, dict):
        for team_name, team_df in lineups_raw.items():
            if isinstance(team_df, pd.DataFrame) and not team_df.empty:
                team_df = team_df.copy()
                team_df["match_id"] = match_id
                team_df["team_name"] = team_name
                team_df["competition_id"] = comp_id
                team_df["season_id"] = season_id
                lineups_batch.append(team_df)


def _write_batch(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
    batch: list[pd.DataFrame],
    replace_where: str,
    logger: logging.Logger,
    required_columns: list[str],
) -> None:
    """Concatenate a list of pandas DataFrames, serialize JSON columns, and write to Delta."""
    if not batch:
        logger.info("No data for %s in this partition", table_name)
        return

    combined = pd.concat(batch, ignore_index=True)
    combined = serialize_json_columns(combined)
    sdf = spark.createDataFrame(combined)
    validate_dataframe(sdf, required_columns, table_name, logger)
    write_delta_table(sdf, catalog, schema, table_name, replace_where=replace_where, logger=logger)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for StatsBomb ingestion."""
    args = parse_ingestion_args("Ingest StatsBomb open data into the bronze layer")
    logger = configure_logging("statsbomb")
    spark = get_spark_session()

    logger.info("Starting StatsBomb ingestion into %s.%s", args.catalog, args.schema)

    competitions_pdf = ingest_competitions(spark, args.catalog, args.schema, logger)
    ingest_matches_and_details(spark, args.catalog, args.schema, competitions_pdf, logger)

    logger.info("StatsBomb ingestion complete")


if __name__ == "__main__":
    main()
