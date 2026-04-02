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

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Max concurrent HTTP requests to StatsBomb API (polite concurrency limit)
_HTTP_MAX_WORKERS = 4


# ---------------------------------------------------------------------------
# Raw extra JSON extraction (for SPADL adapter)
# ---------------------------------------------------------------------------


def _build_raw_extra_json(match_id: int, logger: logging.Logger) -> dict[str, str]:
    """Fetch raw StatsBomb JSON and extract type-specific 'extra' dicts.

    socceraction's SPADL converter needs an ``extra`` dict containing
    type-specific event payloads (e.g. pass.end_location, shot.outcome).
    statsbombpy's default ``fmt="dataframe"`` flattens these, destroying the
    nested structure.  ``fmt="json"`` preserves it.

    Returns:
        Mapping of event_id → JSON string of the extra dict.
    """
    raw_events: dict[str, Any] = sb.events(match_id=match_id, fmt="json")  # type: ignore[assignment]

    extra_map: dict[str, str] = {}
    for event_id_str, raw in raw_events.items():
        type_obj = raw.get("type", {})
        type_name = type_obj.get("name", "") if isinstance(type_obj, dict) else ""
        type_key = type_name.lower().replace(" ", "_").replace("*", "")
        extra: dict[str, Any] = {}
        if type_key and type_key in raw:
            extra[type_key] = raw[type_key]
        for aux_key in ("related_events", "tactics", "50_50"):
            if aux_key in raw:
                extra[aux_key] = raw[aux_key]
        extra_map[str(event_id_str)] = json.dumps(extra, default=str)

    logger.debug("Built raw_extra_json for match %d: %d events", match_id, len(extra_map))
    return extra_map


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
    # Skip guard: return existing data if table already populated
    full_table_name = f"{catalog}.{schema}.statsbomb_competitions"
    try:
        if spark.catalog.tableExists(full_table_name):
            logger.info("statsbomb_competitions already populated — skipping")
            return spark.table(full_table_name).toPandas()
    except Exception:
        logger.info("No existing statsbomb_competitions table — will fetch from API")

    logger.info("Fetching StatsBomb competitions")
    raw = sb.competitions()
    competitions_pdf: pd.DataFrame = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw
    competitions_pdf = serialize_json_columns(competitions_pdf)

    sdf = spark.createDataFrame(competitions_pdf)
    row_count = validate_dataframe(sdf, ["competition_id", "season_id"], "statsbomb_competitions", logger)
    write_delta_table(
        sdf,
        catalog,
        schema,
        "statsbomb_competitions",
        mode="overwrite",
        logger=logger,
        row_count=row_count,
    )

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


def _fetch_match_details(
    match_id: int,
    logger: logging.Logger,
) -> tuple[pd.DataFrame | None, Any, pd.DataFrame | None, dict[str, str] | None]:
    """Fetch events, lineups, 360 frames, and extra JSON for a single match.

    All four HTTP fetches are independent so they run concurrently via
    ``ThreadPoolExecutor``. This function is the unit of work submitted
    to the pool — it is called from a worker thread.

    Returns:
        ``(events_pdf, lineups_raw, frames_pdf, extra_map)`` — any element
        may be ``None`` on fetch failure.
    """
    events_pdf = _safe_fetch(sb.events, match_id=match_id, logger=logger, label="events")

    lineups_raw = _safe_fetch(sb.lineups, match_id=match_id, logger=logger, label="lineups")

    frames_pdf = _safe_fetch(sb.frames, match_id=match_id, logger=logger, label="360")

    extra_map: dict[str, str] | None = None
    if events_pdf is not None and not events_pdf.empty:
        try:
            extra_map = _build_raw_extra_json(match_id, logger)
        except Exception:
            logger.exception("Failed to build _raw_extra_json for match %d", match_id)

    return events_pdf, lineups_raw, frames_pdf, extra_map


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

    # Read existing (competition_id, season_id) combos to skip re-fetching matches
    existing_match_combos: set[tuple[int, int]] = set()
    try:
        combo_rows = (
            spark.table(f"{catalog}.{schema}.statsbomb_matches")
            .select("competition_id", "season_id")
            .distinct()
            .collect()
        )
        existing_match_combos = {(int(row["competition_id"]), int(row["season_id"])) for row in combo_rows}
        logger.info("Found %d existing competition/season combos in statsbomb_matches", len(existing_match_combos))
    except Exception:
        logger.info("No existing statsbomb_matches table — will fetch all combos")

    unique_combos = competitions_pdf[["competition_id", "season_id"]].drop_duplicates()
    total = len(unique_combos)

    for idx, (_, row) in enumerate(unique_combos.iterrows()):
        comp_id = int(row["competition_id"])
        season_id = int(row["season_id"])
        logger.info("Processing competition %d, season %d (%d/%d)", comp_id, season_id, idx + 1, total)

        # Skip guard: if this combo already exists in statsbomb_matches, skip the
        # matches fetch+write entirely. The matches data rarely changes for
        # completed seasons, and new match detection is handled downstream by the
        # existing events skip guard (_read_existing_match_ids).
        if (comp_id, season_id) in existing_match_combos:
            logger.info(
                "Matches already ingested for comp=%d season=%d — skipping fetch+write",
                comp_id,
                season_id,
            )
            continue

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
        row_count = validate_dataframe(
            matches_sdf,
            ["match_id", "competition_id", "season_id"],
            "statsbomb_matches",
            logger,
        )
        replace_expr = f"competition_id = {comp_id} AND season_id = {season_id}"
        write_delta_table(
            matches_sdf,
            catalog,
            schema,
            "statsbomb_matches",
            replace_where=replace_expr,
            logger=logger,
            row_count=row_count,
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

        # Fetch match details concurrently (events, lineups, 360, extra JSON)
        with ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_match_details, match_id, logger): match_id for match_id in new_match_ids}
            for future in as_completed(futures):
                match_id = futures[future]
                try:
                    result = future.result()
                except Exception:
                    logger.exception("Unexpected error fetching details for match %d", match_id)
                    continue

                events_pdf, lineups_raw, frames_pdf, extra_map = result

                # Events
                if events_pdf is not None and not events_pdf.empty:
                    events_pdf["match_id"] = match_id
                    events_pdf["competition_id"] = comp_id
                    events_pdf["season_id"] = season_id

                    # Enrich with raw extra JSON for SPADL adapter
                    if extra_map is not None:
                        events_pdf["_raw_extra_json"] = events_pdf["id"].map(extra_map).fillna("{}")  # type: ignore[arg-type]
                    else:
                        events_pdf["_raw_extra_json"] = "{}"

                    events_batch.append(events_pdf)

                # Lineups
                if lineups_raw is not None:
                    _process_lineups(lineups_raw, match_id, comp_id, season_id, lineups_batch)

                # 360 frames
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
            required_columns=["id", "match_id", "type"],
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
            required_columns=["id", "match_id"],
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
    row_count = validate_dataframe(sdf, required_columns, table_name, logger)
    write_delta_table(sdf, catalog, schema, table_name, replace_where=replace_where, logger=logger, row_count=row_count)


# ---------------------------------------------------------------------------
# 360 backfill
# ---------------------------------------------------------------------------


def backfill_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competitions_pdf: pd.DataFrame,
    logger: logging.Logger,
) -> None:
    """Backfill 360 freeze-frame data for matches already ingested.

    The main ingestion pipeline only fetches 360 data for *new* matches.
    This function targets matches that have events but no 360 data yet,
    enabling one-time catchup after the 360 ingestion code was added.
    """
    existing_360_match_ids = _read_existing_match_ids(spark, catalog, schema, "statsbomb_360", logger)
    existing_event_match_ids = _read_existing_match_ids(spark, catalog, schema, "statsbomb_events", logger)

    # Only process matches that have events but no 360 data
    backfill_candidates = existing_event_match_ids - existing_360_match_ids

    if not backfill_candidates:
        logger.info("No matches need 360 backfill")
        return

    logger.info("Found %d matches needing 360 backfill", len(backfill_candidates))

    # Only iterate competition-seasons that already have 360 data in bronze.
    # Non-360 competitions would produce thousands of empty sb.frames() API calls.
    full_360_table = f"{catalog}.{schema}.statsbomb_360"
    try:
        combos_360 = spark.table(full_360_table).select("competition_id", "season_id").distinct().toPandas()
        unique_combos = combos_360[["competition_id", "season_id"]].drop_duplicates()
        logger.info("Restricting backfill to %d 360-enabled competition-seasons", len(unique_combos))
    except Exception:
        logger.info("Cannot read %s — falling back to all competitions", full_360_table)
        unique_combos = competitions_pdf[["competition_id", "season_id"]].drop_duplicates()

    for _, row in unique_combos.iterrows():
        comp_id = int(row["competition_id"])
        season_id = int(row["season_id"])

        matches_pdf = _safe_fetch(
            sb.matches,
            competition_id=comp_id,
            season_id=season_id,
            logger=logger,
            label="matches",
        )
        if matches_pdf is None or matches_pdf.empty:
            continue

        match_ids_in_season: list[int] = matches_pdf["match_id"].tolist()
        target_ids = set(match_ids_in_season) & backfill_candidates

        if not target_ids:
            continue

        logger.info(
            "Backfilling 360 for %d matches in comp=%d season=%d",
            len(target_ids),
            comp_id,
            season_id,
        )

        frames_batch: list[pd.DataFrame] = []
        for match_id in target_ids:
            frames_pdf = _safe_fetch(sb.frames, match_id=match_id, logger=logger, label="360")
            if frames_pdf is not None and not frames_pdf.empty:
                frames_pdf["match_id"] = match_id
                frames_pdf["competition_id"] = comp_id
                frames_pdf["season_id"] = season_id
                frames_batch.append(frames_pdf)

        replace_expr = f"competition_id = {comp_id} AND season_id = {season_id}"
        _write_batch(
            spark,
            catalog,
            schema,
            "statsbomb_360",
            frames_batch,
            replace_expr,
            logger,
            required_columns=["id", "match_id"],
        )


# ---------------------------------------------------------------------------
# Extra JSON backfill
# ---------------------------------------------------------------------------


def backfill_extra_json(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competitions_pdf: pd.DataFrame,
    logger: logging.Logger,
) -> None:
    """Backfill ``_raw_extra_json`` for existing events that lack it.

    Reads distinct ``match_id`` values where the column is NULL or ``'{}'``,
    fetches the raw JSON from StatsBomb, and overwrites each partition.
    """
    events_table = f"{catalog}.{schema}.statsbomb_events"
    try:
        needs_backfill_rows = spark.sql(
            f"SELECT DISTINCT match_id, competition_id, season_id "  # noqa: S608
            f"FROM {events_table} "
            f"WHERE _raw_extra_json IS NULL OR _raw_extra_json = '{{}}'"
        ).collect()
    except Exception:
        logger.exception("Cannot read %s for backfill — table may not exist", events_table)
        return

    if not needs_backfill_rows:
        logger.info("No matches need _raw_extra_json backfill")
        return

    logger.info("Found %d match partitions needing _raw_extra_json backfill", len(needs_backfill_rows))

    # Pre-fetch all extra JSON maps concurrently (HTTP is the bottleneck)
    match_ids_to_backfill = [int(row["match_id"]) for row in needs_backfill_rows]
    extra_maps: dict[int, dict[str, str]] = {}

    with ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as executor:
        futures = {executor.submit(_build_raw_extra_json, mid, logger): mid for mid in match_ids_to_backfill}
        for future in as_completed(futures):
            mid = futures[future]
            try:
                extra_maps[mid] = future.result()
            except Exception:
                logger.exception("Failed to fetch _raw_extra_json for match %d", mid)

    logger.info("Fetched extra JSON for %d/%d matches", len(extra_maps), len(match_ids_to_backfill))

    # Apply extra JSON maps via Delta MERGE — updates only _raw_extra_json
    # without reading/writing all columns (P0-07).
    for row in needs_backfill_rows:
        match_id = int(row["match_id"])
        comp_id = int(row["competition_id"])
        season_id = int(row["season_id"])

        if match_id not in extra_maps:
            continue

        try:
            extra_map = extra_maps[match_id]
            if not extra_map:
                continue

            # Build a small mapping DataFrame with (event_id, extra_json)
            mapping_rows = [(eid, ejson) for eid, ejson in extra_map.items()]
            mapping_sdf = spark.createDataFrame(mapping_rows, ["_eid", "_extra_json"])
            mapping_sdf.createOrReplaceTempView("_backfill_map")

            spark.sql(
                f"MERGE INTO {events_table} AS t "
                f"USING _backfill_map AS s "
                f"ON t.id = s._eid AND t.match_id = {match_id} "
                "WHEN MATCHED THEN UPDATE SET t._raw_extra_json = s._extra_json"
            )

            logger.info("Backfilled _raw_extra_json for match %d (comp=%d, season=%d)", match_id, comp_id, season_id)
        except Exception:
            logger.exception("Failed backfill for match %d", match_id)


# ---------------------------------------------------------------------------
# Entry points
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


def backfill_360_main() -> None:
    """CLI entry point for backfilling StatsBomb 360 freeze-frame data."""
    args = parse_ingestion_args("Backfill StatsBomb 360 freeze-frame data for existing matches")
    logger = configure_logging("statsbomb_360_backfill")
    spark = get_spark_session()

    logger.info("Starting 360 backfill into %s.%s", args.catalog, args.schema)
    competitions_pdf = ingest_competitions(spark, args.catalog, args.schema, logger)
    backfill_360(spark, args.catalog, args.schema, competitions_pdf, logger)
    logger.info("360 backfill complete")


def backfill_extra_json_main() -> None:
    """CLI entry point for backfilling _raw_extra_json on existing StatsBomb events."""
    args = parse_ingestion_args("Backfill _raw_extra_json for existing StatsBomb events")
    logger = configure_logging("statsbomb_extra_backfill")
    spark = get_spark_session()

    logger.info("Starting _raw_extra_json backfill into %s.%s", args.catalog, args.schema)
    competitions_pdf = ingest_competitions(spark, args.catalog, args.schema, logger)
    backfill_extra_json(spark, args.catalog, args.schema, competitions_pdf, logger)
    logger.info("_raw_extra_json backfill complete")


if __name__ == "__main__":
    main()
