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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    dtype_overrides_from_snapshot,
    expected_cols_from_snapshot,
    finalize_bronze_df,
    get_spark_session,
    load_bronze_snapshot,
    parse_ingestion_args,
    serialize_json_columns,
    validate_dataframe,
    write_delta_table,
)
from shared.access_tier import classify_access_tier
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from ingestion.utils import SparkAnalysisException as _SparkAnalysisException

# Expected bronze schema (G1 — PR #173 drop-safety sweep). Loaded from the
# test-fixture snapshot at import time via shared helpers in
# ``ingestion.utils``; wheel runtime falls back to empty constants.
_STATSBOMB_SNAPSHOT_TABLES = load_bronze_snapshot("statsbomb_bronze_schema_snapshot.json")

_STATSBOMB_COMPETITIONS_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_competitions"
)
_STATSBOMB_COMPETITIONS_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_competitions"
)
_STATSBOMB_MATCHES_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_matches"
)
_STATSBOMB_MATCHES_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_matches"
)
_STATSBOMB_EVENTS_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_events"
)
_STATSBOMB_EVENTS_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_events"
)
_STATSBOMB_LINEUPS_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_lineups"
)
_STATSBOMB_LINEUPS_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_lineups"
)
_STATSBOMB_360_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(_STATSBOMB_SNAPSHOT_TABLES, "statsbomb_360")
_STATSBOMB_360_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _STATSBOMB_SNAPSHOT_TABLES, "statsbomb_360"
)


logger = logging.getLogger(__name__)


class _StatsbombGuard:
    workflow_id = "wf-statsbomb"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Anti-join against sb.competitions() to find new competition/season pairs.

        Fetches the competitions JSON from the StatsBomb open-data GitHub repo
        (raw.githubusercontent.com). Unauthenticated GitHub rate limit is
        60 req/hour; one guard check per daily scheduled run is well within this.
        """
        try:
            sb = _get_sb()
            api_comps = sb.competitions()
        except Exception:
            logger.warning("sb.competitions() failed — failing open")
            return FilterResult(workflow_id=self.workflow_id, count=1)

        # Load existing bronze competitions
        from ingestion.utils import tolerate_missing_table

        bronze_comps_df = None
        with tolerate_missing_table(logger, "statsbomb_competitions not found — first run"):
            bronze_comps_df = (
                spark.table(f"{catalog}.{schema}.statsbomb_competitions")
                .select("competition_id", "season_id")
                .toPandas()
            )

        if bronze_comps_df is None:
            return FilterResult(workflow_id=self.workflow_id, count=1)

        # Anti-join: find competitions in API but not in bronze
        api_keys = set(zip(api_comps["competition_id"], api_comps["season_id"], strict=False))
        bronze_keys = set(zip(bronze_comps_df["competition_id"], bronze_comps_df["season_id"], strict=False))
        new_comps = api_keys - bronze_keys

        if new_comps:
            return FilterResult(
                workflow_id=self.workflow_id,
                count=1,
                metadata={"new_competitions": [f"{c}_{s}" for c, s in new_comps]},
            )

        # Check for new matches within existing competitions.
        # sb.matches() fetches per competition/season — sample up to 3 existing
        # competition/season pairs to check for new match days.
        # Rate-limit budget: 1 (competitions) + 3 (matches) = 4 unauthenticated
        # GitHub raw requests per guard invocation.  Limit is 60 req/hour.
        # Do NOT increase the 3-pair ceiling without verifying daily run cadence.
        bronze_matches_df = None
        with tolerate_missing_table(logger, "statsbomb_matches not found"):
            bronze_matches_df = spark.table(f"{catalog}.{schema}.statsbomb_matches").select("match_id").toPandas()

        if bronze_matches_df is not None:
            bronze_match_ids = set(bronze_matches_df["match_id"])
            for comp_id, season_id in list(bronze_keys)[:3]:
                try:
                    api_matches = sb.matches(competition_id=comp_id, season_id=season_id)
                except Exception:
                    logger.warning("sb.matches(%s, %s) failed — skipping", comp_id, season_id)
                    continue
                api_match_ids = set(api_matches["match_id"])
                new_matches = api_match_ids - bronze_match_ids
                if new_matches:
                    return FilterResult(
                        workflow_id=self.workflow_id,
                        count=1,
                        metadata={"new_match_ids": [str(m) for m in list(new_matches)[:5]]},
                    )

        return FilterResult(workflow_id=self.workflow_id, count=0)


skip_guard = _StatsbombGuard()

# Max concurrent HTTP requests to StatsBomb API (polite concurrency limit)
_HTTP_MAX_WORKERS = 4


def _get_sb() -> Any:
    """Lazy-load the ``statsbombpy.sb`` module.

    Deferred so the module is importable from environments that lack
    ``statsbombpy`` (e.g., the freshness gate's ``default`` environment).
    The guard only needs Spark SQL — ``statsbombpy`` is only required by
    the pipeline functions that call the StatsBomb API.
    """
    import statsbombpy.sb as sb

    return sb


# ---------------------------------------------------------------------------
# Raw extra JSON extraction (for SPADL adapter)
# ---------------------------------------------------------------------------


def _build_raw_extra_json(match_id: int, logger: logging.Logger) -> dict[str, str]:
    """Fetch raw StatsBomb JSON and extract type-specific 'extra' dicts.

    The SPADL converter needs an ``extra`` dict containing
    type-specific event payloads (e.g. pass.end_location, shot.outcome).
    statsbombpy's default ``fmt="dataframe"`` flattens these, destroying the
    nested structure.  ``fmt="json"`` preserves it.

    Returns:
        Mapping of event_id → JSON string of the extra dict.
    """
    raw_events: dict[str, Any] = _get_sb().events(match_id=match_id, fmt="json")  # type: ignore[assignment]

    extra_map: dict[str, str] = {}
    for event_id_str, raw in raw_events.items():
        type_obj = raw.get("type", {})
        type_name = type_obj.get("name", "") if isinstance(type_obj, dict) else ""
        type_key = type_name.lower().replace(" ", "_").replace("*", "")
        extra: dict[str, Any] = {}
        # Try snake_cased key first, then concatenated form (handles
        # "Goal Keeper" → "goal_keeper" vs raw JSON key "goalkeeper")
        for candidate in (type_key, type_key.replace("_", "")):
            if candidate and candidate in raw:
                extra[candidate] = raw[candidate]
                break
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
    except _SparkAnalysisException:
        logger.info("No existing statsbomb_competitions table — will fetch from API")

    logger.info("Fetching StatsBomb competitions")
    raw = _get_sb().competitions()
    competitions_pdf: pd.DataFrame = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw
    competitions_pdf = serialize_json_columns(competitions_pdf)
    competitions_pdf = finalize_bronze_df(
        competitions_pdf,
        expected_cols=_STATSBOMB_COMPETITIONS_EXPECTED_COLS,
        dtype_overrides=_STATSBOMB_COMPETITIONS_DTYPE_OVERRIDES,
    )

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
    from ingestion.utils import tolerate_missing_table

    full_table = f"{catalog}.{schema}.{table}"
    result: set[int] = set()
    with tolerate_missing_table(logger, f"Table {full_table} not found — starting fresh"):
        existing = spark.read.table(full_table).select("match_id").distinct().collect()
        result = {row["match_id"] for row in existing}
    return result


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
    events_pdf = _safe_fetch(_get_sb().events, match_id=match_id, logger=logger, label="events")

    lineups_raw = _safe_fetch(_get_sb().lineups, match_id=match_id, logger=logger, label="lineups")

    frames_pdf = _safe_fetch(_get_sb().frames, match_id=match_id, logger=logger, label="360")

    extra_map: dict[str, str] | None = None
    if events_pdf is not None and not events_pdf.empty:
        try:
            extra_map = _build_raw_extra_json(match_id, logger)
        except Exception:
            logger.exception("Failed to build _raw_extra_json for match %d", match_id)

    return events_pdf, lineups_raw, frames_pdf, extra_map


def stamp_open_match_visibility(df: pd.DataFrame) -> pd.DataFrame:
    """Stamp the OPEN StatsBomb path's per-match redistribution signals (R-16).

    The open (free) StatsBomb feed has no per-match visibility field — every match in it is
    public by licence. This makes that an EXPLICIT per-row value rather than an implicit
    consequence of ``statsbomb`` sitting in ``PUBLIC_BY_LICENSE_PROVIDERS``.

    PR-2b removes statsbomb from that allowlist so commercial 360 data fails safe to
    restricted. At that moment a row carrying ``visibility=None`` ALSO becomes restricted —
    silently withholding the entire open corpus (spec Finding 5). An explicit ``'public'``
    survives, because ``classify_access_tier`` returns PUBLIC on ``visibility == 'public'``
    BEFORE it consults the allowlist.

    Called immediately BEFORE ``finalize_bronze_df``: that helper adds any ``expected_cols``
    missing from the frame as explicitly-typed all-NA columns, so a future path that forgets
    this stamp yields a typed NULL rather than a silent NullType drop — and after PR-2b a
    NULL fails safe to restricted.

    The COMMERCIAL path (PR-4) stamps ``'private'`` at its own site and does not reuse this:
    "open feed => public" is a property of THIS path only.
    """
    df = df.copy()
    df["visibility"] = "public"
    df["access_tier"] = classify_access_tier(provider="statsbomb", visibility="public").value
    return df


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
    from ingestion.utils import tolerate_missing_table

    with tolerate_missing_table(logger, "No existing statsbomb_matches table — will fetch all combos"):
        combo_rows = (
            spark.table(f"{catalog}.{schema}.statsbomb_matches")
            .select("competition_id", "season_id")
            .distinct()
            .collect()
        )
        existing_match_combos = {(int(row["competition_id"]), int(row["season_id"])) for row in combo_rows}
        logger.info("Found %d existing competition/season combos in statsbomb_matches", len(existing_match_combos))

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
            _get_sb().matches,
            competition_id=comp_id,
            season_id=season_id,
            logger=logger,
            label="matches",
        )
        if matches_pdf is None or matches_pdf.empty:
            logger.warning("No matches for competition=%d season=%d, skipping", comp_id, season_id)
            continue

        # Ensure competition_id and season_id columns exist for partitioning
        # (_get_sb().matches() may return these as nested objects rather than flat columns)
        if "competition_id" not in matches_pdf.columns:
            matches_pdf["competition_id"] = comp_id
        if "season_id" not in matches_pdf.columns:
            matches_pdf["season_id"] = season_id

        matches_pdf = serialize_json_columns(matches_pdf)
        matches_pdf = stamp_open_match_visibility(matches_pdf)
        matches_pdf = finalize_bronze_df(
            matches_pdf,
            expected_cols=_STATSBOMB_MATCHES_EXPECTED_COLS,
            dtype_overrides=_STATSBOMB_MATCHES_DTYPE_OVERRIDES,
        )
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
            expected_cols=_STATSBOMB_EVENTS_EXPECTED_COLS,
            dtype_overrides=_STATSBOMB_EVENTS_DTYPE_OVERRIDES,
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
            expected_cols=_STATSBOMB_LINEUPS_EXPECTED_COLS,
            dtype_overrides=_STATSBOMB_LINEUPS_DTYPE_OVERRIDES,
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
            expected_cols=_STATSBOMB_360_EXPECTED_COLS,
            dtype_overrides=_STATSBOMB_360_DTYPE_OVERRIDES,
        )


def _process_lineups(
    lineups_raw: Any,
    match_id: int,
    comp_id: int,
    season_id: int,
    lineups_batch: list[pd.DataFrame],
) -> None:
    """Process lineups response into a flat DataFrame and append to batch.

    ``_get_sb().lineups()`` returns a dict keyed by team name, each value being a
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
    expected_cols: tuple[str, ...] = (),
    dtype_overrides: dict[str, str] | None = None,
) -> None:
    """Concatenate a list of pandas DataFrames, serialize JSON columns, finalize
    against the expected bronze schema (guards against NullType drops when
    ``expected_cols`` is provided), and write to Delta."""
    if not batch:
        logger.info("No data for %s in this partition", table_name)
        return

    combined = pd.concat(batch, ignore_index=True)
    combined = serialize_json_columns(combined)
    if expected_cols:
        combined = finalize_bronze_df(
            combined,
            expected_cols=expected_cols,
            dtype_overrides=dtype_overrides or {},
        )
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
    *,
    match_ids: list[str] | None = None,
) -> None:
    """Backfill 360 freeze-frame data for matches already ingested.

    The main ingestion pipeline only fetches 360 data for *new* matches.
    This function targets matches that have events but no 360 data yet,
    enabling one-time catchup after the 360 ingestion code was added.

    When *match_ids* is provided (from guard metadata), uses those directly
    instead of doing a full set-difference discovery query.
    """
    if match_ids is not None:
        backfill_candidates = {int(m) for m in match_ids}
    else:
        existing_360_match_ids = _read_existing_match_ids(spark, catalog, schema, "statsbomb_360", logger)
        existing_event_match_ids = _read_existing_match_ids(spark, catalog, schema, "statsbomb_events", logger)
        backfill_candidates = existing_event_match_ids - existing_360_match_ids

    if not backfill_candidates:
        logger.info("No matches need 360 backfill")
        return

    logger.info("Found %d matches needing 360 backfill", len(backfill_candidates))

    # Only iterate competition-seasons that already have 360 data in bronze.
    # Non-360 competitions would produce thousands of empty _get_sb().frames() API calls.
    full_360_table = f"{catalog}.{schema}.statsbomb_360"
    try:
        combos_360 = spark.table(full_360_table).select("competition_id", "season_id").distinct().toPandas()
        unique_combos = combos_360[["competition_id", "season_id"]].drop_duplicates()
        logger.info("Restricting backfill to %d 360-enabled competition-seasons", len(unique_combos))
    except _SparkAnalysisException:
        logger.info("Cannot read %s — falling back to all competitions", full_360_table)
        unique_combos = competitions_pdf[["competition_id", "season_id"]].drop_duplicates()

    for _, row in unique_combos.iterrows():
        comp_id = int(row["competition_id"])
        season_id = int(row["season_id"])

        matches_pdf = _safe_fetch(
            _get_sb().matches,
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
            frames_pdf = _safe_fetch(_get_sb().frames, match_id=match_id, logger=logger, label="360")
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
            expected_cols=_STATSBOMB_360_EXPECTED_COLS,
            dtype_overrides=_STATSBOMB_360_DTYPE_OVERRIDES,
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
    *,
    match_ids: list[str] | None = None,
) -> None:
    """Backfill ``_raw_extra_json`` for existing events that lack it.

    Discovers matches needing backfill, then processes them in chunks grouped
    by ``(competition_id, season_id)`` to keep each MERGE well under the
    Spark protobuf serialization limit (~2 GB).

    Args:
        match_ids: Optional pre-filtered match IDs from the guard's metadata.
            When provided, only these matches are considered for backfill.
    """
    events_table = f"{catalog}.{schema}.statsbomb_events"
    try:
        base_query = (
            f"SELECT DISTINCT match_id, competition_id, season_id "  # noqa: S608
            f"FROM {events_table} "
            f"WHERE _raw_extra_json IS NULL"
        )
        if match_ids:
            id_list = ", ".join(str(int(mid)) for mid in match_ids)
            base_query += f" AND match_id IN ({id_list})"
        needs_backfill_rows = spark.sql(base_query).collect()
    except Exception:
        logger.exception("Cannot read %s for backfill — table may not exist", events_table)
        raise

    if not needs_backfill_rows:
        logger.info("No matches need _raw_extra_json backfill")
        return

    logger.info("Found %d match partitions needing _raw_extra_json backfill", len(needs_backfill_rows))

    # Group matches by (competition_id, season_id) for chunked processing.
    # Each chunk gets its own HTTP fetch → createDataFrame → MERGE cycle,
    # keeping DataFrame size well under the protobuf serialization limit.
    chunks: dict[tuple[int, int], list[int]] = defaultdict(list)
    for row in needs_backfill_rows:
        key = (int(row["competition_id"]), int(row["season_id"]))
        chunks[key].append(int(row["match_id"]))

    logger.info(
        "Processing %d competition-season chunks (%d matches total)",
        len(chunks),
        len(needs_backfill_rows),
    )

    total_events_written = 0
    total_matches_written = 0

    for (comp_id, season_id), chunk_match_ids in chunks.items():
        logger.info(
            "Backfilling comp=%d season=%d: %d matches",
            comp_id,
            season_id,
            len(chunk_match_ids),
        )

        # HTTP-fetch extra JSON for this chunk concurrently
        extra_maps: dict[int, dict[str, str]] = {}
        with ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as executor:
            futures = {executor.submit(_build_raw_extra_json, mid, logger): mid for mid in chunk_match_ids}
            for future in as_completed(futures):
                mid = futures[future]
                try:
                    extra_maps[mid] = future.result()
                except (OSError, ValueError, KeyError):
                    logger.exception("Failed to fetch _raw_extra_json for match %d", mid)

        logger.info(
            "Fetched extra JSON for %d/%d matches in comp=%d season=%d",
            len(extra_maps),
            len(chunk_match_ids),
            comp_id,
            season_id,
        )

        # Build mapping rows for this chunk
        mapping_rows: list[tuple[str, str]] = []
        for extra_map in extra_maps.values():
            if not extra_map:
                continue
            for eid, ejson in extra_map.items():
                mapping_rows.append((eid, ejson))

        if not mapping_rows:
            logger.info("No extra JSON mappings for comp=%d season=%d — skipping", comp_id, season_id)
            continue

        # Execute MERGE for this chunk
        try:
            mapping_sdf = spark.createDataFrame(mapping_rows, ["_eid", "_extra_json"])
            mapping_sdf.createOrReplaceTempView("_backfill_map")

            spark.sql(
                f"MERGE INTO {events_table} AS t "
                "USING _backfill_map AS s "
                "ON t.id = s._eid "
                "WHEN MATCHED THEN UPDATE SET t._raw_extra_json = s._extra_json"
            )

            total_events_written += len(mapping_rows)
            total_matches_written += len(extra_maps)
            logger.info(
                "MERGE complete for comp=%d season=%d: %d events across %d matches",
                comp_id,
                season_id,
                len(mapping_rows),
                len(extra_maps),
            )
        except Exception:
            logger.exception(
                "Failed MERGE for _raw_extra_json backfill (comp=%d season=%d, %d events)",
                comp_id,
                season_id,
                len(mapping_rows),
            )
            raise

    logger.info(
        "Backfill complete: %d events updated across %d matches in %d chunks",
        total_events_written,
        total_matches_written,
        len(chunks),
    )


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


@workflow("wf-statsbomb", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest all StatsBomb open data (competitions, matches, events, lineups, 360)."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    ingest_matches_and_details(spark, catalog, schema, competitions_pdf, logger)
    return 0


def main() -> None:
    """CLI entry point for StatsBomb ingestion."""
    args = parse_ingestion_args("Ingest StatsBomb open data into the bronze layer")
    logger = configure_logging("statsbomb")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting StatsBomb ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("StatsBomb ingestion complete")


def backfill_360_main() -> None:
    """CLI entry point for backfilling StatsBomb 360 freeze-frame data."""
    args = parse_ingestion_args("Backfill StatsBomb 360 freeze-frame data for existing matches")
    logger = configure_logging("statsbomb_360_backfill")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    logger.info("Starting 360 backfill into %s.%s", args.catalog, args.schema)
    competitions_pdf = ingest_competitions(spark, args.catalog, args.schema, logger)
    backfill_360(spark, args.catalog, args.schema, competitions_pdf, logger)
    logger.info("360 backfill complete")


def backfill_extra_json_main() -> None:
    """CLI entry point for backfilling _raw_extra_json on existing StatsBomb events."""
    args = parse_ingestion_args("Backfill _raw_extra_json for existing StatsBomb events")
    logger = configure_logging("statsbomb_extra_backfill")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    logger.info("Starting _raw_extra_json backfill into %s.%s", args.catalog, args.schema)
    competitions_pdf = ingest_competitions(spark, args.catalog, args.schema, logger)
    backfill_extra_json(spark, args.catalog, args.schema, competitions_pdf, logger)
    logger.info("_raw_extra_json backfill complete")


if __name__ == "__main__":
    main()
