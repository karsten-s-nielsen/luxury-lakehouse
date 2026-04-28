"""SPADL conversion from bronze event tables.

Reads events from existing bronze Delta tables (``statsbomb_events``,
``wyscout_events``) and converts them into SPADL unified format via
silly-kicks.  Each data source has a dedicated UDF factory (for
``applyInPandas`` distribution) and a bronze-to-SPADL converter function.

This module is consumed by :mod:`ingestion.spadl_vaep` which orchestrates
the end-to-end SPADL → VAEP pipeline.

Reference: Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019).
"Actions Speak Louder than Goals: Valuing Player Actions in Soccer."
KDD 2019.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.spadl_adapter import (
    resolve_statsbomb_home_team_ids,
    resolve_wyscout_home_team_ids,
)
from ingestion.utils import write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SPADL_TABLE = "spadl_actions"


def _make_statsbomb_replace_where(new_game_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to the specific StatsBomb matches being re-processed.

    The predicate MUST NOT be broader than the match_ids actually present in the
    UDF output — a broad ``data_source='statsbomb'`` predicate would wipe all
    existing statsbomb rows on every incremental run.
    """
    if not new_game_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(mid)) for mid in sorted(new_game_ids))
    return f"data_source = 'statsbomb' AND match_id IN ({ids_sql})"


def _make_wyscout_replace_where(new_game_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to the specific Wyscout matches being re-processed.

    See ``_make_statsbomb_replace_where`` for the rationale.
    """
    if not new_game_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(mid)) for mid in sorted(new_game_ids))
    return f"data_source = 'wyscout' AND match_id IN ({ids_sql})"


# ---------------------------------------------------------------------------
# Incremental helpers
# ---------------------------------------------------------------------------


def _read_existing_match_ids(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> set[int]:
    """Return match_ids already present in a Delta table, or empty set if table doesn't exist."""
    from ingestion.utils import tolerate_missing_table

    full_table = f"{catalog}.{schema}.{table}"
    result: set[int] = set()
    with tolerate_missing_table(logger, f"Table {full_table} not found — starting fresh"):
        rows = spark.table(full_table).select("match_id").distinct().collect()
        result = {int(row["match_id"]) for row in rows}
    return result


# ---------------------------------------------------------------------------
# StatsBomb SPADL conversion
# ---------------------------------------------------------------------------


def _make_sb_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for StatsBomb SPADL conversion.

    All library imports happen inside the closure so they are available
    on Spark executors without requiring module-level serialisation.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one game's StatsBomb events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_statsbomb_events as _adapt

        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
                # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.statsbomb as _spadl_sb

        home_team_id = int(pdf["home_team_id"].iloc[0])
        match_id = int(pdf["match_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf, home_team_id)
            actions, _report = _spadl_sb.convert_to_actions(
                adapted,
                home_team_id,
                preserve_native=[
                    "possession",
                    "possession_team_id",
                    "play_pattern",
                    "under_pressure",
                ],
            )
        except Exception as exc:
            msg = f"StatsBomb SPADL conversion failed for match_id={match_id}"
            raise RuntimeError(msg) from exc

        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "statsbomb"

        # Provider-namespace the preserved fields. silly-kicks returns them with
        # their input names (``possession``, ``possession_team_id``, etc.); the
        # bronze + mart conventions use ``statsbomb_*`` per the multi-provider
        # symmetry argument (Wyscout/IDSSE/SkillCorner produce NULL here).
        actions = actions.rename(
            columns={
                "possession": "statsbomb_possession_id",
                "possession_team_id": "statsbomb_possession_team_id",
                "play_pattern": "statsbomb_play_pattern",
                "under_pressure": "statsbomb_under_pressure",
            }
        )

        # Cast original_event_id to str for Spark/PyArrow serialization
        # (silly-kicks outputs object dtype; Spark needs explicit string)
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # Force pandas nullable dtypes on the preserved fields. silly-kicks's
        # ``_add_dribbles`` inserts synthetic dribble rows with NaN in preserved
        # columns; without nullable dtypes the BIGINT columns upcast to float64
        # and the BOOLEAN column upcasts to object — PyArrow then has
        # inconsistent conversion paths to LongType / BooleanType in the
        # applyInPandas schema. Explicit nullable dtypes make the intent
        # contract-level and survive the Spark round-trip cleanly.
        actions["statsbomb_possession_id"] = actions["statsbomb_possession_id"].astype("Int64")
        actions["statsbomb_possession_team_id"] = actions["statsbomb_possession_team_id"].astype("Int64")
        actions["statsbomb_under_pressure"] = actions["statsbomb_under_pressure"].astype("boolean")
        # statsbomb_play_pattern stays object (string with NaN) — fine for StringType.

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_statsbomb_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read StatsBomb events from bronze, adapt, convert to SPADL, write Delta.

    Uses ``groupBy("match_id").applyInPandas`` to distribute per-game
    SPADL conversion across Spark executors instead of sequential driver
    loops with ``.toPandas()``.

    Returns whether any data was written.
    """
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    events_table = f"{catalog}.{schema}.statsbomb_events"
    matches_table = f"{catalog}.{schema}.statsbomb_matches"

    # Check for new games BEFORE pulling metadata tables to driver (avoid
    # wasted .toPandas() on no-op runs).
    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read StatsBomb events bronze table")
        return False

    all_game_rows = events_sdf.select("match_id").distinct().collect()
    all_game_ids = [int(row["match_id"]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]

    if not new_game_ids:
        logger.info("StatsBomb: all %d games already converted — skipping", len(all_game_ids))
        return False

    # Only now pull metadata tables needed for home_team_id resolution
    try:
        all_matches_pdf = spark.table(matches_table).select("match_id", "home_team").toPandas()
    except Exception:
        logger.exception("Cannot read StatsBomb matches bronze table")
        return False

    team_lookup_pdf = events_sdf.select("match_id", "team_id", "team").distinct().toPandas()

    if team_lookup_pdf.empty:
        logger.info("StatsBomb bronze events table is empty — skipping")
        return False

    home_team_map = resolve_statsbomb_home_team_ids(all_matches_pdf, team_lookup_pdf)

    # Filter out games where home_team_id is unknown
    new_game_ids = [gid for gid in new_game_ids if home_team_map.get(gid, 0) != 0]

    if not new_game_ids:
        logger.info("StatsBomb: all %d games already converted — skipping", len(all_game_ids))
        return False

    logger.info("StatsBomb: converting %d new games (of %d total)", len(new_game_ids), len(all_game_ids))

    # Build home_team_id lookup as Spark DataFrame and join to events
    home_rows = [(gid, home_team_map[gid]) for gid in new_game_ids]
    home_schema = StructType(
        [
            StructField("match_id", LongType()),
            StructField("home_team_id", LongType()),
        ]
    )
    home_sdf = spark.createDataFrame(home_rows, schema=home_schema)

    # Filter events to new games and join home_team_id
    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_game_ids)).join(
        home_sdf, on="match_id", how="inner"
    )

    # Define SPADL output schema
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
            # Wyscout / IDSSE / SkillCorner produce NULL.
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )

    udf_fn = _make_sb_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=_make_statsbomb_replace_where(new_game_ids),
        logger=logger,
    )

    logger.info("StatsBomb: SPADL conversion complete for %d games", len(new_game_ids))
    return True


# ---------------------------------------------------------------------------
# Wyscout SPADL conversion
# ---------------------------------------------------------------------------


def _make_ws_spadl_udf(goalkeeper_ids: set[int] | None = None) -> object:
    """Build the ``applyInPandas`` UDF closure for Wyscout SPADL conversion.

    All library imports happen inside the closure so they are available
    on Spark executors without requiring module-level serialisation.

    Args:
        goalkeeper_ids: Wyscout player IDs of goalkeepers.  When provided,
            aerial duels by these players are reclassified as ``keeper_claim``
            by the silly-kicks converter (fixes Wyscout Bug #37).

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _gk_ids = goalkeeper_ids  # captured in closure, serialized to executors

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one game's Wyscout events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_wyscout_events as _adapt

        _spadl_cols = _pd.Index(
            [
                "game_id",
                "match_id",
                "original_event_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "type_id",
                "result_id",
                "bodypart_id",
                "competition_id",
                "season_id",
                "data_source",
                # Multi-source schema parity: Wyscout has no analogues to the
                # StatsBomb-native ``possession`` / ``play_pattern`` /
                # ``under_pressure`` fields, so these columns are NULL on the
                # Wyscout code path.
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.wyscout as _spadl_ws

        home_team_id = int(pdf["home_team_id"].iloc[0])
        # Wyscout uses matchId or match_id depending on ingestion format
        match_id = int(pdf["matchId"].iloc[0]) if "matchId" in pdf.columns else int(pdf["match_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf)
            actions, _report = _spadl_ws.convert_to_actions(adapted, home_team_id, goalkeeper_ids=_gk_ids)
        except Exception as exc:
            msg = f"Wyscout SPADL conversion failed for match_id={match_id}"
            raise RuntimeError(msg) from exc

        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        # Cast original_event_id to str for Spark/PyArrow serialization
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
        # Use explicit nullable pandas dtypes so PyArrow's applyInPandas
        # conversion to LongType / StringType / BooleanType survives without
        # ambiguity (object-dtype all-NA columns can convert inconsistently).
        n = len(actions)
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_wyscout_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read Wyscout events from bronze, adapt, convert to SPADL, write Delta.

    Uses ``groupBy(match_id_col).applyInPandas`` to distribute per-game
    SPADL conversion across Spark executors instead of sequential driver
    loops with ``.toPandas()``.

    Returns whether any data was written.
    """
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    events_table = f"{catalog}.{schema}.wyscout_events"
    matches_table = f"{catalog}.{schema}.wyscout_matches"

    # Check for new games BEFORE pulling metadata tables to driver
    try:
        events_columns = spark.table(events_table).columns
    except Exception:
        logger.exception("Cannot read Wyscout events bronze table")
        return False

    match_id_col = "matchId" if "matchId" in events_columns else "match_id"

    all_game_rows = spark.table(events_table).select(match_id_col).distinct().collect()
    all_game_ids = [int(row[match_id_col]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]

    if not new_game_ids:
        logger.info("Wyscout: all %d games already converted — skipping", len(all_game_ids))
        return False

    # Only now pull metadata tables needed for home_team_id resolution
    try:
        all_matches_pdf = spark.table(matches_table).select("wyId", "teamsData").toPandas()
    except Exception:
        logger.exception("Cannot read Wyscout matches bronze table")
        return False

    home_team_map = resolve_wyscout_home_team_ids(all_matches_pdf)

    # Filter out games where home_team_id is unknown
    new_game_ids = [gid for gid in new_game_ids if home_team_map.get(gid, 0) != 0]

    if not new_game_ids:
        logger.info("Wyscout: all %d games already converted — skipping", len(all_game_ids))
        return False

    logger.info("Wyscout: converting %d new games (of %d total)", len(new_game_ids), len(all_game_ids))

    # Load goalkeeper player IDs for keeper_claim reclassification. If the
    # wyscout_players table doesn't exist yet (first run) we fall back to empty
    # set; any other failure is a real bug and must propagate.
    from ingestion.utils import tolerate_missing_table as _tolerate_missing_table

    goalkeeper_ids: set[int] = set()
    players_table = f"{catalog}.{schema}.wyscout_players"
    with _tolerate_missing_table(
        logger, "Wyscout wyscout_players table missing — keeper_claim reclassification disabled"
    ):
        gk_rows = spark.table(players_table).filter("role:code2 = 'GK'").select("wyId").collect()
        goalkeeper_ids = {int(row["wyId"]) for row in gk_rows}
        logger.info("Wyscout: loaded %d goalkeeper IDs for keeper_claim routing", len(goalkeeper_ids))

    # Build lookup DataFrame with home_team_id, competition_id, season_id per game
    # Derive competition_id and season_id from matches metadata
    match_meta: dict[int, tuple[int, int]] = {}
    if "competitionId" in all_matches_pdf.columns:
        indexed = all_matches_pdf.set_index("wyId")
        comp_ids = indexed["competitionId"].astype(int)
        season_ids = indexed["seasonId"].astype(int) if "seasonId" in indexed.columns else comp_ids * 0
        match_meta = {int(k): (int(c), int(s)) for k, c, s in zip(indexed.index, comp_ids, season_ids, strict=True)}

    lookup_rows = [
        (gid, home_team_map[gid], match_meta.get(gid, (0, 0))[0], match_meta.get(gid, (0, 0))[1])
        for gid in new_game_ids
    ]
    lookup_schema = StructType(
        [
            StructField(match_id_col, LongType()),
            StructField("home_team_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
        ]
    )
    lookup_sdf = spark.createDataFrame(lookup_rows, schema=lookup_schema)

    # Filter events to new games and join metadata
    new_events_sdf = (
        spark.table(events_table)
        .filter(spark_fn.col(match_id_col).isin(new_game_ids))
        .join(lookup_sdf, on=match_id_col, how="inner")
    )

    # Define SPADL output schema (same as StatsBomb).
    # NOTE: 4 statsbomb_* fields are NULL on the Wyscout code path (multi-source parity).
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
            StructField("match_id", LongType()),
            StructField("original_event_id", StringType()),
            StructField("period_id", LongType()),
            StructField("time_seconds", DoubleType()),
            StructField("team_id", LongType()),
            StructField("player_id", LongType()),
            StructField("start_x", DoubleType()),
            StructField("start_y", DoubleType()),
            StructField("end_x", DoubleType()),
            StructField("end_y", DoubleType()),
            StructField("type_id", LongType()),
            StructField("result_id", LongType()),
            StructField("bodypart_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
        ]
    )

    udf_fn = _make_ws_spadl_udf(goalkeeper_ids=goalkeeper_ids or None)
    spadl_sdf = new_events_sdf.groupBy(match_id_col).applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=_make_wyscout_replace_where(new_game_ids),
        logger=logger,
    )

    logger.info("Wyscout: SPADL conversion complete for %d games", len(new_game_ids))
    return True
