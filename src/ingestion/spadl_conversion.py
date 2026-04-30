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
                "action_id",  # LL2: surfaced from silly-kicks convert_to_actions output
                "competition_id",
                "season_id",
                "data_source",
                # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                # LL2 Path B: native string identifiers — populated for ALL sources.
                # For StatsBomb these are stringified ints (numeric native IDs).
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                # PR-LL2 Path B close-out (2026-04-29, ADR-018): silly-kicks 2.0.0
                # sportec tackle qualifier columns. NULL on non-sportec UDFs for
                # multi-source schema parity.
                "tackle_winner_player_id",
                "tackle_winner_team_id",
                "tackle_loser_player_id",
                "tackle_loser_team_id",
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

        # LL2: provider-agnostic post-conversion enrichments. Adds 6 columns:
        # possession_id_heuristic, gk_role, gk_was_distributing, gk_was_engaged,
        # gk_actions_in_possession, defending_gk_player_id. See ADR-016.
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="statsbomb")

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

        # LL2: cast enrichment columns to nullable dtypes for clean PyArrow conversion.
        # action_id and possession_id_heuristic come back as int64 from silly-kicks but
        # synthetic dribble rows can introduce NaN — use Int64 to be safe.
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        # gk_role is pd.Categorical from silly-kicks — convert to object (string) for StringType.
        actions["gk_role"] = actions["gk_role"].astype("object")
        # GK context booleans default to False on non-shot rows (silly-kicks contract).
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        # defending_gk_player_id comes back as float64-with-NaN from silly-kicks; convert to Int64.
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        # LL2 Path B: native string identifiers. StatsBomb has numeric native IDs;
        # stringify them so the column type is STRING across all sources (string-domain
        # joins to dim_teams.native_team_id / dim_competitions.native_competition_id).
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        actions["home_team_id_native"] = str(home_team_id)
        actions["competition_native_id"] = str(competition_id)
        actions["season_native_id"] = str(season_id)
        actions["match_id_native"] = str(match_id)

        # PR-LL2 Path B close-out: tackle qualifier columns NULL-filled on
        # the StatsBomb path (multi-source schema parity; only sportec
        # populates these).
        n = len(actions)
        actions["tackle_winner_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_winner_team_id"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["tackle_loser_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_loser_team_id"] = _pd.array([_pd.NA] * n, dtype="object")

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
            StructField("action_id", LongType()),  # LL2: surfaced from silly-kicks
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            # Provider-namespaced StatsBomb-native fields (silly-kicks 1.5.0+).
            # Wyscout / IDSSE / SkillCorner produce NULL.
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 post-conversion enrichment columns from apply_spadl_enrichments.
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            # LL2 Path B: native string identifiers (Kimball-aligned).
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns. NULL on non-sportec rows.
            StructField("tackle_winner_player_id", LongType()),
            StructField("tackle_winner_team_id", StringType()),
            StructField("tackle_loser_player_id", LongType()),
            StructField("tackle_loser_team_id", StringType()),
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
                "action_id",  # LL2: surfaced from silly-kicks convert_to_actions
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
                # LL2: 6 post-conversion enrichment columns (provider-agnostic — populated for Wyscout).
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                # LL2 Path B: native string identifiers — populated for ALL sources.
                # For Wyscout these are stringified ints (numeric native IDs).
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                # PR-LL2 Path B close-out: silly-kicks 2.0.0 sportec tackle
                # qualifier columns. NULL on Wyscout (multi-source parity).
                "tackle_winner_player_id",
                "tackle_winner_team_id",
                "tackle_loser_player_id",
                "tackle_loser_team_id",
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

        # LL2: provider-agnostic post-conversion enrichments (populated for Wyscout).
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="wyscout")

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

        # LL2: cast enrichment columns to nullable dtypes (same pattern as StatsBomb path).
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        # LL2 Path B: native string identifiers. Wyscout has numeric native IDs;
        # stringify for cross-provider STRING-domain joins to dim_teams /
        # dim_competitions on (provider, native_id).
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        actions["home_team_id_native"] = str(home_team_id)
        actions["competition_native_id"] = str(competition_id)
        actions["season_native_id"] = str(season_id)
        actions["match_id_native"] = str(match_id)

        # PR-LL2 Path B close-out: tackle qualifier columns NULL-filled on
        # the Wyscout path (multi-source schema parity).
        actions["tackle_winner_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_winner_team_id"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["tackle_loser_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_loser_team_id"] = _pd.array([_pd.NA] * n, dtype="object")

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

    # Only now pull metadata tables needed for home_team_id + competition + season resolution.
    # PR-LL2 Path B close-out (2026-04-29, ADR-018): explicitly select competitionId
    # and seasonId so the downstream match_meta dict gets RAW wyscout-native values
    # (pre-close-out the SELECT was missing those columns, causing match_meta to
    # stay empty and all wyscout SPADL rows to default to (competition_id, season_id)
    # = (0, 0). Bronze is now source-faithful per ADR-016 — staging applies any
    # cross-provider mapping if needed, not the bronze writer.
    try:
        all_matches_pdf = spark.table(matches_table).select("wyId", "teamsData", "competitionId", "seasonId").toPandas()
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

    # Build lookup DataFrame with home_team_id, competition_id, season_id per game.
    # PR-LL2 Path B close-out (2026-04-29): we explicitly SELECTed competitionId and
    # seasonId above, so this block is no longer guarded — the columns are
    # guaranteed present. Pre-close-out the guard `if "competitionId" in ...columns`
    # was always False because the SELECT was missing those columns, leading to
    # silent fallback to (0, 0). Bronze is RAW-source-faithful per ADR-016/-018.
    indexed = all_matches_pdf.set_index("wyId")
    comp_ids = indexed["competitionId"].astype(int)
    season_ids = indexed["seasonId"].astype(int)
    match_meta: dict[int, tuple[int, int]] = {
        int(k): (int(c), int(s)) for k, c, s in zip(indexed.index, comp_ids, season_ids, strict=True)
    }

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
            StructField("action_id", LongType()),  # LL2
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            # LL2: 6 enrichment columns
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            # LL2 Path B: native string identifiers (Kimball-aligned).
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns. NULL on Wyscout rows.
            StructField("tackle_winner_player_id", LongType()),
            StructField("tackle_winner_team_id", StringType()),
            StructField("tackle_loser_player_id", LongType()),
            StructField("tackle_loser_team_id", StringType()),
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


# ---------------------------------------------------------------------------
# IDSSE (DFL Bundesliga / Sportec) SPADL conversion — LL2 Path B
# ---------------------------------------------------------------------------
#
# Uses silly-kicks 1.7.0+ ``silly_kicks.spadl.sportec.convert_to_actions``
# (DataFrame-based) against bronze.idsse_events. Native STRING IDs flow
# through unchanged on the new ``*_native`` SPADL columns; legacy BIGINT
# columns are NULL for IDSSE EXCEPT ``match_id`` / ``game_id`` which are
# deterministically hashed via ``hash_native_id_to_bigint`` so the
# downstream VAEP scoring ``groupBy("match_id").applyInPandas(...)``
# can dispatch IDSSE rows correctly. The original strings are preserved
# in the bronze.idsse_events.match_id column.


def _make_idsse_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific IDSSE matches.

    IDSSE bronze.idsse_events.match_id is STRING (e.g. ``'idsse_J03WMX'``)
    but bronze.spadl_actions.match_id is BIGINT — we hash the strings via
    ``hash_native_id_to_bigint`` so this predicate quotes BIGINTs.
    """
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'idsse' AND match_id IN ({ids_sql})"


def _make_idsse_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for IDSSE SPADL conversion.

    All silly-kicks library imports happen inside the closure so they are
    available on Spark executors without module-level serialisation.

    The UDF expects bronze.idsse_events rows that have been augmented with
    PR-LL2 Path B columns: ``competition_native_id``, ``season_native_id``,
    ``home_team_id_native``, ``team_id_native`` (sourced from the DFL
    matchinformation XML during ingestion).
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one IDSSE match's events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import (
            adapt_idsse_events_for_silly_kicks as _adapt,
        )
        from ingestion.spadl_adapter import (
            hash_native_id_to_bigint as _hash_id,
        )

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
                "action_id",
                "competition_id",
                "season_id",
                "data_source",
                # statsbomb_* NULL on the IDSSE code path (multi-source parity).
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                # LL2: 6 post-conversion enrichment columns (provider-agnostic).
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                # LL2 Path B: native string identifiers — populated for IDSSE.
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                # PR-LL2 Path B close-out (2026-04-29, ADR-018): silly-kicks 2.0.0
                # sportec tackle qualifier passthrough. Populated for IDSSE rows
                # where DFL XML has tackle_winner_*; NaN otherwise.
                "tackle_winner_player_id",
                "tackle_winner_team_id",
                "tackle_loser_player_id",
                "tackle_loser_team_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.sportec as _spadl_sportec

        # Match-level metadata (constant across rows of this match) sourced
        # from bronze.idsse_events Path B columns.
        match_id_str = str(pdf["match_id"].iloc[0])
        home_team_id_native = str(pdf["home_team_id_native"].iloc[0])
        away_team_id_native = str(pdf["away_team_id_native"].iloc[0])
        competition_native_id = str(pdf["competition_native_id"].iloc[0])
        season_native_id = str(pdf["season_native_id"].iloc[0])

        try:
            adapted = _adapt(pdf)
            # silly-kicks's ``home_team_id`` is used by ``_fix_direction_of_play``
            # for string equality with the ``team`` column. bronze.idsse_events.team
            # carries 'home' / 'away' / 'unknown' labels — pass the literal
            # 'home' string so direction normalisation matches by value.
            actions, _report = _spadl_sportec.convert_to_actions(
                adapted,
                home_team_id="home",
            )
        except Exception as exc:
            msg = f"IDSSE SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        # LL2 Path B: derive team_id_native from silly-kicks's output team_id
        # BEFORE we NULL-fill the legacy BIGINT team_id column.
        #
        # silly-kicks 2.0.0 ADR-001 ("caller's identifier conventions are
        # sacred") guarantees that sportec.convert_to_actions's output
        # ``team_id`` mirrors the input ``team`` column verbatim — no
        # override from ``tackle_winner_team`` qualifier. Pre-2.0.0 (1.7.0
        # specifically), TacklingGame events with ``tackle_winner``
        # populated had their team rewritten to the raw DFL CLU id
        # ('DFL-CLU-XXXXXX'), which broke this 'home'/'away' mapper for
        # ~56% of IDSSE TacklingGame rows (1412/2522 NULL team_id_native
        # — see PR-LL2-Path-B-close-out spec Bug #3). PR-LL2-Path-B-close-out
        # bumps to silly-kicks 2.0.0, eliminating the need for a
        # ``DFL-``-prefixed passthrough branch.
        #
        # The DFL CLU ids of the winner / loser teams DO surface in the
        # bronze.spadl_actions ``tackle_winner_team_id`` /
        # ``tackle_loser_team_id`` columns (silly-kicks 2.0.0
        # SPORTEC_SPADL_COLUMNS) for analytics consumers that need them.
        def _team_label_to_dfl_id(team_label: object) -> str | None:
            if team_label == "home":
                return home_team_id_native
            if team_label == "away":
                return away_team_id_native
            return None

        actions["team_id_native"] = actions["team_id"].map(_team_label_to_dfl_id).astype("string")

        # silly-kicks's sportec converter emits ``game_id`` and ``team_id`` as
        # the input string values. luxury-lakehouse's bronze.spadl_actions
        # legacy BIGINTs require a deterministic hash for match_id+game_id;
        # team_id / player_id / competition_id / season_id are NULL for IDSSE
        # (Kimball joins use the _native STRING columns instead).
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "idsse"

        # LL2 post-conversion enrichments (provider-agnostic).
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="idsse")

        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        # LL2 enrichment column dtype casts.
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        # LL2 Path B match-level identifiers (constants across all rows
        # including synthetic dribbles).
        actions["home_team_id_native"] = home_team_id_native
        actions["competition_native_id"] = competition_native_id
        actions["season_native_id"] = season_native_id
        actions["match_id_native"] = match_id_str

        # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
        # converter emits 4 tackle qualifier columns directly on the actions
        # DataFrame (SPORTEC_SPADL_COLUMNS extension). NaN on non-tackle rows
        # + on tackle rows where the DFL XML's qualifier was absent. Cast to
        # nullable Int64/object dtypes for clean PyArrow → Spark conversion.
        if "tackle_winner_player_id" in actions.columns:
            actions["tackle_winner_player_id"] = actions["tackle_winner_player_id"].astype("Int64")
        else:
            actions["tackle_winner_player_id"] = _pd.array([_pd.NA] * len(actions), dtype="Int64")
        if "tackle_winner_team_id" in actions.columns:
            actions["tackle_winner_team_id"] = actions["tackle_winner_team_id"].astype("object")
        else:
            actions["tackle_winner_team_id"] = _pd.array([_pd.NA] * len(actions), dtype="object")
        if "tackle_loser_player_id" in actions.columns:
            actions["tackle_loser_player_id"] = actions["tackle_loser_player_id"].astype("Int64")
        else:
            actions["tackle_loser_player_id"] = _pd.array([_pd.NA] * len(actions), dtype="Int64")
        if "tackle_loser_team_id" in actions.columns:
            actions["tackle_loser_team_id"] = actions["tackle_loser_team_id"].astype("object")
        else:
            actions["tackle_loser_team_id"] = _pd.array([_pd.NA] * len(actions), dtype="object")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_idsse_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read IDSSE events from bronze, adapt, convert to SPADL via silly-kicks 1.7.0 sportec, write Delta.

    LL2 Path B: bronze.idsse_events.match_id is STRING (e.g. ``'idsse_J03WMX'``);
    we hash to BIGINT via ``hash_native_id_to_bigint`` so the downstream
    BIGINT-typed bronze.spadl_actions.match_id column accepts the value and
    VAEP ``groupBy("match_id")`` continues to work.

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

    from ingestion.spadl_adapter import hash_native_id_to_bigint

    events_table = f"{catalog}.{schema}.idsse_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read IDSSE events bronze table")
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    # existing_matches contains hashed BIGINT match_ids from
    # bronze.spadl_actions; hash IDSSE strings the same way for comparison.
    new_match_ids: list[str] = [mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches]

    if not new_match_ids:
        logger.info("IDSSE: all %d matches already converted — skipping", len(all_match_ids))
        return False

    logger.info("IDSSE: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))

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
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns. Populated for tackle rows where the
            # DFL XML has the qualifier; NULL elsewhere.
            StructField("tackle_winner_player_id", LongType()),
            StructField("tackle_winner_team_id", StringType()),
            StructField("tackle_loser_player_id", LongType()),
            StructField("tackle_loser_team_id", StringType()),
        ]
    )

    udf_fn = _make_idsse_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    hashed_new_ids = [hash_native_id_to_bigint(mid) for mid in new_match_ids]
    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=_make_idsse_replace_where(hashed_new_ids),
        logger=logger,
    )

    logger.info("IDSSE: SPADL conversion complete for %d matches", len(new_match_ids))
    return True


# ---------------------------------------------------------------------------
# Metrica SPADL conversion — LL2 Path B
# ---------------------------------------------------------------------------
#
# Mirrors the IDSSE pattern: silly-kicks 1.7.0+ ``silly_kicks.spadl.metrica.
# convert_to_actions`` against bronze.metrica_events. ``match_id`` is the
# Metrica string (e.g. ``'Sample_Game_1'``), hashed deterministically for
# the legacy BIGINT column. team labels are ``'Home'`` / ``'Away'``
# (capitalised, distinct from IDSSE's lowercase). Coordinates are
# normalised [0, 1] in bronze; the adapter scales by per-match pitch dims.


def _make_metrica_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific Metrica matches."""
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'metrica' AND match_id IN ({ids_sql})"


def _make_metrica_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for Metrica SPADL conversion.

    The UDF expects bronze.metrica_events rows with PR-LL2 Path B columns:
    ``competition_native_id``, ``season_native_id``, ``home_team_id_native``,
    ``away_team_id_native``, ``team_id_native`` (synthetic IDs synthesized
    during ingestion since the open-data sample lacks real club IDs).
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one Metrica match's events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import (
            adapt_metrica_events_for_silly_kicks as _adapt,
        )
        from ingestion.spadl_adapter import (
            hash_native_id_to_bigint as _hash_id,
        )

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
                "action_id",
                "competition_id",
                "season_id",
                "data_source",
                "statsbomb_possession_id",
                "statsbomb_possession_team_id",
                "statsbomb_play_pattern",
                "statsbomb_under_pressure",
                "possession_id_heuristic",
                "gk_role",
                "gk_was_distributing",
                "gk_was_engaged",
                "gk_actions_in_possession",
                "defending_gk_player_id",
                "team_id_native",
                "home_team_id_native",
                "competition_native_id",
                "season_native_id",
                "match_id_native",
                # PR-LL2 Path B close-out: silly-kicks 2.0.0 sportec tackle
                # qualifier columns. NULL on Metrica (multi-source parity).
                "tackle_winner_player_id",
                "tackle_winner_team_id",
                "tackle_loser_player_id",
                "tackle_loser_team_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.metrica as _spadl_metrica

        match_id_str = str(pdf["match_id"].iloc[0])
        home_team_id_native = str(pdf["home_team_id_native"].iloc[0])
        away_team_id_native = str(pdf["away_team_id_native"].iloc[0])
        competition_native_id = str(pdf["competition_native_id"].iloc[0])
        season_native_id = str(pdf["season_native_id"].iloc[0])

        try:
            adapted = _adapt(pdf)
            # silly-kicks's metrica converter takes ``home_team_id`` as the
            # value to match against the ``team`` column for direction-of-play.
            # bronze.metrica_events.team is 'Home' / 'Away' (capitalised).
            actions, _report = _spadl_metrica.convert_to_actions(
                adapted,
                home_team_id="Home",
            )
        except Exception as exc:
            msg = f"Metrica SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        # LL2 Path B: derive team_id_native from silly-kicks's team_id output
        # BEFORE NULL-filling the legacy team_id BIGINT. silly-kicks's metrica
        # converter (line 252 of upstream) emits team_id as the input ``team``
        # column verbatim; map 'Home'/'Away' to the synthetic native IDs.
        def _team_label_to_native_id(team_label: object) -> str | None:
            if team_label == "Home":
                return home_team_id_native
            if team_label == "Away":
                return away_team_id_native
            return None

        actions["team_id_native"] = actions["team_id"].map(_team_label_to_native_id).astype("string")

        # Hash match_id for legacy BIGINT compatibility; NULL-fill the other
        # legacy BIGINT IDs (Kimball joins use _native cols).
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "metrica"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="metrica")

        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # NULL-fill the StatsBomb-namespaced fields for multi-source parity.
        actions["statsbomb_possession_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_possession_team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["statsbomb_play_pattern"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["statsbomb_under_pressure"] = _pd.array([_pd.NA] * n, dtype="boolean")

        # LL2 enrichment column dtype casts.
        actions["action_id"] = actions["action_id"].astype("Int64")
        actions["possession_id_heuristic"] = actions["possession_id_heuristic"].astype("Int64")
        actions["gk_role"] = actions["gk_role"].astype("object")
        actions["gk_was_distributing"] = actions["gk_was_distributing"].astype("boolean")
        actions["gk_was_engaged"] = actions["gk_was_engaged"].astype("boolean")
        actions["gk_actions_in_possession"] = actions["gk_actions_in_possession"].astype("Int64")
        actions["defending_gk_player_id"] = actions["defending_gk_player_id"].astype("Int64")

        # LL2 Path B match-level constants.
        actions["home_team_id_native"] = home_team_id_native
        actions["competition_native_id"] = competition_native_id
        actions["season_native_id"] = season_native_id
        actions["match_id_native"] = match_id_str

        # PR-LL2 Path B close-out: tackle qualifier columns NULL-filled on
        # the Metrica path (multi-source schema parity).
        actions["tackle_winner_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_winner_team_id"] = _pd.array([_pd.NA] * n, dtype="object")
        actions["tackle_loser_player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["tackle_loser_team_id"] = _pd.array([_pd.NA] * n, dtype="object")

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_metrica_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
) -> bool:
    """Read Metrica events from bronze, adapt, convert to SPADL, write Delta.

    LL2 Path B: bronze.metrica_events.match_id is STRING (e.g.
    ``'Sample_Game_1'``); we hash to BIGINT via ``hash_native_id_to_bigint``
    for the legacy BIGINT column compat.

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

    from ingestion.spadl_adapter import hash_native_id_to_bigint

    events_table = f"{catalog}.{schema}.metrica_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read Metrica events bronze table")
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    new_match_ids: list[str] = [mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches]

    if not new_match_ids:
        logger.info("Metrica: all %d matches already converted — skipping", len(all_match_ids))
        return False

    logger.info("Metrica: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))

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
            StructField("action_id", LongType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
            StructField("statsbomb_possession_id", LongType()),
            StructField("statsbomb_possession_team_id", LongType()),
            StructField("statsbomb_play_pattern", StringType()),
            StructField("statsbomb_under_pressure", BooleanType()),
            StructField("possession_id_heuristic", LongType()),
            StructField("gk_role", StringType()),
            StructField("gk_was_distributing", BooleanType()),
            StructField("gk_was_engaged", BooleanType()),
            StructField("gk_actions_in_possession", LongType()),
            StructField("defending_gk_player_id", LongType()),
            StructField("team_id_native", StringType()),
            StructField("home_team_id_native", StringType()),
            StructField("competition_native_id", StringType()),
            StructField("season_native_id", StringType()),
            StructField("match_id_native", StringType()),
            # PR-LL2 Path B close-out (2026-04-29): silly-kicks 2.0.0 sportec
            # tackle qualifier columns. NULL on Metrica rows.
            StructField("tackle_winner_player_id", LongType()),
            StructField("tackle_winner_team_id", StringType()),
            StructField("tackle_loser_player_id", LongType()),
            StructField("tackle_loser_team_id", StringType()),
        ]
    )

    udf_fn = _make_metrica_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    hashed_new_ids = [hash_native_id_to_bigint(mid) for mid in new_match_ids]
    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where=_make_metrica_replace_where(hashed_new_ids),
        logger=logger,
    )

    logger.info("Metrica: SPADL conversion complete for %d matches", len(new_match_ids))
    return True
