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
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.spadl_adapter import (
    resolve_statsbomb_home_team_ids,
    resolve_wyscout_home_team_ids,
)
from ingestion.utils import write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

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


def _make_sb_spadl_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
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
                "player_id_native",
                # PR-LL2 Path B close-out (2026-04-29, ADR-018): silly-kicks 2.0.0
                # sportec tackle qualifier columns. NULL on non-sportec UDFs for
                # multi-source schema parity.
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
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

        if _report.unrecognized_counts:
            logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id,
                _report.unrecognized_counts,
            )

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

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives,
            apply_player_id_native,
            cast_enrichment_dtypes,
            null_fill_tackle_qualifiers,
        )

        actions = cast_enrichment_dtypes(actions)
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        actions = apply_player_id_native(actions, source="statsbomb")
        actions = apply_match_level_natives(
            actions,
            home_team_id_native=str(home_team_id),
            competition_native_id=str(competition_id),
            season_native_id=str(season_id),
            match_id_native=str(match_id),
        )
        n = len(actions)
        actions = null_fill_tackle_qualifiers(actions, n=n)

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_statsbomb_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
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
    if match_id_filter is not None:
        new_game_ids = [gid for gid in new_game_ids if gid in match_id_filter]

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
            StructField("player_id_native", StringType()),
            # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec
            # tackle qualifier columns. NULL on non-sportec rows.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
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


def _make_ws_spadl_udf(goalkeeper_ids: set[int] | None = None) -> Callable[[pd.DataFrame], pd.DataFrame]:
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
                "player_id_native",
                # PR-LL2 Path B close-out: silly-kicks 2.0.0 sportec tackle
                # qualifier columns. NULL on Wyscout (multi-source parity).
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
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

        if _report.unrecognized_counts:
            logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id,
                _report.unrecognized_counts,
            )

        actions["match_id"] = match_id
        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="wyscout")
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives,
            apply_player_id_native,
            cast_enrichment_dtypes,
            null_fill_statsbomb_columns,
            null_fill_tackle_qualifiers,
        )

        n = len(actions)
        actions = null_fill_statsbomb_columns(actions, n=n)
        actions = cast_enrichment_dtypes(actions)
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        actions = apply_player_id_native(actions, source="wyscout")
        actions = apply_match_level_natives(
            actions,
            home_team_id_native=str(home_team_id),
            competition_native_id=str(competition_id),
            season_native_id=str(season_id),
            match_id_native=str(match_id),
        )
        actions = null_fill_tackle_qualifiers(actions, n=n)

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_wyscout_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
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
    if match_id_filter is not None:
        new_game_ids = [gid for gid in new_game_ids if gid in match_id_filter]

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
            StructField("player_id_native", StringType()),
            # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec
            # tackle qualifier columns. NULL on Wyscout rows.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
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


def _make_idsse_spadl_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
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
        from silly_kicks.providers.sportec import (
            shape_events_to_native as _adapt,
        )

        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
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
                "player_id_native",
                # PR-Cycle-A.4 (2026-04-30, ADR-018 alignment): silly-kicks 2.5.0
                # sportec tackle qualifier passthrough. silly-kicks 2.5.0
                # emits tackle player/team IDs as NATIVE STRINGS (DFL OBJ /
                # CLU IDs); we surface them as ``<col>_native`` (STRING) +
                # ``<col>_key`` (BIGINT surrogate via ``hash_native_id_to_bigint``)
                # to match the LL2 Path B convention (``team_id_native`` /
                # ``team_key`` pattern). NaN on non-tackle rows + on
                # non-IDSSE provider rows.
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
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
            # silly-kicks 3.0.1 (PR-S23) requires explicit per-period direction
            # for Sportec. Derive home_team_start_left authoritatively from the
            # DFL XML's KickOff TeamLeft/TeamRight attributes (captured in
            # bronze as kickoff_team_left). home_team_id="home" is the LABEL
            # used in spadl_actions output; home_team_id_native is the DFL CLU
            # id used for the kickoff comparison.
            # silly-kicks 4.0.0 (PR-S70): symmetric ET guard requires the ET-direction
            # flag too. Derive from the extraTimeFirstHalf KickOff event when ET periods
            # exist; otherwise None (silly-kicks 4.0 accepts None when no ET present).
            from silly_kicks.providers.sportec import (
                derive_idsse_home_team_start_left,
                derive_idsse_home_team_start_left_extratime,
            )

            home_start_left = derive_idsse_home_team_start_left(adapted, home_team_id_native)
            home_start_left_et = derive_idsse_home_team_start_left_extratime(adapted, home_team_id_native)
            actions, _report = _spadl_sportec.convert_to_actions(
                adapted,
                home_team_id="home",
                home_team_start_left=home_start_left,
                home_team_start_left_extratime=home_start_left_et,
            )
        except Exception as exc:
            msg = f"IDSSE SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

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

        # PR-LL3 S2: player_id_native - DFL OBJ IDs, already string-shaped.
        # Must appear BEFORE the legacy BIGINT NULL-fill below.
        from ingestion.spadl_udf_shared import (
            apply_match_level_natives as _apply_match_natives,
        )
        from ingestion.spadl_udf_shared import (
            apply_player_id_native as _apply_pid_native,
        )
        from ingestion.spadl_udf_shared import (
            cast_enrichment_dtypes as _cast_enrichment,
        )
        from ingestion.spadl_udf_shared import (
            null_fill_statsbomb_columns as _null_fill_sb,
        )

        actions = _apply_pid_native(actions, source="idsse")

        # silly-kicks's sportec converter emits ``game_id`` and ``team_id`` as
        # the input string values. luxury-lakehouse's bronze.spadl_actions
        # legacy BIGINTs require a deterministic hash for match_id+game_id;
        # player_id / competition_id / season_id are NULL for IDSSE
        # (Kimball joins use the _native STRING columns instead).
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        # team_id: hash from team_id_native (populated via _team_label_to_dfl_id above).
        # Edge case: silly-kicks emits non-"home"/"away" labels for some
        # freekick_short events → NULL team_id_native. Fill with sentinel.
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "idsse"

        # LL2 post-conversion enrichments (provider-agnostic).
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="idsse")

        actions["original_event_id"] = actions["original_event_id"].astype(str)

        actions = _null_fill_sb(actions, n=n)
        actions = _cast_enrichment(actions)
        actions = _apply_match_natives(
            actions,
            home_team_id_native=home_team_id_native,
            competition_native_id=competition_native_id,
            season_native_id=season_native_id,
            match_id_native=match_id_str,
        )

        # PR-Cycle-A.4 (2026-04-30, ADR-018 alignment): silly-kicks 2.5.0
        # sportec converter emits all 4 tackle qualifier columns as NATIVE
        # STRINGS (DFL OBJ ids for players, DFL CLU ids for teams) directly
        # on the actions DataFrame. We surface them via the LL2 Path B
        # convention: ``<col>_native`` (STRING) + ``<col>_key`` (BIGINT
        # surrogate via ``hash_native_id_to_bigint``) so the bronze schema
        # is consistent with ``team_id_native`` / ``team_key`` (Kimball
        # joins use the BIGINT keys against ``dim_players.player_key`` /
        # ``dim_teams.team_key``; the native strings preserve provenance).
        from typing import Any as _Any

        from ingestion.spadl_adapter import hash_native_id_to_bigint as _hash_native

        def _hash_or_na(v: _Any) -> _Any:
            # Param/return are Any (rather than object) because the value
            # comes from a pandas Series.map and may be str | float (NaN) |
            # pd.NAType; pandas-stubs's pd.isna() accepts Scalar but not
            # the broader ``object`` annotation pyright would otherwise
            # infer here.
            if v is None or _pd.isna(v):
                return _pd.NA
            s = str(v)
            return _hash_native(s) if s else _pd.NA

        for native_col, key_col, sk_col in (
            ("tackle_winner_player_id_native", "tackle_winner_player_key", "tackle_winner_player_id"),
            ("tackle_winner_team_id_native", "tackle_winner_team_key", "tackle_winner_team_id"),
            ("tackle_loser_player_id_native", "tackle_loser_player_key", "tackle_loser_player_id"),
            ("tackle_loser_team_id_native", "tackle_loser_team_key", "tackle_loser_team_id"),
        ):
            if sk_col in actions.columns:
                actions[native_col] = actions[sk_col].astype("string")
                actions[key_col] = actions[native_col].map(_hash_or_na).astype("Int64")
            else:
                actions[native_col] = _pd.array([_pd.NA] * len(actions), dtype="string")
                actions[key_col] = _pd.array([_pd.NA] * len(actions), dtype="Int64")

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_idsse_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
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
    if match_id_filter is not None:
        new_match_ids = [mid for mid in new_match_ids if hash_native_id_to_bigint(mid) in match_id_filter]

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
            StructField("player_id_native", StringType()),
            # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec
            # tackle qualifier columns. Populated for tackle rows where the
            # DFL XML has the qualifier; NULL elsewhere.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
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


def _make_metrica_spadl_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
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
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
        )
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
                "player_id_native",
                # PR-LL2 Path B close-out: silly-kicks 2.0.0 sportec tackle
                # qualifier columns. NULL on Metrica (multi-source parity).
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
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
            # silly-kicks 3.0.1 (PR-S23) requires explicit per-period direction
            # for Metrica. Bronze does not capture a kickoff-side flag, so
            # infer from period-1 SHOT positions (sparse but unambiguous —
            # teams shoot toward the opponent goal).
            # silly-kicks 4.0.0 (PR-S70): symmetric ET guard requires the ET-direction
            # flag too. Empirical from period-3 SHOT positions; None when no ET.
            from ingestion.spadl_adapter import (
                derive_metrica_home_team_start_left,
                derive_metrica_home_team_start_left_extratime,
            )

            home_start_left = derive_metrica_home_team_start_left(adapted, home_team_value="Home")
            home_start_left_et = derive_metrica_home_team_start_left_extratime(adapted, home_team_value="Home")
            actions, _report = _spadl_metrica.convert_to_actions(
                adapted,
                home_team_id="Home",
                home_team_start_left=home_start_left,
                home_team_start_left_extratime=home_start_left_et,
            )
        except Exception as exc:
            msg = f"Metrica SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

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

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives,
            apply_player_id_native,
            cast_enrichment_dtypes,
            null_fill_statsbomb_columns,
            null_fill_tackle_qualifiers,
        )

        # PR-LL3 S2: player_id_native - MUST precede legacy BIGINT NULL-fill.
        actions = apply_player_id_native(actions, source="metrica")

        # Hash match_id for legacy BIGINT compatibility; NULL-fill the other
        # legacy BIGINT IDs (Kimball joins use _native cols).
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)
        # team_id: hash from team_id_native (populated via _team_label_to_native_id above).
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "metrica"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="metrica")
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        actions = null_fill_statsbomb_columns(actions, n=n)
        actions = cast_enrichment_dtypes(actions)
        actions = apply_match_level_natives(
            actions,
            home_team_id_native=home_team_id_native,
            competition_native_id=competition_native_id,
            season_native_id=season_native_id,
            match_id_native=match_id_str,
        )
        actions = null_fill_tackle_qualifiers(actions, n=n)

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_metrica_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
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
    if match_id_filter is not None:
        new_match_ids = [mid for mid in new_match_ids if hash_native_id_to_bigint(mid) in match_id_filter]

    if not new_match_ids:
        logger.info("Metrica: all %d matches already converted — skipping", len(all_match_ids))
        return False

    logger.info("Metrica: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))

    # ADR-040 (Metrica time-base): bronze.metrica_events.start_time_s/end_time_s sit on the
    # ABSOLUTE match clock (P2 ~2885s), not silly-kicks' canonical PERIOD-RELATIVE convention —
    # neither silly-kicks' Metrica converter nor the lakehouse adapter re-bases them, so the
    # AC-1 work-unit time-base guard correctly aborts every Metrica unit. Re-base the event
    # time off the CONTINUOUS frame number, keyed on each period's FIRST tracking frame (read
    # from bronze.metrica_tracking), so SPADL time_seconds resets per period AND aligns exactly
    # with the AC frame "timestamp" — which action_context._process_tracking_match /
    # pipeline.run_work_unit re-base off the SAME min(frame) per (match,period). Frame-number
    # based so Sample_Game_3's hand-curated P2 timestamp reset is irrelevant. start_frame is
    # always present; end_frame may be NULL on instantaneous events → coalesce to start_frame
    # (zero-duration, never NULL).
    tracking_table = f"{catalog}.{schema}.metrica_tracking"
    period_ref = (
        spark.table(tracking_table)
        .filter(spark_fn.col("period").isNotNull() & spark_fn.col("match_id").isin(new_match_ids))
        .groupBy("match_id", "period")
        .agg(
            spark_fn.min("frame").alias("_period_start_frame"),
            spark_fn.first("frame_rate", ignorenulls=True).alias("_metrica_frame_rate"),
        )
    )
    new_events_sdf = new_events_sdf.join(period_ref, on=["match_id", "period"], how="left")
    # Fail loud (ADR-002 §5): Metrica is a tracking provider — a (match,period) with events but
    # no tracking frames is a broken ingest, not a licence to emit an absolute-clock SPADL action.
    _missing = (
        new_events_sdf.filter(spark_fn.col("_period_start_frame").isNull())
        .select("match_id", "period")
        .distinct()
        .collect()
    )
    if _missing:
        pairs = ", ".join(f"{r['match_id']}:p{r['period']}" for r in _missing)
        msg = f"Metrica SPADL time re-base: bronze.metrica_tracking has no frames for (match,period): {pairs}"
        raise RuntimeError(msg)
    _fr = spark_fn.coalesce(spark_fn.col("_metrica_frame_rate").cast("double"), spark_fn.lit(25.0))
    _min_frame = spark_fn.col("_period_start_frame").cast("double")
    new_events_sdf = (
        new_events_sdf.withColumn("start_time_s", (spark_fn.col("start_frame").cast("double") - _min_frame) / _fr)
        .withColumn(
            "end_time_s",
            (spark_fn.coalesce(spark_fn.col("end_frame"), spark_fn.col("start_frame")).cast("double") - _min_frame)
            / _fr,
        )
        .drop("_period_start_frame", "_metrica_frame_rate")
    )

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
            StructField("player_id_native", StringType()),
            # PR-Cycle-A.4 (2026-04-30, ADR-018): silly-kicks 2.5.0 sportec
            # tackle qualifier columns. NULL on Metrica rows.
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
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


# ---------------------------------------------------------------------------
# SkillCorner SPADL conversion
# ---------------------------------------------------------------------------


def _make_skillcorner_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific SkillCorner matches."""
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'skillcorner' AND match_id IN ({ids_sql})"


def _make_skillcorner_spadl_udf(*, match_metadata: dict[str, object]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the applyInPandas UDF closure for SkillCorner SPADL conversion.

    The silly-kicks SkillCorner converter API differs from other providers:
    - Takes (events, match_metadata) instead of (events, home_team_id)
    - Uses POSSESSION_PERSPECTIVE convention (to_spadl_ltr is a no-op)
    - No home_team_start_left kwarg needed

    Args:
        match_metadata: Dict with keys "id", "pitch_length", "pitch_width",
            "home_team" (nested: {"id": int}). Built driver-side from
            bronze.skillcorner_matches. Captured in closure for executors.
    """
    # CPython closure scoping: _match_meta is captured by value (reference to
    # the dict object). The dict is frozen at UDF-construction time on the
    # driver; Spark serializes it into each executor's closure. This is safe
    # because we never mutate _match_meta inside the UDF.
    _match_meta = match_metadata

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one SkillCorner match's events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
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
                "player_id_native",
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.skillcorner as _spadl_sc

        match_id_str = str(pdf["match_id"].iloc[0])

        try:
            actions, _report = _spadl_sc.convert_to_actions(pdf, _match_meta)
        except Exception as exc:
            msg = f"SkillCorner SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            # NOTE: Inside an applyInPandas UDF, Python logging routes to
            # executor stderr (visible in Spark driver logs), NOT the structured
            # JSON pipeline logger. This is acceptable for diagnostics.
            _udf_logger = logging.getLogger(__name__)
            _udf_logger.warning(
                "SPADL conversion unrecognized event types for match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

        # ADR-018: native IDs via canonical generators
        from shared.identifiers import (
            skillcorner_native_match_id,
            skillcorner_native_team_id,
        )

        actions["team_id_native"] = (
            actions["team_id"]
            .apply(lambda tid: skillcorner_native_team_id(tid) if _pd.notna(tid) else _pd.NA)
            .astype("string")
        )

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives,
            apply_player_id_native,
            cast_enrichment_dtypes,
            null_fill_statsbomb_columns,
            null_fill_tackle_qualifiers,
        )

        # player_id_native MUST precede legacy BIGINT NULL-fill
        actions = apply_player_id_native(actions, source="skillcorner")

        # Hash match_id for legacy BIGINT; NULL-fill other legacy BIGINTs
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        null_team_mask = actions["team_id_native"].isna()
        if null_team_mask.any():
            _udf_logger = logging.getLogger(__name__)
            _udf_logger.warning(
                "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
                actions.loc[null_team_mask, "type_id"].unique().tolist(),
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
        n = len(actions)
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["data_source"] = "skillcorner"

        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="skillcorner")
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        actions = null_fill_statsbomb_columns(actions, n=n)
        actions = cast_enrichment_dtypes(actions)
        actions = apply_match_level_natives(
            actions,
            home_team_id_native=str(_match_meta["home_team"]["id"]),  # type: ignore[index]
            competition_native_id=_pd.NA,  # type: ignore[arg-type]  # SkillCorner has no competition_native_id in events
            season_native_id=_pd.NA,  # type: ignore[arg-type]
            match_id_native=skillcorner_native_match_id(match_id_str),
        )
        actions = null_fill_tackle_qualifiers(actions, n=n)

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_skillcorner_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
) -> bool:
    """Read SkillCorner events from bronze, convert to SPADL, write Delta.

    Unlike IDSSE/Metrica, the SkillCorner converter needs a match_metadata
    dict built from bronze.skillcorner_matches. This is resolved driver-side
    per match, then captured in the UDF closure.

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
    from ingestion.utils import tolerate_missing_table

    events_table = f"{catalog}.{schema}.skillcorner_events"
    matches_table = f"{catalog}.{schema}.skillcorner_matches"

    # Check if events table exists
    with tolerate_missing_table(logger, "SkillCorner events bronze table not found -- skipping SPADL"):
        events_sdf = spark.table(events_table)

    if "events_sdf" not in dir():  # tolerate_missing_table suppressed the error
        return False

    all_match_rows = events_sdf.select("match_id").distinct().collect()  # type: ignore[possibly-undefined]
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    new_match_ids: list[str] = [mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches]
    if match_id_filter is not None:
        new_match_ids = [mid for mid in new_match_ids if hash_native_id_to_bigint(mid) in match_id_filter]

    if not new_match_ids:
        logger.info("SkillCorner: all %d matches already converted -- skipping", len(all_match_ids))
        return False

    logger.info("SkillCorner: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    wrote_any = False
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
            StructField("player_id_native", StringType()),
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
        ]
    )

    # TRADEOFF: This loops N applyInPandas calls (one per match) instead of
    # batching all matches in a single groupBy("match_id").applyInPandas like
    # IDSSE/Metrica. The overhead is ~1-2s Spark job-submission latency per match.
    # Acceptable because: (a) A-League has ~27 matches/season, not thousands;
    # (b) each match needs a unique match_metadata dict in the closure; (c)
    # batching would require a UDF that dispatches on match_id at runtime,
    # which is more complex for negligible gain at this scale.
    for mid in new_match_ids:
        # Build match_metadata from bronze.skillcorner_matches (driver-side)
        matches_pdf = (
            spark.table(matches_table)
            .filter(spark_fn.col("match_id") == mid)
            .select("match_id", "pitch_length", "pitch_width", "home_team_id")
            .limit(1)
            .toPandas()
        )

        if matches_pdf.empty:
            logger.warning("SkillCorner: no match metadata for %s -- skipping SPADL", mid)
            continue

        row = matches_pdf.iloc[0]
        match_metadata: dict[str, object] = {
            "id": str(row["match_id"]),
            "pitch_length": int(row["pitch_length"]),
            "pitch_width": int(row["pitch_width"]),
            "home_team": {"id": int(row["home_team_id"])},
        }

        # Build UDF with this match's metadata
        udf_fn = _make_skillcorner_spadl_udf(match_metadata=match_metadata)

        # Filter events for this match
        match_events_sdf = events_sdf.filter(spark_fn.col("match_id") == mid)  # type: ignore[possibly-undefined]

        spadl_sdf = match_events_sdf.groupBy("match_id").applyInPandas(
            udf_fn,  # type: ignore[arg-type]
            schema=spadl_schema,
        )

        hashed_id = hash_native_id_to_bigint(mid)
        write_delta_table(
            spadl_sdf,
            catalog,
            schema,
            _SPADL_TABLE,
            replace_where=_make_skillcorner_replace_where([hashed_id]),
            logger=logger,
        )
        wrote_any = True
        logger.info("SkillCorner: SPADL conversion complete for match %s", mid)

    return wrote_any


# ---------------------------------------------------------------------------
# Gradient Sports SPADL conversion
# ---------------------------------------------------------------------------


def _make_gradientsports_replace_where(hashed_match_ids: list[int]) -> str:
    """Build a replaceWhere predicate scoped to specific Gradient Sports matches."""
    if not hashed_match_ids:
        msg = "replace_where predicate requires at least one match_id"
        raise ValueError(msg)
    ids_sql = ", ".join(str(int(h)) for h in sorted(hashed_match_ids))
    return f"data_source = 'gradientsports' AND match_id IN ({ids_sql})"


def _make_gradientsports_spadl_udf(
    gs_comp_season: dict[str, tuple[str, str]] | None = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the applyInPandas UDF closure for Gradient Sports SPADL conversion.

    Follows the IDSSE batch pattern: metadata extracted from bronze columns
    at execution time (no per-match closure). Tackle qualifier mapping uses
    the IDSSE pattern (_native/_key pairs), NOT null_fill_tackle_qualifiers.

    Parameters
    ----------
    gs_comp_season
        Lookup of match_id → (competition_id, season) from the metadata
        bronze table. When provided, the UDF populates competition_native_id
        and season_native_id on the output SPADL rows. When None (metadata
        table not yet created), both columns are set to pd.NA.
    """
    _gs_comp_season = gs_comp_season

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import logging as _logging

        import pandas as _pd

        _udf_logger = _logging.getLogger(__name__)

        from ingestion.spadl_adapter import (
            UNKNOWN_TEAM_SENTINEL as _SENTINEL,
        )
        from ingestion.spadl_adapter import (
            adapt_gradientsports_events as _adapt,
        )
        from ingestion.spadl_adapter import (
            extract_gradientsports_match_metadata as _extract_meta,
        )
        from ingestion.spadl_adapter import (
            hash_native_id_to_bigint as _hash_id,
        )
        from ingestion.spadl_conversion import _gs_safe_to_dot_rename
        from shared.identifiers import gradientsports_native_competition_id as _gs_comp_id
        from shared.identifiers import gradientsports_native_match_id as _gs_match_id

        # Reverse the Spark-level dot→safe rename so the adapter receives
        # the original dot-notation column names it expects.
        _safe_to_dot = _gs_safe_to_dot_rename()
        pdf = pdf.rename(columns=_safe_to_dot)

        # Column list must stay in sync with the IDSSE UDF's _spadl_cols.
        # Any column added there must be added here too, or the final
        # reindex will KeyError.
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
                "player_id_native",
                "tackle_winner_player_id_native",
                "tackle_winner_player_key",
                "tackle_winner_team_id_native",
                "tackle_winner_team_key",
                "tackle_loser_player_id_native",
                "tackle_loser_player_key",
                "tackle_loser_team_id_native",
                "tackle_loser_team_key",
                # silly-kicks 4.13.0: is_synthetic provenance flag. Native (bool) on
                # the GS converter (True on synthesized foul + cross-goal-shot rows);
                # manufactured False on the 5 non-GS providers (no GS-style row
                # synthesis there). Cross-provider column per the False-default
                # decision — drops silently from the projection if omitted here.
                "is_synthetic",
                # silly-kicks 4.21.0/4.22.0: result_source (SkillCorner native-completion
                # label tier; NULL on other providers) + restart-coordinate enrichment
                # from apply_spadl_enrichments. Drops silently from the projection if
                # omitted here (the LL1 class).
                "result_source",
                "enriched_start_x",
                "enriched_start_y",
                "enriched_end_x",
                "enriched_end_y",
                "start_coord_source",
                "end_coord_source",
                "start_coord_confidence",
                "end_coord_confidence",
                # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056) — actions-level,
                # from apply_spadl_enrichments. Drops silently from the projection if omitted.
                "gk_pass_length_m",
                "gk_pass_length_class",
                "is_launch",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import silly_kicks.spadl.gradientsports as _spadl_gs

        # Match-level metadata from bronze columns (IDSSE batch pattern).
        match_id_str = str(pdf["match_id"].iloc[0])
        metadata = _extract_meta(pdf)

        try:
            adapted = _adapt(pdf)
            actions, _report = _spadl_gs.convert_to_actions(
                adapted,
                home_team_id=metadata["home_team_id"],
                home_team_start_left=metadata["home_team_start_left"],
                home_team_start_left_extratime=metadata["home_team_start_left_extratime"],
            )
        except Exception as exc:
            msg = f"GS SPADL conversion failed for match_id={match_id_str}"
            raise RuntimeError(msg) from exc

        if _report.unrecognized_counts:
            _udf_logger.warning(
                "SPADL conversion unrecognized event types for GS match %s: %s",
                match_id_str,
                _report.unrecognized_counts,
            )

        from ingestion.spadl_udf_shared import (
            apply_match_level_natives as _apply_match_natives,
        )
        from ingestion.spadl_udf_shared import (
            apply_player_id_native as _apply_pid_native,
        )
        from ingestion.spadl_udf_shared import (
            cast_enrichment_dtypes as _cast_enrichment,
        )
        from ingestion.spadl_udf_shared import (
            null_fill_statsbomb_columns as _null_fill_sb,
        )

        # player_id_native -- GS player_ids are Int64, else branch
        # in apply_player_id_native handles .astype("string") correctly.
        actions = _apply_pid_native(actions, source="gradientsports")

        # Hash match_id and team_id to legacy BIGINTs.
        match_id_hashed = _hash_id(match_id_str)
        actions["match_id"] = match_id_hashed
        actions["game_id"] = match_id_hashed
        n = len(actions)

        # team_id: GS converter outputs numeric team_id. Map to native string + hash.
        actions["team_id_native"] = actions["team_id"].astype("Int64").astype("string")
        null_team_mask = actions["team_id_native"].isna() | (actions["team_id_native"] == "<NA>")
        if null_team_mask.any():
            _udf_logger.warning(
                "NULL team_id_native in %d rows for GS match_id=%s. Filling with sentinel hash.",
                null_team_mask.sum(),
                match_id_str,
            )
            actions.loc[null_team_mask, "team_id_native"] = _SENTINEL
        actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")

        # NULL-fill legacy BIGINTs
        actions["player_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["competition_id"] = _pd.array([_pd.NA] * n, dtype="Int64")
        actions["season_id"] = _pd.array([_pd.NA] * n, dtype="Int64")

        # data_source
        actions["data_source"] = "gradientsports"

        # enrichments
        from ingestion.spadl_enrichments import apply_spadl_enrichments as _enrich

        actions = _enrich(actions, source="gradientsports")

        # original_event_id to string
        actions["original_event_id"] = actions["original_event_id"].astype(str)

        # null-fill SB columns + cast enrichment dtypes
        actions = _null_fill_sb(actions, n=n)
        actions = _cast_enrichment(actions)

        # match-level natives — competition/season from metadata bronze lookup
        comp_id_str, season_str = ("", "")
        if _gs_comp_season and match_id_str in _gs_comp_season:
            comp_id_str, season_str = _gs_comp_season[match_id_str]

        actions = _apply_match_natives(
            actions,
            home_team_id_native=str(metadata["home_team_id"]),
            competition_native_id=_gs_comp_id(comp_id_str) if comp_id_str else _pd.NA,  # type: ignore[arg-type]
            season_native_id=season_str if season_str else _pd.NA,  # type: ignore[arg-type]
            match_id_native=_gs_match_id(match_id_str),
        )

        # Tackle qualifier mapping (IDSSE pattern, NOT null_fill_tackle_qualifiers).
        # GS converter outputs 4 Int64 tackle columns on challenge events.
        from typing import Any as _Any

        def _hash_or_na(v: _Any) -> _Any:
            if v is None or _pd.isna(v):
                return _pd.NA
            s = str(v)
            return _hash_id(s) if s else _pd.NA

        for native_col, key_col, sk_col in (
            ("tackle_winner_player_id_native", "tackle_winner_player_key", "tackle_winner_player_id"),
            ("tackle_winner_team_id_native", "tackle_winner_team_key", "tackle_winner_team_id"),
            ("tackle_loser_player_id_native", "tackle_loser_player_key", "tackle_loser_player_id"),
            ("tackle_loser_team_id_native", "tackle_loser_team_key", "tackle_loser_team_id"),
        ):
            if sk_col in actions.columns:
                actions[native_col] = actions[sk_col].astype("string")
                actions[key_col] = actions[native_col].map(_hash_or_na).astype("Int64")
            else:
                actions[native_col] = _pd.array([_pd.NA] * len(actions), dtype="string")
                actions[key_col] = _pd.array([_pd.NA] * len(actions), dtype="Int64")

        # silly-kicks 4.13.0 is_synthetic (sk ADR-018): coerce native bool (GS) /
        # manufacture False (5 non-GS providers) — see ensure_is_synthetic.
        from ingestion.spadl_udf_shared import ensure_is_synthetic as _ensure_is_synthetic
        from ingestion.spadl_udf_shared import ensure_result_source as _ensure_result_source

        actions = _ensure_is_synthetic(actions)
        actions = _ensure_result_source(actions)

        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


_GS_DOT_REPLACEMENT = "___"
"""Separator used to replace literal dots in GS bronze column names.

Spark interprets dots in column names as struct navigation inside
``applyInPandas`` execution plans.  We rename ``foo.bar`` → ``foo___bar``
at the Spark level, then reverse the rename at the top of the UDF so the
pandas adapter (``adapt_gradientsports_events``) receives the original
dot-notation names it expects.
"""


def _gs_needed_bronze_columns() -> set[str]:
    """Return the set of GS bronze column names needed by the SPADL UDF.

    The GS bronze table has ~264 columns with literal dots in their names
    (e.g. ``gameEvents.gameEventType``).  Spark interprets dots as struct
    navigation, so only columns needed by the UDF are projected via
    backtick quoting before ``applyInPandas``.

    Sources:
    - ``_GS_BRONZE_TO_SNAKE`` keys: 1:1 renames (~35 columns)
    - Derived columns read by ``adapt_gradientsports_events``
    - Metadata columns read by ``extract_gradientsports_match_metadata``
    - ``ball`` (JSON string parsed to ball_x/ball_y)
    - ``match_id`` (ingestion-added groupBy key)
    """
    from ingestion.spadl_adapter import _GS_BRONZE_TO_SNAKE

    return {
        *_GS_BRONZE_TO_SNAKE.keys(),
        # Derived columns (adapt_gradientsports_events)
        "gameEventId",
        "gameEvents.period",
        "gameEvents.startGameClock",
        "gameEvents.playerId",
        "gameEvents.teamId",
        "gameEvents.setpieceType",
        # Metadata columns (extract_gradientsports_match_metadata)
        "gameEvents.homeTeam",
        "stadiumMetadata.homeTeamStartLeft",
        "stadiumMetadata.homeTeamStartLeftExtraTime",
        # Ball JSON + groupBy key
        "ball",
        "match_id",
    }


def _gs_dot_to_safe_rename() -> dict[str, str]:
    """Build ``{dot_name: safe_name}`` rename map for all GS dot-notation columns.

    Only columns that actually contain a dot are renamed.  Columns like
    ``match_id`` and ``ball`` pass through unchanged.
    """
    return {c: c.replace(".", _GS_DOT_REPLACEMENT) for c in _gs_needed_bronze_columns() if "." in c}


def _gs_safe_to_dot_rename() -> dict[str, str]:
    """Inverse of :func:`_gs_dot_to_safe_rename` — ``{safe_name: dot_name}``."""
    return {v: k for k, v in _gs_dot_to_safe_rename().items()}


def _convert_gradientsports_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,
) -> bool:
    """Read GS events from bronze, convert to SPADL via silly-kicks, write Delta.

    IDSSE batch pattern: 1 Spark job for all matches via groupBy.applyInPandas.
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

    events_table = f"{catalog}.{schema}.gradientsports_events"

    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read GS events bronze table")
        return False

    # match_id in bronze is a string (e.g. "10502"); spadl_actions.match_id is
    # a BIGINT via hash_native_id_to_bigint.  We compare hashed values here.
    all_match_rows = events_sdf.select("match_id").distinct().collect()
    all_match_ids: list[str] = [str(row["match_id"]) for row in all_match_rows]

    new_match_ids: list[str] = [mid for mid in all_match_ids if hash_native_id_to_bigint(mid) not in existing_matches]
    if match_id_filter is not None:
        new_match_ids = [mid for mid in new_match_ids if hash_native_id_to_bigint(mid) in match_id_filter]

    if not new_match_ids:
        logger.info("GS: all %d matches already converted -- skipping", len(all_match_ids))
        return False

    logger.info("GS: converting %d new matches (of %d total)", len(new_match_ids), len(all_match_ids))

    new_events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(new_match_ids))

    # Project only needed columns with backtick quoting, then RENAME
    # dot-notation columns to use '___' as separator.  Spark interprets
    # dots as struct navigation inside applyInPandas execution plans —
    # backtick quoting fixes the .select() but NOT the subsequent
    # FlatMapGroupsInPandas resolution pass.  Renaming at the Spark level
    # makes the schema dot-free; the UDF reverses the rename at the top.
    needed = _gs_needed_bronze_columns()
    _bronze_field_names = {f.name for f in events_sdf.schema.fields}
    dot_to_safe = _gs_dot_to_safe_rename()
    new_events_sdf = new_events_sdf.select(
        [spark_fn.col(f"`{c}`").alias(dot_to_safe.get(c, c)) for c in sorted(needed) if c in _bronze_field_names]
    )

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
            StructField("player_id_native", StringType()),
            StructField("tackle_winner_player_id_native", StringType()),
            StructField("tackle_winner_player_key", LongType()),
            StructField("tackle_winner_team_id_native", StringType()),
            StructField("tackle_winner_team_key", LongType()),
            StructField("tackle_loser_player_id_native", StringType()),
            StructField("tackle_loser_player_key", LongType()),
            StructField("tackle_loser_team_id_native", StringType()),
            StructField("tackle_loser_team_key", LongType()),
            # silly-kicks 4.13.0 is_synthetic provenance (sk ADR-018): native on GS,
            # manufactured False elsewhere. Must mirror _spadl_cols + _SPADL_SCHEMA.
            StructField("is_synthetic", BooleanType()),
            # silly-kicks 4.21.0/4.22.0: result_source + restart-coordinate enrichment.
            StructField("result_source", StringType()),
            StructField("enriched_start_x", DoubleType()),
            StructField("enriched_start_y", DoubleType()),
            StructField("enriched_end_x", DoubleType()),
            StructField("enriched_end_y", DoubleType()),
            StructField("start_coord_source", StringType()),
            StructField("end_coord_source", StringType()),
            StructField("start_coord_confidence", DoubleType()),
            StructField("end_coord_confidence", DoubleType()),
            # GVM gk-distribution metrics (silly-kicks 4.31.0, ADR-056). Must mirror the
            # projection above + _SPADL_SCHEMA + apply_spadl_enrichments.
            StructField("gk_pass_length_m", DoubleType()),
            StructField("gk_pass_length_class", StringType()),
            StructField("is_launch", BooleanType()),
        ]
    )

    # Read competition/season from metadata bronze (populated by backfill).
    metadata_table = f"{catalog}.{schema}.gradientsports_metadata"
    gs_comp_season: dict[str, tuple[str, str]] = {}
    from ingestion.utils import tolerate_missing_table

    with tolerate_missing_table(logger, "GS metadata table not yet created — competition/season will be NULL"):
        meta_rows = spark.table(metadata_table).select("match_id", "`competition.id`", "season").collect()
        for row in meta_rows:
            mid = str(row["match_id"])
            comp_id = str(row["competition.id"]) if row["competition.id"] is not None else ""
            season = str(row["season"]) if row["season"] is not None else ""
            gs_comp_season[mid] = (comp_id, season)

    udf_fn = _make_gradientsports_spadl_udf(gs_comp_season=gs_comp_season or None)
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
        replace_where=_make_gradientsports_replace_where(hashed_new_ids),
        logger=logger,
    )

    logger.info("GS: SPADL conversion complete for %d matches", len(new_match_ids))
    return True
