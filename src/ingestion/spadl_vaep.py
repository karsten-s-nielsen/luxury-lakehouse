"""SPADL conversion and VAEP action valuation pipeline.

Reads events from existing bronze Delta tables (``statsbomb_events``,
``wyscout_events``), converts them into SPADL unified format via
socceraction, trains VAEP classifiers, and scores every action with
offensive/defensive value.

Bronze tables produced:
  - spadl_actions         -- SPADL-formatted actions (intermediate)
  - vaep_action_values    -- SPADL actions with VAEP scores (final output)

Design: "Fetch Once, Fork Twice" — ingestion tasks populate bronze,
this pipeline reads from bronze.  No external API calls.  Supports
incremental runs by skipping games already converted / scored.
"""

from __future__ import annotations

import gc
import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any

import pandas as pd
import socceraction.spadl as spadl
import socceraction.vaep.features as fs
import socceraction.vaep.formula as vaepformula
import socceraction.vaep.labels as labels
from xgboost import XGBClassifier

from ingestion.spadl_adapter import (
    resolve_statsbomb_home_team_ids,
    resolve_wyscout_home_team_ids,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Feature extraction functions (standard VAEP feature set)
_FEATURE_FNS: list[Any] = [
    fs.actiontype_onehot,
    fs.result_onehot,
    fs.bodypart_onehot,
    fs.time,
    fs.startlocation,
    fs.endlocation,
    fs.startpolar,
    fs.endpolar,
    fs.movement,
    fs.team,
    fs.time_delta,
]

_NB_PREV_ACTIONS = 3
_MAX_TRAINING_GAMES = 200
_SPADL_TABLE = "spadl_actions"
_VAEP_TABLE = "vaep_action_values"


# ---------------------------------------------------------------------------
# Spark type coercion
# ---------------------------------------------------------------------------


def _clean_spadl_for_spark(actions: pd.DataFrame) -> pd.DataFrame:
    """Cast SPADL DataFrame columns to explicit types for Spark compatibility.

    PySpark's schema inference can fail on pandas DataFrames with mixed types
    (e.g. numpy int64 vs float64 with NaN).  This function forces all columns
    to known Spark-compatible types.
    """
    df = actions.copy()

    int_cols = [
        "game_id",
        "period_id",
        "team_id",
        "player_id",
        "type_id",
        "result_id",
        "bodypart_id",
    ]
    for col in int_cols:
        if col in df.columns:
            series: pd.Series = pd.to_numeric(df[col], errors="coerce")  # type: ignore[assignment]
            df[col] = series.fillna(0).astype("int64")

    float_cols = ["time_seconds", "start_x", "start_y", "end_x", "end_y"]
    for col in float_cols:
        if col in df.columns:
            fseries: pd.Series = pd.to_numeric(df[col], errors="coerce")  # type: ignore[assignment]
            df[col] = fseries.fillna(0.0).astype("float64")

    if "competition_id" in df.columns:
        comp_s: pd.Series = pd.to_numeric(df["competition_id"], errors="coerce")  # type: ignore[assignment]
        df["competition_id"] = comp_s.fillna(0).astype("int64")
    if "season_id" in df.columns:
        season_s: pd.Series = pd.to_numeric(df["season_id"], errors="coerce")  # type: ignore[assignment]
        df["season_id"] = season_s.fillna(0).astype("int64")
    if "data_source" in df.columns:
        df["data_source"] = df["data_source"].astype(str)

    # Normalize original_event_id to string (StatsBomb=UUID, Wyscout=int)
    if "original_event_id" in df.columns:
        df["original_event_id"] = df["original_event_id"].astype(str)

    # Drop any columns with dict/list values that Spark can't serialize
    for col in list(df.columns):
        sample = df[col].dropna()
        if not sample.empty and isinstance(sample.iloc[0], dict | list):
            df = df.drop(columns=[col])

    return df


# ---------------------------------------------------------------------------
# Incremental helpers
# ---------------------------------------------------------------------------


def _read_existing_game_ids(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> set[int]:
    """Return game_ids already present in a Delta table, or empty set if table doesn't exist."""
    full_table = f"{catalog}.{schema}.{table}"
    try:
        rows = spark.table(full_table).select("game_id").distinct().collect()
        return {int(row["game_id"]) for row in rows}
    except Exception:
        logger.debug("Table %s not found — starting fresh", full_table, exc_info=True)
        return set()


# ---------------------------------------------------------------------------
# Phase A+B — Convert events to SPADL from bronze tables
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
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import socceraction.spadl.statsbomb as _spadl_sb

        home_team_id = int(pdf["home_team_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf, home_team_id)
            actions = _spadl_sb.convert_to_actions(adapted, home_team_id)
        except Exception:
            return _pd.DataFrame(columns=_spadl_cols)

        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "statsbomb"

        # Keep only the expected output columns (drop any extras from socceraction)
        for col in _spadl_cols:
            if col not in actions.columns:
                actions[col] = 0
        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_statsbomb_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_games: set[int],
) -> bool:
    """Read StatsBomb events from bronze, adapt, convert to SPADL, write Delta.

    Uses ``groupBy("match_id").applyInPandas`` to distribute per-game
    SPADL conversion across Spark executors instead of sequential driver
    loops with ``.toPandas()``.

    Returns whether any data was written.
    """
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    events_table = f"{catalog}.{schema}.statsbomb_events"
    matches_table = f"{catalog}.{schema}.statsbomb_matches"

    try:
        all_matches_pdf = spark.table(matches_table).toPandas()
    except Exception:
        logger.exception("Cannot read StatsBomb matches bronze table")
        return False

    # Pull only team lookup columns (tiny vs full table) for home_team_id resolution
    try:
        team_lookup_pdf = spark.table(events_table).select("match_id", "team_id", "team").distinct().toPandas()
    except Exception:
        logger.exception("Cannot read StatsBomb events bronze table")
        return False

    if team_lookup_pdf.empty:
        logger.info("StatsBomb bronze events table is empty — skipping")
        return False

    home_team_map = resolve_statsbomb_home_team_ids(all_matches_pdf, team_lookup_pdf)

    # Find new game_ids via Spark (tiny result)
    events_sdf = spark.table(events_table)
    all_game_rows = events_sdf.select("match_id").distinct().collect()
    all_game_ids = [int(row["match_id"]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_games]

    # Also filter out games where home_team_id is unknown
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
        replace_where="data_source = 'statsbomb'",
        logger=logger,
    )

    logger.info("StatsBomb: SPADL conversion complete for %d games", len(new_game_ids))
    return True


def _make_ws_spadl_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for Wyscout SPADL conversion.

    All library imports happen inside the closure so they are available
    on Spark executors without requiring module-level serialisation.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Convert one game's Wyscout events to SPADL actions."""
        import pandas as _pd

        from ingestion.spadl_adapter import adapt_wyscout_events as _adapt

        _spadl_cols = _pd.Index(
            [
                "game_id",
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
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_spadl_cols)

        import socceraction.spadl.wyscout as _spadl_ws

        home_team_id = int(pdf["home_team_id"].iloc[0])
        competition_id = int(pdf["competition_id"].iloc[0])
        season_id = int(pdf["season_id"].iloc[0])

        try:
            adapted = _adapt(pdf)
            actions = _spadl_ws.convert_to_actions(adapted, home_team_id)
        except Exception:
            return _pd.DataFrame(columns=_spadl_cols)

        actions["competition_id"] = competition_id
        actions["season_id"] = season_id
        actions["data_source"] = "wyscout"

        # Keep only the expected output columns (drop any extras from socceraction)
        for col in _spadl_cols:
            if col not in actions.columns:
                actions[col] = 0
        return _pd.DataFrame(actions[_spadl_cols])

    return _udf


def _convert_wyscout_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_games: set[int],
) -> bool:
    """Read Wyscout events from bronze, adapt, convert to SPADL, write Delta.

    Uses ``groupBy(match_id_col).applyInPandas`` to distribute per-game
    SPADL conversion across Spark executors instead of sequential driver
    loops with ``.toPandas()``.

    Returns whether any data was written.
    """
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    events_table = f"{catalog}.{schema}.wyscout_events"
    matches_table = f"{catalog}.{schema}.wyscout_matches"

    try:
        all_matches_pdf = spark.table(matches_table).toPandas()
    except Exception:
        logger.exception("Cannot read Wyscout matches bronze table")
        return False

    # Determine match ID column name from events schema (metadata only, no scan)
    try:
        events_columns = spark.table(events_table).columns
    except Exception:
        logger.exception("Cannot read Wyscout events bronze table")
        return False

    match_id_col = "matchId" if "matchId" in events_columns else "match_id"

    # Resolve home_team_id per match
    home_team_map = resolve_wyscout_home_team_ids(all_matches_pdf)

    # Get all game IDs from events via Spark (tiny result)
    all_game_rows = spark.table(events_table).select(match_id_col).distinct().collect()
    all_game_ids = [int(row[match_id_col]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_games]

    # Also filter out games where home_team_id is unknown
    new_game_ids = [gid for gid in new_game_ids if home_team_map.get(gid, 0) != 0]

    if not new_game_ids:
        logger.info("Wyscout: all %d games already converted — skipping", len(all_game_ids))
        return False

    logger.info("Wyscout: converting %d new games (of %d total)", len(new_game_ids), len(all_game_ids))

    # Build lookup DataFrame with home_team_id, competition_id, season_id per game
    # Derive competition_id and season_id from matches metadata
    match_meta: dict[int, tuple[int, int]] = {}
    if "competitionId" in all_matches_pdf.columns:
        for _, mrow in all_matches_pdf.iterrows():
            game_id = int(mrow["wyId"])
            comp_id = int(mrow["competitionId"])
            season_id = int(mrow["seasonId"]) if "seasonId" in all_matches_pdf.columns else 0
            match_meta[game_id] = (comp_id, season_id)

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

    # Define SPADL output schema (same as StatsBomb)
    spadl_schema = StructType(
        [
            StructField("game_id", LongType()),
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
        ]
    )

    udf_fn = _make_ws_spadl_udf()
    spadl_sdf = new_events_sdf.groupBy(match_id_col).applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=spadl_schema,
    )

    write_delta_table(
        spadl_sdf,
        catalog,
        schema,
        _SPADL_TABLE,
        replace_where="data_source = 'wyscout'",
        logger=logger,
    )

    logger.info("Wyscout: SPADL conversion complete for %d games", len(new_game_ids))
    return True


# ---------------------------------------------------------------------------
# Phase C — Extract features & train VAEP models
# ---------------------------------------------------------------------------


def _extract_features_for_games(
    actions: pd.DataFrame,
    game_ids: Any,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract features and labels for a subset of games.

    Returns (X, Y_scores, Y_concedes) for the specified games only.
    """
    named = spadl.add_names(actions)  # type: ignore[arg-type]
    all_x: list[pd.DataFrame] = []
    all_y_scores: list[pd.DataFrame] = []
    all_y_concedes: list[pd.DataFrame] = []

    for game_id in game_ids:
        game_actions = named[named["game_id"] == game_id].reset_index(drop=True)
        if len(game_actions) < 2:
            continue
        try:
            gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)  # type: ignore[arg-type]
            x_game = pd.concat([fn(gamestates) for fn in _FEATURE_FNS], axis=1)
            y_scores = labels.scores(game_actions, nr_actions=10)  # type: ignore[arg-type]
            y_concedes = labels.concedes(game_actions, nr_actions=10)  # type: ignore[arg-type]
            all_x.append(x_game)
            all_y_scores.append(y_scores)
            all_y_concedes.append(y_concedes)
        except Exception:
            logger.exception("Failed feature extraction for game %s", game_id)

    if not all_x:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    return (
        pd.concat(all_x, ignore_index=True),
        pd.concat(all_y_scores, ignore_index=True),
        pd.concat(all_y_concedes, ignore_index=True),
    )


def train_vaep_models(
    x: pd.DataFrame,
    y_scores: pd.DataFrame,
    y_concedes: pd.DataFrame,
    logger: logging.Logger,
) -> tuple[XGBClassifier, XGBClassifier]:
    """Train two XGBoost classifiers for P(scoring) and P(conceding)."""
    model_scores = XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
    )
    model_concedes = XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
    )

    logger.info("Training VAEP scoring model on %d samples", len(x))
    model_scores.fit(x, y_scores["scores"])

    logger.info("Training VAEP conceding model on %d samples", len(x))
    model_concedes.fit(x, y_concedes["concedes"])

    return model_scores, model_concedes


def _load_or_train_models(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    training_game_ids: list[int],
    training_pdf: pd.DataFrame,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Load cached models from UC Volume if hash matches, else train and save.

    Model cache key is a SHA-256 hash of the sorted training game IDs.
    Returns None if training fails (empty features).
    """
    training_hash = hashlib.sha256(json.dumps(sorted(training_game_ids)).encode()).hexdigest()[:12]
    model_dir = f"/Volumes/{catalog}/{schema}/vaep_models"
    scores_path = f"{model_dir}/scores_{training_hash}.json"
    concedes_path = f"{model_dir}/concedes_{training_hash}.json"

    # Try loading cached models
    try:
        if os.path.exists(scores_path) and os.path.exists(concedes_path):
            model_scores = XGBClassifier()
            model_scores.load_model(scores_path)
            model_concedes = XGBClassifier()
            model_concedes.load_model(concedes_path)
            logger.info("Loaded cached VAEP models (hash=%s)", training_hash)
            return model_scores, model_concedes
    except Exception:
        logger.warning("Failed to load cached models — will retrain", exc_info=True)

    # Extract features and train
    x_train, y_scores, y_concedes = _extract_features_for_games(
        training_pdf,
        training_game_ids,
        logger,
    )

    if x_train.empty:
        logger.warning("No features extracted — nothing to train")
        return None

    logger.info("Training features: %d rows x %d cols", len(x_train), x_train.shape[1])
    model_scores, model_concedes = train_vaep_models(x_train, y_scores, y_concedes, logger)

    # Save models to UC Volume
    try:
        os.makedirs(model_dir, exist_ok=True)
        model_scores.save_model(scores_path)
        model_concedes.save_model(concedes_path)
        logger.info("Saved VAEP models to %s (hash=%s)", model_dir, training_hash)
    except Exception:
        logger.warning("Failed to save models to UC Volume — training still succeeded", exc_info=True)

    return model_scores, model_concedes


# ---------------------------------------------------------------------------
# Phase D — Score all actions & write
# ---------------------------------------------------------------------------


def _score_competition(
    actions: pd.DataFrame,
    model_scores: XGBClassifier,
    model_concedes: XGBClassifier,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Extract features and score a single competition's actions.

    This avoids holding features for all competitions in memory at once.
    """
    named = spadl.add_names(actions)  # type: ignore[arg-type]
    game_ids = named["game_id"].unique()

    all_scored: list[pd.DataFrame] = []
    for game_id in game_ids:
        game_actions = named[named["game_id"] == game_id].reset_index(drop=True)
        if len(game_actions) < 2:
            continue
        try:
            gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)  # type: ignore[arg-type]
            x_game = pd.concat([fn(gamestates) for fn in _FEATURE_FNS], axis=1)

            p_scores = pd.Series(model_scores.predict_proba(x_game)[:, 1])
            p_concedes = pd.Series(model_concedes.predict_proba(x_game)[:, 1])
            values = vaepformula.value(game_actions, p_scores, p_concedes)  # type: ignore[arg-type]

            game_out = pd.DataFrame(
                game_actions[
                    [
                        c
                        for c in [
                            "game_id",
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
                            "type_name",
                            "result_id",
                            "result_name",
                            "bodypart_id",
                            "bodypart_name",
                        ]
                        if c in game_actions.columns
                    ]
                ].copy()
            )
            game_out = game_out.rename(
                columns={
                    "type_name": "action_type",
                    "result_name": "action_result",
                    "bodypart_name": "bodypart",
                }
            )
            game_out["offensive_value"] = values["offensive_value"].values
            game_out["defensive_value"] = values["defensive_value"].values
            game_out["vaep_value"] = values["vaep_value"].values
            all_scored.append(game_out)
        except Exception:
            logger.exception("Failed scoring game %s", game_id)

    if not all_scored:
        return pd.DataFrame()

    return pd.concat(all_scored, ignore_index=True)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Execute the full SPADL/VAEP pipeline.

    Memory strategy: never hold all data in memory.  Use Delta as
    intermediate storage between phases:

    1. Read bronze events, convert to SPADL per-competition -> append Delta (incremental)
    2. Read a small training subset from Delta -> extract features -> train (or load cached)
    3. Read per-competition from Delta -> score unscored games -> write results (incremental)
    """
    spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"

    # Phase A+B: Convert events from bronze to SPADL (incremental)
    existing_spadl_games = _read_existing_game_ids(spark, catalog, schema, _SPADL_TABLE, logger)
    if existing_spadl_games:
        logger.info("Found %d games already in %s — will skip", len(existing_spadl_games), _SPADL_TABLE)

    sb_wrote = _convert_statsbomb_from_bronze(spark, catalog, schema, logger, existing_spadl_games)
    ws_wrote = _convert_wyscout_from_bronze(spark, catalog, schema, logger, existing_spadl_games)

    if not sb_wrote and not ws_wrote and not existing_spadl_games:
        msg = "No SPADL actions produced from either StatsBomb or Wyscout"
        logger.error(msg)
        raise RuntimeError(msg)

    # Verify SPADL table has data (limit(1) avoids full DAG recomputation — exact count not needed here)
    if spark.table(spadl_table).limit(1).count() == 0:
        msg = "SPADL table exists but is empty — no actions to score"
        logger.error(msg)
        raise RuntimeError(msg)
    logger.info("SPADL table %s has data — proceeding to training", spadl_table)

    # Phase C: Train on a representative sample read from Delta
    spadl_sdf = spark.table(spadl_table)
    training_games_rows = (
        spadl_sdf.groupBy("game_id").count().orderBy("count", ascending=False).limit(_MAX_TRAINING_GAMES).collect()
    )
    training_game_ids = [int(row["game_id"]) for row in training_games_rows]

    if not training_game_ids:
        logger.warning("No games found for training")
        return

    from pyspark.sql import functions as spark_fn

    training_pdf = spadl_sdf.filter(spark_fn.col("game_id").isin(training_game_ids)).toPandas()

    logger.info(
        "Training subset: %d games, %d actions",
        len(training_game_ids),
        len(training_pdf),
    )

    models = _load_or_train_models(
        spark,
        catalog,
        schema,
        logger,
        training_game_ids,
        training_pdf,
    )
    del training_pdf
    gc.collect()

    if models is None:
        return

    model_scores, model_concedes = models

    # Phase D: Score per-competition from Delta (incremental — skip scored games)
    existing_vaep_games = _read_existing_game_ids(spark, catalog, schema, _VAEP_TABLE, logger)
    if existing_vaep_games:
        logger.info("Found %d games already scored in %s — will skip", len(existing_vaep_games), _VAEP_TABLE)

    comp_source_rows = spadl_sdf.select("competition_id", "data_source").distinct().collect()

    total_scored = 0

    for row in comp_source_rows:
        comp_id, source = int(row["competition_id"]), str(row["data_source"])
        try:
            # Check game IDs via Spark BEFORE pulling into pandas
            comp_filter = (spark_fn.col("competition_id") == comp_id) & (spark_fn.col("data_source") == source)
            comp_game_rows = spadl_sdf.filter(comp_filter).select("game_id").distinct().collect()
            comp_game_ids = [int(r["game_id"]) for r in comp_game_rows]

            new_game_ids = [gid for gid in comp_game_ids if gid not in existing_vaep_games]
            if not new_game_ids:
                logger.info("Comp %d (%s): all %d games already scored — skipping", comp_id, source, len(comp_game_ids))
                continue

            # Pull only unscored games into pandas
            comp_pdf = spadl_sdf.filter(comp_filter).filter(spark_fn.col("game_id").isin(new_game_ids)).toPandas()

            if comp_pdf.empty:
                continue

            scored = _score_competition(comp_pdf, model_scores, model_concedes, logger)
            if scored.empty:
                del comp_pdf
                gc.collect()
                continue

            for col in ["data_source", "competition_id", "season_id"]:
                if col in comp_pdf.columns and col not in scored.columns:
                    scored[col] = comp_pdf[col].values[: len(scored)]

            scored = _clean_spadl_for_spark(scored)
            sdf = spark.createDataFrame(scored)
            replace_expr = f"competition_id = {comp_id} AND data_source = '{source}'"
            write_delta_table(sdf, catalog, schema, _VAEP_TABLE, replace_where=replace_expr, logger=logger)
            total_scored += len(scored)

            logger.info(
                "Scored comp %d (%s): %d actions (total: %d)",
                comp_id,
                source,
                len(scored),
                total_scored,
            )
            del comp_pdf, scored, sdf
        except Exception:
            logger.exception("Failed scoring comp %d (%s)", comp_id, source)
        finally:
            gc.collect()

    logger.info("SPADL/VAEP pipeline complete — %d actions scored", total_scored)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for SPADL conversion and VAEP action valuation."""
    args = parse_ingestion_args("Compute SPADL actions and VAEP scores")
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
