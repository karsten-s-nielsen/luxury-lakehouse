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

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
import socceraction.spadl as spadl
import socceraction.vaep.features as fs
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
from workflows import workflow

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

    # Check for new games BEFORE pulling metadata tables to driver (avoid
    # wasted .toPandas() on no-op runs).
    try:
        events_sdf = spark.table(events_table)
    except Exception:
        logger.exception("Cannot read StatsBomb events bronze table")
        return False

    all_game_rows = events_sdf.select("match_id").distinct().collect()
    all_game_ids = [int(row["match_id"]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_games]

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

    # Check for new games BEFORE pulling metadata tables to driver
    try:
        events_columns = spark.table(events_table).columns
    except Exception:
        logger.exception("Cannot read Wyscout events bronze table")
        return False

    match_id_col = "matchId" if "matchId" in events_columns else "match_id"

    all_game_rows = spark.table(events_table).select(match_id_col).distinct().collect()
    all_game_ids = [int(row[match_id_col]) for row in all_game_rows]
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_games]

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


# NOTE: _extract_features_for_games() and train_vaep_models() are retained
# for reference and local testing. Production training runs on HF Jobs
# via scripts/train_vaep_model_hf.py (PEP 723 standalone script).
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

    # Pre-build game index (CLAUDE.md: no boolean mask filter inside loops)
    _game_groups = dict(iter(named.groupby("game_id")))

    for game_id in game_ids:
        game_actions = _game_groups.get(game_id, pd.DataFrame()).reset_index(drop=True)
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


def _try_load_champion_vaep(
    logger: logging.Logger,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Try to load VAEP models from MLflow @Champion alias.

    Returns (model_scores, model_concedes) if found, None otherwise.
    Falls back gracefully when mlflow is not installed or models are not registered.
    """
    try:
        import importlib

        mlflow_pyfunc = importlib.import_module("mlflow.pyfunc")
    except (ImportError, ModuleNotFoundError):
        logger.info("mlflow not available — will train VAEP models from scratch")
        return None

    model_name = "soccer_analytics.dev_gold.vaep_model"
    try:
        model_uri = f"models:/{model_name}@Champion"
        logger.info("Loading VAEP @Champion from %s", model_uri)
        champion = mlflow_pyfunc.load_model(model_uri)
        # The pyfunc wrapper stores both models as a dict of XGBClassifier
        unwrapped = champion.unwrap_python_model()  # type: ignore[union-attr]
        model_scores: XGBClassifier = unwrapped.scores_model  # type: ignore[union-attr]
        model_concedes: XGBClassifier = unwrapped.concedes_model  # type: ignore[union-attr]
        logger.info("Loaded VAEP @Champion models from MLflow")
        return model_scores, model_concedes
    except Exception:
        logger.info("VAEP @Champion not found in MLflow registry — will train from scratch", exc_info=True)
        return None


def _load_or_train_models(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    training_game_ids: list[int],
    training_pdf: pd.DataFrame,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Load VAEP models from MLflow @Champion registry.

    Training is handled externally by HF Jobs (scripts/train_vaep_model_hf.py).
    The training_game_ids and training_pdf parameters are retained for signature
    compatibility but are no longer used for fallback training.
    """
    champion_models = _try_load_champion_vaep(logger)
    if champion_models is not None:
        return champion_models

    logger.warning(
        "No Champion VAEP model found in MLflow registry. "
        "Run scripts/train_vaep_model_hf.py on HF Jobs to train and register a model."
    )
    return None


# ---------------------------------------------------------------------------
# Phase D — Score all actions & write
# ---------------------------------------------------------------------------


def _make_scoring_udf(scores_raw: bytes, concedes_raw: bytes) -> object:
    """Build the ``applyInPandas`` UDF closure for VAEP scoring.

    Models are deserialized from raw bytes (captured in the closure) and
    cached in a function-level dict so each executor deserializes only once.
    This avoids UC Volume FUSE limitations on serverless where XGBoost's
    C-level file I/O cannot read/write Volume paths.

    Args:
        scores_raw: Raw bytes from ``model.get_booster().save_raw("json")``.
        concedes_raw: Raw bytes from ``model.get_booster().save_raw("json")``.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _nb_prev = _NB_PREV_ACTIONS

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Score one competition's SPADL actions with VAEP models."""
        import pandas as _pd

        _output_cols = _pd.Index(
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
                "action_type",
                "result_id",
                "action_result",
                "bodypart_id",
                "bodypart",
                "offensive_value",
                "defensive_value",
                "vaep_value",
                "competition_id",
                "season_id",
                "data_source",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        import socceraction.spadl as _spadl
        import socceraction.vaep.features as _fs
        import socceraction.vaep.formula as _vaepformula

        _feature_fns: list = [
            _fs.actiontype_onehot,
            _fs.result_onehot,
            _fs.bodypart_onehot,
            _fs.time,
            _fs.startlocation,
            _fs.endlocation,
            _fs.startpolar,
            _fs.endpolar,
            _fs.movement,
            _fs.team,
            _fs.time_delta,
        ]

        # Load models with executor-level caching (deserialize from bytes)
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]

        cache: dict = _udf._model_cache  # type: ignore[attr-defined]
        if "scores" not in cache:
            from xgboost import XGBClassifier

            m_scores = XGBClassifier()
            m_scores.load_model(bytearray(scores_raw))
            cache["scores"] = m_scores

            m_concedes = XGBClassifier()
            m_concedes.load_model(bytearray(concedes_raw))
            cache["concedes"] = m_concedes

        model_scores = cache["scores"]
        model_concedes = cache["concedes"]

        named = _spadl.add_names(pdf)  # type: ignore[arg-type]
        game_ids = named["game_id"].unique()

        # Pre-build game index (CLAUDE.md: no boolean mask filter inside loops)
        _game_groups = dict(iter(named.groupby("game_id")))

        all_scored: list[_pd.DataFrame] = []
        for game_id in game_ids:
            game_actions = _game_groups.get(game_id, _pd.DataFrame()).reset_index(drop=True)
            if len(game_actions) < 2:
                continue
            try:
                gamestates = _fs.gamestates(game_actions, nb_prev_actions=_nb_prev)  # type: ignore[arg-type]
                x_game = _pd.concat([fn(gamestates) for fn in _feature_fns], axis=1)

                p_scores = _pd.Series(model_scores.predict_proba(x_game)[:, 1])
                p_concedes = _pd.Series(model_concedes.predict_proba(x_game)[:, 1])
                values = _vaepformula.value(game_actions, p_scores, p_concedes)  # type: ignore[arg-type]

                game_out = _pd.DataFrame(
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

                # Carry through partition keys from the input
                game_out["competition_id"] = pdf["competition_id"].iloc[0]
                game_out["season_id"] = pdf["season_id"].iloc[0]
                game_out["data_source"] = pdf["data_source"].iloc[0]

                all_scored.append(game_out)
            except Exception:  # noqa: S110
                pass  # executor — cannot log; skip failed games silently

        if not all_scored:
            return _pd.DataFrame(columns=_output_cols)

        result: _pd.DataFrame = _pd.concat(all_scored, ignore_index=True)[_output_cols]  # type: ignore[assignment]
        return result

    return _udf


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-vaep", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx=None,
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

    # Phase C: Load pre-trained models from MLflow @Champion
    # Training is handled by HF Jobs (scripts/train_vaep_model_hf.py)
    spadl_sdf = spark.table(spadl_table)

    models = _load_or_train_models(
        spark,
        catalog,
        schema,
        logger,
        training_game_ids=[],
        training_pdf=pd.DataFrame(),
    )

    if models is None:
        return

    model_scores, model_concedes = models

    # Phase D: Score unscored games via applyInPandas (distributed on executors)
    existing_vaep_games = _read_existing_game_ids(spark, catalog, schema, _VAEP_TABLE, logger)
    if existing_vaep_games:
        logger.info("Found %d games already scored in %s — will skip", len(existing_vaep_games), _VAEP_TABLE)

    # Serialize models to bytes for executor distribution via UDF closure.
    # XGBoost's C-level save_model/load_model cannot use UC Volume FUSE on
    # serverless, so we pass raw bytes through the closure instead.
    scores_raw = bytes(model_scores.get_booster().save_raw("json"))
    concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))
    logger.info("Serialized VAEP models: scores=%d bytes, concedes=%d bytes", len(scores_raw), len(concedes_raw))

    from pyspark.sql import functions as spark_fn

    # Filter SPADL to unscored games only
    all_game_rows = spadl_sdf.select("game_id").distinct().collect()
    all_game_ids = [int(r["game_id"]) for r in all_game_rows]
    unscored_game_ids = [gid for gid in all_game_ids if gid not in existing_vaep_games]

    if not unscored_game_ids:
        logger.info("All %d games already scored — nothing to do", len(all_game_ids))
        return

    logger.info("Scoring %d unscored games (of %d total) via applyInPandas", len(unscored_game_ids), len(all_game_ids))

    unscored_sdf = spadl_sdf.filter(spark_fn.col("game_id").isin(unscored_game_ids))

    # Define output schema for scored actions
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    vaep_schema = StructType(
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
            StructField("action_type", StringType()),
            StructField("result_id", LongType()),
            StructField("action_result", StringType()),
            StructField("bodypart_id", LongType()),
            StructField("bodypart", StringType()),
            StructField("offensive_value", DoubleType()),
            StructField("defensive_value", DoubleType()),
            StructField("vaep_value", DoubleType()),
            StructField("competition_id", LongType()),
            StructField("season_id", LongType()),
            StructField("data_source", StringType()),
        ]
    )

    scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)
    scored_sdf = unscored_sdf.groupBy("competition_id", "data_source").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=vaep_schema,
    )

    # Build replaceWhere predicate targeting only unscored game_ids so
    # existing VAEP scores are preserved (not destroyed by bare overwrite).
    ids_sql = ", ".join(str(gid) for gid in unscored_game_ids)
    write_delta_table(
        scored_sdf,
        catalog,
        schema,
        _VAEP_TABLE,
        replace_where=f"game_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("SPADL/VAEP pipeline complete — scoring distributed across executors")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for SPADL conversion and VAEP action valuation."""
    args = parse_ingestion_args("Compute SPADL actions and VAEP scores")
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, args.catalog, args.schema))

    logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
