"""Batch xG scoring pipeline -- executor-side inference via applyInPandas.

Loads pre-trained logistic and XGBoost models from MLflow @Champion (preferred)
or UC Volume (fallback), scores all shots from fct_shots grouped by
competition_id on Spark executors.  Writes predictions to
``{catalog}.{schema}.xg_predictions`` with ``replaceWhere`` per competition_id.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

_TABLE_NAME = "xg_predictions"


def _make_scoring_udf(
    logistic_bytes: bytes,
    xgboost_bytes: bytes,
) -> Callable[..., pd.DataFrame]:
    """Build ``applyInPandas`` UDF with model bytes captured in closure.

    Models are deserialized once per executor via the ``_model_cache`` pattern
    (function attribute dict survives across groups on the same Python worker).
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import pandas as _pd

        from analytics.xg_model import (
            XGModelConfig,
            build_features,
            deserialize_logistic_model,
            deserialize_xgboost_model,
        )

        # Executor-level model cache: deserialize once, reuse across groups
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]
        cache: dict = _udf._model_cache  # type: ignore[attr-defined]
        if "logistic" not in cache:
            cache["logistic"] = deserialize_logistic_model(logistic_bytes)
            cache["xgboost"] = deserialize_xgboost_model(xgboost_bytes)
            # Extract training-time feature names from XGBoost booster
            cc = next(iter(cache["xgboost"].calibrated_classifiers_))
            xgb_estimator = cc.estimator  # type: ignore[union-attr]
            cache["xgb_features"] = list(xgb_estimator.get_booster().feature_names)

        config = XGModelConfig()
        x, _ = build_features(pdf, config, expected_features=cache.get("xgb_features"))

        # Logistic baseline uses only distance_to_goal + shot_angle
        baseline_cols = [c for c in ["distance_to_goal", "shot_angle"] if c in x.columns]

        return _pd.DataFrame(
            {
                "shot_id": pdf["shot_id"],
                "match_id": pdf["match_id"],
                "competition_id": pdf["competition_id"],
                "xg_logistic": cache["logistic"].predict_proba(x[baseline_cols])[:, 1],
                "xg_gradient_boosted": cache["xgboost"].predict_proba(x)[:, 1],
            }
        )

    return _udf


def _try_load_champion_xg(
    log: logging.Logger,
) -> tuple[bytes, bytes] | None:
    """Try to load xG models from MLflow @Champion alias.

    Returns (logistic_bytes, xgboost_bytes) serialized from the registered
    model, or None if MLflow is not available or Champion is not registered.
    """
    try:
        import importlib

        mlflow_sklearn = importlib.import_module("mlflow.sklearn")
        mlflow_tracking = importlib.import_module("mlflow.tracking")
    except (ImportError, ModuleNotFoundError):
        log.info("mlflow not available — will load xG models from UC Volume")
        return None

    model_name = "soccer_analytics.dev_gold.xg_model"
    try:
        model_uri = f"models:/{model_name}@Champion"
        log.info("Loading xG @Champion from %s", model_uri)
        champion_model = mlflow_sklearn.load_model(model_uri)

        # Also load logistic baseline from the same run's artifacts
        client = mlflow_tracking.MlflowClient()  # type: ignore[union-attr]
        alias_info = client.get_model_version_by_alias(model_name, "Champion")
        run_id = alias_info.run_id
        logistic_uri = f"runs:/{run_id}/logistic_model"
        logistic_model = mlflow_sklearn.load_model(logistic_uri)

        # Serialize models using our JSON-based serialization (no pickle on executors)
        from analytics.xg_model import serialize_logistic_model, serialize_xgboost_model

        logistic_bytes = serialize_logistic_model(logistic_model)  # type: ignore[arg-type]
        xgboost_bytes = serialize_xgboost_model(champion_model)  # type: ignore[arg-type]

        log.info(
            "Loaded xG @Champion from MLflow (logistic=%d bytes, xgboost=%d bytes)",
            len(logistic_bytes),
            len(xgboost_bytes),
        )
        return logistic_bytes, xgboost_bytes
    except Exception:
        log.info("xG @Champion not found in MLflow registry — will load from UC Volume", exc_info=True)
        return None


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    log: logging.Logger,
) -> None:
    """Score all shots with custom xG models.

    Pipeline steps:
      1. Incremental skip guard on ``competition_id``
      2. Load ``fct_shots`` from the gold mart
      3. Load models from MLflow @Champion (preferred) or UC Volume (fallback)
      4. Distribute scoring across executors with ``applyInPandas``
      5. Write per-``competition_id`` with ``replaceWhere`` for idempotency
    """
    from ingestion.utils import write_delta_table

    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # 1. Incremental skip guard on competition_id
    existing: set[str] = set()
    try:
        existing = {
            str(row["competition_id"])
            for row in spark.table(results_table).select("competition_id").distinct().collect()
        }
    except Exception:
        log.info("No existing %s table -- will process all competitions", _TABLE_NAME)

    # 2. Load fct_shots from the gold mart (filter NULL competition_id)
    query = f"""
        SELECT shot_id, match_id, competition_id, player_id, team_id,
               location_x, location_y, end_location_x, end_location_y,
               distance_to_goal, shot_angle, shot_body_part, shot_technique,
               shot_type, play_pattern, is_first_time, period, minute, is_goal
        FROM {catalog}.dev_gold.fct_shots
        WHERE competition_id IS NOT NULL
    """  # noqa: S608
    shots_df = spark.sql(query)

    available_comps = {str(row["competition_id"]) for row in shots_df.select("competition_id").distinct().collect()}
    new_comps = available_comps - existing

    if not new_comps:
        log.info("All competitions already scored -- skipping")
        return

    log.info("Scoring %d new competitions: %s", len(new_comps), sorted(new_comps))

    # Filter to new competitions only
    new_comp_list = ", ".join(f"'{c}'" for c in new_comps)
    filter_expr = f"CAST(competition_id AS STRING) IN ({new_comp_list})"
    shots_filtered = shots_df.filter(filter_expr)

    # 3. Load models from MLflow @Champion (preferred) or UC Volume (fallback)
    champion_result = _try_load_champion_xg(log)
    if champion_result is not None:
        logistic_bytes, xgboost_bytes = champion_result
    else:
        model_dir = f"/Volumes/{catalog}/dev_gold/model_weights/xg_model"
        logistic_bytes = spark.read.format("binaryFile").load(f"{model_dir}/logistic_model.json").first()["content"]
        xgboost_bytes = spark.read.format("binaryFile").load(f"{model_dir}/xgboost_model.json").first()["content"]

    # 4. Build UDF and distribute scoring across executors
    scoring_udf = _make_scoring_udf(logistic_bytes, xgboost_bytes)

    output_schema = (
        "shot_id STRING, match_id BIGINT, competition_id INT, xg_logistic DOUBLE, xg_gradient_boosted DOUBLE"
    )
    scored_df = shots_filtered.groupBy("competition_id").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # 5. Write per competition_id with replaceWhere for idempotency
    for comp_id in new_comps:
        partition = scored_df.filter(f"competition_id = {comp_id}")
        row_count = write_delta_table(
            partition,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"competition_id = {comp_id}",
            logger=log,
        )
        log.info("Wrote %d predictions for competition_id=%s", row_count, comp_id)


def main() -> None:
    """CLI entry point for xG scoring pipeline."""
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

    args = parse_ingestion_args("Score shots with custom xG models")
    log = configure_logging("xg_model")
    spark = get_spark_session()

    log.info("Starting xG scoring pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, log)
