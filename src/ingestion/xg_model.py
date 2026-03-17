"""Batch xG scoring pipeline -- executor-side inference via applyInPandas.

Loads pre-trained logistic and XGBoost models from MLflow @Champion (preferred)
or UC Volume (fallback), scores all shots from fct_shots grouped by
competition_id on Spark executors.  Writes predictions to
``{catalog}.{schema}.xg_predictions`` with ``replaceWhere`` per competition_id.

V2 extension: optionally loads a set encoder model (Deep Sets + MC dropout)
from MLflow ``xg_model_v2@Champion`` or UC Volume fallback.  When available,
produces additional columns: ``xg_set_encoder``, ``xg_ci_lower``, ``xg_ci_upper``.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

_TABLE_NAME = "xg_predictions"


def _make_scoring_udf(
    logistic_bytes: bytes,
    xgboost_bytes: bytes,
    v2_weights_bytes: bytes | None = None,
) -> Callable[..., pd.DataFrame]:
    """Build ``applyInPandas`` UDF with model bytes captured in closure.

    Models are deserialized once per executor via the ``_model_cache`` pattern
    (function attribute dict survives across groups on the same Python worker).

    When ``v2_weights_bytes`` is provided, the UDF also runs the set encoder
    on ``shot_freeze_frame`` JSON to produce ``xg_set_encoder``,
    ``xg_ci_lower``, and ``xg_ci_upper`` columns.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import numpy as _np
        import pandas as _pd

        from analytics.xg_model import (
            XGModelConfig,
            build_features,
            deserialize_logistic_model,
            deserialize_xgboost_model,
            parse_freeze_frame,
        )

        # Executor-level model cache: deserialize once, reuse across groups
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]
        cache: dict[str, Any] = _udf._model_cache  # type: ignore[attr-defined]
        if "logistic" not in cache:
            cache["logistic"] = deserialize_logistic_model(logistic_bytes)
            cache["xgboost"] = deserialize_xgboost_model(xgboost_bytes)
            # Extract training-time feature names from XGBoost booster
            cc = next(iter(cache["xgboost"].calibrated_classifiers_))
            xgb_estimator = cc.estimator  # type: ignore[union-attr]
            cache["xgb_features"] = list(xgb_estimator.get_booster().feature_names)

            # Deserialize v2 set encoder weights if provided
            if v2_weights_bytes is not None:
                from analytics.set_encoder import deserialize_set_encoder_weights

                cache["v2_weights"] = deserialize_set_encoder_weights(v2_weights_bytes)
            else:
                cache["v2_weights"] = None

        config = XGModelConfig()
        x, _ = build_features(pdf, config, expected_features=cache.get("xgb_features"))

        # Logistic baseline uses only distance_to_goal + shot_angle
        baseline_cols = [c for c in ["distance_to_goal", "shot_angle"] if c in x.columns]

        # V1 predictions (always computed)
        xg_logistic = cache["logistic"].predict_proba(x[baseline_cols])[:, 1]
        xg_gradient_boosted = cache["xgboost"].predict_proba(x)[:, 1]

        # V2 predictions (set encoder with MC dropout uncertainty)
        n_rows = len(pdf)
        xg_set_encoder = _np.full(n_rows, _np.nan, dtype=_np.float64)
        xg_ci_lower = _np.full(n_rows, _np.nan, dtype=_np.float64)
        xg_ci_upper = _np.full(n_rows, _np.nan, dtype=_np.float64)

        v2_weights = cache.get("v2_weights")
        if v2_weights is not None and "shot_freeze_frame" in pdf.columns:
            from analytics.set_encoder import (
                encode_player_set,
                predict_xg_with_uncertainty,
            )

            # Pre-extract arrays to avoid O(n²) pdf.iloc[i] (F-04 OPT-AUDIT-200)
            ff_jsons = pdf["shot_freeze_frame"].to_numpy()
            tabular_rows = x.to_numpy().astype(_np.float64)

            for i in range(n_rows):
                ff_json = ff_jsons[i]
                if ff_json is None or (isinstance(ff_json, float) and _np.isnan(ff_json)):
                    continue

                player_features = parse_freeze_frame(str(ff_json))
                if player_features.shape[0] == 0:
                    continue

                context_vector = encode_player_set(player_features, v2_weights)
                tabular = tabular_rows[i]

                mean, _std, ci_lower, ci_upper = predict_xg_with_uncertainty(tabular, context_vector, v2_weights)
                xg_set_encoder[i] = mean
                xg_ci_lower[i] = ci_lower
                xg_ci_upper[i] = ci_upper

        return _pd.DataFrame(
            {
                "shot_id": pdf["shot_id"],
                "match_id": pdf["match_id"],
                "competition_id": pdf["competition_id"],
                "xg_logistic": xg_logistic,
                "xg_gradient_boosted": xg_gradient_boosted,
                "xg_set_encoder": xg_set_encoder,
                "xg_ci_lower": xg_ci_lower,
                "xg_ci_upper": xg_ci_upper,
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


def _try_load_champion_xg_v2(
    log: logging.Logger,
    catalog: str,
    schema: str,
) -> bytes | None:
    """Try to load xG v2 set encoder weights from MLflow @Champion alias.

    Returns serialized weight bytes, or None if the v2 model is not registered.
    Falls back to UC Volume ``model_weights/xg_model_v2/model_weights.json``.
    """
    try:
        mlflow_mod = importlib.import_module("mlflow")
        mlflow_tracking = importlib.import_module("mlflow.tracking")
    except (ImportError, ModuleNotFoundError):
        log.info("mlflow not available — will try UC Volume for xG v2 weights")
        return None

    model_name = f"{catalog}.{schema}.xg_model_v2"
    try:
        client = mlflow_tracking.MlflowClient()  # type: ignore[union-attr]
        alias_info = client.get_model_version_by_alias(model_name, "Champion")
        run_id = alias_info.run_id

        # Download the weights artifact
        artifact_path = mlflow_mod.artifacts.download_artifacts(run_id=run_id, artifact_path="model_weights.json")
        with open(artifact_path, "rb") as f:
            weights_bytes = f.read()

        log.info("Loaded xG v2 @Champion from MLflow (%d bytes, run=%s)", len(weights_bytes), run_id)
        return weights_bytes
    except Exception:
        log.info("xG v2 @Champion not found in MLflow registry", exc_info=True)
        return None


def _load_shots_with_context(
    spark: Any,
    catalog: str,
    schema: str,
) -> Any:
    """Load shots with inline freeze-frame context for v2 scoring.

    Joins ``fct_shots`` (gold) to ``stg_statsbomb__events`` (silver) via
    the surrogate key formula to retrieve ``shot_freeze_frame`` JSON.
    Non-StatsBomb shots get NULL freeze frames (gracefully handled by v2 UDF).
    """
    query = f"""
        SELECT s.shot_id, s.match_id, s.competition_id, s.player_id, s.team_id,
               s.location_x, s.location_y, s.end_location_x, s.end_location_y,
               s.distance_to_goal, s.shot_angle, s.shot_body_part, s.shot_technique,
               s.shot_type, s.play_pattern, s.is_first_time, s.period, s.minute,
               s.is_goal, s.data_source,
               e.shot_freeze_frame
        FROM {catalog}.dev_gold.fct_shots s
        LEFT JOIN {catalog}.dev_silver.stg_statsbomb__events e
            ON s.shot_id = md5(CAST(CONCAT(
                   COALESCE(CAST(e.event_id AS STRING), '_dbt_utils_surrogate_key_null_'),
                   '-',
                   COALESCE(CAST('statsbomb' AS STRING), '_dbt_utils_surrogate_key_null_')
               ) AS STRING))
            AND e.event_type = 'Shot'
        WHERE s.competition_id IS NOT NULL
    """  # noqa: S608
    return spark.sql(query)


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    log: logging.Logger,
) -> None:
    """Score all shots with custom xG models.

    Pipeline steps:
      1. Incremental skip guard on ``competition_id``
      2. Load ``fct_shots`` with freeze-frame context from the gold/silver marts
      3. Load v1 models from MLflow @Champion (preferred) or UC Volume (fallback)
      4. Optionally load v2 set encoder weights
      5. Distribute scoring across executors with ``applyInPandas``
      6. Write per-``competition_id`` with ``replaceWhere`` for idempotency
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

    # 2. Load fct_shots with freeze-frame context
    shots_df = _load_shots_with_context(spark, catalog, schema)

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

    # 3. Load v1 models from MLflow @Champion (preferred) or UC Volume (fallback)
    champion_result = _try_load_champion_xg(log)
    if champion_result is not None:
        logistic_bytes, xgboost_bytes = champion_result
    else:
        model_dir = f"/Volumes/{catalog}/dev_gold/model_weights/xg_model"
        logistic_bytes = spark.read.format("binaryFile").load(f"{model_dir}/logistic_model.json").first()["content"]
        xgboost_bytes = spark.read.format("binaryFile").load(f"{model_dir}/xgboost_model.json").first()["content"]

    # 4. Optionally load v2 set encoder weights
    v2_weights_bytes = _try_load_champion_xg_v2(log, catalog, schema)
    if v2_weights_bytes is None:
        # Fallback: try UC Volume
        v2_model_path = f"/Volumes/{catalog}/dev_gold/model_weights/xg_model_v2/model_weights.json"
        try:
            v2_weights_bytes = spark.read.format("binaryFile").load(v2_model_path).first()["content"]
            log.info("Loaded xG v2 weights from UC Volume (%d bytes)", len(v2_weights_bytes))
        except Exception:
            log.info("No xG v2 weights found -- v2 columns will be NULL")
            v2_weights_bytes = None

    # 5. Build UDF and distribute scoring across executors
    scoring_udf = _make_scoring_udf(logistic_bytes, xgboost_bytes, v2_weights_bytes)

    output_schema = (
        "shot_id STRING, match_id BIGINT, competition_id INT,"
        " xg_logistic DOUBLE, xg_gradient_boosted DOUBLE,"
        " xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE"
    )
    scored_df = shots_filtered.groupBy("competition_id").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # 6. Materialize scored_df to avoid re-executing applyInPandas DAG per
    # competition_id write (F-07 OPT-AUDIT-200).  Without this, each .filter()
    # triggers a full re-run of the UDF across all groups.
    _temp_table = f"{catalog}.{schema}._xg_scored_temp"
    scored_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(_temp_table)
    scored_materialized = spark.table(_temp_table)

    for comp_id in new_comps:
        partition = scored_materialized.filter(f"competition_id = {comp_id}")
        row_count = write_delta_table(
            partition,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"competition_id = {comp_id}",
            logger=log,
        )
        log.info("Wrote %d predictions for competition_id=%s", row_count, comp_id)

    # Clean up temp table
    try:
        spark.sql(f"DROP TABLE IF EXISTS {_temp_table}")
    except Exception:
        log.debug("Could not drop temp table %s", _temp_table, exc_info=True)


def main() -> None:
    """CLI entry point for xG scoring pipeline."""
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

    args = parse_ingestion_args("Score shots with custom xG models")
    log = configure_logging("xg_model")
    spark = get_spark_session()

    log.info("Starting xG scoring pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, log)
