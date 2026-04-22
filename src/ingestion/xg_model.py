"""Batch xG v1 scoring pipeline -- executor-side inference via applyInPandas.

Loads pre-trained logistic and XGBoost models from MLflow @Champion (preferred)
or UC Volume (fallback), scores all shots from fct_shots grouped by
competition_id on Spark executors.  Writes predictions to
``{catalog}.{schema}.xg_predictions`` with ``replaceWhere`` per competition_id.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    _load_mlflow_artifact_hash,
    _load_volume_sidecar_hash,
    verify_artifact_hash,
)
from shared.constants import DEFAULT_GOLD_SCHEMA, mlflow_model_uri
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

_TABLE_NAME = "xg_predictions"
_RESULTS_SCHEMA = (
    "shot_id STRING, match_id BIGINT, competition_id INT, "
    "xg_logistic DOUBLE, xg_gradient_boosted DOUBLE, _ingested_at TIMESTAMP"
)
_guard_logger = logging.getLogger(f"{__name__}.guard")


class _XgV1Guard:
    """SkipGuard adapter for xG v1 scoring pipeline."""

    workflow_id = "wf-xg-v1"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which competitions need xG v1 scoring."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        new_comps = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots",
            results_table=results_table,
            id_column="competition_id",
            source_filter="competition_id IS NOT NULL",
        )
        if not new_comps:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_comps),
            metadata={"new_competition_ids": sorted(new_comps)},
        )


skip_guard = _XgV1Guard()


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
        cache: dict[str, object] = _udf._model_cache  # type: ignore[attr-defined]
        if "logistic" not in cache:
            cache["logistic"] = deserialize_logistic_model(logistic_bytes)
            cache["xgboost"] = deserialize_xgboost_model(xgboost_bytes)
            # Extract training-time feature names from XGBoost booster
            cc = next(iter(cache["xgboost"].calibrated_classifiers_))  # type: ignore[union-attr]
            xgb_estimator = cc.estimator  # type: ignore[union-attr]
            cache["xgb_features"] = list(xgb_estimator.get_booster().feature_names)

        config = XGModelConfig()
        x, _ = build_features(pdf, config, expected_features=cache.get("xgb_features"))  # type: ignore[arg-type]

        # Logistic baseline uses only distance_to_goal + shot_angle
        baseline_cols = [c for c in ["distance_to_goal", "shot_angle"] if c in x.columns]

        xg_logistic = cache["logistic"].predict_proba(x[baseline_cols])[:, 1]  # type: ignore[union-attr]
        xg_gradient_boosted = cache["xgboost"].predict_proba(x)[:, 1]  # type: ignore[union-attr]

        return _pd.DataFrame(
            {
                "shot_id": pdf["shot_id"],
                "match_id": pdf["match_id"],
                "competition_id": pdf["competition_id"],
                "xg_logistic": xg_logistic,
                "xg_gradient_boosted": xg_gradient_boosted,
            }
        )

    return _udf


def _try_load_champion_xg(
    log: logging.Logger,
    catalog: str,
    schema: str,
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

    model_name = mlflow_model_uri(catalog, schema, "xg_model")
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

        # SEC2: verify artifact integrity against recorded MLflow tag (if any)
        expected_hash = _load_mlflow_artifact_hash(client, model_name, alias="Champion")
        verify_artifact_hash(
            data=logistic_bytes,
            expected_sha256=expected_hash,
            artifact_label=f"{model_name}_logistic",
            logger=log,
        )
        verify_artifact_hash(
            data=xgboost_bytes,
            expected_sha256=expected_hash,
            artifact_label=f"{model_name}_xgboost",
            logger=log,
        )

        log.info(
            "Loaded xG @Champion from MLflow (logistic=%d bytes, xgboost=%d bytes)",
            len(logistic_bytes),
            len(xgboost_bytes),
        )
        return logistic_bytes, xgboost_bytes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion
        log.info("xG @Champion not found in MLflow registry — will load from UC Volume", exc_info=True)
        return None


@workflow("wf-xg-v1", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Score all shots with v1 logistic + XGBoost xG models.

    Pipeline steps:
      1. Use guard-provided new competition IDs
      2. Load ``fct_shots`` from the gold mart
      3. Load v1 models from MLflow @Champion (preferred) or UC Volume (fallback)
      4. Distribute scoring across executors with ``applyInPandas``
      5. Write per-``competition_id`` with ``replaceWhere`` for idempotency
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from ingestion.utils import write_delta_table

    # 1. Use guard-provided new competition IDs
    new_comps = filter_result.metadata["new_competition_ids"]

    if not new_comps:
        logger.info("All competitions already scored -- skipping")
        return 0

    logger.info("Scoring %d new competitions: %s", len(new_comps), sorted(new_comps))

    # 2. Load fct_shots from gold mart, filtered to new competitions only.
    # JOIN dim_matches to recover native match_id for bronze write-back —
    # PR 3 (ADR-011) migrated fct_shots from match_id (native BIGINT) to
    # match_key (Kimball surrogate BIGINT). Bronze.xg_predictions v1 schema
    # is preserved for back-compat (Chesterton's Fence per PR 3 spec §5.10),
    # so we recover native match_id here via the Kimball dim.
    shots_df = spark.sql(
        f"""
        SELECT s.*, cast(dm.native_match_id as bigint) AS match_id
        FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
        LEFT JOIN {catalog}.{DEFAULT_GOLD_SCHEMA}.dim_matches dm
          ON s.match_key = dm.match_key
        WHERE s.competition_id IS NOT NULL
        """  # noqa: S608
    )

    new_comp_list = ", ".join(f"'{c}'" for c in new_comps)
    filter_expr = f"CAST(competition_id AS STRING) IN ({new_comp_list})"
    shots_filtered = shots_df.filter(filter_expr)

    # 3. Load v1 models from MLflow @Champion (preferred) or UC Volume (fallback)
    champion_result = _try_load_champion_xg(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if champion_result is not None:
        logistic_bytes, xgboost_bytes = champion_result
    else:
        model_dir = f"/Volumes/{catalog}/{DEFAULT_GOLD_SCHEMA}/model_weights/xg_model"
        logistic_bytes = spark.read.format("binaryFile").load(f"{model_dir}/logistic_model.json").first()["content"]
        xgboost_bytes = spark.read.format("binaryFile").load(f"{model_dir}/xgboost_model.json").first()["content"]

        # SEC2: verify artifact integrity from UC Volume sidecar files
        verify_artifact_hash(
            data=logistic_bytes,
            expected_sha256=_load_volume_sidecar_hash(f"{model_dir}/logistic_model.json"),
            artifact_label="xg_model_logistic_volume",
            logger=logger,
        )
        verify_artifact_hash(
            data=xgboost_bytes,
            expected_sha256=_load_volume_sidecar_hash(f"{model_dir}/xgboost_model.json"),
            artifact_label="xg_model_xgboost_volume",
            logger=logger,
        )

    # 4. Build UDF and distribute scoring across executors
    scoring_udf = _make_scoring_udf(logistic_bytes, xgboost_bytes)

    output_schema = (
        "shot_id STRING, match_id BIGINT, competition_id INT, xg_logistic DOUBLE, xg_gradient_boosted DOUBLE"
    )
    scored_df = shots_filtered.groupBy("competition_id").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # 5. Materialize scored_df to avoid re-executing applyInPandas DAG per
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
            logger=logger,
        )
        logger.info("Wrote %d predictions for competition_id=%s", row_count, comp_id)

    # Clean up temp table. Use DROP TABLE IF EXISTS so missing-table is a no-op;
    # any other exception (permission denied, session dead) is logged at debug
    # since the temp table will be cleaned up by the workspace garbage collector
    # on session termination anyway.
    try:
        spark.sql(f"DROP TABLE IF EXISTS {_temp_table}")
    except Exception:  # noqa: BLE001 — best-effort cleanup; session GC handles it if this fails
        logger.debug("Could not drop temp table %s", _temp_table, exc_info=True)
    return 0


def main() -> None:
    """CLI entry point for xG scoring pipeline."""
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

    args = parse_ingestion_args("Score shots with custom xG models")
    logger = configure_logging("xg_model")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting xG scoring pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
