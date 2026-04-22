"""Batch xG v2 scoring pipeline -- set encoder with MC dropout uncertainty.

Loads the v2 set encoder weights from MLflow ``xg_model_v2@Champion`` (preferred)
or UC Volume (fallback), plus v1 XGBoost model for tabular feature extraction.
Scores all shots with freeze-frame context from ``fct_shots`` joined to
``stg_statsbomb__events``, grouped by ``competition_id`` on Spark executors.
Writes predictions to ``{catalog}.{schema}.xg_predictions_v2`` with
``replaceWhere`` per ``competition_id``.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

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

_TABLE_NAME = "xg_predictions_v2"
# Bronze column contract (Python-side mirror of the post-PR-3 Delta schema).
# Used by src/tests/test_bronze_live_schema.py to assert writer/live parity.
# _ingested_at is added by write_delta_table, not emitted by this module.
# match_id was historically emitted; PR 3 Phase 5 drops it from _RESULTS_SCHEMA
# and a post-merge ALTER TABLE removes it from bronze.
_XG_V2_BRONZE_COLS: tuple[str, ...] = (
    "shot_id",
    "competition_id",
    "xg_set_encoder",
    "xg_ci_lower",
    "xg_ci_upper",
)
_RESULTS_SCHEMA = (
    "shot_id STRING, competition_id INT, "
    "xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE, _ingested_at TIMESTAMP"
)
_guard_logger = logging.getLogger(f"{__name__}.guard")


class _XgV2Guard:
    """SkipGuard adapter for xG v2 scoring pipeline."""

    workflow_id = "wf-xg-v2"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which competitions need xG v2 scoring."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)

        new_comps = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots",
            results_table=f"{catalog}.{schema}.{_TABLE_NAME}",
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


skip_guard = _XgV2Guard()


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

    model_name = mlflow_model_uri(catalog, schema, "xg_model_v2")
    try:
        client = mlflow_tracking.MlflowClient()  # type: ignore[union-attr]
        alias_info = client.get_model_version_by_alias(model_name, "Champion")
        run_id = alias_info.run_id

        # Download the weights artifact
        artifact_path = mlflow_mod.artifacts.download_artifacts(  # type: ignore[union-attr]
            run_id=run_id, artifact_path="model_weights.json"
        )
        with open(artifact_path, "rb") as f:
            weights_bytes = f.read()

        # SEC2: verify artifact integrity against recorded MLflow tag (if any)
        verify_artifact_hash(
            data=weights_bytes,
            expected_sha256=_load_mlflow_artifact_hash(client, model_name, alias="Champion"),
            artifact_label=f"{model_name}_v2_weights",
            logger=log,
        )

        log.info("Loaded xG v2 @Champion from MLflow (%d bytes, run=%s)", len(weights_bytes), run_id)
        return weights_bytes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion
        log.info("xG v2 @Champion not found in MLflow registry", exc_info=True)
        return None


def _try_load_champion_xgboost(
    log: logging.Logger,
    catalog: str,
    schema: str,
) -> bytes | None:
    """Try to load v1 XGBoost model bytes from MLflow @Champion.

    The v2 UDF needs XGBoost to build tabular features (``build_features``
    requires the XGBoost booster's feature names).  Returns serialized bytes
    or None if MLflow is unavailable.
    """
    try:
        mlflow_sklearn = importlib.import_module("mlflow.sklearn")
    except (ImportError, ModuleNotFoundError):
        log.info("mlflow not available — will load XGBoost model from UC Volume")
        return None

    model_name = mlflow_model_uri(catalog, schema, "xg_model")
    try:
        model_uri = f"models:/{model_name}@Champion"
        log.info("Loading XGBoost @Champion from %s", model_uri)
        champion_model = mlflow_sklearn.load_model(model_uri)

        from analytics.xg_model import serialize_xgboost_model

        xgboost_bytes = serialize_xgboost_model(champion_model)  # type: ignore[arg-type]

        # SEC2: verify artifact integrity against recorded MLflow tag (if any)
        mlflow_tracking = importlib.import_module("mlflow.tracking")
        client = mlflow_tracking.MlflowClient()
        verify_artifact_hash(
            data=xgboost_bytes,
            expected_sha256=_load_mlflow_artifact_hash(client, model_name, alias="Champion"),
            artifact_label=f"{model_name}_xgboost_for_v2",
            logger=log,
        )

        log.info("Loaded XGBoost @Champion from MLflow (%d bytes)", len(xgboost_bytes))
        return xgboost_bytes
    except Exception:  # noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion
        log.info("XGBoost @Champion not found in MLflow registry — will load from UC Volume", exc_info=True)
        return None


def _load_shots_with_context(
    spark: Any,
    catalog: str,
) -> Any:
    """Load shots with inline freeze-frame context for v2 scoring.

    Joins ``fct_shots`` (gold) to ``stg_statsbomb__events`` (silver) via
    the surrogate key formula to retrieve ``shot_freeze_frame`` JSON.
    Non-StatsBomb shots get NULL freeze frames (gracefully handled by v2 UDF).
    """
    query = f"""
        SELECT s.shot_id, s.competition_id, s.player_id, s.team_id,
               s.location_x, s.location_y, s.end_location_x, s.end_location_y,
               s.distance_to_goal, s.shot_angle, s.shot_body_part, s.shot_technique,
               s.shot_type, s.play_pattern, s.is_first_time, s.period, s.minute,
               s.is_goal, s.data_source,
               e.shot_freeze_frame
        FROM {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_shots s
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


def _make_v2_scoring_udf(
    v2_weights_bytes: bytes,
    xgboost_bytes: bytes,
) -> Callable[..., pd.DataFrame]:
    """Build ``applyInPandas`` UDF for v2 set encoder scoring.

    The UDF deserializes both the v2 set encoder weights and the v1 XGBoost
    model (needed for ``build_features`` feature names) once per executor
    via the ``_model_cache`` pattern.  For each shot with a non-null
    freeze frame, it encodes the player set and runs MC dropout inference.
    Shots without freeze frames produce NaN predictions.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import numpy as _np
        import pandas as _pd

        from analytics.set_encoder import (
            deserialize_set_encoder_weights,
            encode_player_set,
            predict_xg_with_uncertainty,
        )
        from analytics.xg_model import (
            XGModelConfig,
            build_features,
            deserialize_xgboost_model,
            parse_freeze_frame,
        )

        # Executor-level model cache: deserialize once, reuse across groups
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]
        cache: dict[str, Any] = _udf._model_cache  # type: ignore[attr-defined]
        if "v2_weights" not in cache:
            import json as _json

            cache["v2_weights"] = deserialize_set_encoder_weights(v2_weights_bytes)
            cache["xgboost"] = deserialize_xgboost_model(xgboost_bytes)
            # Extract v1 training-time feature names from the XGBoost booster
            # (used as a fallback for legacy v2 envelopes that don't carry
            # feature_names of their own; see below).
            cc = next(iter(cache["xgboost"].calibrated_classifiers_))
            xgb_estimator = cc.estimator  # type: ignore[union-attr]
            cache["xgb_features"] = list(xgb_estimator.get_booster().feature_names)
            # Prefer v2's own feature list embedded in the weights envelope.
            # Post-2026-04-22 training writes ``envelope["feature_names"]`` so
            # inference aligns tabular input to v2's tabular_dim directly,
            # without coupling to whatever v1 XGBoost happens to be on disk.
            # Legacy envelopes (pre-2026-04-22) lack this field — fall through
            # to ``xgb_features`` to preserve backward compatibility until a
            # fresh v2 training run replaces the weights.
            v2_envelope = _json.loads(v2_weights_bytes.decode("utf-8"))
            v2_features = v2_envelope.get("feature_names")
            cache["v2_features"] = list(v2_features) if v2_features else cache["xgb_features"]

        # Build tabular features for the set encoder's tabular input. Use v2's
        # embedded feature list when present; otherwise fall back to the v1
        # XGBoost feature list (legacy envelope).
        config = XGModelConfig()
        x, _ = build_features(pdf, config, expected_features=cache.get("v2_features"))

        v2_weights = cache["v2_weights"]
        n_rows = len(pdf)
        xg_set_encoder = _np.full(n_rows, _np.nan, dtype=_np.float64)
        xg_ci_lower = _np.full(n_rows, _np.nan, dtype=_np.float64)
        xg_ci_upper = _np.full(n_rows, _np.nan, dtype=_np.float64)

        if "shot_freeze_frame" in pdf.columns:
            # Pre-extract arrays to avoid O(n^2) pdf.iloc[i] (F-04 OPT-AUDIT-200)
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
                "competition_id": pdf["competition_id"],
                "xg_set_encoder": xg_set_encoder,
                "xg_ci_lower": xg_ci_lower,
                "xg_ci_upper": xg_ci_upper,
            }
        )

    return _udf


@workflow("wf-xg-v2", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: Any = None,
) -> int:
    """Score all shots with v2 set encoder xG model (Deep Sets + MC dropout).

    Pipeline steps:
      1. Use guard-provided new competition IDs
      2. Load ``fct_shots`` with freeze-frame context from gold/silver marts
      3. Load v2 set encoder weights from MLflow @Champion or UC Volume
      4. Load v1 XGBoost model (needed for tabular feature extraction)
      5. Distribute scoring across executors with ``applyInPandas``
      6. Write per-``competition_id`` with ``replaceWhere`` for idempotency
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from ingestion.utils import write_delta_table

    # 1. Use guard-provided new competition IDs
    new_comps = filter_result.metadata["new_competition_ids"]

    if not new_comps:
        logger.info("All competitions already scored with v2 -- skipping")
        return 0

    logger.info("Scoring %d new competitions with v2: %s", len(new_comps), sorted(new_comps))

    # 2. Load fct_shots with freeze-frame context, filtered to new competitions
    shots_df = _load_shots_with_context(spark, catalog)

    new_comp_list = ", ".join(f"'{c}'" for c in new_comps)
    filter_expr = f"CAST(competition_id AS STRING) IN ({new_comp_list})"
    shots_filtered = shots_df.filter(filter_expr)

    # 3. Load v2 set encoder weights from MLflow @Champion or UC Volume.
    # MLflow UC model registry lives in the gold schema (DEFAULT_GOLD_SCHEMA="dev_gold"),
    # NOT the pipeline's `schema` arg (which is "bronze" for xg_predictions_v2
    # output). Passing `schema` here hits `{catalog}.bronze.xg_model_v2`, which
    # does not exist, forcing every inference run to fall back to UC Volume.
    # Pre-2026-04-22 that was invisible because training never registered a
    # Champion; post-hardening (train_xg_v2_hf.py) it registers on every run,
    # so we must point at the right namespace to actually find it.
    v2_weights_bytes = _try_load_champion_xg_v2(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if v2_weights_bytes is None:
        v2_model_path = f"/Volumes/{catalog}/{DEFAULT_GOLD_SCHEMA}/model_weights/xg_model_v2/model_weights.json"
        try:
            v2_weights_bytes = spark.read.format("binaryFile").load(v2_model_path).first()["content"]
        except Exception as exc:
            msg = (
                f"xG v2 weights not available at MLflow @Champion OR UC Volume {v2_model_path}. "
                "Cannot run v2 scoring pipeline — weights must be present for inference. "
                "Train and register a model via scripts/train_xg_v2_hf.py before running."
            )
            raise RuntimeError(msg) from exc
        # SEC2: verify artifact integrity from UC Volume sidecar (if any)
        verify_artifact_hash(
            data=v2_weights_bytes,
            expected_sha256=_load_volume_sidecar_hash(v2_model_path),
            artifact_label="xg_model_v2_weights_volume",
            logger=logger,
        )
        logger.info("Loaded xG v2 weights from UC Volume (%d bytes)", len(v2_weights_bytes))

    # 4. Load v1 XGBoost model (needed for tabular feature extraction)
    xgboost_result = _try_load_champion_xgboost(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if xgboost_result is not None:
        xgboost_bytes = xgboost_result
    else:
        model_dir = f"/Volumes/{catalog}/{DEFAULT_GOLD_SCHEMA}/model_weights/xg_model"
        xgboost_bytes = spark.read.format("binaryFile").load(f"{model_dir}/xgboost_model.json").first()["content"]
        # SEC2: verify artifact integrity from UC Volume sidecar (if any)
        verify_artifact_hash(
            data=xgboost_bytes,
            expected_sha256=_load_volume_sidecar_hash(f"{model_dir}/xgboost_model.json"),
            artifact_label="xg_model_xgboost_volume_for_v2",
            logger=logger,
        )

    # 5. Build UDF and distribute scoring across executors
    scoring_udf = _make_v2_scoring_udf(v2_weights_bytes, xgboost_bytes)

    output_schema = "shot_id STRING, competition_id INT, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE"
    scored_df = shots_filtered.groupBy("competition_id").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # 6. Materialize scored_df to avoid re-executing applyInPandas DAG per
    # competition_id write (F-07 OPT-AUDIT-200).  Without this, each .filter()
    # triggers a full re-run of the UDF across all groups.
    _temp_table = f"{catalog}.{schema}._xg_v2_scored_temp"
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
        logger.info("Wrote %d v2 predictions for competition_id=%s", row_count, comp_id)

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
    """CLI entry point for xG v2 scoring pipeline."""
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

    args = parse_ingestion_args("Score shots with xG v2 set encoder model")
    logger = configure_logging("xg_model_v2")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting xG v2 scoring pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
