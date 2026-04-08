"""VAEP action valuation (inference) pipeline.

Orchestrates the end-to-end SPADL conversion and VAEP scoring pipeline:
converts bronze events to SPADL (via :mod:`ingestion.spadl_conversion`),
loads pre-trained VAEP models from the MLflow registry, and scores every
action with offensive/defensive value.

Training code lives in :mod:`ingestion.vaep_training`.
SPADL conversion code lives in :mod:`ingestion.spadl_conversion`.

Bronze tables produced:
  - spadl_actions         -- SPADL-formatted actions (intermediate)
  - vaep_action_values    -- SPADL actions with VAEP scores (final output)

Design: "Fetch Once, Fork Twice" -- ingestion tasks populate bronze,
this pipeline reads from bronze.  No external API calls.  Supports
incremental runs by skipping games already converted / scored.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
import silly_kicks.vaep.features as fs
from xgboost import XGBClassifier

from ingestion.guards import FilterResult
from ingestion.spadl_conversion import (
    _SPADL_TABLE,
    _convert_statsbomb_from_bronze,
    _convert_wyscout_from_bronze,
    _read_existing_match_ids,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA, mlflow_model_uri
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _VaepGuard:
    """SkipGuard adapter for SPADL/VAEP pipeline.

    Two-stage guard: checks both SPADL conversion and VAEP scoring,
    returning combined metadata with match ID lists for each stage.
    """

    workflow_id = "wf-vaep"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if SPADL conversion or VAEP scoring has new work."""
        from ingestion.guards import find_new_ids

        spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"
        vaep_table = f"{catalog}.{schema}.{_VAEP_TABLE}"

        # Stage 1: Source events not yet in SPADL (two sources, union results)
        sb_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.statsbomb_events",
            spadl_table,
        )
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
        )
        new_spadl = sorted(set(sb_new) | set(ws_new))

        # Stage 2: SPADL actions not yet scored with VAEP
        unscored = find_new_ids(
            spark,
            spadl_table,
            vaep_table,
        )

        total_new = len(new_spadl) + len(unscored)

        if total_new == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total_new,
            metadata={
                "new_spadl_match_ids": sorted(new_spadl),
                "unscored_vaep_match_ids": sorted(unscored),
            },
        )


skip_guard = _VaepGuard()

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
_VAEP_TABLE = "vaep_action_values"


# ---------------------------------------------------------------------------
# Phase C -- Load pre-trained VAEP models
# ---------------------------------------------------------------------------
# Training code (extract_features_for_games, train_vaep_models) has been
# extracted to ingestion.vaep_training. Production training runs on HF Jobs
# via scripts/train_vaep_model_hf.py.


def _try_load_champion_vaep(
    logger: logging.Logger,
    catalog: str,
    schema: str,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Try to load VAEP models from MLflow @Champion alias.

    Returns (model_scores, model_concedes) if found, None otherwise.
    Falls back gracefully when mlflow is not installed or models are not registered.
    """
    try:
        import importlib

        mlflow_pyfunc = importlib.import_module("mlflow.pyfunc")
    except (ImportError, ModuleNotFoundError):
        logger.info("mlflow not available -- will train VAEP models from scratch")
        return None

    model_name = mlflow_model_uri(catalog, schema, "vaep_model")
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
        logger.info("VAEP @Champion not found in MLflow registry -- will train from scratch", exc_info=True)
        return None


def _load_models(
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Load VAEP models from MLflow @Champion registry.

    Training is handled externally by HF Jobs (``scripts/train_vaep_model_hf.py``).
    Returns ``None`` when no Champion model is registered.
    """
    champion_models = _try_load_champion_vaep(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if champion_models is not None:
        return champion_models

    logger.warning(
        "No Champion VAEP model found in MLflow registry. "
        "Run scripts/train_vaep_model_hf.py on HF Jobs to train and register a model."
    )
    return None


# ---------------------------------------------------------------------------
# Phase D -- Score all actions & write
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

        import silly_kicks.spadl as _spadl
        import silly_kicks.vaep.features as _fs
        import silly_kicks.vaep.formula as _vaepformula

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
                pass  # executor -- cannot log; skip failed games silently

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
    filter_result: FilterResult | None = None,
    ctx=None,
) -> None:
    """Execute the full SPADL/VAEP pipeline.

    Memory strategy: never hold all data in memory.  Use Delta as
    intermediate storage between phases:

    1. Read bronze events, convert to SPADL per-competition -> append Delta (incremental)
    2. Read a small training subset from Delta -> extract features -> train (or load cached)
    3. Read per-competition from Delta -> score unscored games -> write results (incremental)
    """
    # Early exit if freshness gate determined no new work
    if filter_result and filter_result.count == 0:
        logger.info("Freshness gate: no new SPADL/VAEP work")
        return

    spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"

    # Phase A+B: Convert events from bronze to SPADL (incremental)
    existing_spadl_matches = _read_existing_match_ids(spark, catalog, schema, _SPADL_TABLE, logger)
    if existing_spadl_matches:
        logger.info("Found %d games already in %s -- will skip", len(existing_spadl_matches), _SPADL_TABLE)

    sb_wrote = _convert_statsbomb_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)
    ws_wrote = _convert_wyscout_from_bronze(spark, catalog, schema, logger, existing_spadl_matches)

    if not sb_wrote and not ws_wrote and not existing_spadl_matches:
        msg = "No SPADL actions produced from either StatsBomb or Wyscout"
        logger.error(msg)
        raise RuntimeError(msg)

    # Verify SPADL table has data (limit(1) avoids full DAG recomputation -- exact count not needed here)
    if spark.table(spadl_table).limit(1).count() == 0:
        msg = "SPADL table exists but is empty -- no actions to score"
        logger.error(msg)
        raise RuntimeError(msg)
    logger.info("SPADL table %s has data -- proceeding to scoring", spadl_table)

    # Phase C: Load pre-trained models from MLflow @Champion
    # Training is handled by HF Jobs (scripts/train_vaep_model_hf.py)
    spadl_sdf = spark.table(spadl_table)

    models = _load_models(catalog, schema, logger)

    if models is None:
        return

    model_scores, model_concedes = models

    # Phase D: Score unscored games via applyInPandas (distributed on executors)
    existing_vaep_matches = _read_existing_match_ids(spark, catalog, schema, _VAEP_TABLE, logger)
    if existing_vaep_matches:
        logger.info("Found %d games already scored in %s -- will skip", len(existing_vaep_matches), _VAEP_TABLE)

    # Serialize models to bytes for executor distribution via UDF closure.
    # XGBoost's C-level save_model/load_model cannot use UC Volume FUSE on
    # serverless, so we pass raw bytes through the closure instead.
    scores_raw = bytes(model_scores.get_booster().save_raw("json"))
    concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))
    logger.info("Serialized VAEP models: scores=%d bytes, concedes=%d bytes", len(scores_raw), len(concedes_raw))

    from pyspark.sql import functions as spark_fn

    # Filter SPADL to unscored matches only (using match_id, not game_id)
    all_match_rows = spadl_sdf.select("match_id").distinct().collect()
    all_match_ids = [int(r["match_id"]) for r in all_match_rows]
    unscored_match_ids = [mid for mid in all_match_ids if mid not in existing_vaep_matches]

    if not unscored_match_ids:
        logger.info("All %d matches already scored -- nothing to do", len(all_match_ids))
        return

    logger.info(
        "Scoring %d unscored matches (of %d total) via applyInPandas",
        len(unscored_match_ids),
        len(all_match_ids),
    )

    unscored_sdf = spadl_sdf.filter(spark_fn.col("match_id").isin(unscored_match_ids))

    # Define output schema for scored actions
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    vaep_schema = StructType(
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
    # Group by match_id -- each match is ~1,600 SPADL actions (~5 MB), well
    # within the 800 MB serverless UDF budget.  Competition-level grouping
    # OOMs on large datasets (La Liga = 600K+ rows per group).  The model
    # cache (_model_cache) loads once per executor, not per group.
    scored_sdf = unscored_sdf.groupBy("match_id", "data_source").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=vaep_schema,
    )

    # Build replaceWhere predicate targeting only unscored match_ids so
    # existing VAEP scores are preserved (not destroyed by bare overwrite).
    ids_sql = ", ".join(str(mid) for mid in unscored_match_ids)
    write_delta_table(
        scored_sdf,
        catalog,
        schema,
        _VAEP_TABLE,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("SPADL/VAEP pipeline complete -- scoring distributed across executors")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for SPADL conversion and VAEP action valuation."""
    args = parse_ingestion_args("Compute SPADL actions and VAEP scores")
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-vaep")

    logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
