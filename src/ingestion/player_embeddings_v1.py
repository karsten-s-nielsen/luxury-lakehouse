"""v1 Doc2Vec player embedding pipeline.

Computes 32-dim Doc2Vec embeddings via ``applyInPandas`` with flat
partitioning by ``batch_id``.  The Doc2Vec model is loaded from UC Volumes
on each executor and cached in a module-level dict so it is only loaded
once per JVM lifetime.

Bronze table produced:
  - player_embeddings_raw
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, cast

import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.player_embeddings_common import (
    _PLAYERS_PER_BATCH,
    _TABLE_NAME,
    _build_bronze_dataframe,
    _compute_stat_vectors,
    _load_events_sdf,
    _merge_vectors,
    _save_norm_params,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_RESULTS_SCHEMA = (
    "canonical_player_id STRING, match_id STRING, data_source STRING, "
    "behavioral_vector ARRAY<DOUBLE>, stat_vector ARRAY<DOUBLE>, _ingested_at TIMESTAMP"
)
_guard_logger = logging.getLogger(f"{__name__}.guard")


class _Football2VecGuard:
    """SkipGuard adapter for v1 Doc2Vec player embedding pipeline."""

    workflow_id = "wf-football2vec"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if new source matches need embedding computation."""
        import logging as _logging

        from ingestion.guards import ensure_table, find_new_ids
        from ingestion.utils import tolerate_missing_table

        source_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values"
        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)

        # Defensive fallback: if source returns empty but results exist,
        # skip rather than recomputing everything.
        has_results = False
        with tolerate_missing_table(_logging.getLogger(__name__), f"No results table yet at {results_table}"):
            has_results = spark.table(results_table).limit(1).count() > 0

        new_match_ids = find_new_ids(
            spark,
            source_table=source_table,
            results_table=results_table,
        )

        if not new_match_ids and has_results:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": sorted(new_match_ids)},
        )


skip_guard = _Football2VecGuard()


# ---------------------------------------------------------------------------
# applyInPandas UDF for behavioral inference
# ---------------------------------------------------------------------------


def _make_behavioral_udf(model_path: str) -> object:
    """Build the ``applyInPandas`` UDF closure for Doc2Vec behavioral inference.

    The Doc2Vec model is loaded from a UC Volume path on each executor and
    cached in a module-level dict so it is loaded only once per JVM lifetime.
    All library imports happen inside the closure so they are available on
    Spark executors without requiring module-level serialisation.

    Args:
        model_path: UC Volume path to the Doc2Vec model file
            (e.g. ``/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/player2vec.model``).

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Tokenize events and infer Doc2Vec embeddings for a batch of players."""
        import json as _json
        import math as _math
        from typing import Any as _Any

        import pandas as _pd

        _output_cols = _pd.Index(
            [
                "canonical_player_id",
                "match_id",
                "data_source",
                "behavioral_vector",
                "competition_id",
                "season_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        # ---- Model loading with executor-level cache ----
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]

        cache: dict = _udf._model_cache  # type: ignore[attr-defined]
        if "model" not in cache:
            from typing import cast as _cast

            from gensim.models.doc2vec import Doc2Vec as _Doc2Vec

            cache["model"] = _cast(_Doc2Vec, _Doc2Vec.load(model_path))

        model: _Any = cache["model"]

        # ---- Tokenize events per (player, match) ----
        # SPADL 23-type vocabulary (D29): action_type is the canonical token.
        # Grid: 12x8 on 105x68m SPADL coordinate system.
        grid_cols, grid_rows = 12, 8
        pitch_length, pitch_width = 105.0, 68.0
        cell_w = pitch_length / grid_cols
        cell_h = pitch_width / grid_rows

        sorted_pdf = pdf.sort_values("event_index")
        sequences: dict[tuple[str, str], list[str]] = {}
        match_meta: dict[str, tuple[str, str, str]] = {}

        for rec in sorted_pdf.to_dict("records"):
            # to_dict("records") returns dict[Hashable, Any] per pandas-stubs;
            # at runtime the keys are always column names (str). Narrow via cast.
            rec_dict: dict[str, _Any] = cast("dict[str, _Any]", rec)
            x_val = rec_dict.get("start_x")
            y_val = rec_dict.get("start_y")
            if x_val is None or y_val is None:
                continue
            if isinstance(x_val, float) and _math.isnan(x_val):
                continue
            if isinstance(y_val, float) and _math.isnan(y_val):
                continue

            gx = min(int(x_val / cell_w), grid_cols - 1)
            gy = min(int(y_val / cell_h), grid_rows - 1)

            action = rec_dict.get("action_type", "non_action")
            token = f"{action}_{gx}_{gy}"

            key = (str(rec_dict["canonical_player_id"]), str(rec_dict["match_id"]))
            if key not in sequences:
                sequences[key] = []
            sequences[key].append(token)

            mid = str(rec_dict["match_id"])
            if mid not in match_meta:
                match_meta[mid] = (
                    str(rec_dict.get("data_source", "unknown")),
                    str(rec_dict.get("competition_id", "")),
                    str(rec_dict.get("season_id", "")),
                )

        if not sequences:
            return _pd.DataFrame(columns=_output_cols)

        # ---- Infer Doc2Vec vectors ----
        rows: list[dict[str, _Any]] = []
        for (player_id, match_id), tokens in sequences.items():
            vec = model.infer_vector(tokens, epochs=20)
            bvec_list = [float(v) for v in vec]
            meta = match_meta.get(match_id, ("unknown", "", ""))
            rows.append(
                {
                    "canonical_player_id": player_id,
                    "match_id": match_id,
                    "data_source": meta[0],
                    "behavioral_vector": _json.dumps(bvec_list),
                    "competition_id": meta[1],
                    "season_id": meta[2],
                }
            )

        return _pd.DataFrame(_pd.DataFrame(rows)[_output_cols])

    return _udf


# ---------------------------------------------------------------------------
# v1 pipeline
# ---------------------------------------------------------------------------


@workflow("wf-football2vec", phase="training")
def run_pipeline_v1(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
    """Compute v1 Doc2Vec player embeddings via applyInPandas.

    Decorated with ``wf-football2vec`` for independent cost/runtime tracking.
    Contains the full v1 inference path: incremental check, event loading,
    batch assignment, applyInPandas behavioral inference, stat vector merge,
    and Delta write.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    logger.info("Starting v1 Doc2Vec embedding pipeline for %s.%s", catalog, schema)

    # 0. Extract new match IDs from guard metadata if available
    new_match_ids = filter_result.metadata.get("new_match_ids")
    match_id_set: set[str] | None = set(new_match_ids) if new_match_ids else None

    if new_match_ids:
        logger.info("%d new matches to process — running full pipeline", len(new_match_ids))
    else:
        logger.info("Guard reported %d items — running full pipeline", filter_result.count)

    # 1. Load events as distributed Spark DataFrame (no .toPandas())
    events_sdf = _load_events_sdf(spark, catalog, schema, match_ids=match_id_set)

    # Quick emptiness check via limit(1) — avoids full DAG recomputation
    if events_sdf.limit(1).count() == 0:
        logger.warning("No events found — exiting")
        return

    logger.info("Events DataFrame loaded (distributed — no driver collection)")

    # 2. Assign flat batch_id for balanced distribution across executors.
    #    We partition by canonical_player_id so all events for a player end up
    #    in the same batch (required for per-player-match tokenization).
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import StringType, StructField, StructType

    # Get distinct player IDs and assign batch_id
    player_sdf = events_sdf.select("canonical_player_id").distinct()
    player_count = player_sdf.count()
    num_batches = max(1, player_count // _PLAYERS_PER_BATCH)

    player_batched = player_sdf.withColumn(
        "batch_id",
        (spark_fn.monotonically_increasing_id() % num_batches).cast("int"),
    )

    # Join batch_id back to events
    events_batched = events_sdf.join(player_batched, on="canonical_player_id", how="inner")

    logger.info(
        "Assigned %d players to %d batches (~%d players/batch)",
        player_count,
        num_batches,
        _PLAYERS_PER_BATCH,
    )

    # 3. Build UDF and run applyInPandas for behavioral vector inference
    model_path = f"/Volumes/{catalog}/dev_gold/model_weights/football2vec/player2vec.model"

    behavioral_schema = StructType(
        [
            StructField("canonical_player_id", StringType()),
            StructField("match_id", StringType()),
            StructField("data_source", StringType()),
            StructField("behavioral_vector", StringType()),  # JSON-encoded list[float]
            StructField("competition_id", StringType()),
            StructField("season_id", StringType()),
        ]
    )

    udf_fn = _make_behavioral_udf(model_path)
    behavioral_sdf = events_batched.groupBy("batch_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=behavioral_schema,
    )

    # 4. Collect behavioral results to driver for stat vector merging.
    #    Result size is O(players * matches_per_player) — typically ~90K rows,
    #    each with a short JSON string. Well within driver memory.
    behavioral_pdf = behavioral_sdf.toPandas()

    if behavioral_pdf.empty:
        logger.warning("No behavioral vectors produced — exiting")
        return

    # Deserialize behavioral vectors from JSON strings
    behavioral_pdf["behavioral_vector"] = behavioral_pdf["behavioral_vector"].apply(json.loads)

    # Build behavioral_vectors dict and metadata maps
    behavioral_vectors: dict[tuple[str, str], list[float]] = {}
    match_competition_map: dict[str, tuple[str, str]] = {}
    source_map: dict[str, str] = {}

    pids = behavioral_pdf["canonical_player_id"].astype(str)
    mids = behavioral_pdf["match_id"].astype(str)
    behavioral_vectors = {
        (pid, mid): cast(list[float], vec)
        for pid, mid, vec in zip(pids, mids, behavioral_pdf["behavioral_vector"], strict=True)
    }
    # First occurrence per match_id wins (drop_duplicates keeps first)
    # (OPT-AUDIT: replaced .iterrows() with dict(zip()) — vectorized)
    meta_dedup = behavioral_pdf.drop_duplicates(subset=["match_id"])
    _md_mids = meta_dedup["match_id"].astype(str)
    match_competition_map = dict(
        zip(
            _md_mids,
            zip(meta_dedup["competition_id"].astype(str), meta_dedup["season_id"].astype(str), strict=True),
            strict=True,
        )
    )
    source_map = dict(zip(_md_mids, meta_dedup["data_source"].astype(str), strict=True))

    logger.info("Inferred %d behavioral vectors via applyInPandas", len(behavioral_vectors))

    # 5. Compute stat vectors (filtered to players present in events)
    #    fct_player_stats is small (~20K rows) — driver-side is fine.
    event_player_ids: set[int] = set()
    for pid_str in behavioral_pdf["canonical_player_id"].unique():
        try:
            event_player_ids.add(int(pid_str))
        except (ValueError, TypeError):
            pass  # non-numeric IDs are skipped — stat filter remains broad
    stat_df, norm_params = _compute_stat_vectors(
        spark, catalog, DEFAULT_GOLD_SCHEMA, player_ids=event_player_ids or None
    )
    logger.info("Computed stat vectors for %d player-comp-season entries", len(stat_df))

    # Save normalization params alongside model artifacts
    if norm_params:
        _save_norm_params(catalog, norm_params, logger)

    # 6. Merge stat vectors to behavioral keys
    behavioral_keys = list(behavioral_vectors.keys())
    merged_stats = _merge_vectors(behavioral_keys, stat_df, match_competition_map)

    # 7. Build bronze DataFrame
    bronze_df = _build_bronze_dataframe(behavioral_vectors, merged_stats, source_map)
    logger.info("Built bronze DataFrame: %d rows", len(bronze_df))

    # 8. Write per data source with match-id-scoped replaceWhere for idempotency.
    # D45 fix 2026-04-15: previously replace_where was keyed on data_source
    # alone, which replaced the entire statsbomb/wyscout partition in
    # player_embeddings_raw — clobbering v2's 128d rows whenever v1 processed
    # even a single new match. Scoping the predicate to the specific match_ids
    # v1 just processed keeps v1 writes surgical and leaves v2 rows intact.
    for source in bronze_df["data_source"].unique():
        source_str = str(source)
        source_slice = bronze_df[bronze_df["data_source"] == source_str]

        if source_slice.empty:
            continue

        # Build a SQL-safe IN list from the match_ids actually being written.
        # match_id is stored as STRING in player_embeddings_raw (see _RESULTS_SCHEMA
        # at module top), so quote each value. Escape single quotes defensively
        # even though match_ids in practice are numeric-looking strings.
        source_match_ids: list[str] = sorted({str(m) for m in source_slice["match_id"]})
        escaped = [m.replace("'", "''") for m in source_match_ids]
        in_list = ", ".join(f"'{m}'" for m in escaped)
        escaped_source = source_str.replace("'", "''")
        predicate = f"data_source = '{escaped_source}' AND match_id IN ({in_list})"

        sdf = spark.createDataFrame(source_slice)
        row_count = validate_dataframe(
            sdf,
            ["canonical_player_id", "match_id", "data_source", "behavioral_vector"],
            _TABLE_NAME,
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=predicate,
            logger=logger,
            row_count=row_count,
        )

    logger.info("v1 Doc2Vec embedding pipeline complete")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main_v1() -> None:
    """CLI entry point for v1 Doc2Vec player embedding computation."""
    args = parse_ingestion_args("Compute v1 Doc2Vec player embeddings")
    logger = configure_logging("player_embeddings_v1")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline_v1(spark, args.catalog, args.schema, logger, filter_result=filter_result)
