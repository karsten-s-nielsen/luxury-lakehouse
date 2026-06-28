"""AC-1 — Unified action context pipeline.

Reads SPADL actions + tracking data from bronze, runs the full silly-kicks
enrichment chain in a single Spark UDF pass per match (mapInPandas streaming-group
dispatch, ADR-045), writes results to
bronze.spadl_action_context.

Providers: ALL (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, GradientSports).
Event-only providers get game_state + GK resolution; tracking providers get the full
104-col schema (``analytics.action_context.schema.RESULT_COLUMNS``).
Architecture: "Read from bronze, compute, write to bronze."
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections.abc import Callable, Iterator
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import numpy as np

from analytics.action_context.batching import resolve_frame_batch_size
from analytics.action_context.drain import WATCHDOG_BUDGET_S, assign_workers, drain_worker
from analytics.action_context.ghost_gk_backend import resolve_ghost_gk_backend
from analytics.action_context.pipeline import _reconstruct_xt
from analytics.action_context.schema import (
    ACTION_CONTEXT_DDL as _ACTION_CONTEXT_DDL,
)
from analytics.action_context.schema import (
    build_output as _build_output,
)
from analytics.action_context.work_unit import WorkUnit
from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

logger = logging.getLogger(__name__)


def _resolve_backend_or_exit(explicit: str | None, env_default: str | None) -> str:
    """Resolve the ghost-GK backend at the CLI boundary, translating the domain ``ValueError`` into the
    operator fail-loud ``SystemExit``. Keeps ``resolve_ghost_gk_backend`` pure (domain layer)."""
    try:
        return resolve_ghost_gk_backend(explicit, env_default)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

_TABLE_NAME = "spadl_action_context"

# ── Frame batching ────────────────────────────────────────────────────
# IDSSE (match_id, period) groups are 1.5M-1.7M rows -- exceeds the 1 GB
# Databricks serverless UDF group cap. Sub-batch by frame number. The size is
# PER-PROVIDER + run-overridable (ADR-047 amendment 2: the fixed 2500 OOMed
# idsse:J03WMX:1 in prod once the 4.22 column families landed), resolved via
# analytics.action_context.batching.resolve_frame_batch_size — the SAME module
# the local hexagon resolves through (H3 lockstep by shared import, enforced by
# test_pipeline_dispatch). The run-scoped override arrives as the
# ``frame_batch_size`` job parameter → drain worker ``--frame-batch-size`` →
# driver env AC_FRAME_BATCH_SIZE (resolve_frame_batch_size's env hook).

# ── UDF stage parallelism (ADR-045) ───────────────────────────────────
# groupBy().applyInPandas lets AQE coalesce the shuffle by BYTES (~64 MB advisory),
# blind to Python-UDF cost: a Metrica half (~286 groups at 250 frames, ~60 MB)
# coalesced to ONE task — strictly serial enrichment (measured concurrency 1.00 from
# rendezvous markers; GS's bigger rows got 3-4). repartition(N, keys) with an EXPLICIT
# N is exempt from AQE coalescing, so the mapInPandas dispatch below gets deterministic
# stage parallelism. At 2500-frame batches a half has ~30 groups → ~24 non-empty of the
# 64 partitions (hash collisions put 2 groups in a few) — still ≥2 waves over the
# observed 12-14 peak executor slots; empty partitions are near-free, so 64 stays
# (one variable changed per ADR-047, not two).
_UDF_SHUFFLE_PARTITIONS = 64

# Tolerance (seconds) for buffering actions at batch edges.
_ACTION_TIME_BUFFER_SECONDS = 0.5

# Metrica player ID jersey regex — compiled at module level per convention.
_JERSEY_RE = re.compile(r"Player\s*(\d+)")


# ── Provider classification ──────────────────────────────────────────

_TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
# Action-context is frames-required (ADR-057): valid AC providers are the 4 tracking providers
# plus statsbomb (resolved to the sb360 tier; only matches WITH freeze-frames are enqueued).
# wyscout (and statsbomb matches without 360) are out of scope — there is no event-only tier.
_ALL_PROVIDERS: frozenset[str] = _TRACKING_PROVIDERS | frozenset({"statsbomb"})


def _is_tracking_provider(provider: str) -> bool:
    return provider in _TRACKING_PROVIDERS


# ── DDL parser ────────────────────────────────────────────────────────


def _parse_ddl_to_struct_type(ddl: str) -> StructType:
    """Parse a Spark DDL column-list string into a StructType.

    Handles: STRING, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP.
    Excludes _ingested_at (added by write_delta_table, not by the UDF).
    """
    from pyspark.sql.types import (
        BooleanType,
        DataType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: dict[str, DataType] = {
        "STRING": StringType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "BOOLEAN": BooleanType(),
        "TIMESTAMP": TimestampType(),
    }
    fields: list[StructField] = []
    for token in ddl.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split()
        if len(parts) != 2:
            continue
        col_name, col_type = parts[0], parts[1].upper()
        if col_name == "_ingested_at":
            continue
        spark_type = type_map.get(col_type)
        if spark_type is None:
            msg = f"Unknown Spark type {col_type!r} for column {col_name!r}"
            raise ValueError(msg)
        fields.append(StructField(col_name, spark_type, nullable=True))
    return StructType(fields)


_RESULT_SCHEMA_CACHE: StructType | None = None


def _get_result_schema() -> StructType:
    """Lazy accessor for the applyInPandas StructType schema."""
    global _RESULT_SCHEMA_CACHE
    if _RESULT_SCHEMA_CACHE is None:
        _RESULT_SCHEMA_CACHE = _parse_ddl_to_struct_type(_ACTION_CONTEXT_DDL)
    return _RESULT_SCHEMA_CACHE


# ── xT serialization ─────────────────────────────────────────────────
# Re-uses helpers from tracking_context. Import at call site to avoid
# circular imports at module load time.


def _load_xt_grid_from_delta(
    spark: SparkSession,
    catalog: str,
    schema: str,
    task_logger: logging.Logger,
) -> tuple[list[list[float]], int, int]:
    """Load the pre-computed global xT grid from bronze.expected_threat_grids.

    The grid is written by the ``compute_expected_threat`` pipeline (runs daily).
    It's a tiny table (~192 rows for a 16x12 grid) -- reading it is instant.

    Returns:
        (xt_grid_data, xt_l, xt_w) -- the 2D grid as nested lists, plus dimensions.

    Raises:
        RuntimeError: If the global grid does not exist (bootstrap case --
            ``compute_expected_threat`` must run first).
    """
    table = f"{catalog}.{schema}.expected_threat_grids"
    rows = list(
        spark.sql(
            f"SELECT zone_x, zone_y, xt_value FROM {table} "  # noqa: S608
            f"WHERE competition_id = 'global'"
        ).collect()
    )
    if not rows:
        msg = f"No global xT grid found in {table}. Run compute_expected_threat before compute_action_context."
        raise RuntimeError(msg)

    n_x = max(int(r.zone_x) for r in rows) + 1
    n_y = max(int(r.zone_y) for r in rows) + 1
    grid = np.zeros((n_y, n_x))
    for row in rows:
        grid[int(row.zone_y), int(row.zone_x)] = float(row.xt_value)

    task_logger.info("Loaded global xT grid from Delta (%dx%d, %d cells)", n_x, n_y, len(rows))
    return grid.tolist(), n_x, n_y


# ── Column projection constants ──────────────────────────────────────
# GradientSports is new to AC-1 (not in tracking_context.py).
# IDSSE/Metrica/SkillCorner column lists imported from tracking_context
# inside the UDF (lazy import to avoid import-time pyspark dependency).

_GRADIENTSPORTS_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "period",
    "frame_num",
    "period_elapsed_time",
    "team_side",
    "is_ball",
    "jersey_num",
    "x",
    "y",
    "z",
)


# ── GradientSports bronze -> converter input mapper ───────────────────

_GS_FRAME_RATE = 30  # GradientSports default frame rate


def _bronze_gradientsports_to_converter_input(
    trk_pdf: pd.DataFrame,
    *,
    team_side_to_id: dict[str, str],
    jersey_to_player_id: dict[tuple[str, str], str],
    gk_player_ids: frozenset[str],
) -> pd.DataFrame:
    """Map bronze ``gradientsports_tracking`` columns to silly-kicks converter input.

    Args:
        trk_pdf: Bronze tracking rows (columns per _GRADIENTSPORTS_TRACKING_SELECT_COLS).
        team_side_to_id: Maps team_side ("home"/"away") -> native team_id string.
        jersey_to_player_id: Maps (team_side, jersey_num) -> native player_id string.
        gk_player_ids: Set of player_id strings who are goalkeepers.

    Returns:
        DataFrame with columns matching silly_kicks.tracking.gradientsports.EXPECTED_INPUT_COLUMNS.
    """
    import pandas as _pd

    result = _pd.DataFrame()
    result["game_id"] = trk_pdf["match_id"]
    result["period_id"] = trk_pdf["period"].astype("Int64")
    result["frame_id"] = trk_pdf["frame_num"].astype("Int64")
    result["time_seconds"] = trk_pdf["period_elapsed_time"].astype("float64")
    result["frame_rate"] = _GS_FRAME_RATE
    result["is_ball"] = trk_pdf["is_ball"].fillna(False)
    result["x_centered"] = trk_pdf["x"].astype("float64")
    result["y_centered"] = trk_pdf["y"].astype("float64")
    result["z"] = trk_pdf["z"].astype("float64")
    result["speed_native"] = np.nan  # Derived by converter/post-processing
    result["ball_state"] = "alive"  # GS does not provide per-frame ball state

    # Map team_side -> team_id; ball rows get NaN team_id
    result["team_id"] = trk_pdf["team_side"].map(team_side_to_id)

    # Map (team_side, jersey_num) -> player_id; ball rows get NaN
    _side = trk_pdf["team_side"].fillna("")
    _jersey = trk_pdf["jersey_num"].fillna("")
    result["player_id"] = [jersey_to_player_id.get((s, j)) for s, j in zip(_side, _jersey, strict=False)]

    # is_goalkeeper from roster
    result["is_goalkeeper"] = result["player_id"].isin(gk_player_ids)
    # Ball rows: explicit False for is_goalkeeper
    result.loc[result["is_ball"] == True, "is_goalkeeper"] = False  # noqa: E712

    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


# ── UDF factory ───────────────────────────────────────────────────────


def _make_streaming_group_mapper(
    udf_fn: Callable[[pd.DataFrame], pd.DataFrame],
    key_cols: list[str],
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """Adapt a per-group pandas UDF to a ``mapInPandas`` iterator over key-sorted partitions.

    ADR-045: the ``groupBy().applyInPandas`` shuffle is subject to AQE bytes-based
    coalescing, which packed a Metrica half's ~286 Python-heavy groups into ONE task
    (measured concurrency 1.00). The replacement dispatch is
    ``repartition(_UDF_SHUFFLE_PARTITIONS, *keys).sortWithinPartitions(*keys)
    .mapInPandas(this_mapper, schema)`` — the explicit partition count is exempt from
    AQE coalescing, so stage parallelism is deterministic.

    ``sortWithinPartitions`` guarantees each group's rows are CONTIGUOUS within the
    partition, but Arrow chunking (``spark.sql.execution.arrow.maxRecordsPerBatch``,
    default 10k rows) may split a group across consecutive chunks. The mapper streams
    chunks, emits every complete group through ``udf_fn``, and carries the
    possibly-incomplete tail group into the next chunk; the final carry flushes at
    iterator exhaustion. Each ``udf_fn`` invocation receives exactly the rows
    ``applyInPandas`` would have passed for that group (index reset, all columns).
    """

    def _mapper(chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import pandas as _pd

        carry: pd.DataFrame | None = None
        for chunk in chunks:
            if carry is not None and len(carry):
                chunk = _pd.concat([carry, chunk], ignore_index=True)
            carry = None
            if not len(chunk):
                continue
            # Groups are contiguous (sortWithinPartitions); only the LAST group of the
            # chunk may continue into the next chunk — hold it back as carry.
            gids = chunk.groupby(key_cols, sort=False).ngroup().to_numpy()
            tail_mask = gids == gids[-1]
            carry = chunk[tail_mask].copy()
            head = chunk[~tail_mask]
            if len(head):
                for _, group in head.groupby(key_cols, sort=False):
                    out = udf_fn(group.reset_index(drop=True))
                    if out is not None and len(out):
                        yield out
        if carry is not None and len(carry):
            out = udf_fn(carry.reset_index(drop=True))
            if out is not None and len(out):
                yield out

    return _mapper


def _make_action_context_udf(
    provider: str,
    home_team_id: str,
    home_start_left: bool,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    actions_records: list[dict[str, Any]],
    native_match_id: str,
    *,
    home_team_start_left_extratime: bool | None = None,
    gs_team_side_to_id: dict[str, str] | None = None,
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None,
    gs_gk_player_ids: list[str] | None = None,
    exec_rendezvous_dir: str | None = None,
    kde_backend: str = "fft-cic",
    frame_batch_size: int | None = None,
    ownership_anchors: dict[int, tuple[float, float, float]] | None = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the per-group pandas UDF closure for action context enrichment
    (dispatched via ``_make_streaming_group_mapper`` + ``mapInPandas``, ADR-045).

    All arguments are Python scalar primitives or small serializable structures.
    GradientSports-specific args (gs_*) are only needed for that provider.
    ``exec_rendezvous_dir`` is a pre-created UC Volume dir for executor progress
    markers (see ingestion.exec_visibility); ``None`` disables markers.
    ``frame_batch_size`` MUST be the size the driver used to assign
    ``frame_batch_id`` (H3) — the resolved int travels in the closure because the
    executor cannot see the driver's AC_FRAME_BATCH_SIZE env override.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        # FIRST: point NUMBA_CACHE_DIR at a writable temp dir BEFORE any
        # silly_kicks import below. silly-kicks' @njit(cache=True) kernels raise
        # "no locator available" at import on serverless' read-only ephemeral
        # wheel path unless numba has a writable cache dir. Executors never run
        # bootstrap_hooks, so the UDF must set it itself. See ensure_numba_cache_dir.
        from ingestion.exec_visibility import ensure_numba_cache_dir as _ensure_numba_cache_dir

        _ensure_numba_cache_dir()

        import logging as _logging
        import time as _time
        import traceback as _tb

        import pandas as _pd

        from analytics.action_context.pipeline import enrich_batch as _enrich_batch
        from analytics.action_context.schema import RESULT_COLUMNS as _RC
        from analytics.action_context.work_unit import MatchMeta as _MatchMeta
        from ingestion.exec_visibility import (
            assert_executor_silly_kicks_sane,
            disarm_executor_faulthandler,
            executor_env_fingerprint,
            executor_marker,
            install_executor_faulthandler,
        )

        _logger = _logging.getLogger("action_context_udf")

        if pdf.empty:
            output_cols = [c for c in _RC if c != "_ingested_at"]
            return _pd.DataFrame(columns=_pd.Index(output_cols))

        match_id_val = pdf["match_id"].iloc[0]
        period_val = pdf["period"].iloc[0]
        batch_id_val = pdf["frame_batch_id"].iloc[0] if "frame_batch_id" in pdf.columns else None

        # Executor visibility: if THIS group hangs >120s, faulthandler dumps all
        # thread stacks to executor stderr (the only place to see a silent UDF
        # hang's stuck frame on serverless). See ingestion.exec_visibility.
        _batch_key = f"{match_id_val}_p{period_val}_b{batch_id_val}"
        # If THIS group hangs >90s, faulthandler dumps every thread's stack to
        # executor stderr (read via Spark UI thread dump) — the only way to see
        # WHERE a silent serverless applyInPandas hang is stuck.
        install_executor_faulthandler(timeout_s=90.0, repeat=True)
        # One-shot executor-environment fingerprint (numba threading layer + fork
        # mode + versions + internet reachability) — tests the leading hang
        # hypotheses the instant the worker starts; echoed to the task log by the
        # driver heartbeat. See ingestion.exec_visibility.executor_env_fingerprint.
        executor_env_fingerprint(exec_rendezvous_dir, seq=f"{_batch_key}_envfp")
        # Executor env-drift guard (ADR-044): fail loud NOW if this serverless UDF sandbox
        # resolved a stale/split silly-kicks (the fingerprint above logs __version__, which a
        # split install fools into reporting the healthy 4.20.1 while submodules run 4.12.0 —
        # the 2026-06-09 GS dual-GK ghost-GK crash). Process-local one-shot; see
        # exec_visibility.assert_executor_silly_kicks_sane.
        assert_executor_silly_kicks_sane(batch_key=_batch_key)
        # Per-batch start marker (cheap progress signal + executor-write probe).
        executor_marker(
            exec_rendezvous_dir,
            seq=f"{_batch_key}_start",
            payload=f"start match={match_id_val} period={period_val} batch={batch_id_val} t={_time.time():.3f}",
        )

        if len(pdf) > 2_000_000:
            _logger.warning(
                "Large UDF group: match_id=%s, period=%s, rows=%d (>2M)",
                match_id_val,
                period_val,
                len(pdf),
            )

        _meta = _MatchMeta(
            home_team_id=home_team_id,
            home_start_left=home_start_left,
            home_team_start_left_extratime=home_team_start_left_extratime,
            gs_team_side_to_id=gs_team_side_to_id,
            gs_jersey_to_player_id=gs_jersey_to_player_id,
            gs_gk_player_ids=gs_gk_player_ids,
        )

        _batch_start = _time.monotonic()
        try:
            # One Spark frame_batch_id group == one enrich_batch call (== one
            # iteration of run_work_unit's loop). prod and local run identical code (H3).
            _result = _enrich_batch(
                provider=provider,
                tier="tracking",
                frames_pdf=pdf,
                actions_records=actions_records,
                period=int(period_val),
                xt_grid_data=xt_grid_data,
                xt_l=xt_l,
                xt_w=xt_w,
                meta=_meta,
                native_match_id=native_match_id,
                kde_backend=kde_backend,
                frame_batch_size=frame_batch_size,
                ownership_anchors=ownership_anchors,
            )
            # Executor-side per-batch progress log: serverless / Spark Connect
            # forbids driver-side accumulators (PySparkAttributeError on
            # spark.sparkContext), so we emit one terse line per successful
            # batch from inside the UDF closure. Operator reads these in the
            # Databricks driver log stream — same per-batch visibility as the
            # original driver-side heartbeat design without the Connect violation.
            # See ADR-031.
            _logger.info(
                "AC1_BATCH provider=%s match_id=%s batch_id=%s elapsed_s=%.1f",
                provider,
                match_id_val,
                batch_id_val,
                _time.monotonic() - _batch_start,
            )
            disarm_executor_faulthandler()
            executor_marker(
                exec_rendezvous_dir,
                seq=f"{match_id_val}_p{period_val}_b{batch_id_val}_done",
                payload=(
                    f"done match={match_id_val} period={period_val} batch={batch_id_val} "
                    f"rows={len(_result)} elapsed_s={_time.monotonic() - _batch_start:.1f}"
                ),
            )
            return _result
        except Exception as exc:  # ADR-002 §5 hard-fail-first UDF: re-raise with group key context
            inner_tb = _tb.format_exc()
            _logger.error(
                "UDF failed for match_id=%s, period=%s, batch=%s:\n%s",
                match_id_val,
                period_val,
                batch_id_val,
                inner_tb,
            )
            # Surface the executor exception to the DRIVER task log via a marker
            # (executor stderr / _logger.error never reach it on Spark Connect).
            disarm_executor_faulthandler()
            executor_marker(
                exec_rendezvous_dir,
                seq=f"{_batch_key}_ERROR",
                payload=f"ERROR match={match_id_val} period={period_val} batch={batch_id_val}\n{inner_tb}",
            )
            raise RuntimeError(
                f"action_context UDF failed for match_id={match_id_val}, "
                f"period={period_val}, frame_batch_id={batch_id_val}:\n{inner_tb}"
            ) from exc

    return _udf


# ── Guard ─────────────────────────────────────────────────────────────


def _find_tracking_new_period_pairs(
    spark: SparkSession,
    tracking_table: str,
    spadl_table: str,
    results_table: str,
    provider: str,
) -> list[tuple[str, int]]:
    """Find unprocessed ``(match_id, period)`` pairs for a tracking provider (Spark-native).

    Per-period generalisation of the whole-match discovery — all four tracking bronze tables carry a
    ``period`` column, so every tracking provider now enqueues per-half units like IDSSE (ADR-037
    amendment). Three-way join at period granularity:
      tracking(mid, period) ∩ spadl(mid) \\ results(mid, period)
    """
    from pyspark.sql import functions as F  # noqa: N812

    tracking_df = (
        spark.table(tracking_table)
        .select(
            F.col("match_id").cast("string").alias("_mid"),
            F.col("period").cast("bigint").alias("_period"),
        )
        .distinct()
    )
    spadl_df = (
        spark.table(spadl_table)
        .filter(F.col("data_source") == provider)
        .select(F.col("match_id_native").cast("string").alias("_mid"))
        .distinct()
    )
    results_df = (
        spark.table(results_table)
        .filter(F.col("data_source") == provider)
        .select(
            F.col("match_id").cast("string").alias("_mid"),
            F.col("period_id").cast("bigint").alias("_period"),
        )
        .distinct()
    )
    new_df = tracking_df.join(spadl_df, "_mid", "inner").join(results_df, ["_mid", "_period"], "left_anti")
    return [(str(row["_mid"]), int(row["_period"])) for row in new_df.collect()]


def _find_sb360_new_ids(
    spark: SparkSession,
    spadl_table: str,
    results_table: str,
    sb360_table: str,
) -> list[str]:
    """Find unprocessed StatsBomb matches that HAVE freeze-frames (frames-required; ADR-057).

    statsbomb spadl matches ∩ statsbomb_360 match_ids \\ results — Spark-native, no
    driver-side set difference. Event-only statsbomb matches (no 360) are out of
    action-context scope and never enqueued.

    The join key is CANONICALIZED identically on all three sides: ``cast(long->string)``
    normalizes the ``"366.0"`` vs ``"366"`` float-format mismatch class (ADR-019 / the
    canonical_id GK-feature incident). StatsBomb match ids are integers, so the long cast
    is exact; a real-dtype set-equality probe (``test_sb360_discovery_id_join_is_dtype_safe``)
    backstops this. A pre-recompute live ``DESCRIBE`` of both columns is on the ADR-057
    operational checklist.
    """
    from pyspark.sql import functions as F  # noqa: N812

    def _key(col: str):  # type: ignore[no-untyped-def]  # Spark Column; pyspark is runtime-only
        return F.col(col).cast("long").cast("string").alias("_join_id")

    source_df = (
        spark.table(spadl_table).filter(F.col("data_source") == "statsbomb").select(_key("match_id_native")).distinct()
    )
    have_360_df = spark.table(sb360_table).select(_key("match_id")).distinct()
    results_df = (
        spark.table(results_table).filter(F.col("data_source") == "statsbomb").select(_key("match_id")).distinct()
    )
    new_df = source_df.join(have_360_df, "_join_id", "inner").join(results_df, "_join_id", "left_anti")
    return [str(row["_join_id"]) for row in new_df.collect()]


def _find_idsse_new_period_pairs(
    spark: SparkSession,
    tracking_table: str,
    spadl_table: str,
    results_table: str,
) -> list[tuple[str, int]]:
    """Find unprocessed IDSSE (match_id, period) pairs (Spark-native).

    Three-way join at period granularity:
      tracking(mid, period) ∩ spadl(mid) \\ results(mid, period)
    """
    from pyspark.sql import functions as F  # noqa: N812

    tracking_df = (
        spark.table(tracking_table)
        .select(
            F.col("match_id").cast("string").alias("_mid"),
            F.col("period").cast("bigint").alias("_period"),
        )
        .distinct()
    )
    spadl_df = (
        spark.table(spadl_table)
        .filter(F.col("data_source") == "idsse")
        .select(F.col("match_id_native").cast("string").alias("_mid"))
        .distinct()
    )
    results_df = (
        spark.table(results_table)
        .filter(F.col("data_source") == "idsse")
        .select(
            F.col("match_id").cast("string").alias("_mid"),
            F.col("period_id").cast("bigint").alias("_period"),
        )
        .distinct()
    )
    new_df = tracking_df.join(spadl_df, "_mid", "inner").join(results_df, ["_mid", "_period"], "left_anti")
    return [(str(row["_mid"]), int(row["_period"])) for row in new_df.collect()]


class _ActionContextGuard:
    """SkipGuard adapter for action context pipeline.

    Discovers unprocessed matches across all 6 providers using Spark-native
    joins. Each query is pushed entirely to executors — no full-table scans
    or driver-side set differences.

    Fan-out (ADR-037, worker-drain): preflight discovers UNITS (a match, or a
    ``(match, period)`` half for IDSSE — the 1 GB applyInPandas memory cap), LPT-
    bin-packs them across ``_N_DRAIN_WORKERS`` persistent for-each workers into the
    ``observability.action_context_work_queue``, and emits a constant worker-id task
    value. There is no per-iteration ``chunk_size`` any more — the per-game watchdog
    + persistent worker removed the per-iteration budget chunk_size packed against.

    MEMORY NOTE — per-unit isolation (why a worker can drain many units in one
    persistent driver): each unit runs a SEPARATE ``applyInPandas`` pass and frees
    memory between units (``_process_*`` loops one match at a time). Per-group memory
    is bounded by the per-provider frame batch size (ADR-047 amendment 2: 250 for the
    dense 25 fps providers after the 2500 IDSSE OOM, 2500 where prod-proven — see
    ``analytics.action_context.batching``); concurrent executor
    pressure is bounded by the for_each ``concurrency`` == ``_N_DRAIN_WORKERS``.
    """

    workflow_id = "wf-action-context"
    # Persistent drain workers = the for-each width (ADR-037). Single source of truth:
    # preflight emits this many worker-id task-value entries and the Terraform for-each
    # concurrency is pinned to it (test_terraform_concurrency_matches_n_workers).
    _N_DRAIN_WORKERS = 8

    def __init__(self, *, provider_filter: str | None = None, max_units: int | None = None) -> None:
        """Optional ad-hoc scoping for a one-off preflight run.

        ``provider_filter`` — restrict discovery to a single provider (``None`` =
        all). ``max_units`` — cap each provider's discovered units to ``<=N``
        (``None`` = no cap). A "unit" is whatever the anti-join emits: a match for
        the event-only / non-IDSSE tracking providers, a ``(match, period)`` half
        for IDSSE. Both default to ``None`` so the daily scheduled preflight (and
        the module-level ``skip_guard`` singleton) behave exactly as before.
        """
        self.provider_filter = provider_filter
        self.max_units = max_units
        self._units_cache: list[WorkUnit] | None = None  # set by discover_units()
        self._units_cache_key: tuple[str, str] | None = None  # (catalog, schema) of the cache (R1)

    def _selected(self, provider: str) -> bool:
        """Whether this provider's discovery query should run at all."""
        return self.provider_filter is None or provider == self.provider_filter

    def _cap(self, units: list[Any]) -> list[Any]:
        """Deterministically cap a provider's discovered units to ``max_units``.

        Sorted before truncation so "next N" is stable and walks forward across
        triggers — the anti-join already excludes processed units, so a re-run
        picks up where the last left off. No-op AND order-preserving when
        ``max_units is None`` (the daily path is byte-for-byte unchanged).
        """
        if self.max_units is None:
            return units
        return sorted(units)[: self.max_units]

    def discover_units(self, spark: SparkSession, catalog: str, schema: str) -> list[WorkUnit]:
        """Discover unprocessed action-context units across all 6 providers.

        A unit is a match (most providers) or a ``(match, period)`` half (IDSSE).
        Honors ``provider_filter`` + ``max_units`` (per-provider cap), same as ``check``.

        Memoised on ``(catalog, schema)`` (P1/R1): the 6 anti-joins are expensive, so
        ``check()`` (skip-guard count) and the preflight body (units) share ONE
        discovery. Keying on the target makes the cache safe BY CONSTRUCTION even on
        the long-lived module-level ``skip_guard`` singleton — a different target
        self-invalidates, so it can never serve stale discovery across runs.
        """
        if self._units_cache is not None and self._units_cache_key == (catalog, schema):
            return self._units_cache

        from ingestion.guards import ensure_table

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        spadl_table = f"{catalog}.bronze.spadl_actions"
        ensure_table(spark, results_table, _ACTION_CONTEXT_DDL)

        units: list[WorkUnit] = []
        if self._selected("idsse"):
            pairs = self._cap(
                _find_idsse_new_period_pairs(spark, f"{catalog}.bronze.idsse_tracking", spadl_table, results_table)
            )
            units += [WorkUnit(provider="idsse", match_id=mid, period=period) for mid, period in pairs]

        for prov, table in (
            ("metrica", "metrica_tracking"),
            ("skillcorner", "skillcorner_tracking"),
            ("gradientsports", "gradientsports_tracking"),
        ):
            if self._selected(prov):
                pairs = self._cap(
                    _find_tracking_new_period_pairs(
                        spark, f"{catalog}.bronze.{table}", spadl_table, results_table, prov
                    )
                )
                units += [WorkUnit(provider=prov, match_id=mid, period=period) for mid, period in pairs]

        # StatsBomb (ADR-058): sb360 EXITS the per-match drain — it is processed as ONE distributed
        # cogroup.applyInPandas job by ``main_statsbomb`` (_process_statsbomb_matches), which scans each
        # bronze table once and writes distributed (no driver toPandas, no 8-worker per-match commit
        # contention). It is therefore NOT enqueued as drain units here. wyscout / statsbomb-without-360
        # remain out of action-context scope (frames-required, ADR-057).

        self._units_cache = units
        self._units_cache_key = (catalog, schema)
        return units

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip-guard hook: count of unprocessed units (0 => skip).

        Returns only the generic count; ``FilterResult.chunks`` (the shared fan-out
        field used by other guards) is intentionally NOT populated for AC-1 — the
        worker-drain fan-out reads structured units via ``discover_units`` instead.
        """
        units = self.discover_units(spark, catalog, schema)
        return FilterResult(workflow_id=self.workflow_id, count=len(units))


skip_guard = _ActionContextGuard()


# ── CLI arg parser ────────────────────────────────────────────────────


def _parse_action_match_ids_arg(raw: str | None) -> tuple[str, list[str], int | None] | None:
    """Parse ``--match-ids`` CLI value.

    Formats:
        ``"provider:id1,id2"`` — multiple matches, no period filter.
        ``"provider:id:period"`` — single match + period (IDSSE half-game chunks).
    """
    if raw is None or raw == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2' or 'provider:id:period', got {raw!r}. "
            f"Valid providers: {sorted(_ALL_PROVIDERS)}"
        )
    parts = raw.split(":")
    provider = parts[0]
    if provider not in _ALL_PROVIDERS:
        raise SystemExit(f"Unknown provider {provider!r}. Valid: {sorted(_ALL_PROVIDERS)}")

    # Detect "provider:match_id:period" format (3 parts, last is numeric)
    if len(parts) == 3 and parts[2].strip().isdigit():
        match_id = parts[1].strip()
        period = int(parts[2].strip())
        if not match_id:
            return None
        return (provider, [match_id], period)

    # Standard "provider:id1,id2" format
    id_str = ":".join(parts[1:])
    ids = [i.strip() for i in id_str.split(",") if i.strip()]
    if not ids:
        return None
    return (provider, ids, None)


def _parse_preflight_filters(provider: str | None, max_units: str | None) -> tuple[str | None, int | None]:
    """Validate + coerce the optional ``--provider`` / ``--max-units`` preflight args.

    Both arrive as strings: the daily job passes empty job-parameter values, so
    ``""`` (and whitespace) must coerce to ``None`` = no filter / no cap, leaving
    the scheduled run unchanged. Returns ``(provider_filter, max_units)`` with
    ``None`` for "unset". Raises ``SystemExit`` on an unknown provider or a
    non-positive / non-integer cap.
    """
    provider_filter = provider.strip() if provider and provider.strip() else None
    if provider_filter is not None and provider_filter not in _ALL_PROVIDERS:
        raise SystemExit(f"Unknown --provider {provider_filter!r}. Valid: {sorted(_ALL_PROVIDERS)}")

    raw = max_units.strip() if max_units and max_units.strip() else None
    if raw is None:
        return provider_filter, None
    try:
        capped = int(raw)
    except ValueError:
        raise SystemExit(f"--max-units must be a positive integer, got {max_units!r}") from None
    if capped <= 0:
        raise SystemExit(f"--max-units must be > 0, got {capped}")
    return provider_filter, capped


def _resolve_run_id(args: argparse.Namespace) -> str:
    """The job-level run id, passed as ``--run-id`` (from ``{{job.run_id}}``).

    Never read from the worker's env (``DATABRICKS_RUN_ID`` is per-task / has
    ``"unknown"`` fallbacks; see ADR-037 B1). Falls back to a timestamp only for a
    standalone/manual invocation with no ``--run-id``.
    """
    raw = getattr(args, "run_id", None)
    if raw and str(raw).strip():
        return str(raw).strip()
    import time

    return f"local-{int(time.time())}"


def _set_task_value(key: str, value: object, task_logger: logging.Logger) -> None:
    """Write a Databricks task value (no-op + warn in standalone mode)."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(get_spark_session())
        dbutils.jobs.taskValues.set(key=key, value=value)
        task_logger.info("Wrote task value %r", key)
    except (ImportError, AttributeError, RuntimeError) as exc:
        task_logger.warning("Task values not available (standalone mode) -- %s", exc)


# ── Entry points ──────────────────────────────────────────────────────


def _force_full_rematerialize_on_grid_change(spark: SparkSession, catalog: str, schema: str, logger: Any) -> None:
    """ADR-063 R5b/H1: on a material global-xT-grid change, delete the tracking-provider
    action-context rows so the preflight re-discovers every match and the drain recomputes its
    ``xt_gk_*`` on the new surface. The grid bumps its version only on a MATERIAL change (ADR-063 R4),
    so this fires rarely. The watermark is recorded here (preflight); if the drain is incomplete, the
    normal per-unit anti-join self-heals the remainder on subsequent runs.
    """
    from ingestion.guards import check_upstream_freshness, record_watermarks

    grid = [f"{catalog}.bronze.expected_threat_grids"]
    if check_upstream_freshness(spark, catalog, "wf-action-context", grid).count <= 0:
        return
    providers = ", ".join(f"'{p}'" for p in ("idsse", "metrica", "skillcorner", "gradientsports"))
    logger.info("Action context: global xT grid changed (ADR-063) — deleting tracking AC rows for full re-materialize")
    spark.sql(f"DELETE FROM {catalog}.{schema}.{_TABLE_NAME} WHERE data_source IN ({providers})")  # noqa: S608
    record_watermarks(spark, catalog, "wf-action-context", grid)


def main_preflight() -> None:
    """CLI entry point for action context preflight (ADR-037 worker-drain).

    Discovers unprocessed UNITS once (memoised), LPT-bin-packs them across
    ``_N_DRAIN_WORKERS`` into ``observability.action_context_work_queue``, and emits
    the constant worker-id list + the run_id as task values for the for-each.

    xT grid loading is NOT done here -- each drain worker loads it once at startup.
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed action context units and fill the work-queue",
        extra_args=[
            (
                "--provider",
                {
                    "type": str,
                    "default": None,
                    "help": "Restrict discovery to one provider (default/empty: all). "
                    f"One of {sorted(_ALL_PROVIDERS)}.",
                },
            ),
            (
                "--max-units",
                {
                    "type": str,
                    "default": None,
                    "help": "Cap discovered units to <=N per provider (default/empty: no cap). "
                    "A unit is a match (most providers) or a (match, period) half (IDSSE).",
                },
            ),
            (
                "--run-id",
                {
                    "type": str,
                    "default": None,
                    "help": "Job-level run id (from {{job.run_id}}); shared with the drain workers.",
                },
            ),
            (
                "--ghost-gk-backend",
                {
                    "type": str,
                    "default": None,
                    "help": "Ghost-GK KDE backend; empty resolves to AC1_GHOST_GK_BACKEND env then fft-cic. "
                    "One of {scipy,vectorized,cpu-numba,fft,fft-cic}.",
                },
            ),
        ],
    )
    task_logger = configure_logging("action_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    provider_filter, max_units = _parse_preflight_filters(
        getattr(args, "provider", None), getattr(args, "max_units", None)
    )
    if provider_filter is not None or max_units is not None:
        task_logger.info(
            "Action context preflight SCOPED: provider_filter=%s max_units=%s",
            provider_filter,
            max_units,
        )
    else:
        # ADR-063 R5b/H1 (daily/full path only): on a MATERIAL global-xT-grid change, force a full
        # re-materialize of the tracking providers' action-context (their xt_gk_* are grid-derived).
        _force_full_rematerialize_on_grid_change(spark, args.catalog, args.schema, task_logger)
    guard = _ActionContextGuard(provider_filter=provider_filter, max_units=max_units)
    fr = timed_check(guard, spark, args.catalog, args.schema)  # telemetry (count + skip); discovery memoised
    if fr.count == 0:
        task_logger.info("Action context preflight: nothing to do")
        _set_task_value("action_context_run_id", "", task_logger)
        _set_task_value("action_context_worker_ids", [], task_logger)
        return

    # Spark adapter imported function-locally: it pulls pyspark, and action_context.py must stay
    # importable offline. Tests patch it at its source (ingestion.action_context_queue.DeltaWorkQueue).
    import os

    from ingestion.action_context_queue import DeltaWorkQueue

    kde_backend = _resolve_backend_or_exit(
        getattr(args, "ghost_gk_backend", None), os.environ.get("AC1_GHOST_GK_BACKEND")
    )
    # discover_units is memoised + shared with the skip-guard count path (which is backend-agnostic),
    # so stamp the resolved backend onto every unit here rather than inside discovery (domain policy on
    # the work spec; the queue carries it to the drain workers).
    units = [replace(u, kde_backend=kde_backend) for u in guard.discover_units(spark, args.catalog, args.schema)]
    if kde_backend != "fft-cic":
        task_logger.info("Action context preflight: ghost-GK backend = %s (non-default)", kde_backend)
    assignments = assign_workers(units, _ActionContextGuard._N_DRAIN_WORKERS)
    run_id = _resolve_run_id(args)
    queue = DeltaWorkQueue(spark, args.catalog)
    queue.ensure_table()
    # Self-prune stale per-run scratch rows before enqueueing this run's batch, so the queue
    # does not grow unbounded across daily runs (ADR-037 fan-out leaves one batch per run_id).
    pruned = queue.prune()
    if pruned:
        task_logger.info("Action context preflight: pruned %d stale work-queue rows (retention)", pruned)
    queue.enqueue(run_id, assignments)

    worker_ids = [str(i) for i in range(_ActionContextGuard._N_DRAIN_WORKERS)]
    _set_task_value("action_context_run_id", run_id, task_logger)
    _set_task_value("action_context_worker_ids", worker_ids, task_logger)
    task_logger.info(
        "Action context preflight: %d units across %d workers (run_id=%s)",
        len(units),
        len(worker_ids),
        run_id,
    )


def _iteration_fingerprint(
    *,
    provider: str,
    ids: list[str],
    period_filter: int | None,
    catalog: str,
    schema: str,
) -> dict[str, object]:
    """Build a single-line structured fingerprint of this for-each iteration.

    Emitted at iteration start so ops can grep central log aggregation for
    "what was this iteration doing" without opening the Databricks UI. Captures
    the load-bearing inputs (provider, match-ids count, period filter) plus the
    environment fingerprint (silly-kicks version, wheel version, Databricks run
    context). The hash is deterministic over the inputs so two reruns with
    identical inputs share the same fingerprint_hash (useful for diffing).
    """
    import hashlib
    import json as _json
    import os as _os

    import silly_kicks

    sk_version = getattr(silly_kicks, "__version__", "unknown")
    try:
        from shared.wheel import WHEEL_VERSION
    except (ImportError, AttributeError):
        WHEEL_VERSION = "unknown"  # noqa: N806

    input_blob = _json.dumps({"provider": provider, "ids": sorted(ids), "period": period_filter}, sort_keys=True)
    fp_hash = hashlib.sha256(input_blob.encode("utf-8")).hexdigest()[:12]

    return {
        "event": "ac1_iteration_start",
        "fingerprint_hash": fp_hash,
        "provider": provider,
        "n_match_ids": len(ids),
        "match_ids_sample": ids[:3] if len(ids) > 3 else ids,
        "period_filter": period_filter,
        "catalog": catalog,
        "schema": schema,
        "silly_kicks_version": sk_version,
        "wheel_version": WHEEL_VERSION,
        "databricks_run_id": _os.environ.get("DATABRICKS_RUN_ID", "unknown"),
        "databricks_task_run_id": _os.environ.get("DATABRICKS_TASK_RUN_ID", "unknown"),
    }


def _iteration_summary(
    *,
    provider: str,
    fingerprint_hash: str,
    per_match_written: dict[str, int],
    elapsed_seconds: float,
) -> dict[str, object]:
    """Build a single-line structured summary of this for-each iteration.

    Emitted at iteration end with per-match row counts + duration. Pairs with
    ``_iteration_fingerprint`` via the same ``fingerprint_hash`` so ops can
    JOIN start + end in log aggregation. Surfaces silent-drop cases (matches
    that wrote 0 rows) at a glance.
    """
    total_written = sum(per_match_written.values())
    zero_row_matches = sorted(m for m, n in per_match_written.items() if n == 0)
    return {
        "event": "ac1_iteration_end",
        "fingerprint_hash": fingerprint_hash,
        "provider": provider,
        "n_matches_processed": len(per_match_written),
        "n_matches_zero_rows": len(zero_row_matches),
        "zero_row_match_sample": zero_row_matches[:3] if zero_row_matches else [],
        "total_rows_written": total_written,
        "elapsed_seconds": round(elapsed_seconds, 2),
    }


def main() -> None:
    """CLI entry point for action context enrichment (for_each_task iteration).

    Reads ``--match-ids "provider:id1,id2"`` from the for_each_task input.
    Dispatches to the correct enrichment tier:
    - Tracking providers (IDSSE, Metrica, SkillCorner, GradientSports): applyInPandas
    - StatsBomb: SB360 tier (with freeze-frames) or event-only
    - Wyscout: event-only (driver-side, no tracking)
    """
    import json as _json
    import os
    import time as _time

    args = parse_ingestion_args(
        "Compute action context features",
        extra_args=[
            ("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2"}),
            (
                "--ghost-gk-backend",
                {
                    "type": str,
                    "default": None,
                    "help": "Ghost-GK KDE backend; empty resolves to AC1_GHOST_GK_BACKEND env then fft-cic. "
                    "One of {scipy,vectorized,cpu-numba,fft,fft-cic}.",
                },
            ),
        ],
    )
    task_logger = configure_logging("action_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    kde_backend = _resolve_backend_or_exit(
        getattr(args, "ghost_gk_backend", None), os.environ.get("AC1_GHOST_GK_BACKEND")
    )

    match_ids_parsed = _parse_action_match_ids_arg(getattr(args, "match_ids", None))
    if match_ids_parsed is None:
        raise SystemExit("--match-ids is required")

    provider, ids, period_filter = match_ids_parsed

    # Pre-dispatch fingerprint — structured single-line JSON for log aggregation.
    fp = _iteration_fingerprint(
        provider=provider, ids=ids, period_filter=period_filter, catalog=args.catalog, schema=args.schema
    )
    task_logger.info("AC1_FINGERPRINT %s", _json.dumps(fp, sort_keys=True))
    iteration_start = _time.monotonic()

    # Load xT grid from pre-computed Delta table (written by compute_expected_threat).
    # Each iteration reads independently -- ~192 rows, instant on serverless.
    xt_grid_data, xt_l, xt_w = _load_xt_grid_from_delta(spark, args.catalog, args.schema, task_logger)

    catalog, schema = args.catalog, args.schema
    per_match_written: dict[str, int] = {}

    if provider == "statsbomb":
        # statsbomb (ADR-058): process ALL requested matches in ONE distributed cogroup job — not
        # per-match. (Production runs this via the dedicated main_statsbomb task; this branch handles
        # a manual ``--match-ids statsbomb:a,b`` invocation.)
        task_logger.info("Processing statsbomb (sb360) batch: %d matches", len(ids))
        written = _process_statsbomb_matches(spark, catalog, schema, ids, xt_grid_data, xt_l, xt_w, task_logger)
        per_match_written = {"__statsbomb_batch__": written}
    else:
        for match_id in ids:
            period_str = f" period {period_filter}" if period_filter else ""
            task_logger.info("Processing %s match %s%s", provider, match_id, period_str)

            if _is_tracking_provider(provider):
                written = _process_tracking_match(
                    spark,
                    catalog,
                    schema,
                    provider,
                    match_id,
                    period_filter,
                    xt_grid_data,
                    xt_l,
                    xt_w,
                    task_logger,
                    kde_backend=kde_backend,
                )
            else:
                # Frames-required (ADR-057): wyscout / any non-AC provider is out of scope.
                raise SystemExit(f"Unknown provider: {provider}")
            per_match_written[match_id] = written

    # Post-write summary — same fingerprint_hash for log-aggregation join.
    summary = _iteration_summary(
        provider=provider,
        fingerprint_hash=str(fp["fingerprint_hash"]),
        per_match_written=per_match_written,
        elapsed_seconds=_time.monotonic() - iteration_start,
    )
    task_logger.info("AC1_SUMMARY %s", _json.dumps(summary, sort_keys=True))
    task_logger.info(
        "Iteration complete -- %d rows written for %s (%d/%d matches with zero rows)",
        summary["total_rows_written"],
        provider,
        summary["n_matches_zero_rows"],
        summary["n_matches_processed"],
    )


def main_statsbomb() -> None:
    """Entry point (ADR-058): process ALL pending statsbomb sb360 matches in ONE distributed
    ``cogroup.applyInPandas`` job (``_process_statsbomb_matches``). statsbomb EXITS the per-match drain;
    this is its production path. ``--max-units`` optionally caps the batch (scoped runs); ``--provider``
    is ignored (statsbomb only). Own skip-guard: a no-op when discovery finds nothing.
    """
    args = parse_ingestion_args(
        "Compute action context for statsbomb (sb360) — single distributed cogroup job",
        extra_args=[
            # str (not int): the job parameter arrives as an empty string when unset — parsed below.
            ("--max-units", {"type": str, "default": "", "help": "Cap the number of sb360 matches (empty = all)."}),
        ],
    )
    task_logger = configure_logging("action_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks
    from ingestion.guards import ensure_table

    bootstrap_hooks(spark, args.catalog, args.schema)
    catalog, schema = args.catalog, args.schema

    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    ensure_table(spark, results_table, _ACTION_CONTEXT_DDL)

    ids = _find_sb360_new_ids(
        spark, f"{catalog}.bronze.spadl_actions", results_table, f"{catalog}.bronze.statsbomb_360"
    )
    raw_max = (getattr(args, "max_units", "") or "").strip()
    if raw_max:
        ids = sorted(ids)[: int(raw_max)]  # deterministic cap, mirrors the drain's _cap

    # Skip-guard (ADR-058 / review M-2): the batch entry point owns its own empty-check — statsbomb no
    # longer contributes to the drain skip-guard count.
    if not ids:
        task_logger.info("No pending statsbomb sb360 matches — skipping")
        return

    xt_grid_data, xt_l, xt_w = _load_xt_grid_from_delta(spark, catalog, schema, task_logger)
    written = _process_statsbomb_matches(spark, catalog, schema, ids, xt_grid_data, xt_l, xt_w, task_logger)
    task_logger.info("compute_action_context_statsbomb complete -- %d matches, %d rows written", len(ids), written)


def main_drain_worker() -> None:
    """for-each worker (ADR-037): drain this worker's slice of the action-context queue.

    Receives ``--worker-id "{{input}}"`` and ``--run-id`` from the preflight task
    value (NOT from env -- see ADR-037 B1). The per-game watchdog (2700 s default,
    overridable via ``--watchdog-budget-s``) bounds each unit; the task timeout (8 h)
    bounds the whole drain.

    ``drain_worker`` is module-level (pure, patch on ``ac``); the Spark adapters are
    imported function-locally (they pull pyspark; action_context.py must import offline)
    and tests patch them at their source ``ingestion.action_context_queue.*`` (P2).
    """
    args = parse_ingestion_args(
        "Drain a worker's action-context queue slice",
        extra_args=[
            ("--worker-id", {"type": str, "default": None, "help": "for-each worker index"}),
            ("--run-id", {"type": str, "default": None, "help": "preflight run id (task value)"}),
            (
                "--watchdog-budget-s",
                {
                    "type": str,
                    "default": None,
                    "help": "Per-game watchdog budget seconds (default/empty -> WATCHDOG_BUDGET_S=2700). "
                    "Raise for slower exact ghost-GK backends.",
                },
            ),
            (
                "--frame-batch-size",
                {
                    "type": str,
                    "default": None,
                    "help": "Run-scoped frame batch size override (default/empty -> per-provider defaults "
                    "in analytics.action_context.batching). Set for memory-envelope A/Bs without a release.",
                },
            ),
        ],
    )
    task_logger = configure_logging("action_context_drain")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    raw_wid = getattr(args, "worker_id", None)
    run_id = getattr(args, "run_id", None)
    if raw_wid is None or not str(raw_wid).strip():
        raise SystemExit("--worker-id is required")
    if not run_id or not str(run_id).strip():
        task_logger.info("Empty run_id (preflight found nothing) -- drain worker exits cleanly")
        return
    worker_id = int(str(raw_wid).strip())
    run_id = str(run_id).strip()

    raw_budget = (getattr(args, "watchdog_budget_s", None) or "").strip()
    if raw_budget:
        try:
            budget_s = int(raw_budget)
        except ValueError as exc:
            raise SystemExit(f"--watchdog-budget-s must be an integer, got {raw_budget!r}") from exc
        if budget_s <= 0:
            raise SystemExit(f"--watchdog-budget-s must be > 0, got {budget_s}")
    else:
        budget_s = WATCHDOG_BUDGET_S

    # Run-scoped frame-batch-size override (ADR-047 amendment 2): validate loud at
    # startup, then publish via the driver env hook resolve_frame_batch_size reads —
    # _process_tracking_match resolves per unit and bakes the int into the UDF
    # closure, so executors never need the env var.
    raw_fbs = (getattr(args, "frame_batch_size", None) or "").strip()
    if raw_fbs:
        from analytics.action_context.batching import ENV_VAR as _FBS_ENV_VAR

        try:
            fbs = int(raw_fbs)
        except ValueError as exc:
            raise SystemExit(f"--frame-batch-size must be an integer, got {raw_fbs!r}") from exc
        if fbs <= 0:
            raise SystemExit(f"--frame-batch-size must be > 0, got {fbs}")
        os.environ[_FBS_ENV_VAR] = str(fbs)
        task_logger.info("Frame-batch-size override active for this run: %d", fbs)

    from ingestion.action_context_queue import DeltaWorkQueue, SparkGameProcessor, SparkInterruptWatchdog

    queue = DeltaWorkQueue(spark, args.catalog)
    # Short-circuit empty slices (e.g. scoped/provider-filtered runs with fewer units than
    # workers): skip the xT-grid load + processor build for workers with no assigned units.
    units = queue.units_for_worker(run_id, worker_id)
    if not units:
        task_logger.info("Drain worker %d: no units assigned for run %s -- exiting", worker_id, run_id)
        return

    processor = SparkGameProcessor(spark, args.catalog, args.schema)
    watchdog = SparkInterruptWatchdog(spark)
    summary = drain_worker(queue, processor, watchdog, run_id, worker_id, task_logger, units=units, budget_s=budget_s)
    task_logger.info(
        "Drain worker %d complete: processed=%d failed=%d timed_out=%d rows=%d",
        worker_id,
        summary.processed,
        summary.failed,
        summary.timed_out,
        summary.total_rows,
    )


# ── Provider-specific processing ──────────────────────────────────────


# GS bronze (events/roster) use pd.json_normalize dot-named columns and are very wide
# (events ~264 cols). Collecting the full width via `toPandas()` on the Spark Connect
# serverless driver trips a Catalyst attribute-resolution bug ("Cannot find column index for
# attribute possessionEvents.carrySuccessful#..."). We project to only the needed columns
# (backtick-quoted for the dots) before toPandas. `_GS_EVENTS_META_COLS` MUST cover everything
# `extract_gradientsports_match_metadata` reads — guarded by
# `test_narrow_events_cols_sufficient_for_extractor`. The other tracking providers don't hit
# this (their bronze reads are narrow / not json_normalize-wide). See project memory
# project_gradientsports_player_id_space_bug + the events-read blocker note.
_GS_EVENTS_META_COLS: tuple[str, ...] = (
    "gameEvents.homeTeam",
    "gameEvents.teamId",
    "stadiumMetadata.homeTeamStartLeft",
    "stadiumMetadata.homeTeamStartLeftExtraTime",
)
_GS_ROSTER_COLS: tuple[str, ...] = ("team.id", "shirtNumber", "player.id", "positionGroupType")


def _build_gradientsports_roster_dicts(
    roster_pdf: pd.DataFrame, home_team_id: str
) -> tuple[dict[str, str], dict[tuple[str, str], str], list[str]]:
    """Build the GS ``MatchMeta`` dicts from a non-empty ``bronze.gradientsports_roster`` frame.

    Returns ``(team_side_to_id, jersey_to_player_id, gk_player_ids)``.

    ``bronze.gradientsports_roster`` columns are dot-notation from ``pd.json_normalize``
    of the GS API payload: ``team.id``, ``shirtNumber``, ``player.id``,
    ``positionGroupType`` (NOT snake_case). The resolved ``player.id`` is the native
    GS id (string), which matches actions' ``player_id_native`` — the identity-resolution
    join key (GS SPADL stores ``player_id`` as NA + the native string in
    ``player_id_native``). Reading snake_case names KeyErrors / silently empties these
    dicts → GS carrier + possession resolution breaks. Verified against the live bronze
    schema 2026-06-01.

    FOLLOW-UP (tracked): replace this hand-rolled ``(side, jersey) -> player_id``
    resolution with silly-kicks 4.x ``add_gradientsports_player_ids`` (cross-layer:
    ``MatchMeta`` roster-records + ``convert.py``; breaks the ``test_convert_drift`` AST
    guard; int-space reconciliation vs hash-bigint events ``team_id``). See ROADMAP /
    project memory.
    """
    all_team_ids = roster_pdf["team.id"].dropna().unique()
    away_tids = [str(t) for t in all_team_ids if str(t) != home_team_id]
    away_team_id = away_tids[0] if away_tids else home_team_id
    team_side_to_id = {"home": home_team_id, "away": away_team_id}

    jersey_to_player_id: dict[tuple[str, str], str] = {}
    for _, row in roster_pdf.iterrows():
        tid = str(row.get("team.id", ""))
        side = "home" if tid == home_team_id else "away"
        jersey = str(row.get("shirtNumber", ""))
        pid = str(row.get("player.id", ""))
        if jersey and pid:
            jersey_to_player_id[(side, jersey)] = pid

    if "positionGroupType" in roster_pdf.columns:
        gk_rows = roster_pdf[roster_pdf["positionGroupType"].str.upper() == "GK"]
    else:
        gk_rows = roster_pdf.iloc[0:0]
    gk_player_ids = [str(r["player.id"]) for _, r in gk_rows.iterrows()]

    return team_side_to_id, jersey_to_player_id, gk_player_ids


def _period_replace_where(match_id: str, period_filter: int | None) -> str:
    """Delta ``replaceWhere`` predicate for a (match, optional-period) write.

    Pure (no Spark) so the per-period disjoint-write invariant is unit-testable: when ``period_filter``
    is set the predicate is period-scoped (``match_id AND period_id``), so two per-period units of the
    same match replace disjoint partitions; when ``None`` the whole match is replaced.
    """
    if period_filter is not None:
        return f"match_id = '{match_id}' AND period_id = {period_filter}"
    return f"match_id = '{match_id}'"


def _process_tracking_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    provider: str,
    match_id: str,
    period_filter: int | None,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    task_logger: logging.Logger,
    profile: bool = False,
    profile_max_batches: int = 0,
    kde_backend: str = "fft-cic",
) -> int:
    """Process a single tracking-provider match via applyInPandas.

    When ``profile=True`` (the ``profile_action_context`` observability entry
    point), the IDENTICAL input prep runs, but instead of the distributed
    ``applyInPandas`` write the per-batch ``enrich_batch`` chain runs single-
    process on the driver under ``cProfile`` and a cumulative-time breakdown is
    written to the UC Volume rendezvous dir (no bronze write). Single-process is
    the right shape for a "where does the time go" stage breakdown and mirrors
    what a single-node GPU / HF Jobs venue would look like. The ``profile`` flag
    defaults False so the production path is behaviour-identical.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.exec_visibility import PhaseHeartbeat
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )
    from ingestion.utils import ensure_volume_directory, write_delta_table

    # ── Executor→driver visibility: rendezvous dir + driver heartbeat ──
    # The driver creates the rendezvous dir (Files API needs the driver token);
    # executors write markers into it via raw open(). The heartbeat thread prints
    # elapsed + current driver phase + target row count + marker count to the
    # task log every 15s, so a hang is localized to a specific phase in real time
    # instead of the bare 3-line silence. See ingestion.exec_visibility + ADR-031.
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    exec_rendezvous_dir: str | None = (
        f"/Volumes/{catalog}/{schema}/_staging/ac1_progress/{provider}_{match_id}_p{period_filter}"
    )
    try:
        ensure_volume_directory(exec_rendezvous_dir)
    except Exception as exc:  # noqa: BLE001 — visibility is best-effort; never block compute
        task_logger.warning("Could not create rendezvous dir %s: %s", exec_rendezvous_dir, exc)
        exec_rendezvous_dir = None

    _count_where = _period_replace_where(match_id, period_filter)
    hb = PhaseHeartbeat(
        tag="AC1_HEARTBEAT",
        interval_s=15.0,
        spark=spark,
        count_table=results_table,
        count_where=_count_where,
        rendezvous_dir=exec_rendezvous_dir,
    )
    hb.start("read_tracking")

    # ── Read tracking (Spark DataFrame — no .toPandas()) ──
    if provider == "idsse":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_IDSSE_TRACKING_SELECT_COLS)
        )
    elif provider == "metrica":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_METRICA_TRACKING_SELECT_COLS)
        )
    elif provider == "skillcorner":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
        )
        matches_meta = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select(
                F.col("player_id"),
                F.col("team_id").cast("string").alias("team_id"),  # silly-kicks SC builder contract (TF-23)
                (F.col("position_acronym") == "GK").alias("is_goalkeeper"),
            )
        )
        trk_sdf = trk_sdf.join(F.broadcast(matches_meta), on="player_id", how="left")
    elif provider == "gradientsports":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.gradientsports_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_GRADIENTSPORTS_TRACKING_SELECT_COLS)
        )
    else:
        raise ValueError(f"Unknown tracking provider: {provider}")

    if period_filter is not None:
        trk_sdf = trk_sdf.filter(F.col("period") == period_filter)

    hb.set_phase("tracking_limit1_count")
    if trk_sdf.limit(1).count() == 0:
        task_logger.warning("No tracking data for %s match %s", provider, match_id)
        hb.stop()
        return 0

    # ── Read SPADL actions ──
    hb.set_phase("toPandas_actions")
    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for %s match %s", provider, match_id)
        hb.stop()
        return 0
    actions_records: list[dict[str, Any]] = actions_pdf.to_dict("records")  # type: ignore[assignment]

    # ── Resolve match-level metadata (driver scalars) ──
    home_start_left = True
    home_team_start_left_extratime: bool | None = None  # silly-kicks 4.0+ ET guard input
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None

    if provider == "idsse":
        from silly_kicks.providers.sportec import (
            derive_idsse_home_team_start_left,
            derive_idsse_home_team_start_left_extratime,
            shape_events_to_native,
        )

        hb.set_phase("toPandas_idsse_events")
        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = shape_events_to_native(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
        home_team_start_left_extratime = derive_idsse_home_team_start_left_extratime(adapted_events, home_team_id)
        del events_pdf, adapted_events
    elif provider == "metrica":
        home_team_id = "Home"
        # ET-flag derivation deferred: Metrica events would require a separate
        # bronze read here; zero Metrica ET matches in bronze today (§8 audit
        # 2026-05-30), so None is correct under silly-kicks 4.0's guard.
        # When IDSSE-style Metrica ET data lands, plumb
        # derive_metrica_home_team_start_left_extratime() in via a bronze
        # events read mirroring the IDSSE branch above.
    elif provider == "skillcorner":
        row = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select("home_team_id")
            .limit(1)
            .collect()[0]
        )
        home_team_id = str(row["home_team_id"])
    elif provider == "gradientsports":
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        gs_events_tbl = f"{catalog}.bronze.gradientsports_events"
        # Narrow projection (NOT the full 264-col wide table) — see _GS_EVENTS_META_COLS:
        # the wide toPandas trips a Spark Connect Catalyst bug on serverless. Backtick-quote
        # the dot-named columns so they resolve as flat columns, not nested-field accesses.
        events_pdf = (
            spark.table(gs_events_tbl)
            .filter(F.col("match_id") == match_id)
            .select(*[f"`{c}`" for c in _GS_EVENTS_META_COLS])
            .toPandas()
        )
        gs_meta = extract_gradientsports_match_metadata(events_pdf)
        home_team_id = str(gs_meta["home_team_id"])
        home_start_left = gs_meta["home_team_start_left"]
        # GS bronze carries stadiumMetadata.homeTeamStartLeftExtraTime already.
        home_team_start_left_extratime = gs_meta["home_team_start_left_extratime"]
        del events_pdf

        # Build team_side->team_id + (side,jersey)->player_id + GK dicts from the
        # roster (see _build_gradientsports_roster_dicts for the bronze dot-notation
        # column contract + the silly-kicks add_gradientsports_player_ids follow-up).
        gs_roster_tbl = f"{catalog}.bronze.gradientsports_roster"
        # Narrow projection (same dot-named / wide-toPandas guard as the events read above).
        roster_pdf = (
            spark.table(gs_roster_tbl)
            .filter(F.col("match_id") == match_id)
            .select(*[f"`{c}`" for c in _GS_ROSTER_COLS])
            .toPandas()
        )
        if not roster_pdf.empty:
            gs_team_side_to_id, gs_jersey_to_player_id, gs_gk_player_ids = _build_gradientsports_roster_dicts(
                roster_pdf, home_team_id
            )

        del roster_pdf

    # ── Frame batching + UDF dispatch ──
    # Use "frame" for most providers; GradientSports uses "frame_num"
    # Per-provider size, run-overridable via AC_FRAME_BATCH_SIZE (ADR-047 am. 2).
    # The SAME resolved value travels into the UDF closure below so the M13
    # single-owner action math batches identically (H3).
    frame_batch_size = resolve_frame_batch_size(provider)
    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    trk_sdf = trk_sdf.withColumn(
        "frame_batch_id",
        F.floor(F.col(frame_col) / F.lit(frame_batch_size)),
    )

    # GradientSports: the frame-batch / link / owned-action logic needs a "timestamp" column,
    # but the GS converter (_bronze_gradientsports_to_converter_input) reads "period_elapsed_time".
    # ADD an alias rather than rename — a destructive rename drops period_elapsed_time and the
    # converter then KeyErrors (latent until the upstream GS blockers were cleared).
    if provider == "gradientsports":
        trk_sdf = trk_sdf.withColumn("timestamp", F.col("period_elapsed_time"))

    # Metrica (ADR-040): bronze.metrica_tracking.timestamp is the ABSOLUTE match clock
    # (and Sample_Game_3's resets to 0 in P2 — the 3 open-data games are hand-curated and
    # inconsistent). Re-base "timestamp" to PERIOD-RELATIVE via the CONTINUOUS frame number,
    # keyed on each period's FIRST frame, so it aligns with the SPADL action time_seconds —
    # which _convert_metrica_from_bronze re-bases off the SAME min(frame) per (match,period)
    # from this same bronze.metrica_tracking. Frame-number based (NOT the timestamp) so
    # Sample_Game_3's timestamp reset is irrelevant. Mirrors pipeline.run_work_unit, kept in
    # lockstep by test_metrica_period_relative_time's sentinel.
    if provider == "metrica":
        from pyspark.sql import Window as _Window

        _period_w = _Window.partitionBy("match_id", "period")
        _fr_col = F.coalesce(F.col("frame_rate").cast("double"), F.lit(25.0))
        trk_sdf = (
            trk_sdf.withColumn("_period_min_frame", F.min("frame").over(_period_w))
            .withColumn(
                "timestamp",
                (F.col("frame").cast("double") - F.col("_period_min_frame").cast("double")) / _fr_col,
            )
            .drop("_period_min_frame")
        )

    # SkillCorner (ADR-040 amendment): bronze "timestamp" is the ABSOLUTE broadcast clock
    # (P2 = 2700s+) while SPADL actions are period-relative. The CONVERTER already re-bases
    # the converted frames (_bronze_skillcorner_to_frames), but THIS dispatch layer's batch
    # window filter + M13 ownership read the bronze column directly and silently dropped
    # ~90% of P2 actions (2026-06-11 scoped-run census: 65/536 and 50/573 emitted). Subtract
    # the SAME nominal offsets the converter uses — one imported constant, no second copy.
    # Mirrors pipeline.run_work_unit; lockstep via test_skillcorner_dispatch_time_base.
    if provider == "skillcorner":
        # B' (TF-23): single-source the SC period offset from silly-kicks; lakehouse copy deleted.
        from silly_kicks.spadl.skillcorner import _PERIOD_START_SECONDS as _SKILLCORNER_PERIOD_START_SECONDS

        _sc_offset = F.coalesce(
            F.create_map(*[F.lit(x) for kv in sorted(_SKILLCORNER_PERIOD_START_SECONDS.items()) for x in kv])[
                F.col("period")
            ],
            F.lit(0.0),
        )
        trk_sdf = trk_sdf.withColumn("timestamp", F.col("timestamp").cast("double") - _sc_offset)

    # Work-unit time-base guard (ADR-040): assert the work unit's actions are period-relative
    # (not on an absolute match clock — the GS period-2 class) before the per-batch applyInPandas
    # dispatch. Frame-independent (action min per period from the in-driver actions_pdf); mirrors
    # the local hexagon (pipeline.run_work_unit), kept in lockstep by test_time_base_guard's sentinel.
    from analytics.action_context.time_base_guard import assert_frames_time_base, assert_work_unit_time_base

    if "time_seconds" in actions_pdf.columns:
        assert_work_unit_time_base(
            {
                int(p): float(s.min())
                for p, s in actions_pdf.dropna(subset=["time_seconds"]).groupby("period_id")["time_seconds"]
            }
        )

    # Frames-side time-base guard (ADR-040 amendment, two-sided): after ALL provider re-bases,
    # each period's earliest frame time must be near its own kickoff — a frames-side absolute
    # clock silently empties the per-batch action window (the SkillCorner P2 class the
    # actions-side guard above cannot see). One tiny per-period agg per unit; min-based so
    # sparse frame coverage never false-fires. Mirrors pipeline.run_work_unit (same lockstep
    # sentinel). The same agg also yields: (a) the (min, max) windows for the post-write
    # completeness invariant below, and (b) the M13 GLOBAL ownership anchors — the frame at
    # each period's earliest/latest timestamp — so every UDF batch claims actions off the
    # IDENTICAL frame↔time line (per-batch fits drift on gappy tracking and double-claim
    # boundary actions; lockstep via test_m13_global_anchor).
    _frame_windows: dict[int, tuple[float, float]] = {}
    _ownership_anchors: dict[int, tuple[float, float, float]] = {}
    _anchor_frame_col = "frame_num" if provider == "gradientsports" else "frame"
    if "timestamp" in trk_sdf.columns:
        _ts_rows = (
            trk_sdf.groupBy("period")
            .agg(
                F.min("timestamp").alias("_ts_min"),
                F.max("timestamp").alias("_ts_max"),
                F.min_by(_anchor_frame_col, "timestamp").alias("_f_at_min"),
                F.max_by(_anchor_frame_col, "timestamp").alias("_f_at_max"),
            )
            .collect()
        )
        _frame_windows = {
            int(r["period"]): (float(r["_ts_min"]), float(r["_ts_max"])) for r in _ts_rows if r["_ts_min"] is not None
        }
        for r in _ts_rows:
            if r["_ts_min"] is None or r["_f_at_min"] is None or r["_ts_max"] == r["_ts_min"]:
                continue
            _t0, _t1 = float(r["_ts_min"]), float(r["_ts_max"])
            _f0, _f1 = float(r["_f_at_min"]), float(r["_f_at_max"])
            _ownership_anchors[int(r["period"])] = (_t0, _f0, (_f1 - _f0) / (_t1 - _t0))
        assert_frames_time_base({p: w[0] for p, w in _frame_windows.items()})

    # Observability branch: single-process cProfile on the driver instead of the
    # distributed applyInPandas write. trk_sdf is already shaped exactly like the
    # UDF's groups (frame_batch_id present), so the per-batch enrich_batch calls
    # are identical to production — only serial + profiled. No bronze write.
    if profile:
        hb.set_phase("profile_driver_cprofile")
        try:
            return _run_profile_on_driver(
                trk_sdf=trk_sdf,
                provider=provider,
                native_match_id=match_id,
                actions_records=actions_records,
                xt_grid_data=xt_grid_data,
                xt_l=xt_l,
                xt_w=xt_w,
                home_team_id=home_team_id,
                home_start_left=home_start_left,
                home_team_start_left_extratime=home_team_start_left_extratime,
                gs_team_side_to_id=gs_team_side_to_id,
                gs_jersey_to_player_id=gs_jersey_to_player_id,
                gs_gk_player_ids=gs_gk_player_ids,
                exec_rendezvous_dir=exec_rendezvous_dir,
                task_logger=task_logger,
                max_batches=profile_max_batches,
                kde_backend=kde_backend,
                frame_batch_size=frame_batch_size,
                ownership_anchors=_ownership_anchors,
            )
        finally:
            hb.stop()

    # Progress visibility comes from the UDF closure itself: each successful
    # batch emits one AC1_BATCH log line (see _make_action_context_udf). The
    # earlier driver-aggregated design (LongAccumulator + heartbeat thread)
    # crashed on Databricks serverless because Spark Connect forbids
    # spark.sparkContext access. See ADR-031.
    udf_fn = _make_action_context_udf(
        provider=provider,
        home_team_id=home_team_id,
        home_start_left=home_start_left,
        xt_grid_data=xt_grid_data,
        xt_l=xt_l,
        xt_w=xt_w,
        actions_records=actions_records,
        native_match_id=match_id,
        home_team_start_left_extratime=home_team_start_left_extratime,
        gs_team_side_to_id=gs_team_side_to_id,
        gs_jersey_to_player_id=gs_jersey_to_player_id,
        gs_gk_player_ids=gs_gk_player_ids,
        exec_rendezvous_dir=exec_rendezvous_dir,
        kde_backend=kde_backend,
        frame_batch_size=frame_batch_size,
        ownership_anchors=_ownership_anchors,
    )

    # GradientSports uses "period" (not "period_id") in bronze
    hb.set_phase("applyInPandas_build_dag")
    # ADR-045: repartition with an EXPLICIT N (exempt from AQE bytes-based coalescing) +
    # sortWithinPartitions (groups contiguous) + mapInPandas (streaming per-group dispatch)
    # replaces groupBy().applyInPandas, whose shuffle AQE coalesced to ~1 task for a whole
    # Metrica half (measured concurrency 1.00 → strictly serial enrichment). Same udf_fn,
    # same per-group inputs, same output schema — only the task topology changes.
    _group_keys = ["match_id", "period", "frame_batch_id"]
    result_sdf = (
        trk_sdf.repartition(_UDF_SHUFFLE_PARTITIONS, *_group_keys)
        .sortWithinPartitions(*_group_keys)
        .mapInPandas(
            _make_streaming_group_mapper(udf_fn, _group_keys),
            schema=_get_result_schema(),
        )
    )

    rw = _period_replace_where(match_id, period_filter)

    # This is the action that materializes the applyInPandas DAG. If the hang is
    # in the UDF, the heartbeat keeps printing "phase=write_delta_applyInPandas"
    # with rows=0 and (if executor writes work) markers>0; if the hang is a
    # driver-side action above, we never reach this phase.
    hb.set_phase("write_delta_applyInPandas")
    try:
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=rw,
            logger=task_logger,
        )
    finally:
        hb.stop()

    # Per-unit completeness invariant (ADR-040 amendment): emitted rows vs the actions the
    # frames COVER (per-period frame window, post-rebase) — converts silent data loss into a
    # loud unit failure (the SkillCorner P2 class shipped 12% of a half as a "successful"
    # unit). Window-relative so partial broadcast coverage stays valid. Mirrors
    # pipeline.run_work_unit (lockstep via test_skillcorner_dispatch_time_base).
    from analytics.action_context.completeness import (
        assert_unit_action_completeness,
        expected_actions_within_coverage,
    )

    if _frame_windows and "time_seconds" in actions_pdf.columns:
        _act_pdf = actions_pdf.dropna(subset=["time_seconds"])
        if period_filter is not None and "period_id" in _act_pdf.columns:
            _act_pdf = _act_pdf[_act_pdf["period_id"] == int(period_filter)]
        _times = {int(p): s.tolist() for p, s in _act_pdf.groupby("period_id")["time_seconds"]}
        assert_unit_action_completeness(
            emitted=written,
            expected=expected_actions_within_coverage(_times, _frame_windows, buffer_s=_ACTION_TIME_BUFFER_SECONDS),
            unit_desc=f"{provider}:{match_id}:{period_filter}",
        )
    del actions_pdf, actions_records
    return written


# Stage-entry function names whose cumulative time we roll up explicitly in the
# profile summary (substring match against pstats funcnames). These are the
# expensive tracking-frame-level silly-kicks / accessible-space stages from
# analytics.action_context.enrich._enrich_tracking_match. The top-N-by-cumtime
# table catches anything not listed here; this rollup gives the headline
# "DAS = X%, pitch control = Y%" answer directly.
_PROFILE_STAGE_FUNCS: tuple[str, ...] = (
    "get_dangerous_accessible_space",  # DAS (accessible-space entry)
    "add_das",
    "get_das",
    "simulate_passes",  # accessible-space inner sim (F x PHI x T)
    "pitch_control_at_target",
    "add_obso",
    "add_ghost_gk",
    "add_space_creation",
    "add_cover_shadows",
    "add_gk_influence",
    "add_shape_graph",
    "add_defensive_line",
    "add_line_break",
    "add_team_shape",
    "add_pressure_on_actor",
    "add_action_context",
    "add_actor_pre_window",
    "add_off_ball_context",
    "infer_ball_carrier",
    "derive_team_in_possession",
    "link_actions_to_frames",
    "add_pre_shot_gk_context",
    "add_pausa",
    "add_elastic_sync",
)


def _format_profile_summary(
    profiler: object, *, total_s: float, n_rows: int, n_batches: int, n_total_batches: int
) -> str:
    """Render a cProfile result into a human-readable cumulative-time breakdown.

    Two sections: (1) a curated rollup of the known expensive enrichment stages
    (``_PROFILE_STAGE_FUNCS``) with cumtime + % of wall, and (2) the top 40
    functions by cumulative time (catches anything the rollup misses).
    """
    import io as _io
    import pstats as _pstats

    stream = _io.StringIO()
    stats = _pstats.Stats(profiler, stream=stream)  # type: ignore[arg-type]

    # stats.stats maps (file, lineno, func) -> (cc, nc, tt, ct, callers).
    by_func = stats.stats  # type: ignore[attr-defined]
    stage_rows: list[tuple[float, int, str]] = []
    for (_fname, _lineno, func), (_cc, nc, _tt, ct, _callers) in by_func.items():
        for marker in _PROFILE_STAGE_FUNCS:
            if marker in func:
                stage_rows.append((ct, nc, func))
                break
    stage_rows.sort(reverse=True)

    lines: list[str] = []
    lines.append("AC1_CPROFILE — single-process driver profile of the tracking enrichment")
    sampled = " (SAMPLE)" if n_batches < n_total_batches else " (FULL MATCH)"
    lines.append(f"wall_s={total_s:.1f} rows_enriched={n_rows} batches_profiled={n_batches}/{n_total_batches}{sampled}")
    lines.append("")
    lines.append("=== STAGE ROLLUP (cumtime, % of wall, ncalls) ===")
    if not stage_rows:
        lines.append("  <no known stage functions matched — see top-by-cumtime below>")
    for ct, nc, func in stage_rows:
        pct = 100.0 * ct / total_s if total_s > 0 else 0.0
        lines.append(f"  {ct:8.1f}s  {pct:5.1f}%  n={nc:<7d}  {func}")
    lines.append("")
    lines.append("=== TOP 40 BY CUMULATIVE TIME ===")
    stats.sort_stats("cumulative")
    stats.print_stats(40)
    lines.append(stream.getvalue())
    return "\n".join(lines)


def _run_profile_on_driver(
    *,
    trk_sdf: DataFrame,
    provider: str,
    native_match_id: str,
    actions_records: list[dict[str, Any]],
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    home_team_id: str,
    home_start_left: bool,
    home_team_start_left_extratime: bool | None,
    gs_team_side_to_id: dict[str, str] | None,
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None,
    gs_gk_player_ids: list[str] | None,
    exec_rendezvous_dir: str | None,
    task_logger: logging.Logger,
    max_batches: int = 0,
    kde_backend: str = "fft-cic",
    frame_batch_size: int | None = None,
    ownership_anchors: dict[int, tuple[float, float, float]] | None = None,
) -> int:
    """Pull the whole match to the driver, run ``enrich_batch`` per frame
    batch under ``cProfile`` (single-process), and write the breakdown to the
    UC Volume rendezvous dir. Returns the number of enriched rows (NOT written
    to bronze — this is a measurement path). ``frame_batch_size`` must match the
    size the caller used to assign ``frame_batch_id`` on ``trk_sdf`` (H3).

    ``max_batches`` > 0 profiles only the first N (period, frame_batch_id) groups
    — a representative sample for the relative stage breakdown at a fraction of
    the serial wall-clock. ``0`` profiles the whole match (high fidelity, but
    serial: can be much slower than the distributed production run). NOTE: a
    sample over-weights one-time costs (model load, numba JIT warmup) relative
    to a full-match run — read the rollup as relative stage shares, not absolutes.
    """
    import cProfile
    import time as _time

    from analytics.action_context.pipeline import enrich_batch
    from analytics.action_context.work_unit import MatchMeta
    from ingestion.exec_visibility import ensure_numba_cache_dir, executor_marker

    ensure_numba_cache_dir()  # match the UDF's serverless numba-cache setup

    # Self-certify the analytics libs the executor env actually resolved. The
    # serverless env can silently resolve a stale silly-kicks (PyPI/index lag or
    # a cached env) despite an explicit ``>=`` pin — a profile attributed to the
    # wrong version is worse than no profile. Printed BEFORE the heavy compute so
    # the driver log reveals the version within seconds (kill early if wrong).
    import importlib.metadata as _md

    _versions = {}
    for _pkg in ("silly-kicks", "accessible-space", "numba", "numpy", "scipy"):
        try:
            _versions[_pkg] = _md.version(_pkg)
        except _md.PackageNotFoundError:
            _versions[_pkg] = "<absent>"
    task_logger.info(
        "AC1_PROFILE env_versions silly-kicks=%s accessible-space=%s numba=%s numpy=%s scipy=%s",
        _versions["silly-kicks"],
        _versions["accessible-space"],
        _versions["numba"],
        _versions["numpy"],
        _versions["scipy"],
    )

    frames_all = trk_sdf.toPandas()  # one match fits the 16 GB driver
    task_logger.info("AC1_PROFILE pulled %d tracking rows for %s/%s", len(frames_all), provider, native_match_id)
    if frames_all.empty:
        task_logger.warning("AC1_PROFILE no tracking rows for %s/%s — nothing to profile", provider, native_match_id)
        return 0

    meta = MatchMeta(
        home_team_id=home_team_id,
        home_start_left=home_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
        gs_team_side_to_id=gs_team_side_to_id,
        gs_jersey_to_player_id=gs_jersey_to_player_id,
        gs_gk_player_ids=gs_gk_player_ids,
    )

    # One groupby group == one applyInPandas group == one enrich_batch call.
    groups = list(frames_all.groupby(["period", "frame_batch_id"], sort=True))
    n_total_batches = len(groups)
    if max_batches and max_batches > 0 and n_total_batches > max_batches:
        groups = groups[:max_batches]
        task_logger.info(
            "AC1_PROFILE sampling first %d of %d batches (relative stage shares; one-time costs over-weighted)",
            max_batches,
            n_total_batches,
        )
    n_rows = 0
    # Result-health: non-null counts for carrier/possession-dependent columns. Proves the
    # enrichment RESOLVES (not just "no crash") — e.g. catches GS possession breaking when
    # frame ids don't match the action id space. All-zero here == broken resolution.
    _health_cols = ("das_team", "das_opponent", "das_diff", "ghost_gk_x", "ghost_gk_density_spread")
    health_nonnull: dict[str, int] = dict.fromkeys(_health_cols, 0)
    profiler = cProfile.Profile()
    t0 = _time.monotonic()
    profiler.enable()
    for (period_val, _batch_id), group_pdf in groups:
        result = enrich_batch(
            provider=provider,
            tier="tracking",
            frames_pdf=group_pdf,
            actions_records=actions_records,
            period=int(period_val),
            xt_grid_data=xt_grid_data,
            xt_l=xt_l,
            xt_w=xt_w,
            meta=meta,
            native_match_id=native_match_id,
            kde_backend=kde_backend,
            frame_batch_size=frame_batch_size,
            ownership_anchors=ownership_anchors,
        )
        n_rows += len(result)
        for _c in _health_cols:
            if _c in result.columns:
                health_nonnull[_c] += int(result[_c].notna().sum())
    profiler.disable()
    total_s = _time.monotonic() - t0
    health_str = " ".join(
        f"{c}={100.0 * health_nonnull[c] / n_rows:.0f}%" if n_rows else f"{c}=n/a" for c in _health_cols
    )
    task_logger.info("AC1_PROFILE result-health (non-null rate over %d rows): %s", n_rows, health_str)

    summary = _format_profile_summary(
        profiler, total_s=total_s, n_rows=n_rows, n_batches=len(groups), n_total_batches=n_total_batches
    )
    # ALWAYS log the full summary to the driver task log. This is the reliable
    # retrieval channel: the job runs as the ingestion SP (for UC Volume marker
    # writes) but the operator submits under their own identity, which may lack
    # READ VOLUME on _staging — so the cprofile_summary marker can be unreadable
    # by the submitter. jobs.get_run_output returns the task log regardless. The
    # marker + .pstats below are a convenience (submit_ac1_oneshot._dump_markers /
    # offline deep-dive), not the source of truth.
    task_logger.info(
        "AC1_PROFILE done: wall_s=%.1f rows=%d batches=%d/%d\n%s",
        total_s,
        n_rows,
        len(groups),
        n_total_batches,
        summary,
    )
    if exec_rendezvous_dir:
        executor_marker(exec_rendezvous_dir, seq="cprofile_summary", payload=summary)
        try:
            profiler.dump_stats(f"{exec_rendezvous_dir}/cprofile.pstats")
        except OSError as exc:
            task_logger.warning("AC1_PROFILE could not dump .pstats: %s", exc)
    return n_rows


def profile_action_context() -> None:
    """Observability entry point — cProfile ONE tracking match's enrichment.

    Runs the identical production input prep, then profiles the per-batch
    ``enrich_batch`` chain single-process on the driver (no bronze write). The
    cumulative-time breakdown is written to the UC Volume rendezvous dir, which
    ``scripts/submit_ac1_oneshot.py --profile`` prints back. Use to quantify each
    enrichment stage's share of per-match wall-clock (e.g. DAS vs pitch control)
    in the real serverless environment.

    Usage (via the one-shot submitter, on serverless)::

        uv run python scripts/submit_ac1_oneshot.py --profile --match-ids skillcorner:2011166
    """
    args = parse_ingestion_args(
        "Profile action context enrichment for one tracking match",
        extra_args=[
            ("--match-ids", {"type": str, "default": None, "help": "provider:id (tracking providers only)"}),
            (
                "--max-batches",
                {
                    "type": int,
                    "default": 0,
                    "help": "Profile only the first N frame batches (representative sample; "
                    "0 = whole match, high fidelity but serial-slow).",
                },
            ),
        ],
    )
    task_logger = configure_logging("action_context_profile")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids_parsed = _parse_action_match_ids_arg(getattr(args, "match_ids", None))
    if match_ids_parsed is None:
        raise SystemExit("--match-ids is required")
    provider, ids, period_filter = match_ids_parsed
    if not _is_tracking_provider(provider):
        raise SystemExit(f"--profile only supports tracking providers, got {provider!r}")
    if len(ids) != 1:
        raise SystemExit(f"--profile takes exactly one match, got {len(ids)}: {ids}")

    xt_grid_data, xt_l, xt_w = _load_xt_grid_from_delta(spark, args.catalog, args.schema, task_logger)
    n_rows = _process_tracking_match(
        spark,
        args.catalog,
        args.schema,
        provider,
        ids[0],
        period_filter,
        xt_grid_data,
        xt_l,
        xt_w,
        task_logger,
        profile=True,
        profile_max_batches=int(getattr(args, "max_batches", 0)),
    )
    task_logger.info("AC1_PROFILE complete for %s/%s — %d rows enriched (not written)", provider, ids[0], n_rows)


# ── SB360 distributed (cogroup.applyInPandas) processing (ADR-058) ────────────
# statsbomb is processed as ONE distributed job over all pending sb360 matches (not per-match in the
# drain): scan each bronze table once, enrich per match on executors, distributed write. This removes
# the driver-side toPandas + the 8-worker per-match replaceWhere commit contention.


def _canon_key(col: str):
    """Canonicalize an id column on EVERY join side exactly as ``_find_sb360_new_ids`` (ADR-019):
    ``cast("long").cast("string")`` normalizes the ``"3788746.0"`` vs ``"3788746"`` float-format
    class. A bare ``cast("string")`` on a double-typed id yields ``"3788746.0"`` → silently drops from
    ``.isin`` + mis-aligns the cogroup. The IN-list ids are the ``"3788746"``-style native strings."""
    from pyspark.sql import functions as F  # noqa: N812

    return F.col(col).cast("long").cast("string")


def _empty_result_pdf() -> pd.DataFrame:
    """0-row frame with RESULT_COLUMNS, dtype-correct (``build_output`` fills STRING→object,
    numeric→float64). Returned by the cogroup UDF for empty / 0-frame matches; with 0 rows Arrow has
    nothing to convert, so the float64↔BIGINT schema seam is not exercised on this path."""
    return _build_output(pd.DataFrame(), match_id_native="", data_source="statsbomb")


def _make_sb360_cogroup_udf(xt_grid_data: list[list[float]], xt_l: int, xt_w: int):
    """Build the ``cogroup.applyInPandas`` UDF: ``(actions_pdf, sb360_pdf)`` for ONE match → AC rows,
    on an executor. Closure captures only picklable scalars (xt grid + dims) per ADR-045; the ghost-GK
    model is no longer needed on sb360 (ADR-058)."""

    def _udf(actions_pdf: pd.DataFrame, sb360_pdf: pd.DataFrame) -> pd.DataFrame:
        from analytics.action_context.enrich import _enrich_sb360_match
        from analytics.action_context.sb360_snapshots import build_sb360_snapshots, resolve_home_team_id

        if actions_pdf.empty:
            return _empty_result_pdf()
        # DETERMINISM (ADR-058): cogroup gives no row-order guarantee — sort so the dup-event
        # keep="last" tie-break + any iloc[0] are reproducible (identical to the legacy path).
        actions_pdf = actions_pdf.sort_values("action_id").reset_index(drop=True)
        match_id = str(actions_pdf["match_id_native"].iloc[0])
        frames = build_sb360_snapshots(actions_pdf, sb360_pdf)
        if frames.empty:
            return _empty_result_pdf()
        home = resolve_home_team_id(actions_pdf)
        xt = _reconstruct_xt(xt_grid_data, xt_l, xt_w)
        result = _enrich_sb360_match(actions_pdf, frames, home, xt)
        return _build_output(result, match_id_native=match_id, data_source="statsbomb")

    return _udf


def _process_statsbomb_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    match_ids: list[str],
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    task_logger: logging.Logger,
) -> int:
    """Process all given statsbomb sb360 matches in ONE distributed ``cogroup.applyInPandas`` job."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    if not match_ids:  # empty -> "match_id IN ()" is a SQL syntax error; the discovery skip-guard
        task_logger.info("No statsbomb sb360 matches to process — skipping")  # should pre-empt this.
        return 0

    # CRITICAL (ADR-058): discovery is INCREMENTAL + CAPPED — ``match_ids`` is a SUBSET, never the
    # full statsbomb corpus. A full-partition ``replace_where="data_source='statsbomb'"`` would DELETE
    # every previously-processed match and rewrite only this batch → silent data loss. Use the
    # incremental ``match_id IN (...)`` list (codebase convention: formations_efpi / defcon_lite_360).
    actions_sdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("data_source") == "statsbomb") & _canon_key("match_id_native").isin(match_ids))
        .withColumn("_ck", _canon_key("match_id_native"))
    )
    sb360_sdf = (
        spark.table(f"{catalog}.bronze.statsbomb_360")
        .filter(_canon_key("match_id").isin(match_ids))
        .withColumn("_ck", _canon_key("match_id"))
    )
    result_sdf = (
        actions_sdf.groupBy("_ck")
        .cogroup(sb360_sdf.groupBy("_ck"))
        .applyInPandas(_make_sb360_cogroup_udf(xt_grid_data, xt_l, xt_w), schema=_get_result_schema())
    )
    # build_output writes match_id = match_id_native (native; no hashing) → IN-list = those native strings.
    ids_sql = ", ".join("'" + str(m).replace("'", "''") + "'" for m in match_ids)
    return write_delta_table(
        result_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"data_source = 'statsbomb' AND match_id IN ({ids_sql})",
        logger=task_logger,
    )


if __name__ == "__main__":
    main()
