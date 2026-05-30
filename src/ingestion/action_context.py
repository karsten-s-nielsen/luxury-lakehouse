"""AC-1 — Unified action context pipeline.

Reads SPADL actions + tracking data from bronze, runs the full silly-kicks
enrichment chain in a single applyInPandas pass per match, writes results to
bronze.spadl_action_context.

Providers: ALL (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, GradientSports).
Event-only providers get game_state + GK resolution; tracking providers get ~102 cols.
Architecture: "Read from bronze, compute, write to bronze."
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

import numpy as np

from analytics.action_context.enrich import (
    _enrich_event_only_match,
    _enrich_sb360_match,
)
from analytics.action_context.schema import (
    ACTION_CONTEXT_DDL as _ACTION_CONTEXT_DDL,
)
from analytics.action_context.schema import (
    build_output as _build_output,
)
from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

_TABLE_NAME = "spadl_action_context"

# ── Frame batching ────────────────────────────────────────────────────
# IDSSE (match_id, period) groups are 1.5M-1.7M rows -- exceeds the 1 GB
# Databricks serverless UDF group cap. Sub-batch by frame number.
_FRAME_BATCH_SIZE = 250

# Tolerance (seconds) for buffering actions at batch edges.
_ACTION_TIME_BUFFER_SECONDS = 0.5

# Metrica player ID jersey regex — compiled at module level per convention.
_JERSEY_RE = re.compile(r"Player\s*(\d+)")


# ── Provider classification ──────────────────────────────────────────

_TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
_EVENT_ONLY_PROVIDERS: frozenset[str] = frozenset({"statsbomb", "wyscout"})
_ALL_PROVIDERS: frozenset[str] = _TRACKING_PROVIDERS | _EVENT_ONLY_PROVIDERS


def _is_tracking_provider(provider: str) -> bool:
    return provider in _TRACKING_PROVIDERS


def _is_event_only_provider(provider: str) -> bool:
    return provider in _EVENT_ONLY_PROVIDERS


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


class _BatchHeartbeat:
    """Driver-side thread that periodically logs Spark applyInPandas progress.

    Tracking-heavy AC-1 batches take 10+ minutes total to enrich + write, and
    Spark's default logging is silent for the duration of an applyInPandas
    action. Without a heartbeat, the operator sees no signal between
    "Processing match X" and "wrote N rows" — making it impossible to
    distinguish slow-but-progressing from stuck.

    Spark-agnostic by design: takes a ``read_progress: Callable[[], int]``
    closure (typically ``lambda: accumulator.value``) so unit tests can verify
    lifecycle without a SparkSession. Use as a context manager so the polling
    thread is guaranteed to stop even if the Spark action raises.

    Daemon thread + ``threading.Event``-driven sleep ensures script exit is
    instant if the heartbeat survives past main().
    """

    def __init__(
        self,
        *,
        read_progress: Callable[[], int],
        interval_s: float,
        logger: logging.Logger,
        label: str = "batches_completed",
    ) -> None:
        if interval_s <= 0:
            msg = f"_BatchHeartbeat: interval_s must be > 0 (got {interval_s})"
            raise ValueError(msg)
        self._read_progress = read_progress
        self._interval_s = float(interval_s)
        self._logger = logger
        self._label = label
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at: float | None = None

    def __enter__(self) -> _BatchHeartbeat:
        import time as _time

        self._started_at = _time.monotonic()
        self._thread = threading.Thread(target=self._loop, name="ac1-heartbeat", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self._stop_event.set()
        if self._thread is not None:
            # Wait briefly for the thread to acknowledge stop. Daemon ensures
            # process exit isn't blocked even if join times out.
            self._thread.join(timeout=max(self._interval_s, 5.0))
            self._thread = None

    def _loop(self) -> None:
        import time as _time

        # First wait BEFORE first log so we don't log "0 batches" immediately on start.
        while not self._stop_event.wait(self._interval_s):
            try:
                count = self._read_progress()
            except Exception as exc:  # noqa: BLE001 — heartbeat must never crash the worker
                self._logger.warning("heartbeat read_progress failed: %s", exc)
                continue
            elapsed = _time.monotonic() - (self._started_at or _time.monotonic())
            self._logger.info(
                "HEARTBEAT %s=%d elapsed_s=%.1f",
                self._label,
                count,
                elapsed,
            )


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
    batches_counter: Any = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the applyInPandas UDF closure for action context enrichment.

    All arguments are Python scalar primitives or small serializable structures.
    GradientSports-specific args (gs_*) are only needed for that provider.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import logging as _logging
        import traceback as _tb

        import pandas as _pd

        from analytics.action_context.pipeline import enrich_batch as _enrich_batch
        from analytics.action_context.schema import RESULT_COLUMNS as _RC
        from analytics.action_context.work_unit import MatchMeta as _MatchMeta

        _logger = _logging.getLogger("action_context_udf")

        if pdf.empty:
            output_cols = [c for c in _RC if c != "_ingested_at"]
            return _pd.DataFrame(columns=_pd.Index(output_cols))

        match_id_val = pdf["match_id"].iloc[0]
        period_val = pdf["period"].iloc[0]
        batch_id_val = pdf["frame_batch_id"].iloc[0] if "frame_batch_id" in pdf.columns else None

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
            )
            # Increment ONLY on success — failed batches must not look "completed"
            # in the heartbeat. Closed over from the driver-side LongAccumulator.
            if batches_counter is not None:
                batches_counter.add(1)
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
            raise RuntimeError(
                f"action_context UDF failed for match_id={match_id_val}, "
                f"period={period_val}, frame_batch_id={batch_id_val}:\n{inner_tb}"
            ) from exc

    return _udf


# ── Guard ─────────────────────────────────────────────────────────────


def _find_tracking_new_ids(
    spark: SparkSession,
    tracking_table: str,
    spadl_table: str,
    results_table: str,
    provider: str,
) -> list[str]:
    """Find unprocessed tracking matches that also have SPADL actions (Spark-native).

    Three-way query pushed entirely to Spark executors:
      tracking ∩ spadl (INNER JOIN) \\ results (LEFT ANTI JOIN)
    """
    from pyspark.sql import functions as F  # noqa: N812

    tracking_df = spark.table(tracking_table).select(F.col("match_id").cast("string").alias("_join_id")).distinct()
    spadl_df = (
        spark.table(spadl_table)
        .filter(F.col("data_source") == provider)
        .select(F.col("match_id_native").cast("string").alias("_join_id"))
        .distinct()
    )
    results_df = (
        spark.table(results_table)
        .filter(F.col("data_source") == provider)
        .select(F.col("match_id").cast("string").alias("_join_id"))
        .distinct()
    )
    new_df = tracking_df.join(spadl_df, "_join_id", "inner").join(results_df, "_join_id", "left_anti")
    return [str(row["_join_id"]) for row in new_df.collect()]


def _find_event_only_new_ids(
    spark: SparkSession,
    spadl_table: str,
    results_table: str,
    provider: str,
) -> list[str]:
    """Find unprocessed event-only matches (Spark-native LEFT ANTI JOIN).

    spadl_actions(provider) \\ results(provider) — no full-table scan or
    driver-side set difference.
    """
    from pyspark.sql import functions as F  # noqa: N812

    source_df = (
        spark.table(spadl_table)
        .filter(F.col("data_source") == provider)
        .select(F.col("match_id_native").cast("string").alias("_join_id"))
        .distinct()
    )
    results_df = (
        spark.table(results_table)
        .filter(F.col("data_source") == provider)
        .select(F.col("match_id").cast("string").alias("_join_id"))
        .distinct()
    )
    new_df = source_df.join(results_df, "_join_id", "left_anti")
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

    Chunk sizes are a WALL-CLOCK knob here, not a memory knob — see the
    memory note below. They are sized against the 1800 s
    ``compute_action_context_iteration`` timeout and the 48 KB Databricks
    task-value cap:

      - IDSSE (1 half / iter) — each half is ~1.5 M tracking rows; processed
        one half per iteration (period-level). Handled outside ``chunk_sizes``
        via ``_find_idsse_new_period_pairs``.
      - Tracking providers — full 20-step enrichment chain via applyInPandas.
        ``tracking_context`` precedent (chunk_size=2 with the same iteration
        timeout) implies ≤900 s/match; chunk_size=5 gives a 360 s per-match
        budget (≥2x safety). Gradient Sports holds 4.21 M tracking rows/match
        avg (4.4x SkillCorner) so it gets a more conservative chunk_size=4.
      - StatsBomb (event-only mixed with 9.3 % SB360 freeze-frame tier) —
        chunk_size=100 caps worst-case 360-tier work per iteration at
        ~9 x 60 s = ~540 s out of the 1800 s budget.
      - Wyscout (pure event-only, ~5 s/match) — chunk_size=200 matches the
        ``spadl_vaep`` proven pattern for event-only providers.

    MEMORY NOTE — why these run wider than off_ball_xt / pitch_control's
    ``_MATCHES_PER_CHUNK = 2``:
        Those pipelines fold ALL matches in a chunk into a SINGLE
        ``groupBy(match_id, frame_batch_id).applyInPandas`` pass, so their
        concurrent executor-group count scales with matches-per-chunk and the
        ``2`` is derived from the 800 MB UDF executor budget.

        action_context instead loops ``for match_id in ids:`` in ``main()`` and
        runs a SEPARATE applyInPandas pass per match, freeing memory between
        matches. At any instant one iteration holds only one match's frame-batch
        groups. Per-group memory is bounded by ``_FRAME_BATCH_SIZE`` (250 frames,
        ~200 MB — the executor-OOM mitigation inherited from tracking_context,
        commits 0542a8b + b12fb60), and concurrent executor pressure is bounded
        by the for_each ``concurrency`` (8), NOT by chunk_size. Raising
        ``concurrency`` above 8 is therefore the lever that increases shared
        executor pressure — chunk_size is not. Keep that invariant if this guard
        is ever refactored to fold a chunk into one pass: at that point
        chunk_size WOULD become memory-bound and the ``2`` ceiling applies.
    """

    workflow_id = "wf-action-context"
    chunk_sizes: ClassVar[dict[str, int]] = {
        "metrica": 5,
        "skillcorner": 5,
        "gradientsports": 4,  # 4.21 M tracking rows/match avg — more conservative than metrica/skillcorner
        "statsbomb": 100,  # 9.3 % of SB matches use the heavier SB360 tier; 100 caps 360-tier work
        "wyscout": 200,  # pure event-only — matches spadl_vaep proven pattern
    }

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check for unprocessed matches across all 6 providers."""
        from ingestion.guards import ensure_table

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        spadl_table = f"{catalog}.bronze.spadl_actions"
        ensure_table(spark, results_table, _ACTION_CONTEXT_DDL)

        # ── IDSSE: period-level discovery ──
        idsse_pairs = _find_idsse_new_period_pairs(
            spark,
            f"{catalog}.bronze.idsse_tracking",
            spadl_table,
            results_table,
        )
        idsse_half_chunks: list[str] = [f"idsse:{mid}:{period}" for mid, period in idsse_pairs]

        # ── Other tracking providers: match-level discovery ──
        metrica_ids = _find_tracking_new_ids(
            spark, f"{catalog}.bronze.metrica_tracking", spadl_table, results_table, "metrica"
        )
        skillcorner_ids = _find_tracking_new_ids(
            spark, f"{catalog}.bronze.skillcorner_tracking", spadl_table, results_table, "skillcorner"
        )
        gradientsports_ids = _find_tracking_new_ids(
            spark, f"{catalog}.bronze.gradientsports_tracking", spadl_table, results_table, "gradientsports"
        )

        # ── Event-only providers: Spark-native anti-join ──
        statsbomb_ids = _find_event_only_new_ids(spark, spadl_table, results_table, "statsbomb")
        wyscout_ids = _find_event_only_new_ids(spark, spadl_table, results_table, "wyscout")

        total = (
            len(idsse_half_chunks)
            + len(metrica_ids)
            + len(skillcorner_ids)
            + len(gradientsports_ids)
            + len(statsbomb_ids)
            + len(wyscout_ids)
        )
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Build chunks
        chunks: list[list[str]] = []
        for chunk_str in idsse_half_chunks:
            chunks.append([chunk_str])

        for prov, ids in [
            ("metrica", metrica_ids),
            ("skillcorner", skillcorner_ids),
            ("gradientsports", gradientsports_ids),
            ("statsbomb", statsbomb_ids),
            ("wyscout", wyscout_ids),
        ]:
            cs = self.chunk_sizes.get(prov, 2)
            for i in range(0, len(ids), cs):
                batch = ids[i : i + cs]
                chunks.append([f"{prov}:{','.join(batch)}"])

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            chunks=chunks,
        )


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


def _write_action_chunks_task_value(
    chunks_for_inputs: list[str],
    task_logger: logging.Logger,
) -> None:
    """Write discovered chunks as a Databricks task value."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        spark = get_spark_session()
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="action_context_chunks", value=chunks_for_inputs)
        task_logger.info("Wrote task value 'action_context_chunks' (%d chunks)", len(chunks_for_inputs))
    except (ImportError, AttributeError, RuntimeError) as exc:
        task_logger.warning("Task values not available (standalone mode) -- %s", exc)


# ── Entry points ──────────────────────────────────────────────────────


def main_preflight() -> None:
    """CLI entry point for action context preflight.

    Runs the skip guard, partitions discovered matches into fan-out chunks,
    writes chunk list as a Databricks task value for the downstream for_each_task.

    xT grid loading is NOT done here -- each downstream iteration reads the
    pre-computed global grid from bronze.expected_threat_grids independently
    (~192 rows, instant). This keeps the preflight O(1) w.r.t. data volume.
    """
    args = parse_ingestion_args("Preflight: discover unprocessed action context matches and emit chunks")
    task_logger = configure_logging("action_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    task_logger.info(
        "Action context preflight: %d missing matches across %d chunks",
        fr.count,
        len(chunks_for_inputs),
    )

    _write_action_chunks_task_value(chunks_for_inputs, task_logger)


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
    import time as _time

    args = parse_ingestion_args(
        "Compute action context features",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2"})],
    )
    task_logger = configure_logging("action_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

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
            )
        elif provider == "statsbomb":
            written = _process_statsbomb_match(spark, catalog, schema, match_id, task_logger)
        elif provider == "wyscout":
            written = _process_event_only_match(spark, catalog, schema, "wyscout", match_id, task_logger)
        else:
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


# ── Provider-specific processing ──────────────────────────────────────


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
) -> int:
    """Process a single tracking-provider match via applyInPandas."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )
    from ingestion.utils import write_delta_table

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
                F.col("team_id").cast("string").alias("team"),
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

    if trk_sdf.limit(1).count() == 0:
        task_logger.warning("No tracking data for %s match %s", provider, match_id)
        return 0

    # ── Read SPADL actions ──
    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for %s match %s", provider, match_id)
        return 0
    actions_records: list[dict[str, Any]] = actions_pdf.to_dict("records")  # type: ignore[assignment]

    # ── Resolve match-level metadata (driver scalars) ──
    home_start_left = True
    home_team_start_left_extratime: bool | None = None  # silly-kicks 4.0+ ET guard input
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None

    if provider == "idsse":
        from ingestion.spadl_adapter import (
            adapt_idsse_events_for_silly_kicks,
            derive_idsse_home_team_start_left,
            derive_idsse_home_team_start_left_extratime,
        )

        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
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
        events_pdf = spark.table(gs_events_tbl).filter(F.col("match_id") == match_id).toPandas()
        gs_meta = extract_gradientsports_match_metadata(events_pdf)
        home_team_id = str(gs_meta["home_team_id"])
        home_start_left = gs_meta["home_team_start_left"]
        # GS bronze carries stadiumMetadata.homeTeamStartLeftExtraTime already.
        home_team_start_left_extratime = gs_meta["home_team_start_left_extratime"]
        del events_pdf

        # Build team_side -> team_id mapping and jersey -> player_id from roster
        gs_roster_tbl = f"{catalog}.bronze.gradientsports_roster"
        roster_pdf = spark.table(gs_roster_tbl).filter(F.col("match_id") == match_id).toPandas()
        if not roster_pdf.empty:
            # Derive away_team_id from roster (the team that is not home)
            all_team_ids = roster_pdf["team_id"].dropna().unique()
            home_tid = str(gs_meta["home_team_id"])
            away_tids = [str(t) for t in all_team_ids if str(t) != home_tid]
            away_team_id = away_tids[0] if away_tids else home_tid

            gs_team_side_to_id = {"home": home_tid, "away": away_team_id}

            # Build (team_side, jersey_num) -> player_id mapping
            gs_jersey_to_player_id = {}
            for _, row in roster_pdf.iterrows():
                tid = str(row.get("team_id", ""))
                side = "home" if tid == home_tid else "away"
                jersey = str(row.get("jersey_number", ""))
                pid = str(row.get("player_id", ""))
                if jersey and pid:
                    gs_jersey_to_player_id[(side, jersey)] = pid

            # GK player IDs
            if "position" in roster_pdf.columns:
                gk_rows = roster_pdf[roster_pdf["position"].str.upper() == "GK"]
            else:
                gk_rows = roster_pdf.iloc[0:0]
            gs_gk_player_ids = [str(r["player_id"]) for _, r in gk_rows.iterrows()]

        del roster_pdf

    # ── Frame batching + UDF dispatch ──
    # Use "frame" for most providers; GradientSports uses "frame_num"
    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    trk_sdf = trk_sdf.withColumn(
        "frame_batch_id",
        F.floor(F.col(frame_col) / F.lit(_FRAME_BATCH_SIZE)),
    )

    # GradientSports uses "period_elapsed_time" as timestamp, rename for consistency
    if provider == "gradientsports":
        trk_sdf = trk_sdf.withColumnRenamed("period_elapsed_time", "timestamp")

    # Spark LongAccumulator: each UDF call increments by 1 on success. Driver
    # reads `.value` from the heartbeat thread to surface progress mid-action.
    batches_counter = spark.sparkContext.accumulator(0)

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
        batches_counter=batches_counter,
    )

    # GradientSports uses "period" (not "period_id") in bronze
    result_sdf = trk_sdf.groupBy("match_id", "period", "frame_batch_id").applyInPandas(
        udf_fn,
        schema=_get_result_schema(),
    )

    if period_filter is not None:
        rw = f"match_id = '{match_id}' AND period_id = {period_filter}"
    else:
        rw = f"match_id = '{match_id}'"

    # Heartbeat: log batches_completed every 30s during the long write_delta_table
    # action so the operator sees progress (otherwise silent for the 10+ min duration).
    with _BatchHeartbeat(
        read_progress=lambda: batches_counter.value,
        interval_s=30.0,
        logger=task_logger,
        label="ac1_batches_completed",
    ):
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=rw,
            logger=task_logger,
        )
    del actions_pdf, actions_records
    return written


def _process_statsbomb_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    match_id: str,
    task_logger: logging.Logger,
) -> int:
    """Process a StatsBomb match — SB360 tier (with freeze-frames) or event-only."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    # Read SPADL actions
    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "statsbomb"))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for statsbomb match %s", match_id)
        return 0

    # Check for SB360 freeze-frame data
    from ingestion.utils import tolerate_missing_table

    has_360 = False
    with tolerate_missing_table(task_logger, "statsbomb_360 table missing"):
        count_360 = (
            spark.table(f"{catalog}.bronze.statsbomb_360").filter(F.col("match_id") == match_id).limit(1).count()
        )
        has_360 = count_360 > 0

    if has_360:
        task_logger.info("StatsBomb match %s has 360 data — using SB360 tier", match_id)
        result_pdf = _run_sb360_enrichment(
            spark,
            catalog,
            actions_pdf,
            match_id,
            task_logger,
        )
    else:
        task_logger.info("StatsBomb match %s — event-only tier", match_id)
        result_pdf = _enrich_event_only_match(actions_pdf)

    out_pdf = _build_output(result_pdf, match_id_native=match_id, data_source="statsbomb")

    # Convert to Spark DataFrame and write
    out_sdf = spark.createDataFrame(out_pdf)
    written = write_delta_table(
        out_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id = '{match_id}'",
        logger=task_logger,
        row_count=len(out_pdf),
    )
    return written


def _run_sb360_enrichment(
    spark: SparkSession,
    catalog: str,
    actions_pdf: pd.DataFrame,
    match_id: str,
    task_logger: logging.Logger,
) -> pd.DataFrame:
    """Run SB360 enrichment — converts freeze-frames to synthetic tracking then enriches."""
    import pandas as pd
    from pyspark.sql import functions as F  # noqa: N812

    # Read SB360 freeze-frame data
    sb360_pdf = spark.table(f"{catalog}.bronze.statsbomb_360").filter(F.col("match_id") == match_id).toPandas()

    if sb360_pdf.empty:
        task_logger.warning("SB360 data empty for match %s — falling back to event-only", match_id)
        return _enrich_event_only_match(actions_pdf)

    # Map event_uuid → action_id via original_event_id in SPADL actions
    # SB360.id = event_uuid; spadl_actions.original_event_id = event_uuid
    _event_ids = actions_pdf["original_event_id"].dropna()
    _action_ids = actions_pdf.loc[_event_ids.index, "action_id"]
    event_to_action = dict(zip(_event_ids, _action_ids, strict=True))

    # Pre-build indexed lookups — avoids O(n*m) boolean mask filtering in loop.
    action_to_team: dict[Any, str] = dict(
        zip(actions_pdf["action_id"], actions_pdf["team_id"].astype(str), strict=False)
    )
    all_teams = [str(t) for t in actions_pdf["team_id"].dropna().unique()]

    # Build snapshot format: action_id, team_id, is_goalkeeper, x, y
    # SB360 has: id (event_uuid), teammate (bool), actor (bool), keeper (bool), location (JSON [x,y])
    import json

    snapshots: list[dict[str, Any]] = []
    for _, row in sb360_pdf.iterrows():
        event_uuid = str(row.get("id", ""))
        action_id = event_to_action.get(event_uuid)
        if action_id is None:
            continue

        # Resolve team_id from teammate flag + acting team (dict lookup, O(1))
        acting_team_id = action_to_team.get(action_id)
        if acting_team_id is None:
            continue

        opponent_teams = [t for t in all_teams if t != acting_team_id]
        opponent_team_id = opponent_teams[0] if opponent_teams else acting_team_id

        is_teammate = bool(row.get("teammate", False))
        team_id = acting_team_id if is_teammate else opponent_team_id
        is_gk = bool(row.get("keeper", False))

        # Parse location
        loc = row.get("location")
        if loc is None:
            continue
        if isinstance(loc, str):
            try:
                loc = json.loads(loc)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(loc, (list, tuple)) or len(loc) < 2:
            continue

        snapshots.append(
            {
                "action_id": int(action_id),
                "team_id": team_id,
                "is_goalkeeper": is_gk,
                "x": float(loc[0]),
                "y": float(loc[1]),
            }
        )

    if not snapshots:
        task_logger.warning("No valid snapshots for match %s — falling back to event-only", match_id)
        return _enrich_event_only_match(actions_pdf)

    freeze_frames = pd.DataFrame(snapshots)

    # Derive home_team_id for SB360 (use first team in actions as home approximation)
    unique_teams = actions_pdf["team_id"].dropna().unique()
    home_team_id = str(unique_teams[0]) if len(unique_teams) > 0 else "unknown"

    return _enrich_sb360_match(actions_pdf, freeze_frames, home_team_id)


def _process_event_only_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    provider: str,
    match_id: str,
    task_logger: logging.Logger,
) -> int:
    """Process a pure event-only match (no tracking data)."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for %s match %s", provider, match_id)
        return 0

    result_pdf = _enrich_event_only_match(actions_pdf)
    out_pdf = _build_output(result_pdf, match_id_native=match_id, data_source=provider)

    out_sdf = spark.createDataFrame(out_pdf)
    written = write_delta_table(
        out_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id = '{match_id}'",
        logger=task_logger,
        row_count=len(out_pdf),
    )
    return written


if __name__ == "__main__":
    main()
