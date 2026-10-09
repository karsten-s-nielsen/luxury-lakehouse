"""Spark dispatch for the haloed frame-window build seam (ADR-089).

Lifts the pure ``analytics.action_context.frame_windows`` core to Spark: ``assign_frame_windows`` adds
``_window_id`` + ``_is_halo`` (dense frame-ordinal windows + halo replication) at the Spark level, and
``windowed_build_sdf`` dispatches a per-``(match, period, window_id)`` ``mapInPandas`` that builds each
``(core+halo)`` window via the sk build and trims the halo (``build_windowed``). The union of windows ==
the whole-unit built frame (neutrality proven in ``tests/action_context/test_frame_windows.py``).

Pure cores (window assignment, halo trim) are unit-tested on fixtures; THIS module's Spark plumbing is
validated by the Docker two-stage test (``tests/tracking_marts/test_windowed_stage1.py``) + the live drain,
matching ``tracking_marts_dispatch`` / ``xg_shot_scorer`` posture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession


def _frame_col(provider: str) -> str:
    return "frame_num" if provider == "gradientsports" else "frame"


def assign_frame_windows(
    raw_sdf: SparkDataFrame,
    provider: str,
    *,
    target_window_frames: int,
) -> SparkDataFrame:
    """Add ``_window_id`` (dense frame-ordinal // target) + ``_is_halo`` to a unit's raw tracking rows.

    Mirrors the pure ``frame_windows.assign_window_ids``: each frame's rows appear once as CORE in their
    own window and are REPLICATED (``_is_halo=True``) into a neighbour window when within
    ``halo_frames(provider)`` of that neighbour's boundary. Keys are ``(match_id, period)`` — windows are
    within a period. The returned DF is LONGER than the input (the halo duplicates).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F  # noqa: N812

    from analytics.action_context.frame_windows import halo_frames, provider_hz

    halo = halo_frames(provider, provider_hz(provider))
    frame_col = _frame_col(provider)
    unit = Window.partitionBy("match_id", "period").orderBy(frame_col)

    # dense_rank over DISTINCT frames (ties share a rank) → 0-based frame ordinal; a frame = ~22 rows.
    ranked = raw_sdf.withColumn("_ord", F.dense_rank().over(unit) - 1)
    max_ord = Window.partitionBy("match_id", "period")
    ranked = ranked.withColumn("_max_ord", F.max("_ord").over(max_ord))
    tw = F.lit(int(target_window_frames))
    ranked = (
        ranked.withColumn("_window_id", (F.col("_ord") / tw).cast("int"))
        .withColumn("_pos", F.col("_ord") - F.col("_window_id") * tw)  # position within the core window
        .withColumn("_last_window", (F.col("_max_ord") / tw).cast("int"))
    )

    core = ranked.withColumn("_is_halo", F.lit(False))
    # left-halo: first `halo` ordinals of window w (w>=1) → feed w-1's right edge.
    left = (
        ranked.where((F.col("_pos") < F.lit(halo)) & (F.col("_window_id") >= 1))
        .withColumn("_window_id", F.col("_window_id") - 1)
        .withColumn("_is_halo", F.lit(True))
    )
    # right-halo: last `halo` ordinals of window w (w < last) → feed w+1's left edge.
    dist_to_end = (F.col("_window_id") + 1) * tw - 1 - F.col("_ord")
    right = (
        ranked.where((dist_to_end < F.lit(halo)) & (F.col("_window_id") < F.col("_last_window")))
        .withColumn("_window_id", F.col("_window_id") + 1)
        .withColumn("_is_halo", F.lit(True))
    )
    helper = ("_ord", "_max_ord", "_pos", "_last_window")
    return core.unionByName(left).unionByName(right).drop(*helper)


def windowed_build_sdf(
    spark: SparkSession,
    raw_sdf: SparkDataFrame,
    provider: str,
    build_fn: Any,  # Callable[[pd.DataFrame], pd.DataFrame] — the sk build over raw frames
    built_schema: Any,  # pyspark StructType of the BUILT frame
    *,
    target_window_frames: int,
    only_window_id: int | None = None,
) -> SparkDataFrame:
    """Lazy built-frame DF: window the raw frames (halo), build each window, trim the halo, union.

    Replaces a whole-unit ``repartition(match_id,period).mapInPandas(build_fn)`` — bounds the worker peak
    by ``target_window_frames``, not unit size. The per-group UDF runs ``build_windowed`` (build on
    core+halo → drop halo rows); the result's schema is ``built_schema`` (halo trimmed, so same columns).

    ``only_window_id`` (ADR-087 Probe M): restrict the build to the single ``_window_id`` (its core+halo
    rows) — used by the in-preflight memory-fit canary to build the GLOBAL densest window ALONE, bounding
    the probe to O(one window). ``None`` (default) builds every window, the production drain behaviour.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from analytics.action_context.frame_windows import _WINDOW_COL, build_windowed
    from ingestion.action_context import _UDF_SHUFFLE_PARTITIONS, _make_streaming_group_mapper

    frame_col = _frame_col(provider)
    assigned = assign_frame_windows(raw_sdf, provider, target_window_frames=target_window_frames)
    if only_window_id is not None:
        assigned = assigned.where(F.col(_WINDOW_COL) == int(only_window_id))
    keys = ["match_id", "period", _WINDOW_COL]

    def _udf(window_rows: pd.DataFrame) -> pd.DataFrame:
        # window_rows carries _window_id + _is_halo; build_windowed drops BOTH before build_fn + trims halo.
        return build_windowed(window_rows, build_fn, frame_col=frame_col)

    sort_cols = [*keys, frame_col] if frame_col in assigned.columns else keys
    return (
        assigned.repartition(_UDF_SHUFFLE_PARTITIONS, *keys)
        .sortWithinPartitions(*sort_cols)
        .mapInPandas(_make_streaming_group_mapper(_udf, keys), schema=built_schema)
    )


__all__ = ["assign_frame_windows", "windowed_build_sdf"]
