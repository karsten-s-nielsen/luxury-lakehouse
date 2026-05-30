"""Gradient Sports tracking ingestion — tracking artifact to bronze.

Parses the tracking artifact from the pining-for-the-data API into
narrow format (one row per player per frame) and writes to
bronze.gradientsports_tracking.

Artifact format: ``tracking.jsonl.bz2`` — bz2-compressed newline-delimited JSON.
Each line is one frame with ``homePlayers``, ``awayPlayers``, ``balls``,
and their smoothed counterparts, plus frame-level event annotations.

Coordinate system: center-origin meters (preserved as-is in bronze).
The silly-kicks ``convert_to_frames`` converter handles the final transform.

Memory model: the streaming path (``stream_tracking_to_parquet``) never holds
more than one batch of frames (~10K frames ≈ 50-100 MB) in memory at a time.
This allows 8 concurrent for_each_task iterations on the 16 GB serverless driver.
"""

from __future__ import annotations

import bz2
import json
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.utils import ensure_volume_directory, validate_dataframe, write_delta_table

if TYPE_CHECKING:
    import requests
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Frames per Parquet row-group batch.  ~10K frames x ~24 entities/frame =
# ~240K rows = 50-100 MB peak per batch -- safe for 8 concurrent iterations
# on a 16 GB serverless driver.
_BATCH_FRAMES = 10_000

# Arrow schema for the narrow tracking table.  Defined once to guarantee
# identical column order and types across all batches and matches.
_ARROW_SCHEMA = pa.schema(
    [
        ("match_id", pa.string()),
        ("game_ref_id", pa.float64()),
        ("frame_num", pa.float64()),
        ("period", pa.float64()),
        ("period_elapsed_time", pa.float64()),
        ("period_game_clock_time", pa.float64()),
        ("video_time_ms", pa.float64()),
        ("version", pa.string()),
        ("generated_time", pa.string()),
        ("smoothed_time", pa.string()),
        ("game_event_id", pa.float64()),
        ("possession_event_id", pa.float64()),
        ("_game_event_json", pa.string()),
        ("_possession_event_json", pa.string()),
        ("team_side", pa.string()),
        ("is_ball", pa.bool_()),
        ("jersey_num", pa.string()),
        ("confidence", pa.string()),
        ("visibility", pa.string()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
        ("x_smoothed", pa.float64()),
        ("y_smoothed", pa.float64()),
        ("z_smoothed", pa.float64()),
        ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def _iter_unique_frames(
    frames_iter: Iterator[dict],
    *,
    log: logging.Logger | None = None,
) -> Iterator[dict]:
    """Keep-first dedup wrapper for the GS frame stream.

    The GS provider ships content-divergent duplicate ``(period, frameNum)``
    records — observed up to 16 copies of a single frame in match 10502
    (silly-kicks PR-S72 heads-up, 2026-05-30). Each duplicated frame fans
    out to 23 duplicate narrow-format rows (22 players + 1 ball), which:

    1. crashes silly-kicks ``_pressure_bekkers`` on a 3-D ball_pos broadcast,
    2. silently inflates ~15 other downstream features (pitch-control, DAS,
       team-shape, GK-influence, ...) on the affected frames — wrong values,
       no error.

    Dedup at the bronze-writer boundary is the long-term home (every
    downstream consumer benefits without per-feature defense-in-depth).
    silly-kicks 4.0.1 ships defense-in-depth for the bekkers crash; this
    helper closes the silent-inflation gap.

    Frames missing ``period`` or ``frameNum`` are yielded as-is (caller's
    schema problem, not a dedup concern).
    """
    seen: set[tuple[float, float]] = set()
    dup_count = 0
    for frame in frames_iter:
        period = frame.get("period")
        frame_num = frame.get("frameNum")
        if period is None or frame_num is None:
            yield frame
            continue
        try:
            key = (float(period), float(frame_num))
        except (TypeError, ValueError):
            yield frame
            continue
        if key in seen:
            dup_count += 1
            continue
        seen.add(key)
        yield frame
    if dup_count > 0 and log is not None:
        log.warning(
            "GS bronze dedup: dropped %d duplicate (period, frameNum) record(s) out of %d unique frame(s) kept",
            dup_count,
            len(seen),
        )


def _flatten_frame(frame: dict, match_id: str, ingested_at: datetime) -> list[dict]:
    """Flatten one tracking frame into narrow-format row dicts.

    Shared by both the in-memory ``parse_tracking`` (tests) and the
    streaming ``stream_tracking_to_parquet`` (production) paths.
    """
    game_event_json = json.dumps(frame["game_event"]) if frame.get("game_event") else None
    possession_event_json = json.dumps(frame["possession_event"]) if frame.get("possession_event") else None

    base = {
        "match_id": match_id,
        "game_ref_id": float(frame["gameRefId"]) if frame.get("gameRefId") is not None else None,
        "frame_num": float(frame["frameNum"]) if frame.get("frameNum") is not None else None,
        "period": float(frame["period"]) if frame.get("period") is not None else None,
        "period_elapsed_time": frame.get("periodElapsedTime"),
        "period_game_clock_time": frame.get("periodGameClockTime"),
        "video_time_ms": frame.get("videoTimeMs"),
        "version": frame.get("version"),
        "generated_time": frame.get("generatedTime"),
        "smoothed_time": frame.get("smoothedTime"),
        "game_event_id": frame.get("game_event_id"),
        "possession_event_id": frame.get("possession_event_id"),
        "_game_event_json": game_event_json,
        "_possession_event_json": possession_event_json,
        "_ingested_at": ingested_at,
    }

    rows: list[dict] = []

    # Home players (raw + smoothed)
    for player in frame.get("homePlayers") or []:
        smoothed = _find_smoothed(frame.get("homePlayersSmoothed") or [], player)
        rows.append(
            {
                **base,
                "team_side": "home",
                "is_ball": False,
                "jersey_num": player.get("jerseyNum"),
                "confidence": player.get("confidence"),
                "visibility": player.get("visibility"),
                "x": player.get("x"),
                "y": player.get("y"),
                "z": None,
                "x_smoothed": smoothed.get("x") if smoothed else None,
                "y_smoothed": smoothed.get("y") if smoothed else None,
                "z_smoothed": None,
            }
        )

    # Away players (raw + smoothed)
    for player in frame.get("awayPlayers") or []:
        smoothed = _find_smoothed(frame.get("awayPlayersSmoothed") or [], player)
        rows.append(
            {
                **base,
                "team_side": "away",
                "is_ball": False,
                "jersey_num": player.get("jerseyNum"),
                "confidence": player.get("confidence"),
                "visibility": player.get("visibility"),
                "x": player.get("x"),
                "y": player.get("y"),
                "z": None,
                "x_smoothed": smoothed.get("x") if smoothed else None,
                "y_smoothed": smoothed.get("y") if smoothed else None,
                "z_smoothed": None,
            }
        )

    # Ball(s)
    for ball in frame.get("balls") or []:
        ball_smoothed = frame.get("ballsSmoothed")
        if isinstance(ball_smoothed, list) and ball_smoothed:
            ball_smoothed = ball_smoothed[0]
        elif not isinstance(ball_smoothed, dict):
            ball_smoothed = None
        rows.append(
            {
                **base,
                "team_side": None,
                "is_ball": True,
                "jersey_num": None,
                "confidence": None,
                "visibility": ball.get("visibility"),
                "x": ball.get("x"),
                "y": ball.get("y"),
                "z": ball.get("z"),
                "x_smoothed": ball_smoothed.get("x") if ball_smoothed else None,
                "y_smoothed": ball_smoothed.get("y") if ball_smoothed else None,
                "z_smoothed": ball_smoothed.get("z") if ball_smoothed else None,
            }
        )

    return rows


def _iter_frames_from_bz2_stream(
    response: requests.Response,
    chunk_size: int = 256 * 1024,
) -> Iterator[dict]:
    """Yield parsed frame dicts from a streaming bz2-compressed JSONL response.

    Decompresses incrementally — never holds the full decompressed payload.
    """
    decompressor = bz2.BZ2Decompressor()
    buf = b""

    for raw_chunk in response.iter_content(chunk_size=chunk_size):
        if not raw_chunk:
            continue
        try:
            decompressed = decompressor.decompress(raw_chunk)
        except EOFError:
            break
        buf += decompressed

        # Extract complete lines from buffer
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line = line.strip()
            if line:
                yield json.loads(line)

    # Final line (no trailing newline)
    if buf.strip():
        yield json.loads(buf)


def _rows_to_arrow_batch(rows: list[dict]) -> pa.RecordBatch:
    """Convert flat row dicts to an Arrow RecordBatch with the canonical schema."""
    # Build column arrays from row dicts — guarantees schema alignment
    arrays = []
    for field in _ARROW_SCHEMA:
        col_name = field.name
        values = [r.get(col_name) for r in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=_ARROW_SCHEMA)


def stream_tracking_to_parquet(
    response: requests.Response,
    *,
    match_id: str,
    parquet_path: str,
    log: logging.Logger,
) -> int:
    """Stream-parse tracking data and write directly to a Parquet file.

    Incrementally decompresses bz2, flattens frames in batches of
    ``_BATCH_FRAMES``, and appends each batch as a row group to a single
    Parquet file via ``pyarrow.parquet.ParquetWriter``.

    Returns:
        Total number of rows written.
    """
    ingested_at = datetime.now(timezone.utc)
    total_rows = 0
    frame_count = 0
    batch_rows: list[dict] = []

    writer = pq.ParquetWriter(parquet_path, _ARROW_SCHEMA)
    try:
        # Dedup keep-first on (period, frameNum) — GS provider ships duplicates;
        # see _iter_unique_frames docstring.
        for frame in _iter_unique_frames(_iter_frames_from_bz2_stream(response), log=log):
            batch_rows.extend(_flatten_frame(frame, match_id, ingested_at))
            frame_count += 1

            if frame_count % _BATCH_FRAMES == 0:
                batch = _rows_to_arrow_batch(batch_rows)
                writer.write_batch(batch)
                total_rows += len(batch_rows)
                log.info(
                    "Flushed batch: %d frames, %d rows (cumulative: %d rows)",
                    _BATCH_FRAMES,
                    len(batch_rows),
                    total_rows,
                )
                batch_rows = []

        # Flush remaining rows
        if batch_rows:
            batch = _rows_to_arrow_batch(batch_rows)
            writer.write_batch(batch)
            total_rows += len(batch_rows)
    finally:
        writer.close()

    log.info("Streamed %d frames (%d rows) to %s", frame_count, total_rows, parquet_path)
    return total_rows


def parse_tracking(source: bytes | str | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports tracking data into narrow-format DataFrame.

    In-memory path used by unit tests. Production uses
    ``stream_tracking_to_parquet`` which writes directly to Parquet
    without materializing the full DataFrame.

    Args:
        source: Raw tracking data — bz2-compressed bytes (from API),
            or pre-parsed list of frame dicts (for testing).
        match_id: Native match ID.

    Returns:
        DataFrame in narrow format (one row per player/ball per frame).
    """
    if isinstance(source, bytes):
        decompressed = bz2.decompress(source)
        lines = decompressed.decode("utf-8").strip().split("\n")
        frames_list = [json.loads(line) for line in lines]
    elif isinstance(source, str):
        data = json.loads(source)
        frames_list = data if isinstance(data, list) else [data]
    else:
        frames_list = source

    ingested_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    # Dedup keep-first on (period, frameNum) — symmetry with stream_tracking_to_parquet.
    # In-memory path: no logger; dups silently dropped (tests assert the count).
    for frame in _iter_unique_frames(iter(frames_list)):
        rows.extend(_flatten_frame(frame, match_id, ingested_at))

    df = pd.DataFrame(rows)

    # Widen all integer columns to float64 so Spark always infers DOUBLE.
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype("float64")

    df["_ingested_at"] = ingested_at
    return df


def _find_smoothed(
    smoothed_list: list[dict],
    raw_player: dict,
) -> dict | None:
    """Match a smoothed entry to its raw counterpart by jerseyNum."""
    jersey = raw_player.get("jerseyNum")
    if jersey is None:
        return None
    for s in smoothed_list:
        if s.get("jerseyNum") == jersey:
            return s
    return None


def _staging_path(catalog: str, schema: str, match_id: str) -> str:
    """UC Volume staging path for Parquet intermediates.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name (flows from CLI args, not hardcoded).
        match_id: Gradient Sports match ID.
    """
    return f"/Volumes/{catalog}/{schema}/_staging/gradientsports_tracking/{match_id}.parquet"


def write_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
    *,
    staging_parquet: str | None = None,
    df: pd.DataFrame | None = None,
) -> int:
    """Write tracking data to bronze.gradientsports_tracking.

    Two modes:
      - **Streaming (production)**: ``staging_parquet`` points to a Parquet
        file already written by ``stream_tracking_to_parquet``.
      - **DataFrame (legacy/test)**: ``df`` is a pandas DataFrame that gets
        staged to Parquet first.

    Exactly one of ``staging_parquet`` or ``df`` must be provided.
    """
    import os

    if staging_parquet is not None and df is not None:
        msg = "Provide staging_parquet or df, not both"
        raise ValueError(msg)
    if staging_parquet is None and df is None:
        msg = "Provide staging_parquet or df"
        raise ValueError(msg)

    if df is not None:
        # Legacy/test path: stage DataFrame to Parquet
        staging_parquet = _staging_path(catalog, schema, match_id)
        ensure_volume_directory(os.path.dirname(staging_parquet))
        df.to_parquet(staging_parquet, index=False)
        logger.info("Staged %d tracking rows to %s", len(df), staging_parquet)

    if staging_parquet is None:  # unreachable — narrowed by df-branch or caller contract
        msg = "staging_parquet must be set by this point"
        raise ValueError(msg)
    try:
        sdf = spark.read.parquet(staging_parquet)
        row_count = validate_dataframe(
            sdf,
            ["match_id", "frame_num", "period"],
            "gradientsports_tracking",
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            "gradientsports_tracking",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )
    finally:
        try:
            os.remove(staging_parquet)
            logger.debug("Cleaned up staging file %s", staging_parquet)
        except OSError:
            logger.debug("Staging cleanup failed for %s (will be overwritten on next run)", staging_parquet)

    return row_count
