"""SkillCorner tracking ingestion -- tracking_extrapolated.jsonl to bronze.

Streams the JSONL artifact line-by-line, reshapes to narrow format
(one row per player per frame), normalizes timestamp from string to
float seconds, and renames is_detected -> is_visible.

Bronze table: bronze.skillcorner_tracking
Coordinate system: center-origin meters (preserved as-is).
"""

from __future__ import annotations

import gzip
import itertools
import json
import logging
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# A-League broadcast tracking is a fixed 10 fps. RM full-format tracking is derived
# per-match from cadence (see derive_frame_rate) — this constant is the A-League path ONLY.
_FRAME_RATE = 10

# RM frame-rate: derive from 1/median(Δt) per period, then snap to the nearest plausible
# rate within tolerance (raw won't be exactly nominal: 0.04004 s → 24.97, not 25.0).
_ALLOWED_FRAME_RATES = (10, 25, 30)
_FRAME_RATE_TOLERANCE = 0.05  # ±5%

_TIMESTAMP_PATTERN = re.compile(r"^(\d+):(\d+):(\d+(?:\.\d+)?)$")

_TRACKING_DTYPE_OVERRIDES: dict[str, str] = {
    "period": "Int64",
    "frame": "Int64",
    "timestamp": "Float64",
    "player_id": "Int64",
    "x": "Float64",
    "y": "Float64",
    "ball_x": "Float64",
    "ball_y": "Float64",
    "ball_z": "Float64",
    "frame_rate": "Int64",
    "is_visible": "boolean",
    "ball_is_detected": "boolean",
}


def _parse_timestamp(ts_str: str) -> float:
    """Parse 'HH:MM:SS.ms' to float seconds.

    Examples:
        '00:12:34.90' -> 754.9
        '01:30:00.00' -> 5400.0
    """
    m = _TIMESTAMP_PATTERN.match(ts_str)
    if m is None:
        raise ValueError(f"Cannot parse timestamp: {ts_str!r}")
    hours = int(m.group(1))
    minutes = int(m.group(2))
    seconds = float(m.group(3))
    return hours * 3600.0 + minutes * 60.0 + seconds


def _frame_to_rows(frame_obj: dict, match_id: str) -> list[dict[str, object]]:
    """Reshape ONE tracking frame to narrow per-player rows (WITHOUT ``frame_rate``).

    Shared by the A-League JSONL reader and the RM gzip-JSON reader — the frame model is
    identical (``frame``/``period``/``timestamp``/``ball_data``/``player_data``); only the
    serialization differs (RM frames also carry ``possession``/``image_corners_projection``,
    ignored). The caller assigns ``frame_rate`` on the assembled DataFrame (constant 10 for
    A-League, per-match-derived for RM).
    """
    frame_num = frame_obj["frame"]
    period = frame_obj["period"]
    ts_raw = frame_obj.get("timestamp")
    timestamp = _parse_timestamp(ts_raw) if ts_raw else None

    ball = frame_obj.get("ball_data") or {}
    ball_x, ball_y, ball_z = ball.get("x"), ball.get("y"), ball.get("z")
    ball_is_detected = ball.get("is_detected")

    return [
        {
            "match_id": match_id,
            "period": period,
            "frame": frame_num,
            "timestamp": timestamp,
            "player_id": player["player_id"],
            "x": player.get("x"),
            "y": player.get("y"),
            "is_visible": player.get("is_detected"),
            "ball_x": ball_x,
            "ball_y": ball_y,
            "ball_z": ball_z,
            "ball_is_detected": ball_is_detected,
        }
        for player in frame_obj.get("player_data", [])
    ]


def derive_frame_rate(timestamps_by_period: dict[object, list[float]]) -> int:
    """Derive the tracking frame rate from cadence: ``1/median(Δt)``, snapped to the nearest
    allowed rate within tolerance.

    Δt is computed WITHIN each period (never across the period boundary — that gap would skew
    the median), pooled across periods, then the median. Snap to the nearest of
    ``_ALLOWED_FRAME_RATES`` within ``_FRAME_RATE_TOLERANCE`` (the raw value won't be exactly
    nominal — 0.04004 s → 24.97, not 25.0). **Fail loud, never default** — ``frame_rate`` is a
    metric-validity gate: it drives velocity/DAS AND the goal-kick origin's
    ``round(frame_rate * 1 s)`` keeper-detection window, so a 10-vs-25 error silently degrades
    RM ``xt_gk`` comparability. (Spec §5.3.)
    """
    diffs: list[float] = []
    for ts_list in timestamps_by_period.values():
        ts = sorted(t for t in ts_list if t is not None)
        diffs.extend(b - a for a, b in itertools.pairwise(ts) if b > a)
    if not diffs:
        raise ValueError("cannot derive frame_rate: no within-period timestamp deltas")
    median_dt = statistics.median(diffs)
    if median_dt <= 0:
        raise ValueError(f"cannot derive frame_rate: non-positive median Δt {median_dt}")
    raw = 1.0 / median_dt
    for rate in _ALLOWED_FRAME_RATES:
        if abs(raw - rate) / rate <= _FRAME_RATE_TOLERANCE:
            return rate
    raise ValueError(
        f"derived frame rate {raw:.2f} fps (median Δt {median_dt:.5f}s) is not within "
        f"±{_FRAME_RATE_TOLERANCE:.0%} of any allowed rate {_ALLOWED_FRAME_RATES}"
    )


def _finalize_tracking_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the shared dtype overrides (Arrow/Spark compat) + ``_ingested_at``."""
    for col, dtype in _TRACKING_DTYPE_OVERRIDES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)  # type: ignore[arg-type]
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def parse_tracking_jsonl(source: str, *, match_id: str) -> pd.DataFrame:
    """Parse an A-League ``tracking_extrapolated.jsonl`` file to narrow-format DataFrame.

    Streams line-by-line (one JSON object per frame); reshapes via ``_frame_to_rows``.
    A-League is a fixed 10 fps (``_FRAME_RATE``). Output byte-identical to the pre-refactor
    reader.
    """
    rows: list[dict[str, object]] = []
    with open(source, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.extend(_frame_to_rows(json.loads(line), match_id))

    df = pd.DataFrame(rows)
    df["frame_rate"] = _FRAME_RATE
    return _finalize_tracking_df(df)


def _iter_tracking_frames(source: str) -> Iterator[dict]:
    """Yield tracking frames from an RM gzipped-JSON-array artifact.

    Load-all per match: one match's decompressed tracking is held at a time and freed by the
    caller's ``gc.collect`` between matches (bounded — NOT a whole-corpus load; the ingest
    loop is per-match sequential). The 1.3M-row narrow list the caller builds dominates memory
    either way. Isolated here so a future switch to a streaming parser (``ijson`` over the gz
    handle) is a one-function change without touching the reshape/rate logic. (Spec §5.3 —
    stream-parse deferred: ijson is not a current dep, and the streaming saving is marginal vs
    the unavoidable row list.)
    """
    with gzip.open(source, "rt", encoding="utf-8") as fh:
        frames = json.load(fh)
    yield from frames


def parse_tracking_gz(source: str, *, match_id: str) -> pd.DataFrame:
    """Parse an RM ``tracking.json.gz`` artifact to the same narrow bronze shape as A-League.

    Same frame model as the A-League JSONL (verified: ``ball_data{x,y,z,is_detected}`` +
    ``player_data[{player_id,x,y,is_detected}]``), gzip-JSON-array serialized. ``frame_rate``
    is DERIVED from cadence (``derive_frame_rate``) — never the A-League ``_FRAME_RATE=10``
    (metric-validity gate, spec §5.3). Bronze schema unchanged.
    """
    frames = list(_iter_tracking_frames(source))

    ts_by_period: dict[object, list[float]] = defaultdict(list)
    for fr in frames:
        ts_raw = fr.get("timestamp")
        if ts_raw:
            ts_by_period[fr["period"]].append(_parse_timestamp(ts_raw))
    frame_rate = derive_frame_rate(ts_by_period)

    rows: list[dict[str, object]] = []
    for fr in frames:
        rows.extend(_frame_to_rows(fr, match_id))

    df = pd.DataFrame(rows)
    df["frame_rate"] = frame_rate
    return _finalize_tracking_df(df)


def _lookup_skillcorner_visibility(
    spark: SparkSession,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> str | None:
    """Read the per-match ``visibility`` from bronze.skillcorner_matches (the authoritative signal).

    Returns ``None`` (→ provider-default public via the classifier) when the column or row is
    absent (e.g. pre-migration bronze) — never raises. The publish-time fail-safe is the backstop.
    """
    from pyspark.sql import functions as spark_fn

    from ingestion.utils import tolerate_missing_table

    matches_table = f"{catalog}.{schema}.skillcorner_matches"
    with tolerate_missing_table(logger, "skillcorner_matches not found -- access_tier defaults to provider policy"):
        matches_sdf = spark.table(matches_table)
        if "visibility" not in {f.name for f in matches_sdf.schema.fields}:
            return None
        rows = matches_sdf.filter(spark_fn.col("match_id") == match_id).select("visibility").limit(1).collect()
        if rows and rows[0]["visibility"] is not None:
            return str(rows[0]["visibility"])
    return None


def write_tracking(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed tracking DataFrame to bronze.skillcorner_tracking."""
    # Per-match HF redistribution tier (spec 2026-06-29). SkillCorner carries a real per-match
    # `visibility` feed: private matches MUST become restricted so they never reach a public HF
    # repo (the pitch-control publisher splits on access_tier). DIRECT stamp from the authoritative
    # match-info bronze visibility (NOT a dim_matches join). Missing column/row → provider default
    # (public) with no failure (pre-migration safety; fail-safe is enforced at publish time).
    from shared.access_tier import classify_access_tier

    visibility = _lookup_skillcorner_visibility(spark, catalog, schema, match_id, logger)
    df = df.copy()
    df["access_tier"] = classify_access_tier(provider="skillcorner", visibility=visibility).value

    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "frame", "period", "player_id", "x", "y"],
        "skillcorner_tracking",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_tracking",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
