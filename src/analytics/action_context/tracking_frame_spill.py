"""Two-stage tracking-marts transport (rev3): spill built oriented frames, read them back faithfully.

``compute_tracking_marts`` emits SIX marts of different cardinality (off_ball_runs / defensive_credit
agg+long / gkdv_observations / gk_decision / rest_defense) from one unit's built inputs. They cannot share
one ``mapInPandas`` output schema, and rebuilding the oriented frame once per mart would pay the dominant
BUILD cost six times. So the drain builds each unit's oriented frames ONCE (Stage 1) and spills them to a
UC-Volume Parquet keyed by ``(provider, match_id, period)``; each mart then scores from the spill (Stage 2,
six cheap passes). This module is the spill/read seam.

Memory posture (the OOM fix): the old driver held a whole ``(match, period)`` half as pandas (16 GB OOM).
Here the build holds ~0.64 GB (float32) on an EXECUTOR task, spills it, and each Stage-2 mart reads that
~0.64 GB back on an executor and lets silly-kicks' internal ``batch_size`` bound the scorer working set —
never the 16 GB driver.

Dtype faithfulness (TM-PLAN-12) is VERSION-AGNOSTIC: the spill preserves whatever dtypes the built frame
carries — float64/object on sk 4.123, and ``float32`` coords / ``team_id`` ``category`` / ``player_id``
``Int64`` once sk 4.128 (ADR-106) is adopted (player_id STAYS Int64: mutated post-build, the sk ADR-103
dynamic-column rule). The LOCAL writer embeds pandas metadata so ``pd.read_parquet`` restores category /
nullable-int automatically; a DISTRIBUTED Spark write loses that metadata, so the Stage-2 reader passes the
built-frame dtype schema (captured on the driver) via ``restore_dtypes``. Row order is preserved end-to-end
(never re-sorted on read) so frame order survives.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

# ── Stage-1 built-frame Spark schema (the mapInPandas output schema) ───────────────────────────────
# The built oriented frame's column set is per-provider: idsse/gradientsports carry 3 extra preprocess
# columns (x_smoothed / y_smoothed / _preprocessed_with); skillcorner/metrica are the strict subset. Both
# share dtypes on common columns. The schema is declared explicitly (ADR-033 — never infer a mapInPandas
# schema) and a parity test (``tests/tracking_marts/test_build_spill``) asserts it matches a freshly-built
# fixture frame, so the sk-4.128 float32 shift (DoubleType -> FloatType) is caught as a parity failure at
# bump time rather than a silent at-rest change. Column ORDER matches ``build_unit_inputs`` output.
_BUILT_FRAME_DTYPES_BASE: tuple[tuple[str, str], ...] = (
    ("game_id", "int64"),
    ("period_id", "int64"),
    ("frame_id", "int64"),
    ("time_seconds", "float64"),
    ("frame_rate", "float64"),
    ("player_id", "object"),
    ("team_id", "category"),
    ("is_ball", "bool"),
    ("is_goalkeeper", "bool"),
    ("x", "float32"),
    ("y", "float32"),
    ("z", "float32"),
    ("speed", "float32"),
    ("speed_source", "object"),
    ("ball_state", "category"),
    ("team_attacking_direction", "object"),
    ("visibility", "object"),
    ("source_provider", "category"),
    ("is_goalkeeper_source", "category"),
    # Smoothing columns — present only for providers that run the preprocess smoothing path (idsse/
    # gradientsports); absent for skillcorner/metrica. Inserted here (before vx/vy) to match build order.
    ("x_smoothed", "float32"),
    ("y_smoothed", "float32"),
    ("_preprocessed_with", "category"),
    ("vx", "float32"),
    ("vy", "float32"),
)
_SMOOTHING_COLS: frozenset[str] = frozenset({"x_smoothed", "y_smoothed", "_preprocessed_with"})
#: Providers whose built frame carries the smoothing columns (parity-tested).
_SMOOTHING_PROVIDERS: frozenset[str] = frozenset({"idsse", "gradientsports"})
#: Providers whose builder keeps ``team_id`` as object (not the F1b category) — gradientsports' converter
#: path does not categorize it, unlike the kloppy/sportec builders (idsse/skillcorner/metrica). Parity-tested.
_OBJECT_TEAM_ID_PROVIDERS: frozenset[str] = frozenset({"gradientsports"})

_PANDAS_TO_SPARK: dict[str, str] = {
    "int64": "LongType",
    "float64": "DoubleType",
    "float32": "FloatType",
    "object": "StringType",
    "bool": "BooleanType",
    "Int64": "LongType",
    "category": "StringType",
}

#: Per-column Spark-type overrides where the pandas dtype (object) does not imply the Spark type. The sk
#: builder leaves ``visibility`` as object holding bool|NaN (SkillCorner emits bools; idsse/metrica all-NaN),
#: so it serializes to BooleanType, not StringType ("Expected bytes, got bool" otherwise).
_SPARK_TYPE_OVERRIDE: dict[str, str] = {"visibility": "BooleanType"}


def built_frame_dtype_order(provider: str) -> tuple[tuple[str, str], ...]:
    """The built frame's ``(column, pandas_dtype)`` schema for a provider.

    Two per-provider variances (parity-tested): smoothing columns appear only for
    ``_SMOOTHING_PROVIDERS``; ``team_id`` is object (not category) for ``_OBJECT_TEAM_ID_PROVIDERS``.
    """
    cols = _BUILT_FRAME_DTYPES_BASE
    if provider not in _SMOOTHING_PROVIDERS:
        cols = tuple((c, d) for c, d in cols if c not in _SMOOTHING_COLS)
    if provider in _OBJECT_TEAM_ID_PROVIDERS:
        cols = tuple((c, "object" if c == "team_id" else d) for c, d in cols)
    return cols


def built_frame_struct_type(provider: str) -> Any:
    """Explicit Spark ``StructType`` for a provider's built frame — the Stage-1 mapInPandas output schema."""
    from pyspark.sql import types as T  # noqa: N812

    fields = [
        T.StructField(col, getattr(T, _SPARK_TYPE_OVERRIDE.get(col, _PANDAS_TO_SPARK[dtype]))(), True)
        for col, dtype in built_frame_dtype_order(provider)
    ]
    return T.StructType(fields)


def built_frame_dtypes(built: pd.DataFrame) -> dict[str, str]:
    """Capture a built frame's dtype schema (driver-side) to pass to :func:`read_built_frames` in prod.

    A Spark-written Parquet drops pandas' category / nullable-int metadata, so the Stage-2 reader cannot
    recover the built dtypes from the file alone — it restores them from this captured schema.
    """
    return {str(col): str(dtype) for col, dtype in built.dtypes.items()}


def spill_built_frames(built: pd.DataFrame, path: str | Path) -> int:
    """Write one unit's BUILT oriented frames to ``path`` (Parquet), preserving row order. Returns rows.

    Local/per-unit writer (and the round-trip test seam); embeds pandas metadata so a pandas read restores
    dtypes. In the distributed drain the Stage-1 ``mapInPandas`` output is written to the UC Volume by a
    driver-orchestrated Spark write instead, and the reader is given ``restore_dtypes`` explicitly.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    built.to_parquet(p, index=False)
    return len(built)


def read_built_frames(path: str | Path, *, restore_dtypes: dict[str, str] | None = None) -> pd.DataFrame:
    """Read a spilled built-frame Parquet back faithfully (TM-PLAN-12).

    Row order is preserved (no sort) so intra-unit frame order survives. With ``restore_dtypes`` (the prod
    Spark-written path, which loses pandas metadata) each listed column present is cast back to its built
    dtype; the cast is idempotent. Without it (the local pandas-written path) ``pd.read_parquet`` already
    restores dtypes from the embedded pandas metadata.
    """
    import pandas as pd

    df = pd.read_parquet(path)
    if restore_dtypes:
        for col, dtype in restore_dtypes.items():
            if col in df.columns and str(df[col].dtype) != dtype:
                df[col] = df[col].astype(dtype)  # pyright: ignore[reportCallIssue, reportArgumentType]
    return df


__all__ = [
    "built_frame_dtype_order",
    "built_frame_dtypes",
    "built_frame_struct_type",
    "read_built_frames",
    "spill_built_frames",
]
