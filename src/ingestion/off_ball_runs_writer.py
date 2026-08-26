"""Off-ball runs scorer/writer — ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises ``fct_off_ball_runs`` (spec §7.5, Task 17e), grain **one row per (action, runner)**. Per
tracking work unit it reconstructs the oriented ``(actions, frames, xt)`` (the shared
``analytics.action_context.unit_inputs.build_unit_inputs`` seam — the same frames the AC drain builds),
detects qualifying off-ball runs (``detect_off_ball_runs``, TF-4/TF-35) and values them against the
fitted expected-threat grid (``value_off_ball_runs``), then writes bronze ``off_ball_runs``.

**Null-rate (review-4 B5).** ``value_off_ball_runs`` values ONLY completed passes/crosses with a
resolved receiver; every other run is legitimately off-domain -> ``run_value`` NaN and ``role`` <NA>.
Most rows carry NaN ``run_value`` by construction — the mart's null-rate bounds are sized for that large,
correct NaN share (a tracking-visibility gap also survives as NaN, never a zero — ADR-033).

**Validation boundary (spec Part B).** ``compute_off_ball_runs`` is unit-tested on fixtures; the Spark
``run_pipeline`` (per-unit bronze reads via ``tracking_marts_driver``) is validated by the live Part-B
recompute (Task 22b), same posture as ``xg_shot_scorer.run_pipeline``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md §serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 90, 1)

CATALOG = "soccer_analytics"
MODEL_NAME = "off_ball_runs"
BRONZE_TABLE = "off_ball_runs"

# Native identity stamped by the writer (surrogate keys resolve in the mart — ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id")

# silly-kicks 4.87.0 detect columns (RUN_COLUMNS) + value columns (RUN_VALUE_COLUMNS), in order.
_DETECT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "period_id",
    "action_id",
    "player_id",
    "run_start_x",
    "run_start_y",
    "run_end_x",
    "run_end_y",
    "displacement_m",
    "duration_s",
    "mean_speed_ms",
    "peak_speed_ms",
    "peak_speed_source",
    "toward_goal",
)
_VALUE_COLUMNS: tuple[str, ...] = ("role", "is_receiver", "run_value", "enabled_pass_credit")

# Full bronze output column order (identity + 18 silly-kicks columns).
OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_DETECT_COLUMNS, *_VALUE_COLUMNS)

# Column -> Spark SQL type (kept in lockstep with the bronze DDL / migration).
_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "game_id": "long",
    "period_id": "long",
    "action_id": "long",
    "player_id": "string",
    "run_start_x": "double",
    "run_start_y": "double",
    "run_end_x": "double",
    "run_end_y": "double",
    "displacement_m": "double",
    "duration_s": "double",
    "mean_speed_ms": "double",
    "peak_speed_ms": "double",
    "peak_speed_source": "string",
    "toward_goal": "boolean",
    "role": "string",
    "is_receiver": "boolean",
    "run_value": "double",
    "enabled_pass_credit": "double",
}

# Canonical bronze DDL (mirrored by the 2026-08-20-add-marts1 migration; parity-tested).
OFF_BALL_RUNS_DDL = (
    "data_source STRING, match_id STRING, game_id BIGINT, period_id BIGINT, action_id BIGINT, "
    "player_id STRING, run_start_x DOUBLE, run_start_y DOUBLE, run_end_x DOUBLE, run_end_y DOUBLE, "
    "displacement_m DOUBLE, duration_s DOUBLE, mean_speed_ms DOUBLE, peak_speed_ms DOUBLE, "
    "peak_speed_source STRING, toward_goal BOOLEAN, role STRING, is_receiver BOOLEAN, "
    "run_value DOUBLE, enabled_pass_credit DOUBLE, _ingested_at TIMESTAMP"
)


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def compute_off_ball_runs(actions: pd.DataFrame, frames: pd.DataFrame, xt: Any) -> pd.DataFrame:
    """Detect + value off-ball runs for one unit -> identity + 18 columns, grain (action, runner).

    ``actions`` must be the oriented, identity-resolved SPADL actions (it carries ``data_source`` +
    ``match_id_native``, stamped onto every emitted run). ``xt`` is a FITTED ``ExpectedThreat``.
    Returns an empty frame with the full schema when nothing qualifies.
    """
    import pandas as pd
    from silly_kicks.tracking import detect_off_ball_runs, value_off_ball_runs

    runs = detect_off_ball_runs(actions, frames)
    valued = value_off_ball_runs(runs, actions, frames, xt)

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])

    out = valued.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    out["player_id"] = out["player_id"].astype("string")
    if out.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))
    return out[list(OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) — validated by the live Part-B gate, not unit tests
# ---------------------------------------------------------------------------


def _struct_type() -> Any:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "boolean": BooleanType(), "string": StringType()}
    return StructType([StructField(c, type_map[_OUTPUT_TYPES[c]], True) for c in OUTPUT_COLUMNS])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to score off_ball_runs."
        )
