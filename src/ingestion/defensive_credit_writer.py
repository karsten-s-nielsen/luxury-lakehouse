"""Defensive-credit scorer/writer — ADR-013 Python-writer -> bronze -> dbt staging -> gold marts.

Materialises TWO Rev-6 marts from silly-kicks 4.87.0's TF-51 defensive-credit family (spec §7.5):

* **fct_action_defensive** (Task 17d) — per-action defending-team aggregate, ``add_defensive_credit``:
  ``defensive_credit_net`` / ``_plus`` / ``_minus`` (DOUBLE, **0.0 not NaN** when no credit),
  ``n_defensive_credits`` (BIGINT). Downstream of ``fct_shot_xg`` (needs a per-shot xG column), NOT
  ``fct_action_values`` — putting an xG-derived column into ``fct_action_values`` would be a dbt cycle
  (``fct_shot_xg`` already ``ref()``s it, spec §7.5 / review-4).
* **fct_defensive_credit_attributions** (Task 17f) — long-form ``compute_defensive_credits``, one row per
  ``(action, credited player, rule)``: the 11 columns ``game_id, period_id, action_id, player_id,
  team_id, rule, signed_value, anchor_type, frame_id, sizing, resolution``.

**xG merge (review-4 B3).** ``add_defensive_credit`` / ``compute_defensive_credits`` need a per-shot
``xg`` column on ``actions`` (the credit rules fire only on shot / cross-resulting-in-shot rows).
``attach_xg`` LEFT-JOINs the xG predictions onto the actions on the native shot identity
``(data_source, match_id_native, action_id)`` — the shot's own ``action_id`` is the join key (ADR-013
resolves ``fct_shot_xg`` on the ``(match_key, action_id)`` shot key; the writer reads the native-keyed
``bronze.xg_shot_predictions`` so no surrogate resolution is needed at write time). Non-shot actions get
NaN ``xg``, which is correct. ``blocked_column="shot_blocked"`` is a SPADL enrichment on
``bronze.spadl_actions`` (spec §7.2 / Task 7); ``on_target_column="shot_on_target_derived"`` is NOT a
persisted bronze column — silly-kicks derives it from the frames (TF-48 ``add_shot_goalmouth``) when
absent, which is the live path here.

**Validation boundary (spec Part B).** The pure ``compute_*`` cores are unit-tested on fixtures; the
Spark ``run_pipeline`` (per-unit bronze reads via ``tracking_marts_driver`` + the ``xg_shot_predictions``
merge) is validated by the live Part-B recompute (Task 22b, after ``fct_shot_xg`` rebuild), same posture
as ``xg_shot_scorer.run_pipeline``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.constants import DEFAULT_BRONZE_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md §serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 90, 1)

CATALOG = "soccer_analytics"
XG_COLUMN = "xg"
BLOCKED_COLUMN = "shot_blocked"

# ── Per-action aggregate (Task 17d) ──
AGG_TABLE = "action_defensive_credit"
# period_id is carried for PER-PERIOD replaceWhere idempotency: IDSSE processes per (match, period), so
# a per-match replaceWhere would drop period 1 when period 2 writes. Not exposed by the mart.
_AGG_IDENTITY: tuple[str, ...] = ("data_source", "match_id", "period_id", "action_id")
_AGG_CREDIT_COLUMNS: tuple[str, ...] = (
    "defensive_credit_net",
    "defensive_credit_plus",
    "defensive_credit_minus",
    "n_defensive_credits",
)
AGG_OUTPUT_COLUMNS: tuple[str, ...] = (*_AGG_IDENTITY, *_AGG_CREDIT_COLUMNS)
_AGG_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "period_id": "long",
    "action_id": "long",
    "defensive_credit_net": "double",
    "defensive_credit_plus": "double",
    "defensive_credit_minus": "double",
    "n_defensive_credits": "long",
}
ACTION_DEFENSIVE_DDL = (
    "data_source STRING, match_id STRING, period_id BIGINT, action_id BIGINT, "
    "defensive_credit_net DOUBLE, defensive_credit_plus DOUBLE, defensive_credit_minus DOUBLE, "
    "n_defensive_credits BIGINT, _ingested_at TIMESTAMP"
)

# ── Long-form attributions (Task 17f) ──
LONG_TABLE = "defensive_credit_attributions"
_LONG_IDENTITY: tuple[str, ...] = ("data_source", "match_id")
# silly-kicks 4.87.0 compute_defensive_credits columns (_LONG_COLS), in order.
_LONG_SK_COLUMNS: tuple[str, ...] = (
    "game_id",
    "period_id",
    "action_id",
    "player_id",
    "team_id",
    "rule",
    "signed_value",
    "anchor_type",
    "frame_id",
    "sizing",
    "resolution",
)
LONG_OUTPUT_COLUMNS: tuple[str, ...] = (*_LONG_IDENTITY, *_LONG_SK_COLUMNS)
_LONG_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "game_id": "long",
    "period_id": "long",
    "action_id": "long",
    "player_id": "string",
    "team_id": "string",
    "rule": "string",
    "signed_value": "double",
    "anchor_type": "string",
    "frame_id": "long",
    "sizing": "string",
    "resolution": "string",
}
DEFENSIVE_CREDIT_ATTRIBUTIONS_DDL = (
    "data_source STRING, match_id STRING, game_id BIGINT, period_id BIGINT, action_id BIGINT, "
    "player_id STRING, team_id STRING, rule STRING, signed_value DOUBLE, anchor_type STRING, "
    "frame_id BIGINT, sizing STRING, resolution STRING, _ingested_at TIMESTAMP"
)

# The xG-prediction columns the writer reads (native-keyed — bronze.xg_shot_predictions).
XG_PRED_COLUMNS: tuple[str, ...] = ("data_source", "match_id_native", "action_id", "xg")


# ---------------------------------------------------------------------------
# xG merge (pure; unit-tested)
# ---------------------------------------------------------------------------


def attach_xg(actions: pd.DataFrame, xg_preds: pd.DataFrame, *, xg_column: str = XG_COLUMN) -> pd.DataFrame:
    """LEFT-JOIN per-shot xG onto ``actions`` on the native shot identity.

    ``xg_preds`` carries ``(data_source, match_id_native, action_id, xg)`` (bronze.xg_shot_predictions).
    The shot's own ``action_id`` is the join key; non-shot actions get NaN ``xg``. Returns a COPY.
    """
    keys = ["data_source", "match_id_native", "action_id"]
    right = xg_preds[[*keys, "xg"]].drop_duplicates(subset=keys).rename(columns={"xg": xg_column})
    merged = actions.merge(right, on=keys, how="left")
    return merged


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def compute_action_defensive_credit(
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: Any,
    *,
    xg_column: str = XG_COLUMN,
    blocked_column: str = BLOCKED_COLUMN,
) -> pd.DataFrame:
    """Per-action defending-team credit aggregate -> identity + 4 credit columns (Task 17d).

    ``actions`` must carry ``xg_column`` (see :func:`attach_xg`) + ``data_source`` + ``match_id_native``.
    ``defensive_credit_*`` are 0.0 (not NaN) where no credit; ``n_defensive_credits`` is 0. Grain =
    one row per input action (all action types).
    """
    from silly_kicks.tracking import add_defensive_credit

    scored = add_defensive_credit(actions, frames, xg_column=xg_column, xt=xt, blocked_column=blocked_column)
    out = scored.copy()
    out["data_source"] = out["data_source"].astype("string")
    out["match_id"] = out["match_id_native"].astype("string")
    return out[list(AGG_OUTPUT_COLUMNS)].reset_index(drop=True)


def compute_defensive_credit_long(
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: Any,
    *,
    xg_column: str = XG_COLUMN,
    blocked_column: str = BLOCKED_COLUMN,
) -> pd.DataFrame:
    """Long-form per-(action, player, rule) signed credit -> identity + 11 columns (Task 17f)."""
    import pandas as pd
    from silly_kicks.tracking import compute_defensive_credits

    credits = compute_defensive_credits(actions, frames, xg_column=xg_column, xt=xt, blocked_column=blocked_column)
    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    out = credits.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    if out.empty:
        return pd.DataFrame(columns=list(LONG_OUTPUT_COLUMNS))
    out["player_id"] = out["player_id"].astype("string")
    out["team_id"] = out["team_id"].astype("string")
    return out[list(LONG_OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) — validated by the live Part-B gate, not unit tests
# ---------------------------------------------------------------------------


def _struct_type(columns: tuple[str, ...], types: dict[str, str]) -> Any:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "string": StringType()}
    return StructType([StructField(c, type_map[types[c]], True) for c in columns])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to score defensive credit."
        )


def _read_xg_preds(spark: SparkSession, catalog: str, provider: str, match_id: str) -> pd.DataFrame:
    """Read this match's native-keyed xG predictions (bounded by the match filter)."""
    from pyspark.sql import functions as F  # noqa: N812

    return (
        spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.xg_shot_predictions")
        .filter((F.col("data_source") == provider) & (F.col("match_id_native") == match_id))
        .select(*XG_PRED_COLUMNS)
        .toPandas()
    )
