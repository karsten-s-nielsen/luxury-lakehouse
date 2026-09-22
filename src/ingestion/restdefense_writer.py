"""Rest-defense scorer/writer — ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises ``fct_rest_defense`` (sk4118 P1 Phase E; silly-kicks 4.102/4.103 restdefense L1+L2), grain
**one row per in-possession on-ball action** (the DEFENDING team's rest-defense shape at that moment). Per
tracking work unit it reuses the oriented ``(actions, frames, xt)`` the AC drain builds (the shared
``analytics.action_context.unit_inputs.build_unit_inputs`` seam) and calls
``silly_kicks.restdefense.compute_rest_defense`` — Layer 1 (11 descriptive geometry cols) always, Layer 2
(5 xT/space-control cols) when a fitted ``xt`` is injected (``inputs.xt`` is a fitted ``ExpectedThreat``).

**Descriptive / team-structural — NOT per-player-evaluative.** restdefense measures a team's rest-defence
GEOMETRY (line height, compactness, numerical superiority, danger behind the line), never an individual's
rating, so it carries NO EU-AI-Act governance card (unlike shot_stopping / gk_decision / territory /
duels). See ``AI_GOVERNANCE.md`` — it is deliberately absent from ``PER_PLAYER_EVALUATIVE_CARDS``.

**FOV companions are structurally N/A here (spec deviation, ADR-085-adjacent).** ``compute_rest_defense``
emits its ``<col>_observed_fraction`` companions only when a ``visible_area`` (``action_id -> polygon``)
table is passed. The repo's only visible_area source (``analytics.action_context.visible_area``) is
SB360-only, and StatsBomb/SB360 EXITS to ``main_statsbomb`` — it never enters the tracking-marts drain
(idsse / metrica / skillcorner / gradientsports). So no visible_area exists for restdefense's providers;
we pass ``visible_area=None`` and do not declare the companion columns. L3 deterrence arms are DEMOTED
(not adopted) per spec, so the arm columns are excluded too.

**Runs in the tracking-marts drain (ADR-082).** Like ``off_ball_runs_writer`` / ``defensive_credit_writer``
this module is pure compute (``compute_rest_defense_samples``); the per-unit dispatch + Spark write live in
``ingestion.tracking_marts_processor.TrackingMartsProcessor`` (validated by the live Part-B recompute).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from silly_kicks.restdefense import RD_METRIC_COLUMNS, RD_SAMPLE_KEYS

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 121, 0)

CATALOG = "soccer_analytics"
MODEL_NAME = "rest_defense"
BRONZE_TABLE = "rest_defense"

# Native identity stamped by the writer (surrogate keys resolve in the mart — ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "team_id_native")

# The sk ``compute_rest_defense`` sample-table columns, in emission order (pinned; a drift test asserts
# this equals the live sk output so a silly-kicks re-order/rename fails loud rather than silently mis-maps).
# ``possession_id`` / ``is_possession_loss`` are the per-sample META cols; then the 16 metric cols (11 L1 +
# 5 L2, ``RD_METRIC_COLUMNS``); then the geometry-provenance flag.
_GEOMETRY_SOURCE = "rd_geometry_source"
_SAMPLE_META: tuple[str, ...] = ("possession_id", "is_possession_loss")
_SK_SAMPLE_COLUMNS: tuple[str, ...] = (*RD_SAMPLE_KEYS, *_SAMPLE_META, *RD_METRIC_COLUMNS, _GEOMETRY_SOURCE)

# Full bronze output column order (identity + 23 silly-kicks sample cols + access_tier).
OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_SK_SAMPLE_COLUMNS, "access_tier")

# The three integer count metrics silly-kicks emits as Int64 (nullable) -> Spark BIGINT; every other
# metric is a float64 double, except the categorical staggered-shape flag (string).
_COUNT_METRIC_COLUMNS: tuple[str, ...] = ("rd_num_superiority", "rd_num_superiority_gk", "rd_zone_occupancy")
_SHAPE_STAGGER = "rd_shape_2_3_vs_3_2"

# Column -> Spark SQL type (kept in lockstep with the bronze DDL / migration).
_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "team_id_native": "string",
    "game_id": "long",
    "period_id": "long",
    "team_id": "string",
    "action_id": "long",
    "possession_id": "long",
    "is_possession_loss": "boolean",
    _GEOMETRY_SOURCE: "string",
    _SHAPE_STAGGER: "string",
    "access_tier": "string",
    # every count metric -> long; every remaining metric -> double (filled below).
    **{c: "long" for c in _COUNT_METRIC_COLUMNS},
    **{c: "double" for c in RD_METRIC_COLUMNS if c not in _COUNT_METRIC_COLUMNS and c != _SHAPE_STAGGER},
}

# Canonical bronze DDL (mirrored by the Phase E migration; parity-tested).
REST_DEFENSE_DDL = ", ".join(f"{c} {_OUTPUT_TYPES[c].upper().replace('LONG', 'BIGINT')}" for c in OUTPUT_COLUMNS) + (
    ", _ingested_at TIMESTAMP"
)


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def compute_rest_defense_samples(
    actions: pd.DataFrame, frames: pd.DataFrame, xt: Any, *, access_tier: str | None
) -> pd.DataFrame:
    """Score one unit's rest-defense samples -> identity + 23 sk cols + access_tier.

    ``actions`` must be the oriented, identity-resolved SPADL actions (it carries ``data_source`` +
    ``match_id_native`` + ``team_id`` / ``team_id_native``). ``xt`` is a FITTED ``ExpectedThreat`` — the
    Layer-2 cols are NaN without it (never fabricated). ``visible_area`` is not passed (FOV companions are
    structurally N/A in the tracking-marts drain — see the module docstring). Returns an empty frame with
    the full schema when nothing qualifies.
    """
    import pandas as pd
    from silly_kicks.restdefense import compute_rest_defense

    samples, _report = compute_rest_defense(actions, frames, xt=xt)

    if samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    # Map the sk-emitted (possibly enrichment-mutated) team_id -> the true native team id.
    # ``_resolve_enrichment_identity`` sets ``actions.team_id`` to ``team_id_native`` for
    # idsse/skillcorner/gradientsports and to "Home"/"Away" for metrica; BOTH teams appear in the match's
    # actions, so this per-match map covers the DEFENDING team the sample is keyed on.
    team_map = dict(zip(actions["team_id"].astype(str), actions["team_id_native"].astype(str), strict=False))

    out = samples.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    out["team_id"] = out["team_id"].astype(str)
    out["team_id_native"] = out["team_id"].map(team_map)
    out["access_tier"] = access_tier
    return out[list(OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark schema (Databricks) — the drain processor writes; validated by the live Part-B gate
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
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to score rest_defense."
        )
