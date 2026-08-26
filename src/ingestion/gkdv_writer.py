"""GKDV (GK Deterrent Value) scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.87.0 ``gkdv`` counterfactual family (spec sec 7.5, Task 17h) and lands it
in the existing ``fct_gk_shot_stopping_pooled`` mart, per-keeper-pooled over ``(competition, season)``.

**Mini-pipeline (mirrors the silly-kicks maintainer driver ``scripts/build_gkdv_arm_values.py``):**
per tracking work unit -> ``infer_ball_carrier`` -> ``derive_team_in_possession`` (the arms route
through DAS, which REQUIRES ``team_in_possession``) -> ``build_ghost_frames(home_team_id, carrier=)``
-> per SCORED-and-DEFENDING frame, ``delta_das`` (accessible-space) + ``delta_threat_suppression``
(fitted xT, ``goal_map``) -> ``aggregate_by_keeper`` per value-col, partitioned by ``(comp, season)``.

**Drop-reason exclusion (review-4 B1 -- HIGH, silent null-bias).** ``build_ghost_frames`` returns the
FULL counterfactual frames; a dropped frame (missing/NaN GK, off-domain) is BYTE-IDENTICAL across the
actual/ghost legs, so differencing it yields ``delta == 0`` and biases every keeper aggregate toward the
null. :func:`_scored_defending_keepers` restricts to ``provenance["drop_reason"].isna()`` **and** the
DEFENDING keeper (``gk_team_id == defending_team_id``) BEFORE any differencing -- the serving seam writes
BOTH teams' keepers, only one of which moved. Nothing downstream re-derives it.

**Native ids -> Kimball surrogates (review-4 B2, ADR-013).** ``aggregate_by_keeper`` keys on the
frames-resolved NATIVE ``player_id`` (the library deliberately avoids the gold join). The writer emits
native ``(player_id, competition_id, season_id)``; ``stg_gkdv`` resolves ``player_id -> player_key`` via
``dim_players`` on ``(provider, native_player_id)`` and ``competition_id -> competition_key`` /
``season_id`` via the SAME ``generate_competition_key`` macro + ``cast(... as int)`` ``dim_matches`` uses,
so the mart join on ``(player_key, competition_key, season_id)`` cannot silently land all-NULL. The
NATIVE ``(competition_id, season_id)`` per match mirror ``dim_matches`` exactly (idsse/metrica season is
NULL; metrica competition is the ``'metrica-sample'`` staging constant).

**gkdv API caveat.** The gkdv surface is evolving upstream; this writer is pinned to the silly-kicks
**4.87.0** signatures (``build_ghost_frames`` / ``delta_das`` / ``delta_threat_suppression`` /
``aggregate_by_keeper``). Requires the ``[das]`` extra (accessible-space) for the DAS arm.

**Validation boundary (spec Part B).** The pure cores (:func:`_scored_defending_keepers`,
:func:`build_keeper_observations`, :func:`pool_keepers`) are unit-tested on fixtures; the Spark
``run_pipeline`` (per-unit bronze reads via ``tracking_marts_driver`` + the expensive
per-frame accessible-space/pitch-control scoring) is validated by the live Part-B recompute (Task 22b),
same posture as ``xg_shot_scorer.run_pipeline``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from shared.constants import DEFAULT_BRONZE_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 90, 1)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "gkdv_keeper_pooled"

# GKDV covers the TRACKING providers only (it needs real tracking frames for the counterfactual).
_TRACKING_PROVIDERS: tuple[str, ...] = ("idsse", "metrica", "skillcorner", "gradientsports")

# Registered clustering floors (spec sec 6.1; silly-kicks aggregate_by_keeper defaults).
_MIN_NONZERO = 20
_MIN_GAMES = 2

# The per-keeper aggregate columns silly-kicks emits, in order (aggregate_by_keeper._OUTPUT_COLUMNS
# minus the player_id key). Renamed per value-col to gkdv_delta_das_* / gkdv_delta_threat_*.
_AGG_VALUE_COLS: tuple[str, ...] = ("mean", "median", "n", "n_nonzero", "n_games", "gate_eligible")

_DAS = "delta_das"
_THREAT = "delta_threat_suppression"

# Native identity + pooled columns emitted to bronze (surrogates resolve in staging -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "player_id", "competition_id", "season_id")
_DAS_COLUMNS: tuple[str, ...] = tuple(f"gkdv_delta_das_{c}" for c in _AGG_VALUE_COLS)
_THREAT_COLUMNS: tuple[str, ...] = tuple(f"gkdv_delta_threat_{c}" for c in _AGG_VALUE_COLS)
POOLED_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_DAS_COLUMNS, *_THREAT_COLUMNS)


def _agg_col_type(col: str) -> str:
    """Spark SQL type for a pooled aggregate column (mean/median = double, gate = boolean, counts = long)."""
    if col.endswith("gate_eligible"):
        return "boolean"
    if col.endswith(("mean", "median")):
        return "double"
    return "long"


_POOLED_TYPES: dict[str, str] = {
    "data_source": "string",
    "player_id": "string",
    "competition_id": "string",
    "season_id": "string",
    **{c: _agg_col_type(c) for c in (*_DAS_COLUMNS, *_THREAT_COLUMNS)},
}

# Canonical bronze DDL (mirrored by the 2026-08-20-add-marts2 migration; keep in sync).
GKDV_KEEPER_POOLED_DDL = (
    "data_source STRING, player_id STRING, competition_id STRING, season_id STRING, "
    "gkdv_delta_das_mean DOUBLE, gkdv_delta_das_median DOUBLE, gkdv_delta_das_n BIGINT, "
    "gkdv_delta_das_n_nonzero BIGINT, gkdv_delta_das_n_games BIGINT, gkdv_delta_das_gate_eligible BOOLEAN, "
    "gkdv_delta_threat_mean DOUBLE, gkdv_delta_threat_median DOUBLE, gkdv_delta_threat_n BIGINT, "
    "gkdv_delta_threat_n_nonzero BIGINT, gkdv_delta_threat_n_games BIGINT, gkdv_delta_threat_gate_eligible BOOLEAN, "
    "_ingested_at TIMESTAMP"
)


# ---------------------------------------------------------------------------
# Pure scoring cores (unit-tested; no Spark)
# ---------------------------------------------------------------------------


# (game_id, period_id, frame_id) -- the per-frame grain both arms score on.
_FRAME_KEYS: tuple[str, ...] = ("game_id", "period_id", "frame_id")


def _index_frames_by_key(frames: pd.DataFrame) -> dict[tuple[Any, ...], pd.DataFrame]:
    """Pre-build a ``(game_id, period_id, frame_id) -> slice`` index ONCE per unit.

    Replaces a per-scored-frame boolean-mask scan (``frames[frames.col == v]`` inside the loop), which on
    tracking-scale data is the O(n x m) hidden-nested-loop pattern CLAUDE.md forbids (always Critical, never
    Minor). A single ``groupby`` builds every slice up front; the loop then does O(1) dict lookups.
    ``sort=False`` preserves original row order WITHIN each group, so a looked-up slice is byte-identical
    (rows AND order) to what the mask returned -- which the DAS arm's positional actual/ghost alignment
    depends on. A missing key yields no entry; the caller falls back to an empty same-schema slice, matching
    the old mask's empty result.
    """
    return dict(iter(frames.groupby(list(_FRAME_KEYS), sort=False)))


def _attacking_team_id(frame_slice: pd.DataFrame, defending_team_id: Any) -> Any:
    """The in-possession team: the non-ball team that is NOT defending (ADR-019 dtype-safe)."""
    from silly_kicks.id_compat import same_id

    teams = frame_slice.loc[~frame_slice["is_ball"].astype(bool), "team_id"].dropna().unique()
    other = [t for t in teams if not same_id(t, defending_team_id)]
    return other[0] if other else None


def _scored_defending_keepers(provenance: pd.DataFrame) -> pd.DataFrame:
    """Scored (``drop_reason`` NaN) rows for the DEFENDING keeper only -- the review-4 B1 guard.

    A dropped frame is byte-identical across the actual/ghost legs (delta == 0), and the serving seam
    writes BOTH teams' keepers per scored frame while ``build_ghost_frames`` moved only the defending
    one -- so this selection is what keeps every zero-delta / wrong-keeper row OUT of the aggregate.
    ``reset_index`` before masking: the ``ids_equal`` Series is indexed 0..n-1 while the drop-reason
    filter leaves the original non-contiguous index (mirrors the sk maintainer driver).
    """
    import numpy as np
    from silly_kicks.id_compat import ids_equal

    scored = provenance[provenance["drop_reason"].isna()].reset_index(drop=True)
    if scored.empty:
        return scored
    keep = np.asarray(ids_equal(scored["gk_team_id"], scored["defending_team_id"]), dtype=bool)
    return scored[keep].reset_index(drop=True)


def build_keeper_observations(
    frames: pd.DataFrame,
    counterfactual_frames: pd.DataFrame,
    provenance: pd.DataFrame,
    xt: Any,
    *,
    want_threat: bool = True,
    params: Any = None,
) -> pd.DataFrame:
    """Per SCORED-and-DEFENDING frame -> observation rows (``player_id`` + arm deltas).

    Iterates ONLY :func:`_scored_defending_keepers` (review-4 B1 -- dropped frames never differenced).
    ``xt`` is a fitted ``ExpectedThreat`` (required when ``want_threat``); ``goal_map`` is pinned ONCE
    from the FULL factual frames (the seam's quantity is the mean GK-x per (game, period, team), so a
    per-frame map would be a different estimator -- mirrors the sk driver).
    """
    import pandas as pd
    from silly_kicks.gkdv import delta_das, delta_threat_suppression
    from silly_kicks.tracking import resolve_defended_goals

    cols = ["player_id", "period_id", "frame_id", _DAS, *([_THREAT] if want_threat else [])]
    scored = _scored_defending_keepers(provenance)
    if scored.empty:
        return pd.DataFrame(columns=cols)

    goal_map: Any = resolve_defended_goals(frames) if want_threat else None
    arm_kwargs = {"params": params} if params is not None else {}

    # Pre-index BOTH legs ONCE per unit (no boolean-mask scan inside the per-frame loop -- CLAUDE.md
    # O(n x m) prohibition, Critical on tracking data). The empty same-schema fallback reproduces the
    # old mask's empty result for a key the index does not carry.
    actual_index = _index_frames_by_key(frames)
    ghost_index = _index_frames_by_key(counterfactual_frames)
    empty_actual = frames.iloc[0:0]
    empty_ghost = counterfactual_frames.iloc[0:0]

    rows: list[dict] = []
    for rec in scored.to_dict("records"):
        per, fid = rec["period_id"], rec["frame_id"]
        key = (rec["game_id"], per, fid)
        actual = actual_index.get(key, empty_actual)
        ghost = ghost_index.get(key, empty_ghost)
        atk = _attacking_team_id(actual, rec["defending_team_id"])
        if atk is None:
            continue
        row = {
            "player_id": rec["player_id"],
            "period_id": per,
            "frame_id": fid,
            _DAS: float(delta_das(actual, ghost, attacking_team_id=atk, **arm_kwargs)),
        }
        if want_threat:
            row[_THREAT] = float(
                delta_threat_suppression(actual, ghost, attacking_team_id=atk, xt=xt, goal_map=goal_map, **arm_kwargs)
            )
        rows.append(row)
    return pd.DataFrame(rows, columns=cols)


def score_unit(
    frames: pd.DataFrame,
    home_team_id: Any,
    xt: Any,
    *,
    want_threat: bool = True,
    params: Any = None,
) -> tuple[pd.DataFrame, Any]:
    """One unit's oriented frames -> ``(observations, GkdvReport)``.

    Derives ``team_in_possession`` (the DAS arm requires it) then builds the ghost counterfactual and
    scores it. ``frames`` are the AC-oriented home-LTR frames from ``build_unit_inputs``.
    """
    from silly_kicks.gkdv import build_ghost_frames
    from silly_kicks.tracking import derive_team_in_possession, infer_ball_carrier

    f = frames.drop(columns=["team_in_possession"], errors="ignore")
    carrier = infer_ball_carrier(f)
    f = derive_team_in_possession(f, carrier)

    build_kwargs = {"home_team_id": home_team_id, "carrier": carrier}
    if params is not None:
        build_kwargs["params"] = params
    counterfactual, provenance, report = build_ghost_frames(f, **build_kwargs)
    observations = build_keeper_observations(f, counterfactual, provenance, xt, want_threat=want_threat, params=params)
    return observations, report


def score_gkdv_unit(
    frames: pd.DataFrame,
    home_team_id: Any,
    xt: Any,
    *,
    data_source: str,
    match_id: str,
    competition_id: str | None,
    season_id: str | None,
    want_threat: bool = True,
) -> pd.DataFrame:
    """One unit's oriented frames -> stamped per-frame observations (ADR-037 per-unit drain body).

    Factors the per-unit body of :func:`run_pipeline` (score + identity stamp) so a per-unit drain
    processor scores one unit at a time and appends to the ``bronze.gkdv_observations`` intermediate.
    Stamps the four identity columns exactly as the corpus loop does: ``data_source``, ``game_id``
    (= the native ``match_id`` -> ``aggregate_by_keeper`` ``n_games``), ``competition_id``, ``season_id``.
    The reduce (:func:`pool_keepers`) runs later over the whole ``gkdv_observations`` corpus.
    """
    observations, _report = score_unit(frames, home_team_id, xt, want_threat=want_threat)
    observations = observations.copy()
    observations["data_source"] = data_source
    observations["game_id"] = match_id  # native match id -> aggregate_by_keeper n_games
    observations["competition_id"] = competition_id
    observations["season_id"] = season_id
    return observations


def pool_keepers(
    observations: pd.DataFrame,
    *,
    min_nonzero: int = _MIN_NONZERO,
    min_games: int = _MIN_GAMES,
    want_threat: bool = True,
) -> pd.DataFrame:
    """Aggregate per-frame observations to per-keeper-pooled rows, partitioned by (comp, season).

    ``observations`` must carry ``player_id``, ``game_id`` and the arm value column(s), plus the
    ``(data_source, competition_id, season_id)`` tags. Runs silly-kicks ``aggregate_by_keeper`` per
    ``(data_source, competition_id, season_id)`` group per value-col so the pooling grain matches the
    mart's ``(player_key, competition_key, season_id)``.
    """
    import pandas as pd
    from silly_kicks.gkdv import aggregate_by_keeper

    if observations.empty:
        return pd.DataFrame(columns=list(POOLED_COLUMNS))

    parts: list[pd.DataFrame] = []
    for group_key, grp in observations.groupby(["data_source", "competition_id", "season_id"], dropna=False):
        ds, comp, season = cast("tuple[Any, Any, Any]", group_key)
        das = aggregate_by_keeper(grp, value_col=_DAS, min_nonzero=min_nonzero, min_games=min_games)
        das = das.rename(columns={c: f"gkdv_delta_das_{c}" for c in _AGG_VALUE_COLS})
        merged = das
        if want_threat:
            threat = aggregate_by_keeper(grp, value_col=_THREAT, min_nonzero=min_nonzero, min_games=min_games)
            threat = threat.rename(columns={c: f"gkdv_delta_threat_{c}" for c in _AGG_VALUE_COLS})
            merged = das.merge(threat, on="player_id", how="outer")
        else:
            for c in _THREAT_COLUMNS:
                merged[c] = pd.NA
        # .assign broadcasts the group-key scalars (typed Hashable by groupby) to every row.
        merged = merged.assign(data_source=ds, competition_id=comp, season_id=season)
        parts.append(merged)

    out = pd.concat(parts, ignore_index=True)
    out["player_id"] = out["player_id"].astype("string")
    return out[list(POOLED_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) -- validated by the live Part-B gate, not unit tests
# ---------------------------------------------------------------------------


def _pooled_struct_type() -> Any:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "boolean": BooleanType(), "string": StringType()}
    return StructType([StructField(c, type_map[_POOLED_TYPES[c]], True) for c in POOLED_COLUMNS])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score gkdv."
        )


def _build_comp_season_lookup(
    spark: SparkSession, catalog: str, providers: tuple[str, ...]
) -> dict[tuple[str, str], tuple[str | None, str | None]]:
    """``(provider, native match_id) -> (native competition_id, native season_id)``, mirroring dim_matches.

    idsse/metrica season is NULL (dim_matches fiat); metrica competition is the ``'metrica-sample'``
    staging constant. Uses ``.collect()`` on small bounded distinct reads (not ``.toPandas()``).
    """
    from pyspark.sql import functions as F  # noqa: N812

    lookup: dict[tuple[str, str], tuple[str | None, str | None]] = {}
    for provider in providers:
        if provider == "idsse":
            rows = (
                spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.idsse_tracking")
                .select(
                    F.col("match_id").cast("string").alias("match_id"),
                    F.col("competition_native_id").cast("string").alias("competition_id"),
                )
                .distinct()
                .collect()
            )
            for r in rows:
                lookup[(provider, str(r["match_id"]))] = (r["competition_id"], None)
        elif provider == "metrica":
            rows = (
                spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.metrica_tracking")
                .select(F.col("match_id").cast("string").alias("match_id"))
                .distinct()
                .collect()
            )
            for r in rows:
                lookup[(provider, str(r["match_id"]))] = ("metrica-sample", None)
        elif provider == "skillcorner":
            rows = (
                spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.skillcorner_matches")
                .groupBy(F.col("match_id").cast("string").alias("match_id"))
                .agg(
                    F.max(F.col("competition_id").cast("string")).alias("competition_id"),
                    F.max(F.col("season_id").cast("string")).alias("season_id"),
                )
                .collect()
            )
            for r in rows:
                lookup[(provider, str(r["match_id"]))] = (r["competition_id"], r["season_id"])
        elif provider == "gradientsports":
            rows = (
                spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.gradientsports_metadata")
                .select(
                    F.col("match_id").cast("string").alias("match_id"),
                    F.col("`competition.id`").cast("string").alias("competition_id"),
                    F.col("season").cast("string").alias("season_id"),
                )
                .distinct()
                .collect()
            )
            for r in rows:
                lookup[(provider, str(r["match_id"]))] = (r["competition_id"], r["season_id"])
    return lookup
