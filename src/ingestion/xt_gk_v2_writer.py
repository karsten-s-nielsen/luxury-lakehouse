"""xT-GK **v2** scorer/writer — ADR-013 Python-writer → bronze → dbt staging → gold mart.

v2 REPLACES the in-repo v1 ``add_xt_gk`` metric (spec §7.4). The v2 possession-value surface
(``MarkovPossessionValue``) and empirical turnover cost (``EmpiricalTurnoverValue``) are FIT on the
gold action corpus by ``scripts/train_xt_gk_v2_hf.py`` (ADR-012) and delivered to a UC Volume; the
retention model ships bundled in the wheel (``GkRetentionModel.from_variant``). This writer loads the
fitted bundle, scores ``xt_gk_v2`` per GK-distribution action, and writes bronze
``xt_gk_v2_predictions`` — a two-tier mart-join column set (NOT an AC drain column; the AC drain runs
only bundled models, ADR-013).

**Acyclicity (spec §7.4 / review-4 A1).** The writer reads the *v2-free* corpus — bronze
``spadl_action_context`` — and NEVER the post-join ``fct_action_context`` mart (which contains this
writer's own v2 output). Nothing this writer consumes is downstream of ``xt_gk_v2``.

**Scoring sequence (verified against silly-kicks 4.87.0 ``xtgk``):**

1. Pre-filter to ``is_gk_distribution`` rows ONLY — ``compute_xt_gk_v2`` scores every finite row in a
   per-action Python loop (review-4 A3), so passing the full action stream both scores off-domain rows
   and OOM/timeouts on 3M+ actions.
2. ``apply_resolved_gk_geometry`` — overrides the GK-distribution start/end coords with gold's resolved
   keeper geometry (the persisted ``xt_gk_origin_x/_y`` + ``xt_gk_dest_x/_y`` audit columns; those 4
   drain columns are DELIBERATELY kept when v1 is retired — they are this writer's only geometry
   bridge, since the writer has no tracking frames) and stamps ``gk_geometry_source``.
3. ``extract_retention_features`` — mart-native rho features off the SAME resolved frame.
4. ``compute_xt_gk_v2`` — per-provider ``retention`` (skillcorner→its variant, else ``default``);
   pooled ``possession_value`` + ``turnover_cost``. Ordering enforced by v2's internal
   ``_check_coordinate_coherence`` (resolve → features → score).

**Artifact packaging (review-4 A4).** ``MarkovPossessionValue.save`` writes a *directory*
(npz + json + SHA256SUMS) and ``EmpiricalTurnoverValue`` ships NO serializer at 4.87.0, while ADR-012's
``upload_weights_to_uc_volume`` delivers a single ``.json``. So the trainer packs all three ports into
ONE self-contained JSON envelope (surfaces as nested lists) via :func:`serialize_xt_gk_v2_bundle`, and
this writer reconstructs them via :func:`deserialize_xt_gk_v2_bundle`. ``pressure_levels`` round-trips in
the envelope so the metric's terciles match the surfaces V was fit on (never refit — v2's guard).
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    from silly_kicks.xtgk import EmpiricalTurnoverValue, MarkovPossessionValue, PressureLevels

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md §serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 90, 1)

# The AC-layer pressure that feeds the retention model's `release_pressure` feature AND the
# possession/turnover pressure terciles. andrienko_oval is silly-kicks' DEFAULT pressure method
# (velocity-independent, always populated) and the natural canonical choice; the trainer and the
# writer MUST use the SAME column so the metric's terciles match the surfaces V was fit on.
PRESSURE_SOURCE_COLUMN = "pressure_on_actor__andrienko_oval"
# The column name the silly-kicks xtgk API expects (extract_retention_features / compute_xt_gk_v2
# default `pressure_column="pressure"`).
PRESSURE_COLUMN = "pressure"

CATALOG = "soccer_analytics"
MODEL_NAME = "xt_gk_v2"
# The writer's OWN bronze table (two-tier mart-join; NOT bronze.spadl_action_context — spec §7.4).
BRONZE_TABLE = "xt_gk_v2_predictions"
# Single-JSON delivery artifact name (ADR-012 `upload_weights_to_uc_volume` requires a `.json`).
WEIGHTS_FILENAME = "xt_gk_v2_bundle.json"

# The five v2 VALUE columns (NaN off-domain / non-finite coords) + the geometry-source provenance.
V2_VALUE_COLUMNS: tuple[str, ...] = (
    "xt_gk_v2_position",
    "xt_gk_v2_pev",
    "xt_gk_v2_retention_loss",
    "xt_gk_v2_dzv",
    "xt_gk_v2",
)
GK_GEOMETRY_SOURCE_COLUMN = "gk_geometry_source"
# The 6 mart-join columns this writer emits (5 value + 1 provenance).
V2_OUTPUT_COLUMNS: tuple[str, ...] = (*V2_VALUE_COLUMNS, GK_GEOMETRY_SOURCE_COLUMN)

# Identity columns emitted alongside the predictions (native ids; surrogate keys resolve in the mart
# LEFT JOIN — ADR-013). Mirrors the AC bronze identity (data_source, match_id, action_id).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "action_id")

# The bronze columns the writer READS from spadl_action_context (the v2-free corpus).
INPUT_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id",
    "action_id",
    "type_name",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "xt_gk_origin_x",
    "xt_gk_origin_y",
    "xt_gk_dest_x",
    "xt_gk_dest_y",
    "is_gk_distribution",
    PRESSURE_SOURCE_COLUMN,
)

# Canonical bronze DDL for the writer's own table (mirrored by the 2026-08-20-add-xt-gk-v2 migration,
# which CREATE-IF-NOT-EXISTS's it so dbt staging can read it (empty) before Part-B scoring populates it).
XT_GK_V2_DDL = (
    "data_source STRING, match_id STRING, action_id BIGINT, "
    "xt_gk_v2_position DOUBLE, xt_gk_v2_pev DOUBLE, xt_gk_v2_retention_loss DOUBLE, "
    "xt_gk_v2_dzv DOUBLE, xt_gk_v2 DOUBLE, gk_geometry_source STRING, "
    "_ingested_at TIMESTAMP"
)

_BUNDLE_FORMAT_VERSION = "xt_gk_v2_bundle/1"


# ---------------------------------------------------------------------------
# Fitted-bundle serialization (single self-contained JSON — see module docstring)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class XtGkV2Bundle:
    """The three fitted ports + fit metadata, reconstructed from the UC-Volume JSON envelope."""

    possession_value: MarkovPossessionValue
    turnover_cost: EmpiricalTurnoverValue
    pressure_levels: PressureLevels
    xg_column: str
    pressure_column: str
    retention_variant_default: str


def _surfaces_to_json(surfaces: dict[Any, Any]) -> dict[str, list[list[float]]]:
    # dict[Any, Any] accepts both the Markov surfaces (keyed by Literal[1,2,3] PressureLevel) and the
    # turnover surfaces (keyed by int) — the reconstruction reaches into those private dicts (see the
    # module docstring: no single-file serializer ships that fits ADR-012's .json delivery).
    return {str(p): np.asarray(surfaces[p]).tolist() for p in (1, 2, 3)}


def _surfaces_from_json(obj: dict[str, Any], *, dtype: type) -> dict[Any, Any]:
    return {p: np.asarray(obj[str(p)], dtype=dtype) for p in (1, 2, 3)}


def serialize_xt_gk_v2_bundle(
    possession_value: MarkovPossessionValue,
    turnover_cost: EmpiricalTurnoverValue,
    pressure_levels: PressureLevels,
    *,
    xg_column: str,
    pressure_column: str = PRESSURE_COLUMN,
    retention_variant_default: str = "default",
) -> bytes:
    """Pack the three fitted xtgk-v2 ports into one self-contained JSON envelope (bytes).

    Why a bespoke envelope rather than the library serializers: ``MarkovPossessionValue.save`` writes a
    *directory* and ``EmpiricalTurnoverValue`` has no serializer at 4.87.0, but ADR-012's
    ``upload_weights_to_uc_volume`` delivers a single ``.json``. The surfaces are tiny (a 16x12 grid x 3
    pressure levels), so nested JSON lists are compact and human-auditable.
    """
    pv, tc, pl = possession_value, turnover_cost, pressure_levels
    envelope: dict[str, Any] = {
        "format_version": _BUNDLE_FORMAT_VERSION,
        "xg_column": xg_column,
        "pressure_column": pressure_column,
        "retention_variant_default": retention_variant_default,
        "pressure_levels": pl.to_meta(),
        "possession_value": {
            "l": int(pv.l),
            "w": int(pv.w),
            "method": pv.method,
            "xg_column": pv.xg_column,
            "provenance": pv.provenance,
            "surfaces": _surfaces_to_json(pv._surfaces),
            "support": _surfaces_to_json(pv._support),
        },
        "turnover_cost": {
            "l": int(tc.l),
            "w": int(tc.w),
            "window_seconds": tc.window_seconds,
            "min_support": int(tc.min_support),
            "coarsen": int(tc.coarsen),
            "surfaces": _surfaces_to_json(tc._surfaces),
            "support": _surfaces_to_json(tc._support),
            "levels": _surfaces_to_json(tc._levels),
        },
    }
    return json.dumps(envelope).encode("utf-8")


def deserialize_xt_gk_v2_bundle(data: bytes) -> XtGkV2Bundle:
    """Reconstruct the three fitted ports from the JSON envelope written by :func:`serialize_xt_gk_v2_bundle`.

    Reaches into the ports' private surface dicts because neither exposes a single-file loader compatible
    with ADR-012's ``.json`` delivery (see the module docstring). Guarded by the recorded format version.
    """
    from silly_kicks.xtgk import EmpiricalTurnoverValue, MarkovPossessionValue, PressureLevels

    obj = json.loads(data.decode("utf-8"))
    fmt = obj.get("format_version")
    if fmt != _BUNDLE_FORMAT_VERSION:
        raise ValueError(f"xt_gk_v2 bundle format {fmt!r} != expected {_BUNDLE_FORMAT_VERSION!r}")

    pl = PressureLevels.from_meta(obj["pressure_levels"])

    pv_obj = obj["possession_value"]
    pv = MarkovPossessionValue(l=int(pv_obj["l"]), w=int(pv_obj["w"]), method=pv_obj.get("method", "singh_counts"))
    pv._surfaces = _surfaces_from_json(pv_obj["surfaces"], dtype=float)
    pv._support = _surfaces_from_json(pv_obj["support"], dtype=int)
    pv.provenance = pv_obj.get("provenance", {})
    pv.xg_column = pv_obj.get("xg_column")
    pv.pressure_levels = pl
    pv._fitted = True

    tc_obj = obj["turnover_cost"]
    tc = EmpiricalTurnoverValue(
        l=int(tc_obj["l"]),
        w=int(tc_obj["w"]),
        window_seconds=tc_obj.get("window_seconds"),
        min_support=int(tc_obj.get("min_support", 30)),
        coarsen=int(tc_obj.get("coarsen", 4)),
    )
    tc._surfaces = _surfaces_from_json(tc_obj["surfaces"], dtype=float)
    tc._support = _surfaces_from_json(tc_obj["support"], dtype=int)
    tc._levels = _surfaces_from_json(tc_obj["levels"], dtype=int)
    tc._fitted = True

    return XtGkV2Bundle(
        possession_value=pv,
        turnover_cost=tc,
        pressure_levels=pl,
        xg_column=str(obj.get("xg_column", "")),
        pressure_column=str(obj.get("pressure_column", PRESSURE_COLUMN)),
        retention_variant_default=str(obj.get("retention_variant_default", "default")),
    )


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _retention_variant_for_provider(provider: object) -> str:
    """Map a provider to its bundled retention variant.

    Mirrors ``silly_kicks.xtgk._retention._PROVIDER_VARIANT`` (private): skillcorner routes to its own
    passing variant; every other provider falls back to ``default``. Replicated (not imported) to avoid a
    new private import; extend when a passing per-provider variant is bundled upstream.
    """
    return "skillcorner" if str(provider).lower() == "skillcorner" else "default"


def prepare_scoring_frame(actions: pd.DataFrame, *, pressure_column: str = PRESSURE_COLUMN) -> pd.DataFrame:
    """Pre-filter to GK-distribution rows and shape the frame for the xtgk-v2 API.

    * MANDATORY ``is_gk_distribution`` pre-filter (review-4 A3).
    * ``type_name`` -> ``type_id`` (extract_retention_features needs the numeric type).
    * ``PRESSURE_SOURCE_COLUMN`` -> ``pressure``, NaN coalesced to 0.0 — every GK-distribution row is
      frame-linked (tracking-derived), so a null pressure is a genuinely unpressured restart -> LOW
      tercile, matching ``coalesce_frame_present_null_pressure`` (frame-present & null -> 0.0). This also
      prevents ``PressureLevels.apply`` from raising on a NaN pressure.
    """
    import silly_kicks.spadl.config as spadlconfig

    domain = actions["is_gk_distribution"].fillna(False).to_numpy(dtype=bool)
    gk = actions.loc[domain].copy()
    if gk.empty:
        return gk

    if "type_id" not in gk.columns and "type_name" in gk.columns:
        gk["type_id"] = gk["type_name"].map(spadlconfig.actiontype_id)

    if pressure_column not in gk.columns and PRESSURE_SOURCE_COLUMN in gk.columns:
        gk[pressure_column] = pd.to_numeric(gk[PRESSURE_SOURCE_COLUMN], errors="coerce")
    if pressure_column in gk.columns:
        gk[pressure_column] = pd.to_numeric(gk[pressure_column], errors="coerce").fillna(0.0)
    return gk


def score_xt_gk_v2(
    actions: pd.DataFrame,
    bundle: XtGkV2Bundle,
    *,
    pressure_column: str = PRESSURE_COLUMN,
) -> pd.DataFrame:
    """Score ``xt_gk_v2`` for the GK-distribution rows of ``actions`` (identity + 6 v2 columns).

    Returns one row per in-domain action carrying the ``_IDENTITY_COLUMNS`` + :data:`V2_OUTPUT_COLUMNS`.
    Off-domain actions are simply absent (they get NULL v2 via the mart LEFT JOIN — correct).
    """
    from silly_kicks.xtgk import (
        GkRetentionModel,
        apply_resolved_gk_geometry,
        compute_xt_gk_v2,
        extract_retention_features,
    )

    gk = prepare_scoring_frame(actions, pressure_column=pressure_column)
    if gk.empty:
        return pd.DataFrame(columns=[*_IDENTITY_COLUMNS, *V2_OUTPUT_COLUMNS])

    frames: list[pd.DataFrame] = []
    # Per-provider retention (pooled possession/turnover); provider column carried on the AC corpus.
    for provider, group in gk.groupby("data_source", sort=False):
        resolved = apply_resolved_gk_geometry(group)
        rf = extract_retention_features(resolved, pressure_column=pressure_column)
        retention = GkRetentionModel.from_variant(_retention_variant_for_provider(provider))
        v2 = compute_xt_gk_v2(
            resolved,
            possession_value=bundle.possession_value,
            retention=retention,
            turnover_cost=bundle.turnover_cost,
            pressure_column=pressure_column,
            pressure_levels=bundle.pressure_levels,
            retention_features=rf,
        )
        out = pd.DataFrame(index=group.index)
        for col in _IDENTITY_COLUMNS:
            out[col] = group[col].to_numpy()
        for col in V2_VALUE_COLUMNS:
            out[col] = v2[col].to_numpy()
        out[GK_GEOMETRY_SOURCE_COLUMN] = resolved[GK_GEOMETRY_SOURCE_COLUMN].to_numpy()
        frames.append(out)

    result = pd.concat(frames, axis=0, ignore_index=True)
    return result[[*_IDENTITY_COLUMNS, *V2_OUTPUT_COLUMNS]]


# ---------------------------------------------------------------------------
# Bundle loading from UC Volume
# ---------------------------------------------------------------------------


def load_bundle_from_volume(spark: SparkSession, volume_path: str, *, filename: str = WEIGHTS_FILENAME) -> XtGkV2Bundle:
    """Read the fitted-bundle JSON from a UC Volume path and reconstruct the ports.

    Uses ``spark.read.format("binaryFile")`` — the serverless-safe read for UC Volume artifacts — rather
    than a bare ``open()`` on the ``/Volumes`` path, and verifies the sha256 sidecar the trainer writes
    (ADR-012 / SEC2), matching ``ingestion.xg_shot_scorer._load_champion_weights``.
    """
    from ingestion.utils import _load_volume_sidecar_hash, verify_artifact_hash

    artifact_path = f"{volume_path.rstrip('/')}/{MODEL_NAME}/{filename}"
    try:
        row = spark.read.format("binaryFile").load(artifact_path).first()
        if row is None:
            raise RuntimeError(f"UC Volume bundle is empty: {artifact_path}")
        data = bytes(row["content"])
    except Exception as exc:
        raise RuntimeError(
            f"xt_gk_v2 bundle not available at UC Volume {artifact_path}. Train + deliver a bundle "
            "via scripts/train_xt_gk_v2_hf.py before running the writer."
        ) from exc
    verify_artifact_hash(
        data=data,
        expected_sha256=_load_volume_sidecar_hash(artifact_path),
        artifact_label=f"{MODEL_NAME}_bundle_volume",
        logger=logger,
    )
    return deserialize_xt_gk_v2_bundle(data)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks)
# ---------------------------------------------------------------------------


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to score xt_gk_v2."
        )


def _output_spark_schema() -> StructType:
    """Explicit Spark schema for the scored DataFrame (ADR-033).

    Never let ``createDataFrame`` infer types for a typed Delta target: a value column that is all-NaN
    for a batch would infer to the wrong type (or NullType-and-drop), silently corrupting the write.
    Field order + types mirror ``[*_IDENTITY_COLUMNS, *V2_OUTPUT_COLUMNS]`` and ``XT_GK_V2_DDL``
    (``_ingested_at`` is added downstream by ``write_delta_table``).
    """
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("data_source", StringType(), True),
            StructField("match_id", StringType(), True),
            StructField("action_id", LongType(), True),
            StructField("xt_gk_v2_position", DoubleType(), True),
            StructField("xt_gk_v2_pev", DoubleType(), True),
            StructField("xt_gk_v2_retention_loss", DoubleType(), True),
            StructField("xt_gk_v2_dzv", DoubleType(), True),
            StructField("xt_gk_v2", DoubleType(), True),
            StructField("gk_geometry_source", StringType(), True),
        ]
    )


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    volume_path: str,
    *,
    bundle: XtGkV2Bundle | None = None,
) -> int:
    """Score ``xt_gk_v2`` over bronze ``spadl_action_context`` and write bronze ``xt_gk_v2_predictions``.

    Reads the v2-free corpus (bronze AC), scores the GK-distribution slice on the driver (a small
    fraction of actions), and writes the per-action predictions with a per-provider ``replaceWhere``.
    """
    from ingestion.utils import validate_dataframe, write_delta_table

    _assert_silly_kicks_min()
    if bundle is None:
        bundle = load_bundle_from_volume(spark, volume_path)

    source_table = f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.spadl_action_context"
    logger.info("Reading GK-distribution actions from %s", source_table)
    # Bounded by the .where filter: is_gk_distribution is a small fraction of the action corpus.
    pdf = spark.table(source_table).select(*INPUT_COLUMNS).where("is_gk_distribution = true").toPandas()
    logger.info("Loaded %d GK-distribution actions", len(pdf))

    scored = score_xt_gk_v2(pdf, bundle)
    logger.info("Scored %d xt_gk_v2 rows", len(scored))
    if scored.empty:
        logger.info("No xt_gk_v2 rows to write")
        return 0

    sdf = spark.createDataFrame(scored, schema=_output_spark_schema())
    providers = [str(r["data_source"]) for r in sdf.select("data_source").distinct().collect()]
    quoted = ", ".join(f"'{p}'" for p in providers)
    replace_where = f"data_source IN ({quoted})"
    row_count = validate_dataframe(sdf, list(scored.columns), MODEL_NAME, logger)
    return write_delta_table(
        sdf,
        catalog,
        DEFAULT_BRONZE_SCHEMA,
        BRONZE_TABLE,
        replace_where=replace_where,
        logger=logger,
        row_count=row_count,
    )


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score xt_gk_v2 to bronze")
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument(
        "--volume-path",
        default=f"/Volumes/{CATALOG}/dev_gold/model_weights",
        help="UC Volume root holding the xt_gk_v2/ fitted bundle",
    )
    args = parser.parse_args()

    if not IDENTIFIER_RE.match(args.catalog):
        raise SystemExit(f"Invalid catalog name: {args.catalog!r}")

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, DEFAULT_BRONZE_SCHEMA)
    run_pipeline(spark, args.catalog, args.volume_path)


if __name__ == "__main__":
    main()
