"""Compute tracking-modality PSxG predictions → bronze (ADR-013 inference writer).

Loads the PSxG model (the same 2-feature logistic the StatsBomb path delivers),
reads the on-target *tracking* shots from gold ``fct_action_context`` (joined to
``fct_action_values`` for the goal label used to fit calibration), scores them
via :mod:`analytics.psxg_tracking`, fits an out-of-sample Platt recalibration
(GroupKFold by match), and writes ``bronze.psxg_tracking_predictions`` keyed
``(match_key, action_id, data_source)``.

Coverage: GradientSports / SkillCorner / IDSSE (Metrica is auto-excluded — its
bronze has no ball-z, so ``shot_crossing_z IS NULL``). Tiny driver-side workload
(~hundreds of on-target shots) — ``.toPandas()`` is bounded by the on-target
filter (see ``src/tests/_topandas_exemptions.yml`` if the boundedness gate flags it).

The model itself is unchanged by this writer (the StatsBomb retrain is a separate
delivery); StatsBomb rows carry ``psxg_calibration = 'none'`` in the unified fact,
tracking rows carry ``'platt'``.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import TYPE_CHECKING

from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

    from analytics.goalkeeper import PSxGModel

TABLE_NAME = "psxg_tracking_predictions"
_NORMALIZATION_VERSION = "spadl-goalwidth-7.32-v1"
# Tracking providers with a real ball-z signal resolve naturally via the z guard
# (Metrica's shot_crossing_z is NULL — no bronze ball height — so it is excluded).
_SHOT_TYPES = ("shot", "shot_freekick", "shot_penalty")

logger = logging.getLogger("compute_psxg_tracking")


def _read_on_target_tracking_shots(spark: SparkSession, catalog: str, gold_schema: str) -> pd.DataFrame:
    """Bounded read of on-target tracking shots + goal label (driver-side pandas)."""
    shot_types = ", ".join(f"'{t}'" for t in _SHOT_TYPES)
    sql = f"""
        SELECT ac.match_key,
               ac.action_id,
               ac.data_source,
               ac.shot_crossing_y,
               ac.shot_crossing_z,
               ac.shot_crossing_confidence,
               ac.shot_fit_rmse,
               av.action_result
        FROM {catalog}.{gold_schema}.fct_action_context ac
        LEFT JOIN {catalog}.{gold_schema}.fct_action_values av
          ON ac.match_key = av.match_key AND ac.action_id = av.action_id
        WHERE ac.type_name IN ({shot_types})
          AND ac.shot_on_target_derived = true
          AND ac.shot_crossing_z IS NOT NULL
    """  # noqa: S608 — identifiers validated by IDENTIFIER_RE; shot_types is a literal allowlist
    return spark.sql(sql).toPandas()


def build_predictions(shots: pd.DataFrame, model: PSxGModel, *, model_version: str) -> pd.DataFrame:
    """Pure: score + calibrate on-target tracking shots → bronze prediction rows.

    Drops ``yellow_card`` shot rows with a logged count (D-F: no silent caps);
    fits out-of-sample Platt on the gate-passed, labelled subset.
    """
    import numpy as np
    import pandas as pd

    from analytics.psxg_tracking import apply_platt, fit_platt_calibration, score_tracking_psxg

    n_yellow = int((shots["action_result"] == "yellow_card").sum())
    if n_yellow:
        logger.info("Dropping %d yellow_card shot rows (off-scope for PSxG)", n_yellow)
    shots = shots[shots["action_result"] != "yellow_card"].copy()
    shots["is_goal"] = (shots["action_result"] == "success").astype("int64")

    scored = score_tracking_psxg(shots, model)

    # Fit Platt on gate-passed, labelled shots (out-of-sample CV reported).
    fit_mask = (~scored["psxg_gated"].to_numpy(dtype=bool)) & scored["psxg"].notna().to_numpy()
    fitted = scored.loc[fit_mask]
    if len(fitted) == 0:
        raise RuntimeError("no gate-passed tracking shots to calibrate — check the gate thresholds")
    report = fit_platt_calibration(
        fitted["psxg"].to_numpy(dtype=float),
        fitted["is_goal"].to_numpy(dtype=int),
        fitted["match_key"].to_numpy(),
    )
    logger.info(
        "Platt calibration: n=%d groups=%d cv_brier=%.4f (uncalibrated=%.4f)",
        report.n_shots,
        report.n_groups,
        report.cv_brier,
        report.cv_brier_uncalibrated,
    )
    for provider, grp in fitted.groupby("data_source"):
        logger.info("  reliability[%s]: n=%d goal_rate=%.3f", provider, len(grp), float(grp["is_goal"].mean()))

    raw = scored["psxg"].to_numpy(dtype=float)
    recal = apply_platt(raw, report.params)
    # Gated rows have NaN raw psxg → keep recalibrated NaN too.
    recal = np.where(scored["psxg_gated"].to_numpy(dtype=bool), np.nan, recal)
    platt_version = f"platt-gkf-n{report.n_shots}-g{report.n_groups}"

    out = pd.DataFrame(
        {
            "match_key": scored["match_key"].to_numpy(),
            "action_id": scored["action_id"].to_numpy(),
            "data_source": scored["data_source"].astype(str).to_numpy(),
            "psxg": raw,
            "psxg_recalibrated": recal,
            "psxg_gated": scored["psxg_gated"].astype("boolean").to_numpy(),
            "psxg_calibration": "platt",
            "model_version": model_version,
            "platt_version": platt_version,
            "normalization_version": _NORMALIZATION_VERSION,
        }
    )
    return out


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    bronze_schema: str,
    model_path: str,
    *,
    model_version: str,
) -> int:
    """Score on-target tracking shots and write bronze.psxg_tracking_predictions."""
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from analytics.goalkeeper import load_psxg_model

    model = load_psxg_model(model_path)
    shots = _read_on_target_tracking_shots(spark, catalog, gold_schema)
    logger.info("Read %d on-target tracking shots", len(shots))
    if len(shots) == 0:
        logger.warning("No on-target tracking shots found — nothing to write")
        return 0

    out_pdf = build_predictions(shots, model, model_version=model_version)

    sdf = spark.createDataFrame(out_pdf).withColumn("_ingested_at", spark_fn.current_timestamp())
    table = f"{catalog}.{bronze_schema}.{TABLE_NAME}"
    providers = [str(r["data_source"]) for r in sdf.select("data_source").distinct().collect()]
    replace_where = "data_source IN (" + ", ".join(f"'{p}'" for p in providers) + ")"
    start = time.time()
    (
        sdf.write.format("delta")
        .option("mergeSchema", "true")
        .option("replaceWhere", replace_where)
        .mode("overwrite")
        .saveAsTable(table)
    )
    elapsed = time.time() - start
    logger.info("Wrote %d rows to %s (replaceWhere=%s) in %.2fs", len(out_pdf), table, replace_where, elapsed)
    return 0


def main() -> None:
    """CLI entry point — compute tracking PSxG predictions."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Compute tracking PSxG predictions to bronze")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--gold-schema", default="dev_gold")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--model-path", required=True, help="UC Volume path to psxg_model.json")
    parser.add_argument("--model-version", required=True, help="PSxG model version being scored")
    args = parser.parse_args()

    identifiers = [
        ("catalog", args.catalog),
        ("gold-schema", args.gold_schema),
        ("bronze-schema", args.bronze_schema),
    ]
    for field_name, value in identifiers:
        if not IDENTIFIER_RE.match(value):
            raise SystemExit(f"Invalid {field_name} '{value}': must match {IDENTIFIER_RE.pattern}")

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]
    raise SystemExit(
        run_pipeline(
            spark,
            args.catalog,
            args.gold_schema,
            args.bronze_schema,
            args.model_path,
            model_version=args.model_version,
        )
    )


if __name__ == "__main__":
    main()
