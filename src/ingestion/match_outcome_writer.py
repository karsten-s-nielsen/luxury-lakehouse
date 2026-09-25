"""Match-outcome scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``match_outcome.compute_match_outcome`` family (TF-53) and lands it
in the wide-by-granularity team-match mart ``fct_team_metrics`` (shared with ``team_metrics``). Per
``(game_id, team_id)`` it integrates the injected per-shot xG into a win / draw / loss simplex plus
xPoints and expected_goals via an exact Poisson-binomial core with two orthogonal honesty corrections
(possession-collapse + Dixon-Coles team dependence), both ON by default (sk:ADR-097).

**xG injection (REQUIRED).** ``compute_match_outcome`` takes ``xg_column`` with NO default -- the whole
metric is an xG integration. The lakehouse serves pre-shot xG via ``xg_model_v3`` into the native-keyed
``bronze.xg_shot_predictions`` table (the same source ``defensive_credit_writer`` reads). The writer
LEFT-JOINs the per-shot xG onto the actions on the native shot identity
``(data_source, match_id_native, action_id)`` -- the shot's own ``action_id`` is the join key; non-shot
actions get NaN xG, which is correct (compute_match_outcome integrates only the shot rows).

**Event-only, all providers.** Beyond the xG column, ``compute_match_outcome`` needs only the SPADL
actions. It dispatches per match via ``applyInPandas`` (one bounded group per match), same posture as
``bravery_writer`` / ``team_metrics_writer``.

**Native ids (ADR-013).** The NATIVE ``team_id_native`` is fed as ``team_id`` so the emitted keys are
native; ``fct_team_metrics`` resolves ``team_key`` via ``dim_teams`` and ``match_key`` via ``dim_matches``.

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` read straight off
``bronze.spadl_actions`` (single-tier per match) -- a direct per-row stamp, never a publish-time join.

**Governance.** match_outcome is a per-TEAM win-probability aggregate, NOT a per-player-evaluative system
-- no EU-AI-Act model card; deliberately excluded from ``PER_PLAYER_EVALUATIVE_CARDS``.

**Schema-drift guard (SK-EXPORT).** The metric column set is pinned to
``silly_kicks.metric_contracts.METRIC_CONTRACTS['match_outcome']`` via the parity test.

**Validation boundary (spec Part B).** The pure ``_compute_match_outcome_group`` core is unit-tested on
fixtures; the Spark ``run_pipeline`` (``applyInPandas`` dispatch + the xG merge) is validated by the live
Part-B recompute, same posture as ``defensive_credit_writer``.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from silly_kicks.match_outcome import MATCH_OUTCOME_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (AGENTS.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "match_outcome"
SPADL_TABLE = "spadl_actions"
XG_PRED_TABLE = "xg_shot_predictions"
XG_COLUMN = "xg"

# All event providers (match_outcome is event-only -- it needs no tracking frames).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions. compute_match_outcome reads game_id / team_id / type_id /
# result_id (owngoal is encoded via result_id, not a separate column) + the injected xg; its default
# possession-collapse correction (sk:ADR-097) runs spadl.add_possessions, which additionally requires
# action_id / period_id / time_seconds. The rest are the canonical identity / access_tier (action_id
# doubles as the xG-merge join key).
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "access_tier",
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id_native",
    "type_id",
    "result_id",
)

# The xG-prediction columns the writer reads (native-keyed -- bronze.xg_shot_predictions).
XG_PRED_COLUMNS: tuple[str, ...] = ("data_source", "match_id_native", "action_id", XG_COLUMN)

# The columns compute_match_outcome reads (team_id supplied from team_id_native; xg injected). The
# possession-collapse correction's add_possessions needs action_id / period_id / time_seconds too.
_SK_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "type_id",
    "result_id",
)

# Native identity + access_tier stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "team_id", "access_tier")

# silly-kicks 4.120.0 compute_match_outcome metric columns (p_win/p_draw/p_loss/xpoints/expected_goals),
# in order. Pinned to the SK-EXPORT registry by test_match_outcome_writer.
_MATCH_OUTCOME_METRIC_COLUMNS: tuple[str, ...] = tuple(MATCH_OUTCOME_METRIC_COLUMNS)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_MATCH_OUTCOME_METRIC_COLUMNS)

# sk 'match_outcome' column_types (SK-EXPORT registry): all 5 metric cols are float64 -> double.
_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "team_id": "string",
    "access_tier": "string",
    **{c: "double" for c in _MATCH_OUTCOME_METRIC_COLUMNS},
}


def _ddl() -> str:
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-match-outcome migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


MATCH_OUTCOME_DDL = _ddl()


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _score_match_outcome(actions: pd.DataFrame) -> tuple[pd.DataFrame, Any]:
    """Run silly-kicks ``compute_match_outcome`` on one match's actions (with xg) -> (samples, report).

    Feeds the NATIVE ``team_id_native`` as ``team_id``. ``actions`` must already carry the ``xg`` column
    (see :func:`attach_xg`). Default ``MatchOutcomeParams()`` runs both honesty corrections (sk:ADR-097).
    """
    from silly_kicks.match_outcome import compute_match_outcome

    scoring_input = actions[[*_SK_INPUT_COLUMNS, XG_COLUMN]].copy()
    scoring_input["team_id"] = actions["team_id_native"].to_numpy()
    return compute_match_outcome(scoring_input, xg_column=XG_COLUMN)


def _compute_match_outcome_group(actions: pd.DataFrame) -> pd.DataFrame:
    """Match outcome for ONE match's actions -> identity + 5 metric columns, grain (match, team).

    ``actions`` is one match's ``bronze.spadl_actions`` rows already LEFT-joined to per-shot xG. It
    carries ``data_source`` / ``match_id_native`` / ``access_tier`` (single-valued for a match). Returns
    an empty frame with the full schema when the match has no scorable rows.
    """
    import pandas as pd

    if actions.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    access_tier = str(actions["access_tier"].iloc[0]) if actions["access_tier"].notna().any() else None

    samples, _report = _score_match_outcome(actions)
    if samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    out = samples.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    out["team_id"] = out["team_id"].astype("string")
    out["access_tier"] = access_tier
    return out[list(OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) -- validated by the live Part-B gate, not unit tests
# ---------------------------------------------------------------------------


def _struct_type() -> Any:
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    type_map = {"double": DoubleType(), "string": StringType()}
    return StructType([StructField(c, type_map[_OUTPUT_TYPES[c]], True) for c in OUTPUT_COLUMNS])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score match_outcome."
        )


def _match_outcome_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas closure: one match's SPADL+xg rows -> its outcome rows (module-level, picklable)."""
    return _compute_match_outcome_group(pdf)


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score match outcome over every match of ``providers`` -> bronze ``match_outcome`` (replaceWhere).

    LEFT-joins per-shot xG (bronze.xg_shot_predictions) onto the SPADL actions on the native shot
    identity, then dispatches per ``(data_source, match_id_native)`` group via ``applyInPandas``. Writes
    each provider's slice idempotently. Returns the total rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    _assert_silly_kicks_min()
    schema_out = _struct_type()

    quoted = ", ".join(f"'{p}'" for p in providers)
    actions = (
        spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{SPADL_TABLE}")
        .where(f"data_source IN ({quoted})")
        .select(*_SPADL_READ_COLUMNS)
    )
    xg_preds = (
        spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{XG_PRED_TABLE}")
        .where(f"data_source IN ({quoted})")
        .select(*XG_PRED_COLUMNS)
    )
    joined = actions.join(xg_preds, on=["data_source", "match_id_native", "action_id"], how="left")
    scored = joined.groupBy("data_source", "match_id_native").applyInPandas(_match_outcome_udf, schema=schema_out)

    total = 0
    for provider in providers:
        slice_df = scored.where(F.col("data_source") == provider)
        total += write_delta_table(
            slice_df,
            catalog,
            DEFAULT_BRONZE_SCHEMA,
            BRONZE_TABLE,
            replace_where=f"data_source = '{provider}'",
            logger=logger,
        )
    logger.info("match_outcome: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score match outcome (win/draw/loss, xPoints) to bronze")
    parser.add_argument("--catalog", default=CATALOG)
    args = parser.parse_args()
    if not IDENTIFIER_RE.match(args.catalog):
        raise SystemExit(f"Invalid catalog name: {args.catalog!r}")

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]
    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, DEFAULT_BRONZE_SCHEMA)
    run_pipeline(spark, args.catalog)


if __name__ == "__main__":
    main()
