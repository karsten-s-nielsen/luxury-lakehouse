"""Team-match KPI scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``team_metrics.compute_team_kpis`` family (TF-52) and lands it in
the wide-by-granularity team-match mart ``fct_team_metrics`` (shared with ``match_outcome``). The 44
per-``(game_id, team_id)`` KPIs are the Twelve match-report glossary (PPDA, field tilt, pass tempo, line
heights, recoveries, conversion chain) plus the event-based practitioner set (counter-press windows,
post-regain security, build-up taxonomy, breakout-by-channel, switch-conditioned press).

**Event-only, all providers.** ``compute_team_kpis`` needs ONLY the SPADL actions (canonical SPADL
columns + the ``shot_blocked`` / ``cross_blocked`` enrichments; it rests on ``spadl.add_possessions``
which derives possessions internally). So the writer reads ``bronze.spadl_actions`` directly and covers
every event provider (statsbomb / wyscout / idsse / metrica / skillcorner / gradientsports). It dispatches
per match via ``applyInPandas`` -- one bounded group per match, far more scalable than a per-match driver
round-trip over the ~3.5k-match StatsBomb corpus (same posture as ``bravery_writer``).

**Native ids (ADR-013).** ``compute_team_kpis`` groups on whatever ``team_id`` it is handed, so the
writer feeds it the NATIVE ``team_id_native`` (never the hashed BIGINT surrogate) -> the emitted
``team_id`` is the native team id, and ``fct_team_metrics`` resolves it to ``team_key`` via ``dim_teams``
on ``(provider, native_team_id)``. ``match_id`` is the native match id, resolved to ``match_key`` via
``dim_matches``.

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` read straight
off ``bronze.spadl_actions`` (a match is single-tier) -- a direct per-row stamp, never a publish-time
join. Fail-safe classification already happened at SPADL write time (``stamp_access_tier``).

**Governance.** team_metrics is a per-TEAM aggregate, NOT a per-player-evaluative system -- it carries
no EU-AI-Act model card and is deliberately excluded from ``PER_PLAYER_EVALUATIVE_CARDS`` (the KPIs are
team-structural, not individual-player evaluations).

**Schema-drift guard (SK-EXPORT / sk:ADR-098).** The metric column set is pinned to the uniform
``silly_kicks.metric_contracts.METRIC_CONTRACTS['team_metrics']`` registry via the parity test -- the
writer imports the ordered ``TEAM_KPI_METRIC_COLUMNS`` constant, and the test asserts equality against
the registry with zero per-package name knowledge.

**Validation boundary (spec Part B).** The pure ``_compute_team_kpis_group`` core is unit-tested on
fixtures; the Spark ``run_pipeline`` (``applyInPandas`` dispatch) is validated by the live Part-B
recompute, same posture as the sibling ADR-013 writers.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from silly_kicks.team_metrics import TEAM_KPI_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 121, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "team_metrics"
SPADL_TABLE = "spadl_actions"

# All event providers (team_metrics is event-only -- it needs no tracking frames).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions. compute_team_kpis consumes canonical SPADL columns (it rests
# on spadl.add_possessions, which derives possessions from these) -- start_x/start_y/end_x/end_y,
# time_seconds, type_id, result_id, bodypart_id, period_id, action_id, player_id + the native identity
# and access_tier. team_id is fed NATIVE (team_id_native).
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "access_tier",
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id_native",
    "player_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "type_id",
    "result_id",
    "bodypart_id",
    "shot_blocked",
    "cross_blocked",
)

# The canonical SPADL columns compute_team_kpis reads (team_id supplied from team_id_native below).
_SK_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "player_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "type_id",
    "result_id",
    "bodypart_id",
    "shot_blocked",
    "cross_blocked",
)

# Native identity + access_tier stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "team_id", "access_tier")

# silly-kicks 4.120.0 compute_team_kpis metric columns (TEAM_KPI_METRIC_COLUMNS), in order. Pinned to
# the SK-EXPORT registry by test_team_metrics_writer.test_team_metrics_mart_matches_sk_columns.
_TEAM_KPI_METRIC_COLUMNS: tuple[str, ...] = tuple(TEAM_KPI_METRIC_COLUMNS)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_TEAM_KPI_METRIC_COLUMNS)

# The sk 'team_metrics' column_types (SK-EXPORT registry) mapped to Spark SQL types. float64 -> double,
# Int64 -> long (nullable). The 4 identity columns are all string.
_FLOAT_METRICS: frozenset[str] = frozenset(
    {
        "ppda",
        "defensive_intensity",
        "time_to_defensive_action_s",
        "time_to_recovery_s",
        "recoveries_within_ns_pct",
        "counterpress_regain_pct",
        "field_tilt_pct",
        "pass_tempo",
        "long_ball_pct",
        "defensive_action_height_m",
        "recovery_line_height_m",
        "turnover_line_height_m",
        "poss_to_final_third_pct",
        "final_third_to_box_pct",
        "box_to_shot_pct",
        "breakout_left_pct",
        "breakout_center_pct",
        "breakout_right_pct",
        "possessions_retained_after_ns_pct",
        "buildup_success_pct",
        "post_regain_second_pass_pct",
        "post_regain_forward_first_pct",
        "switch_press_success_pct",
    }
)


def _metric_type(col: str) -> str:
    """Spark SQL type for a metric column: float64 -> double, else Int64 -> long (sk column_types)."""
    return "double" if col in _FLOAT_METRICS else "long"


_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "team_id": "string",
    "access_tier": "string",
    **{c: _metric_type(c) for c in _TEAM_KPI_METRIC_COLUMNS},
}


def _ddl() -> str:
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-team-metrics migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE", "long": "BIGINT"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


TEAM_METRICS_DDL = _ddl()


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _score_team_kpis(actions: pd.DataFrame) -> tuple[pd.DataFrame, Any]:
    """Run silly-kicks ``compute_team_kpis`` on one match's actions -> (samples, TeamKpiReport).

    Feeds the NATIVE ``team_id_native`` as ``team_id`` so the emitted keys are native. Returns the raw
    sk output (game_id / team_id keys + 44 metric columns) and the conservation report.
    """
    from silly_kicks.team_metrics import compute_team_kpis

    scoring_input = actions[list(_SK_INPUT_COLUMNS)].copy()
    scoring_input["team_id"] = actions["team_id_native"].to_numpy()
    return compute_team_kpis(scoring_input, xg_column=None)


def _compute_team_kpis_group(actions: pd.DataFrame) -> pd.DataFrame:
    """Team KPIs for ONE match's actions -> identity + 44 metric columns, grain (match, team).

    ``actions`` is one match's ``bronze.spadl_actions`` rows: it carries ``data_source`` /
    ``match_id_native`` / ``access_tier`` (all single-valued for a match) and the SPADL/enrichment
    columns. Returns an empty frame with the full schema when the match has no scorable rows
    (compute_team_kpis's own empty contract).
    """
    import pandas as pd

    if actions.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    access_tier = str(actions["access_tier"].iloc[0]) if actions["access_tier"].notna().any() else None

    samples, _report = _score_team_kpis(actions)
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
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "string": StringType()}
    return StructType([StructField(c, type_map[_OUTPUT_TYPES[c]], True) for c in OUTPUT_COLUMNS])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score team_metrics."
        )


def _team_kpis_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas closure: one match's SPADL rows -> its team-KPI rows (module-level, picklable)."""
    return _compute_team_kpis_group(pdf)


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score team KPIs over every match of ``providers`` -> bronze ``team_metrics`` (per-provider replaceWhere).

    Dispatches per ``(data_source, match_id_native)`` group via ``applyInPandas``; writes each
    provider's slice idempotently. Returns the total rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    _assert_silly_kicks_min()
    schema_out = _struct_type()

    quoted = ", ".join(f"'{p}'" for p in providers)
    source = (
        spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{SPADL_TABLE}")
        .where(f"data_source IN ({quoted})")
        .select(*_SPADL_READ_COLUMNS)
    )
    scored = source.groupBy("data_source", "match_id_native").applyInPandas(_team_kpis_udf, schema=schema_out)

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
    logger.info("team_metrics: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score team-match KPIs (per match, per team) to bronze")
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
