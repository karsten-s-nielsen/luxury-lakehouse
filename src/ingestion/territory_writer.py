"""Territorial-dominance scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``territory.compute_territorial_dominance`` family (TF-54 / TF-54b)
and lands it in the wide-by-granularity player-match mart ``fct_player_match_metrics`` (shared with
``duels``). Per ``(game_id, player_id)`` it measures a defender's territorial dominance -- the xT conceded
vs prevented inside the convex hull of their defensive actions -- reported for BOTH methods:

* **completed_failed** (v1, default): the 12 ``TERRITORY_METRIC_COLUMNS`` + the ``territory_hull_source``
  provenance (15 cols incl. keys).
* **counterfactual** (TF-54b): the v1 15 + 5 counterfactual-only columns (``territory_expected_threat_
  faced`` / ``territory_xt_prevented_above_expectation`` / ``territory_passes_aimed_into_hull`` [Int64] /
  ``territory_mean_completion_faced`` + the ``territory_target_source`` provenance) = 20 cols.

The bronze row carries the 20-column counterfactual union (12 v1 metric + ``territory_hull_source`` + 4 cf
metric + ``territory_target_source``) + native ids + access_tier.

**Two contracts [SPEC-R5-01].** ``TERRITORY_METRIC_COLUMNS`` = 12 and the counterfactual columns are
METHOD-CONDITIONAL, NOT in ``METRIC_CONTRACTS['territory']`` (which is v1-only, 12 metric + 2 keys). The
cf-only surface is obtained by set-diff of ``columns_for_method('counterfactual') -
columns_for_method('completed_failed')`` -- sk owns those 5 names + dtypes. The lakehouse guards the two
surfaces SEPARATELY (the registry parity NEVER folds the cf cols into the 12).

**xT + completion-model injection.** ``compute_territorial_dominance`` REQUIRES a FITTED ``ExpectedThreat``
(``.xT`` surface + ``.transition_matrix``) for both methods; the counterfactual method additionally
consumes the transition matrix through ``destination_profiles``. ExT-v2 (ADR-085): the writer LOADS the
canonical global sk ``ExpectedThreat`` (its persisted ``to_dict`` JSON, incl. the transition matrix) from
bronze ``expected_threat_grids`` -- the SAME single surface the AC drain uses, no driver re-fit -- and
broadcasts that JSON string into the ``applyInPandas`` closure
(reconstructed per group via :func:`_reconstruct_fitted_xt`). ``PassCompletionModel.bundled()`` (no-arg
fitted classmethod, 4.120.0 full-open-data re-fit) is constructed once inside the closure -- only the
counterfactual pass needs it.

**Event-only, all providers.** Beyond the fitted xt, ``compute_territorial_dominance`` needs only the
SPADL actions (``game_id`` / ``player_id`` / ``team_id`` / ``type_id`` / ``result_id`` / start+end coords).
It dispatches per match via ``applyInPandas`` (one bounded group per match), same posture as
``bravery_writer`` / ``team_metrics_writer``.

**Native ids (ADR-013).** The NATIVE ``player_id_native`` is fed as ``player_id`` and ``team_id_native`` as
``team_id`` so the emitted keys are native; ``fct_player_match_metrics`` resolves ``player_key`` via
``dim_players`` and ``match_key`` via ``dim_matches``.

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` read straight off
``bronze.spadl_actions`` (single-tier per match) -- a direct per-row stamp, never a publish-time join.

**Governance.** territory is per-PLAYER evaluative (a per-defender territorial-dominance measure) --
``wf-territory`` is a member of ``PER_PLAYER_EVALUATIVE_CARDS`` and carries the ``docs/huggingface/model-
cards/territory.md`` EU-AI-Act model card. It is RANKING-LICENSED per the sk defender-ranking census
(ICC lo=0.121>0), so the card MAY present a per-defender ranking (distinct from gk_decision's
instrument-not-ranking limit). Any cited PassCompletion correlation is a PRE-4.120 measure [SPEC-R4-02].

**Schema-drift guard (SK-EXPORT / sk:ADR-098).** The v1 metric column set is pinned to the uniform
``silly_kicks.metric_contracts.METRIC_CONTRACTS['territory']`` registry; the cf-only set is pinned to
``columns_for_method`` -- both via the parity test.

**Validation boundary (spec Part B).** The pure ``_compute_territory_group`` core is unit-tested on
fixtures; the Spark ``run_pipeline`` (xt fit + ``applyInPandas`` dispatch) is validated by the live Part-B
recompute, same posture as the sibling ADR-013 writers.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from silly_kicks import territory as _sk_territory
from silly_kicks.territory import TERRITORY_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

# columns_for_method is an sk re-export pyright misses (it resolves the module's constants but not this
# def re-export); access via the module object so the attribute-access ignore stays local.
_columns_for_method = _sk_territory.columns_for_method  # pyright: ignore[reportAttributeAccessIssue]

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (AGENTS.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "territory"
SPADL_TABLE = "spadl_actions"

# All event providers (territory is event-only -- it needs no tracking frames).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions. compute_territorial_dominance reads game_id / player_id /
# team_id / type_id / result_id / start_x / start_y / end_x / end_y; the writer feeds player_id/team_id
# NATIVE. Plus the identity trio + access_tier stamped onto the emitted rows.
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "access_tier",
    "game_id",
    "team_id_native",
    "player_id_native",
    "type_id",
    "result_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
)

# The canonical SPADL columns compute_territorial_dominance reads (player_id / team_id supplied native).
_SK_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "type_id",
    "result_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
)

# Native identity + access_tier stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "player_id", "team_id", "access_tier")

# silly-kicks 4.120.0 v1 (completed_failed) metric columns (12), in order. Pinned to the SK-EXPORT
# METRIC_CONTRACTS registry by test_territory_writer.test_territory_v1_registry_parity.
_TERRITORY_METRIC_COLUMNS: tuple[str, ...] = tuple(TERRITORY_METRIC_COLUMNS)

# The v1 provenance column (present for BOTH methods) -- columns_for_method('completed_failed') minus keys
# minus the 12 metric cols.
_V1_PROVENANCE_COLUMN = "territory_hull_source"

# The 5 counterfactual-only columns [SPEC-R5-01]: columns_for_method('counterfactual') minus
# columns_for_method('completed_failed'). sk owns these names + dtypes; NOT in the registry.
_CF_ONLY_COLUMNS: tuple[str, ...] = tuple(
    c for c in _columns_for_method("counterfactual") if c not in _columns_for_method("completed_failed")
)

# The 20-col counterfactual-union metric/provenance surface (12 v1 metric + hull_source + 5 cf-only), in
# order: v1 metric cols, the v1 provenance col, then the cf-only cols (4 metric + target_source).
_METRIC_AND_PROVENANCE_COLUMNS: tuple[str, ...] = (
    *_TERRITORY_METRIC_COLUMNS,
    _V1_PROVENANCE_COLUMN,
    *_CF_ONLY_COLUMNS,
)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_METRIC_AND_PROVENANCE_COLUMNS)

# sk territory column_types: territory_passes_into_hull / territory_defensive_actions_in_hull /
# territory_passes_aimed_into_hull are Int64 -> long; the provenance cols are string; the rest are double.
_LONG_METRICS: frozenset[str] = frozenset(
    {
        "territory_passes_into_hull",
        "territory_defensive_actions_in_hull",
        "territory_passes_aimed_into_hull",
    }
)
_STRING_METRICS: frozenset[str] = frozenset({_V1_PROVENANCE_COLUMN, "territory_target_source"})


def _metric_type(col: str) -> str:
    """Spark SQL type for a metric/provenance column (sk territory column_types + cf dtypes)."""
    if col in _STRING_METRICS:
        return "string"
    if col in _LONG_METRICS:
        return "long"
    return "double"


_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "player_id": "string",
    "team_id": "string",
    "access_tier": "string",
    **{c: _metric_type(c) for c in _METRIC_AND_PROVENANCE_COLUMNS},
}


def _ddl() -> str:
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-territory migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE", "long": "BIGINT"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


TERRITORY_DDL = _ddl()


# ---------------------------------------------------------------------------
# Fitted-xT injection (load the canonical global sk ExpectedThreat to_dict from bronze; broadcast the JSON)
# ---------------------------------------------------------------------------


def _load_global_xt_model_json(spark: Any, catalog: str) -> str:
    """Load the canonical global sk ``ExpectedThreat.to_dict()`` JSON from bronze (ExT-v2 / ADR-085).

    The single surface the AC drain also consumes: the persisted ``to_dict`` carries the
    ``.transition_matrix`` the counterfactual method needs (via ``destination_profiles``), so territory
    no longer driver-fits its own xT. The JSON string is picklable → broadcast into the closure and
    reconstructed per group via :func:`_reconstruct_fitted_xt`.
    """
    table = f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.expected_threat_grids"
    rows = spark.sql(
        f"SELECT xt_model_json FROM {table} WHERE competition_id = 'global'"  # noqa: S608
    ).collect()
    if not rows or not rows[0]["xt_model_json"]:
        raise RuntimeError(f"No global xT model in {table}. Run compute_expected_threat before territory.")
    return str(rows[0]["xt_model_json"])


def _reconstruct_fitted_xt(xt_model_json: str) -> Any:
    """Rebuild a FITTED ``ExpectedThreat`` (``.xT`` + ``.transition_matrix``) from the broadcast to_dict JSON."""
    import json

    from silly_kicks.xthreat import ExpectedThreat

    return ExpectedThreat.from_dict(json.loads(xt_model_json))


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _score_territory(actions: pd.DataFrame, xt: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run BOTH territory methods on one match's actions -> (v1_samples, cf_samples).

    Feeds the NATIVE ``player_id_native`` / ``team_id_native`` as ``player_id`` / ``team_id``. The v1
    (completed_failed) pass yields 15 cols (2 keys + 12 metric + hull_source); the counterfactual pass
    yields 20 (v1 15 + the 5 cf-only). ``PassCompletionModel.bundled()`` is constructed once here (only the
    counterfactual pass needs it).
    """
    # CounterfactualParams is an sk re-export pyright misses (resolves at runtime); import via the module
    # object so the attribute-access ignore is local and does not blanket the whole import statement.
    from silly_kicks import territory as _territory
    from silly_kicks.expected_passing import PassCompletionModel

    compute_territorial_dominance = _territory.compute_territorial_dominance
    counterfactual_params = _territory.CounterfactualParams  # pyright: ignore[reportAttributeAccessIssue]

    scoring_input = actions[list(_SK_INPUT_COLUMNS)].copy()
    scoring_input["player_id"] = actions["player_id_native"].to_numpy()
    scoring_input["team_id"] = actions["team_id_native"].to_numpy()

    v1_samples, _v1_report = compute_territorial_dominance(scoring_input, xt=xt)
    pcm = PassCompletionModel.bundled()
    # pyright reads a stale/truncated view of compute_territorial_dominance's signature (it resolves the
    # NAME but not the 4.120.0 completion_model / cf_params kwargs, which ARE present in the installed
    # _compute.py); the runtime contract is verified by test_territory_writer. The kwargs are bundled into
    # a dict so the single-line ignore covers pyright's per-argument reportCallIssue.
    cf_kwargs = {"method": "counterfactual", "completion_model": pcm, "cf_params": counterfactual_params()}
    cf_samples, _cf_report = compute_territorial_dominance(scoring_input, xt=xt, **cf_kwargs)  # pyright: ignore[reportCallIssue]
    return v1_samples, cf_samples


def _compute_territory_group(
    actions: pd.DataFrame,
    xt_model_json: str,
) -> pd.DataFrame:
    """Territorial dominance for ONE match's actions -> identity + 20-col cf-union, grain (match, player).

    ``actions`` is one match's ``bronze.spadl_actions`` rows: it carries ``data_source`` /
    ``match_id_native`` / ``access_tier`` (single-valued for a match) and the SPADL columns. Runs BOTH
    methods and LEFT-merges the 5 cf-only cols onto the v1 samples on ``TERRITORY_KEYS`` -> the 20-col
    row. Returns an empty full-schema frame when the match has no scorable rows.
    """
    import pandas as pd

    if actions.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    access_tier = str(actions["access_tier"].iloc[0]) if actions["access_tier"].notna().any() else None

    xt = _reconstruct_fitted_xt(xt_model_json)
    v1_samples, cf_samples = _score_territory(actions, xt)
    if v1_samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    from silly_kicks.territory import TERRITORY_KEYS

    keys = list(TERRITORY_KEYS)
    cf_slice = cf_samples[[*keys, *_CF_ONLY_COLUMNS]].copy()
    merged = v1_samples.merge(cf_slice, on=keys, how="left")

    # territory keys on (game_id, player_id) only -- it emits NO team column. The player's native team is
    # resolved from a per-match player_id_native -> team_id_native map (a defender belongs to one team).
    team_map = dict(zip(actions["player_id_native"], actions["team_id_native"], strict=False))

    out = merged.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    out["team_id"] = out["player_id"].map(team_map).astype("string")
    out["player_id"] = out["player_id"].astype("string")
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
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score territory."
        )


def _make_territory_udf(xt_model_json: str) -> Any:
    """Build the ``applyInPandas`` closure capturing the broadcast xT to_dict JSON (picklable)."""

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return _compute_territory_group(pdf, xt_model_json)

    return _udf


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score territorial dominance over every match of ``providers`` -> bronze ``territory`` (replaceWhere).

    LOADS the canonical global sk ``ExpectedThreat`` (``to_dict``) from bronze -- the same single surface
    the AC drain uses -- broadcasts its JSON into the closure, and dispatches per ``(data_source,
    match_id_native)`` group via
    ``applyInPandas``. Writes each provider's slice idempotently. Returns the total rows written.
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

    # ExT-v2 (ADR-085): load the canonical global sk ExpectedThreat (to_dict) from bronze -- the SAME
    # single surface the AC drain consumes -- instead of a driver re-fit. The persisted to_dict carries the
    # .transition_matrix the counterfactual method needs; from_dict reconstructs per group. This removes the
    # former full-fact narrow-column driver .toPandas() fit (and its _topandas_exemptions.yml entry).
    xt_model_json = _load_global_xt_model_json(spark, catalog)

    udf = _make_territory_udf(xt_model_json)
    scored = source.groupBy("data_source", "match_id_native").applyInPandas(udf, schema=schema_out)

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
    logger.info("territory: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score territorial dominance (per match, per player) to bronze")
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
