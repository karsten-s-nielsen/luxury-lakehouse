"""GK shot-stopping scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``shot_stopping.compute_shot_stopping`` family (TF-59 PR2) and
lands it in ``bronze.shot_stopping`` -- Goals Prevented / GSAA per ``(game_id, defending keeper)``, with
and without in-play penalties (the 8 ``SHOT_STOPPING_METRIC_COLUMNS``). It SUPERSEDES the inline GSAA
rollup in the three GK marts: ``fct_gk_shot_stopping`` / ``fct_gk_shot_stopping_pooled`` re-source their
GSAA/psxg columns from this writer's ``stg_shot_stopping`` (canonical-id keeper grouping fixes
cross-provider keeper-id fragmentation; the ``_excl_penalties`` split + honest attribution census are the
real gains), and ``fct_goalkeeper_stats.psxg_agg`` follows.

**Event-only + injected Post-Shot xG.** ``compute_shot_stopping`` reads SPADL ``actions`` (``game_id`` /
``type_id`` / ``result_id`` / ``period_id`` / ``shot_blocked``) + an injected per-shot Post-Shot xG
(``psxg_column``; silly-kicks ships no xG/PSxG model) + the PR1-stamped ``defending_gk_player_id`` /
``defending_gk_team_id`` columns. The writer LEFT-JOINs the gold PSxG fact ``fct_shot_psxg`` per shot on
the native shot identity ``(match_key, action_id)`` (its ``psxg_recalibrated`` is Post-Shot xG). A
non-shot / gate-failed row gets NaN psxg, which is exactly the on-target gate ``compute_shot_stopping``
applies (PSxG presence IS the on-target gate).

**defending_gk_team_id derivation (owner-decided 2026-09-20, gold-standard).** ``compute_shot_stopping``
RAISES ``KeyError`` unless BOTH ``defending_gk_player_id`` AND ``defending_gk_team_id`` are present on
``actions``. ``bronze.spadl_actions`` carries ``defending_gk_player_id`` (from
``add_pre_shot_gk_context``) but NOT ``defending_gk_team_id``. The writer DERIVES it in per-match input
prep: it builds a ``player_id -> team_id_native`` map from the match's own actions and stamps
``defending_gk_team_id = map[defending_gk_player_id]`` onto every row. This is the EXACT team of the
defending GK (what sk's keeper-identity map would produce), read from data already on the table -- NO new
bronze column, NO SPADL re-convert, NO ``resolve_keeper_identities`` producer. Open-play non-shot rows
with a NULL ``defending_gk_player_id`` pass through with a NULL team (shot_stopping scores shots only).

**Native ids -> Kimball surrogates (ADR-013).** ``compute_shot_stopping`` emits the RAW keeper id it was
handed in ``defending_gk_player_id`` (the working SPADL ``player_id``); the writer remaps it to the
keeper's ``player_id_native`` via the same per-match ``player_id -> player_id_native`` map so bronze
carries the NATIVE keeper id, and ``fct_gk_shot_stopping`` resolves ``player_key`` via ``dim_players`` on
``(provider, native_player_id)``. ``team_id`` is the native ``team_id_native`` (mapped the same way).
``match_id`` is the native match id (resolved to ``match_key`` via ``dim_matches``).

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` read straight off
``bronze.spadl_actions`` (single-tier per match) -- a direct per-row stamp, never a publish-time join.

**Governance.** shot_stopping is a per-KEEPER evaluative system (Goals Prevented / GSAA) -- ``wf-shot-
stopping`` is a member of ``PER_PLAYER_EVALUATIVE_CARDS`` and carries the ``docs/huggingface/model-cards/
shot-stopping.md`` EU-AI-Act model card (the honest limit: GSAA is low-sample per match; the pooled layer
``fct_gk_shot_stopping_pooled`` is the evaluative surface).

**Schema-drift guard (SK-EXPORT / sk:ADR-098).** The metric column set is pinned to the uniform
``silly_kicks.metric_contracts.METRIC_CONTRACTS['shot_stopping']`` registry via the parity test.

**Validation boundary (spec Part B).** The pure ``_compute_shot_stopping_group`` core is unit-tested on
fixtures; the Spark ``run_pipeline`` (``applyInPandas`` dispatch + the PSxG merge) is validated by the
live Part-B recompute, same posture as the sibling ADR-013 writers.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from silly_kicks.shot_stopping import SHOT_STOPPING_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, DEFAULT_GOLD_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "shot_stopping"
SPADL_TABLE = "spadl_actions"
PSXG_MART = "fct_shot_psxg"
PSXG_COLUMN = "psxg_recalibrated"

# All event providers (shot_stopping is event-only; PSxG covers tracking + statsbomb shots).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions. compute_shot_stopping needs game_id / type_id / result_id /
# period_id / shot_blocked + defending_gk_player_id (the PR1-stamped keeper) + the injected PSxG. The
# writer also reads player_id / player_id_native / team_id_native / action_id / match_key to build the
# per-match keeper->native maps, join PSxG, and stamp native identity + access_tier.
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "match_key",
    "access_tier",
    "game_id",
    "action_id",
    "period_id",
    "type_id",
    "result_id",
    "shot_blocked",
    "player_id",
    "player_id_native",
    "team_id_native",
    "defending_gk_player_id",
)

# The PSxG-fact columns the writer reads (native shot identity + Post-Shot xG).
PSXG_READ_COLUMNS: tuple[str, ...] = ("match_key", "action_id", PSXG_COLUMN)

# The columns compute_shot_stopping reads (defending_gk_team_id derived below; psxg injected).
_SK_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "type_id",
    "result_id",
    "period_id",
    "shot_blocked",
    "defending_gk_player_id",
)

# Native identity + access_tier stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "player_id", "team_id", "access_tier")

# silly-kicks 4.120.0 compute_shot_stopping metric columns (shots_faced / goals_conceded / psxg_faced /
# goals_prevented + the 4 _excl_penalties companions), in order. Pinned to the SK-EXPORT registry by
# test_shot_stopping_writer.test_shot_stopping_mart_matches_sk_columns.
_SHOT_STOPPING_METRIC_COLUMNS: tuple[str, ...] = tuple(SHOT_STOPPING_METRIC_COLUMNS)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_SHOT_STOPPING_METRIC_COLUMNS)

# sk 'shot_stopping' column_types (SK-EXPORT registry): shots_faced / goals_conceded (+ _excl_penalties)
# are Int64 -> long (nullable); psxg_faced / goals_prevented (+ _excl_penalties) are float64 -> double.
_LONG_METRICS: frozenset[str] = frozenset(
    {
        "shots_faced",
        "goals_conceded",
        "shots_faced_excl_penalties",
        "goals_conceded_excl_penalties",
    }
)


def _metric_type(col: str) -> str:
    """Spark SQL type for a metric column: count -> long, else psxg/goals_prevented -> double."""
    return "long" if col in _LONG_METRICS else "double"


_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "player_id": "string",
    "team_id": "string",
    "access_tier": "string",
    **{c: _metric_type(c) for c in _SHOT_STOPPING_METRIC_COLUMNS},
}


def _ddl() -> str:
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-shot-stopping migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE", "long": "BIGINT"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


SHOT_STOPPING_DDL = _ddl()


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _score_shot_stopping(actions: pd.DataFrame) -> tuple[pd.DataFrame, Any]:
    """Run silly-kicks ``compute_shot_stopping`` on one match's actions -> (samples, ShotStoppingReport).

    ``actions`` must already carry ``defending_gk_team_id`` (derived in :func:`_derive_defending_gk_team`)
    and the injected ``psxg_recalibrated`` column. Emits the RAW keeper id from
    ``defending_gk_player_id`` + its authoritative team from ``defending_gk_team_id``.
    """
    from silly_kicks.shot_stopping import compute_shot_stopping

    return compute_shot_stopping(actions, psxg_column=PSXG_COLUMN)


def _derive_defending_gk_team(actions: pd.DataFrame) -> pd.DataFrame:
    """Stamp ``defending_gk_team_id`` from a per-match ``player_id -> team_id_native`` map (owner-decided).

    The defending GK's authoritative team is the ``team_id_native`` the keeper's own SPADL rows carry.
    A NULL / unmapped ``defending_gk_player_id`` (open-play non-shot rows) yields a NULL team -- fine,
    ``compute_shot_stopping`` scores on-target shots only.
    """
    out = actions.copy()
    team_map = dict(zip(actions["player_id"], actions["team_id_native"], strict=False))
    out["defending_gk_team_id"] = actions["defending_gk_player_id"].map(team_map)
    return out


def _keeper_native_map(actions: pd.DataFrame) -> dict[Any, Any]:
    """A per-match ``player_id -> player_id_native`` map (working id -> native id for the keeper)."""
    return dict(zip(actions["player_id"], actions["player_id_native"], strict=False))


def _compute_shot_stopping_group(actions: pd.DataFrame) -> pd.DataFrame:
    """Shot-stopping for ONE match's actions -> identity + 8 metric columns, grain (match, keeper).

    ``actions`` is one match's ``bronze.spadl_actions`` rows LEFT-joined to per-shot PSxG. Carries
    ``data_source`` / ``match_id_native`` / ``access_tier`` (single-valued for a match). The emitted
    ``player_id`` is remapped to the keeper's ``player_id_native`` (native id for the dim_players join);
    ``team_id`` is the native keeper team. Returns an empty full-schema frame for a match with no
    on-target attributed shots (compute_shot_stopping's own empty contract).
    """
    import pandas as pd

    if actions.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])
    access_tier = str(actions["access_tier"].iloc[0]) if actions["access_tier"].notna().any() else None

    prepped = _derive_defending_gk_team(actions)
    native_map = _keeper_native_map(actions)
    samples, _report = _score_shot_stopping(prepped)
    if samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    out = samples.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    # sk emits the RAW keeper id (the working SPADL player_id) -> remap to the keeper's native id so
    # dim_players resolves player_key on (provider, native_player_id). team_id is already the native team.
    out["player_id"] = out["player_id"].map(native_map).astype("string")
    out["team_id"] = out["team_id"].astype("string")
    out["access_tier"] = access_tier
    return out[list(OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) -- validated by the live Part-B gate, not unit tests
# ---------------------------------------------------------------------------


def _struct_type() -> Any:
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    type_map = {"long": LongType(), "double": DoubleType(), "string": StringType()}
    return StructType([StructField(c, type_map[_OUTPUT_TYPES[c]], True) for c in OUTPUT_COLUMNS])


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score shot_stopping."
        )


def _shot_stopping_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas closure: one match's SPADL+psxg rows -> its keeper rows (module-level, picklable)."""
    return _compute_shot_stopping_group(pdf)


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score shot-stopping over every match of ``providers`` -> bronze ``shot_stopping`` (replaceWhere).

    LEFT-joins per-shot PSxG (gold ``fct_shot_psxg`` on the native ``(match_key, action_id)``) onto the
    SPADL actions, then dispatches per ``(data_source, match_id_native)`` group via ``applyInPandas``.
    Writes each provider's slice idempotently. Returns the total rows written.
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
    psxg = spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{PSXG_MART}").select(*PSXG_READ_COLUMNS)
    joined = actions.join(psxg, on=["match_key", "action_id"], how="left")
    scored = joined.groupBy("data_source", "match_id_native").applyInPandas(_shot_stopping_udf, schema=schema_out)

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
    logger.info("shot_stopping: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score GK shot-stopping (Goals Prevented / GSAA) to bronze")
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
