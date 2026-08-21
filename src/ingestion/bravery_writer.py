"""Bravery scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.87.0 ``compute_bravery`` family (spec sec 7.5, Task 17g) and lands it
in the existing ``fct_match_summary`` mart. Bravery is the **defending team's** willingness to block:
the % of the opponent's final actions (shots + open-play crosses) that the defending team blocks,
per ``(game_id, defending team)``.

**Event-only, all providers.** ``compute_bravery`` needs ONLY the SPADL actions (``type_id`` +
``shot_blocked`` / ``cross_blocked`` -- the enrichment columns baked into every ``convert_to_actions``
since 4.56, spec sec 7.2); it needs no tracking frames or xT. So the writer reads ``bronze.spadl_actions``
directly and covers every event provider (statsbomb / wyscout / idsse / metrica / skillcorner /
gradientsports), NOT just the tracking cohort of the sibling grain-mart writers. It dispatches per
match via ``applyInPandas`` (event-only, one bounded group per match -- far more scalable than a
per-match driver round-trip over the ~3.5k-match StatsBomb corpus).

**Native ids (ADR-013).** ``compute_bravery`` groups on whatever ``team_id`` it is handed, so the
writer feeds it the NATIVE ``team_id_native`` (never the hashed BIGINT surrogate) -> the emitted
``team_id`` is the native defending-team id, and ``fct_match_summary`` resolves it to ``team_key`` via
``dim_teams`` on ``(provider, native_team_id)`` (review-4 B2 -- a hashed BIGINT would land all-NULL in
that join). ``match_id`` is the native match id, resolved to ``match_key`` via ``dim_matches``.

**Grain (review-4 B4).** One row per ``(match, DEFENDING team)`` -- two rows per two-team match. The
mart is one-row-per-match with home_/away_ pivots, so ``fct_match_summary`` LEFT-JOINs this bronze
TWICE (defending-team == home_team_key, and == away_team_key), landing ``home_bravery_*`` /
``away_bravery_*``.

**Validation boundary (spec Part B).** The pure ``_compute_bravery_group`` core is unit-tested on
fixtures; the Spark ``run_pipeline`` (``applyInPandas`` dispatch) is validated by the live Part-B
recompute (Task 22b), same posture as the sibling ADR-013 writers.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (CLAUDE.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 89, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "bravery"
SPADL_TABLE = "spadl_actions"

# All event providers (bravery is event-only -- it needs no tracking frames).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions (the minimal projection compute_bravery + the writer need).
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "game_id",
    "type_id",
    "team_id_native",
    "shot_blocked",
    "cross_blocked",
)

# Native identity stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "team_id")

# silly-kicks 4.87.0 compute_bravery metric columns (its _COLS minus the game_id/team_id keys), in order.
_BRAVERY_METRIC_COLUMNS: tuple[str, ...] = (
    "bravery_shots",
    "bravery_open_play_crosses",
    "bravery_set_piece_crosses",
    "bravery_pct_known_domain",
    "n_shots_faced",
    "n_open_play_crosses_faced",
    "n_set_piece_crosses_faced",
    "n_blocks_known",
)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_BRAVERY_METRIC_COLUMNS)

_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "team_id": "string",
    "bravery_shots": "double",
    "bravery_open_play_crosses": "double",
    "bravery_set_piece_crosses": "double",
    "bravery_pct_known_domain": "double",
    "n_shots_faced": "long",
    "n_open_play_crosses_faced": "long",
    "n_set_piece_crosses_faced": "long",
    "n_blocks_known": "long",
}

# Canonical bronze DDL (mirrored by the 2026-08-20-add-marts2 migration; keep in sync).
BRAVERY_DDL = (
    "data_source STRING, match_id STRING, team_id STRING, "
    "bravery_shots DOUBLE, bravery_open_play_crosses DOUBLE, bravery_set_piece_crosses DOUBLE, "
    "bravery_pct_known_domain DOUBLE, n_shots_faced BIGINT, n_open_play_crosses_faced BIGINT, "
    "n_set_piece_crosses_faced BIGINT, n_blocks_known BIGINT, _ingested_at TIMESTAMP"
)


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark)
# ---------------------------------------------------------------------------


def _compute_bravery_group(actions: pd.DataFrame) -> pd.DataFrame:
    """Bravery for ONE match's actions -> identity + 8 metric columns, grain (match, defending team).

    ``actions`` is one match's ``bronze.spadl_actions`` rows: it carries ``data_source`` +
    ``match_id_native`` (both single-valued for a match) and the SPADL/enrichment columns
    ``game_id`` / ``type_id`` / ``team_id_native`` / ``shot_blocked`` / ``cross_blocked``.

    The NATIVE ``team_id_native`` is fed to silly-kicks as ``team_id`` so the emitted defending
    ``team_id`` is native (resolves to ``team_key`` via ``dim_teams`` -- review-4 B2). Returns an
    empty frame with the full schema when the match has no shots/crosses (compute_bravery's own
    empty contract).
    """
    import pandas as pd
    from silly_kicks.tracking import compute_bravery

    if actions.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    data_source = str(actions["data_source"].iloc[0])
    match_id = str(actions["match_id_native"].iloc[0])

    scoring_input = actions[["game_id", "type_id", "shot_blocked", "cross_blocked"]].copy()
    scoring_input["team_id"] = actions["team_id_native"]

    bravery = compute_bravery(scoring_input)
    if bravery.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    out = bravery.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    out["team_id"] = out["team_id"].astype("string")
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
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score bravery."
        )


def _bravery_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas closure: one match's SPADL rows -> its bravery rows (module-level, picklable)."""
    return _compute_bravery_group(pdf)


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score bravery over every match of ``providers`` -> bronze ``bravery`` (per-provider replaceWhere).

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
    scored = source.groupBy("data_source", "match_id_native").applyInPandas(_bravery_udf, schema=schema_out)

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
    logger.info("bravery: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score bravery (per match, per defending team) to bronze")
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
