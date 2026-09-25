"""Ground-duel rating scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``duels.compute_duel_ratings`` family (TF-55) and lands it in the
wide-by-granularity player-match mart ``fct_player_match_metrics`` (shared with ``territory``). Per
``(game_id, player_id)`` it emits a Glicko-2 ground-duel rating (rating / deviation / volatility) + the
per-match contested/won/lost counts + the ``duel_winner_source`` provenance (native sportec winner/loser vs
derived tackle/take_on adjacency).

**STATEFUL -- single-driver ORDERED pass (deviation from the bravery template).** Glicko-2 is a stateful
rating system: a player's rating carries forward across matches in ascending ``game_id`` (each match is one
Glicko rating period). ``compute_duel_ratings`` therefore MUST see the whole corpus IN ORDER -- a per-match
``applyInPandas`` is WRONG (its groups are unordered and independent, so each match would reset to the
1500/350/0.06 seed and the carry-forward would be lost). Because the corpus is bounded and existing-only,
the writer does the simplest deterministic thing: ONE ordered pass over the full corpus at the driver
(``compute_duel_ratings`` sorts internally by ``game_id`` and returns every ``(game, player)`` row).
``DuelRatingReport.final_ratings`` is the resume seed, but the writer never relies on resume-equivalence --
it always recomputes the full ordered pass.

**Event-only, all providers.** ``compute_duel_ratings`` reads SPADL ``actions`` (``game_id`` /
``period_id`` / ``time_seconds`` / ``action_id`` / ``type_id`` / ``result_id`` / ``player_id`` /
``team_id`` -- for the derived tackle/take_on adjacency) + optionally the sportec native
``tackle_winner_player_id`` columns. The writer feeds ``player_id`` / ``team_id`` NATIVE.

**Native ids (ADR-013).** The NATIVE ``player_id_native`` is fed as ``player_id`` and ``team_id_native`` as
``team_id`` so the emitted ``player_id`` key is native; ``fct_player_match_metrics`` resolves ``player_key``
via ``dim_players`` and ``match_key`` via ``dim_matches``. duels keys on ``(game_id, player_id)`` only (it
emits no team column), so the player's native team is resolved from a per-corpus ``player_id_native ->
team_id_native`` map.

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` read straight off
``bronze.spadl_actions`` (single-tier per match) -- a direct per-row stamp resolved from a per-corpus
``match_id_native -> access_tier`` map.

**Governance.** duels is per-PLAYER evaluative (a per-player ground-duel rating) -- ``wf-duels`` is a member
of ``PER_PLAYER_EVALUATIVE_CARDS`` and carries the ``docs/huggingface/model-cards/duels.md`` EU-AI-Act model
card (the honest limit: GROUND-duels only + derived-adjacency ~11/match).

**Schema-drift guard (SK-EXPORT / sk:ADR-098).** The metric column set is pinned to the uniform
``silly_kicks.metric_contracts.METRIC_CONTRACTS['duels']`` registry via the parity test.

**Validation boundary (spec Part B).** The pure ``_score_all_matches`` / ``_stamp_identity`` cores are
unit-tested on fixtures; the Spark ``run_pipeline`` (the driver-side ordered read + write) is validated by
the live Part-B recompute.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from silly_kicks.duels import DUEL_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (AGENTS.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "duels"
SPADL_TABLE = "spadl_actions"

# All event providers (duels is event-only -- it needs no tracking frames).
_ALL_PROVIDERS: tuple[str, ...] = (
    "statsbomb",
    "wyscout",
    "idsse",
    "metrica",
    "skillcorner",
    "gradientsports",
)

# Columns read from bronze.spadl_actions. compute_duel_ratings reads game_id / period_id / time_seconds /
# action_id / type_id / result_id / player_id / team_id (for the derived tackle/take_on adjacency). The
# writer feeds player_id/team_id NATIVE + carries the identity trio + access_tier for stamping.
_SPADL_READ_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id_native",
    "access_tier",
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id_native",
    "player_id_native",
    "type_id",
    "result_id",
)

# The canonical SPADL columns compute_duel_ratings reads (player_id / team_id supplied native).
_SK_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "action_id",
    "period_id",
    "time_seconds",
    "type_id",
    "result_id",
)

# Native identity + access_tier stamped by the writer (surrogate keys resolve in the mart -- ADR-013).
_IDENTITY_COLUMNS: tuple[str, ...] = ("data_source", "match_id", "player_id", "team_id", "access_tier")

# silly-kicks 4.120.0 compute_duel_ratings metric columns (duel_rating / duel_rating_deviation /
# duel_volatility / duels_contested / duels_won / duels_lost), in order. Pinned to the SK-EXPORT registry
# by test_duels_writer.test_duels_registry_parity.
_DUEL_METRIC_COLUMNS: tuple[str, ...] = tuple(DUEL_METRIC_COLUMNS)

# The provenance column emitted alongside the 6 metric cols (native vs derived winner/loser labeling).
_PROVENANCE_COLUMN = "duel_winner_source"

_METRIC_AND_PROVENANCE_COLUMNS: tuple[str, ...] = (*_DUEL_METRIC_COLUMNS, _PROVENANCE_COLUMN)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_METRIC_AND_PROVENANCE_COLUMNS)

# sk 'duels' column_types (SK-EXPORT registry): duel_rating / duel_rating_deviation / duel_volatility are
# float64 -> double; duels_contested / duels_won / duels_lost are Int64 -> long; duel_winner_source string.
_LONG_METRICS: frozenset[str] = frozenset({"duels_contested", "duels_won", "duels_lost"})


def _metric_type(col: str) -> str:
    """Spark SQL type for a metric/provenance column (sk duels column_types)."""
    if col == _PROVENANCE_COLUMN:
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
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-duels migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE", "long": "BIGINT"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


DUELS_DDL = _ddl()


# ---------------------------------------------------------------------------
# Pure scoring (unit-tested; no Spark) -- single ORDERED pass over the whole corpus
# ---------------------------------------------------------------------------


def _score_all_matches(actions: pd.DataFrame) -> tuple[pd.DataFrame, Any]:
    """Run silly-kicks ``compute_duel_ratings`` ONCE over the full corpus -> (samples, DuelRatingReport).

    Glicko-2 is stateful; ``compute_duel_ratings`` sorts internally by ``game_id`` and carries ratings
    forward across matches, so this is one ordered pass -- NOT a per-match dispatch. Feeds the NATIVE
    ``player_id_native`` / ``team_id_native`` as ``player_id`` / ``team_id``.
    """
    from silly_kicks.duels import compute_duel_ratings

    scoring_input = actions[list(_SK_INPUT_COLUMNS)].copy()
    scoring_input["player_id"] = actions["player_id_native"].to_numpy()
    scoring_input["team_id"] = actions["team_id_native"].to_numpy()
    return compute_duel_ratings(scoring_input)


def _stamp_identity(samples: pd.DataFrame, actions: pd.DataFrame) -> pd.DataFrame:
    """Stamp native identity + team + access_tier onto the ``(game_id, player_id)`` duel samples.

    ``samples`` carries keys ``(game_id, player_id)`` + the 6 metric cols + ``duel_winner_source``. The
    player's native team + the match's access_tier are resolved from per-corpus maps built off the source
    actions (a player belongs to one team; a match is single-tier). Returns an empty full-schema frame
    when there are no samples.
    """
    import pandas as pd

    if samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    team_map = dict(zip(actions["player_id_native"], actions["team_id_native"], strict=False))
    match_map = dict(zip(actions["game_id"], actions["match_id_native"], strict=False))
    source_map = dict(zip(actions["game_id"], actions["data_source"], strict=False))
    tier_map = dict(zip(actions["game_id"], actions["access_tier"], strict=False))

    out = samples.copy()
    out["data_source"] = out["game_id"].map(source_map).astype("string")
    out["match_id"] = out["game_id"].map(match_map).astype("string")
    out["team_id"] = out["player_id"].map(team_map).astype("string")
    out["player_id"] = out["player_id"].astype("string")
    out["access_tier"] = out["game_id"].map(tier_map).astype("string")
    return out[list(OUTPUT_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spark pipeline (Databricks) -- single-driver ORDERED pass, validated live in Part B
# ---------------------------------------------------------------------------


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score duels."
        )


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
) -> int:
    """Score ground-duel ratings over every match of ``providers`` -> bronze ``duels`` (per-provider replaceWhere).

    A SINGLE-DRIVER ORDERED PASS (NOT applyInPandas): Glicko-2 ratings carry forward across matches in
    ascending ``game_id``, so each provider's whole action corpus is pulled to the driver (bounded to the
    11 narrow duel-input columns) and scored in one ordered ``compute_duel_ratings`` call. Writes each
    provider's slice idempotently. Returns the total rows written.
    """
    from ingestion.utils import write_delta_table

    _assert_silly_kicks_min()

    total = 0
    for provider in providers:
        # One provider's whole corpus to the driver (11 narrow cols); duels needs the full ordered stream.
        actions = (
            spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{SPADL_TABLE}")
            .where(f"data_source = '{provider}'")
            .select(*_SPADL_READ_COLUMNS)
            .toPandas()  # type: ignore[union-attr]
        )
        if actions.empty:
            logger.info("duels: provider %s has no actions -- skipping", provider)
            continue

        samples, report = _score_all_matches(actions)
        out = _stamp_identity(samples, actions)
        logger.info(
            "duels: provider %s scored %d (game, player) rows from %d duels (%d matches)",
            provider,
            len(out),
            report.n_duels,
            report.n_matches,
        )
        if out.empty:
            continue

        result = spark.createDataFrame(out)
        total += write_delta_table(
            result,
            catalog,
            DEFAULT_BRONZE_SCHEMA,
            BRONZE_TABLE,
            replace_where=f"data_source = '{provider}'",
            logger=logger,
            row_count=len(out),
        )
    logger.info("duels: wrote %d total rows across %d providers", total, len(providers))
    return total


def main() -> None:
    """CLI entry point (Databricks)."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Score ground-duel Glicko-2 ratings (per match, per player) to bronze")
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
