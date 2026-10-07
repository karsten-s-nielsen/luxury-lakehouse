"""Shared per-unit driver for the Rev-6 tracking grain marts (fct_off_ball_runs /
fct_action_defensive / fct_defensive_credit_attributions).

These three ADR-013 writers all need the SAME oriented ``(actions, frames, xt)`` the AC drain builds,
then call a different silly-kicks function. This module centralises the LIVE per-unit reading so the
three writers do not each re-derive it: it reuses the AC drain's importable read constants + meta
helpers (``ingestion.action_context``) and the shared conversion seam
(``analytics.action_context.unit_inputs.build_unit_inputs``).

**Validation boundary (spec Part B).** The pure per-mart cores + ``build_unit_inputs`` are unit-tested on
fixtures. THIS module's Spark reads + provider meta resolution mirror ``_process_tracking_match`` and are
validated by the live Part-B recompute (Task 22b), not by unit tests — same posture as
``xg_shot_scorer.run_pipeline``. It is driver-mode (per ``(match, period)`` unit, 16 GB); a unit's frames
fit, and the run-detection / defensive-credit functions are whole-unit computations (no frame-batching).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

    from analytics.action_context.work_unit import MatchMeta

logger = logging.getLogger(__name__)

_TRACKING_PROVIDERS: tuple[str, ...] = ("idsse", "metrica", "skillcorner", "gradientsports")


def discover_tracking_units(
    spark: SparkSession,
    catalog: str,
    *,
    providers: tuple[str, ...] = _TRACKING_PROVIDERS,
    schema: str = "bronze",
) -> list[tuple[str, str, int]]:
    """Return ``(provider, match_id, period)`` for every AC-processed tracking unit.

    The unit set is exactly what the AC drain already materialised (``bronze.spadl_action_context``):
    those are the ``(match, period)`` halves with real tracking frames, which is precisely the domain
    of these grain marts. Reusing that set means these writers never enumerate a unit the drain could
    not build frames for.
    """
    from pyspark.sql import functions as F  # noqa: N812

    quoted = ", ".join(f"'{p}'" for p in providers)
    rows = (
        spark.table(f"{catalog}.{schema}.spadl_action_context")
        .where(f"data_source IN ({quoted})")
        .select(
            F.col("data_source").cast("string"),
            F.col("match_id").cast("string"),
            F.col("period_id").cast("bigint"),
        )
        .distinct()
        .collect()
    )
    return sorted((str(r[0]), str(r[1]), int(r[2])) for r in rows if r[2] is not None)


def read_raw_trk_sdf(
    spark: SparkSession,
    catalog: str,
    provider: str,
    match_id: str,
    period: int,
) -> SparkDataFrame:
    """Return ONE unit's raw tracking rows as a Spark DataFrame (projected to the builder's input cols).

    The executor-distributed seam: the two-stage dispatch (``tracking_marts_dispatch``) repartitions this
    DataFrame and builds the oriented frames inside a ``mapInPandas`` pass — frames NEVER hit the driver.
    Reuses ``ingestion.action_context``'s per-provider ``*_TRACKING_SELECT_COLS`` projections + the
    SkillCorner meta broadcast-join so orientation is byte-identical to the AC drain.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion import action_context as ac

    if provider == "idsse":
        return (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter((F.col("match_id") == match_id) & (F.col("period") == period))
            .select(*ac._IDSSE_TRACKING_SELECT_COLS)
        )
    if provider == "metrica":
        return (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter((F.col("match_id") == match_id) & (F.col("period") == period))
            .select(*ac._METRICA_TRACKING_SELECT_COLS)
        )
    if provider == "skillcorner":
        trk = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter((F.col("match_id") == match_id) & (F.col("period") == period))
            .select(*ac._SKILLCORNER_TRACKING_SELECT_COLS)
        )
        matches_meta = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select(
                F.col("player_id"),
                F.col("team_id").cast("string").alias("team_id"),
                (F.col("position_acronym") == "GK").alias("is_goalkeeper"),
                F.col("pitch_length").cast("double").alias("pitch_length"),
                F.col("pitch_width").cast("double").alias("pitch_width"),
            )
        )
        return trk.join(F.broadcast(matches_meta), on="player_id", how="left")
    if provider == "gradientsports":
        return (
            spark.table(f"{catalog}.bronze.gradientsports_tracking")
            .filter((F.col("match_id") == match_id) & (F.col("period") == period))
            .select(*ac._GRADIENTSPORTS_TRACKING_SELECT_COLS)
        )
    raise ValueError(f"Unknown tracking provider: {provider}")


def read_unit_actions(spark: SparkSession, catalog: str, provider: str, match_id: str) -> pd.DataFrame:
    """Return one match's SPADL actions as driver pandas (small/bounded; the per-unit closure input)."""
    from pyspark.sql import functions as F  # noqa: N812

    return (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )


def resolve_unit_meta(spark: SparkSession, catalog: str, provider: str, match_id: str) -> MatchMeta:
    """Public accessor for a unit's resolved ``MatchMeta`` (home_team_id etc.) — a UDF-closure input.

    The gkdv scorer (via ``ingestion.tracking_marts_processor.TrackingMartsProcessor``) needs ``home_team_id``
    to build the ghost counterfactual (``silly_kicks.gkdv.build_ghost_frames(home_team_id=...)``). Reuses the
    SAME per-provider resolution the AC drain applies, so orientation/home-team identity stay byte-identical.
    """
    return _resolve_meta(spark, catalog, provider, match_id)


def _resolve_meta(spark: SparkSession, catalog: str, provider: str, match_id: str) -> MatchMeta:
    """Resolve driver-scalar match metadata per provider (mirrors ``_process_tracking_match``)."""
    from pyspark.sql import functions as F  # noqa: N812

    from analytics.action_context.work_unit import MatchMeta

    if provider == "idsse":
        from silly_kicks.providers.sportec import derive_idsse_home_team_start_left, shape_events_to_native

        from ingestion.spadl_adapter import derive_idsse_home_team_start_left_extratime

        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted = shape_events_to_native(events_pdf)
        return MatchMeta(
            home_team_id=home_team_id,
            home_start_left=derive_idsse_home_team_start_left(adapted, home_team_id),
            home_team_start_left_extratime=derive_idsse_home_team_start_left_extratime(adapted, home_team_id),
        )

    if provider == "metrica":
        return MatchMeta(home_team_id="Home", home_start_left=True)

    if provider == "skillcorner":
        row = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select("home_team_id")
            .limit(1)
            .collect()[0]
        )
        return MatchMeta(home_team_id=str(row["home_team_id"]), home_start_left=True)

    if provider == "gradientsports":
        from ingestion import action_context as ac
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        events_pdf = (
            spark.table(f"{catalog}.bronze.gradientsports_events")
            .filter(F.col("match_id") == match_id)
            .select(*[f"`{c}`" for c in ac._GS_EVENTS_META_COLS])
            .toPandas()
        )
        gs_meta = extract_gradientsports_match_metadata(events_pdf)
        home_team_id = str(gs_meta["home_team_id"])
        roster_pdf = (
            spark.table(f"{catalog}.bronze.gradientsports_roster")
            .filter(F.col("match_id") == match_id)
            .select(*[f"`{c}`" for c in ac._GS_ROSTER_COLS])
            .toPandas()
        )
        side_to_id: dict[str, str] | None = None
        jersey_to_pid: dict[tuple[str, str], str] | None = None
        gk_ids: list[str] | None = None
        if not roster_pdf.empty:
            side_to_id, jersey_to_pid, gk_ids = ac._build_gradientsports_roster_dicts(roster_pdf, home_team_id)
        return MatchMeta(
            home_team_id=home_team_id,
            home_start_left=gs_meta["home_team_start_left"],
            home_team_start_left_extratime=gs_meta["home_team_start_left_extratime"],
            gs_team_side_to_id=side_to_id,
            gs_jersey_to_player_id=jersey_to_pid,
            gs_gk_player_ids=gk_ids,
        )

    raise ValueError(f"Unknown tracking provider: {provider}")


def ac_xt_grid(spark: SparkSession, catalog: str, schema: str) -> tuple[list[list[float]], int, int]:
    """Load the global xT grid (reuses the AC drain's Delta loader)."""
    from ingestion.action_context import _load_xt_grid_from_delta

    return _load_xt_grid_from_delta(spark, catalog, schema, logger)
