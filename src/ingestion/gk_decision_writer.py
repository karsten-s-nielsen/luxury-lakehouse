"""GK decision-value scorer/writer -- ADR-013 Python-writer -> bronze -> dbt staging -> gold mart.

Materialises the silly-kicks 4.120.0 ``gk_decision.compute_gk_decision_value`` family (TF-62) and lands
it in ``bronze.gk_decision`` -- the goalkeeper's build-up DISTRIBUTION DECISION value (chosen-vs-available)
per ``(game_id, keeper)``: given a GK distribution action, how good was the chosen option vs the reachable
alternatives (5 metric cols: decision_value / chosen_ev / best_ev / sel_efficiency / decision_pct), plus the
provenance ``n_options`` / ``option_set_source``.

**SCOPE (owner-decided 2026-09-20): RECONSTRUCTION TIER ONLY.** The metric has two tiers:
  * NATIVE (``SkillCornerGIOptionSet``) -- reads SkillCorner game-intelligence ``passing_option`` rows via
    ``silly_kicks.providers.skillcorner.parse_passing_options``. That INGESTION DOES NOT EXIST in the
    lakehouse (only a source-yml stub; no ``parse_passing_options`` producer / staging). It is a separate
    data-acquisition feature, OUT of this metric-adoption cycle -- documented as a tracked not-yet-sourced
    follow-up in the model card + workflow card. The ``option_set_source`` column distinguishes the tiers
    so the native GI branch drops in cleanly when GI lands.
  * RECONSTRUCTION (``ReconstructedOptionSet``) -- reconstructs the option set from SB360 freeze-frames
    (+ full-tracking) with the bundled ``PassCompletionModel``. THIS is what this writer builds + scores.

**TRACKING-consuming -> tracking-marts drain (P2).** The reconstruction tier reads tracking FRAMES, so
this writer's per-unit core (:func:`score_gk_decision_unit`) is called by the tracking-marts drain's
``TrackingMartsProcessor`` per work unit (like ``gkdv_writer.score_gkdv_unit``), NOT a pure event
``applyInPandas``. The drain-wiring (registering into ``TrackingMartsProcessor`` + the gate) is the P2
phase; this module ships the pure scoring core + the bronze/mart/staging/governance ADR-013 slice.

**Per-unit mini-pipeline (mirrors the sk SB360 e2e worked example).** For one unit's oriented
``(actions, frames)``: ``snapshot_to_tracking_frames`` is NOT re-run (the AC drain already supplies
tracking-schema frames); ``apply_actor_identities_to_frames`` stamps the real keeper id onto the actor
row; ``gk_distribution_mask`` selects the GK-distribution actions; ``ReconstructedOptionSet(gk_actions,
frames, xpass=PassCompletionModel.bundled(), params, keeper_ids)`` builds the tier-agnostic option set;
``compute_gk_decision_value(os, extra_drops=os.drop_counts())`` scores it (the ``extra_drops`` keeps the
Report census the TRUE decision population).

**Native ids (ADR-013).** ``compute_gk_decision_value`` emits the RAW keeper id (``keeper`` == the SPADL
``player_id`` the action carried) + ``keeper_raw``; the writer stamps native ``(data_source, match_id,
keeper)`` and ``fct_gk_decision`` resolves ``keeper -> player_key`` via ``dim_players`` +
``match_id -> match_key`` via ``dim_matches``.

**access_tier (ADR-064).** Every bronze row is stamped with the match's ``access_tier`` -- a direct
per-row stamp, never a publish-time join.

**Governance.** gk_decision is a per-KEEPER evaluative system -- ``wf-gk-decision`` is a member of
``PER_PLAYER_EVALUATIVE_CARDS`` and carries the ``docs/huggingface/model-cards/gk-decision.md`` EU-AI-Act
model card. The HONEST LIMIT: this is an INSTRUMENT (how good was this decision vs available), NOT a
ranking -- the reconstruction-tier correlation to ground truth is only MODERATE, and any cited number is a
PRE-4.120 measure that must be re-measured against the 4.120.0 bundle before display.

**Schema-drift guard (SK-EXPORT / sk:ADR-098).** The metric column set is pinned to the uniform
``silly_kicks.metric_contracts.METRIC_CONTRACTS['gk_decision']`` registry via the parity test (its
``column_types`` is ``None`` -> the lakehouse infers the DDL, OI-3).

**Validation boundary (spec Part B).** The pure ``score_gk_decision_unit`` core is unit-tested on a
synthetic SB360 fixture; the Spark drain dispatch is validated by the live Part-B recompute (P2), same
posture as ``gkdv_writer``.
"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any, Literal

from silly_kicks.gk_decision import GK_DECISION_METRIC_COLUMNS

from shared.constants import DEFAULT_BRONZE_SCHEMA, IDENTIFIER_RE

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

logger = logging.getLogger(__name__)

# Keep in lockstep with the other silly-kicks-consuming entry points (AGENTS.md sec serverless env pins).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

CATALOG = "soccer_analytics"
BRONZE_TABLE = "gk_decision"

# gk_decision reconstruction tier needs tracking frames -> TRACKING providers only.
_TRACKING_PROVIDERS: tuple[str, ...] = ("idsse", "metrica", "skillcorner", "gradientsports")

# The reconstruction-tier provenance value (sk OPTION_SET_SOURCE_VALUES). The NATIVE 'skillcorner' GI
# tier is not-yet-sourced (no parse_passing_options ingestion) -- when it lands, its rows carry
# option_set_source='native'.
_OPTION_SET_SOURCE_RECONSTRUCTED = "reconstructed"

# silly-kicks 4.120.0 gk_decision metric columns (decision_value / chosen_ev / best_ev / sel_efficiency /
# decision_pct), in order. Pinned to the SK-EXPORT registry by the parity test.
_GK_DECISION_METRIC_COLUMNS: tuple[str, ...] = tuple(GK_DECISION_METRIC_COLUMNS)

# The two non-metric provenance columns from GK_DECISION_SAMPLE_COLUMNS the mart carries.
_PROVENANCE_COLUMNS: tuple[str, ...] = ("n_options", "option_set_source")

# Per-DECISION grain (sk4118 Phase E, owner decision A). sk ``compute_gk_decision_value`` returns ONE row
# per GK-distribution DECISION (``GK_DECISION_SAMPLE_COLUMNS`` carry ``period_id`` + ``decision_id``;
# ``decision_id`` == the SPADL ``action_id`` — stable across re-runs, so the per-unit ``replaceWhere``
# keyed on (data_source, match_id, period_id) is idempotent). The bronze/mart are per-decision
# (match x keeper x decision); a per-keeper-match summary is a DOWNSTREAM aggregate
# (``silly_kicks.gk_decision.summarize_gk_decision``), never this fact's grain. This corrects the earlier
# per-keeper mislabel (sk samples are per-decision; the writer must not collapse them without aggregating,
# which would have made ``gk_decision_id`` non-unique + violated the mart grain at the live P2 build).
_IDENTITY_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id",
    "period_id",
    "decision_id",
    "keeper",
    "team_id",
    "access_tier",
)

OUTPUT_COLUMNS: tuple[str, ...] = (*_IDENTITY_COLUMNS, *_GK_DECISION_METRIC_COLUMNS, *_PROVENANCE_COLUMNS)

# gk_decision METRIC_CONTRACTS['gk_decision'].column_types is None -> the lakehouse infers the DDL (OI-3).
# The 5 metric cols are double; n_options is long; option_set_source is string; identity all string.
_OUTPUT_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "period_id": "long",
    "decision_id": "long",  # == the SPADL action_id of the GK-distribution decision
    "keeper": "string",
    "team_id": "string",
    "access_tier": "string",
    **{c: "double" for c in _GK_DECISION_METRIC_COLUMNS},
    "n_options": "long",
    "option_set_source": "string",
}


def _ddl() -> str:
    """Canonical bronze DDL (mirrored by the 2026-09-17-add-gk-decision migration; keep in sync)."""
    sql_type = {"string": "STRING", "double": "DOUBLE", "long": "BIGINT"}
    cols = ", ".join(f"{c} {sql_type[_OUTPUT_TYPES[c]]}" for c in OUTPUT_COLUMNS)
    return f"{cols}, _ingested_at TIMESTAMP"


GK_DECISION_DDL = _ddl()


# ---------------------------------------------------------------------------
# Pure scoring core (unit-tested on a synthetic SB360 fixture; no Spark)
# ---------------------------------------------------------------------------


def _bundled_completion_model() -> Any:
    """The 4.120.0 full-open-data-refit bundled ``PassCompletionModel`` (loop-invariant; broadcast once)."""
    from silly_kicks.expected_passing import PassCompletionModel

    return PassCompletionModel.bundled()


def score_gk_decision_reconstructed(
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    *,
    keeper_ids: list[Any],
    completion_model: Any,
    params: Any = None,
    visible_area: pd.DataFrame | None = None,
    frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None,
) -> tuple[pd.DataFrame, Any]:
    """One unit's ``(actions, frames)`` -> reconstruction-tier ``(samples, GkDecisionReport)``.

    Stamps the real keeper id onto the anonymous actor row (``apply_actor_identities_to_frames``), selects
    the GK-distribution actions, builds a ``ReconstructedOptionSet`` with the bundled ``PassCompletionModel``,
    and scores it. ``extra_drops=option_set.drop_counts()`` keeps the Report census the TRUE decision
    population (adapter drops -- no_frame / fov_cropped -- are read AFTER option_rows()).
    """
    from silly_kicks.gk_decision import GkDecisionParams, ReconstructedOptionSet, compute_gk_decision_value
    from silly_kicks.keeper_identity import apply_actor_identities_to_frames
    from silly_kicks.tracking import gk_distribution_mask

    resolved_params = params if params is not None else GkDecisionParams()
    # SB360 snapshots carry an anonymous actor row (is_actor) bridged to the real keeper id and use the
    # per_action_ltr convention (frame_id == action_id). Raw tracking frames already carry real player_ids
    # (no is_actor) and link via match_ltr (link_actions_to_frames + resolve_defended_goals, sk-internal).
    # Both derive from the SAME signal — the presence of an is_actor column — so infer the convention +
    # skip the SB360 bridge when the caller does not force a convention (drain-hardening 2026-09-23, ADR-087).
    is_snapshot = "is_actor" in frames.columns
    conv = frame_convention if frame_convention is not None else ("per_action_ltr" if is_snapshot else "match_ltr")
    frames_with_ids = apply_actor_identities_to_frames(frames, actions) if is_snapshot else frames
    gk_actions = actions[gk_distribution_mask(actions, frames_with_ids, resolve_gk="robust").to_numpy()]

    option_set = ReconstructedOptionSet(
        gk_actions,
        frames_with_ids,
        xpass=completion_model,
        params=resolved_params,
        keeper_ids=keeper_ids,
        frame_convention=conv,
        visible_area=visible_area,
    )
    return compute_gk_decision_value(option_set, extra_drops=option_set.drop_counts())


def score_gk_decision_unit(
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    completion_model: Any,
    *,
    data_source: str,
    match_id: str,
    access_tier: str | None,
    keeper_ids: list[Any] | None = None,
    params: Any = None,
    visible_area: pd.DataFrame | None = None,
    frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None,
) -> pd.DataFrame:
    """One unit's oriented ``(actions, frames)`` -> stamped per-DECISION gk_decision rows (ADR-037 drain body).

    Factors the per-unit body so the tracking-marts drain (P1 Phase E wiring) scores one unit at a time and
    writes ``bronze.gk_decision`` with a per-``(data_source, match_id, period_id)`` ``replaceWhere``. sk
    ``compute_gk_decision_value`` returns ONE row per GK DECISION (``decision_id`` == the SPADL action_id);
    this stamps native identity + access_tier + the reconstruction-tier ``option_set_source`` and keeps the
    per-decision grain (NO aggregation — a per-keeper summary is downstream). ``keeper_ids`` defaults to
    every distinct actor of a GK-distribution action (the reconstruction path keys the OptionSet on them).
    """
    import pandas as pd

    if actions.empty or frames.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    roster = keeper_ids if keeper_ids is not None else _default_keeper_ids(actions, frames)
    samples, _report = score_gk_decision_reconstructed(
        actions,
        frames,
        keeper_ids=roster,
        completion_model=completion_model,
        params=params,
        visible_area=visible_area,
        frame_convention=frame_convention,
    )
    if samples.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    out = samples.copy()
    out["data_source"] = data_source
    out["match_id"] = match_id
    # sk emits GK_DECISION_KEYS == ('game_id','keeper'); the mart resolves keeper -> player_key.
    out["keeper"] = out["keeper"].astype("string")
    out["team_id"] = out["team_id"].astype("string")
    out["access_tier"] = access_tier
    # Reconstruction tier -- sk already stamps option_set_source='reconstructed'; keep it explicit for the
    # NULL-safe case (an empty adapter would leave it unset).
    if "option_set_source" not in out.columns or out["option_set_source"].isna().any():
        out["option_set_source"] = out.get("option_set_source", _OPTION_SET_SOURCE_RECONSTRUCTED)
    return out.reindex(columns=list(OUTPUT_COLUMNS)).reset_index(drop=True)


def _default_keeper_ids(actions: pd.DataFrame, frames: pd.DataFrame) -> list[Any]:
    """Roster keeper ids = the goalkeepers observed in the frames (fallback: GK-distribution actors)."""
    from silly_kicks.tracking import gk_distribution_mask

    if "is_goalkeeper" in frames.columns:
        gk_rows = frames[frames["is_goalkeeper"].astype("boolean").fillna(False).to_numpy(dtype=bool)]
        ids = [k for k in gk_rows["player_id"].dropna().unique().tolist() if k is not None]
        if ids:
            return ids
    gk_actions = actions[gk_distribution_mask(actions, frames, resolve_gk="robust").to_numpy()]
    return [k for k in gk_actions["player_id"].dropna().unique().tolist() if k is not None]


# ---------------------------------------------------------------------------
# Spark schema + version guard (drain wiring is P1 Phase E in TrackingMartsProcessor; main() is a stub)
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
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} -- refusing to score gk_decision."
        )


def main() -> None:
    """CLI entry point (Databricks).

    gk_decision is TRACKING-consuming (reconstruction tier reads frames) and runs in the tracking-marts
    worker-drain (P2) via ``TrackingMartsProcessor.process`` calling :func:`score_gk_decision_unit` per
    unit -- NOT a standalone per-match applyInPandas. This entry point asserts the sk floor + creates the
    bronze table so the drain-wiring (P2) has a stable target; it is registered for pyproject/card parity.
    """
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="GK decision-value (reconstruction tier) -- drain-scored (P2)")
    parser.add_argument("--catalog", default=CATALOG)
    args = parser.parse_args()
    if not IDENTIFIER_RE.match(args.catalog):
        raise SystemExit(f"Invalid catalog name: {args.catalog!r}")

    _assert_silly_kicks_min()
    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]
    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, DEFAULT_BRONZE_SCHEMA)
    logger.info(
        "gk_decision reconstruction-tier scoring runs in the tracking-marts drain (P2). "
        "Ensured sk>=%s + bronze target.",
        ".".join(str(p) for p in _REQUIRED_SK_MIN),
    )


if __name__ == "__main__":
    main()
