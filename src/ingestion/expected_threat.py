"""Expected Threat batch pipeline — fits per-competition + global xT models from SPADL actions.

ExT-v2 single-canonical-surface migration (lakehouse ADR-085): the canonical xT surface is a fitted
silly-kicks ``ExpectedThreat`` (16x12), persisted to bronze ``expected_threat_grids`` as its
``to_dict()`` JSON (values + transition_matrix + prob matrices), and reconstructed downstream via
``ExpectedThreat.from_dict``. Replaces the retired in-repo ``analytics.expected_threat`` v1 grid.

ADR-063 guards (directionality / structural / materiality-drift) are preserved via
``analytics.xt_grid_guards`` (reimplemented against the sk model / its ``.xT``).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "expected_threat_grids"
# ADR-085: one row per competition (+ a `global` row) carrying the fitted sk ExpectedThreat as JSON.
_RESULTS_SCHEMA = "competition_id STRING, xt_model_json STRING, format_version INT, _ingested_at TIMESTAMP"
_GOLD_TABLE = "fct_action_values"

# ADR-085 G-fix (sk4118): derived long-form physical projection of the canonical model, written
# alongside the JSON. It exists ONLY for the dbt SQL zone-lookup consumers (fct_action_values.gk_xt_delta
# = ADR-056, fct_goalkeeper_stats) that cannot query a JSON `xt_model_json` blob per-zone. It is a
# deterministic read-only projection of the SAME fitted model (NOT a second fit) — single-canonical-surface
# invariant preserved. 16x12 (matches _XT_L/_XT_W), physical-oriented (ADR-041 via `physical_grid`).
_ZONES_TABLE = "expected_threat_grid_zones"
_ZONES_SCHEMA = (
    "competition_id STRING, zone_x INT, zone_y INT, xt_value DOUBLE, format_version INT, _ingested_at TIMESTAMP"
)

# Canonical single-surface grid resolution (sk default; matches territory_writer._XT_L/_XT_W).
_XT_L = 16
_XT_W = 12

# Canonical SPADL pitch dimensions (silly-kicks spadlconfig SSOT: 105 x 68).
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

# SPADL action types relevant to xT
_RELEVANT_TYPES = (
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "dribble",
    "goalkick",
    "clearance",
    "shot",
    "shot_penalty",
    "shot_freekick",
)

logger = logging.getLogger(__name__)
_guard_logger = logging.getLogger(f"{__name__}.guard")

_WORKFLOW_ID = "wf-xt-grids"


def _decide_rebuild(
    find_new: list[str],
    all_comps: list[str],
    *,
    upstream_changed: bool,
    global_exists: bool,
) -> tuple[list[str], bool]:
    """Pure guard decision (ADR-063, review L10 — testable without Spark).

    Returns ``(competitions_to_build, need_global)``.

    - When the upstream mart (``fct_action_values``) was re-derived (watermark changed), rebuild **all**
      competition grids + global — ``find_new_ids`` only catches genuinely-new competitions and would otherwise
      leave every existing per-comp grid stale (the build-if-absent bug this ADR fixes).
    - Otherwise build only genuinely-new competitions; rebuild global only if it is absent.
    """
    need_global = upstream_changed or not global_exists
    comps = sorted(set(find_new) | set(all_comps)) if upstream_changed else sorted(set(find_new))
    return comps, need_global


# Per-comp grids at/above this action count get the directionality assert; smaller ones are exempt
# (ADR-063 M5/M6 — small/noisy competitions must not false-fail).
_MIN_ACTIONS_DIRECTIONAL = 5000
# Write-if-changed materiality (ADR-063 R4). Cells below the floor are noise; a grid is written +
# propagated only if the max relative change among above-floor cells (vs the last-PROPAGATED grid =
# the current table contents) reaches the threshold. PROVISIONAL — tune after observing the drift
# logged each run; gating vs the current table (only ever holds propagated grids) bounds cumulative drift.
_MATERIALITY_REL_THRESHOLD = 0.10


def _to_sk_actions(actions: pd.DataFrame) -> pd.DataFrame:
    """Map the gold action slice to the sk ``ExpectedThreat.fit`` input contract.

    sk fit reads numeric ``type_id`` / ``result_id`` + ``start_x/y`` / ``end_x/y`` (no game/period/
    action ids). We map the canonical SPADL name columns to sk's numeric ids via ``spadlconfig`` (total
    over ``_RELEVANT_TYPES`` + the standard result names), keeping the pull schema-robust regardless of
    whether the mart carries the numeric ids.
    """
    from silly_kicks.spadl import config as spadlconfig

    out = pd.DataFrame(
        {
            "type_id": actions["type_name"].map(spadlconfig.actiontype_id).astype("Int64"),
            "result_id": actions["result_name"].map(spadlconfig.result_id).astype("Int64"),
            "start_x": actions["start_x"].astype(float),
            "start_y": actions["start_y"].astype(float),
            "end_x": actions["end_x"].astype(float),
            "end_y": actions["end_y"].astype(float),
        }
    )
    # Drop rows whose type/result name is not in the SPADL vocab (sk needs concrete ids).
    return out.dropna(subset=["type_id", "result_id"]).astype({"type_id": "int64", "result_id": "int64"})


def _fit_sk_grid(actions: pd.DataFrame) -> Any:
    """Fit a canonical 16x12 sk ``ExpectedThreat`` on a gold action slice. Returns the fitted model."""
    from silly_kicks.xthreat import ExpectedThreat

    return ExpectedThreat(l=_XT_L, w=_XT_W).fit(_to_sk_actions(actions))


def _project_physical_zones(model: Any, *, n_x: int = _XT_L, n_y: int = _XT_W) -> pd.DataFrame:
    """Project a fitted sk ``ExpectedThreat`` to a physical-oriented long-form zone grid (ADR-085 G-fix).

    Deterministic READ-ONLY projection of THE canonical model (never a second fit) for the dbt SQL zone
    consumers. Sampled at cell centres through the sk ``physical_grid`` seam so the ``.xT`` y-inversion is
    neutralised (ADR-041): ``zone_x`` rises toward the attacking goal (ascending physical x), ``zone_y``
    is physical bottom->top. Returns one row per cell: ``(zone_x 0..n_x-1, zone_y 0..n_y-1, xt_value)``.
    Matches the dbt binning ``floor(x / (105/n_x))`` / ``floor(y / (68/n_y))``.
    """
    import numpy as np
    from silly_kicks.xthreat import physical_grid

    cell_x = _PITCH_LENGTH / n_x
    cell_y = _PITCH_WIDTH / n_y
    xs = np.arange(n_x, dtype=np.float64) * cell_x + 0.5 * cell_x  # ascending physical x cell centres
    ys = np.arange(n_y, dtype=np.float64) * cell_y + 0.5 * cell_y
    grid = np.asarray(physical_grid(model, xs, ys), dtype=np.float64)  # (n_y, n_x) physically oriented
    rows = [
        {"zone_x": int(xi), "zone_y": int(yi), "xt_value": float(grid[yi, xi])}
        for yi in range(n_y)
        for xi in range(n_x)
    ]
    return pd.DataFrame(rows).astype({"zone_x": "int32", "zone_y": "int32", "xt_value": "float64"})


def _grid_drift(new_values: np.ndarray, previous_values: np.ndarray | None) -> float | None:
    """ADR-063 R4 materiality drift — delegates to the shared guard core (kept as a thin alias)."""
    from analytics.xt_grid_guards import grid_drift

    return grid_drift(new_values, previous_values)


def _write_grid_if_material(
    spark: SparkSession,
    model: Any,
    *,
    catalog: str,
    schema: str,
    comp_id: str,
    logger: logging.Logger,
) -> bool:
    """WARN-only differential + write-only-on-material-change (ADR-063 R4/H3). Returns True if written.

    ``model`` is a fitted sk ``ExpectedThreat``; the persisted payload is its ``to_dict()`` JSON.
    """
    from analytics.xt_grid_guards import validate_differential

    previous_values = _load_previous_grid(spark, catalog, schema, comp_id, logger)
    new_values = np.asarray(model.xT, dtype=np.float64)
    # Differential is advisory only (ADR-063 H3): never raise — the directionality assert is the hard
    # gate, and a hard differential would deadlock the auto-rebuild on a legitimate large shift.
    try:
        validate_differential(new_values, previous_values)
    except ValueError as exc:
        logger.warning("xT grid '%s' differential WARN (not blocking, ADR-063 H3): %s", comp_id, exc)
    drift = _grid_drift(new_values, previous_values)
    logger.info(
        "xT grid '%s' drift vs last-propagated: %s",
        comp_id,
        "n/a (no baseline)" if drift is None else f"{drift:.4f}",
    )
    if drift is not None and drift < _MATERIALITY_REL_THRESHOLD:
        logger.info(
            "xT grid '%s' immaterial (drift %.4f < %.2f) — skip write, no version bump (ADR-063 R4)",
            comp_id,
            drift,
            _MATERIALITY_REL_THRESHOLD,
        )
        return False
    payload = pd.DataFrame(
        {
            "competition_id": [comp_id],
            "xt_model_json": [json.dumps(model.to_dict())],
            "format_version": [1],
        }
    )
    write_delta_table(
        spark.createDataFrame(payload),
        catalog=catalog,
        schema=schema,
        table_name=_TABLE_NAME,
        replace_where=f"competition_id = '{comp_id}'",
        logger=logger,
    )

    # ADR-085 G-fix: write the derived long-form zone projection alongside the canonical JSON so the two
    # never diverge (same materiality gate). Serves the dbt SQL zone-lookup consumers.
    zones = _project_physical_zones(model)
    zones.insert(0, "competition_id", comp_id)
    zones["format_version"] = 1
    zones = zones.astype({"format_version": "int32"})
    write_delta_table(
        spark.createDataFrame(zones),
        catalog=catalog,
        schema=schema,
        table_name=_ZONES_TABLE,
        replace_where=f"competition_id = '{comp_id}'",
        logger=logger,
    )
    return True


class _ExpectedThreatGuard:
    """SkipGuard adapter for expected threat grid computation."""

    workflow_id = _WORKFLOW_ID

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which xT grids need (re)computation.

        Watermark-aware (ADR-063): if the upstream gold mart ``fct_action_values`` was re-derived
        (e.g. the SPADL->LTR migration), rebuild ALL grids — not just absent ones — because the
        build-if-absent pattern silently froze the grid for ~2 months. ``find_new_ids`` still catches
        genuinely-new competitions; ``check_upstream_freshness`` catches in-place re-derivation.
        """
        from ingestion.guards import (
            check_upstream_freshness,
            ensure_table,
            find_new_ids,
            resolve_upstream_tables_from_card,
        )

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        # ADR-085 G-fix: the derived long-form zone projection (written by the producer alongside the
        # JSON) for the dbt SQL zone consumers.
        ensure_table(spark, f"{catalog}.{schema}.{_ZONES_TABLE}", _ZONES_SCHEMA)
        types_sql = ", ".join(f"'{t}'" for t in _RELEVANT_TYPES)
        new_comps = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}",
            results_table=results_table,
            id_column="competition_id",
            source_filter=f"action_type IN ({types_sql}) AND competition_id IS NOT NULL",
        )

        try:
            existing = {
                str(row["competition_id"])
                for row in spark.table(results_table).select("competition_id").distinct().collect()
            }
            global_exists = "global" in existing
        except Exception:  # noqa: BLE001 — first-run fallback: any table-read failure means rebuild global grid
            global_exists = False

        # Watermark on the upstream mart (the card pins `{catalog}.dev_gold.fct_action_values`).
        upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        upstream_changed = check_upstream_freshness(spark, catalog, self.workflow_id, upstream).count > 0

        all_comps = _list_relevant_competition_ids(spark, catalog) if upstream_changed else []
        comps, need_global = _decide_rebuild(
            new_comps, all_comps, upstream_changed=upstream_changed, global_exists=global_exists
        )

        total = len(comps) + (1 if need_global else 0)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            metadata={"new_competition_ids": comps, "need_global": need_global},
        )


skip_guard = _ExpectedThreatGuard()


def _load_previous_grid(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competition_id: str,
    logger: logging.Logger,
) -> np.ndarray | None:
    """Load the previous run's fitted-model ``.xT`` values for the given competition_id.

    Returns ``None`` if no prior grid exists (first run for this competition_id, or the bronze table is
    empty / missing). Reads the stored ``xt_model_json`` (ADR-085) and reconstructs via ``from_dict``.
    """
    from silly_kicks.xthreat import ExpectedThreat

    from ingestion.utils import tolerate_missing_table

    table = f"{catalog}.{schema}.{_TABLE_NAME}"
    rows: list = []
    with tolerate_missing_table(
        logger,
        f"first run on {table} — no previous grid for differential check",
    ):
        rows = list(
            spark.sql(
                f"SELECT xt_model_json FROM {table} WHERE competition_id = '{competition_id}'"  # noqa: S608
            ).collect()
        )

    if not rows or not rows[0]["xt_model_json"]:
        return None
    model = ExpectedThreat.from_dict(json.loads(rows[0]["xt_model_json"]))
    return np.asarray(model.xT, dtype=np.float64)


def _list_relevant_competition_ids(spark: SparkSession, catalog: str) -> list[str]:
    """Return distinct competition_ids in fct_action_values restricted to
    xT-relevant action types.

    Bounded — the column has ~22 distinct values across all current
    sources. Returns a list of strings (we never join numerically here).
    Driver memory: O(n_competitions x ~30 bytes) = trivial.
    """
    from pyspark.sql.functions import col

    rows = (
        spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}")
        .filter(col("action_type").isin(list(_RELEVANT_TYPES)))
        .filter(col("competition_id").isNotNull())
        .select("competition_id")
        .distinct()
        .collect()
    )
    return [str(row["competition_id"]) for row in rows]


def _load_actions_for_competition(
    spark: SparkSession,
    catalog: str,
    competition_id: str,
) -> pd.DataFrame:
    """Pull a single competition's xT-relevant actions to driver memory.

    Bounded by per-competition row count — largest competition (a full league season's SPADL events)
    is ~500K rows x 6 cols ≈ 24 MB, well below the 16 GB driver budget. The ``.filter`` on
    ``competition_id`` is the toPandas bound.
    """
    from pyspark.sql.functions import col

    return (
        spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}")
        .filter(col("action_type").isin(list(_RELEVANT_TYPES)))
        .filter(col("competition_id") == competition_id)
        .selectExpr(
            "CAST(competition_id AS STRING) AS competition_id",
            "action_type AS type_name",
            "action_result AS result_name",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
        )
        .toPandas()  # type: ignore[union-attr]
    )


@workflow("wf-xt-grids", phase="grid_computation")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Fit per-competition and global sk ``ExpectedThreat`` models, persist ``to_dict`` to Delta.

    Streams per-competition action slices (~24 MB each, ``competition_id``-filtered). The GLOBAL model
    is fit on the union of the visited per-comp slices — sk ``ExpectedThreat`` has no additive
    ``ZoneCounters`` equivalent (unlike the retired v1), so the single-canonical-surface global grid
    requires the full corpus at the driver. This reverts the OPT-1 counter-accumulation for the global
    grid (ADR-085); peak driver memory is the union of the visited slices (~≤1 GB, bounded << 16 GB),
    and the per-comp ``.toPandas()`` calls stay individually ``competition_id``-filtered.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from analytics.xt_grid_guards import assert_directional, validate_structural

    # Use guard-provided metadata instead of inline re-computation
    new_comps = filter_result.metadata["new_competition_ids"]
    need_global = filter_result.metadata.get("need_global", False)

    if not new_comps and not need_global:
        logger.info("All xT grids already computed (including global) — skipping")
        return 0

    logger.info(
        "Need to compute %d new competition grids%s",
        len(new_comps),
        " + global" if need_global else "",
    )

    new_comp_set = {str(c) for c in new_comps}
    if need_global:
        all_comp_ids = _list_relevant_competition_ids(spark, catalog)
        comps_to_visit = sorted(set(all_comp_ids) | new_comp_set)
    else:
        comps_to_visit = sorted(new_comp_set)

    global_action_slices: list[pd.DataFrame] = []
    competitions_written = 0
    total_actions_accumulated = 0

    for comp_id in comps_to_visit:
        comp_actions = _load_actions_for_competition(spark, catalog, comp_id)
        if comp_actions.empty:
            continue
        n_events = len(comp_actions)
        total_actions_accumulated += n_events

        # Accumulate for the global fit (ADR-085 — no additive counters in sk).
        if need_global:
            global_action_slices.append(comp_actions)

        # Per-comp grid only for competitions the guard flagged as new.
        if comp_id in new_comp_set:
            if n_events < 100:
                logger.warning(
                    "Competition %s has only %d events — skipping per-comp grid",
                    comp_id,
                    n_events,
                )
                continue

            model = _fit_sk_grid(comp_actions)

            # Directionality gate for substantial competitions only (ADR-063 M5/M6): small/noisy
            # per-comp grids are exempt to avoid false-fails; large ones must not be silently inverted.
            if n_events >= _MIN_ACTIONS_DIRECTIONAL:
                assert_directional(model, competition_id=comp_id, logger=logger)

            if _write_grid_if_material(spark, model, catalog=catalog, schema=schema, comp_id=comp_id, logger=logger):
                competitions_written += 1
                logger.info(
                    "Competition %s: %d events, max xT=%.5f",
                    comp_id,
                    n_events,
                    float(np.asarray(model.xT).max()),
                )

    # ── Global grid (fit on the union of visited per-comp slices) ─────
    if need_global:
        if not global_action_slices:
            logger.warning("No relevant actions found across any competition — skipping global xT grid")
        else:
            global_actions = pd.concat(global_action_slices, ignore_index=True)
            global_model = _fit_sk_grid(global_actions)

            # HARD gate (ADR-063 R1): a non-directional / out-of-range global grid is a build FAILURE —
            # raises before any watermark is recorded, so a stale/broken grid forces a re-run rather
            # than silently propagating (the negative-DZV root cause). max_value=0.50 is the v1 ceiling.
            validate_structural(np.asarray(global_model.xT, dtype=np.float64), max_value=0.50)
            assert_directional(global_model, competition_id="global", logger=logger)

            if _write_grid_if_material(
                spark, global_model, catalog=catalog, schema=schema, comp_id="global", logger=logger
            ):
                logger.info(
                    "Global grid: %d events accumulated across %d competitions, max xT=%.5f",
                    len(global_actions),
                    len(comps_to_visit),
                    float(np.asarray(global_model.xT).max()),
                )

    logger.info(
        "Done — wrote %d competition grids%s (streamed %d total actions across %d competitions)",
        competitions_written,
        " + global" if need_global else "",
        total_actions_accumulated,
        len(comps_to_visit),
    )

    # Record the upstream watermark ONLY after a validated, successful run (ADR-063 H3). If the
    # directionality assert raised above, we never reach here → the guard re-fires next run rather
    # than recording "fresh" on an un-rebuilt grid (the silent-staleness failure this ADR targets).
    from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card

    upstream = resolve_upstream_tables_from_card(_WORKFLOW_ID, catalog, schema)
    record_watermarks(spark, catalog, _WORKFLOW_ID, upstream)
    return 0


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args("Compute Expected Threat grids from SPADL actions")
    logger = configure_logging("expected_threat")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
