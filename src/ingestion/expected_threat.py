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
import numpy.typing as npt
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

# sk4123 P2 (ADR-085 amendment, SK-XT-COUNTS): the daily producer fits from a DISTRIBUTED count
# aggregation (fit_from_counts) instead of pulling the corpus to the driver. These are the ONLY types
# the sk grid counts — _scoring_prob filters `shot`, _get_move_actions filters pass|dribble|cross; the
# rest of _RELEVANT_TYPES are handed to .fit() but never counted. == ExpectedThreat.MOVE_TYPE_NAMES /
# SHOT_TYPE_NAME (parity-tested); the count SQL restricts to these four so the grids stay byte-identical.
_MOVE_TYPE_NAMES: tuple[str, str, str] = ("pass", "dribble", "cross")
_SHOT_TYPE_NAME = "shot"

# sk floor for fit_from_counts (SK-XT-COUNTS, ADR-102). Runtime-asserted in the fit path as defense in
# depth; the serverless env is ADR-046 exact-pinned to this, so this only fires on a misbuilt env.
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 123, 0)

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


# ── sk4123 P2: distributed counts-based fit (SK-XT-COUNTS, ADR-085 amendment) ──────────────────────
_COUNT_KEYS = ("shot_counts", "goal_counts", "move_counts", "transition_start_counts", "transition_counts")


def _assert_sk_version() -> None:
    """Fail loud if the installed silly-kicks predates ``fit_from_counts`` (SK-XT-COUNTS)."""
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        required = ".".join(str(p) for p in _REQUIRED_SK_MIN)
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < {required} — ExpectedThreat.fit_from_counts "
            "unavailable; refusing to fit xT grids from counts."
        )


def _zone_flat_index(xs: npt.ArrayLike, ys: npt.ArrayLike, l: int = _XT_L, w: int = _XT_W) -> npt.NDArray[np.int64]:  # noqa: E741 — sk grid-dim convention (mirrors _get_cell_indexes)
    """Pure-numpy mirror of sk ``_get_flat_indexes``: the y-inverted ``(w-1-yj)*l + xi`` flat index.

    ``xi = clip(int(x / field_length * l), 0, l-1)`` (truncate-toward-zero, matching pandas
    ``.astype("int64")``), ``yj`` analogously. The single binning primitive the count aggregation uses;
    :func:`_sql_flat_index` is its Spark-SQL twin (same constants). Parity-gated vs sk.
    """
    x = np.asarray(xs, dtype=np.float64)
    y = np.asarray(ys, dtype=np.float64)
    xi = np.clip((x / _PITCH_LENGTH * l).astype(np.int64), 0, l - 1)
    yj = np.clip((y / _PITCH_WIDTH * w).astype(np.int64), 0, w - 1)
    return (w - 1 - yj) * l + xi


def _sql_flat_index(x_expr: str, y_expr: str, l: int = _XT_L, w: int = _XT_W) -> str:  # noqa: E741 — sk grid-dim convention
    """Spark-SQL twin of :func:`_zone_flat_index` (same formula/constants) — the distributed binning."""
    xi = f"least(greatest(cast({x_expr} / ({_PITCH_LENGTH} / {l}) as int), 0), {l - 1})"
    yj = f"least(greatest(cast({y_expr} / ({_PITCH_WIDTH} / {w}) as int), 0), {w - 1})"
    return f"({w} - 1 - {yj}) * {l} + {xi}"


def _aggregate_counts(actions: pd.DataFrame, l: int = _XT_L, w: int = _XT_W) -> dict[str, npt.NDArray[np.int64]]:  # noqa: E741 — sk grid-dim convention
    """Pandas reference aggregator — the 5 ``fit_from_counts`` arrays from a gold action slice.

    Mirrors the Spark SQL in :func:`_aggregate_counts_spark` EXACTLY (same type filters + binning) so the
    Spark-free tests pin correctness. ``actions`` is gold-form (``action_type`` / ``action_result`` name
    columns + ``start_x/y`` / ``end_x/y``). Only ``{pass,dribble,cross,shot}`` are counted (the sk grid
    ignores the rest). ``move_counts`` = valid-START moves (feeds ``_action_prob``); ``transition_start_counts``
    = valid-START-AND-END moves (the Singh row denominator — a DIFFERENT population, the D1-SPEC-01 crux);
    ``transition_counts`` = SUCCESSFUL valid-start-and-end ``flat(from)→flat(to)`` (the Singh numerator).
    NaN-start rows are dropped (matches sk ``_count``).
    """
    n = l * w
    is_shot = actions["action_type"] == _SHOT_TYPE_NAME
    is_move = actions["action_type"].isin(_MOVE_TYPE_NAMES)
    is_success = actions["action_result"] == "success"
    valid_start = actions["start_x"].notna() & actions["start_y"].notna()
    valid_end = actions["end_x"].notna() & actions["end_y"].notna()

    def _zone(mask: pd.Series) -> npt.NDArray[np.int64]:
        m = mask & valid_start
        vec = np.zeros(n, dtype=np.int64)
        if bool(m.any()):
            flat = _zone_flat_index(actions.loc[m, "start_x"], actions.loc[m, "start_y"], l, w)
            np.add.at(vec, flat, 1)
        return vec.reshape((w, l))

    tmask = is_move & valid_start & valid_end & is_success
    transition = np.zeros((n, n), dtype=np.int64)
    if bool(tmask.any()):
        flat_s = _zone_flat_index(actions.loc[tmask, "start_x"], actions.loc[tmask, "start_y"], l, w)
        flat_e = _zone_flat_index(actions.loc[tmask, "end_x"], actions.loc[tmask, "end_y"], l, w)
        np.add.at(transition, (flat_s, flat_e), 1)

    return {
        "shot_counts": _zone(is_shot),
        "goal_counts": _zone(is_shot & is_success),
        "move_counts": _zone(is_move),
        "transition_start_counts": _zone(is_move & valid_end),
        "transition_counts": transition,
    }


def _sum_counts(per_comp: list[dict[str, npt.NDArray[np.int64]]]) -> dict[str, npt.NDArray[np.int64]]:
    """Element-wise sum of per-competition count dicts → the ``global`` counts (additivity, SK-XT-COUNTS)."""
    if not per_comp:
        raise ValueError("_sum_counts requires at least one competition's counts")
    return {k: np.sum([c[k] for c in per_comp], axis=0) for k in _COUNT_KEYS}


def _counts_n_actions(counts: dict[str, npt.NDArray[np.int64]]) -> int:
    """Relevant action count (valid-start shots + moves) — the small/empty-grid gate input."""
    return int(counts["shot_counts"].sum() + counts["move_counts"].sum())


def _fit_grid_from_counts(counts: dict[str, npt.NDArray[np.int64]]) -> Any:
    """Fit a canonical 16x12 sk ``ExpectedThreat`` from the 5 zone-count arrays (SK-XT-COUNTS)."""
    from silly_kicks.xthreat import ExpectedThreat

    _assert_sk_version()
    return ExpectedThreat(l=_XT_L, w=_XT_W).fit_from_counts(
        shot_counts=counts["shot_counts"],
        goal_counts=counts["goal_counts"],
        move_counts=counts["move_counts"],
        transition_start_counts=counts["transition_start_counts"],
        transition_counts=counts["transition_counts"],
    )


def _aggregate_counts_spark(spark: SparkSession, catalog: str) -> dict[str, dict[str, npt.NDArray[np.int64]]]:
    """ONE distributed Spark pass → per-``competition_id`` 5-count arrays (no corpus pull to the driver).

    Two ``groupBy`` reductions over ``fct_action_values`` restricted to ``{pass,dribble,cross,shot}``:
    (A) per ``(competition_id, flat_start)`` conditional sums → shot/goal/move/transition_start;
    (B) per ``(competition_id, flat_start, flat_end)`` successful-move counts → transition. Only the tiny
    reduced tables (≤~192 and ≤~192² cells per comp) cross to the driver; arrays are built via
    ``vector.reshape((w, l))`` (matching sk ``_count``). This restores the OPT-1 single-pass distributed
    accumulation ADR-085 had to revert (sk lacked a counts fit). Returns ``{competition_id_str: counts}``.
    """
    n = _XT_L * _XT_W
    gold = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}"
    move_in = ", ".join(f"'{t}'" for t in _MOVE_TYPE_NAMES)
    type_in = ", ".join(f"'{t}'" for t in (*_MOVE_TYPE_NAMES, _SHOT_TYPE_NAME))
    flat_s = _sql_flat_index("start_x", "start_y")
    flat_e = _sql_flat_index("end_x", "end_y")

    zone_sql = f"""
        SELECT CAST(competition_id AS STRING) AS comp, {flat_s} AS fs,
            SUM(CASE WHEN action_type = '{_SHOT_TYPE_NAME}' THEN 1 ELSE 0 END) AS shot_c,
            SUM(CASE WHEN action_type = '{_SHOT_TYPE_NAME}' AND action_result = 'success' THEN 1 ELSE 0 END)
                AS goal_c,
            SUM(CASE WHEN action_type IN ({move_in}) THEN 1 ELSE 0 END) AS move_c,
            SUM(CASE WHEN action_type IN ({move_in}) AND end_x IS NOT NULL AND end_y IS NOT NULL THEN 1 ELSE 0 END)
                AS tstart_c
        FROM {gold}
        WHERE action_type IN ({type_in}) AND competition_id IS NOT NULL
            AND start_x IS NOT NULL AND start_y IS NOT NULL
        GROUP BY CAST(competition_id AS STRING), {flat_s}
    """  # noqa: S608 -- fixed identifiers + literal type names, no user input

    trans_sql = f"""
        SELECT CAST(competition_id AS STRING) AS comp, {flat_s} AS fs, {flat_e} AS fe, COUNT(*) AS tc
        FROM {gold}
        WHERE action_type IN ({move_in}) AND action_result = 'success' AND competition_id IS NOT NULL
            AND start_x IS NOT NULL AND start_y IS NOT NULL AND end_x IS NOT NULL AND end_y IS NOT NULL
        GROUP BY CAST(competition_id AS STRING), {flat_s}, {flat_e}
    """  # noqa: S608 -- fixed identifiers + literal type names, no user input

    def _blank() -> dict[str, npt.NDArray[np.int64]]:
        return {
            "shot_counts": np.zeros((_XT_W, _XT_L), dtype=np.int64),
            "goal_counts": np.zeros((_XT_W, _XT_L), dtype=np.int64),
            "move_counts": np.zeros((_XT_W, _XT_L), dtype=np.int64),
            "transition_start_counts": np.zeros((_XT_W, _XT_L), dtype=np.int64),
            "transition_counts": np.zeros((n, n), dtype=np.int64),
        }

    result: dict[str, dict[str, npt.NDArray[np.int64]]] = {}
    for r in spark.sql(zone_sql).collect():
        c = result.setdefault(str(r["comp"]), _blank())
        f = int(r["fs"])
        c["shot_counts"].reshape(-1)[f] = int(r["shot_c"])
        c["goal_counts"].reshape(-1)[f] = int(r["goal_c"])
        c["move_counts"].reshape(-1)[f] = int(r["move_c"])
        c["transition_start_counts"].reshape(-1)[f] = int(r["tstart_c"])
    for r in spark.sql(trans_sql).collect():
        c = result.setdefault(str(r["comp"]), _blank())
        c["transition_counts"][int(r["fs"]), int(r["fe"])] = int(r["tc"])
    return result


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


def _grids_payload(model: Any, comp_id: str) -> pd.DataFrame:
    """The bronze ``expected_threat_grids`` write payload — dtypes pinned to ``_RESULTS_SCHEMA``.

    ``format_version`` is cast to int32 to match the ``format_version INT`` column: a bare Python ``1``
    infers to int64/LONG, and Delta ``replaceWhere`` rejects the LONG↔INT merge
    (``DELTA_FAILED_TO_MERGE_FIELDS``). Guarded by ``test_writer_ddl_dtype_parity``.
    """
    return pd.DataFrame(
        {
            "competition_id": [comp_id],
            "xt_model_json": [json.dumps(model.to_dict())],
            "format_version": [1],
        }
    ).astype({"format_version": "int32"})


def _zones_payload(model: Any, comp_id: str) -> pd.DataFrame:
    """The bronze ``expected_threat_grid_zones`` write payload — dtypes pinned to ``_ZONES_SCHEMA``
    (``zone_x``/``zone_y``/``format_version`` all INT → int32)."""
    zones = _project_physical_zones(model)
    zones.insert(0, "competition_id", comp_id)
    zones["format_version"] = 1
    return zones.astype({"zone_x": "int32", "zone_y": "int32", "format_version": "int32"})


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
    write_delta_table(
        spark.createDataFrame(_grids_payload(model, comp_id)),
        catalog=catalog,
        schema=schema,
        table_name=_TABLE_NAME,
        replace_where=f"competition_id = '{comp_id}'",
        logger=logger,
    )

    # ADR-085 G-fix: write the derived long-form zone projection alongside the canonical JSON so the two
    # never diverge (same materiality gate). Serves the dbt SQL zone-lookup consumers.
    write_delta_table(
        spark.createDataFrame(_zones_payload(model, comp_id)),
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

    Fits from a SINGLE distributed count aggregation (``fit_from_counts``, SK-XT-COUNTS): one Spark
    ``groupBy`` reduces the corpus to per-(competition, zone) counts, and every grid — including the
    ``global`` grid (the element-wise sum of the per-competition counts, additive) — is fit from those.
    No ``.toPandas()`` of action rows, no ``pd.concat`` of the corpus at the driver. This restores the
    OPT-1 single-pass distributed accumulation that ADR-085 had to revert when sk exposed only
    ``.fit(actions)``. Grids are byte-identical to the ``.fit`` path.
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

    # ONE distributed count aggregation over the whole corpus (SK-XT-COUNTS): the only data crossing to
    # the driver is the tiny per-(competition, zone) reduced tables — never the ~9.5M action rows.
    counts_by_comp = _aggregate_counts_spark(spark, catalog)

    competitions_written = 0
    for comp_id in sorted(new_comp_set):
        counts = counts_by_comp.get(comp_id)
        if counts is None:
            continue
        n_events = _counts_n_actions(counts)
        if n_events < 100:
            logger.warning("Competition %s has only %d relevant actions — skipping per-comp grid", comp_id, n_events)
            continue

        model = _fit_grid_from_counts(counts)

        # Directionality gate for substantial competitions only (ADR-063 M5/M6): small/noisy per-comp
        # grids are exempt to avoid false-fails; large ones must not be silently inverted.
        if n_events >= _MIN_ACTIONS_DIRECTIONAL:
            assert_directional(model, competition_id=comp_id, logger=logger)

        if _write_grid_if_material(spark, model, catalog=catalog, schema=schema, comp_id=comp_id, logger=logger):
            competitions_written += 1
            logger.info(
                "Competition %s: %d relevant actions, max xT=%.5f",
                comp_id,
                n_events,
                float(np.asarray(model.xT).max()),
            )

    # ── Global grid (element-wise sum of EVERY competition's counts — additive, no second pass) ──────
    if need_global:
        if not counts_by_comp:
            logger.warning("No relevant actions found across any competition — skipping global xT grid")
        else:
            global_counts = _sum_counts(list(counts_by_comp.values()))
            global_model = _fit_grid_from_counts(global_counts)

            # HARD gate (ADR-063 R1): a non-directional / out-of-range global grid is a build FAILURE —
            # raises before any watermark is recorded, so a stale/broken grid forces a re-run rather
            # than silently propagating (the negative-DZV root cause). max_value=0.50 is the v1 ceiling.
            validate_structural(np.asarray(global_model.xT, dtype=np.float64), max_value=0.50)
            assert_directional(global_model, competition_id="global", logger=logger)

            if _write_grid_if_material(
                spark, global_model, catalog=catalog, schema=schema, comp_id="global", logger=logger
            ):
                logger.info(
                    "Global grid: %d relevant actions across %d competitions, max xT=%.5f",
                    _counts_n_actions(global_counts),
                    len(counts_by_comp),
                    float(np.asarray(global_model.xT).max()),
                )

    logger.info(
        "Done — wrote %d competition grids%s (%d competitions aggregated in one distributed pass)",
        competitions_written,
        " + global" if need_global else "",
        len(counts_by_comp),
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
