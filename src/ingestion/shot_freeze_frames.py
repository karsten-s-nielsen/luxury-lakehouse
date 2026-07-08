"""Bronze writer + compute driver for the pre-shot tracking freeze-frame snapshots (Task 0.5).

Driver-side persistence for the per-(shot, player) snapshots produced by
``analytics.action_context.tracking_snapshots``. This lives in the INGESTION layer
(not analytics) because it calls ``ingestion.utils.write_delta_table`` — ``analytics``
must not import ``ingestion`` (import-linter contract; the allowed direction is
ingestion -> analytics). The canonical column list + table name are the data-shape
source of truth in the analytics builder, so they are imported from there.

``main()`` is the ``compute_shot_freeze_frames`` mega-job task (registered in
``pyproject.toml`` [project.scripts]). It reads shot actions from ``bronze.spadl_actions``,
converts the linked tracking frames to canonical home-LTR AC frames via the SAME
``analytics.action_context.pipeline._convert_tracking_batch`` the AC pipeline uses, builds
the per-(shot, player) snapshots with ``build_tracking_snapshots_spark``, and writes
``bronze.shot_freeze_frames`` (``replaceWhere`` per ``match_key``).

==============================================================================
INTERIM SCOPE — GradientSports + SkillCorner ONLY (deliberate; do not widen casually)
==============================================================================
The daily/incremental run defaults to ``--providers gradientsports,skillcorner`` (the
``xg_model_v3`` training cohort). IDSSE and Metrica are DEFERRED because this driver converts
frames **on the driver, one ``(match, period)`` at a time** (``_process_match`` → per-period
``.toPandas()`` → ``_convert_tracking_batch``): IDSSE periods are ~1.5M rows, so that risks a
16 GB-driver OOM, and the single-group-per-period path has no M13 boundary de-dup (the AC
pipeline's per-batch owner assignment) to guard against double-counting shots at batch edges.
GS/SkillCorner periods are small enough that neither concern bites.

LONG-TERM (how IDSSE/Metrica onboard): replace this driver-side loop with a DISTRIBUTED SINK
mirroring ``compute_action_context`` — a per-``(match, period)`` work-queue in
``ingestion.action_context_queue`` + a ``compute_shot_freeze_frames_drain_worker`` entry point.
That (a) moves conversion onto Spark executors (via the same ``mapInPandas`` frame-batching +
M13 owner de-dup ``_process_tracking_match`` uses) so the dense providers scale, and (b)
amortizes the serverless cold-start across the whole drain instead of paying it per match.
IDSSE + Metrica are enabled (``--providers …,idsse,metrica``) only once that sink lands.

Design notes / assumptions (surfaced for review — see the PR description):

* **Frame source** — ``build_tracking_snapshots_spark`` requires ALREADY-CONVERTED, home-LTR
  AC result frames (``sk_frame_adapters`` shape), NOT raw bronze. Only
  ``_convert_tracking_batch`` produces them, and it needs the per-provider ``MatchMeta`` +
  the dispatcher's period-relative clock. This driver therefore MIRRORS the input-prep of
  ``ingestion.action_context._process_tracking_match`` (metadata resolution via the shared
  importable helpers; clock rebasing) — kept in lockstep with that function by construction.
  The per-period conversion runs single-process on the driver (mirrors
  ``_run_profile_on_driver``; one match/period fits the 16 GB driver), NOT distributed —
  the snapshot set is tiny (~shots * players). IDSSE periods are ~1.5M rows: bounded, but
  heavy; see the ``.toPandas`` note below.
* **``match_key`` + ``access_tier`` resolution** — ``match_key`` is a Kimball surrogate resolved
  in the GOLD ``dim_matches`` (ADR-013: bronze carries only native ids). The writer/DDL key on
  ``match_key``, so this driver resolves it from ``dim_matches`` on
  ``(data_source, match_id_native)`` and stamps it onto the shot actions before the snapshot
  build. The SAME ``dim_matches`` row also carries the ADR-064 per-match ``access_tier``
  (``public``/``restricted``) — ``_resolve_match_identity`` returns both from a single read, and
  the driver stamps ``access_tier`` per snapshot row so a downstream HF publisher can split public
  vs restricted rows (``bronze.spadl_actions`` does not carry it; the builder does not compute it).
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import TYPE_CHECKING, NamedTuple

from analytics.action_context.tracking_snapshots import (
    _SHOT_FF_COLUMNS,
    _TABLE_NAME,
    _shot_ff_struct_type,
    build_tracking_snapshots_spark,
)
from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession

    from analytics.action_context.work_unit import MatchMeta

logger = logging.getLogger(__name__)

# Tracking providers whose linked frames carry a full-enough player set for the pre-shot
# freeze frame. Mirrors ``ingestion.action_context._TRACKING_PROVIDERS`` (the AC tracking tier).
_TRACKING_PROVIDERS: frozenset[str] = frozenset({"gradientsports", "skillcorner", "idsse", "metrica"})

# INTERIM SCOPE (see module docstring): the daily run is limited to GradientSports + SkillCorner —
# the only providers small enough for the current driver-side per-(match, period) conversion.
# IDSSE/Metrica onboard with the distributed-sink rewrite (work-queue + drain worker), NOT by
# widening this default. Overridable via ``--providers`` for a deliberate opt-in.
_DEFAULT_PROVIDERS = "gradientsports,skillcorner"

# ── Driver input-table column contract (SSOT for the discovery/resolution SQL AND the schema guard) ──
# The join that resolves the Kimball match_key surrogate is:
#   bronze.spadl_actions.<_SPADL_DATA_SOURCE_COL> = dev_gold.dim_matches.<_DIM_MATCHES_PROVIDER_COL>
#   AND bronze.spadl_actions.<_SPADL_NATIVE_ID_COL> = dev_gold.dim_matches.<_DIM_MATCHES_NATIVE_ID_COL>
#   → dev_gold.dim_matches.<_DIM_MATCHES_KEY_COL>
# dim_matches' identity columns are provider / native_match_id / match_key — NOT data_source /
# match_id_native (those are the bronze.spadl_actions names). Verified live 2026-07-07; a prior
# join on dim_matches.data_source/match_id_native raised UNRESOLVED_COLUMN. The schema guard
# (test_shot_freeze_frames_schema_guard.py) asserts these constants against the real schemas.
_SPADL_DATA_SOURCE_COL = "data_source"
_SPADL_NATIVE_ID_COL = "match_id_native"
_SPADL_TYPE_ID_COL = "type_id"
_DIM_MATCHES_PROVIDER_COL = "provider"
_DIM_MATCHES_NATIVE_ID_COL = "native_match_id"
_DIM_MATCHES_KEY_COL = "match_key"
# ADR-064 per-match access tier ('public'/'restricted'); resolved from the SAME dim_matches row as
# match_key and stamped per-row onto shot_freeze_frames for the downstream public/restricted HF split.
_DIM_MATCHES_ACCESS_TIER_COL = "access_tier"


class _MatchIdentity(NamedTuple):
    """The gold-``dim_matches`` identity for a tracking match: the Kimball ``match_key`` surrogate plus
    the ADR-064 per-match ``access_tier`` — both resolved from a SINGLE ``dim_matches`` read."""

    match_key: int
    access_tier: str | None


def write_shot_freeze_frames(
    snapshots_df: DataFrame,
    catalog: str,
    schema: str,
    match_keys: Sequence[int],
    *,
    row_count: int | None = None,
) -> int:
    """Persist the collected per-(shot, player) snapshot set to ``bronze.shot_freeze_frames``.

    Driver-side persistence step: the per-(match, period) cogroup runs
    :func:`analytics.action_context.tracking_snapshots.build_tracking_snapshots_spark` inside its
    ``applyInPandas`` UDF (no Delta writes from executors — serverless forbids it) and yields
    ``snapshots_df``, a Spark DataFrame conforming to :data:`_SHOT_FF_COLUMNS`. We select the
    canonical column order and write it idempotently, ``replaceWhere`` keyed on ``match_key``
    (mirrors ``xg_model_v2.run_pipeline``'s per-competition bulk write). All periods of a match land
    in the same bulk write, so a ``match_key``-only predicate is safe.

    Parameters
    ----------
    snapshots_df :
        Spark DataFrame with (at least) the :data:`_SHOT_FF_COLUMNS` columns.
    catalog, schema :
        Unity Catalog target (e.g. ``soccer_analytics`` / ``bronze``).
    match_keys :
        The ``match_key`` set covered by this write — the run's work units. Used to build the
        ``replaceWhere`` predicate so a re-run overwrites exactly these matches.
    row_count :
        Optional pre-computed row count forwarded to ``write_delta_table`` to skip a redundant
        ``df.count()`` DAG recomputation.

    Returns
    -------
    int
        Number of rows written.

    Raises
    ------
    ValueError
        If ``match_keys`` is empty (a ``replaceWhere`` with no keys would be a no-op predicate
        that silently writes nothing).
    """
    from ingestion.utils import write_delta_table

    keys = sorted({int(k) for k in match_keys})
    if not keys:
        msg = "write_shot_freeze_frames requires a non-empty match_keys set for the replaceWhere predicate"
        raise ValueError(msg)

    ordered = snapshots_df.select(*_SHOT_FF_COLUMNS)
    key_list = ", ".join(str(k) for k in keys)
    return write_delta_table(
        ordered,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_key IN ({key_list})",
        logger=logger,
        row_count=row_count,
    )


# ── CLI arg parsing (pure) ────────────────────────────────────────────────


def _parse_freeze_frame_match_ids_arg(raw: str | None) -> tuple[str, list[str]] | None:
    """Parse the ``--match-ids`` CLI value into ``(provider, [native_id, ...])``.

    Format ``"provider:id1,id2"`` (mirrors ``action_context._parse_action_match_ids_arg`` but
    without the per-period variant — the freeze-frame writer is per-MATCH, ``replaceWhere`` keyed
    on ``match_key``). ``None`` / empty → ``None`` (incremental discovery path). Unknown provider or
    a malformed value raises ``SystemExit`` (loud CLI failure, no silent default).
    """
    if raw is None or raw.strip() == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2', got {raw!r}. Valid providers: {sorted(_TRACKING_PROVIDERS)}"
        )
    provider, _, id_str = raw.partition(":")
    provider = provider.strip()
    if provider not in _TRACKING_PROVIDERS:
        raise SystemExit(f"Unknown provider {provider!r}. Valid: {sorted(_TRACKING_PROVIDERS)}")
    ids = [i.strip() for i in id_str.split(",") if i.strip()]
    if not ids:
        return None
    return (provider, ids)


def _parse_providers_arg(raw: str | None) -> frozenset[str]:
    """Parse/validate the ``--providers`` comma-list against the known tracking-provider set.

    Empty / ``None`` → the interim default (``gradientsports,skillcorner``). Unknown providers raise
    a loud ``SystemExit``. Enabling ``idsse``/``metrica`` here is a deliberate opt-in (see the module
    docstring's INTERIM SCOPE note — the driver-side conversion is not yet safe for their dense
    periods).
    """
    if raw is None or raw.strip() == "":
        raw = _DEFAULT_PROVIDERS
    selected = {p.strip() for p in raw.split(",") if p.strip()}
    if not selected:
        raise SystemExit(f"--providers must be a non-empty comma-list. Valid: {sorted(_TRACKING_PROVIDERS)}")
    unknown = selected - _TRACKING_PROVIDERS
    if unknown:
        raise SystemExit(f"Unknown provider(s) in --providers: {sorted(unknown)}. Valid: {sorted(_TRACKING_PROVIDERS)}")
    return frozenset(selected)


def _units_from_match_ids(parsed: tuple[str, list[str]], selected: frozenset[str]) -> list[tuple[str, str]]:
    """Build ``(provider, native_id)`` units from a parsed ``--match-ids``, enforcing ``--providers`` scope.

    Rejects (loud ``SystemExit``) a backfill whose provider is outside the selected set — so a
    ``--match-ids idsse:…`` cannot silently bypass the INTERIM GS+SkillCorner scope.
    """
    provider, ids = parsed
    if provider not in selected:
        raise SystemExit(
            f"--match-ids provider {provider!r} is outside the selected --providers set "
            f"{sorted(selected)}. Add it to --providers to process it (INTERIM SCOPE — idsse/metrica "
            f"await the distributed-sink rewrite; see the module docstring)."
        )
    return [(provider, native_id) for native_id in ids]


# ── Incremental missing-match discovery ────────────────────────────────────


def _missing_units_sql(catalog: str, gold_schema: str, providers: frozenset[str]) -> str:
    """Build the incremental discovery SQL: tracking shot-matches NOT yet in ``shot_freeze_frames``.

    Anti-set on ``match_key``: every ``providers``-scoped tracking match that has a ``shot`` action in
    ``bronze.spadl_actions`` AND resolves a ``match_key`` in gold ``dim_matches``, MINUS the
    ``match_key`` set already present in ``bronze.shot_freeze_frames``. So the daily run only
    processes new matches; a ``--match-ids`` backfill overrides this. ``providers`` is the
    ``--providers``-selected set (default GS+SkillCorner) — discovery NEVER considers a provider
    outside it (INTERIM SCOPE, see module docstring).

    The join uses the REAL column names on each side (see the module SSOT constants):
    ``spadl_actions.data_source = dim_matches.provider`` AND
    ``spadl_actions.match_id_native = dim_matches.native_match_id``. The shot filter uses
    ``type_id`` (bronze.spadl_actions has ``type_id``, NOT ``type_name``). Identifiers are validated
    by ``IDENTIFIER_RE`` at the CLI boundary; the provider list (from the validated ``--providers``
    allowlist) + the shot ``type_id`` int are literals — no injection.
    """
    from analytics.action_context.tracking_snapshots import _shot_type_id

    providers_sql = ", ".join(f"'{p}'" for p in sorted(providers))
    shot_type_id = _shot_type_id()
    return (
        f"SELECT dm.{_DIM_MATCHES_PROVIDER_COL} AS provider, "  # noqa: S608 — identifiers validated by IDENTIFIER_RE; literals only
        f"CAST(sa.{_SPADL_NATIVE_ID_COL} AS STRING) AS native_id, dm.{_DIM_MATCHES_KEY_COL} AS match_key "
        f"FROM {catalog}.bronze.spadl_actions sa "
        f"JOIN {catalog}.{gold_schema}.dim_matches dm "
        f"  ON sa.{_SPADL_DATA_SOURCE_COL} = dm.{_DIM_MATCHES_PROVIDER_COL} "
        f"  AND CAST(sa.{_SPADL_NATIVE_ID_COL} AS STRING) = CAST(dm.{_DIM_MATCHES_NATIVE_ID_COL} AS STRING) "
        f"WHERE sa.{_SPADL_DATA_SOURCE_COL} IN ({providers_sql}) "
        f"  AND sa.{_SPADL_TYPE_ID_COL} = {shot_type_id} "
        f"  AND dm.{_DIM_MATCHES_KEY_COL} NOT IN (SELECT match_key FROM {catalog}.bronze.{_TABLE_NAME}) "
        f"GROUP BY dm.{_DIM_MATCHES_PROVIDER_COL}, sa.{_SPADL_NATIVE_ID_COL}, dm.{_DIM_MATCHES_KEY_COL}"
    )


def _discover_missing_units(
    spark: SparkSession, catalog: str, gold_schema: str, providers: frozenset[str]
) -> list[tuple[str, str]]:
    """Return ``[(provider, native_id), ...]`` for ``providers``-scoped shot-matches not yet freeze-framed.

    Discovery is CONSTRAINED to the ``--providers``-selected set (default GS+SkillCorner) — an
    idsse/metrica match is never returned unless explicitly enabled (INTERIM SCOPE). Uses
    ``tolerate_missing_table`` so the first run (before the table is populated / when the migration
    has just created an empty table) does not spuriously fail; a genuinely absent
    ``shot_freeze_frames`` means "nothing processed yet" → discover everything in scope.
    """
    from ingestion.utils import tolerate_missing_table

    with tolerate_missing_table(logger, "shot_freeze_frames not found — treating all tracking matches as unprocessed"):
        rows = spark.sql(_missing_units_sql(catalog, gold_schema, providers)).collect()
        return [(str(r["provider"]), str(r["native_id"])) for r in rows]
    return []


def _resolve_match_identity(
    spark: SparkSession, catalog: str, gold_schema: str, provider: str, native_id: str
) -> _MatchIdentity:
    """Resolve the ``(match_key, access_tier)`` identity from gold ``dim_matches`` in ONE read (ADR-013).

    ``bronze.spadl_actions`` carries only native ids; the freeze-frame writer keys on ``match_key`` and
    stamps the ADR-064 per-match ``access_tier`` per row, so both are resolved here from the SAME
    ``dim_matches`` row (no second query). Raises loudly (no silent NULL key) if the match is absent
    from ``dim_matches`` — that would mean the match has not been Kimball-dimensioned yet. The raw
    ``access_tier`` value is passed through unchanged (never invented); the downstream publisher's
    ``split_restricted`` fail-safes a NULL to restricted.
    """
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    rows = (
        spark.table(f"{catalog}.{gold_schema}.dim_matches")
        .filter(
            (spark_fn.col(_DIM_MATCHES_PROVIDER_COL) == provider)
            & (spark_fn.col(_DIM_MATCHES_NATIVE_ID_COL).cast("string") == str(native_id))
        )
        .select(_DIM_MATCHES_KEY_COL, _DIM_MATCHES_ACCESS_TIER_COL)
        .limit(1)
        .collect()
    )
    if not rows:
        raise RuntimeError(f"No match_key in {catalog}.{gold_schema}.dim_matches for {provider}:{native_id}")
    row = rows[0]
    access_tier = row[_DIM_MATCHES_ACCESS_TIER_COL]
    return _MatchIdentity(int(row[_DIM_MATCHES_KEY_COL]), None if access_tier is None else str(access_tier))


# ── Per-provider match metadata + clock rebasing ──────────────────────────
# LOCKSTEP: these two helpers MIRROR the input-prep of
# ``ingestion.action_context._process_tracking_match`` (metadata resolution + the dispatch-clock
# rebasing). They are a DELIBERATE lockstep mirror (NOT a shared extraction) so that a change to the
# AC production hot path is not coupled to this newer, lower-traffic driver — but any change to the
# per-provider metadata resolution or clock rebasing in ``_process_tracking_match`` MUST be applied
# here in the same PR (the SkillCorner-P2 / Metrica-clock silent-drop class lives exactly here).
# See the module docstring's "Frame source" note. A future refactor may extract a single shared
# helper; that is a reviewed design decision, not a silent one.


def _resolve_tracking_match_meta(
    spark: SparkSession,
    catalog: str,
    provider: str,
    native_id: str,
    actions_pdf: pd.DataFrame,
) -> MatchMeta:
    """Resolve the per-provider ``MatchMeta`` (home team, LTR flags, GS rosters) for one match.

    Mirror of ``_process_tracking_match``'s metadata block — reuses the SAME importable helpers
    (``derive_idsse_*``, ``extract_gradientsports_match_metadata``, ``_build_gradientsports_roster_dicts``)
    so the resolution logic itself is not re-implemented.
    """
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from analytics.action_context.work_unit import MatchMeta

    home_start_left = True
    home_team_start_left_extratime: bool | None = None
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None

    if provider == "idsse":
        from silly_kicks.providers.sportec import (
            derive_idsse_home_team_start_left,
            derive_idsse_home_team_start_left_extratime,
            shape_events_to_native,
        )

        events_pdf = (
            spark.table(f"{catalog}.bronze.idsse_events").filter(spark_fn.col("match_id") == native_id).toPandas()
        )
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = shape_events_to_native(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
        home_team_start_left_extratime = derive_idsse_home_team_start_left_extratime(adapted_events, home_team_id)
    elif provider == "metrica":
        home_team_id = "Home"
    elif provider == "skillcorner":
        row = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(spark_fn.col("match_id") == native_id)
            .select("home_team_id")
            .limit(1)
            .collect()[0]
        )
        home_team_id = str(row["home_team_id"])
    elif provider == "gradientsports":
        from ingestion.action_context import (
            _GS_EVENTS_META_COLS,
            _GS_ROSTER_COLS,
            _build_gradientsports_roster_dicts,
        )
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        events_pdf = (
            spark.table(f"{catalog}.bronze.gradientsports_events")
            .filter(spark_fn.col("match_id") == native_id)
            .select(*[f"`{c}`" for c in _GS_EVENTS_META_COLS])
            .toPandas()
        )
        gs_meta = extract_gradientsports_match_metadata(events_pdf)
        home_team_id = str(gs_meta["home_team_id"])
        home_start_left = gs_meta["home_team_start_left"]
        home_team_start_left_extratime = gs_meta["home_team_start_left_extratime"]
        roster_pdf = (
            spark.table(f"{catalog}.bronze.gradientsports_roster")
            .filter(spark_fn.col("match_id") == native_id)
            .select(*[f"`{c}`" for c in _GS_ROSTER_COLS])
            .toPandas()
        )
        if not roster_pdf.empty:
            gs_team_side_to_id, gs_jersey_to_player_id, gs_gk_player_ids = _build_gradientsports_roster_dicts(
                roster_pdf, home_team_id
            )
    else:
        raise ValueError(f"Unknown tracking provider: {provider}")

    return MatchMeta(
        home_team_id=home_team_id,
        home_start_left=home_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
        gs_team_side_to_id=gs_team_side_to_id,
        gs_jersey_to_player_id=gs_jersey_to_player_id,
        gs_gk_player_ids=gs_gk_player_ids,
    )


def _prepare_tracking_frames_for_match(trk_sdf: DataFrame, provider: str) -> DataFrame:
    """Rebase the dispatch clock to period-relative (per provider) so frames↔actions align.

    Mirror of ``_process_tracking_match``'s clock rebasing (ADR-040) — WITHOUT the ``frame_batch_id``
    column (freeze frames convert a whole period per group, they do not sub-batch). The rebased
    ``timestamp`` is what ``_convert_tracking_batch`` uses as the period-relative clock the converted
    frames carry, so a stale absolute clock here silently empties the action↔frame linkage.
    """
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    if provider == "gradientsports":
        # ADD an alias (NOT a rename) — the GS converter reads period_elapsed_time.
        return trk_sdf.withColumn("timestamp", spark_fn.col("period_elapsed_time"))

    if provider == "metrica":
        from pyspark.sql import Window

        period_w = Window.partitionBy("match_id", "period")
        fr_col = spark_fn.coalesce(spark_fn.col("frame_rate").cast("double"), spark_fn.lit(25.0))
        return (
            trk_sdf.withColumn("_period_min_frame", spark_fn.min("frame").over(period_w))
            .withColumn(
                "timestamp",
                (spark_fn.col("frame").cast("double") - spark_fn.col("_period_min_frame").cast("double")) / fr_col,
            )
            .drop("_period_min_frame")
        )

    if provider == "skillcorner":
        from silly_kicks.spadl.skillcorner import _PERIOD_START_SECONDS

        sc_offset = spark_fn.coalesce(
            spark_fn.create_map(*[spark_fn.lit(x) for kv in sorted(_PERIOD_START_SECONDS.items()) for x in kv])[
                spark_fn.col("period")
            ],
            spark_fn.lit(0.0),
        )
        return trk_sdf.withColumn("timestamp", spark_fn.col("timestamp").cast("double") - sc_offset)

    # idsse: no dispatch-side timestamp rebasing (the sportec converter owns its clock).
    return trk_sdf


# ── Per-period snapshot build (pure-pandas seam) ───────────────────────────


def _period_snapshots(
    provider: str,
    trk_period_pdf: pd.DataFrame,
    actions_pdf: pd.DataFrame,
    meta: MatchMeta,
    native_id: str,
) -> pd.DataFrame:
    """Convert one period's raw tracking to home-LTR AC frames, then build per-(shot, player) rows.

    Reuses the AC pipeline's ``_convert_tracking_batch`` (the single source of frame conversion +
    orientation) so freeze frames and action-context enrichment cannot drift apart. ``actions_pdf``
    is the whole match's SPADL actions (carrying the driver-stamped ``match_key``); we filter to this
    period, convert with the ORIGINAL actions (matching ``enrich_batch``'s order), then apply the AC
    pipeline's ``_resolve_enrichment_identity`` MUTATE contract so the actions' ``team_id`` lands in
    the SAME frame-compatible native id space as the converted frames (without this, ``is_teammate``
    resolves all-zero — 2026-07-07 live finding). ``home_team_id`` (also frame-compatible) drives
    ``shooter_attacks_high_x``.
    """
    import pandas as _pd

    import analytics.action_context.pipeline as _ac_pipeline
    from analytics.action_context.enrich import _resolve_enrichment_identity

    if trk_period_pdf.empty or actions_pdf.empty:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    period = int(trk_period_pdf["period"].iloc[0])
    actions_period = actions_pdf[actions_pdf["period_id"] == period].copy()
    if actions_period.empty:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    # Convert with the ORIGINAL (pre-remap) actions — the converter reads game_id + native columns,
    # never the hashed team_id — exactly as enrich_batch does (convert THEN resolve identity).
    frames = _ac_pipeline._convert_tracking_batch(provider, trk_period_pdf, actions_period, meta)
    if frames is None or len(frames) == 0:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))
    frames["game_id"] = int(actions_period["game_id"].iloc[0])

    # MUTATE contract: overwrite team_id/player_id with the frame-compatible native ids so the
    # snapshot builder's is_teammate equality holds against the converted frames.
    actions_fc = _resolve_enrichment_identity(actions_period, provider=provider, match_id_native=native_id)
    return build_tracking_snapshots_spark(actions_fc, frames, home_team_id=meta.home_team_id)


def _process_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    gold_schema: str,
    provider: str,
    native_id: str,
    task_logger: logging.Logger,
) -> tuple[int, int]:
    """Process ONE tracking match → ``bronze.shot_freeze_frames``. Returns ``(match_key, rows)``.

    Mirrors ``action_context._process_tracking_match``'s input prep (metadata resolution + clock
    rebasing) but swaps the heavy enrichment for the freeze-frame snapshot build. Hard-fail-first:
    any failure propagates with the ``provider:native_id`` in the message (ADR-002 §5).
    """
    import pandas as _pd
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from ingestion.action_context import _GRADIENTSPORTS_TRACKING_SELECT_COLS
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    try:
        # ── Read raw tracking (Spark; no unbounded toPandas — filtered per match) ──
        if provider == "idsse":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.idsse_tracking")
                .filter(spark_fn.col("match_id") == native_id)
                .select(*_IDSSE_TRACKING_SELECT_COLS)
            )
        elif provider == "metrica":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.metrica_tracking")
                .filter(spark_fn.col("match_id") == native_id)
                .select(*_METRICA_TRACKING_SELECT_COLS)
            )
        elif provider == "skillcorner":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.skillcorner_tracking")
                .filter(spark_fn.col("match_id") == native_id)
                .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
            )
            matches_meta = (
                spark.table(f"{catalog}.bronze.skillcorner_matches")
                .filter(spark_fn.col("match_id") == native_id)
                .select(
                    spark_fn.col("player_id"),
                    spark_fn.col("team_id").cast("string").alias("team_id"),
                    (spark_fn.col("position_acronym") == "GK").alias("is_goalkeeper"),
                )
            )
            trk_sdf = trk_sdf.join(spark_fn.broadcast(matches_meta), on="player_id", how="left")
        elif provider == "gradientsports":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.gradientsports_tracking")
                .filter(spark_fn.col("match_id") == native_id)
                .select(*_GRADIENTSPORTS_TRACKING_SELECT_COLS)
            )
        else:
            raise ValueError(f"Unknown tracking provider: {provider}")

        if trk_sdf.limit(1).count() == 0:
            task_logger.warning("No tracking data for %s match %s", provider, native_id)
            return (0, 0)

        # ── Read SPADL actions (bounded per match) + resolve match_key (gold dim_matches) ──
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((spark_fn.col("match_id_native") == native_id) & (spark_fn.col("data_source") == provider))
            .toPandas()
        )
        if actions_pdf.empty:
            task_logger.warning("No SPADL actions for %s match %s", provider, native_id)
            return (0, 0)
        match_key, access_tier = _resolve_match_identity(spark, catalog, gold_schema, provider, native_id)
        actions_pdf["match_key"] = match_key

        # ── Resolve per-provider match metadata + rebase the dispatch clock (shared with AC) ──
        meta = _resolve_tracking_match_meta(spark, catalog, provider, native_id, actions_pdf)
        trk_sdf = _prepare_tracking_frames_for_match(trk_sdf, provider)

        # ── Per-period conversion + snapshot build (driver-side; snapshot set is tiny) ──
        periods = [int(r["period"]) for r in trk_sdf.select("period").distinct().collect()]
        snapshot_frames: list[pd.DataFrame] = []
        for period in sorted(periods):
            trk_period_pdf = trk_sdf.filter(spark_fn.col("period") == period).toPandas()
            snaps = _period_snapshots(provider, trk_period_pdf, actions_pdf, meta, native_id)
            if len(snaps):
                snapshot_frames.append(snaps)

        if not snapshot_frames:
            task_logger.warning(
                "No shot freeze-frames produced for %s match %s (match_key=%s)", provider, native_id, match_key
            )
            return (match_key, 0)

        # Stamp the driver-owned ADR-064 ``access_tier`` per row BEFORE the reindex: the pure builder
        # output (``_SNAPSHOT_COLUMNS``) does not carry it, so the reindex to ``_SHOT_FF_COLUMNS`` would
        # KeyError without it. The raw dim_matches value is stamped verbatim (a NULL fail-safes to
        # restricted in the downstream publisher's split_restricted — never invented here).
        all_snaps = _pd.concat(snapshot_frames, ignore_index=True)
        all_snaps["access_tier"] = access_tier
        all_snaps = all_snaps[list(_SHOT_FF_COLUMNS)]
        n_rows = len(all_snaps)
        snapshots_sdf = spark.createDataFrame(all_snaps, schema=_shot_ff_struct_type())
        written = write_shot_freeze_frames(snapshots_sdf, catalog, schema, [match_key], row_count=n_rows)
        return (match_key, written)
    except Exception as exc:  # ADR-002 §5 — hard-fail-first with the match key in the message
        raise RuntimeError(f"compute_shot_freeze_frames failed for {provider}:{native_id}") from exc


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    gold_schema: str,
    units: Sequence[tuple[str, str]],
    task_logger: logging.Logger,
) -> int:
    """Process each ``(provider, native_id)`` unit, writing ``bronze.shot_freeze_frames``.

    Structured per-match JSON logging (provider, native_id, match_key, row_count, elapsed). Returns
    the total rows written across all units.
    """
    total = 0
    for provider, native_id in units:
        start = time.time()
        match_key, rows = _process_match(spark, catalog, schema, gold_schema, provider, native_id, task_logger)
        elapsed = time.time() - start
        task_logger.info(
            "shot_freeze_frames match complete",
            extra={
                "extra_fields": {
                    "provider": provider,
                    "native_id": native_id,
                    "match_key": match_key,
                    "row_count": rows,
                    "elapsed_seconds": round(elapsed, 2),
                }
            },
        )
        total += rows
    return total


def main() -> None:
    """CLI entry point — ``compute_shot_freeze_frames`` mega-job task.

    ``--providers`` selects the tracking-provider scope (default ``gradientsports,skillcorner`` —
    the INTERIM SCOPE; see the module docstring). ``--match-ids "provider:id1,id2"`` runs an explicit
    (backfill) set, rejected if its provider is outside ``--providers``; omitting it runs the
    INCREMENTAL default — only ``--providers``-scoped shot-matches not yet present in
    ``bronze.shot_freeze_frames``. A ``SystemExit`` escaping the entry point is treated as a workload
    failure by the Databricks ``python_wheel_task`` runner (even ``SystemExit(0)``), so we return
    normally on success.
    """
    from ingestion.utils import configure_logging, get_spark_session

    parser = argparse.ArgumentParser(description="Compute pre-shot tracking freeze frames to bronze")
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--gold-schema", default="dev_gold", help="Gold schema holding dim_matches (match_key source)")
    parser.add_argument(
        "--providers",
        default=_DEFAULT_PROVIDERS,
        help=(
            f"Comma-list of tracking providers to process (default {_DEFAULT_PROVIDERS!r} — the interim "
            f"GS+SkillCorner scope). idsse/metrica are a deliberate opt-in pending the distributed-sink "
            f"rewrite. Valid: {sorted(_TRACKING_PROVIDERS)}"
        ),
    )
    parser.add_argument(
        "--match-ids",
        default=None,
        help="'provider:id1,id2' explicit backfill set; empty => incremental (unprocessed matches only)",
    )
    args = parser.parse_args()

    for field_name, value in (("catalog", args.catalog), ("schema", args.schema), ("gold-schema", args.gold_schema)):
        if not IDENTIFIER_RE.match(value):
            raise SystemExit(f"Invalid {field_name} '{value}': must match {IDENTIFIER_RE.pattern}")

    selected = _parse_providers_arg(getattr(args, "providers", None))

    task_logger = configure_logging("shot_freeze_frames")
    spark = get_spark_session()

    parsed = _parse_freeze_frame_match_ids_arg(getattr(args, "match_ids", None))
    if parsed is not None:
        units = _units_from_match_ids(parsed, selected)
        task_logger.info("shot_freeze_frames: explicit backfill of %d %s match(es)", len(units), parsed[0])
    else:
        units = _discover_missing_units(spark, args.catalog, args.gold_schema, selected)
        task_logger.info(
            "shot_freeze_frames: incremental discovery found %d unprocessed match(es) in scope %s",
            len(units),
            sorted(selected),
        )

    if not units:
        task_logger.info("shot_freeze_frames: nothing to do")
        return

    total = run_pipeline(spark, args.catalog, args.schema, args.gold_schema, units, task_logger)
    task_logger.info("shot_freeze_frames complete: %d units, %d rows written", len(units), total)


if __name__ == "__main__":
    main()
