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

StatsBomb (SB-360) is a supported provider but a **deliberate opt-in** for the one-time SB-360
freeze-frame backfill (``--providers …,statsbomb`` / ``--match-ids statsbomb:…``) — it is NOT in the
daily default because StatsBomb open data is historical (not continuously ingested), so it does not
need to be rediscovered daily. The statsbomb path is distinct from tracking: it has NO tracking frames
and NO per-period conversion — it reads the shot's ``bronze.statsbomb_360`` freeze-frame directly and
builds canonical-SPADL snapshots via ``analytics.action_context.sb360_freeze_frames`` (which is already
shooter-normalized, so no orientation, and stamps ``access_tier='public'`` itself).

rev3 (2026-10-06) moved per-``(match, period)`` conversion off the driver onto Spark executors via
``mapInPandas`` frame-batching (the same seam ``_process_tracking_match`` uses), with M13 owner de-dup.
That removed the driver ``toPandas`` that had deferred IDSSE's dense ~1.5M-row periods, so IDSSE is now
a daily source. METRICA stays out of the default — its three legacy sample matches are not quality data
(owner decision 2026-10-06), not a scaling limit; name it explicitly via ``--providers`` to backfill.
LONG-TERM a full DISTRIBUTED SINK (a per-``(match, period)`` work-queue in ``ingestion.drain_adapters``
+ a ``compute_shot_freeze_frames_drain_worker`` entry point, mirroring ``compute_action_context``) would
further amortize serverless cold-start across the drain instead of paying it per match.

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

# The full freeze-frame provider set = the tracking tier PLUS statsbomb (SB-360). statsbomb is a
# freeze-frame provider (its 360 freeze-frames feed the SAME bronze.shot_freeze_frames) but takes a
# DISTINCT, non-tracking code path (no frame conversion; see ``_process_statsbomb_match``). It is a
# valid ``--providers`` / ``--match-ids`` value but is NOT in the daily default (opt-in backfill only).
_FREEZE_FRAME_PROVIDERS: frozenset[str] = _TRACKING_PROVIDERS | frozenset({"statsbomb"})

# Daily scope: GradientSports + SkillCorner + IDSSE. The executor-distributed mapInPandas refactor (rev3)
# removed the per-(match, period) driver ``toPandas`` that had deferred IDSSE (~1.5M rows/period), so IDSSE
# now onboards. METRICA is deliberately EXCLUDED (owner decision 2026-10-06): its three legacy sample
# matches are not quality data; it remains an opt-in via ``--providers`` but is not a daily source.
# statsbomb (SB-360) is a deliberate opt-in for the one-time backfill, NOT a daily source.
_DEFAULT_PROVIDERS = "gradientsports,skillcorner,idsse"

# Haloed frame-window seam (ADR-089): target CORE frames per window for the per-(period, window_id) build,
# bounding the executor peak by window size, not the ~1.5M-row dense IDSSE half. Matches tracking-marts.
_SF_WINDOW_FRAMES = 20000

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
    (a per-match idempotent bulk write). All periods of a match land
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
    a malformed value raises ``SystemExit`` (loud CLI failure, no silent default). ``statsbomb`` is a
    valid provider here (the SB-360 backfill), alongside the tracking providers.
    """
    if raw is None or raw.strip() == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2', got {raw!r}. Valid providers: {sorted(_FREEZE_FRAME_PROVIDERS)}"
        )
    provider, _, id_str = raw.partition(":")
    provider = provider.strip()
    if provider not in _FREEZE_FRAME_PROVIDERS:
        raise SystemExit(f"Unknown provider {provider!r}. Valid: {sorted(_FREEZE_FRAME_PROVIDERS)}")
    ids = [i.strip() for i in id_str.split(",") if i.strip()]
    if not ids:
        return None
    return (provider, ids)


def _parse_providers_arg(raw: str | None) -> frozenset[str]:
    """Parse/validate the ``--providers`` comma-list against the known freeze-frame-provider set.

    Empty / ``None`` → the daily default (``gradientsports,skillcorner,idsse``). Unknown providers raise
    a loud ``SystemExit``. ``metrica`` is a deliberate opt-in (excluded from the default on data quality,
    not conversion-safety — see the module docstring); ``statsbomb`` is a deliberate opt-in for the
    one-time SB-360 backfill (never in the daily default).
    """
    if raw is None or raw.strip() == "":
        raw = _DEFAULT_PROVIDERS
    selected = {p.strip() for p in raw.split(",") if p.strip()}
    if not selected:
        raise SystemExit(f"--providers must be a non-empty comma-list. Valid: {sorted(_FREEZE_FRAME_PROVIDERS)}")
    unknown = selected - _FREEZE_FRAME_PROVIDERS
    if unknown:
        raise SystemExit(
            f"Unknown provider(s) in --providers: {sorted(unknown)}. Valid: {sorted(_FREEZE_FRAME_PROVIDERS)}"
        )
    return frozenset(selected)


def _units_from_match_ids(parsed: tuple[str, list[str]], selected: frozenset[str]) -> list[tuple[str, str]]:
    """Build ``(provider, native_id)`` units from a parsed ``--match-ids``, enforcing ``--providers`` scope.

    Rejects (loud ``SystemExit``) a backfill whose provider is outside the selected set — so a
    ``--match-ids metrica:…`` cannot silently bypass the daily GS+SkillCorner+IDSSE scope.
    """
    provider, ids = parsed
    if provider not in selected:
        raise SystemExit(
            f"--match-ids provider {provider!r} is outside the selected --providers set "
            f"{sorted(selected)}. Add it to --providers to process it (metrica/statsbomb are "
            f"deliberate opt-ins; see the module docstring)."
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


def _missing_statsbomb_units_sql(catalog: str, gold_schema: str) -> str:
    """StatsBomb-specific discovery SQL: shot-matches WITH 360 data, NOT yet in ``shot_freeze_frames``.

    Distinct from :func:`_missing_units_sql` (the generic tracking discovery) in ONE load-bearing way:
    it restricts to statsbomb matches that have rows in ``bronze.statsbomb_360``. There are ~74k
    non-360 statsbomb shots — a non-360 match would resolve a ``match_key`` and pass the shot filter
    but produce ZERO freeze frames (no 360 data), wasting a work unit and never clearing itself from
    the anti-set (it would be rediscovered every run). The ``statsbomb_360`` existence subquery gates
    that out. Otherwise identical anti-set structure: ``spadl_actions`` shots ∩ ``dim_matches``
    (``match_key`` resolution) MINUS the ``match_key`` set already in ``shot_freeze_frames``.
    """
    from analytics.action_context.tracking_snapshots import _shot_type_id

    shot_type_id = _shot_type_id()
    return (
        f"SELECT dm.{_DIM_MATCHES_PROVIDER_COL} AS provider, "  # noqa: S608 — identifiers validated by IDENTIFIER_RE; literals only
        f"CAST(sa.{_SPADL_NATIVE_ID_COL} AS STRING) AS native_id, dm.{_DIM_MATCHES_KEY_COL} AS match_key "
        f"FROM {catalog}.bronze.spadl_actions sa "
        f"JOIN {catalog}.{gold_schema}.dim_matches dm "
        f"  ON sa.{_SPADL_DATA_SOURCE_COL} = dm.{_DIM_MATCHES_PROVIDER_COL} "
        f"  AND CAST(sa.{_SPADL_NATIVE_ID_COL} AS STRING) = CAST(dm.{_DIM_MATCHES_NATIVE_ID_COL} AS STRING) "
        f"WHERE sa.{_SPADL_DATA_SOURCE_COL} = 'statsbomb' "
        f"  AND sa.{_SPADL_TYPE_ID_COL} = {shot_type_id} "
        f"  AND CAST(sa.{_SPADL_NATIVE_ID_COL} AS STRING) IN "
        f"    (SELECT DISTINCT CAST(match_id AS STRING) FROM {catalog}.bronze.statsbomb_360) "
        f"  AND dm.{_DIM_MATCHES_KEY_COL} NOT IN (SELECT match_key FROM {catalog}.bronze.{_TABLE_NAME}) "
        f"GROUP BY dm.{_DIM_MATCHES_PROVIDER_COL}, sa.{_SPADL_NATIVE_ID_COL}, dm.{_DIM_MATCHES_KEY_COL}"
    )


def _discover_missing_units(
    spark: SparkSession, catalog: str, gold_schema: str, providers: frozenset[str]
) -> list[tuple[str, str]]:
    """Return ``[(provider, native_id), ...]`` for ``providers``-scoped shot-matches not yet freeze-framed.

    Discovery is CONSTRAINED to the ``--providers``-selected set (default GS+SkillCorner+IDSSE) — a
    metrica/statsbomb match is never returned unless explicitly enabled (deliberate opt-ins).
    Tracking providers use the generic anti-set SQL; ``statsbomb`` uses its own
    :func:`_missing_statsbomb_units_sql` (which additionally requires ``bronze.statsbomb_360`` data),
    and the two result sets are unioned. Uses ``tolerate_missing_table`` so the first run (before the
    table is populated / when the migration has just created an empty table) does not spuriously fail;
    a genuinely absent ``shot_freeze_frames`` means "nothing processed yet" → discover everything in scope.
    """
    from ingestion.utils import tolerate_missing_table

    tracking_scope = providers & _TRACKING_PROVIDERS
    with tolerate_missing_table(logger, "shot_freeze_frames not found — treating all matches as unprocessed"):
        units: list[tuple[str, str]] = []
        if tracking_scope:
            tracking_rows = spark.sql(_missing_units_sql(catalog, gold_schema, tracking_scope)).collect()
            units.extend((str(r["provider"]), str(r["native_id"])) for r in tracking_rows)
        if "statsbomb" in providers:
            sb_rows = spark.sql(_missing_statsbomb_units_sql(catalog, gold_schema)).collect()
            units.extend((str(r["provider"]), str(r["native_id"])) for r in sb_rows)
        return units
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
            shape_events_to_native,
        )

        # ET deriver via the lakehouse defensive wrapper (preserves the "no period column -> None"
        # contract that silly-kicks 4.87.0 changed to a RuntimeError). See spadl_adapter.
        from ingestion.spadl_adapter import derive_idsse_home_team_start_left_extratime

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
    from analytics.action_context.frame_windows import _HALO_COL, build_windowed

    if trk_period_pdf.empty or actions_pdf.empty:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    period = int(trk_period_pdf["period"].iloc[0])
    actions_period = actions_pdf[actions_pdf["period_id"] == period].copy()
    if actions_period.empty:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    def _build(raw: pd.DataFrame) -> pd.DataFrame:
        # Convert with the ORIGINAL (pre-remap) actions — the converter reads game_id + native columns,
        # never the hashed team_id — exactly as enrich_batch does (convert THEN resolve identity).
        f = _ac_pipeline._convert_tracking_batch(provider, raw, actions_period, meta)
        if f is None or len(f) == 0:
            return f if f is not None else _pd.DataFrame()
        f = f.copy()
        f["game_id"] = int(actions_period["game_id"].iloc[0])
        return f

    # Haloed frame-window build (ADR-089): the input is ONE (period, window_id) group (core+halo); build on
    # core+halo for velocity continuity, trim the halo. Each shot's frame lands in exactly ONE core window,
    # so passing the whole period's actions snapshots only the core-frame shots here (single-owner). A call
    # WITHOUT the halo helper (legacy/whole-period) falls back to the plain convert.
    if _HALO_COL in trk_period_pdf.columns:
        frames = build_windowed(trk_period_pdf, _build, frame_col=frame_col)
    else:
        frames = _build(trk_period_pdf)
    if frames is None or len(frames) == 0:
        return _pd.DataFrame(columns=list(_SHOT_FF_COLUMNS))

    # MUTATE contract: overwrite team_id/player_id with the frame-compatible native ids so the
    # snapshot builder's is_teammate equality holds against the converted frames.
    actions_fc = _resolve_enrichment_identity(actions_period, provider=provider, match_id_native=native_id)
    return build_tracking_snapshots_spark(actions_fc, frames, home_team_id=meta.home_team_id)


# StatsBomb metadata gap: a few matches have a NULL shot_fidelity_version (and NULL xy_fidelity_version).
# fidelity_version ONLY sets the cell-center offset in silly_kicks._convert_locations (~0.4 m, sub-meter —
# immaterial to xG geometry + the sum-encoder, inside the co-location golden's +/-2 m band). silly_kicks
# itself treats ANY non-2 value as the low-fidelity 1.0-cell default, so NULL maps to 1 — the same value the
# shot ACTION conversion applied for these matches, keeping frame and action consistent. We default (loud
# WARNING, never silent) instead of hard-failing the whole backfill for one match's missing metadata.
_DEFAULT_SHOT_FIDELITY_VERSION = 1


def _resolve_shot_fidelity_version(raw_value: str | int | None, native_id: str, task_logger: logging.Logger) -> int:
    """Resolve a match's StatsBomb ``shot_fidelity_version`` (stored STRING) to an int, NULL -> 1 (loud).

    ``None`` (NULL in bronze, or the match absent from ``statsbomb_matches``) is a known StatsBomb
    metadata gap; it maps to ``_DEFAULT_SHOT_FIDELITY_VERSION`` (1) — silly_kicks' own non-2 default — so
    the freeze frame stays consistent with the shot action's conversion. Logged at WARNING with the match
    id (visible degradation, not silent). Any other value is cast to int (silly_kicks compares ``== 2``).
    """
    if raw_value is None:
        task_logger.warning(
            "NULL shot_fidelity_version for statsbomb match %s — defaulting to %d "
            "(sub-meter cell-offset impact; silly_kicks' own non-2 default, consistent with the action)",
            native_id,
            _DEFAULT_SHOT_FIDELITY_VERSION,
        )
        return _DEFAULT_SHOT_FIDELITY_VERSION
    return int(raw_value)


def _process_statsbomb_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    gold_schema: str,
    native_id: str,
    task_logger: logging.Logger,
) -> tuple[int, int]:
    """Process ONE StatsBomb-360 match → ``bronze.shot_freeze_frames``. Returns ``(match_key, rows)``.

    The statsbomb path is distinct from the tracking path: there are NO tracking frames and NO
    per-period conversion. It reads the shot's ``bronze.statsbomb_360`` freeze-frame directly and
    builds canonical-SPADL snapshots via ``analytics.action_context.sb360_freeze_frames`` (which is
    already shooter-normalized — no orientation — and stamps ``access_tier='public'`` itself, so the
    tracking path's per-row ``access_tier`` stamp is NOT applied here). Reads are bounded per match:
    ~22 freeze-frame rows/shot and the match's shot actions only. Hard-fail-first (ADR-002 §5).

    ``build_sb360_snapshots`` uses ``actions_df.team_id`` only to label acting vs opponent players
    (the 360 ``teammate`` flag is authoritative) — so the raw ``bronze.spadl_actions.team_id`` (native
    StatsBomb integer id; StatsBomb ids are never hashed) is internally consistent and used as-is, with
    NO ``_resolve_enrichment_identity`` remap (mirrors the AC ``_enrich_sb360_match`` path exactly).
    """
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from analytics.action_context.sb360_freeze_frames import build_sb360_freeze_frames
    from analytics.action_context.tracking_snapshots import _shot_type_id

    try:
        # ── Read this match's 360 freeze frames (bounded: ~22 rows/shot times shots) ──
        sb360_pdf = (
            spark.table(f"{catalog}.bronze.statsbomb_360")
            .filter(spark_fn.col("match_id").cast("string") == str(native_id))
            .select("id", "actor", "teammate", "keeper", "location")
            .toPandas()
        )
        if sb360_pdf.empty:
            task_logger.warning("No statsbomb_360 freeze frames for statsbomb match %s (non-360 match)", native_id)
            return (0, 0)

        # ── Read this match's shot actions (bounded) + resolve match_key (gold dim_matches) ──
        shot_type_id = _shot_type_id()
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter(
                (spark_fn.col("data_source") == "statsbomb")
                & (spark_fn.col("match_id_native").cast("string") == str(native_id))
                & (spark_fn.col("type_id") == shot_type_id)
            )
            .select("original_event_id", "action_id", "team_id")
            .toPandas()
        )
        if actions_pdf.empty:
            task_logger.warning("No statsbomb shot actions for match %s", native_id)
            return (0, 0)
        match_key, _access_tier = _resolve_match_identity(spark, catalog, gold_schema, "statsbomb", native_id)
        actions_pdf["match_key"] = match_key

        # ── shot_fidelity_version (drives the 120x80 -> 105x68 cell offset; NULL -> default, see helper) ──
        fidelity_rows = (
            spark.table(f"{catalog}.bronze.statsbomb_matches")
            .filter(spark_fn.col("match_id").cast("string") == str(native_id))
            .select("shot_fidelity_version")
            .limit(1)
            .collect()
        )
        raw_fidelity = fidelity_rows[0]["shot_fidelity_version"] if fidelity_rows else None
        shot_fidelity_version = _resolve_shot_fidelity_version(raw_fidelity, native_id, task_logger)

        out_df = build_sb360_freeze_frames(actions_pdf, sb360_pdf, shot_fidelity_version)
        if out_df.empty:
            task_logger.warning(
                "No SB-360 freeze frames produced for statsbomb match %s (match_key=%s)", native_id, match_key
            )
            return (match_key, 0)

        # The SB-360 builder already stamps access_tier='public' (public data; no per-row driver stamp).
        if "access_tier" not in out_df.columns:
            raise RuntimeError("build_sb360_freeze_frames output is missing the access_tier column")
        n_rows = len(out_df)
        snapshots_sdf = spark.createDataFrame(out_df, schema=_shot_ff_struct_type())
        written = write_shot_freeze_frames(snapshots_sdf, catalog, schema, [match_key], row_count=n_rows)
        return (match_key, written)
    except Exception as exc:  # ADR-002 §5 — hard-fail-first with the match key in the message
        raise RuntimeError(f"compute_shot_freeze_frames failed for statsbomb:{native_id}") from exc


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

    ``statsbomb`` takes a DISTINCT path (:func:`_process_statsbomb_match`) — no tracking frames, no
    per-period conversion — dispatched here BEFORE any tracking read.
    """
    if provider == "statsbomb":
        return _process_statsbomb_match(spark, catalog, schema, gold_schema, native_id, task_logger)

    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from ingestion.action_context import (
        _GRADIENTSPORTS_TRACKING_SELECT_COLS,
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

        # ── Per-period snapshot build, EXECUTOR-DISTRIBUTED (mapInPandas; ADR-045) ──
        # Replaces the per-period ``toPandas()`` loop — the whole-half driver pull that deferred idsse/metrica
        # (~1.5M rows/period). One ``period`` group == one executor task; frames never hit the driver. Unlike
        # the six-mart tracking-marts drain this is SINGLE-output, so the snapshot rows are returned directly
        # as the result schema — no spill. The driver-owned ADR-064 ``access_tier`` is stamped in the UDF
        # (the pure builder output does not carry it; a NULL fail-safes to restricted downstream).
        from ingestion.action_context import _UDF_SHUFFLE_PARTITIONS, _make_streaming_group_mapper

        frame_col = "frame_num" if provider == "gradientsports" else "frame"
        _ff_cols = list(_SHOT_FF_COLUMNS)

        def _snapshot_udf(period_pdf: pd.DataFrame) -> pd.DataFrame:
            import pandas as _p

            snaps = _period_snapshots(provider, period_pdf, actions_pdf, meta, native_id)
            if not len(snaps):
                return _p.DataFrame(columns=_ff_cols)
            snaps = snaps.copy()
            snaps["access_tier"] = access_tier
            return snaps[_ff_cols]

        # Haloed frame-window seam (ADR-089): bound the per-task build by window size (not the ~1.5M-row
        # dense IDSSE half). assign_frame_windows adds _window_id + _is_halo; the group is (period,
        # window_id); _snapshot_udf (via _period_snapshots -> build_windowed) builds core+halo + trims +
        # snapshots the core-frame shots. All shot_freeze tracking providers (idsse/skillcorner/GS) are
        # seam targets (metrica is not a shot_freeze provider).
        from analytics.action_context.frame_windows import _WINDOW_COL
        from ingestion.frame_window_dispatch import assign_frame_windows

        windowed_sdf = assign_frame_windows(trk_sdf, provider, target_window_frames=_SF_WINDOW_FRAMES)
        keys = ["period", _WINDOW_COL]
        sort_cols = [*keys, frame_col] if frame_col in trk_sdf.columns else keys
        result_sdf = (
            windowed_sdf.repartition(_UDF_SHUFFLE_PARTITIONS, *keys)
            .sortWithinPartitions(*sort_cols)
            .mapInPandas(_make_streaming_group_mapper(_snapshot_udf, keys), schema=_shot_ff_struct_type())
        )
        written = write_shot_freeze_frames(result_sdf, catalog, schema, [match_key], row_count=None)
        if written == 0:
            task_logger.warning(
                "No shot freeze-frames produced for %s match %s (match_key=%s)", provider, native_id, match_key
            )
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

    ``--providers`` selects the freeze-frame provider scope (default ``gradientsports,skillcorner`` —
    the INTERIM SCOPE; see the module docstring). ``statsbomb`` (SB-360) is a deliberate opt-in for the
    one-time backfill. ``--match-ids "provider:id1,id2"`` runs an explicit (backfill) set, rejected if
    its provider is outside ``--providers``; omitting it runs the
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
            f"Comma-list of freeze-frame providers to process (default {_DEFAULT_PROVIDERS!r} — the interim "
            f"GS+SkillCorner scope). idsse/metrica are a deliberate opt-in pending the distributed-sink "
            f"rewrite; statsbomb (SB-360) is a deliberate opt-in for the one-time backfill (not a daily "
            f"source). Valid: {sorted(_FREEZE_FRAME_PROVIDERS)}"
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
