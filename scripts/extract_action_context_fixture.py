"""Extract an action-context fixture (frames + actions + xT + meta + legacy oracles) from Databricks.

Repeatable, provider-parameterized. Writes committed Parquet fixtures consumed by the local
hexagon adapters (`analytics.action_context.local.parquet_sources`) so `run_work_unit` can run a
real game locally with zero Databricks dependency, and by the differential harness (Phase C) so
the local result can be checked against the retiring legacy pipelines.

Read-only. Uses the Databricks SDK Statement Execution API (auto-resolves auth, auto-starts the
warehouse — see project memory `reference_sdk_over_sql_connector.md`). Never triggers a job.

Fixture layout written::

    src/tests/fixtures/action_context/
        xt_grid.parquet                       # global xT grid (shared)
        <provider>/<match>[_p<period>]/
            frames.parquet                    # bronze tracking (tracking providers only)
            actions.parquet                   # bronze.spadl_actions for the match
            meta.parquet                      # home_team_id, home_start_left [, gs_*_json]
            oracle_fct_tracking_context.parquet   # tracking providers (via match_key)
            oracle_fct_pausa_values.parquet       # IDSSE only (idsse_-prefixed match_id)
            oracle_elastic_sync_results.parquet   # IDSSE only (deduped authoritative set)

Env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID.

Usage::

    uv run python scripts/extract_action_context_fixture.py --provider wyscout --match-id <id>
    uv run python scripts/extract_action_context_fixture.py --provider idsse --match-id J03WMX \
        --period 1 --num-batches 30

Note: IDSSE ``frame`` is not 0-based (period 1 starts at 10000, period 2 at 100000). Prefer
``--num-batches`` over absolute ``--frame-start/--frame-end`` to avoid the per-provider offset.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd

# Frame batch size — resolved per provider via the SAME module prod + local resolve
# through (L11 batch-alignment; ADR-047 amendment 2). analytics.action_context.batching
# is stdlib-only, so importing it here does NOT pull pyspark.
from analytics.action_context.batching import resolve_frame_batch_size

logger = logging.getLogger(__name__)

FIXTURE_ROOT = Path("src/tests/fixtures/action_context")

# Sub-chunk size (frames) for tracking pulls, to stay under the INLINE statement result cap.
_SUBCHUNK_FRAMES = 1000

_TRACKING_PROVIDERS = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
_EVENT_ONLY_PROVIDERS = frozenset({"statsbomb", "wyscout"})
_ALL_PROVIDERS = _TRACKING_PROVIDERS | _EVENT_ONLY_PROVIDERS

# Per-provider bronze tracking projections.
# Source of truth: ingestion.tracking_context._{IDSSE,METRICA,SKILLCORNER}_TRACKING_SELECT_COLS
# and ingestion.action_context._GRADIENTSPORTS_TRACKING_SELECT_COLS. Hardcoded here because those
# modules import pyspark (unavailable in the local extract environment). Keep in sync.
_TRACKING_SELECT_COLS: dict[str, tuple[str, ...]] = {
    "idsse": (
        "match_id", "period", "frame", "timestamp", "x", "y", "s", "ball_status",
        "frame_rate", "player_id", "team_id", "is_goalkeeper", "ball_x", "ball_y", "ball_z", "ball_s",
    ),
    "metrica": (
        "match_id", "period", "frame", "timestamp", "frame_rate",
        "gk_jersey_numbers", "home_players", "away_players", "ball_x", "ball_y",
    ),
    "skillcorner": (
        "match_id", "frame", "period", "timestamp", "player_id", "x", "y", "frame_rate", "ball_x", "ball_y",
    ),
    "gradientsports": (
        "match_id", "period", "frame_num", "period_elapsed_time", "team_side",
        "is_ball", "jersey_num", "x", "y", "z",
    ),
}  # fmt: skip

# Frame-number column per provider (gradientsports uses frame_num).
_FRAME_COL: dict[str, str] = {p: ("frame_num" if p == "gradientsports" else "frame") for p in _TRACKING_PROVIDERS}


# ── SDK statement execution ────────────────────────────────────────────


def _execute_query_to_df(sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute read-only SQL via the Databricks SDK; return a typed DataFrame."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import Disposition, Format

    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql.strip(),
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    while result.status and result.status.state and result.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(3)
        assert result.statement_id is not None  # noqa: S101
        result = w.statement_execution.get_statement(result.statement_id)

    if not (result.status and result.status.state and result.status.state.value == "SUCCEEDED"):
        raise RuntimeError(f"Query failed: {result.status}")

    assert result.manifest is not None and result.manifest.schema is not None  # noqa: S101
    columns = [col.name for col in (result.manifest.schema.columns or [])]
    rows = result.result.data_array if result.result and result.result.data_array else []
    df = pd.DataFrame(rows, columns=columns)

    for col_meta in result.manifest.schema.columns or []:
        name = col_meta.name
        type_name = col_meta.type_text or ""
        if type_name in ("BIGINT", "INT", "LONG", "SMALLINT", "TINYINT"):
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif type_name in ("DOUBLE", "FLOAT", "DECIMAL"):
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif type_name == "BOOLEAN":
            df[name] = df[name].map({"true": True, "false": False, None: None}).astype("boolean")
        elif type_name == "TIMESTAMP":
            df[name] = pd.to_datetime(df[name], errors="coerce")
    return df


def _q(value: str) -> str:
    """Single-quote-escape a SQL string literal."""
    return value.replace("'", "''")


# ── Fixture-dir helpers ────────────────────────────────────────────────


def _unit_dir(provider: str, match_id: str, period: int | None) -> Path:
    name = match_id if period is None else f"{match_id}_p{period}"
    return FIXTURE_ROOT / provider / name


# ── Pulls ──────────────────────────────────────────────────────────────


def _pull_tracking(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    frame_start: int | None,
    frame_end: int | None,
    catalog: str,
    bronze: str,
    warehouse_id: str,
) -> pd.DataFrame:
    cols = ", ".join(_TRACKING_SELECT_COLS[provider])
    frame_col = _FRAME_COL[provider]
    table = f"{catalog}.{bronze}.{provider}_tracking"
    base = f"SELECT {cols} FROM {table} WHERE match_id = '{_q(match_id)}'"  # noqa: S608
    if period is not None:
        base += f" AND period = {int(period)}"

    if frame_start is None or frame_end is None:
        sql = f"{base} ORDER BY {frame_col}"
        logger.info("Pulling all tracking frames for %s/%s", provider, match_id)
        return _execute_query_to_df(sql, warehouse_id)

    parts: list[pd.DataFrame] = []
    for sub_start in range(frame_start, frame_end, _SUBCHUNK_FRAMES):
        sub_end = min(sub_start + _SUBCHUNK_FRAMES, frame_end)
        sql = f"{base} AND {frame_col} >= {sub_start} AND {frame_col} < {sub_end} ORDER BY {frame_col}"
        logger.info("  tracking sub-chunk frames [%d, %d)", sub_start, sub_end)
        parts.append(_execute_query_to_df(sql, warehouse_id))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _resolve_num_batches_range(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    num_batches: int,
    catalog: str,
    bronze: str,
    warehouse_id: str,
    frame_batch_size: int,
) -> tuple[int, int]:
    """Resolve a batch-aligned [start, end) frame window of ``num_batches`` ``frame_batch_size``-frame batches.

    Probes ``min(frame)`` for the (match, period) so the caller does not need to know the
    provider's frame-numbering offset (IDSSE period frames start at 10000 / 100000, not 0).
    """
    frame_col = _FRAME_COL[provider]
    table = f"{catalog}.{bronze}.{provider}_tracking"
    sql = f"SELECT min({frame_col}) AS mn FROM {table} WHERE match_id = '{_q(match_id)}'"  # noqa: S608
    if period is not None:
        sql += f" AND period = {int(period)}"
    mn_df = _execute_query_to_df(sql, warehouse_id)
    if mn_df.empty or pd.isna(mn_df["mn"].iloc[0]):
        raise SystemExit(f"No tracking rows to resolve --num-batches for {provider}/{match_id}")
    raw_min = int(mn_df["mn"].iloc[0])
    start = (raw_min // frame_batch_size) * frame_batch_size
    end = start + num_batches * frame_batch_size
    logger.info("Resolved --num-batches %d -> frames [%d, %d) (min frame %d)", num_batches, start, end, raw_min)
    return start, end


def _pull_actions(*, provider: str, match_id: str, catalog: str, bronze: str, warehouse_id: str) -> pd.DataFrame:
    sql = (
        f"SELECT * FROM {catalog}.{bronze}.spadl_actions "  # noqa: S608
        f"WHERE match_id_native = '{_q(match_id)}' AND data_source = '{_q(provider)}' "
        f"ORDER BY period_id, action_id"
    )
    return _execute_query_to_df(sql, warehouse_id)


def _attach_skillcorner_roster(
    frames: pd.DataFrame, *, match_id: str, catalog: str, bronze: str, warehouse_id: str
) -> pd.DataFrame:
    """Attach ``team`` + ``is_goalkeeper`` to SkillCorner tracking rows.

    Mirrors the production driver EXACTLY (ingestion.action_context._process_tracking_match's
    broadcast join with bronze.skillcorner_matches): the tracking JSONL has no team/GK fields,
    so the fixture must carry the post-join shape or the local converter KeyErrors on
    ['team', 'is_goalkeeper'] (bit the first 1886347_p2 extract, 2026-06-11).
    """
    roster = _execute_query_to_df(
        f"SELECT player_id, CAST(team_id AS STRING) AS team, position_acronym = 'GK' AS is_goalkeeper "  # noqa: S608
        f"FROM {catalog}.{bronze}.skillcorner_matches WHERE match_id = '{_q(match_id)}'",
        warehouse_id,
    )
    return frames.merge(roster, on="player_id", how="left")


def _pull_sb360_snapshots(
    *, match_id: str, actions_pdf: pd.DataFrame, catalog: str, bronze: str, warehouse_id: str
) -> pd.DataFrame:
    """Build the SB360 snapshot frame (action_id, team_id, is_goalkeeper, x, y) from
    bronze.statsbomb_360 — mirrors ingestion.action_context._run_sb360_enrichment exactly so the
    committed fixture matches production. Empty DataFrame if the match has no 360 data."""
    sb = _execute_query_to_df(
        f"SELECT id, teammate, keeper, location FROM {catalog}.{bronze}.statsbomb_360 "  # noqa: S608
        f"WHERE match_id = {int(match_id)}",
        warehouse_id,
    )
    if sb.empty:
        return pd.DataFrame(columns=["action_id", "team_id", "is_goalkeeper", "x", "y"])
    _ev = actions_pdf["original_event_id"].dropna()
    ev2act = dict(zip(_ev, actions_pdf.loc[_ev.index, "action_id"], strict=True))
    act2team = dict(zip(actions_pdf["action_id"], actions_pdf["team_id"].astype(str), strict=False))
    all_teams = [str(t) for t in actions_pdf["team_id"].dropna().unique()]
    rows: list[dict[str, Any]] = []
    for _, row in sb.iterrows():
        aid = ev2act.get(str(row.get("id", "")))
        if aid is None:
            continue
        at = act2team.get(aid)
        if at is None:
            continue
        opp = [t for t in all_teams if t != at]
        team_id = at if bool(row.get("teammate", False)) else (opp[0] if opp else at)
        loc = row.get("location")
        if isinstance(loc, str):
            try:
                loc = json.loads(loc)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(loc, (list, tuple)) or len(loc) < 2:
            continue
        rows.append(
            {
                "action_id": int(aid),
                "team_id": team_id,
                "is_goalkeeper": bool(row.get("keeper", False)),
                "x": float(loc[0]),
                "y": float(loc[1]),
            }
        )
    return pd.DataFrame(rows)


def _pull_xt_grid(*, catalog: str, bronze: str, warehouse_id: str) -> pd.DataFrame:
    sql = (
        f"SELECT zone_x, zone_y, xt_value FROM {catalog}.{bronze}.expected_threat_grids "  # noqa: S608
        f"WHERE competition_id = 'global'"
    )
    return _execute_query_to_df(sql, warehouse_id)


# ── Meta resolution (mirrors ingestion.action_context._process_tracking_match) ──


def _resolve_meta(
    *,
    provider: str,
    match_id: str,
    actions_pdf: pd.DataFrame,
    catalog: str,
    bronze: str,
    warehouse_id: str,
) -> dict[str, object]:
    """Resolve home_team_id + home_start_left (+ gs_* maps) exactly as production does."""
    meta: dict[str, object] = {"home_team_id": "unknown", "home_start_left": True}

    if provider == "idsse":
        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks, derive_idsse_home_team_start_left

        events = _execute_query_to_df(
            f"SELECT * FROM {catalog}.{bronze}.idsse_events WHERE match_id = '{_q(match_id)}'",  # noqa: S608
            warehouse_id,
        )
        home_team_id = str(events["home_team_id_native"].dropna().iloc[0])
        adapted = adapt_idsse_events_for_silly_kicks(events)
        meta["home_team_id"] = home_team_id
        meta["home_start_left"] = bool(derive_idsse_home_team_start_left(adapted, home_team_id))

    elif provider == "metrica":
        meta["home_team_id"] = "Home"  # production default; home_start_left stays True

    elif provider == "skillcorner":
        row = _execute_query_to_df(
            f"SELECT home_team_id FROM {catalog}.{bronze}.skillcorner_matches "  # noqa: S608
            f"WHERE match_id = '{_q(match_id)}' LIMIT 1",
            warehouse_id,
        )
        meta["home_team_id"] = str(row["home_team_id"].iloc[0])

    elif provider == "gradientsports":
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        # Only the 4 columns extract_gradientsports_match_metadata reads — NOT SELECT *.
        # The live table has a scan-planning defect on `possessionEvents.carrySuccessful`
        # (any query touching it fails, including SELECT *; observed 2026-06-10, suspected
        # PR #355 column-mapping migration fallout), and metadata needs none of the 260
        # event columns anyway.
        events = _execute_query_to_df(
            f"SELECT `gameEvents.homeTeam`, `gameEvents.teamId`, "  # noqa: S608
            f"`stadiumMetadata.homeTeamStartLeft`, `stadiumMetadata.homeTeamStartLeftExtraTime` "
            f"FROM {catalog}.{bronze}.gradientsports_events WHERE match_id = '{_q(match_id)}'",
            warehouse_id,
        )
        gs_meta = extract_gradientsports_match_metadata(events)
        home_tid = str(gs_meta["home_team_id"])
        meta["home_team_id"] = home_tid
        meta["home_start_left"] = bool(gs_meta["home_team_start_left"])
        # ET direction flag (ADR-029): silly-kicks raises on period 3/4 conversion without it,
        # so an extra-time fixture (e.g. 10517 p3) is unusable if this is dropped. None is safe
        # for no-ET matches; ParquetMatchMetadataSource tolerates the missing/NaN column.
        meta["home_team_start_left_extratime"] = gs_meta["home_team_start_left_extratime"]

        roster = _execute_query_to_df(
            f"SELECT * FROM {catalog}.{bronze}.gradientsports_roster WHERE match_id = '{_q(match_id)}'",  # noqa: S608
            warehouse_id,
        )
        if not roster.empty:
            import json

            # Reuse production's roster-dict builder — the bronze roster uses pd.json_normalize
            # dot-notation columns (`team.id`, `shirtNumber`, `player.id`, `positionGroupType`),
            # and a snake_case reimplementation here KeyErrors (its docstring calls out exactly
            # this bug class; bitten 2026-06-10 when this branch first ran against live data).
            from ingestion.action_context import _build_gradientsports_roster_dicts

            team_side_to_id, j2p_tuples, gk_player_ids = _build_gradientsports_roster_dicts(roster, home_tid)
            meta["gs_team_side_to_id_json"] = json.dumps(team_side_to_id)
            meta["gs_jersey_to_player_id_json"] = json.dumps(
                {f"{side}\t{jersey}": pid for (side, jersey), pid in j2p_tuples.items()}
            )
            meta["gs_gk_player_ids_json"] = json.dumps(gk_player_ids)

    else:  # event-only: enrich_event_only ignores meta; home_team_id = first acting team
        teams = actions_pdf["team_id"].dropna().unique() if "team_id" in actions_pdf.columns else []
        meta["home_team_id"] = str(teams[0]) if len(teams) > 0 else "unknown"

    return meta


# ── Oracle pulls (Phase C differential) ────────────────────────────────


def _pull_oracles(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    out_dir: Path,
    catalog: str,
    gold: str,
    bronze: str,
    warehouse_id: str,
) -> None:
    # tracking_context oracle (tracking providers, NOT gradientsports — 0 rows there)
    if provider in {"idsse", "skillcorner", "metrica"}:
        key_df = _execute_query_to_df(
            f"SELECT match_key FROM {catalog}.{gold}.dim_matches "  # noqa: S608
            f"WHERE provider = '{_q(provider)}' AND native_match_id = '{_q(match_id)}' LIMIT 1",
            warehouse_id,
        )
        if key_df.empty:
            logger.warning(
                "No dim_matches row for provider=%s native_match_id=%s — skipping tracking oracle", provider, match_id
            )
        else:
            match_key = key_df["match_key"].iloc[0]
            sql = f"SELECT * FROM {catalog}.{gold}.fct_tracking_context WHERE match_key = {int(match_key)}"  # noqa: S608
            if period is not None:
                sql += f" AND period_id = {int(period)}"
            df = _execute_query_to_df(sql, warehouse_id)
            df.to_parquet(out_dir / "oracle_fct_tracking_context.parquet", index=False)
            logger.info("  oracle fct_tracking_context: %d rows (match_key=%s)", len(df), match_key)

    if provider != "idsse":
        return  # OBSO/PAUSA/elastic oracles are IDSSE-only

    # PAUSA + OBSO oracle (prefixed native id)
    prefixed = f"idsse_{match_id}"
    pausa = _execute_query_to_df(
        f"SELECT * FROM {catalog}.{gold}.fct_pausa_values WHERE match_id = '{_q(prefixed)}'",  # noqa: S608
        warehouse_id,
    )
    pausa.to_parquet(out_dir / "oracle_fct_pausa_values.parquet", index=False)
    logger.info("  oracle fct_pausa_values: %d rows (match_id=%s)", len(pausa), prefixed)

    # elastic_sync_results — stored under BOTH J03WMX and idsse_J03WMX; keep authoritative (max _ingested_at)
    elastic = _execute_query_to_df(
        f"SELECT * FROM {catalog}.{bronze}.elastic_sync_results "  # noqa: S608
        f"WHERE match_id IN ('{_q(match_id)}', '{_q(prefixed)}')",
        warehouse_id,
    )
    if not elastic.empty:
        elastic = elastic.copy()
        elastic["_match_norm"] = elastic["match_id"].astype(str).str.replace("^idsse_", "", regex=True)
        key_cols = ["_match_norm"] + (["event_id"] if "event_id" in elastic.columns else [])
        if "_ingested_at" in elastic.columns:
            elastic = (
                elastic.sort_values("_ingested_at")
                .drop_duplicates(subset=key_cols, keep="last")
                .drop(columns="_match_norm")
            )
        else:
            elastic = elastic.drop(columns="_match_norm")
    elastic.to_parquet(out_dir / "oracle_elastic_sync_results.parquet", index=False)
    logger.info("  oracle elastic_sync_results: %d rows (deduped)", len(elastic))


# ── CLI ────────────────────────────────────────────────────────────────


def _validate_frame_range(
    frame_start: int | None, frame_end: int | None, frame_batch_size: int
) -> tuple[int | None, int | None]:
    """Enforce batch alignment (L11): start + span both multiples of the provider's batch size."""
    if frame_start is None and frame_end is None:
        return None, None
    if frame_start is None or frame_end is None:
        raise SystemExit("--frame-start and --frame-end must be given together")
    if frame_start % frame_batch_size != 0 or (frame_end - frame_start) % frame_batch_size != 0:
        rounded_start = (frame_start // frame_batch_size) * frame_batch_size
        span = frame_end - rounded_start
        rounded_end = rounded_start + ((span + frame_batch_size - 1) // frame_batch_size) * frame_batch_size
        logger.warning(
            "Frame range [%d, %d) not batch-aligned to %d; rounding to [%d, %d)",
            frame_start,
            frame_end,
            frame_batch_size,
            rounded_start,
            rounded_end,
        )
        return rounded_start, rounded_end
    return frame_start, frame_end


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Extract an action-context fixture from Databricks (read-only).")
    parser.add_argument("--provider", required=True, choices=sorted(_ALL_PROVIDERS))
    parser.add_argument("--match-id", required=True)
    parser.add_argument("--period", type=int, default=None)
    parser.add_argument("--frame-start", type=int, default=None)
    parser.add_argument("--frame-end", type=int, default=None)
    parser.add_argument(
        "--num-batches",
        type=int,
        default=None,
        help="Pull N frame batches (provider's resolved batch size) from the first frame "
        "(avoids provider frame-offset footgun).",
    )
    parser.add_argument("--catalog", default="soccer_analytics")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--gold-schema", default="dev_gold")
    parser.add_argument("--no-oracles", action="store_true", help="Skip legacy oracle pulls (frames/actions only).")
    parser.add_argument(
        "--max-actions",
        type=int,
        default=None,
        help="Slice actions to the first N (keeps the SB360 fixture small; statsbomb only).",
    )
    args = parser.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_SQL_WAREHOUSE_ID"):
        if not os.environ.get(var):
            raise SystemExit(f"Missing env var: {var}")
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]

    provider, match_id, period = args.provider, args.match_id, args.period
    catalog, bronze, gold = args.catalog, args.bronze_schema, args.gold_schema
    # Per-provider size (ADR-047 amendment 2) — MUST match what prod's dispatcher uses
    # for this provider so the fixture's frame windows align to prod batch boundaries (L11).
    frame_batch_size = resolve_frame_batch_size(provider)

    if args.num_batches is not None:
        if args.frame_start is not None or args.frame_end is not None:
            raise SystemExit("--num-batches is mutually exclusive with --frame-start/--frame-end")
        if provider not in _TRACKING_PROVIDERS:
            raise SystemExit("--num-batches only applies to tracking providers")
        frame_start, frame_end = _resolve_num_batches_range(
            provider=provider,
            match_id=match_id,
            period=period,
            num_batches=args.num_batches,
            catalog=catalog,
            bronze=bronze,
            warehouse_id=warehouse_id,
            frame_batch_size=frame_batch_size,
        )
    else:
        frame_start, frame_end = _validate_frame_range(args.frame_start, args.frame_end, frame_batch_size)

    out_dir = _unit_dir(provider, match_id, period)
    out_dir.mkdir(parents=True, exist_ok=True)
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)

    # actions (always)
    actions = _pull_actions(
        provider=provider, match_id=match_id, catalog=catalog, bronze=bronze, warehouse_id=warehouse_id
    )
    if actions.empty:
        raise SystemExit(f"No SPADL actions for {provider}/{match_id} — nothing to extract.")
    if args.max_actions is not None:
        actions = actions.head(args.max_actions).copy()
        logger.info("sliced actions to first %d", args.max_actions)
    actions.to_parquet(out_dir / "actions.parquet", index=False)
    logger.info("actions: %d rows", len(actions))

    # SB360 freeze-frame snapshots (statsbomb only — drives the sb360 hexagon tier).
    if provider == "statsbomb":
        sb360 = _pull_sb360_snapshots(
            match_id=match_id, actions_pdf=actions, catalog=catalog, bronze=bronze, warehouse_id=warehouse_id
        )
        if not sb360.empty:
            sb360.to_parquet(out_dir / "sb360.parquet", index=False)
            logger.info("sb360: %d snapshot rows (%d actions)", len(sb360), sb360["action_id"].nunique())
        else:
            logger.info("sb360: no freeze-frame data for %s — event-only fixture", match_id)

    # tracking (tracking providers only)
    if provider in _TRACKING_PROVIDERS:
        frames = _pull_tracking(
            provider=provider,
            match_id=match_id,
            period=period,
            frame_start=frame_start,
            frame_end=frame_end,
            catalog=catalog,
            bronze=bronze,
            warehouse_id=warehouse_id,
        )
        if frames.empty:
            raise SystemExit(f"No tracking rows for {provider}/{match_id} in the requested range.")
        if provider == "skillcorner":
            frames = _attach_skillcorner_roster(
                frames, match_id=match_id, catalog=catalog, bronze=bronze, warehouse_id=warehouse_id
            )
        frames.to_parquet(out_dir / "frames.parquet", index=False)
        logger.info("frames: %d rows", len(frames))

    # xT grid (shared at root)
    xt_path = FIXTURE_ROOT / "xt_grid.parquet"
    if not xt_path.exists():
        xt = _pull_xt_grid(catalog=catalog, bronze=bronze, warehouse_id=warehouse_id)
        xt.to_parquet(xt_path, index=False)
        logger.info("xt_grid: %d cells -> %s", len(xt), xt_path)

    # meta
    meta = _resolve_meta(
        provider=provider,
        match_id=match_id,
        actions_pdf=actions,
        catalog=catalog,
        bronze=bronze,
        warehouse_id=warehouse_id,
    )
    pd.DataFrame([meta]).to_parquet(out_dir / "meta.parquet", index=False)
    logger.info("meta: %s", {k: v for k, v in meta.items() if not k.endswith("_json")})

    # oracles
    if not args.no_oracles:
        _pull_oracles(
            provider=provider,
            match_id=match_id,
            period=period,
            out_dir=out_dir,
            catalog=catalog,
            gold=gold,
            bronze=bronze,
            warehouse_id=warehouse_id,
        )

    logger.info("Done. Fixture at %s", out_dir)


if __name__ == "__main__":
    main()
