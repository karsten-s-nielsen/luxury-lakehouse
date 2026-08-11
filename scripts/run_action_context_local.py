"""Run AC-1 action-context LOCALLY against real Databricks data and write back.

Why this exists: ``compute_action_context`` has never produced a row on
Databricks serverless (it hangs silently inside ``applyInPandas`` — see
project memory ``ac1-serverless-hang-open``). The hexagonal architecture
(ADR-028) lets us run the IDENTICAL pure-pandas enrichment locally: read bronze
from Databricks via the SDK Statement Execution API, run ``run_work_unit`` (the
same ``enrich_batch`` frame-batch loop production dispatches via Spark groupBy),
and write the result back to the real ``bronze.spadl_action_context`` table.

Two wins in one:
  1. Populates the empty target table with real rows so downstream dbt/synced
     tables / TF work can proceed.
  2. Is itself a hang test — if the enrichment hangs HERE, we have caught the
     serverless bug locally with a debugger attached (PhaseHeartbeat prints the
     stuck phase). If it runs clean, the hang is serverless-environmental
     (fork+threads / numba / no-internet), not the compute.

Transport (best-practice per project memory ``reference_sdk_over_sql_connector``):
  * READ (small: actions / xt / meta / events) — SDK ``statement_execution``
            INLINE (auto-auth, auto-starts warehouse). Reuses the helpers in
            ``extract_action_context_fixture``.
  * READ (bulk: tracking, can be >>25 MiB) — SDK ``statement_execution`` with
            ``Disposition.EXTERNAL_LINKS`` + ``Format.ARROW_STREAM``. Returns
            presigned cloud-storage URLs to Arrow files (NO 25 MiB INLINE cap),
            downloaded directly + concatenated. INLINE would hang/error on a
            full game's tracking — this is the documented bulk-read path.
  * WRITE — stage result Parquet to UC Volume, ``DELETE`` the (match, period)
            partition, then ``COPY INTO`` (idempotent bulk load; types carried
            by the Parquet schema). Mirrors production's ``replaceWhere``.

Modes (run in this order to de-risk grants):
  --dry-run       Read + compute + write result Parquet locally only. No
                  Databricks writes. Proves read/auth/compute (and surfaces any
                  hang) with zero blast radius.
  --write-probe   Issue a no-op ``DELETE ... WHERE match_id='__never__'`` to
                  verify this identity holds MODIFY on bronze BEFORE writing.
  (default)       Full read -> compute -> DELETE partition -> chunked INSERT INTO
                  -> verify. Write uses ONLY table MODIFY (no UC Volume / Files
                  API / COPY INTO), so it works with catalog privileges the
                  caller already holds — interactive users lack READ/WRITE VOLUME.

Env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID.

Cost control — two local caches (both under gitignored dirs):
  * ``--use-cache`` saves pulled inputs to ``_ac1_cache/<provider>/<match>/`` on
    first read. A later run with a complete cache contacts NO warehouse, so the
    SQL warehouse can be STOPPED between the one-time extract and any number of
    offline recomputes. The global xT grid is cached at ``_ac1_cache/xt_grid.parquet``.
  * Every run ALWAYS writes the enriched result to ``_ac1_local_out/<provider>/``.
    ``--from-result PATH`` writes that saved parquet back to Databricks WITHOUT
    recomputing (seconds vs ~50 min/game) — re-runnable any time.

Usage::

    # 1. dry run + cache the inputs (one warehouse hit; result saved locally)
    uv run python scripts/run_action_context_local.py --provider metrica --match-id Sample_Game_1 --dry-run --use-cache

    # 1b. recompute offline from cache — warehouse can be STOPPED
    uv run python scripts/run_action_context_local.py --provider metrica --match-id Sample_Game_1 --dry-run --use-cache

    # 2. confirm write grant cheaply
    uv run python scripts/run_action_context_local.py --provider metrica --all-matches --write-probe

    # 3a. write a saved result back (no recompute, seconds)
    uv run python scripts/run_action_context_local.py --provider metrica --match-id Sample_Game_1 \
        --from-result _ac1_local_out/metrica/Sample_Game_1_pall.parquet

    # 3b. full population (read+compute+write per match)
    uv run python scripts/run_action_context_local.py --provider metrica --all-matches --use-cache
    uv run python scripts/run_action_context_local.py --provider skillcorner --all-matches --use-cache
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

# Reuse the proven SMALL-read helpers from the fixture extractor (INLINE path is
# fine for actions / xt / meta / events — all well under the 25 MiB INLINE cap).
# Tracking is pulled via the EXTERNAL_LINKS bulk reader below (no size cap).
sys.path.insert(0, str(Path(__file__).parent))
from extract_action_context_fixture import (
    _TRACKING_SELECT_COLS,
    _execute_query_to_df,
    _pull_actions,
    _pull_xt_grid,
    _q,
    _resolve_meta,
)

from ingestion.databricks_auth import workspace_client

if TYPE_CHECKING:
    from analytics.action_context.work_unit import MatchMeta, WorkUnit
    from ingestion.exec_visibility import PhaseHeartbeat

logger = logging.getLogger("ac1_local")

_TABLE = "spadl_action_context"
_FRAME_COL = {p: ("frame_num" if p == "gradientsports" else "frame") for p in _TRACKING_SELECT_COLS}

# Local caches (gitignored — see _ensure_gitignore). The input cache lets us
# extract a game ONCE from the warehouse, then recompute action-context offline
# any number of times with the warehouse stopped (no Databricks cost). The
# result cache holds the enriched output so a write-back never has to recompute.
_INPUT_CACHE_ROOT = Path("_ac1_cache")
_RESULT_OUT_ROOT = Path("_ac1_local_out")


def _input_cache_dir(provider: str, match_id: str, period: int | None) -> Path:
    name = match_id if period is None else f"{match_id}_p{period}"
    return _INPUT_CACHE_ROOT / provider / name


def _result_path(provider: str, match_id: str, period: int | None) -> Path:
    return _RESULT_OUT_ROOT / provider / f"{match_id}_p{period or 'all'}.parquet"


# ── Bulk tracking read (EXTERNAL_LINKS + ARROW_STREAM, no 25 MiB cap) ──────


def _pull_tracking_bulk(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    catalog: str,
    bronze: str,
    warehouse_id: str,
) -> pd.DataFrame:
    """Pull a full game's tracking via EXTERNAL_LINKS + ARROW_STREAM.

    INLINE disposition caps at 25 MiB and silently churns on a full game's
    tracking; EXTERNAL_LINKS returns presigned cloud URLs to Arrow chunks with
    no cap. Each chunk is downloaded (presigned — no auth header) and read via
    pyarrow, then concatenated.
    """
    import pyarrow as pa
    import requests
    from databricks.sdk.service.sql import Disposition, Format

    cols = ", ".join(_TRACKING_SELECT_COLS[provider])
    frame_col = _FRAME_COL[provider]
    table = f"{catalog}.{bronze}.{provider}_tracking"
    sql = f"SELECT {cols} FROM {table} WHERE match_id = '{_q(match_id)}'"  # noqa: S608
    if period is not None:
        sql += f" AND period = {int(period)}"
    sql += f" ORDER BY {frame_col}"

    w = workspace_client()
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
    )
    while result.status and result.status.state and result.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(3)
        assert result.statement_id is not None  # noqa: S101
        result = w.statement_execution.get_statement(result.statement_id)
    if not (result.status and result.status.state and result.status.state.value == "SUCCEEDED"):
        raise RuntimeError(f"Tracking query failed: {result.status}")

    statement_id = result.statement_id
    assert statement_id is not None  # noqa: S101

    # Iterate ALL chunks by index from the manifest (the robust pattern used by
    # the publish_*_hf.py scripts). The earlier next_chunk_index walk silently
    # stopped after the first chunk, dropping ~87% of a full game's tracking.
    total_chunks = 0
    if result.manifest is not None and result.manifest.total_chunk_count is not None:
        total_chunks = int(result.manifest.total_chunk_count)
    expected_rows = (
        int(result.manifest.total_row_count)
        if result.manifest is not None and result.manifest.total_row_count is not None
        else None
    )

    tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk = w.statement_execution.get_statement_result_chunk_n(statement_id, chunk_idx)
        for link in chunk.external_links or []:
            url = link.external_link
            assert url is not None  # noqa: S101
            if not url.startswith("https://"):  # presigned cloud URL must be HTTPS
                raise RuntimeError(f"Refusing non-HTTPS external link: {url[:60]}")
            resp = requests.get(url, timeout=(10, 120), verify=True)  # presigned — no auth header
            resp.raise_for_status()
            with pa.ipc.open_stream(pa.BufferReader(resp.content)) as reader:
                tables.append(reader.read_all())

    if not tables:
        return pd.DataFrame(columns=list(_TRACKING_SELECT_COLS[provider]))
    df = pa.concat_tables(tables).to_pandas()
    # Loud guard: silent under-read is exactly the bug this replaced.
    if expected_rows is not None and len(df) != expected_rows:
        raise RuntimeError(
            f"Tracking read incomplete: got {len(df)} rows, manifest says {expected_rows} "
            f"({total_chunks} chunks) for {provider}/{match_id}"
        )
    return df


# ── In-memory ports (data already pulled to pandas) ────────────────────────


class _MemFrameSource:
    def __init__(self, frames: pd.DataFrame, tier: str) -> None:
        self._frames = frames
        self._tier = tier

    def frames(self, wu: WorkUnit):  # WorkUnit unused: data pre-pulled
        from analytics.action_context.work_unit import FrameBundle

        return FrameBundle(tier=self._tier, frames=self._frames)


class _MemActionsSource:
    def __init__(self, actions: pd.DataFrame) -> None:
        self._actions = actions

    def actions(self, wu: WorkUnit) -> pd.DataFrame:
        return self._actions


class _MemXtSource:
    def __init__(self, grid: list[list[float]], n_x: int, n_y: int) -> None:
        self._grid, self._n_x, self._n_y = grid, n_x, n_y

    def grid(self) -> tuple[list[list[float]], int, int]:
        return self._grid, self._n_x, self._n_y


class _MemMetaSource:
    def __init__(self, meta: MatchMeta) -> None:
        self._meta = meta

    def metadata(self, wu: WorkUnit) -> MatchMeta:
        return self._meta


class _CollectingSink:
    """Captures the enriched result so the driver can write it back to Databricks."""

    def __init__(self) -> None:
        self.result: pd.DataFrame | None = None

    def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
        self.result = result_df
        return len(result_df)


# ── Helpers ────────────────────────────────────────────────────────────────


def _xt_grid_to_meta(xt_df: pd.DataFrame) -> tuple[list[list[float]], int, int]:
    import numpy as np

    zx = xt_df["zone_x"].to_numpy(dtype=int)
    zy = xt_df["zone_y"].to_numpy(dtype=int)
    vals = xt_df["xt_value"].to_numpy(dtype=float)
    n_x, n_y = int(zx.max()) + 1, int(zy.max()) + 1
    grid = np.zeros((n_y, n_x))
    grid[zy, zx] = vals
    return grid.tolist(), n_x, n_y


def _meta_dict_to_matchmeta(meta_d: dict[str, object]) -> MatchMeta:
    import json

    from analytics.action_context.work_unit import MatchMeta

    def _j(key: str):
        raw = meta_d.get(key)
        return json.loads(raw) if isinstance(raw, str) else None

    j2p_raw = _j("gs_jersey_to_player_id_json")
    j2p = {tuple(k.split("\t")): v for k, v in j2p_raw.items()} if j2p_raw else None
    return MatchMeta(
        home_team_id=str(meta_d.get("home_team_id", "unknown")),
        home_start_left=bool(meta_d.get("home_start_left", True)),
        home_team_start_left_extratime=(
            bool(meta_d["home_team_start_left_extratime"])
            if meta_d.get("home_team_start_left_extratime") is not None
            else None
        ),
        gs_team_side_to_id=_j("gs_team_side_to_id_json"),
        gs_jersey_to_player_id=j2p,
        gs_gk_player_ids=_j("gs_gk_player_ids_json"),
    )


def _list_match_ids(provider: str, catalog: str, bronze: str, warehouse_id: str) -> list[str]:
    """All match_ids with both tracking AND spadl_actions but NOT yet in results."""
    sql = (
        f"SELECT DISTINCT t.match_id FROM {catalog}.{bronze}.{provider}_tracking t "  # noqa: S608
        f"JOIN {catalog}.{bronze}.spadl_actions s "
        f"  ON CAST(t.match_id AS STRING) = CAST(s.match_id_native AS STRING) AND s.data_source = '{_q(provider)}' "
        f"LEFT ANTI JOIN {catalog}.{bronze}.{_TABLE} r "
        f"  ON CAST(t.match_id AS STRING) = CAST(r.match_id AS STRING) AND r.data_source = '{_q(provider)}'"
    )
    df = _execute_query_to_df(sql, warehouse_id)
    return [str(x) for x in df["match_id"].tolist()] if not df.empty else []


def _ensure_results_table(catalog: str, bronze: str, warehouse_id: str) -> None:
    from analytics.action_context.schema import ACTION_CONTEXT_DDL

    sql = (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.{_TABLE} ({ACTION_CONTEXT_DDL}) USING DELTA "
        "TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true', 'delta.autoOptimize.optimizeWrite'='true')"
    )
    _execute_query_to_df(sql, warehouse_id)


def _write_probe(catalog: str, bronze: str, warehouse_id: str) -> bool:
    """No-op DELETE to verify MODIFY grant without mutating data."""
    sql = f"DELETE FROM {catalog}.{bronze}.{_TABLE} WHERE match_id = '__never__'"  # noqa: S608
    try:
        _execute_query_to_df(sql, warehouse_id)
        logger.info("WRITE_PROBE ok — identity holds MODIFY on %s.%s.%s", catalog, bronze, _TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — probe: report grant failure verbatim
        logger.error("WRITE_PROBE FAILED — likely missing MODIFY grant: %s", exc)
        return False


# INSERT-batch size: rows per multi-VALUES statement. ~100 keeps each statement
# well under the SQL statement-size limit (103 cols x 100 rows) while minimizing
# round-trips. The whole game (~1.9k rows) is ~19 statements.
_INSERT_BATCH_ROWS = 100


def _ddl_type_map() -> dict[str, str]:
    """Parse ACTION_CONTEXT_DDL into {column: SPARK_TYPE} for literal rendering."""
    from analytics.action_context.schema import ACTION_CONTEXT_DDL

    type_map: dict[str, str] = {}
    for token in ACTION_CONTEXT_DDL.split(","):
        parts = token.strip().split()
        if len(parts) == 2:
            type_map[parts[0]] = parts[1].upper()
    return type_map


def _sql_literal(value: object, spark_type: str) -> str:
    """Render a single pandas value as a typed Spark SQL literal."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "NULL"
    if spark_type == "STRING":
        return "'" + str(value).replace("'", "''") + "'"
    if spark_type == "BOOLEAN":
        return "true" if bool(value) else "false"
    if spark_type == "BIGINT":
        try:
            return str(int(value))
        except (ValueError, TypeError):
            return "NULL"
    if spark_type == "DOUBLE":
        import math

        f = float(value)
        if math.isnan(f) or math.isinf(f):  # NaN / inf are not valid SQL literals
            return "NULL"
        return repr(f)
    if spark_type == "TIMESTAMP":
        ts = pd.Timestamp(value)
        return f"CAST('{ts.strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
    # Fallback: treat as string literal (safe).
    return "'" + str(value).replace("'", "''") + "'"


def _write_back(
    result: pd.DataFrame,
    *,
    provider: str,
    match_id: str,
    period: int | None,
    catalog: str,
    bronze: str,
    warehouse_id: str,
) -> int:
    """DELETE the (match, period) partition, then chunked typed INSERT INTO, verify.

    Uses ONLY table MODIFY (no UC Volume / Files API / COPY INTO), so it works
    with the catalog privileges the caller already holds — the Files-API upload
    path needs READ/WRITE VOLUME which is not granted to interactive users.
    Mirrors production's replaceWhere via DELETE + INSERT in one logical op.
    """
    out = result.copy()
    out["_ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)

    type_map = _ddl_type_map()
    # Column order = the result columns intersected with the table DDL (drop any
    # stray cols; require all are known so a schema drift fails loud, not silent).
    cols = [c for c in out.columns if c in type_map]
    unknown = [c for c in out.columns if c not in type_map]
    if unknown:
        raise RuntimeError(f"Result has columns not in target DDL (schema drift): {unknown}")
    fq = f"{catalog}.{bronze}.{_TABLE}"
    col_list = ", ".join(cols)

    # replaceWhere semantics: DELETE the partition first (idempotent re-runs).
    where = f"match_id = '{_q(match_id)}' AND data_source = '{_q(provider)}'"
    if period is not None:
        where += f" AND period_id = {int(period)}"
    _execute_query_to_df(f"DELETE FROM {fq} WHERE {where}", warehouse_id)  # noqa: S608

    # Chunked typed INSERT INTO ... VALUES.
    records = out[cols].to_dict("records")
    total = len(records)
    written = 0
    for start in range(0, total, _INSERT_BATCH_ROWS):
        batch = records[start : start + _INSERT_BATCH_ROWS]
        rows_sql = []
        for rec in batch:
            vals = ", ".join(_sql_literal(rec[c], type_map[c]) for c in cols)
            rows_sql.append(f"({vals})")
        insert_sql = f"INSERT INTO {fq} ({col_list}) VALUES " + ", ".join(rows_sql)  # noqa: S608
        _execute_query_to_df(insert_sql, warehouse_id)
        written += len(batch)
        logger.info("  inserted %d/%d rows for %s/%s", written, total, provider, match_id)

    verify = _execute_query_to_df(
        f"SELECT COUNT(*) AS n FROM {fq} WHERE {where}",  # noqa: S608
        warehouse_id,
    )
    n = int(verify["n"].iloc[0]) if not verify.empty else 0
    logger.info("VERIFY %s match=%s period=%s -> %d rows in table", provider, match_id, period, n)
    return n


def _load_inputs(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    catalog: str,
    bronze: str,
    warehouse_id: str | None,
    use_cache: bool,
    hb: PhaseHeartbeat,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object], str] | None:
    """Return (actions, frames, meta_dict, tier), from local cache or the warehouse.

    With ``use_cache`` and a complete cache present, this touches NO warehouse —
    so the SQL warehouse can be stopped. On a cache miss it pulls from the
    warehouse and writes the cache for next time. ``warehouse_id`` may be None
    only when a complete cache exists.
    """
    import json

    from analytics.action_context.work_unit import WorkUnit, provider_tier

    tier = provider_tier(WorkUnit(provider=provider, match_id=match_id, period=period))
    cdir = _input_cache_dir(provider, match_id, period)
    actions_p, frames_p, meta_p = cdir / "actions.parquet", cdir / "frames.parquet", cdir / "meta.parquet"

    cache_complete = use_cache and actions_p.exists() and meta_p.exists() and (tier != "tracking" or frames_p.exists())

    if cache_complete:
        logger.info("INPUT CACHE HIT %s/%s -> %s (warehouse not contacted)", provider, match_id, cdir)
        actions = pd.read_parquet(actions_p)
        frames = pd.read_parquet(frames_p) if tier == "tracking" else pd.DataFrame()
        meta_d = json.loads((cdir / "meta.json").read_text(encoding="utf-8"))
        return actions, frames, meta_d, tier

    if warehouse_id is None:
        logger.error("Cache miss for %s/%s and no warehouse available — cannot load inputs", provider, match_id)
        return None

    hb.set_phase("pull_actions")
    actions = _pull_actions(
        provider=provider, match_id=match_id, catalog=catalog, bronze=bronze, warehouse_id=warehouse_id
    )
    if actions.empty:
        logger.warning("No SPADL actions for %s/%s — skipping", provider, match_id)
        return None

    frames = pd.DataFrame()
    if tier == "tracking":
        hb.set_phase("pull_tracking_bulk")
        frames = _pull_tracking_bulk(
            provider=provider,
            match_id=match_id,
            period=period,
            catalog=catalog,
            bronze=bronze,
            warehouse_id=warehouse_id,
        )
        logger.info("Pulled %d tracking rows for %s/%s", len(frames), provider, match_id)
        if frames.empty:
            logger.warning("No tracking for %s/%s — skipping", provider, match_id)
            return None

    hb.set_phase("resolve_meta")
    meta_d = _resolve_meta(
        provider=provider,
        match_id=match_id,
        actions_pdf=actions,
        catalog=catalog,
        bronze=bronze,
        warehouse_id=warehouse_id,
    )

    if use_cache:
        cdir.mkdir(parents=True, exist_ok=True)
        actions.to_parquet(actions_p, index=False)
        if tier == "tracking":
            frames.to_parquet(frames_p, index=False)
        # meta as JSON (preserves the gs_*_json string fields cleanly).
        (cdir / "meta.json").write_text(json.dumps(meta_d), encoding="utf-8")
        logger.info("INPUT CACHE WRITE %s/%s -> %s", provider, match_id, cdir)

    return actions, frames, meta_d, tier


def _process_one(
    *,
    provider: str,
    match_id: str,
    period: int | None,
    catalog: str,
    bronze: str,
    warehouse_id: str | None,
    xt_meta: tuple[list[list[float]], int, int],
    dry_run: bool,
    use_cache: bool,
) -> int:
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.exec_visibility import PhaseHeartbeat

    wu = WorkUnit(provider=provider, match_id=match_id, period=period)
    hb = PhaseHeartbeat(tag=f"AC1_LOCAL[{provider}/{match_id}]", interval_s=15.0)
    hb.start("load_inputs")

    loaded = _load_inputs(
        provider=provider,
        match_id=match_id,
        period=period,
        catalog=catalog,
        bronze=bronze,
        warehouse_id=warehouse_id,
        use_cache=use_cache,
        hb=hb,
    )
    if loaded is None:
        hb.stop()
        return 0
    actions, frames, meta_d, tier = loaded
    meta = _meta_dict_to_matchmeta(meta_d)

    sink = _CollectingSink()
    hb.set_phase("enrich")  # the long phase — heartbeat reveals if THIS hangs
    run_work_unit(
        wu,
        frames=_MemFrameSource(frames, tier),
        actions=_MemActionsSource(actions),
        xt=_MemXtSource(*xt_meta),
        meta=_MemMetaSource(meta),
        sink=sink,
    )
    result = sink.result
    if result is None or result.empty:
        logger.warning("Enrichment produced 0 rows for %s/%s", provider, match_id)
        hb.stop()
        return 0
    logger.info("Enriched %s/%s -> %d rows x %d cols", provider, match_id, len(result), len(result.columns))

    # ALWAYS persist the enriched result locally so a write-back can be redone
    # later via --from-result without recomputing (50+ min/game).
    out_path = _result_path(provider, match_id, period)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, index=False)
    logger.info("Saved result parquet -> %s", out_path)

    if dry_run:
        logger.info("DRY-RUN — no Databricks write")
        hb.stop()
        return len(result)

    if warehouse_id is None:
        hb.stop()
        raise SystemExit("Write-back requires a warehouse (DATABRICKS_SQL_WAREHOUSE_ID) but none is available.")

    hb.set_phase("write_back")
    n = _write_back(
        result,
        provider=provider,
        match_id=match_id,
        period=period,
        catalog=catalog,
        bronze=bronze,
        warehouse_id=warehouse_id,
    )
    hb.stop()
    return n


def _resolve_xt_meta(
    catalog: str, bronze: str, warehouse_id: str | None, use_cache: bool
) -> tuple[list[list[float]], int, int]:
    """Global xT grid, from local cache if present (warehouse-free), else pull + cache."""
    xt_cache = _INPUT_CACHE_ROOT / "xt_grid.parquet"
    if use_cache and xt_cache.exists():
        logger.info("XT CACHE HIT -> %s", xt_cache)
        return _xt_grid_to_meta(pd.read_parquet(xt_cache))
    if warehouse_id is None:
        raise SystemExit("No cached xT grid and no warehouse available — cannot resolve xT grid.")
    xt_df = _pull_xt_grid(catalog=catalog, bronze=bronze, warehouse_id=warehouse_id)
    if xt_df.empty:
        raise SystemExit("No global xT grid in bronze.expected_threat_grids — run compute_expected_threat first.")
    if use_cache:
        _INPUT_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
        xt_df.to_parquet(xt_cache, index=False)
        logger.info("XT CACHE WRITE -> %s", xt_cache)
    return _xt_grid_to_meta(xt_df)


def _write_from_result_file(
    path: Path, *, provider: str, match_id: str, period: int | None, catalog: str, bronze: str, warehouse_id: str
) -> int:
    """Write an already-computed result parquet back to Databricks (no recompute)."""
    result = pd.read_parquet(path)
    if "_ingested_at" in result.columns:
        result = result.drop(columns=["_ingested_at"])  # re-stamped fresh in _write_back
    logger.info("Writing %d rows from %s (no recompute)", len(result), path)
    return _write_back(
        result,
        provider=provider,
        match_id=match_id,
        period=period,
        catalog=catalog,
        bronze=bronze,
        warehouse_id=warehouse_id,
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Run AC-1 locally against real Databricks data and write back.")
    p.add_argument(
        "--provider",
        required=True,
        choices=["metrica", "skillcorner", "idsse", "gradientsports", "wyscout", "statsbomb"],
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--match-id", help="Single match id")
    g.add_argument("--all-matches", action="store_true", help="All unprocessed matches for the provider")
    p.add_argument("--period", type=int, default=None, help="Period filter (tracking providers)")
    p.add_argument("--catalog", default="soccer_analytics")
    p.add_argument("--bronze-schema", default="bronze")
    p.add_argument("--dry-run", action="store_true", help="Read+compute+local parquet only; no Databricks write")
    p.add_argument("--write-probe", action="store_true", help="No-op DELETE to verify MODIFY grant, then exit")
    p.add_argument(
        "--use-cache",
        action="store_true",
        help="Cache pulled inputs (frames/actions/meta/xt) under _ac1_cache/ and reuse them. "
        "With a complete cache, recompute touches NO warehouse (stop it to save cost).",
    )
    p.add_argument(
        "--from-result",
        metavar="PARQUET",
        help="Skip read+compute; write this already-computed result parquet back to Databricks. "
        "Requires --provider + --match-id (+ --period if partial). Seconds, not minutes.",
    )
    args = p.parse_args()

    # Warehouse is optional: a fully-cached dry-run / recompute needs no warehouse.
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    have_creds = all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")) and warehouse_id
    if not have_creds:
        warehouse_id = None
        logger.warning("Databricks creds incomplete — running in warehouse-free mode (cache-only).")
    catalog, bronze = args.catalog, args.bronze_schema

    # ── --from-result: write a saved parquet, no recompute ──
    if args.from_result:
        if not args.match_id:
            raise SystemExit("--from-result requires --match-id (to key the DELETE partition).")
        if warehouse_id is None:
            raise SystemExit("--from-result writes to Databricks — DATABRICKS_* env vars required.")
        _ensure_results_table(catalog, bronze, warehouse_id)
        n = _write_from_result_file(
            Path(args.from_result),
            provider=args.provider,
            match_id=args.match_id,
            period=args.period,
            catalog=catalog,
            bronze=bronze,
            warehouse_id=warehouse_id,
        )
        logger.info("DONE --from-result wrote %d rows for %s/%s", n, args.provider, args.match_id)
        return 0

    if args.write_probe:
        if warehouse_id is None:
            raise SystemExit("--write-probe needs a warehouse.")
        ok = _write_probe(catalog, bronze, warehouse_id)
        return 0 if ok else 1

    # Discovering matches needs the warehouse; an explicit --match-id does not.
    if args.match_id:
        match_ids = [args.match_id]
    elif warehouse_id is not None:
        match_ids = _list_match_ids(args.provider, catalog, bronze, warehouse_id)
    else:
        raise SystemExit("--all-matches needs a warehouse to discover matches; use --match-id in cache-only mode.")
    if not match_ids:
        logger.info("No unprocessed matches for provider=%s — nothing to do.", args.provider)
        return 0
    logger.info("Provider=%s: %d match(es) to process: %s", args.provider, len(match_ids), match_ids)

    # Writes (not dry-run) need the results table to exist.
    if not args.dry_run and warehouse_id is not None:
        _ensure_results_table(catalog, bronze, warehouse_id)

    xt_meta = _resolve_xt_meta(catalog, bronze, warehouse_id, args.use_cache)

    total = 0
    t0 = time.monotonic()
    for mid in match_ids:
        try:
            total += _process_one(
                provider=args.provider,
                match_id=mid,
                period=args.period,
                catalog=catalog,
                bronze=bronze,
                warehouse_id=warehouse_id,
                xt_meta=xt_meta,
                dry_run=args.dry_run,
                use_cache=args.use_cache,
            )
        except Exception:
            logger.exception("FAILED on %s/%s", args.provider, mid)
            raise
    logger.info(
        "DONE provider=%s matches=%d total_rows=%d elapsed=%.0fs%s",
        args.provider,
        len(match_ids),
        total,
        time.monotonic() - t0,
        " (DRY-RUN)" if args.dry_run else "",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
