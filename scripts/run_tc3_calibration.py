"""TC-3: Optuna Calibration Sweep for silly-kicks tracking defaults.

Replaces engineering-choice defaults with data-calibrated values:
  - Stage 1: infer_ball_carrier (tolerance_m, beta, gamma) — carrier accuracy
  - Stage 2: LinkParams.k3 + off-ball-runs (pre_seconds, min_displacement_m) — VAEP Brier

Usage:
  uv run python scripts/run_tc3_calibration.py --stage 0   # Data loading + validation
  uv run python scripts/run_tc3_calibration.py --stage 1   # Carrier accuracy sweep
  uv run python scripts/run_tc3_calibration.py --stage 2   # VAEP Brier sweep
  uv run python scripts/run_tc3_calibration.py --stage diagnostics  # Post-hoc analysis
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("tc3_calibration")

# ── Paths ────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("docs/evolve/tc3-calibration")
CACHE_DIR = Path(".tc3_cache")

# ── Provider configuration ────────────────────────────────────────────────
PROVIDERS = ("idsse", "skillcorner", "gradientsports")


@dataclass(frozen=True)
class MatchData:
    """Cached per-match data for calibration.

    Frames are NOT held in memory — loaded on demand from parquet cache
    to avoid OOM when 78 matches (13 GB compressed, ~50 GB in-memory)
    are loaded simultaneously.
    """

    match_id: str
    provider: str
    actions: pd.DataFrame
    frames_path: Path
    home_team_id: str
    home_start_left: bool

    def load_frames(self) -> pd.DataFrame:
        """Load tracking frames from parquet cache (on demand)."""
        return pd.read_parquet(self.frames_path)


@dataclass
class CalibrationDataset:
    """All loaded match data + metadata for the calibration sweep."""

    matches: list[MatchData] = field(default_factory=list)
    xt: Any = None  # ExpectedThreat
    vaep_labels: dict[str, pd.DataFrame] = field(default_factory=dict)

    def matches_by_provider(self, provider: str) -> list[MatchData]:
        return [m for m in self.matches if m.provider == provider]


# ── Phase 0: Data Loading ────────────────────────────────────────────────


def _pull_provider_data_sql(provider: str) -> list[MatchData]:
    """Pull tracking + actions for a provider via Databricks SQL connector.

    Returns list of MatchData, one per match.
    """
    import os

    from databricks import sql as dbsql

    host = os.environ["DATABRICKS_HOST"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]
    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    schema = "bronze"

    conn = dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )

    try:
        # Discover tracking matches that have SPADL actions
        cursor = conn.cursor()
        tracking_table = "idsse_tracking" if provider == "idsse" else f"{provider}_tracking"
        cursor.execute(f"""
            SELECT DISTINCT a.match_id_native
            FROM {catalog}.{schema}.spadl_actions a
            WHERE a.data_source = '{provider}'
              AND EXISTS (
                SELECT 1 FROM {catalog}.{schema}.{tracking_table} t
                WHERE t.match_id = a.match_id_native
              )
        """)
        match_ids = [str(row[0]) for row in cursor.fetchall()]
        cursor.close()
        logger.info(
            "Found %d %s matches with paired tracking + SPADL",
            len(match_ids),
            provider,
        )

        results: list[MatchData] = []
        for mid in match_ids:
            cache_path = CACHE_DIR / provider / mid
            if (cache_path / "actions.parquet").exists() and (cache_path / "frames.parquet").exists():
                logger.info("Cache hit: %s/%s", provider, mid)
                actions = pd.read_parquet(cache_path / "actions.parquet")
                meta = json.loads((cache_path / "meta.json").read_text())
                results.append(
                    MatchData(
                        match_id=mid,
                        provider=provider,
                        actions=actions,
                        frames_path=cache_path / "frames.parquet",
                        home_team_id=meta["home_team_id"],
                        home_start_left=meta["home_start_left"],
                    )
                )
                continue

            logger.info("Pulling %s match %s from Databricks...", provider, mid)

            # Pull actions
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {catalog}.{schema}.spadl_actions
                WHERE data_source = '{provider}' AND match_id_native = '{mid}'
            """)
            actions = cursor.fetchall_arrow().to_pandas()
            cursor.close()

            if actions.empty:
                logger.warning("No actions for %s/%s — skipping", provider, mid)
                continue

            # Pull tracking
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {catalog}.{schema}.{tracking_table}
                WHERE match_id = '{mid}'
            """)
            frames_raw = cursor.fetchall_arrow().to_pandas()
            cursor.close()

            if frames_raw.empty:
                logger.warning("No tracking for %s/%s — skipping", provider, mid)
                continue

            # SkillCorner: join with matches to add team + is_goalkeeper
            # (mirrors production pipeline tracking_context.py:1703-1713)
            if provider == "skillcorner":
                cursor2 = conn.cursor()
                cursor2.execute(f"""
                    SELECT player_id,
                           CAST(team_id AS STRING) AS team,
                           (position_acronym = 'GK') AS is_goalkeeper
                    FROM {catalog}.{schema}.skillcorner_matches
                    WHERE match_id = '{mid}'
                """)
                matches_meta = cursor2.fetchall_arrow().to_pandas()
                cursor2.close()
                frames_raw = frames_raw.merge(matches_meta, on="player_id", how="left")

            # Gradient Sports: join roster for player_id/team_id/is_goalkeeper,
            # and metadata for fps. Keyed on jersey_num + team_side.
            if provider == "gradientsports":
                cursor2 = conn.cursor()
                cursor2.execute(f"""
                    SELECT `homeTeam.id` AS home_team_id, fps
                    FROM {catalog}.{schema}.gradientsports_metadata
                    WHERE match_id = '{mid}'
                """)
                meta_row = cursor2.fetchone()
                cursor2.close()
                gs_home_team = str(meta_row[0]) if meta_row else None
                gs_fps = float(meta_row[1]) if meta_row and meta_row[1] else 25.0

                cursor2 = conn.cursor()
                cursor2.execute(f"""
                    SELECT `player.id` AS player_id,
                           `team.id` AS team_id,
                           shirtNumber AS jersey_num,
                           (positionGroupType = 'GK') AS is_gk
                    FROM {catalog}.{schema}.gradientsports_roster
                    WHERE match_id = '{mid}'
                """)
                roster = cursor2.fetchall_arrow().to_pandas()
                cursor2.close()

                # Build team_side → team_id mapping
                _gs_home = gs_home_team  # bind for lambda (B023)
                roster["team_side"] = roster["team_id"].apply(
                    lambda tid, ht=_gs_home: "home" if str(tid) == ht else "away"
                )

                # Merge roster onto tracking by team_side + jersey_num
                frames_raw["jersey_num"] = frames_raw["jersey_num"].astype(str)
                roster["jersey_num"] = roster["jersey_num"].astype(str)
                frames_raw = frames_raw.merge(
                    roster[["team_side", "jersey_num", "player_id", "team_id", "is_gk"]],
                    on=["team_side", "jersey_num"],
                    how="left",
                )
                frames_raw.rename(
                    columns={
                        "player_id": "_gs_roster_player",
                        "team_id": "_gs_roster_team",
                        "is_gk": "_gs_roster_gk",
                    },
                    inplace=True,
                )
                frames_raw["_gs_fps"] = gs_fps

            # Resolve home_team_id + home_start_left per provider
            try:
                home_team_id, home_start_left = _resolve_match_metadata(
                    conn,
                    catalog,
                    schema,
                    provider,
                    mid,
                    actions,
                    frames_raw,
                )

                # Convert bronze tracking to silly-kicks frames format
                frames = _convert_tracking_to_frames(
                    provider,
                    frames_raw,
                    actions,
                    mid,
                    home_team_id,
                    home_start_left,
                )
            except Exception as exc:
                logger.warning(
                    "Skipping %s/%s — conversion failed: %s",
                    provider,
                    mid,
                    exc,
                )
                continue

            # Cache
            cache_path.mkdir(parents=True, exist_ok=True)
            actions.to_parquet(cache_path / "actions.parquet")
            frames.to_parquet(cache_path / "frames.parquet")
            (cache_path / "meta.json").write_text(
                json.dumps(
                    {
                        "home_team_id": str(home_team_id),
                        "home_start_left": home_start_left,
                    }
                )
            )

            results.append(
                MatchData(
                    match_id=mid,
                    provider=provider,
                    actions=actions,
                    frames_path=cache_path / "frames.parquet",
                    home_team_id=str(home_team_id),
                    home_start_left=home_start_left,
                )
            )

        return results
    finally:
        conn.close()


def _resolve_match_metadata(
    conn: Any,
    catalog: str,
    schema: str,
    provider: str,
    mid: str,
    actions: pd.DataFrame,
    frames_raw: pd.DataFrame,
) -> tuple[str, bool]:
    """Resolve home_team_id and home_start_left for a match."""
    if provider == "idsse":
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT home_team_id_native
            FROM {catalog}.{schema}.idsse_events
            WHERE match_id = '{mid}'
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        home_team_id = str(row[0]) if row else str(actions["team_id_native"].dropna().iloc[0])
        from ingestion.spadl_adapter import derive_idsse_home_team_start_left

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {catalog}.{schema}.idsse_events
            WHERE match_id = '{mid}'
        """)
        events_df = cursor.fetchall_arrow().to_pandas()
        cursor.close()
        from ingestion.spadl_adapter import adapt_idsse_events_for_silly_kicks

        adapted = adapt_idsse_events_for_silly_kicks(events_df)
        home_start_left = derive_idsse_home_team_start_left(adapted, home_team_id)
        return home_team_id, home_start_left

    elif provider == "skillcorner":
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT home_team_id
            FROM {catalog}.{schema}.skillcorner_matches
            WHERE match_id = '{mid}'
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        home_team_id = str(row[0]) if row else "unknown"
        # True is the production convention (tracking_context.py:1738) —
        # only IDSSE overrides. SkillCorner data is pre-normalized by provider.
        return home_team_id, True

    elif provider == "gradientsports":
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT `homeTeam.id`, `awayTeam.id`, homeTeamStartLeft
            FROM {catalog}.{schema}.gradientsports_metadata
            WHERE match_id = '{mid}'
        """)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            msg = f"No GS metadata for match {mid}"
            raise ValueError(msg)
        home_team_id = str(row[0])
        home_start_left = bool(row[2])
        return home_team_id, home_start_left

    msg = f"Unknown provider: {provider}"
    raise ValueError(msg)


def _bronze_gs_to_converter_input(
    frames_raw: pd.DataFrame,
    mid: str,
    home_team_id: str,
) -> pd.DataFrame:
    """Transform bronze GS tracking columns to silly-kicks converter input.

    Bronze columns: match_id, frame_num, period, period_elapsed_time,
        team_side (home/away/None), jersey_num, is_ball, x, y, z, fps (from metadata)
    Converter expects: game_id, period_id, frame_id, time_seconds, frame_rate,
        player_id, team_id, is_ball, is_goalkeeper, x_centered, y_centered, z,
        speed_native, ball_state
    """
    out = frames_raw.copy()

    # Rename direct mappings
    out["game_id"] = int(mid)
    out["period_id"] = out["period"].astype("Int64")
    out["frame_id"] = out["frame_num"].astype("Int64")
    out["time_seconds"] = out["period_elapsed_time"]

    # Frame rate from metadata (stored as _gs_fps by caller)
    if "_gs_fps" in out.columns:
        out["frame_rate"] = out["_gs_fps"]
    else:
        out["frame_rate"] = 25.0  # fallback

    # Resolve player_id and team_id from jersey_num + team_side via roster
    # Roster is stored as _gs_roster by the caller: jersey_num -> (player_id, team_id, is_gk)
    if "_gs_roster_player" in out.columns:
        out["player_id"] = out["_gs_roster_player"].astype("Int64")
        out["team_id"] = out["_gs_roster_team"].astype("Int64")
        out["is_goalkeeper"] = out["_gs_roster_gk"].fillna(False).astype(bool)
    else:
        # Fallback: no roster data — use jersey_num as player_id
        out["player_id"] = pd.to_numeric(out["jersey_num"], errors="coerce").astype("Int64")
        # Map team_side to team_id
        out["team_id"] = pd.array([pd.NA] * len(out), dtype="Int64")
        out["is_goalkeeper"] = False

    # Ball rows: null out player fields
    ball_mask = out["is_ball"] == True  # noqa: E712
    out.loc[ball_mask, "player_id"] = pd.NA
    out.loc[ball_mask, "team_id"] = pd.NA

    # Coordinates are already center-origin in bronze
    out["x_centered"] = out["x"]
    out["y_centered"] = out["y"]

    # Speed not available in bronze (derived by converter)
    out["speed_native"] = np.nan

    # Ball state not available in bronze
    out["ball_state"] = pd.NA

    # Select only the expected columns
    expected = [
        "game_id",
        "period_id",
        "frame_id",
        "time_seconds",
        "frame_rate",
        "player_id",
        "team_id",
        "is_ball",
        "is_goalkeeper",
        "x_centered",
        "y_centered",
        "z",
        "speed_native",
        "ball_state",
    ]
    return out[expected]


def _convert_tracking_to_frames(
    provider: str,
    frames_raw: pd.DataFrame,
    actions: pd.DataFrame,
    mid: str,
    home_team_id: str,
    home_start_left: bool,
) -> pd.DataFrame:
    """Convert bronze tracking to silly-kicks frames format."""
    if provider == "idsse":
        from silly_kicks.tracking import PreprocessConfig
        from silly_kicks.tracking.sportec import convert_to_frames

        from ingestion.tracking_context import _bronze_idsse_to_sportec_input

        sportec_input = _bronze_idsse_to_sportec_input(frames_raw)
        frames, _report = convert_to_frames(
            sportec_input,
            home_team_id=home_team_id,
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        return frames

    elif provider == "skillcorner":
        from ingestion.tracking_context import _bronze_skillcorner_to_frames

        game_id = int(actions["game_id"].iloc[0])
        frames = _bronze_skillcorner_to_frames(frames_raw, game_id=game_id)
        return frames

    elif provider == "gradientsports":
        from silly_kicks.tracking import PreprocessConfig
        from silly_kicks.tracking.gradientsports import convert_to_frames

        # Bronze has raw GS columns; convert_to_frames expects processed format.
        # Transform: jersey_num+team_side → player_id/team_id via roster lookup.
        processed = _bronze_gs_to_converter_input(frames_raw, mid, home_team_id)
        frames, _report = convert_to_frames(
            processed,
            home_team_id=int(home_team_id),
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        return frames

    msg = f"Unknown provider: {provider}"
    raise ValueError(msg)


def _validate_gradient_sports(matches: list[MatchData]) -> list[MatchData]:
    """Gradient Sports Phase 0 validation gate (M6).

    Per-match checks:
    - Frame count sanity (min/max, flag outliers)
    - GK identification via is_goalkeeper column
    - NaN prevalence in x/y
    - At least one action from each team

    Returns filtered list (anomalous matches excluded).
    """
    valid: list[MatchData] = []
    for m in matches:
        issues: list[str] = []
        frames = m.load_frames()

        # Frame count (at 25fps, 25000 frames = ~17 min = one full half minimum)
        n_frames = frames["frame_id"].nunique()
        if n_frames < 25_000:
            issues.append(f"Low frame count: {n_frames} (min 25,000 for meaningful features)")
        if n_frames > 500_000:
            issues.append(f"Suspiciously high frame count: {n_frames}")

        # GK identification
        gk_mask = frames["is_goalkeeper"] == True  # noqa: E712
        n_gk = frames.loc[gk_mask, "player_id"].nunique()
        if n_gk < 2:
            issues.append(f"Only {n_gk} distinct GK player(s) identified (expected 2)")

        # NaN prevalence
        xy_nan_frac = frames[["x", "y"]].isna().mean().max()
        if xy_nan_frac > 0.3:
            issues.append(f"High NaN rate in x/y: {xy_nan_frac:.1%}")

        del frames  # Release memory immediately

        # Team coverage
        n_teams = m.actions["team_id_native"].nunique()
        if n_teams < 2:
            issues.append(f"Only {n_teams} team(s) in actions")

        if issues:
            logger.warning(
                "Gradient Sports match %s excluded: %s",
                m.match_id,
                "; ".join(issues),
            )
        else:
            valid.append(m)

    logger.info(
        "Gradient Sports validation: %d/%d matches passed",
        len(valid),
        len(matches),
    )
    return valid


def _add_spadl_names(actions: pd.DataFrame) -> pd.DataFrame:
    """Add type_name/result_name/bodypart_name from integer IDs."""
    from silly_kicks.spadl.config import actiontypes, bodyparts, results

    actions = actions.copy()
    actions["type_name"] = actions["type_id"].map(dict(enumerate(actiontypes)))
    actions["result_name"] = actions["result_id"].map(dict(enumerate(results)))
    actions["bodypart_name"] = actions["bodypart_id"].map(dict(enumerate(bodyparts)))
    return actions


def _compute_vaep_labels(matches: list[MatchData]) -> dict[str, pd.DataFrame]:
    """Compute VAEP scoring/conceding labels per match."""
    from silly_kicks.vaep.labels import concedes as compute_concedes
    from silly_kicks.vaep.labels import scores as compute_scores

    labels: dict[str, pd.DataFrame] = {}
    for m in matches:
        acts = _add_spadl_names(m.actions)
        sc = compute_scores(acts, nr_actions=10)
        co = compute_concedes(acts, nr_actions=10)
        labels[m.match_id] = pd.DataFrame(
            {
                "scores": sc.values.ravel(),
                "concedes": co.values.ravel(),
            }
        )
    return labels


def run_phase0(args: argparse.Namespace) -> CalibrationDataset:
    """Phase 0: Data loading + validation."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    dataset = CalibrationDataset()

    # Pull data per provider
    for provider in PROVIDERS:
        try:
            matches = _pull_provider_data_sql(provider)
            if provider == "gradientsports":
                # Gradient Sports is a new untested source — apply validation gate.
                # IDSSE/SkillCorner are validated by lakehouse dbt tests + TC-1 pipeline.
                matches = _validate_gradient_sports(matches)
            dataset.matches.extend(matches)
            logger.info("Loaded %d %s matches", len(matches), provider)
        except Exception as exc:
            logger.error("Failed to load %s data: %s", provider, exc)
            if provider in ("idsse", "skillcorner"):
                raise  # Core providers must succeed
            # Gradient Sports failure is non-fatal — sweep proceeds with existing data

    if not dataset.matches:
        logger.error("No match data loaded — cannot proceed")
        sys.exit(1)

    # Fit xT grid
    from silly_kicks.xthreat import ExpectedThreat

    all_actions = pd.concat([m.actions for m in dataset.matches], ignore_index=True)
    dataset.xt = ExpectedThreat().fit(all_actions)
    logger.info("xT model fitted (grid shape %s)", dataset.xt.xT.shape)

    # Compute VAEP labels
    dataset.vaep_labels = _compute_vaep_labels(dataset.matches)
    logger.info("VAEP labels computed for %d matches", len(dataset.vaep_labels))

    # Summary
    for provider in PROVIDERS:
        n = len(dataset.matches_by_provider(provider))
        if n > 0:
            logger.info("  %s: %d matches", provider, n)

    return dataset


# ── Calibration enrichment (mirrors _enrich_match with param injection) ──


def _enrich_match_with_params(
    *,
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: Any,
    home_team_id: str,
    match_id_native: str,
    data_source: str,
    # Tunable params — Stage 1
    carrier_tolerance_m: float = 3.0,
    carrier_beta: float = 0.5,
    carrier_gamma: float = 1.0,
    # Tunable params — Stage 2
    k3: float = 1.0,
    pre_seconds: float = 1.5,
    min_displacement_m: float = 3.0,
) -> pd.DataFrame:
    """Enrichment chain with injectable parameters for calibration.

    Mirrors production _enrich_match (tracking_context.py:603-825) but
    accepts tunable parameters for the three target subsystems.

    Returns the enriched actions DataFrame (no identity restore — calibration
    doesn't write to bronze, it just extracts feature vectors).
    """
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        LinkParams,
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_defensive_line,
        add_gk_influence,
        add_line_break,
        add_off_ball_context,
        add_pressure_on_actor,
        add_sync_score,
        add_team_shape,
        derive_team_in_possession,
        infer_ball_carrier,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    # Resolve enrichment-compatible identity
    actions = _resolve_enrichment_identity(
        actions.copy(),
        provider=data_source,
        match_id_native=match_id_native,
    )

    # Step 0: Link actions to frames
    links, _report = link_actions_to_frames(actions, frames)

    # Step 1: GK resolution
    actions = add_pre_shot_gk_context(actions, frames=frames)

    # Step 2: Action context
    actions = add_action_context(actions, frames, links=links)

    # Step 3: Actor pre-window
    actions = add_actor_pre_window(actions, frames, links=links)

    # Step 4a: Pressure — andrienko_oval (no k3 dependency)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("andrienko_oval",),
    )

    # Step 4b: Pressure — link_zones (k3 INJECTED here)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("link_zones",),
        params_per_method={"link_zones": LinkParams(k3=k3)},
    )

    # Step 4c: Pressure — bekkers_pi
    try:
        actions = add_pressure_on_actor(
            actions,
            frames,
            links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            actions["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise

    # Steps 5-7: Pitch control
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(actions, frames, links=links, method=method)
        actions[s.name] = s.values

    # Step 8: Defensive line
    actions = add_defensive_line(actions, frames, links=links, home_team_id=home_team_id)

    # Step 9: Off-ball context (pre_seconds, min_displacement_m INJECTED)
    actions = add_off_ball_context(
        actions,
        frames,
        links=links,
        home_team_id=home_team_id,
        pre_seconds=pre_seconds,
        min_displacement_m=min_displacement_m,
    )

    # Step 10: Ward line-breaking
    actions = add_line_break(
        actions,
        frames,
        links=links,
        method="ward",
        home_team_id=home_team_id,
    )

    # Step 11: Team shape
    actions = add_team_shape(actions, frames, links=links, home_team_id=home_team_id)

    # Step 12: DAS (carrier params INJECTED here)
    from silly_kicks.tracking._das import get_individual_das

    try:
        carrier = infer_ball_carrier(
            frames,
            tolerance_m=carrier_tolerance_m,
            beta=carrier_beta,
            gamma=carrier_gamma,
        )
        frames_with_tip = derive_team_in_possession(frames, carrier)
        del carrier

        linked = links[["action_id", "frame_id"]].dropna(subset=["frame_id"])
        linked = linked.merge(actions[["action_id", "period_id"]], on="action_id", how="left")
        linked_frame_ids = linked[["period_id", "frame_id"]].drop_duplicates()
        das_frames = frames_with_tip.merge(linked_frame_ids, on=["period_id", "frame_id"], how="inner")
        del linked, frames_with_tip

        das_result = get_individual_das(das_frames, use_progress_bar=False, chunk_size=10)
        del das_frames

        player_rows = das_result[das_result["is_ball"] != True]  # noqa: E712
        valid_rows = player_rows.dropna(subset=["DAS"])
        das_lookup: dict[tuple, dict] = {}
        for (pid, fid, tid), grp in valid_rows.groupby(["period_id", "frame_id", "team_id"]):
            das_lookup.setdefault((pid, fid), {})[tid] = float(grp["DAS"].sum())
        del das_result, player_rows, valid_rows

        pointer_lookup = links.set_index("action_id")
        team_vals = np.full(len(actions), np.nan)
        opp_vals = np.full(len(actions), np.nan)

        # itertuples is 10-50x faster than iterrows
        for row in actions.itertuples():
            i = row.Index
            aid = row.action_id
            if aid not in pointer_lookup.index:
                continue
            fid_raw = pointer_lookup.at[aid, "frame_id"]
            if pd.isna(fid_raw):
                continue
            key = (row.period_id, int(float(fid_raw)))
            if key not in das_lookup:
                continue
            team_id = row.team_id
            team_vals[i] = das_lookup[key].get(team_id, np.nan)
            opp = [v for k, v in das_lookup[key].items() if k != team_id]
            if opp:
                opp_vals[i] = opp[0]

        actions["das_team"] = team_vals
        actions["das_opponent"] = opp_vals
        actions["das_diff"] = team_vals - opp_vals

    except (IndexError, ValueError, RuntimeError, TypeError):
        logger.exception("DAS degraded to NaN for match %s", match_id_native)
        actions["das_team"] = np.nan
        actions["das_opponent"] = np.nan
        actions["das_diff"] = np.nan

    # Step 13: GK influence
    actions = add_gk_influence(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows
    actions = add_cover_shadows(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 15: Sync score
    actions = add_sync_score(actions, links)

    return actions


# ── Stage 2: Invariant enrichment cache ──────────────────────────────────
# Steps 4b (link_zones pressure, depends on k3) and 9 (off-ball context,
# depends on pre_seconds + min_displacement_m) are the ONLY trial-varying
# steps.  The other 14 steps are invariant across Stage 2 trials.
# Computing them once and caching saves ~95% of per-trial wall time.

# Columns written by the two trial-dependent steps:
_TRIAL_DEPENDENT_COLS = [
    "pressure_on_actor__link_zones",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
]


def _enrich_match_invariant(
    *,
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: Any,
    home_team_id: str,
    match_id_native: str,
    data_source: str,
    carrier_tolerance_m: float,
    carrier_beta: float,
    carrier_gamma: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run all enrichment steps EXCEPT the trial-dependent ones (4b, 9).

    Returns (enriched_actions, links) — links are needed by the patch step.
    Trial-dependent columns are set to NaN as placeholders.
    """
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_defensive_line,
        add_gk_influence,
        add_line_break,
        add_pressure_on_actor,
        add_sync_score,
        add_team_shape,
        derive_team_in_possession,
        infer_ball_carrier,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    actions = _resolve_enrichment_identity(
        actions.copy(),
        provider=data_source,
        match_id_native=match_id_native,
    )

    # Step 0: Link actions to frames
    links, _report = link_actions_to_frames(actions, frames)

    # Step 1: GK resolution
    actions = add_pre_shot_gk_context(actions, frames=frames)

    # Step 2: Action context
    actions = add_action_context(actions, frames, links=links)

    # Step 3: Actor pre-window
    actions = add_actor_pre_window(actions, frames, links=links)

    # Step 4a: Pressure — andrienko_oval (invariant)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("andrienko_oval",),
    )

    # Step 4b: SKIPPED — link_zones depends on k3 (trial-dependent)
    actions["pressure_on_actor__link_zones"] = np.nan

    # Step 4c: Pressure — bekkers_pi (invariant)
    try:
        actions = add_pressure_on_actor(
            actions,
            frames,
            links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            actions["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise

    # Steps 5-7: Pitch control (invariant)
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(actions, frames, links=links, method=method)
        actions[s.name] = s.values

    # Step 8: Defensive line (invariant)
    actions = add_defensive_line(actions, frames, links=links, home_team_id=home_team_id)

    # Step 9: SKIPPED — off-ball context depends on pre_seconds/min_displacement_m
    for col in _TRIAL_DEPENDENT_COLS[1:]:  # skip pressure col already set above
        actions[col] = np.nan

    # Step 10: Ward line-breaking (invariant)
    actions = add_line_break(
        actions,
        frames,
        links=links,
        method="ward",
        home_team_id=home_team_id,
    )

    # Step 11: Team shape (invariant)
    actions = add_team_shape(actions, frames, links=links, home_team_id=home_team_id)

    # Step 12: DAS (carrier params fixed from Stage 1 — invariant)
    from silly_kicks.tracking._das import get_individual_das

    try:
        carrier = infer_ball_carrier(
            frames,
            tolerance_m=carrier_tolerance_m,
            beta=carrier_beta,
            gamma=carrier_gamma,
        )
        frames_with_tip = derive_team_in_possession(frames, carrier)
        del carrier

        linked = links[["action_id", "frame_id"]].dropna(subset=["frame_id"])
        linked = linked.merge(actions[["action_id", "period_id"]], on="action_id", how="left")
        linked_frame_ids = linked[["period_id", "frame_id"]].drop_duplicates()
        das_frames = frames_with_tip.merge(linked_frame_ids, on=["period_id", "frame_id"], how="inner")
        del linked, frames_with_tip

        das_result = get_individual_das(das_frames, use_progress_bar=False, chunk_size=10)
        del das_frames

        player_rows = das_result[das_result["is_ball"] != True]  # noqa: E712
        valid_rows = player_rows.dropna(subset=["DAS"])
        das_lookup: dict[tuple, dict] = {}
        for (pid, fid, tid), grp in valid_rows.groupby(["period_id", "frame_id", "team_id"]):
            das_lookup.setdefault((pid, fid), {})[tid] = float(grp["DAS"].sum())
        del das_result, player_rows, valid_rows

        pointer_lookup = links.set_index("action_id")
        team_vals = np.full(len(actions), np.nan)
        opp_vals = np.full(len(actions), np.nan)

        for row in actions.itertuples():
            i = row.Index
            aid = row.action_id
            if aid not in pointer_lookup.index:
                continue
            fid_raw = pointer_lookup.at[aid, "frame_id"]
            if pd.isna(fid_raw):
                continue
            key = (row.period_id, int(float(fid_raw)))
            if key not in das_lookup:
                continue
            team_id = row.team_id
            team_vals[i] = das_lookup[key].get(team_id, np.nan)
            opp = [v for k, v in das_lookup[key].items() if k != team_id]
            if opp:
                opp_vals[i] = opp[0]

        actions["das_team"] = team_vals
        actions["das_opponent"] = opp_vals
        actions["das_diff"] = team_vals - opp_vals

    except (IndexError, ValueError, RuntimeError, TypeError):
        logger.exception("DAS degraded to NaN for match %s", match_id_native)
        actions["das_team"] = np.nan
        actions["das_opponent"] = np.nan
        actions["das_diff"] = np.nan

    # Step 13: GK influence (invariant)
    actions = add_gk_influence(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows (invariant)
    actions = add_cover_shadows(actions, frames, xt, links=links, home_team_id=home_team_id)

    # Step 15: Sync score (invariant)
    actions = add_sync_score(actions, links)

    return actions, links


def _patch_trial_columns(
    *,
    base_actions: pd.DataFrame,
    frames: pd.DataFrame,
    links: pd.DataFrame,
    home_team_id: str,
    k3: float,
    pre_seconds: float,
    min_displacement_m: float,
) -> pd.DataFrame:
    """Overwrite only the trial-dependent columns on a cached base enrichment.

    Runs Steps 4b (link_zones pressure) and 9 (off-ball context) only.
    """
    from silly_kicks.tracking import (
        LinkParams,
        add_off_ball_context,
        add_pressure_on_actor,
    )

    actions = base_actions.copy()

    # Step 4b: link_zones pressure (k3)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("link_zones",),
        params_per_method={"link_zones": LinkParams(k3=k3)},
    )

    # Step 9: Off-ball context (pre_seconds, min_displacement_m)
    actions = add_off_ball_context(
        actions,
        frames,
        links=links,
        home_team_id=home_team_id,
        pre_seconds=pre_seconds,
        min_displacement_m=min_displacement_m,
    )

    return actions


# ── Stage 1: Carrier accuracy ────────────────────────────────────────────


def _compute_carrier_accuracy_for_match(
    match: MatchData,
    *,
    tolerance_m: float,
    beta: float,
    gamma: float,
) -> tuple[float, float]:
    """Compute carrier accuracy and switch rate for one match.

    Compares inferred ball carrier to SPADL action actor at linked timestamps.
    Filters to action types where actor == ball carrier by definition
    (pass, cross, shot, dribble). Tackles/interceptions/clearances excluded
    because the actor is the interceptor, not the ball carrier.

    Returns:
        (accuracy, switches_per_minute) tuple.
    """
    from silly_kicks.tracking import infer_ball_carrier, link_actions_to_frames

    frames = match.load_frames()

    carrier = infer_ball_carrier(
        frames,
        tolerance_m=tolerance_m,
        beta=beta,
        gamma=gamma,
    )

    # Link actions to frames to get ground-truth timestamps
    links, _ = link_actions_to_frames(match.actions, frames)

    # Filter to action types where actor == ball carrier by definition.
    carrier_action_types = {"pass", "cross", "shot", "dribble"}
    if "type_name" in match.actions.columns:
        actor_mask = match.actions["type_name"].isin(carrier_action_types)
    elif "type_id" in match.actions.columns:
        from silly_kicks.spadl.config import actiontypes

        type_ids = {i for i, name in enumerate(actiontypes) if name in carrier_action_types}
        actor_mask = match.actions["type_id"].isin(type_ids)
    else:
        actor_mask = pd.Series(True, index=match.actions.index)

    filtered_actions = match.actions[actor_mask]

    # Merge carrier (one row per frame) with links to get carrier at action time
    # links has frame_id but not period_id, so merge on frame_id only
    merged = links.merge(
        carrier[["frame_id", "ball_carrier_player_id"]],
        on="frame_id",
        how="inner",
    )
    merged = merged.merge(
        filtered_actions[["action_id", "player_id"]].rename(columns={"player_id": "true_carrier"}),
        on="action_id",
        how="inner",
    )

    if merged.empty:
        return 0.0, 0.0

    # Cast to string to avoid dtype mismatch (Int64 vs int64 vs object)
    accuracy = (merged["ball_carrier_player_id"].astype(str) == merged["true_carrier"].astype(str)).mean()

    # Carrier switch rate diagnostic
    carrier_sorted = carrier.sort_values(["period_id", "frame_id"])
    switches = (carrier_sorted["ball_carrier_player_id"] != carrier_sorted["ball_carrier_player_id"].shift()).sum()
    total_seconds = frames["time_seconds"].max() - frames["time_seconds"].min()
    switches_per_min = (switches / max(total_seconds, 1)) * 60

    del frames  # Release memory after use

    return float(accuracy), float(switches_per_min)


def run_stage1(
    dataset: CalibrationDataset,
    *,
    n_trials: int = 100,
    n_workers: int = 4,
    seed: int = 42,
) -> None:
    """Stage 1: Optimize carrier accuracy via Optuna TPE."""
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    import optuna

    storage = f"sqlite:///{OUTPUT_DIR / 'tc3_stage1.db'}"
    study = optuna.create_study(
        study_name="tc3_stage1_carrier",
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        storage=storage,
        load_if_exists=True,
    )

    # Warm-start: enqueue current defaults as first trial
    study.enqueue_trial(
        {
            "tolerance_m": 3.0,
            "beta": 0.5,
            "gamma": 1.0,
        }
    )

    def objective(trial: optuna.Trial) -> float:
        tolerance_m = trial.suggest_float("tolerance_m", 1.0, 8.0)
        beta = trial.suggest_float("beta", 0.0, 2.0)
        gamma = trial.suggest_float("gamma", 0.0, 3.0)

        fn = partial(
            _compute_carrier_accuracy_for_match,
            tolerance_m=tolerance_m,
            beta=beta,
            gamma=gamma,
        )

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            results = list(pool.map(fn, dataset.matches))

        accuracies = [r[0] for r in results]
        switch_rates = [r[1] for r in results]

        # Per-provider averaging
        provider_accs: dict[str, list[float]] = {}
        for m, acc in zip(dataset.matches, accuracies):
            provider_accs.setdefault(m.provider, []).append(acc)
        mean_acc = np.mean([np.mean(accs) for accs in provider_accs.values()])

        trial.set_user_attr(
            "per_match_accuracy",
            {m.match_id: a for m, a in zip(dataset.matches, accuracies)},
        )
        trial.set_user_attr("mean_switch_rate", float(np.mean(switch_rates)))
        trial.set_user_attr(
            "per_provider_accuracy",
            {p: float(np.mean(a)) for p, a in provider_accs.items()},
        )

        mean_switch = np.mean(switch_rates)
        if mean_switch < 15:
            logger.warning(
                "Trial %d: low switch rate %.1f/min (literature: 15-25). gamma=%.2f may be too sticky.",
                trial.number,
                mean_switch,
                gamma,
            )

        return float(mean_acc)

    remaining = n_trials - len(study.trials)
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)

    best = study.best_trial
    results_dict = {
        "best_params": best.params,
        "best_accuracy": best.value,
        "best_switch_rate": best.user_attrs.get("mean_switch_rate"),
        "per_provider_accuracy": best.user_attrs.get("per_provider_accuracy"),
        "n_trials": len(study.trials),
        "all_trials": [
            {
                "number": t.number,
                "params": t.params,
                "value": t.value,
                "switch_rate": t.user_attrs.get("mean_switch_rate"),
            }
            for t in study.trials
        ],
    }
    (OUTPUT_DIR / "stage1_results.json").write_text(json.dumps(results_dict, indent=2))
    logger.info(
        "Stage 1 complete: best accuracy=%.4f, params=%s",
        best.value,
        best.params,
    )


# ── Stage 2: VAEP Brier ─────────────────────────────────────────────────

_SPADL_FEATURES = [
    "type_id",
    "bodypart_id",
    "result_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
]

_TRACKING_FEATURES = [
    "nearest_defender_distance",
    "actor_speed",
    "receiver_zone_density",
    "defenders_in_triangle_to_goal",
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    "pressure_on_actor__andrienko_oval",
    "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    "pitch_control_at_ball__spearman",
    "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    "defensive_line_x",
    "back_line_high_x",
    "compactness_x",
    "lateral_width",
    "max_lateral_gap",
    "back_n_count",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    "team_shape_centroid_x_attacking",
    "team_shape_centroid_y_attacking",
    "team_shape_convex_hull_area_attacking",
    "team_shape_team_length_attacking",
    "team_shape_team_width_attacking",
    "team_shape_stretch_index_attacking",
    "team_shape_centroid_x_defending",
    "team_shape_centroid_y_defending",
    "team_shape_convex_hull_area_defending",
    "team_shape_team_length_defending",
    "team_shape_team_width_defending",
    "team_shape_stretch_index_defending",
    "das_team",
    "das_opponent",
    "das_diff",
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    "n_blocked_receivers",
    "n_potential_receivers",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    "sync_score_min",
    "sync_score_mean",
    "sync_score_high_quality_frac",
]

ALL_FEATURES = _SPADL_FEATURES + _TRACKING_FEATURES

# Deliberate penalty above any reasonable Brier (scoring ~1-3% → optimal ~0.01-0.03)
PENALTY_BRIER = 0.25
VARIANCE_GATE_RATIO = 0.1  # H1: 10% of default variance


def _enrich_match_worker(args: tuple) -> pd.DataFrame:
    """Worker function for ThreadPoolExecutor — module-level for clarity."""
    match, xt, carrier_params, trial_params = args
    frames = match.load_frames()
    result = _enrich_match_with_params(
        actions=match.actions.copy(),
        frames=frames,
        xt=xt,
        home_team_id=match.home_team_id,
        match_id_native=match.match_id,
        data_source=match.provider,
        carrier_tolerance_m=carrier_params["tolerance_m"],
        carrier_beta=carrier_params["beta"],
        carrier_gamma=carrier_params["gamma"],
        k3=trial_params["k3"],
        pre_seconds=trial_params["pre_seconds"],
        min_displacement_m=trial_params["min_displacement_m"],
    )
    del frames
    return result


def _invariant_worker(args: tuple) -> tuple[str, pd.DataFrame, pd.DataFrame]:
    """Compute invariant enrichment for a single match and cache to parquet.

    Returns (match_id, base_actions, links).
    """
    match, xt, carrier_params = args
    cache_dir = CACHE_DIR / match.provider / match.match_id
    base_path = cache_dir / "enriched_base.parquet"
    links_path = cache_dir / "links.parquet"

    if base_path.exists() and links_path.exists():
        logger.info("Invariant cache hit: %s/%s", match.provider, match.match_id)
        return (
            match.match_id,
            pd.read_parquet(base_path),
            pd.read_parquet(links_path),
        )

    logger.info("Computing invariant enrichment: %s/%s", match.provider, match.match_id)
    frames = match.load_frames()
    base_actions, links = _enrich_match_invariant(
        actions=match.actions.copy(),
        frames=frames,
        xt=xt,
        home_team_id=match.home_team_id,
        match_id_native=match.match_id,
        data_source=match.provider,
        carrier_tolerance_m=carrier_params["tolerance_m"],
        carrier_beta=carrier_params["beta"],
        carrier_gamma=carrier_params["gamma"],
    )
    del frames

    cache_dir.mkdir(parents=True, exist_ok=True)
    base_actions.to_parquet(base_path, index=False)
    links.to_parquet(links_path, index=False)
    logger.info("Cached invariant enrichment: %s/%s", match.provider, match.match_id)

    return match.match_id, base_actions, links


def _patch_worker(args: tuple) -> pd.DataFrame:
    """Apply trial-dependent columns to a cached base enrichment."""
    match, base_actions, links, trial_params = args
    frames = match.load_frames()
    result = _patch_trial_columns(
        base_actions=base_actions,
        frames=frames,
        links=links,
        home_team_id=match.home_team_id,
        k3=trial_params["k3"],
        pre_seconds=trial_params["pre_seconds"],
        min_displacement_m=trial_params["min_displacement_m"],
    )
    del frames
    return result


def _compute_provider_brier(
    enriched_actions: list[pd.DataFrame],
    matches: list[MatchData],
    vaep_labels: dict[str, pd.DataFrame],
    default_feature_variances: dict[str, float] | None,
    seed: int = 42,
) -> tuple[float, dict[str, float], dict[str, float] | None]:
    """Compute per-provider match-stratified CV Brier scores.

    Returns:
        (mean_brier, per_provider_brier, feature_importances_or_None)
    """
    import xgboost as xgb
    from sklearn.metrics import brier_score_loss
    from sklearn.model_selection import GroupKFold

    all_X: list[pd.DataFrame] = []
    all_y_scores: list[np.ndarray] = []
    all_match_ids: list[str] = []
    all_providers: list[str] = []

    for enriched, match in zip(enriched_actions, matches):
        labels = vaep_labels.get(match.match_id)
        if labels is None:
            continue

        if len(enriched) != len(labels):
            msg = f"Row count mismatch for {match.match_id}: enriched={len(enriched)}, labels={len(labels)}"
            raise ValueError(msg)
        features = enriched[ALL_FEATURES].copy()
        features = features.fillna(0)
        all_X.append(features)
        all_y_scores.append(labels["scores"].values)
        n_rows = len(features)
        all_match_ids.extend([match.match_id] * n_rows)
        all_providers.extend([match.provider] * n_rows)

    if not all_X:
        return PENALTY_BRIER, {}, None

    X = pd.concat(all_X, ignore_index=True)
    y = np.concatenate(all_y_scores)
    match_ids_arr = np.array(all_match_ids)
    providers_arr = np.array(all_providers)

    # H1: Variance sanity gate
    if default_feature_variances is not None:
        optimized_cols = [
            "pressure_on_actor__link_zones",
            "n_off_ball_runners_pre_window",
            "max_off_ball_run_displacement_pre_window",
            "mean_off_ball_run_speed_pre_window",
            "n_off_ball_runners_toward_goal_pre_window",
        ]
        for col in optimized_cols:
            if col in X.columns and col in default_feature_variances:
                current_var = X[col].var()
                default_var = default_feature_variances[col]
                if default_var > 0 and current_var / default_var < VARIANCE_GATE_RATIO:
                    logger.warning(
                        "H1 gate: %s variance %.6f < 10%% of default %.6f — penalty Brier",
                        col,
                        current_var,
                        default_var,
                    )
                    return PENALTY_BRIER, {}, None

    # Per-provider CV Brier
    provider_briers: dict[str, float] = {}
    feature_importances: dict[str, float] = {}

    for provider in PROVIDERS:
        mask = providers_arr == provider
        if mask.sum() == 0:
            continue

        X_prov = X[mask].reset_index(drop=True)
        y_prov = y[mask]
        mids_prov = match_ids_arr[mask]

        unique_matches = np.unique(mids_prov)
        n_matches = len(unique_matches)

        if n_matches <= 7:
            n_splits = n_matches
        else:
            n_splits = 5

        gkf = GroupKFold(n_splits=n_splits)
        fold_briers: list[float] = []
        fold_importances: list[np.ndarray] = []

        for train_idx, test_idx in gkf.split(X_prov, y_prov, groups=mids_prov):
            X_train = X_prov.iloc[train_idx]
            X_test = X_prov.iloc[test_idx]
            y_train, y_test = y_prov[train_idx], y_prov[test_idx]

            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=seed,
                verbosity=0,
            )
            model.fit(X_train, y_train)
            probs = model.predict_proba(X_test)[:, 1]
            fold_briers.append(brier_score_loss(y_test, probs))
            if hasattr(model, "feature_importances_"):
                fold_importances.append(model.feature_importances_)

        provider_briers[provider] = float(np.mean(fold_briers))

        if fold_importances:
            avg_imp = np.mean(fold_importances, axis=0)
            for feat, imp in zip(ALL_FEATURES, avg_imp):
                feature_importances[feat] = feature_importances.get(feat, 0) + imp

    if not provider_briers:
        return PENALTY_BRIER, {}, None

    # Average feature importances across providers (not additive)
    n_providers = len(provider_briers)
    if n_providers > 0 and feature_importances:
        feature_importances = {k: v / n_providers for k, v in feature_importances.items()}

    # Equal provider weight
    mean_brier = float(np.mean(list(provider_briers.values())))
    return mean_brier, provider_briers, feature_importances


def run_stage2(
    dataset: CalibrationDataset,
    *,
    n_trials: int = 100,
    n_workers: int = 2,
    seed: int = 42,
) -> None:
    """Stage 2: Optimize k3 + off-ball-runs via augmented VAEP Brier.

    Two-phase approach for efficiency:
      Phase A (once): compute invariant enrichment for all 78 matches and cache
        to parquet.  Steps 4b (link_zones, k3) and 9 (off-ball, pre_seconds +
        min_displacement_m) are skipped — their columns set to NaN placeholders.
      Phase B (per trial): load cached base + links, run ONLY steps 4b and 9
        with the trial's candidate params, then evaluate VAEP Brier.

    This avoids re-running pitch control (x3), DAS, GK influence, cover
    shadows, team shape, defensive line, etc. on every trial (~95% of wall
    time).
    """
    import time
    from concurrent.futures import ThreadPoolExecutor

    import optuna

    stage1_path = OUTPUT_DIR / "stage1_results.json"
    if not stage1_path.exists():
        logger.error("Stage 1 results not found at %s — run stage 1 first", stage1_path)
        sys.exit(1)

    stage1 = json.loads(stage1_path.read_text())
    carrier_params = stage1["best_params"]
    logger.info("Using Stage 1 carrier params: %s", carrier_params)

    # ── Phase A: Invariant enrichment (cached) ───────────────────────────
    logger.info("Phase A: Computing/loading invariant enrichment for %d matches...", len(dataset.matches))
    t0 = time.monotonic()

    invariant_args = [(m, dataset.xt, carrier_params) for m in dataset.matches]
    base_cache: dict[str, pd.DataFrame] = {}  # match_id → base_actions
    links_cache: dict[str, pd.DataFrame] = {}  # match_id → links

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for match_id, base_actions, links in pool.map(_invariant_worker, invariant_args):
            base_cache[match_id] = base_actions
            links_cache[match_id] = links

    logger.info(
        "Phase A complete: %d matches cached in %.1f min",
        len(base_cache),
        (time.monotonic() - t0) / 60,
    )

    # ── Default feature variances for H1 gate ────────────────────────────
    variance_cache_path = OUTPUT_DIR / "default_variances.json"
    if variance_cache_path.exists():
        logger.info("Loading cached default feature variances from %s", variance_cache_path)
        default_variances = json.loads(variance_cache_path.read_text())
    else:
        logger.info("Computing default feature variances for H1 gate...")
        default_trial_params = {
            "k3": 1.0,
            "pre_seconds": 1.5,
            "min_displacement_m": 3.0,
        }
        default_patch_args = [
            (m, base_cache[m.match_id], links_cache[m.match_id], default_trial_params) for m in dataset.matches
        ]
        default_enriched: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for result in pool.map(_patch_worker, default_patch_args):
                default_enriched.append(result)

        default_features = pd.concat(
            [e[ALL_FEATURES] for e in default_enriched],
            ignore_index=True,
        ).fillna(0)
        default_variances = {col: float(default_features[col].var()) for col in ALL_FEATURES}
        variance_cache_path.write_text(json.dumps(default_variances, indent=2))
        logger.info("Cached default variances to %s", variance_cache_path)

    # ── Phase B: Optuna trial loop (patch only) ──────────────────────────
    storage = f"sqlite:///{OUTPUT_DIR / 'tc3_stage2.db'}"
    study = optuna.create_study(
        study_name="tc3_stage2_vaep_brier",
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        storage=storage,
        load_if_exists=True,
    )

    study.enqueue_trial(
        {
            "k3": 1.0,
            "pre_seconds": 1.5,
            "min_displacement_m": 3.0,
        }
    )

    def objective(trial: optuna.Trial) -> float:
        t_start = time.monotonic()
        k3 = trial.suggest_float("k3", 0.1, 5.0, log=True)
        pre_seconds = trial.suggest_float("pre_seconds", 0.5, 5.0)
        min_displacement_m = trial.suggest_float("min_displacement_m", 1.0, 8.0)

        trial_params = {
            "k3": k3,
            "pre_seconds": pre_seconds,
            "min_displacement_m": min_displacement_m,
        }

        patch_args = [(m, base_cache[m.match_id], links_cache[m.match_id], trial_params) for m in dataset.matches]

        enriched_actions: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for result in pool.map(_patch_worker, patch_args):
                enriched_actions.append(result)

        mean_brier, provider_briers, feat_importances = _compute_provider_brier(
            enriched_actions,
            dataset.matches,
            dataset.vaep_labels,
            default_variances,
            seed=seed,
        )

        trial.set_user_attr("per_provider_brier", provider_briers)
        if feat_importances:
            trial.set_user_attr("feature_importances", feat_importances)

        elapsed_min = (time.monotonic() - t_start) / 60
        logger.info(
            "Trial %d: Brier=%.6f, params=%s (%.1f min)",
            trial.number,
            mean_brier,
            trial_params,
            elapsed_min,
        )

        return mean_brier

    remaining = n_trials - len(study.trials)
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)

    best = study.best_trial
    results_dict = {
        "best_params": best.params,
        "best_brier": best.value,
        "carrier_params_from_stage1": carrier_params,
        "per_provider_brier": best.user_attrs.get("per_provider_brier"),
        "feature_importances": best.user_attrs.get("feature_importances"),
        "default_feature_variances": default_variances,
        "n_trials": len(study.trials),
        "all_trials": [
            {
                "number": t.number,
                "params": t.params,
                "value": t.value,
                "per_provider_brier": t.user_attrs.get("per_provider_brier"),
            }
            for t in study.trials
        ],
    }
    (OUTPUT_DIR / "stage2_results.json").write_text(json.dumps(results_dict, indent=2))
    logger.info(
        "Stage 2 complete: best Brier=%.6f, params=%s",
        best.value,
        best.params,
    )


# ── Phase 3: Post-hoc diagnostics ────────────────────────────────────────


def run_diagnostics(dataset: CalibrationDataset) -> None:
    """Phase 3: Post-hoc diagnostics and TF-25 gate evaluation."""
    from concurrent.futures import ThreadPoolExecutor

    from silly_kicks.tracking import (
        LinkParams,
        add_pressure_on_actor,
        link_actions_to_frames,
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    stage1 = json.loads((OUTPUT_DIR / "stage1_results.json").read_text())
    stage2 = json.loads((OUTPUT_DIR / "stage2_results.json").read_text())

    carrier_params = stage1["best_params"]
    best_params = stage2["best_params"]
    global_brier = stage2["best_brier"]

    diagnostics: dict[str, Any] = {
        "carrier_params": carrier_params,
        "stage2_params": best_params,
        "global_brier": global_brier,
    }

    # 1. Per-provider re-evaluation at global optimum (parallel + cached)
    logger.info("Running per-provider re-evaluation at global optimum...")
    optimum_worker_args = [(m, dataset.xt, carrier_params, best_params) for m in dataset.matches]
    optimum_enriched: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(_enrich_match_worker, optimum_worker_args):
            optimum_enriched.append(result)

    per_provider_results: dict[str, dict] = {}
    match_to_enriched = dict(zip([m.match_id for m in dataset.matches], optimum_enriched))
    for provider in PROVIDERS:
        provider_matches = dataset.matches_by_provider(provider)
        if not provider_matches:
            continue

        enriched = [match_to_enriched[m.match_id] for m in provider_matches]
        brier, _, _feat_imp = _compute_provider_brier(
            enriched,
            provider_matches,
            dataset.vaep_labels,
            None,
        )
        per_provider_results[provider] = {
            "brier_at_global_optimum": brier,
            "n_matches": len(provider_matches),
        }

    diagnostics["per_provider_at_global_optimum"] = per_provider_results

    # 2. k3 1D sensitivity curve per provider
    # Only k3 changes → only re-run add_pressure_on_actor(link_zones)
    logger.info("Running k3 sensitivity scan (pressure-only re-evaluation)...")
    k3_values = np.logspace(np.log10(0.1), np.log10(5.0), 20).tolist()
    k3_sensitivity: dict[str, list[dict]] = {}

    # Pre-resolve identity + links once per match (load frames temporarily)
    match_links: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    match_frames_cache: dict[str, pd.DataFrame] = {}
    for m in dataset.matches:
        try:
            frames = m.load_frames()
            actions_resolved = _resolve_enrichment_identity(
                m.actions.copy(),
                provider=m.provider,
                match_id_native=m.match_id,
            )
            links, _ = link_actions_to_frames(actions_resolved, frames)
            match_links[m.match_id] = (actions_resolved, links)
            match_frames_cache[m.match_id] = frames
        except Exception:
            logger.debug(
                "Pre-resolve failed for %s — skipping in k3 scan",
                m.match_id,
                exc_info=True,
            )

    for provider in PROVIDERS:
        provider_matches = dataset.matches_by_provider(provider)
        if not provider_matches:
            continue

        base_enriched = [match_to_enriched[m.match_id] for m in provider_matches]

        provider_curve: list[dict] = []
        for k3_val in k3_values:
            lp = LinkParams(k3=k3_val)
            patched: list[pd.DataFrame] = []
            for m, base_e in zip(provider_matches, base_enriched):
                if m.match_id not in match_links:
                    continue
                try:
                    actions_pre, links_pre = match_links[m.match_id]
                    pressure_result = add_pressure_on_actor(
                        actions_pre.copy(),
                        match_frames_cache[m.match_id],
                        links=links_pre,
                        methods=("link_zones",),
                        params_per_method={"link_zones": lp},
                    )
                    e = base_e.copy()
                    e["pressure_on_actor__link_zones"] = pressure_result["pressure_on_actor__link_zones"].values
                    patched.append(e)
                except Exception:
                    logger.debug(
                        "k3=%.3f failed for %s",
                        k3_val,
                        m.match_id,
                        exc_info=True,
                    )

            if patched:
                brier, _, _ = _compute_provider_brier(
                    patched,
                    provider_matches[: len(patched)],
                    dataset.vaep_labels,
                    None,
                )
                provider_curve.append({"k3": k3_val, "brier": brier})

        k3_sensitivity[provider] = provider_curve
        logger.info("k3 sensitivity for %s: %d points", provider, len(provider_curve))

    diagnostics["k3_sensitivity"] = k3_sensitivity

    # 3. TF-25 gate decision
    logger.info("Evaluating TF-25 gate criterion...")
    tf25_evaluation: dict[str, dict] = {}
    for provider, curve in k3_sensitivity.items():
        if not curve:
            continue
        best_point = min(curve, key=lambda p: p["brier"])
        global_point = min(curve, key=lambda p: abs(p["k3"] - best_params["k3"]))

        gap = global_point["brier"] - best_point["brier"]
        provider_brier = per_provider_results.get(provider, {}).get("brier_at_global_optimum", 0)
        n_matches = per_provider_results.get(provider, {}).get("n_matches", 1)
        se_estimate = provider_brier / max(np.sqrt(n_matches), 1)

        tf25_evaluation[provider] = {
            "global_brier": global_point["brier"],
            "provider_best_brier": best_point["brier"],
            "provider_best_k3": best_point["k3"],
            "gap": gap,
            "cv_se_estimate": se_estimate,
            "needs_own_k3": gap > se_estimate,
        }

    diagnostics["tf25_gate"] = tf25_evaluation
    needs_tf25 = any(v["needs_own_k3"] for v in tf25_evaluation.values())
    diagnostics["tf25_recommendation"] = (
        "TF-25 RECOMMENDED: at least one provider shows gap > CV SE"
        if needs_tf25
        else "TF-25 NOT NEEDED: global optimum generalizes across all providers"
    )

    # 4. Geometry sensitivity scan
    # NOTE: This is a "does this parameter do anything?" sensitivity check,
    # NOT an optimization. Measures pressure variance/mean, not Brier.
    # If the scan shows a parameter has meaningful effect on pressure output,
    # a follow-up Optuna sweep over geometry params can be done separately.
    logger.info("Running geometry sensitivity scan (r_hoz, r_lz, r_hz)...")
    geometry_results: dict[str, list[dict]] = {}

    subset_matches = [m for m in dataset.matches[:10] if m.match_id in match_links]
    subset_links = [(m, match_links[m.match_id]) for m in subset_matches]

    for geom_param, _default_val, scan_range in [
        ("r_hoz", 4.0, np.linspace(2.0, 8.0, 10)),
        ("r_lz", 3.0, np.linspace(1.0, 6.0, 10)),
        ("r_hz", 2.0, np.linspace(0.5, 4.0, 10)),
    ]:
        curve: list[dict] = []
        for val in scan_range:
            kwargs: dict[str, float] = {"k3": best_params["k3"]}
            kwargs[geom_param] = val
            lp = LinkParams(**kwargs)

            enriched_list = []
            for m, (actions_pre, links_pre) in subset_links:
                try:
                    actions_out = add_pressure_on_actor(
                        actions_pre.copy(),
                        match_frames_cache[m.match_id],
                        links=links_pre,
                        methods=("link_zones",),
                        params_per_method={"link_zones": lp},
                    )
                    enriched_list.append(actions_out)
                except Exception:
                    logger.debug(
                        "Geometry %s=%.3f failed for %s",
                        geom_param,
                        val,
                        m.match_id,
                        exc_info=True,
                    )

            if enriched_list:
                pressures = pd.concat(
                    [e["pressure_on_actor__link_zones"] for e in enriched_list],
                )
                curve.append(
                    {
                        geom_param: float(val),
                        "pressure_variance": float(pressures.var()),
                        "pressure_mean": float(pressures.mean()),
                    }
                )

        geometry_results[geom_param] = curve
        logger.info("Geometry scan for %s: %d points", geom_param, len(curve))

    diagnostics["geometry_sensitivity"] = geometry_results

    # Release frames cache — no longer needed
    del match_frames_cache

    # 5. Feature importance comparison
    diagnostics["feature_importance_comparison"] = {
        "at_optimum": stage2.get("feature_importances", {}),
    }

    (OUTPUT_DIR / "per_provider_diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2, default=str),
    )

    _write_summary(stage1, stage2, diagnostics)
    logger.info("Diagnostics complete — results at %s", OUTPUT_DIR)


def _write_summary(
    stage1: dict,
    stage2: dict,
    diagnostics: dict,
) -> None:
    """Generate human-readable SUMMARY.md."""
    lines = [
        "# TC-3 Calibration Results\n",
        f"**Date**: {pd.Timestamp.now().strftime('%Y-%m-%d')}\n",
        "",
        "## Stage 1: Carrier Accuracy",
        "",
        f"**Best accuracy**: {stage1['best_accuracy']:.4f}",
        (
            f"**Best params**: `tolerance_m={stage1['best_params']['tolerance_m']:.3f}`, "
            f"`beta={stage1['best_params']['beta']:.3f}`, "
            f"`gamma={stage1['best_params']['gamma']:.3f}`"
        ),
        f"**Mean switch rate**: {stage1.get('best_switch_rate', 'N/A')} switches/min",
        f"**Trials**: {stage1['n_trials']}",
        "",
        "### Per-provider accuracy",
        "",
    ]
    for provider, acc in (stage1.get("per_provider_accuracy") or {}).items():
        lines.append(f"- {provider}: {acc:.4f}")

    lines.extend(
        [
            "",
            "## Stage 2: VAEP Brier",
            "",
            f"**Best Brier**: {stage2['best_brier']:.6f}",
            (
                f"**Best params**: `k3={stage2['best_params']['k3']:.3f}`, "
                f"`pre_seconds={stage2['best_params']['pre_seconds']:.3f}`, "
                f"`min_displacement_m={stage2['best_params']['min_displacement_m']:.3f}`"
            ),
            f"**Trials**: {stage2['n_trials']}",
            "",
            "### Per-provider Brier",
            "",
        ]
    )
    for provider, brier in (stage2.get("per_provider_brier") or {}).items():
        lines.append(f"- {provider}: {brier:.6f}")

    lines.extend(
        [
            "",
            "## TF-25 Gate",
            "",
            f"**Recommendation**: {diagnostics.get('tf25_recommendation', 'N/A')}",
            "",
        ]
    )
    for provider, gate in (diagnostics.get("tf25_gate") or {}).items():
        lines.append(
            f"- {provider}: gap={gate['gap']:.6f}, SE={gate['cv_se_estimate']:.6f}, needs_own_k3={gate['needs_own_k3']}"
        )

    lines.extend(
        [
            "",
            "## Provenance",
            "",
            f"Optuna-calibrated against lakehouse production data on {pd.Timestamp.now().strftime('%Y-%m-%d')}.",
            f"Providers: {', '.join(PROVIDERS)}.",
            f"Stage 1 trials: {stage1['n_trials']}, Stage 2 trials: {stage2['n_trials']}.",
            "",
            "## Recommended Default Updates (silly-kicks PR)",
            "",
            "```python",
            "# infer_ball_carrier defaults",
            f"tolerance_m = {stage1['best_params']['tolerance_m']:.3f}",
            f"beta = {stage1['best_params']['beta']:.3f}",
            f"gamma = {stage1['best_params']['gamma']:.3f}",
            "",
            "# LinkParams.k3",
            f"k3 = {stage2['best_params']['k3']:.3f}",
            "",
            "# add_off_ball_runs / add_off_ball_context defaults",
            f"pre_seconds = {stage2['best_params']['pre_seconds']:.3f}",
            f"min_displacement_m = {stage2['best_params']['min_displacement_m']:.3f}",
            "```",
        ]
    )

    (OUTPUT_DIR / "SUMMARY.md").write_text("\n".join(lines))


# ── CLI ──────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="TC-3 Optuna Calibration Sweep")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["0", "1", "2", "diagnostics", "all"],
        help="Which stage to run",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=100,
        help="Number of Optuna trials per stage (default: 100)",
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=2,
        help="ThreadPoolExecutor workers (default: 2; each worker loads ~1 GB frames)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for TPE sampler + XGBoost (default: 42)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    dataset: CalibrationDataset | None = None

    if args.stage in ("0", "all"):
        dataset = run_phase0(args)
        logger.info("Phase 0 complete: %d total matches loaded", len(dataset.matches))

    if args.stage == "1" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_stage1(
            dataset,
            n_trials=args.n_trials,
            n_workers=args.n_workers,
            seed=args.seed,
        )

    if args.stage == "2" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_stage2(
            dataset,
            n_trials=args.n_trials,
            n_workers=args.n_workers,
            seed=args.seed,
        )

    if args.stage == "diagnostics" or args.stage == "all":
        if dataset is None:
            dataset = run_phase0(args)
        run_diagnostics(dataset)


if __name__ == "__main__":
    main()
