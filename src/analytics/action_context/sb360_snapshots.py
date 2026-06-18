"""Vectorized StatsBomb-360 freeze-frame -> snapshot conversion + home-team resolution.

Pure pandas/numpy (NO pyspark): both the production Spark path (cogroup UDF / `_run_sb360_enrichment`)
and the local hexagon (`enrich_batch`) import these, so there is a single impl and local mirrors
production. The snapshot builder replaces the per-row ``iterrows``+``json.loads`` loop that dominated
sb360 wall-time (~147s/match on serverless; ADR-058).

Output snapshot schema (identical to the legacy loop): ``action_id`` (int64), ``team_id`` (str),
``is_goalkeeper`` (bool), ``x`` (float64), ``y`` (float64).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

_SNAPSHOT_COLUMNS = ["action_id", "team_id", "is_goalkeeper", "x", "y"]


def build_sb360_snapshots(actions_df: pd.DataFrame, sb360_raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw ``bronze.statsbomb_360`` rows to the snapshot frame ``_enrich_sb360_match`` consumes.

    Vectorized equivalent of the legacy loop. ``event_uuid`` (``id``) maps to ``action_id`` via the
    action's ``original_event_id``; on a duplicated ``original_event_id`` the LAST action wins
    (``keep="last"``), matching the legacy ``dict(zip(...))`` semantics. ``team_id`` is the acting
    team when ``teammate`` else the opponent. ``location`` is a JSON-ish ``"[x, y]"`` STRING in bronze;
    malformed / single-value / unmapped rows are dropped (same skips as the loop).
    """
    if sb360_raw_df.empty or actions_df.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    ev = actions_df["original_event_id"].dropna()
    ev_to_action = pd.DataFrame(
        {
            "id": ev.astype(str).to_numpy(),
            "action_id": actions_df.loc[ev.index, "action_id"].to_numpy(),
            "acting_team": actions_df.loc[ev.index, "team_id"].astype(str).to_numpy(),
        }
    ).drop_duplicates("id", keep="last")  # MATCH the loop: dict(zip(...)) keeps the LAST action.

    teams = [str(t) for t in actions_df["team_id"].dropna().unique()]
    opp_of = {t: next((o for o in teams if o != t), t) for t in teams}

    df = sb360_raw_df.copy()
    df["id"] = df["id"].astype(str)
    df = df.merge(ev_to_action, on="id", how="inner")  # drops unmapped events
    if df.empty:
        return pd.DataFrame(columns=_SNAPSHOT_COLUMNS)

    teammate = df["teammate"].astype(bool).to_numpy()
    acting = df["acting_team"].to_numpy()
    opponent = np.array([opp_of.get(t, t) for t in acting], dtype=object)
    team_id = np.where(teammate, acting, opponent)

    # Vectorized "[x, y]" string parse; malformed -> NaN -> dropped (mirrors the loop's None/JSON/len skips).
    loc = df["location"].astype(str).str.strip().str.strip("[]")
    xy = loc.str.split(",", n=1, expand=True)
    x = pd.to_numeric(xy[0], errors="coerce")
    y = pd.to_numeric(xy[1], errors="coerce") if xy.shape[1] > 1 else pd.Series(np.nan, index=df.index)

    out = pd.DataFrame(
        {
            "action_id": df["action_id"].astype("int64").to_numpy(),
            "team_id": team_id,
            "is_goalkeeper": df["keeper"].astype(bool).to_numpy(),
            "x": x.to_numpy(dtype="float64"),
            "y": y.to_numpy(dtype="float64"),
        }
    )
    return out[out["x"].notna() & out["y"].notna()].reset_index(drop=True)


def resolve_home_team_id(actions_df: pd.DataFrame) -> str:
    """Home team id for sb360 orientation.

    ``home_team_id_native`` is the real home id (LL2 Path B, ``identifiers.py``; populated for all
    providers in ``spadl_conversion.py``) — the same form the tracking driver uses
    (``action_context.py`` idsse branch). The previous ``unique()[0]`` returned an arbitrary (often
    away) team, so orientation-aware enrichers (team_shape / defensive_line / line_break(ward) /
    shape_graph / gk_influence) were systematically wrong for ~half of matches (ADR-058). The
    sorted-unique fallback is deterministic and orientation-only.
    """
    if "home_team_id_native" in actions_df.columns:
        h = actions_df["home_team_id_native"].dropna()
        if not h.empty:
            return str(h.iloc[0])
    teams = sorted(str(t) for t in actions_df["team_id"].dropna().unique())
    return teams[0] if teams else "unknown"
