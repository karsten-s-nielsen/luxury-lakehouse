"""Snapshot REAL prototype data for the GK-redesign v3 tab mockups.

Source of truth (transient, scoped v4 run — 2 matches x 2 halves per tracking
provider): ``soccer_analytics.bronze.spadl_action_context``. The gold mart is
empty and the staging view is schema-stale — do NOT point this at either.

Writes parquet snapshots to ./data/ so the render script needs no warehouse
access. Re-run when the recompute lands (row volume will grow ~100x).

Run:  uv run python docs/ui-cycles/gk-redesign/extract_proto_data.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from databricks import sql

OUT = Path(__file__).parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

AC = "soccer_analytics.bronze.spadl_action_context"
SPADL = "soccer_analytics.bronze.spadl_actions"
DIM_PLAYERS = "soccer_analytics.dev_gold.dim_players"
DIM_MATCHES = "soccer_analytics.dev_gold.dim_matches"
FRAMES = "soccer_analytics.dev_gold.fct_tracking_frames"

TRACKING = "('gradientsports','idsse','skillcorner','metrica')"


def q(cur, sql_text: str) -> pd.DataFrame:
    cur.execute(sql_text)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def main() -> None:
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cur = conn.cursor()

    # 1. GK distributions with the xT-GK family (GK-distribution rows only by design)
    dist = q(cur, f"""
        SELECT data_source, match_id, action_id, player_id, team_id, type_name, game_state,
               start_x, start_y, end_x, end_y,
               xt_gk, xt_gk_possession, xt_gk_counter, xt_gk_direct, xt_gk_high_press,
               xt_gk_low_block, xt_gk_base, xt_gk_pev, xt_gk_rav, xt_gk_dzv, xt_gk_pressure,
               gk_completion, pressure_on_actor__andrienko_oval
        FROM {AC}
        WHERE xt_gk IS NOT NULL AND data_source IN {TRACKING}
        LIMIT 5000
    """)
    dist.to_parquet(OUT / "distributions.parquet", index=False)
    print("distributions:", len(dist), "rows")

    # 2. Shots faced, with GK geometry + ghost. Outcome joined pandas-side: bronze
    #    spadl_actions.game_id is hash_native_id_to_bigint(native) (ADR-016) — inlined
    #    here (sha256[:15] hex -> int); both bare and provider-prefixed inputs tried.
    shots = q(cur, f"""
        SELECT data_source, match_id, action_id, player_id, team_id,
               type_name, game_state, start_x, start_y, frame_id, period_id,
               pre_shot_gk_x, pre_shot_gk_y, pre_shot_gk_distance_to_goal,
               pre_shot_gk_distance_to_shot, pre_shot_gk_angle_to_shot_trajectory,
               pre_shot_gk_angle_off_goal_line,
               ghost_gk_x, ghost_gk_y, ghost_gk_density_spread,
               defending_gk_player_id_native, defensive_line_x
        FROM {AC}
        WHERE pre_shot_gk_x IS NOT NULL AND data_source IN {TRACKING}
        LIMIT 2000
    """)
    import hashlib

    def _hash_id(value: str) -> int:
        return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:15], 16)

    candidates: dict[int, str] = {}
    for ds, mid in shots[["data_source", "match_id"]].drop_duplicates().itertuples(index=False):
        candidates[_hash_id(mid)] = mid
        candidates[_hash_id(f"{ds}_{mid}")] = mid
    game_ids = ",".join(str(g) for g in candidates)
    res = q(cur, f"SELECT game_id, action_id, result_id, type_id FROM {SPADL} "
                 f"WHERE game_id IN ({game_ids})")
    res["match_id"] = res["game_id"].map(candidates)
    shots = shots.merge(res[["match_id", "action_id", "result_id", "type_id"]],
                        on=["match_id", "action_id"], how="left")
    shots.to_parquet(OUT / "shots.parquet", index=False)
    print("shots:", len(shots), "rows; result_id non-null:", int(shots["result_id"].notna().sum()))

    # 3. Per-action GK defensive context (every tracking row): closing times, reach, share
    ctx = q(cur, f"""
        SELECT data_source, match_id, action_id, game_state, defensive_line_x,
               defending_gk_player_id_native,
               gk_pitch_control_share_weighted, gk_reachable_area_m2,
               gk_closing_time_mean_s__six_yard_box, gk_closing_time_min_s__six_yard_box,
               gk_closing_time_mean_s__near_post, gk_closing_time_min_s__near_post,
               gk_closing_time_mean_s__far_post, gk_closing_time_min_s__far_post,
               ghost_gk_x, ghost_gk_y, ghost_gk_density_spread
        FROM {AC}
        WHERE ghost_gk_x IS NOT NULL AND data_source IN {TRACKING}
        LIMIT 30000
    """)
    ctx.to_parquet(OUT / "context.parquet", index=False)
    print("context:", len(ctx), "rows")

    # 4. Display names (raw IDs never reach the user — even in a prototype)
    names = q(cur, f"SELECT provider, native_player_id, player_display_name FROM {DIM_PLAYERS} "
                   f"WHERE provider IN {TRACKING} LIMIT 5000")
    names.to_parquet(OUT / "names.parquet", index=False)
    print("names:", len(names), "rows")

    # 5. Scene: the most interesting real shot (largest actual-vs-ghost deviation, non-metrica),
    #    plus all player positions at its linked frame for context dots.
    #    COORDINATE RECONCILIATION (real-data finding, 2026-06-11): ghost_gk_* is canonical
    #    defended-goal-at-x~0; pre_shot_gk_* is FRAME orientation (defended end varies by
    #    team/period). Prototype heuristic: mirror pre_shot to the ghost's end when they sit
    #    on opposite halves. Flagged for the spec — the page needs the authoritative transform.
    s = shots[(shots["data_source"] != "metrica") & shots["ghost_gk_x"].notna()].copy()
    for c in ("pre_shot_gk_x", "pre_shot_gk_y", "ghost_gk_x", "ghost_gk_y"):
        s[c] = pd.to_numeric(s[c])
    flip = (s.pre_shot_gk_x - s.ghost_gk_x).abs() > 52.5
    s["actual_x"] = np.where(flip, 105.0 - s.pre_shot_gk_x, s.pre_shot_gk_x)
    s["actual_y"] = np.where(flip, 68.0 - s.pre_shot_gk_y, s.pre_shot_gk_y)
    s["dev"] = np.hypot(s.actual_x - s.ghost_gk_x, s.actual_y - s.ghost_gk_y)
    s = s[s["frame_id"].notna()]
    # Largest PLAUSIBLE deviation: rows with dev > ~8 m in this sample carry sentinel-ish
    # geometry (pre_shot_gk_x == 105.0 exactly, angle_off_goal_line == -pi/2 exactly) —
    # pick the hero scene from the credible range instead.
    plausible = s[(s["dev"] > 1.0) & (s["dev"] < 8.0)]
    scene = (plausible if len(plausible) else s).sort_values("dev", ascending=False).iloc[0]
    mk = q(cur, f"SELECT match_key, home_team_name, away_team_name FROM {DIM_MATCHES} "
                f"WHERE provider = '{scene.data_source}' AND native_match_id = '{scene.match_id}'")
    frame_players = pd.DataFrame()
    if len(mk):
        cur.execute(f"DESCRIBE {FRAMES}")
        fcols = [r[0] for r in cur.fetchall()]
        frame_col = "frame" if "frame" in fcols else "frame_id"
        period_col = "period" if "period" in fcols else "period_id"
        frame_players = q(cur, f"""
            SELECT * FROM {FRAMES}
            WHERE match_key = '{mk.iloc[0].match_key}'
              AND {period_col} = {int(scene.period_id)}
              AND {frame_col} = {int(scene.frame_id)}
            LIMIT 60
        """)
    frame_players.to_parquet(OUT / "scene_frame.parquet", index=False)
    scene_meta = {k: (v.item() if hasattr(v, "item") else v) for k, v in scene.to_dict().items()}
    label = scene.match_id
    if len(mk) and mk.iloc[0].home_team_name and mk.iloc[0].away_team_name:
        label = f"{mk.iloc[0].home_team_name} vs {mk.iloc[0].away_team_name}"
    scene_meta["match_label"] = f"{label} ({scene.data_source})"
    (OUT / "scene.json").write_text(json.dumps(scene_meta, default=str, indent=1), encoding="utf-8")
    print("scene:", scene.data_source, scene.match_id, "action", scene.action_id,
          f"dev={scene.dev:.1f}m; frame players:", len(frame_players))

    cur.close()
    conn.close()
    print("done ->", OUT)


if __name__ == "__main__":
    main()
