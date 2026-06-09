"""Provider bronze->frames converters (pure pandas/numpy/scipy).

COPIED VERBATIM (M4) from ``ingestion.tracking_context`` (idsse/metrica/skillcorner
+ the shared ``_derive_velocities_savgol`` helper) and ``ingestion.action_context``
(gradientsports). The legacy copies are left UNTOUCHED so they remain the differential
oracle; ``src/tests/action_context/test_convert_drift.py`` asserts the two copies stay
identical. De-duplicate only after the legacy pipelines retire.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

_IDSSE_CONSUMED_COLS = frozenset(
    {
        "ball_x",
        "timestamp",
        "ball_z",
        "player_id",
        "period",
        "match_id",
        "ball_s",
        "is_goalkeeper",
        "team_id",
        "frame_rate",
        "ball_y",
        "frame",
        "s",
        "y",
        "x",
        "ball_status",
    }
)
_METRICA_CONSUMED_COLS = frozenset(
    {
        "ball_x",
        "timestamp",
        "home_players",
        "away_players",
        "period",
        "gk_jersey_numbers",
        "frame_rate",
        "ball_y",
        "frame",
    }
)
_SKILLCORNER_CONSUMED_COLS = frozenset(
    {"ball_x", "timestamp", "team", "player_id", "period", "y", "x", "is_goalkeeper", "frame_rate", "ball_y", "frame"}
)
# SkillCorner bronze ``timestamp`` is the CONTINUOUS broadcast clock (2nd half = 45:00+, ET = 90:00+/
# 105:00+). silly-kicks 4.20.1 re-based SkillCorner SPADL ``time_seconds`` to PERIOD-RELATIVE
# (skillcorner.py ``_PERIOD_START_SECONDS``). The action↔frame linker matches action ``time_seconds``
# to frame ``time_seconds``, so the frames MUST share that period-relative base — otherwise 2nd-half+
# linkage collapses (the mirror image of the GS absolute-clock class; see ADR-040). Offsets mirror
# silly-kicks' nominal period starts exactly.
_SKILLCORNER_PERIOD_START_SECONDS: dict[int, float] = {1: 0.0, 2: 45 * 60.0, 3: 90 * 60.0, 4: 105 * 60.0, 5: 120 * 60.0}
_GS_FRAME_RATE = 30


def _derive_velocities_savgol(
    frames: pd.DataFrame,
    provider: str,
    frame_rate: int,
) -> None:
    """Derive vx/vy/speed via Savitzky-Golay smoothed differentiation (in-place).

    NOTE: silly-kicks uses a two-pass pipeline (smooth_frames → derive_velocities
    on smoothed positions). This helper applies a single SG derivative pass on raw
    positions — numerically slightly noisier but practically equivalent for
    well-formed data. Acceptable for v1; align with two-pass if velocity quality
    proves insufficient on SkillCorner 10fps data.

    Uses silly-kicks per-provider defaults from _provider_defaults_generated.py:
    - Metrica:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps
    - SkillCorner: sg_window_seconds=1.0, sg_poly_order=3 → window=11 at 10fps
    - Sportec:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps

    Ball velocity IS derived (silly-kicks groups by [period_id, is_ball, player_id]).

    Args:
        frames: Must have columns [player_id, is_ball, x, y] sorted by time
                within each player/ball group.
        provider: "metrica" or "skillcorner" — selects SG parameters.
        frame_rate: Tracking data frame rate (Hz).
    """
    from scipy.signal import savgol_filter

    # Per-provider SG defaults matching silly-kicks _provider_defaults_generated.py
    _sg_defaults: dict[str, tuple[float, int]] = {
        "metrica": (0.4, 3),  # sg_window_seconds, sg_poly_order
        "skillcorner": (1.0, 3),
        "sportec": (0.4, 3),  # IDSSE uses convert_to_frames, but fallback
    }
    sg_window_s, polyorder = _sg_defaults.get(provider, (0.4, 3))

    dt = 1.0 / frame_rate
    window = max(round(sg_window_s * frame_rate) | 1, polyorder + 2)
    if window % 2 == 0:
        window += 1

    # Initialize with NaN (not 0.0 — 0.0 implies stationary, NaN implies unknown)
    frames["vx"] = np.nan
    frames["vy"] = np.nan

    # Group by (period_id, is_ball, player_id) — matching silly-kicks pipeline.
    # Ball rows ARE processed (pid=None, is_ball=True).
    for _key, idx in frames.groupby(["period_id", "is_ball", "player_id"]).groups.items():
        group = frames.loc[idx]
        x_raw = group["x"].to_numpy(dtype=float)
        y_raw = group["y"].to_numpy(dtype=float)
        nan_mask = np.isnan(x_raw) | np.isnan(y_raw)

        if nan_mask.all():
            continue

        # Short groups: np.gradient fallback (matches silly-kicks _velocity.py)
        if len(group) < window:
            x_safe = np.where(nan_mask, 0.0, x_raw)
            y_safe = np.where(nan_mask, 0.0, y_raw)
            vx_g = np.gradient(x_safe, dt)
            vy_g = np.gradient(y_safe, dt)
            vx_g[nan_mask] = np.nan
            vy_g[nan_mask] = np.nan
            frames.loc[idx, "vx"] = vx_g
            frames.loc[idx, "vy"] = vy_g
            continue

        # Interpolate NaN positions before SG filtering (linear interp across gaps),
        # then re-mask original NaN positions back to NaN in the output.
        # Matches silly-kicks derive_velocities (_velocity.py:84-124).
        valid_idx = np.where(~nan_mask)[0]
        x_filled = np.interp(np.arange(len(group)), valid_idx, x_raw[~nan_mask])
        y_filled = np.interp(np.arange(len(group)), valid_idx, y_raw[~nan_mask])

        vx_g = np.asarray(savgol_filter(x_filled, window, polyorder, deriv=1, delta=dt), dtype=float)
        vy_g = np.asarray(savgol_filter(y_filled, window, polyorder, deriv=1, delta=dt), dtype=float)
        vx_g[nan_mask] = np.nan
        vy_g[nan_mask] = np.nan

        frames.loc[idx, "vx"] = vx_g
        frames.loc[idx, "vy"] = vy_g

    # Compute speed from velocity (matches silly-kicks derive_velocities output)
    frames["speed"] = np.sqrt(frames["vx"] ** 2 + frames["vy"] ** 2)


def _bronze_idsse_to_sportec_input(trk_pdf: pd.DataFrame) -> pd.DataFrame:
    """Map bronze ``idsse_tracking`` columns to silly-kicks sportec input schema.

    Bronze ``idsse_tracking`` stores one row per player per frame with ball
    data denormalized as ``ball_x``/``ball_y``/``ball_z``/``ball_status``
    columns on every player row.  ``convert_to_frames`` expects the sportec
    ``EXPECTED_INPUT_COLUMNS`` schema which includes separate ball rows
    (``is_ball=True``, ``player_id=NaN``, ``team_id=NaN``).

    Column mapping (bronze → sportec input):

    +--------------+--------------+--------------------------------------+
    | Bronze       | Sportec      | Notes                                |
    +--------------+--------------+--------------------------------------+
    | match_id     | game_id      | rename                               |
    | period       | period_id    | rename                               |
    | frame        | frame_id     | rename                               |
    | timestamp    | time_seconds | rename                               |
    | x            | x_centered   | already DFL-centered (±52.5)         |
    | y            | y_centered   | already DFL-centered (±34.0)         |
    | s            | speed_native | rename                               |
    | ball_status  | ball_state   | ``0``→``dead``, ``1``→``alive``,     |
    |              |              | legacy ``Alive``/``Dead`` lowercased |
    | frame_rate   | frame_rate   | identity                             |
    | player_id    | player_id    | identity                             |
    | team_id      | team_id      | identity                             |
    | is_goalkeeper| is_goalkeeper| identity                             |
    +--------------+--------------+--------------------------------------+

    Synthetic ball rows are created by deduplicating
    ``(frame, period)`` and pivoting ``ball_x``/``ball_y``/``ball_z``
    into ``x_centered``/``y_centered``/``z``.  Player rows get
    ``z=NaN`` (DFL does not provide z for non-ball objects).
    """
    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_IDSSE_CONSUMED_COLS)].copy()

    # ── Player rows ──────────────────────────────────────────────
    players = trk_pdf.rename(
        columns={
            "match_id": "game_id",
            "period": "period_id",
            "frame": "frame_id",
            "timestamp": "time_seconds",
            "x": "x_centered",
            "y": "y_centered",
            "s": "speed_native",
            "ball_status": "ball_state",
        },
    ).copy()
    players["is_ball"] = False
    players["z"] = np.nan

    # ball_state: DFL XML BallStatus is "0" (dead) / "1" (alive) in IDSSE;
    # infer_ball_carrier checks `bs == "dead"`.  Map before lowercasing so
    # both legacy "Alive"/"Dead" and IDSSE "0"/"1" resolve correctly.
    _ball_status_map = {"0": "dead", "1": "alive"}
    bs = players["ball_state"]
    players["ball_state"] = bs.map(_ball_status_map).fillna(bs.str.lower()).where(bs.notna(), other=None)  # type: ignore[arg-type]  # None→NA fill is valid at runtime; pandas-stubs over-narrows `other`

    # ── Synthetic ball rows (one per frame) ──────────────────────
    ball_src = trk_pdf[
        [
            "frame",
            "period",
            "timestamp",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_s",
            "ball_status",
            "match_id",
            "frame_rate",
        ]
    ].copy()
    ball_src = ball_src.drop_duplicates(subset=["frame", "period"])
    ball_src.rename(
        columns={
            "match_id": "game_id",
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "ball_x": "x_centered",
            "ball_y": "y_centered",
            "ball_z": "z",
            "ball_s": "speed_native",
            "ball_status": "ball_state",
        },
        inplace=True,
    )
    bs_ball = ball_src["ball_state"]
    ball_src["ball_state"] = (
        bs_ball.map(_ball_status_map).fillna(bs_ball.str.lower()).where(bs_ball.notna(), other=None)  # type: ignore[arg-type]  # None→NA fill is valid at runtime; pandas-stubs over-narrows `other`
    )
    ball_src["player_id"] = None
    ball_src["team_id"] = None
    ball_src["is_ball"] = True
    ball_src["is_goalkeeper"] = False

    # ── Combine and select only EXPECTED_INPUT_COLUMNS ───────────
    expected_cols = [
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
    result = pd.concat(
        [players[expected_cols], ball_src[expected_cols]],
        ignore_index=True,
    )
    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _bronze_metrica_to_frames(
    trk_pdf: pd.DataFrame,
    game_id: int,
    *,
    jersey_to_pid: dict[str, str],
    fallback_fmt: str,
) -> pd.DataFrame:
    """Convert Metrica bronze tracking (frame-level JSON) to silly-kicks frames.

    Bronze schema: period, frame, timestamp, ball_x, ball_y,
    home_players (JSON), away_players (JSON), gk_jersey_numbers (JSON),
    pitch_length_m, pitch_width_m, frame_rate.

    COORDINATE CONVERSION: Metrica 0-1 normalized → SPADL 105x68 meters.
    - x_spadl = x_01 * 105.0
    - y_spadl = (1 - y_01) * 68.0  (Metrica y-axis is flipped: 0=top, 1=bottom)

    Do NOT use metrica_to_statsbomb() — that produces 120x80 StatsBomb yards,
    not 105x68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)x(0,68).
    """
    import json

    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_METRICA_CONSUMED_COLS)].copy()

    # Parse GK jersey numbers (match-level constant)
    gk_jerseys: set[str] = set()
    if "gk_jersey_numbers" in trk_pdf.columns:
        gk_raw = trk_pdf["gk_jersey_numbers"].dropna()
        if not gk_raw.empty:
            parsed = json.loads(gk_raw.iloc[0]) if isinstance(gk_raw.iloc[0], str) else gk_raw.iloc[0]
            gk_jerseys = {str(j) for j in parsed} if parsed else set()

    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 25

    rows: list[dict] = []
    for _, row in trk_pdf.iterrows():
        # Skip rows with NaN period (e.g. pre-match warmup data)
        if pd.isna(row["period"]):
            continue
        fid = int(row["frame"])
        pid = int(row["period"])
        t = float(row["timestamp"])

        # Home and away player rows from JSON
        for team_label, json_col in [("Home", "home_players"), ("Away", "away_players")]:
            raw = row.get(json_col)
            if pd.isna(raw) or raw is None:
                continue
            players = json.loads(raw) if isinstance(raw, str) else raw
            for jersey, coords in players.items():
                if isinstance(coords, dict) and "x" in coords and "y" in coords:
                    # Direct Metrica 0-1 → SPADL 105x68 (NOT StatsBomb 120x80)
                    x_spadl = float(coords["x"]) * 105.0
                    y_spadl = (1.0 - float(coords["y"])) * 68.0
                    rows.append(
                        {
                            "game_id": game_id,
                            "frame_id": fid,
                            "period_id": pid,
                            "time_seconds": t,
                            "player_id": jersey_to_pid.get(jersey, fallback_fmt.format(jersey)),
                            "team_id": team_label,
                            "x": x_spadl,
                            "y": y_spadl,
                            "is_goalkeeper": jersey in gk_jerseys,
                            "is_ball": False,
                        }
                    )

        # Ball row
        bx, by = row.get("ball_x"), row.get("ball_y")
        if not pd.isna(bx) and not pd.isna(by):
            rows.append(
                {
                    "game_id": game_id,
                    "frame_id": fid,
                    "period_id": pid,
                    "time_seconds": t,
                    "player_id": None,
                    "team_id": None,
                    "x": float(bx) * 105.0,
                    "y": (1.0 - float(by)) * 68.0,
                    "is_goalkeeper": False,
                    "is_ball": True,
                }
            )

    frames = pd.DataFrame(rows)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "metrica"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None  # Metrica bronze doesn't carry ball state
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="metrica", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _bronze_skillcorner_to_frames(trk_pdf: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Convert SkillCorner bronze tracking (narrow) to silly-kicks frames.

    Bronze schema (narrow, one row per player per frame):
    period, frame, timestamp, player_id, team, x, y, ball_x, ball_y,
    ball_z, is_goalkeeper, home_team_id, away_team_id, frame_rate.

    COORDINATE CONVERSION: center-origin meters → SPADL 105x68 meters.
    - x_spadl = x_center + 52.5
    - y_spadl = y_center + 34.0

    Do NOT use center_m_to_statsbomb() — that produces 120x80 StatsBomb yards,
    not 105x68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)x(0,68).
    """
    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_SKILLCORNER_CONSUMED_COLS)].copy()

    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 10

    # Player rows — rename to match TRACKING_FRAMES_COLUMNS
    players = trk_pdf[["frame", "period", "timestamp", "player_id", "team", "x", "y", "is_goalkeeper"]].copy()
    # Convert player_id to string for identity-resolution consistency
    # (SPADL player_id_native is stringified numeric — must match frames).
    players["player_id"] = players["player_id"].astype(str)
    players.rename(
        columns={
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "team": "team_id",
        },
        inplace=True,
    )
    # Direct center-origin meters → SPADL 105x68 (NOT StatsBomb 120x80)
    players["x"] = players["x"] + 52.5
    players["y"] = players["y"] + 34.0
    players["is_ball"] = False
    players["game_id"] = game_id

    # Ball rows — deduplicate (ball_x/ball_y are on every player row)
    ball_src = trk_pdf[["frame", "period", "timestamp", "ball_x", "ball_y"]].copy()
    ball_src = ball_src.drop_duplicates(subset=["frame", "period"])
    ball_src.rename(
        columns={
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "ball_x": "x",
            "ball_y": "y",
        },
        inplace=True,
    )
    ball_src["x"] = ball_src["x"] + 52.5
    ball_src["y"] = ball_src["y"] + 34.0
    ball_src["player_id"] = None
    ball_src["team_id"] = None
    ball_src["is_goalkeeper"] = False
    ball_src["is_ball"] = True
    ball_src["game_id"] = game_id

    frames = pd.concat([players, ball_src], ignore_index=True)

    # Re-base the continuous broadcast clock to PERIOD-RELATIVE time_seconds so frames align with
    # silly-kicks 4.20.1's now-period-relative SkillCorner actions (see _SKILLCORNER_PERIOD_START_SECONDS).
    frames["time_seconds"] = frames["time_seconds"] - frames["period_id"].map(_SKILLCORNER_PERIOD_START_SECONDS).fillna(
        0.0
    ).astype(float)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "skillcorner"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="skillcorner", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _bronze_gradientsports_to_converter_input(
    trk_pdf: pd.DataFrame,
    *,
    team_side_to_id: dict[str, str],
    jersey_to_player_id: dict[tuple[str, str], str],
    gk_player_ids: frozenset[str],
) -> pd.DataFrame:
    """Map bronze ``gradientsports_tracking`` columns to silly-kicks converter input.

    Args:
        trk_pdf: Bronze tracking rows (columns per _GRADIENTSPORTS_TRACKING_SELECT_COLS).
        team_side_to_id: Maps team_side ("home"/"away") -> native team_id string.
        jersey_to_player_id: Maps (team_side, jersey_num) -> native player_id string.
        gk_player_ids: Set of player_id strings who are goalkeepers.

    Returns:
        DataFrame with columns matching silly_kicks.tracking.gradientsports.EXPECTED_INPUT_COLUMNS.
    """
    import pandas as _pd

    result = _pd.DataFrame()
    result["game_id"] = trk_pdf["match_id"]
    result["period_id"] = trk_pdf["period"].astype("Int64")
    result["frame_id"] = trk_pdf["frame_num"].astype("Int64")
    result["time_seconds"] = trk_pdf["period_elapsed_time"].astype("float64")
    result["frame_rate"] = _GS_FRAME_RATE
    result["is_ball"] = trk_pdf["is_ball"].fillna(False)
    result["x_centered"] = trk_pdf["x"].astype("float64")
    result["y_centered"] = trk_pdf["y"].astype("float64")
    result["z"] = trk_pdf["z"].astype("float64")
    result["speed_native"] = np.nan  # Derived by converter/post-processing
    result["ball_state"] = "alive"  # GS does not provide per-frame ball state

    # Map team_side -> team_id; ball rows get NaN team_id
    result["team_id"] = trk_pdf["team_side"].map(team_side_to_id)

    # Map (team_side, jersey_num) -> player_id; ball rows get NaN
    _side = trk_pdf["team_side"].fillna("")
    _jersey = trk_pdf["jersey_num"].fillna("")
    result["player_id"] = [jersey_to_player_id.get((s, j)) for s, j in zip(_side, _jersey, strict=False)]

    # is_goalkeeper from roster
    result["is_goalkeeper"] = result["player_id"].isin(gk_player_ids)
    # Ball rows: explicit False for is_goalkeeper
    result.loc[result["is_ball"] == True, "is_goalkeeper"] = False  # noqa: E712

    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _coerce_gradientsports_frame_ids_to_native_str(frames: pd.DataFrame) -> pd.DataFrame:
    """Coerce GS frame ``player_id``/``team_id`` from Int64 back to native string in place.

    silly-kicks' ``GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS`` schema forces
    ``player_id``/``team_id`` to ``Int64`` inside ``convert_to_frames``. But every
    downstream consumer compares frame ids to the NATIVE-STRING action ids —
    ``_resolve_action_frame_context`` does ``player_id_frame == player_id_action`` /
    ``team_id_frame != team_id_action`` (actions carry ``player_id_native`` /
    ``team_id_native`` after ``_resolve_enrichment_identity``), and the silly-kicks
    kernels compare ``frames["team_id"] == home_team_id`` where ``home_team_id`` is a
    ``str``. ``Int64(366) == "366"`` is ``False`` → GS carrier/possession/actor/opponent/
    defensive-line resolution silently breaks. SkillCorner (``.astype(str)``) and IDSSE
    (sportec ``object`` schema) already emit native-string frame ids; GS is the schema
    outlier, so we realign it here. NA (ball rows) → ``None``. See project memory
    ``project_gradientsports_player_id_space_bug``.
    """
    for col in ("player_id", "team_id"):
        if col in frames.columns:
            # Int64 -> StringDtype ("366"/<NA>, no ".0") -> object (native str + NA for ball).
            frames[col] = frames[col].astype("string").astype(object)
    return frames
