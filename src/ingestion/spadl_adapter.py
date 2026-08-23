"""Adapters to transform bronze event tables into SPADL-converter-compatible DataFrames.

The SPADL/VAEP pipeline reads from existing bronze Delta tables instead of
re-fetching from external APIs.  These adapters bridge the gap between the
serialized bronze column layout and the DataFrame shape that silly-kicks'
``convert_to_actions()`` functions expect.

Supported sources:
  - StatsBomb        (``adapt_statsbomb_events``, ``resolve_statsbomb_home_team_ids``)
  - Wyscout          (``adapt_wyscout_events``,   ``resolve_wyscout_home_team_ids``)
  - IDSSE            (moved to the silly-kicks DFL parse port
                       ``silly_kicks.providers.sportec.shape_events_to_native``
                       under delete-and-depend, ADR-031 T3 / Gate B)
  - Metrica          (``adapt_metrica_events_for_silly_kicks``)
  - Gradient Sports  (``adapt_gradientsports_events``, ``extract_gradientsports_match_metadata``)
"""

from __future__ import annotations

import hashlib
import json

import pandas as pd

# ---------------------------------------------------------------------------
# Deterministic STRING -> BIGINT hashing (LL2 Path B)
# ---------------------------------------------------------------------------
#
# IDSSE + Metrica match_ids are strings (e.g. ``idsse_J03WMX``,
# ``Sample_Game_1``) but the SPADL/VAEP pipeline declares ``match_id BIGINT``
# in the Delta schema and uses ``groupBy("match_id")`` for per-match
# applyInPandas dispatch.  Hashing to a 60-bit BIGINT preserves uniqueness
# (collision probability ~10^-9 across ~3,500 matches) while keeping the
# legacy schema usable.  The original strings are preserved alongside in
# ``match_id_native`` columns on bronze events tables (LL2 Path B).
#
# Stability contract: SHA-256 of the UTF-8 bytes, first 15 hex chars (60 bits)
# converted as int. Same input always yields same output across runs.

_HASH_HEX_CHARS = 15
"""Number of hex characters from SHA-256 to keep (60 bits → fits in BIGINT)."""

UNKNOWN_TEAM_SENTINEL = "__UNKNOWN_TEAM__"
"""Deterministic sentinel for rows where ``team_id_native`` is NULL.

Used by tracking-provider SPADL UDFs (IDSSE, Metrica, SkillCorner) when
silly-kicks emits a team label that the home/away mapper cannot resolve
(e.g. freekick_short events). Hashed via ``hash_native_id_to_bigint`` to
produce a stable BIGINT that differs from all real team hashes.

Single source of truth — imported by all 3 UDFs and test assertions.
"""


def hash_native_id_to_bigint(value: str) -> int:
    """Deterministically hash a native string ID to a 60-bit BIGINT.

    Used by IDSSE + Metrica SPADL UDFs to fit ``match_id`` / ``game_id``
    string identifiers into the legacy ``BIGINT`` column type. The
    original string is always preserved alongside in the corresponding
    ``*_native`` STRING column on bronze (LL2 Path B convention).

    Args:
        value: Any non-empty string identifier (e.g. ``'idsse_J03WMX'``,
            ``'Sample_Game_1'``).

    Returns:
        Deterministic 60-bit positive integer.
    """
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:_HASH_HEX_CHARS], 16)


# ---------------------------------------------------------------------------
# StatsBomb adapters
# ---------------------------------------------------------------------------

_SB_RENAME = {
    "match_id": "game_id",
    "id": "event_id",
    "period": "period_id",
    "type": "type_name",
}


def adapt_statsbomb_events(
    events_pdf: pd.DataFrame,
    home_team_id: int,
) -> pd.DataFrame:
    """Convert bronze ``statsbomb_events`` rows to SPADL converter input format.

    Column mapping:
        ``match_id`` -> ``game_id``, ``id`` -> ``event_id``,
        ``period`` -> ``period_id``, ``type`` -> ``type_name``,
        ``_raw_extra_json`` -> ``extra`` (dict)

    Args:
        events_pdf: DataFrame read from the ``statsbomb_events`` bronze table.
        home_team_id: The home team's ``team_id`` for this game
            (used by ``spadl_sb.convert_to_actions``).

    Returns:
        Adapted DataFrame ready for ``silly_kicks.spadl.statsbomb.convert_to_actions``.
    """
    adapted = events_pdf.rename(columns=_SB_RENAME)

    if "_raw_extra_json" in adapted.columns:
        raw_extra: pd.Series = adapted["_raw_extra_json"]  # type: ignore[assignment]
    else:
        raw_extra = pd.Series("{}", index=adapted.index)
    adapted["extra"] = raw_extra.apply(lambda s: json.loads(s) if isinstance(s, str) and s.strip() else {})

    if "location" in adapted.columns:
        adapted["location"] = adapted["location"].apply(
            lambda s: json.loads(s) if isinstance(s, str) and s not in ("", "null") else s
        )

    # Ensure timestamp is timedelta (the SPADL converter expects this)
    if "timestamp" in adapted.columns:
        ts_series: pd.Series = adapted["timestamp"]  # type: ignore[assignment]
        adapted["timestamp"] = pd.to_timedelta(ts_series, errors="coerce").fillna(pd.Timedelta(0))

    adapted["home_team_id"] = home_team_id
    return adapted


def resolve_statsbomb_home_team_ids(
    matches_pdf: pd.DataFrame,
    events_pdf: pd.DataFrame,
) -> dict[int, int]:
    """Derive ``home_team_id`` per match from events + match metadata.

    ``statsbomb_matches`` has ``home_team`` (team name string).
    ``statsbomb_events`` has ``team_id`` (int) and ``team`` (name string).
    We join on match_id + team name to resolve the numeric ID.

    Args:
        matches_pdf: DataFrame from ``statsbomb_matches`` with ``match_id``, ``home_team``.
        events_pdf: DataFrame from ``statsbomb_events`` with ``match_id``, ``team_id``, ``team``.

    Returns:
        Mapping of ``match_id`` -> ``home_team_id``.
    """
    team_names = events_pdf[["match_id", "team_id", "team"]].drop_duplicates()
    home_lookup = matches_pdf[["match_id", "home_team"]].rename(columns={"home_team": "team"})  # type: ignore[call-overload]
    merged = home_lookup.merge(team_names, on=["match_id", "team"], how="left")
    return dict(zip(merged["match_id"], merged["team_id"].fillna(0).astype(int), strict=False))


# ---------------------------------------------------------------------------
# Wyscout adapters
# ---------------------------------------------------------------------------

_WS_RENAME = {
    "matchId": "game_id",
    "id": "event_id",
    "eventId": "type_id",
    "subEventId": "subtype_id",
    "playerId": "player_id",
    "teamId": "team_id",
}

_WS_PERIOD_MAP = {"1H": 1, "2H": 2, "E1": 3, "E2": 4, "P": 5}


def adapt_wyscout_events(events_pdf: pd.DataFrame) -> pd.DataFrame:
    """Convert bronze ``wyscout_events`` rows to SPADL converter input format.

    Column mapping:
        ``matchId`` -> ``game_id``, ``eventId`` -> ``type_id``,
        ``subEventId`` -> ``subtype_id``, ``playerId`` -> ``player_id``,
        ``teamId`` -> ``team_id``

    Transforms:
        ``matchPeriod`` (str) -> ``period_id`` (int):  1H->1, 2H->2, E1->3, E2->4, P->5
        ``eventSec`` (float) -> ``milliseconds`` (float):  x 1000
        ``positions`` / ``tags`` (JSON string) -> parsed list

    Args:
        events_pdf: DataFrame read from the ``wyscout_events`` bronze table.

    Returns:
        Adapted DataFrame ready for ``silly_kicks.spadl.wyscout.convert_to_actions``.
    """
    adapted = events_pdf.rename(columns=_WS_RENAME)

    if "matchPeriod" in adapted.columns:
        adapted["period_id"] = adapted["matchPeriod"].map(_WS_PERIOD_MAP).fillna(1).astype(int)  # type: ignore[arg-type]

    if "eventSec" in adapted.columns:
        adapted["milliseconds"] = adapted["eventSec"].astype(float) * 1000

    for col in ("positions", "tags"):
        if col in adapted.columns:
            adapted[col] = adapted[col].apply(lambda s: json.loads(s) if isinstance(s, str) else s)

    return adapted


def resolve_wyscout_home_team_ids(matches_pdf: pd.DataFrame) -> dict[int, int]:
    """Extract ``home_team_id`` from ``teamsData`` JSON per match.

    The ``teamsData`` column is a JSON string whose first key is the home team ID
    (per the Wyscout data specification).

    Args:
        matches_pdf: DataFrame from ``wyscout_matches`` with ``wyId`` and ``teamsData``.

    Returns:
        Mapping of ``match_id`` (wyId) -> ``home_team_id``.
    """
    result: dict[int, int] = {}
    for wy_id, raw_td in zip(matches_pdf["wyId"], matches_pdf["teamsData"], strict=False):
        td_str: str = str(raw_td) if raw_td is not None else "{}"
        teams_data: dict[str, object] = json.loads(td_str) if td_str else {}
        if teams_data:
            home_id = int(next(iter(teams_data)))
            result[int(wy_id)] = home_id
    return result


# ---------------------------------------------------------------------------
# IDSSE (Sportec / Bundesliga DFL) adapter — LL2 Path B
# ---------------------------------------------------------------------------
#
# luxury-lakehouse's ``bronze.idsse_events`` already stores DFL events in
# the canonical shape silly-kicks 1.7.0+ ``silly_kicks.spadl.sportec.
# convert_to_actions`` expects: required cols ``match_id, event_id,
# event_type, period, timestamp_seconds, player_id, team, x, y`` are all
# present natively (the DFL XML attribute names map identically).
#
# However, DFL XML set-piece events (ThrowIn, FreeKick, GoalKick,
# CornerKick) and Foul events store the acting team's DFL CLU id and the
# acting player's DFL OBJ id in event-type-specific qualifier columns
# (``play_team`` / ``throwin_team`` / ``foul_team_fouler`` for team;
# ``play_player`` / ``foul_fouler`` for player) rather than the generic
# ``team`` / ``player_id`` attributes — which carry ``'unknown'`` / ``''``
# on those event types.  The adapter resolves these before handing off to
# silly-kicks so the SPADL output has correct team/player attribution.
#
# Coordinate system: bronze.idsse_events stores ``x ∈ [0, 105], y ∈ [0, 68]``
# already (corner-flag origin meters) — silly-kicks's converter clips to the
# same range, so identity passthrough is correct.
#
# Direction of play: silly-kicks's converter takes ``home_team_id: str`` and
# uses string equality with the ``team`` column for direction normalisation.
# bronze.idsse_events.team carries ``'home'`` / ``'away'`` / ``'unknown'``
# labels, so the UDF passes the literal string ``'home'`` (NOT the DFL
# CLU id) so direction-of-play flips correctly. The actual DFL TeamId for
# each row's acting team lands in the ``team_id_native`` SPADL column.


# NOTE: the DFL qualifier team/player resolution for IDSSE (formerly
# ``_resolve_idsse_team_from_qualifiers`` / ``_resolve_idsse_player_from_qualifiers`` +
# their ``_TEAM_QUALIFIER_PRIORITY`` / ``_PLAYER_QUALIFIER_PRIORITY`` lists) now lives
# UPSTREAM in ``silly_kicks.providers.sportec.shape_events_to_native`` — the production
# IDSSE UDF (``ingestion.spadl_conversion``) imports the adapter from there. The former
# lakehouse copy was removed once the logic was upstreamed (it had drifted — it lacked the
# ``freekick_team`` / ``goalkick_team`` qualifiers). Do NOT re-add a lakehouse copy; extend
# the sportec parser in silly-kicks instead.


# ---------------------------------------------------------------------------
# Metrica adapter — LL2 Path B
# ---------------------------------------------------------------------------
#
# silly-kicks 1.7.0+ ``silly_kicks.spadl.metrica.convert_to_actions``
# expects required cols: ``match_id, event_id, type, subtype, period,
# start_time_s, end_time_s, player, team, start_x, start_y, end_x, end_y``.
# bronze.metrica_events already stores all of these.
#
# Coordinate system: bronze.metrica_events stores ``[0, 1]`` normalised
# coords (Metrica-native, source-faithful per Q1 design decision).
# silly-kicks's converter takes coords AS-IS and clips to ``[0, 105] / [0, 68]``
# at output. Without scaling, all output coords would clip to ``1.0`` — so
# the adapter MUST scale by per-match ``pitch_length_m`` / ``pitch_width_m``
# (or the SPADL defaults 105 / 68 when bronze pitch dims are NULL).
#
# Direction of play: bronze.metrica_events.team carries ``'Home'`` / ``'Away'``
# (capitalised — distinct from IDSSE's lowercase). The Metrica UDF passes
# ``home_team_id='Home'`` literal to silly-kicks for direction normalisation.


def adapt_metrica_events_for_silly_kicks(events_pdf: pd.DataFrame) -> pd.DataFrame:
    """Convert bronze ``metrica_events`` rows to silly-kicks 1.7.0 metrica input.

    Scales the [0, 1] normalised coordinates from bronze to SPADL-frame
    metres using per-match ``pitch_length_m`` / ``pitch_width_m`` (or
    SPADL defaults 105 / 68 when those are NULL — the case for the CSV-source
    Games 1-2 in the Metrica open-data sample).

    Args:
        events_pdf: DataFrame read from the ``bronze.metrica_events`` Delta
            table. Must include ``start_x, start_y, end_x, end_y,
            pitch_length_m, pitch_width_m`` columns.

    Returns:
        Adapted DataFrame ready for ``silly_kicks.spadl.metrica.
        convert_to_actions(events, home_team_id='Home')``.
    """
    adapted = events_pdf.copy()

    # Per-match pitch dimensions (constant within a match). Default to SPADL
    # standard when bronze metadata is NULL — Metrica CSV path doesn't carry
    # pitch dims.
    if "pitch_length_m" in adapted.columns and adapted["pitch_length_m"].notna().any():
        pitch_x = float(adapted["pitch_length_m"].dropna().iloc[0])
    else:
        pitch_x = 105.0
    if "pitch_width_m" in adapted.columns and adapted["pitch_width_m"].notna().any():
        pitch_y = float(adapted["pitch_width_m"].dropna().iloc[0])
    else:
        pitch_y = 68.0

    for col, scale in (
        ("start_x", pitch_x),
        ("end_x", pitch_x),
        ("start_y", pitch_y),
        ("end_y", pitch_y),
    ):
        if col in adapted.columns:
            adapted[col] = adapted[col].astype("float64") * scale

    return adapted


# ---------------------------------------------------------------------------
# SK3-MIG (2026-05-02): home_team_start_left derivation per provider
# ---------------------------------------------------------------------------
#
# silly-kicks 3.0.1 (PR-S23) requires Sportec + Metrica callers to pass
# ``home_team_start_left: bool`` on ``convert_to_actions(...)``. Bronze
# storage of this flag varies per provider:
#
#   IDSSE / Sportec — AUTHORITATIVE: moved to the silly-kicks DFL parse port
#     (``silly_kicks.providers.sportec.derive_idsse_home_team_start_left``)
#     under delete-and-depend (ADR-031 T3 / Gate B). The lakehouse no longer
#     owns the IDSSE deriver — production callers import it from the port.
#
#   Metrica — EMPIRICAL: bronze does not capture a per-period direction flag.
#     ``derive_metrica_home_team_start_left`` infers from period-1 SHOT
#     positions: if home-team avg shot start_x > pitch_mid, home was shooting
#     toward the right goal in period 1 (i.e., home defends the LEFT goal,
#     so home_team_start_left=True). Falls back to all home period-1 events
#     when shots are sparse, then raises if no usable signal exists.


def derive_metrica_home_team_start_left(
    events: pd.DataFrame,
    home_team_value: str = "Home",
    *,
    pitch_length_m: float = 105.0,
) -> bool:
    """Derive ``home_team_start_left`` for a Metrica match by empirical inference.

    Metrica bronze does NOT capture a per-period direction flag. Inference
    uses period-1 SHOT positions: if home-team avg shot start_x > pitch_mid,
    home was shooting toward the right goal in period 1 (home defends LEFT
    goal, so home_team_start_left=True). Falls back to all home period-1
    events when shots are sparse, then raises if no usable signal exists.

    Parameters
    ----------
    events : pd.DataFrame
        Metrica adapted DataFrame (post ``adapt_metrica_events_for_silly_kicks``).
        Must contain ``team``, ``period``, ``start_x``, ``type`` columns.
    home_team_value : str
        Value in the ``team`` column representing the home team. Default
        ``"Home"`` matches the Metrica adapter's output convention.
    pitch_length_m : float
        Full pitch length in meters. Default 105.0 (canonical SPADL).

    Returns
    -------
    bool

    Raises
    ------
    RuntimeError
        Insufficient period-1 home-team data to determine direction.
    """
    pitch_mid = pitch_length_m / 2.0
    period_1_home = events[(events["period"] == 1) & (events["team"] == home_team_value)]

    home_p1_shots = period_1_home[period_1_home["type"] == "SHOT"]
    if len(home_p1_shots) >= 2:
        avg_x = float(home_p1_shots["start_x"].mean())
        return avg_x > pitch_mid

    home_p1_with_x = period_1_home[period_1_home["start_x"].notna()]
    if len(home_p1_with_x) >= 5:
        avg_x = float(home_p1_with_x["start_x"].mean())
        return avg_x > pitch_mid

    msg = (
        f"Metrica: insufficient period-1 home-team data to derive "
        f"home_team_start_left (home shots={len(home_p1_shots)}, "
        f"home events with x={len(home_p1_with_x)}). Need ≥2 shots OR ≥5 "
        f"events with non-null start_x."
    )
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# silly-kicks 4.0.0 (PR-S70): ET start-direction derivation per provider
# ---------------------------------------------------------------------------
#
# silly-kicks 4.0.0's symmetric ET guard (`require_et_direction`) raises if any
# per-period-absolute converter (Sportec/Metrica tracking+events,
# GradientSports tracking+events) is called with frames containing period_id in
# {3, 4} but no ``home_team_start_left_extratime``. To stay correct under 4.0.0
# the lakehouse must derive that flag per provider:
#
#   IDSSE / Sportec — AUTHORITATIVE: the direction logic lives in the silly-kicks
#     DFL parse port (``silly_kicks.providers.sportec.
#     derive_idsse_home_team_start_left_extratime``) under delete-and-depend
#     (ADR-031 T3 / Gate B). The lakehouse keeps a THIN defensive wrapper
#     (``derive_idsse_home_team_start_left_extratime`` below) that preserves the
#     historical "no period column at all -> no ET -> None" contract and delegates
#     everything else to the port. See its docstring for the 4.87.0 rationale.
#
#   Metrica — EMPIRICAL: bronze has no per-period direction flag. Mirror the
#     period-1 inference using period-3 SHOT positions.
#
# Returns ``None`` when the match has no ET periods — that's the correct value
# to pass through; silly-kicks 4.0 accepts ``None`` if no ET data is present
# and only raises when both signals are missing simultaneously.


def derive_idsse_home_team_start_left_extratime(
    events: pd.DataFrame,
    home_team_id_native: str,
) -> bool | None:
    """Lakehouse defensive wrapper over the silly-kicks IDSSE ET-direction port.

    silly-kicks 4.87.0 changed ``silly_kicks.providers.sportec.
    derive_idsse_home_team_start_left_extratime``: it now RAISES ``RuntimeError``
    (via ``_resolve_period_column``) when ``events`` carries NEITHER a ``period_id``
    NOR a ``period`` column — the port treats a period-less frame as a caller bug
    ("refusing to report a silent pass") rather than a no-ET match. Earlier port
    versions returned ``None`` for that shape.

    The lakehouse's historical contract is "events lacking a period column -> this
    match has no ET -> ``None``". This wrapper preserves that contract: it
    short-circuits to ``None`` when no period column is present, and otherwise
    delegates to the port UNCHANGED. Every OTHER port raise (ET periods present but
    no ET KickOff row; null ``kickoff_team_left``) is preserved — those paths run
    only AFTER the period column resolves, so they are past the short-circuit.

    Production always passes ``shape_events_to_native(...)`` output, which always
    carries a ``period`` column, so on real IDSSE data this delegates every time and
    behaviour is identical to calling the port directly. The short-circuit only fires
    for a genuinely period-less frame (defensive; the historical no-ET signal).
    """
    from silly_kicks.providers.sportec import (
        derive_idsse_home_team_start_left_extratime as _sk_derive_idsse_et,
    )

    if not ({"period_id", "period"} & set(events.columns)):
        return None
    return _sk_derive_idsse_et(events, home_team_id_native)


def derive_metrica_home_team_start_left_extratime(
    events: pd.DataFrame,
    home_team_value: str = "Home",
    *,
    pitch_length_m: float = 105.0,
) -> bool | None:
    """Derive ``home_team_start_left_extratime`` for a Metrica match.

    Metrica bronze has no per-period direction flag. Mirrors the period-1
    inference using period-3 SHOT positions: if home-team avg ET shot
    ``start_x > pitch_mid``, home shot toward the right goal in period 3
    (home defends LEFT, so ``home_team_start_left_extratime=True``). Falls
    back to all period-3 home events when shots are sparse; raises if no
    usable signal exists despite ET periods being present.

    Returns ``None`` when the match has no period-3 home-team data at all
    (no ET in this match) — that's the correct value to pass to silly-kicks
    4.0+ (its guard accepts None when no ET periods present).

    Parameters
    ----------
    events : pd.DataFrame
        Metrica adapted DataFrame. Must contain ``team``, ``period``,
        ``start_x``, ``type``.
    home_team_value : str
        Value in ``team`` representing the home team. Default ``"Home"``.
    pitch_length_m : float
        Full pitch length in meters. Default 105.0 (canonical SPADL).

    Returns
    -------
    bool | None

    Raises
    ------
    RuntimeError
        ET periods recorded but insufficient home-team period-3 data to
        determine direction.
    """
    has_et_periods = "period" in events.columns and events["period"].isin([3, 4]).any()
    if not has_et_periods:
        return None

    pitch_mid = pitch_length_m / 2.0
    period_3_home = events[(events["period"] == 3) & (events["team"] == home_team_value)]

    home_p3_shots = period_3_home[period_3_home["type"] == "SHOT"]
    if len(home_p3_shots) >= 2:
        avg_x = float(home_p3_shots["start_x"].mean())
        return avg_x > pitch_mid

    home_p3_with_x = period_3_home[period_3_home["start_x"].notna()]
    if len(home_p3_with_x) >= 5:
        avg_x = float(home_p3_with_x["start_x"].mean())
        return avg_x > pitch_mid

    msg = (
        f"Metrica: ET periods present but insufficient period-3 home-team data "
        f"to derive home_team_start_left_extratime (home shots={len(home_p3_shots)}, "
        f"home events with x={len(home_p3_with_x)}). Need ≥2 shots OR ≥5 events "
        f"with non-null start_x."
    )
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Gradient Sports adapter (WC2022 PFF open dataset)
# ---------------------------------------------------------------------------

# Bronze uses json_normalize dot-notation (e.g., "possessionEvents.passType").
# silly-kicks expects 48 snake_case columns (EXPECTED_INPUT_COLUMNS) — 47 pre-4.89.0 plus
# `start_time` (the raw absolute event clock), added as a required input by 4.89.0 (ADR-065).
#
# CRITICAL: The bronze schema was verified via DESCRIBE (264 columns).
# Several columns live under gameEvents.*, NOT possessionEvents.*:
#   gameEventId (top-level) -> event_id
#   gameEvents.period -> period_id
#   gameEvents.startGameClock -> time_seconds
#   gameEvents.playerId -> player_id
#   gameEvents.teamId -> team_id
#   gameEvents.setpieceType -> set_piece_type
# Ball coordinates are in a JSON string column `ball`, NOT possessionEvents.ballX/Y.
# challenger_team_id / challenge_winner_team_id DO NOT EXIST in bronze
# (the converter tolerates NaN for these).
#
# 1:1 renames (possessionEvents.*, fouls.*, gameEvents.gameEventType, gameId):
_GS_BRONZE_TO_SNAKE: dict[str, str] = {
    # Top-level scalars
    "gameId": "game_id",
    "possessionEventId": "possession_event_id",
    # Absolute event clock (top-level scalars, double). silly-kicks 4.89.0 (ADR-065/PR-S159) made
    # `start_time` a REQUIRED converter-input column: it is the chronological sort tiebreak AND the
    # order-insensitive basis for imputing a null-`startGameClock` FOUL's `time_seconds` (measured
    # 0/144,374 inversions vs the game clock on 64 real WC2022 matches). `event_time` is the optional
    # fallback ordering key when a row's `start_time` is NaN. Both are raw GS scalars (double in
    # bronze — verified in the DESCRIBE snapshot) passed through un-normalized: the converter uses
    # them only as ordering keys, never as emitted time values (period-relative `time_seconds` is
    # derived separately in Step 2b), so no offset subtraction applies here.
    "startTime": "start_time",
    "eventTime": "event_time",
    # possessionEvents.* -> snake_case (direct 1:1 renames)
    "possessionEvents.possessionEventType": "possession_event_type",
    "possessionEvents.passType": "pass_type",
    "possessionEvents.passOutcomeType": "pass_outcome_type",
    "possessionEvents.crossType": "cross_type",
    "possessionEvents.crossOutcomeType": "cross_outcome_type",
    "possessionEvents.crossZoneType": "cross_zone_type",
    "possessionEvents.shotType": "shot_type",
    "possessionEvents.shotOutcomeType": "shot_outcome_type",
    "possessionEvents.shotNatureType": "shot_nature_type",
    "possessionEvents.shotInitialHeightType": "shot_initial_height_type",
    "possessionEvents.touchType": "touch_type",
    "possessionEvents.touchOutcomeType": "touch_outcome_type",
    "possessionEvents.challengeType": "challenge_type",
    "possessionEvents.challengeOutcomeType": "challenge_outcome_type",
    "possessionEvents.challengeWinnerPlayerId": "challenge_winner_player_id",
    "possessionEvents.challengerPlayerId": "challenger_player_id",
    "possessionEvents.tackleAttemptType": "tackle_attempt_type",
    "possessionEvents.bodyType": "body_type",
    "possessionEvents.ballHeightType": "ball_height_type",
    "possessionEvents.clearanceOutcomeType": "clearance_outcome_type",
    "possessionEvents.ballCarryOutcome": "ball_carry_outcome",
    "possessionEvents.carryType": "carry_type",
    "possessionEvents.carryIntent": "carry_intent",
    "possessionEvents.carryDefenderPlayerId": "carry_defender_player_id",
    "possessionEvents.keeperTouchType": "keeper_touch_type",
    "possessionEvents.saveHeightType": "save_height_type",
    "possessionEvents.saveReboundType": "save_rebound_type",
    "possessionEvents.reboundOutcomeType": "rebound_outcome_type",
    "possessionEvents.incompletionReasonType": "incompletion_reason_type",
    # nonEvent: voided ("annulled") play flag — silly-kicks 4.13.0 Component-4 excludes
    # nonEvent==True rows (foul/advantage/offside callbacks + disallowed goals). Kept as the
    # literal camelCase `nonEvent` the converter reads (NOT snake) so the exclusion fires.
    "possessionEvents.nonEvent": "nonEvent",
    # gameEvents.* (only gameEventType is a 1:1 rename; period/playerId/
    # teamId/setpieceType are derived, not renamed — see _DERIVED_COLUMNS below)
    "gameEvents.gameEventType": "game_event_type",
    # fouls.*
    "fouls.foulType": "foul_type",
    "fouls.onFieldFoulOutcomeType": "on_field_foul_outcome_type",
    "fouls.finalFoulOutcomeType": "final_foul_outcome_type",
    "fouls.onFieldOffenseType": "on_field_offense_type",
    "fouls.finalOffenseType": "final_offense_type",
}


def _parse_ball_json(ball_series: pd.Series) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
    """Parse GS bronze `ball` JSON string column to (ball_x, ball_y) float Series.

    Bronze format: '[{"visibility": "VISIBLE", "x": 18.5, "y": -21.33, "z": 0.0}]'
    Always a single-element JSON array. Returns (NaN, NaN) for null/malformed rows.
    """
    import json as _json

    def _extract(val: object) -> tuple[float, float]:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return (float("nan"), float("nan"))
        try:
            parsed = _json.loads(str(val))
            if isinstance(parsed, list) and len(parsed) > 0:
                return (float(parsed[0]["x"]), float(parsed[0]["y"]))
        except (ValueError, KeyError, TypeError, IndexError):
            pass
        return (float("nan"), float("nan"))

    pairs = ball_series.map(_extract)
    ball_x = pairs.map(lambda p: p[0])
    ball_y = pairs.map(lambda p: p[1])
    return ball_x, ball_y


# GradientSports bronze ``gameEvents.startGameClock`` is a NOMINAL absolute game clock: each
# period's clock starts at a fixed nominal offset (0 / 45 / 90 / 105 / 120 min) and runs forward,
# so ``startGameClock == nominal_offset[period] + period_elapsed`` (verified on GS match 10503:
# p2 game-clock [2700, 5842.8] == period_elapsed [0, 3142.8] + 2700, exactly). silly-kicks'
# canonical SPADL ``time_seconds`` is PERIOD-RELATIVE — seconds since the start of the current
# period, resetting to 0 each period (matches Opta/StatsBomb and GS *tracking*'s
# ``period_elapsed_time``). We subtract the nominal offset so GS *actions* share the per-period
# base that GS tracking frames and every other provider already use, fixing the action<->frame
# time-base mismatch that silently dropped ~81% of GS period-2 actions in AC-1 enrichment.
# These offsets are the inverse of ``stg_spadl__action_values.sql``'s ``minute`` derivation; keep
# the two in lockstep. See ADR-040.
_GS_NOMINAL_PERIOD_START_SECONDS: dict[int, float] = {1: 0.0, 2: 2700.0, 3: 5400.0, 4: 6300.0, 5: 7200.0}


def _gs_nominal_period_start(period: object) -> float:
    """Nominal seconds-from-kickoff at which ``period`` starts (0.0 if NaN/unknown).

    Unknown or NaN periods get a 0.0 offset (value left untouched) rather than raising —
    the AC-1 work-unit time-base guard (``analytics.action_context.time_base_guard``) is the
    place that fails loud on a genuine base mismatch; the adapter stays a pure transform.
    """
    if pd.isna(period):  # type: ignore[arg-type]  # scalar object accepted by pd.isna
        return 0.0
    return _GS_NOMINAL_PERIOD_START_SECONDS.get(int(period), 0.0)  # type: ignore[arg-type]


def adapt_gradientsports_events(pdf: pd.DataFrame) -> pd.DataFrame:
    """Rename + derive bronze columns to match silly-kicks EXPECTED_INPUT_COLUMNS.

    Three transformation categories:
    1. Direct 1:1 renames via _GS_BRONZE_TO_SNAKE (~35 columns)
    2. Derived columns from gameEvents.* namespace (6 columns):
       gameEventId -> event_id, gameEvents.period -> period_id,
       gameEvents.startGameClock -> time_seconds, gameEvents.playerId -> player_id,
       gameEvents.teamId -> team_id, gameEvents.setpieceType -> set_piece_type
    2b. Time-base normalization: gameEvents.startGameClock is a NOMINAL absolute
        clock; subtract the nominal period-start offset so time_seconds is silly-kicks'
        canonical PERIOD-RELATIVE value (see _GS_NOMINAL_PERIOD_START_SECONDS / ADR-040).
    3. Ball JSON parsing: ball -> ball_x, ball_y

    Args:
        pdf: Raw bronze DataFrame from ``gradientsports_events``.

    Returns:
        DataFrame with all 48 ``EXPECTED_INPUT_COLUMNS`` present (47 pre-4.89.0 plus the
        4.89.0-required ``start_time``). ``event_time`` (the optional NaN-``start_time``
        fallback ordering key) also rides through from bronze ``eventTime``.
        Missing optional columns are NaN-filled.
    """
    from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS

    if pdf.empty:
        return pd.DataFrame(columns=sorted(EXPECTED_INPUT_COLUMNS))

    # Step 1: Apply 1:1 renames
    rename_map = {k: v for k, v in _GS_BRONZE_TO_SNAKE.items() if k in pdf.columns}
    adapted = pdf.rename(columns=rename_map)

    # Step 2: Derived columns from gameEvents.* namespace
    # These columns live under gameEvents.*, NOT possessionEvents.*
    derived_columns: dict[str, str] = {
        "gameEventId": "event_id",
        "gameEvents.period": "period_id",
        "gameEvents.startGameClock": "time_seconds",
        "gameEvents.playerId": "player_id",
        "gameEvents.teamId": "team_id",
        "gameEvents.setpieceType": "set_piece_type",
    }
    for bronze_col, snake_col in derived_columns.items():
        if bronze_col in adapted.columns:
            adapted[snake_col] = adapted[bronze_col]
        elif bronze_col in pdf.columns:
            adapted[snake_col] = pdf[bronze_col]
    # Drop source columns to avoid polluting output with dot-notation leftovers
    adapted = adapted.drop(
        columns=[k for k in derived_columns if k in adapted.columns],
        errors="ignore",
    )

    # Step 2b: normalize the nominal absolute game-clock to PERIOD-RELATIVE time_seconds.
    # startGameClock = nominal_offset[period] + period_elapsed; subtract the offset so GS actions
    # reset to 0 each period, matching GS tracking's period_elapsed_time and silly-kicks' canonical
    # convention. Without this, GS period-2 actions sit on the match clock (2700+) while frames are
    # period-relative (0..), so action<->frame linkage silently drops them. See ADR-040.
    if "period_id" in adapted.columns and "time_seconds" in adapted.columns:
        period_offsets = adapted["period_id"].map(_gs_nominal_period_start)
        adapted["time_seconds"] = adapted["time_seconds"].astype("float64") - period_offsets

    # Step 3: Parse ball JSON string -> ball_x, ball_y
    # (O(n) Python loop per match — fine for 64 WC2022 matches; revisit if scaling)
    if "ball" in adapted.columns:
        adapted["ball_x"], adapted["ball_y"] = _parse_ball_json(adapted["ball"])
        adapted = adapted.drop(columns=["ball"], errors="ignore")
    elif "ball" in pdf.columns:
        adapted["ball_x"], adapted["ball_y"] = _parse_ball_json(pdf["ball"])

    # Step 4: NaN-fill any remaining missing expected columns
    # (e.g., challenger_team_id, challenge_winner_team_id which don't exist in bronze)
    for col in EXPECTED_INPUT_COLUMNS:
        if col not in adapted.columns:
            adapted[col] = pd.NA

    return adapted


def extract_gradientsports_match_metadata(pdf: pd.DataFrame) -> dict:
    """Extract match-level metadata from GS bronze rows.

    GS bronze denormalizes match metadata into every event row.
    ``home_team_id`` is derived from ``gameEvents.homeTeam`` (boolean) +
    ``gameEvents.teamId`` because ``stadiumMetadata.homeTeamId`` does NOT
    exist in the bronze schema.

    Args:
        pdf: Raw bronze DataFrame (pre-rename, dot-notation columns).

    Returns:
        Dict with ``home_team_id`` (int), ``home_team_start_left`` (bool),
        ``home_team_start_left_extratime`` (bool | None).

    Raises:
        ValueError: If pdf is empty or no homeTeam=True rows found.
    """
    if pdf.empty:
        raise ValueError("Cannot extract metadata from empty DataFrame")

    # Derive home_team_id: find the first row where gameEvents.homeTeam is True
    home_mask = pdf["gameEvents.homeTeam"] == True  # noqa: E712 — bronze may return string "true"
    if not home_mask.any():
        # Fallback: try string comparison for Spark-serialized booleans
        home_mask = pdf["gameEvents.homeTeam"].astype(str).str.lower() == "true"
    if not home_mask.any():
        raise ValueError("No rows with gameEvents.homeTeam=True found — cannot derive home_team_id")

    home_team_id = int(float(pdf.loc[home_mask, "gameEvents.teamId"].iloc[0]))

    # Direction flag
    row = pdf.iloc[0]
    htsl_val = row["stadiumMetadata.homeTeamStartLeft"]
    if isinstance(htsl_val, str):
        home_team_start_left = htsl_val.lower() == "true"
    else:
        home_team_start_left = bool(htsl_val)

    # Extra-time direction flag -- may be absent or null
    et_col = "stadiumMetadata.homeTeamStartLeftExtraTime"
    if et_col in pdf.columns:
        et_val = row[et_col]
        if pd.notna(et_val):
            if isinstance(et_val, str):
                home_team_start_left_extratime: bool | None = et_val.lower() == "true"
            else:
                home_team_start_left_extratime = bool(et_val)
        else:
            home_team_start_left_extratime = None
    else:
        home_team_start_left_extratime = None

    return {
        "home_team_id": home_team_id,
        "home_team_start_left": home_team_start_left,
        "home_team_start_left_extratime": home_team_start_left_extratime,
    }
