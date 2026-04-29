"""Adapters to transform bronze event tables into SPADL-converter-compatible DataFrames.

The SPADL/VAEP pipeline reads from existing bronze Delta tables instead of
re-fetching from external APIs.  These adapters bridge the gap between the
serialized bronze column layout and the DataFrame shape that silly-kicks'
``convert_to_actions()`` functions expect.

Supported sources:
  - StatsBomb (``adapt_statsbomb_events``, ``resolve_statsbomb_home_team_ids``)
  - Wyscout   (``adapt_wyscout_events``,   ``resolve_wyscout_home_team_ids``)
  - IDSSE     (``adapt_idsse_events_for_silly_kicks``)
  - Metrica   (``adapt_metrica_events_for_silly_kicks``)
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
# present natively (the DFL XML attribute names map identically). The
# adapter therefore is a near-identity passthrough — it only ensures the
# required columns are present + correctly typed. The match-level metadata
# (competition_native_id, season_native_id, home_team_id_native, etc.) is
# loaded from the bronze.idsse_events Path-B-added columns by the UDF.
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


def adapt_idsse_events_for_silly_kicks(events_pdf: pd.DataFrame) -> pd.DataFrame:
    """Convert bronze ``idsse_events`` rows to silly-kicks 1.7.0 sportec input.

    Near-identity passthrough — bronze already stores the column names
    silly-kicks expects (``match_id, event_id, event_type, period,
    timestamp_seconds, player_id, team, x, y`` + optional qualifier columns
    via the DFL ``_RECOGNIZED_QUALIFIER_COLUMNS`` set). Returns a copy so
    silly-kicks's internal mutations don't leak back to the caller.

    Args:
        events_pdf: DataFrame read from the ``bronze.idsse_events`` Delta
            table.

    Returns:
        Adapted DataFrame ready for ``silly_kicks.spadl.sportec.
        convert_to_actions(events, home_team_id='home')``.
    """
    # silly-kicks's converter mutates+writes intermediate columns on its
    # input — return a copy to honor the "input not mutated" contract that
    # silly-kicks's own tests assert.
    return events_pdf.copy()


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
