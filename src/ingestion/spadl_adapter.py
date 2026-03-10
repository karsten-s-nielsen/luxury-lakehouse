"""Adapters to transform bronze event tables into socceraction-compatible DataFrames.

The SPADL/VAEP pipeline reads from existing bronze Delta tables instead of
re-fetching from external APIs.  These adapters bridge the gap between the
serialized bronze column layout and the DataFrame shape that socceraction's
``convert_to_actions()`` functions expect.

Supported sources:
  - StatsBomb (``adapt_statsbomb_events``, ``resolve_statsbomb_home_team_ids``)
  - Wyscout   (``adapt_wyscout_events``,   ``resolve_wyscout_home_team_ids``)
"""

from __future__ import annotations

import json

import pandas as pd

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
    """Convert bronze ``statsbomb_events`` rows to socceraction input format.

    Column mapping:
        ``match_id`` -> ``game_id``, ``id`` -> ``event_id``,
        ``period`` -> ``period_id``, ``type`` -> ``type_name``,
        ``_raw_extra_json`` -> ``extra`` (dict)

    Args:
        events_pdf: DataFrame read from the ``statsbomb_events`` bronze table.
        home_team_id: The home team's ``team_id`` for this game
            (used by ``spadl_sb.convert_to_actions``).

    Returns:
        Adapted DataFrame ready for ``socceraction.spadl.statsbomb.convert_to_actions``.
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

    # Ensure timestamp is timedelta (socceraction expects this)
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
    """Convert bronze ``wyscout_events`` rows to socceraction input format.

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
        Adapted DataFrame ready for ``socceraction.spadl.wyscout.convert_to_actions``.
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
