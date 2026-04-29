#!/usr/bin/env python3
"""One-shot builder for luxury-lakehouse test fixtures.

Currently builds:
    src/tests/fixtures/spadl_3match_statsbomb_for_f1.parquet
        — 3 StatsBomb open-data matches converted to SPADL with native
          ``possession`` preserved. Used by
          test_spadl_enrichments.py::TestBoundaryF1 to validate the
          add_possessions heuristic against StatsBomb's native
          possession_id (boundary-F1 ≥ 0.85).

Re-run only when silly-kicks's StatsBomb converter output shape changes
or fixture matches are swapped.

Usage:
    uv run --extra analytics python scripts/build_test_fixtures.py

Requires network access to GitHub (statsbomb/open-data raw URLs).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

# Three matches across competition classes for diversity:
#   7298  — Women's World Cup 2023 group stage
#   7584  — Champions League knockout (also used as silly-kicks's e2e fixture path)
#   3754058 — Premier League 2015/16 regular season (Leicester vs Bournemouth)
# If any are unavailable, swap before running. List taken from the public
# StatsBomb open-data competitions/matches index.
_MATCH_IDS: list[int] = [7298, 7584, 3754058]

_SB_OPEN_DATA_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
_FIXTURE_OUT = (
    Path(__file__).resolve().parent.parent / "src" / "tests" / "fixtures" / "spadl_3match_statsbomb_for_f1.parquet"
)


def _fetch_match_events(match_id: int) -> list[dict]:
    """Fetch raw StatsBomb event JSON for one match."""
    url = f"{_SB_OPEN_DATA_BASE}/events/{match_id}.json"
    resp = requests.get(url, timeout=(10, 30))
    resp.raise_for_status()
    return json.loads(resp.text)


def _adapt_to_silly_kicks_input(events_raw: list[dict], match_id: int) -> pd.DataFrame:
    """Flatten StatsBomb open-data event JSON into silly-kicks's expected DataFrame shape.

    Mirrors the adapter pattern used in silly-kicks's tests/spadl/test_add_possessions.py
    (the @pytest.mark.e2e fixture loader at test_add_possessions.py:547-596).
    """
    _top_level_keys = {"id", "period", "timestamp", "team", "player", "type", "location"}
    return pd.DataFrame(
        [
            {
                "game_id": match_id,
                "event_id": e.get("id"),
                "period_id": e.get("period"),
                "timestamp": e.get("timestamp"),
                "team_id": (e.get("team") or {}).get("id"),
                "player_id": (e.get("player") or {}).get("id"),
                "type_name": (e.get("type") or {}).get("name"),
                "location": e.get("location"),
                "extra": {k: v for k, v in e.items() if k not in _top_level_keys},
                # preserve_native target — top-level possession sequence number
                "possession": e.get("possession"),
            }
            for e in events_raw
        ]
    )


def _build_one_match(match_id: int) -> pd.DataFrame:
    """Convert one StatsBomb match to SPADL with native possession preserved."""
    from silly_kicks.spadl import statsbomb

    events_raw = _fetch_match_events(match_id)
    adapted = _adapt_to_silly_kicks_input(events_raw, match_id)
    if len(adapted) == 0:
        msg = f"empty events for match_id={match_id}"
        raise RuntimeError(msg)
    home_team_id = int(adapted["team_id"].dropna().iloc[0])
    actions, _report = statsbomb.convert_to_actions(
        adapted,
        home_team_id=home_team_id,
        preserve_native=["possession"],
    )
    actions["match_id"] = match_id
    return actions


def main() -> None:
    print(f"Building {_FIXTURE_OUT.name} from {len(_MATCH_IDS)} matches...")
    all_actions: list[pd.DataFrame] = []
    for mid in _MATCH_IDS:
        print(f"  fetching match {mid}...")
        df = _build_one_match(mid)
        print(f"    -> {len(df):,} SPADL actions")
        all_actions.append(df)

    combined = pd.concat(all_actions, ignore_index=True)
    print(f"  combined: {len(combined):,} actions across {combined['match_id'].nunique()} matches")

    _FIXTURE_OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(_FIXTURE_OUT, index=False)
    print(f"  wrote {_FIXTURE_OUT}")
    print(f"  size: {_FIXTURE_OUT.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
