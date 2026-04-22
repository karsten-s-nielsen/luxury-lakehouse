"""Assert wyscout.py exposes module-level expected-col constants that
match the bronze schema snapshot.

Companion to test_statsbomb_bronze_expected_cols.py. G1c of the PR #173
bronze drop-safety sweep.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_FIXTURE = Path(__file__).parent / "fixtures" / "wyscout_bronze_schema_snapshot.json"


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


_AUDIT_ONLY_COLS = frozenset({"_ingested_at"})


def _expected_names(snapshot: dict, table: str) -> set[str]:
    return {entry["name"] for entry in snapshot["tables"][table] if entry["name"] not in _AUDIT_ONLY_COLS}


def test_events_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_EVENTS_EXPECTED_COLS

    assert set(_WYSCOUT_EVENTS_EXPECTED_COLS) == _expected_names(snapshot, "wyscout_events")


def test_matches_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_MATCHES_EXPECTED_COLS

    assert set(_WYSCOUT_MATCHES_EXPECTED_COLS) == _expected_names(snapshot, "wyscout_matches")


def test_players_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.wyscout import _WYSCOUT_PLAYERS_EXPECTED_COLS

    assert set(_WYSCOUT_PLAYERS_EXPECTED_COLS) == _expected_names(snapshot, "wyscout_players")


def test_dtype_overrides_match_snapshot_types(snapshot: dict) -> None:
    """Non-string columns in snapshot map to pandas nullable dtypes."""
    from ingestion.wyscout import (
        _WYSCOUT_EVENTS_DTYPE_OVERRIDES,
        _WYSCOUT_MATCHES_DTYPE_OVERRIDES,
        _WYSCOUT_PLAYERS_DTYPE_OVERRIDES,
    )

    pandas_for = {"bigint": "Int64", "int": "Int64", "double": "Float64", "float": "Float64", "boolean": "boolean"}

    for table, overrides in (
        ("wyscout_events", _WYSCOUT_EVENTS_DTYPE_OVERRIDES),
        ("wyscout_matches", _WYSCOUT_MATCHES_DTYPE_OVERRIDES),
        ("wyscout_players", _WYSCOUT_PLAYERS_DTYPE_OVERRIDES),
    ):
        expected_overrides = {
            entry["name"]: pandas_for[entry["type"]]
            for entry in snapshot["tables"][table]
            if entry["type"] in pandas_for and entry["name"] not in _AUDIT_ONLY_COLS
        }
        assert overrides == expected_overrides, (
            f"[{table}] dtype overrides drift:\n  module:   {overrides}\n  expected: {expected_overrides}"
        )
