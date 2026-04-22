"""Assert statsbomb.py exposes module-level expected-col constants that
match the bronze schema snapshot.

Ships the constants as a machine-checked source of truth for what
``finalize_bronze_df`` protects against NullType drops. G1a of the PR
#173 bronze drop-safety sweep.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_FIXTURE = Path(__file__).parent / "fixtures" / "statsbomb_bronze_schema_snapshot.json"


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


_AUDIT_ONLY_COLS = frozenset({"_ingested_at"})


def _expected_names(snapshot: dict, table: str) -> set[str]:
    return {entry["name"] for entry in snapshot["tables"][table] if entry["name"] not in _AUDIT_ONLY_COLS}


def test_competitions_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_COMPETITIONS_EXPECTED_COLS

    assert set(_STATSBOMB_COMPETITIONS_EXPECTED_COLS) == _expected_names(snapshot, "statsbomb_competitions")


def test_matches_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_MATCHES_EXPECTED_COLS

    assert set(_STATSBOMB_MATCHES_EXPECTED_COLS) == _expected_names(snapshot, "statsbomb_matches")


def test_events_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_EVENTS_EXPECTED_COLS

    assert set(_STATSBOMB_EVENTS_EXPECTED_COLS) == _expected_names(snapshot, "statsbomb_events")


def test_lineups_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_LINEUPS_EXPECTED_COLS

    assert set(_STATSBOMB_LINEUPS_EXPECTED_COLS) == _expected_names(snapshot, "statsbomb_lineups")


def test_360_expected_cols_matches_snapshot(snapshot: dict) -> None:
    from ingestion.statsbomb import _STATSBOMB_360_EXPECTED_COLS

    assert set(_STATSBOMB_360_EXPECTED_COLS) == _expected_names(snapshot, "statsbomb_360")


def test_dtype_overrides_match_snapshot_types(snapshot: dict) -> None:
    """Non-string columns in snapshot map to pandas nullable dtypes."""
    from ingestion.statsbomb import (
        _STATSBOMB_360_DTYPE_OVERRIDES,
        _STATSBOMB_COMPETITIONS_DTYPE_OVERRIDES,
        _STATSBOMB_EVENTS_DTYPE_OVERRIDES,
        _STATSBOMB_LINEUPS_DTYPE_OVERRIDES,
        _STATSBOMB_MATCHES_DTYPE_OVERRIDES,
    )

    # Map Spark type to expected pandas nullable dtype; strings are absent
    # from overrides (finalize_bronze_df defaults to "string").
    pandas_for = {"bigint": "Int64", "int": "Int64", "double": "Float64", "float": "Float64", "boolean": "boolean"}

    for table, overrides in (
        ("statsbomb_competitions", _STATSBOMB_COMPETITIONS_DTYPE_OVERRIDES),
        ("statsbomb_matches", _STATSBOMB_MATCHES_DTYPE_OVERRIDES),
        ("statsbomb_events", _STATSBOMB_EVENTS_DTYPE_OVERRIDES),
        ("statsbomb_lineups", _STATSBOMB_LINEUPS_DTYPE_OVERRIDES),
        ("statsbomb_360", _STATSBOMB_360_DTYPE_OVERRIDES),
    ):
        expected_overrides = {
            entry["name"]: pandas_for[entry["type"]]
            for entry in snapshot["tables"][table]
            if entry["type"] in pandas_for and entry["name"] not in _AUDIT_ONLY_COLS
        }
        assert overrides == expected_overrides, (
            f"[{table}] dtype overrides drift:\n  module:   {overrides}\n  expected: {expected_overrides}"
        )
