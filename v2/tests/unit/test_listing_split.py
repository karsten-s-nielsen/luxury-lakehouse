"""Source-level listings split by record visibility (spec §10.5)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from lakehouse.bronze.listings import split_listing


def _players(visibility: list[str | None]) -> pa.Table:
    return pa.table({"player_id": [f"p{i}" for i in range(len(visibility))], "visibility": visibility})


def test_mixed_listing_split() -> None:
    records = _players(["public", "private", "public", "private", "public"])
    parts = split_listing(records, visibility_column="visibility", source_visibility="private")
    assert sorted(parts) == ["lake", "restricted"]
    assert parts["lake"]["player_id"].to_pylist() == ["p0", "p2", "p4"]
    assert parts["restricted"]["player_id"].to_pylist() == ["p1", "p3"]


def test_public_only_listing_has_no_restricted_part() -> None:
    parts = split_listing(_players(["public", "public"]), visibility_column="visibility", source_visibility="private")
    assert list(parts) == ["lake"]


def test_listing_without_flags_follows_source() -> None:
    records = pa.table({"match_id": ["1", "2"]})
    assert list(split_listing(records, visibility_column=None, source_visibility="public")) == ["lake"]
    assert list(split_listing(records, visibility_column=None, source_visibility="private")) == ["restricted"]


def test_missing_flag_follows_source() -> None:
    parts = split_listing(_players(["public", None]), visibility_column="visibility", source_visibility="private")
    assert parts["restricted"]["player_id"].to_pylist() == ["p1"]


def test_unknown_visibility_value_refused() -> None:
    with pytest.raises(ValueError, match="secret"):
        split_listing(_players(["public", "secret"]), visibility_column="visibility", source_visibility="public")


def test_empty_listing_gives_no_parts() -> None:
    assert split_listing(_players([]), visibility_column="visibility", source_visibility="public") == {}
