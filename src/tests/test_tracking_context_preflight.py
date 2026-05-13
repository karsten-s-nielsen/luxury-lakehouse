"""Tests for tracking context preflight chunking and --match-ids parsing."""

from __future__ import annotations


def test_parse_tracking_match_ids_arg_none() -> None:
    """None input returns None (no filter)."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg(None) is None


def test_parse_tracking_match_ids_arg_empty() -> None:
    """Empty string returns None."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg("") is None


def test_parse_tracking_match_ids_arg_valid() -> None:
    """Comma-separated string returns parsed (provider, ids) tuple."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("idsse:J03WMX,J03WN1")
    assert result is not None
    assert result == ("idsse", ["J03WMX", "J03WN1"])


def test_parse_tracking_match_ids_arg_single() -> None:
    """Single match ID works."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("metrica:match_001")
    assert result is not None
    assert result == ("metrica", ["match_001"])


def test_parse_tracking_match_ids_arg_bad_format() -> None:
    """Missing provider prefix raises SystemExit."""
    import pytest

    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    with pytest.raises(SystemExit, match="must be 'provider:id1,id2'"):
        _parse_tracking_match_ids_arg("J03WMX,J03WN1")


def test_parse_tracking_match_ids_arg_unknown_provider() -> None:
    """Unknown provider raises SystemExit."""
    import pytest

    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    with pytest.raises(SystemExit, match="Unknown provider"):
        _parse_tracking_match_ids_arg("opta:12345")


def test_chunk_encoding_round_trip() -> None:
    """Chunk string 'provider:id1,id2' round-trips through parse."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    chunk_str = "idsse:J03WMX,J03WN1"
    result = _parse_tracking_match_ids_arg(chunk_str)
    assert result is not None
    provider, ids = result
    assert provider == "idsse"
    assert ids == ["J03WMX", "J03WN1"]
    reconstructed = f"{provider}:{','.join(ids)}"
    assert reconstructed == chunk_str


def test_serialize_xt_grid_produces_valid_task_value() -> None:
    """_serialize_xt_grid output is JSON-serializable with expected keys."""
    import json

    import numpy as np

    from ingestion.tracking_context import _serialize_xt_grid

    grid = np.ones((12, 16), dtype=np.float64)
    data = _serialize_xt_grid(grid, grid_l=16, grid_w=12)

    # Must be JSON-serializable (Databricks task values are JSON)
    json_str = json.dumps(data)
    restored = json.loads(json_str)

    assert restored["l"] == 16
    assert restored["w"] == 12
    assert len(restored["xt_grid"]) == 12
    assert len(restored["xt_grid"][0]) == 16


def test_guard_chunk_sizes_are_set() -> None:
    """_TrackingContextGuard has provider-specific chunk sizes."""
    from ingestion.tracking_context import skip_guard

    assert hasattr(skip_guard, "chunk_sizes")
    assert skip_guard.chunk_sizes["idsse"] == 1
    assert skip_guard.chunk_sizes["metrica"] == 2
    assert skip_guard.chunk_sizes["skillcorner"] == 2


def test_guard_excludes_tracking_without_spadl() -> None:
    """Skip guard excludes tracking matches that have no paired SPADL actions.

    SkillCorner has tracking data but no SPADL converter, so those match IDs
    must not appear in the guard's output — otherwise they'd be rediscovered
    on every run, wasting a serverless driver each time.
    """
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    # find_new_ids returns unprocessed tracking match IDs per provider
    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        if "idsse" in source_table:
            return ["J03WMX", "J03WN1"]
        if "metrica" in source_table:
            return ["Sample_Game_1"]
        if "skillcorner" in source_table:
            return ["sc_match_1", "sc_match_2"]
        return []

    # SPADL actions exist only for IDSSE and Metrica — not SkillCorner
    mock_spadl_ids = {
        "idsse": {"J03WMX", "J03WN1"},
        "metrica": {"Sample_Game_1"},
        # No "skillcorner" key — no SPADL actions
    }

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value=mock_spadl_ids),
        patch("ingestion.guards.ensure_table"),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    # Total should be 3 (2 IDSSE + 1 Metrica), NOT 5
    assert result.count == 3
    assert result.metadata["idsse_ids"] == ["J03WMX", "J03WN1"]
    assert result.metadata["metrica_ids"] == ["Sample_Game_1"]
    assert result.metadata["skillcorner_ids"] == []


def test_guard_returns_zero_when_no_spadl_matches() -> None:
    """Guard returns count=0 when tracking exists but no provider has SPADL."""
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        if "skillcorner" in source_table:
            return ["sc_match_1"]
        return []

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value={}),
        patch("ingestion.guards.ensure_table"),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    assert result.count == 0


def test_guard_partial_spadl_filters_correctly() -> None:
    """Guard filters individual match IDs — keeps only those with SPADL."""
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    # IDSSE has 3 tracking matches, but only 2 have SPADL actions
    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        if "idsse" in source_table:
            return ["J03WMX", "J03WN1", "J03WN2"]
        return []

    mock_spadl_ids = {"idsse": {"J03WMX", "J03WN2"}}  # J03WN1 missing

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value=mock_spadl_ids),
        patch("ingestion.guards.ensure_table"),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    assert result.count == 2
    assert result.metadata["idsse_ids"] == ["J03WMX", "J03WN2"]
