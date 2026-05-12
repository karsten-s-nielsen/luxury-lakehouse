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


def test_guard_chunk_sizes_are_set() -> None:
    """_TrackingContextGuard has provider-specific chunk sizes."""
    from ingestion.tracking_context import skip_guard

    assert hasattr(skip_guard, "chunk_sizes")
    assert skip_guard.chunk_sizes["idsse"] == 1
    assert skip_guard.chunk_sizes["metrica"] == 2
    assert skip_guard.chunk_sizes["skillcorner"] == 2
