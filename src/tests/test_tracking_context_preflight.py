"""Tests for tracking context preflight chunking and --match-ids parsing."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _mock_pyspark_modules():
    """Inject mock pyspark modules so guard imports succeed without real pyspark."""
    pyspark_mod = MagicMock()
    pyspark_sql_mod = MagicMock()
    pyspark_sql_functions_mod = MagicMock()
    pyspark_sql_mod.functions = pyspark_sql_functions_mod

    # F.col(...).cast(...).alias(...) must chain correctly
    mock_col = MagicMock()
    pyspark_sql_functions_mod.col.return_value = mock_col

    sys.modules.setdefault("pyspark", pyspark_mod)
    sys.modules.setdefault("pyspark.sql", pyspark_sql_mod)
    sys.modules.setdefault("pyspark.sql.functions", pyspark_sql_functions_mod)
    sys.modules.setdefault("pyspark.sql.types", MagicMock())
    sys.modules.setdefault("pyspark.dbutils", MagicMock())

    yield

    # conftest._restore_pyspark_modules handles cleanup


def test_parse_tracking_match_ids_arg_none() -> None:
    """None input returns None (no filter)."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg(None) is None


def test_parse_tracking_match_ids_arg_empty() -> None:
    """Empty string returns None."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    assert _parse_tracking_match_ids_arg("") is None


def test_parse_tracking_match_ids_arg_valid() -> None:
    """Comma-separated string returns parsed (provider, ids, None) tuple."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("idsse:J03WMX,J03WN1")
    assert result is not None
    assert result == ("idsse", ["J03WMX", "J03WN1"], None)


def test_parse_tracking_match_ids_arg_single() -> None:
    """Single match ID works."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("metrica:match_001")
    assert result is not None
    assert result == ("metrica", ["match_001"], None)


def test_parse_tracking_match_ids_arg_with_period() -> None:
    """IDSSE half-game format parses period correctly."""
    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    result = _parse_tracking_match_ids_arg("idsse:J03WMX:1")
    assert result == ("idsse", ["J03WMX"], 1)

    result = _parse_tracking_match_ids_arg("idsse:J03WMX:2")
    assert result == ("idsse", ["J03WMX"], 2)


def test_parse_tracking_match_ids_arg_bad_format() -> None:
    """Missing provider prefix raises SystemExit."""
    import pytest

    from ingestion.tracking_context import _parse_tracking_match_ids_arg

    with pytest.raises(SystemExit, match="must be"):
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

    # Metrica multi-match format
    chunk_str = "metrica:Game1,Game2"
    result = _parse_tracking_match_ids_arg(chunk_str)
    assert result is not None
    provider, ids, period = result
    assert provider == "metrica"
    assert ids == ["Game1", "Game2"]
    assert period is None
    reconstructed = f"{provider}:{','.join(ids)}"
    assert reconstructed == chunk_str

    # IDSSE half-game format
    chunk_str = "idsse:J03WMX:1"
    result = _parse_tracking_match_ids_arg(chunk_str)
    assert result is not None
    provider, ids, period = result
    assert provider == "idsse"
    assert ids == ["J03WMX"]
    assert period == 1
    reconstructed = f"{provider}:{ids[0]}:{period}"
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
    """_TrackingContextGuard has provider-specific chunk sizes for non-IDSSE."""
    from ingestion.tracking_context import skip_guard

    assert hasattr(skip_guard, "chunk_sizes")
    # IDSSE uses half-game chunking (no chunk_sizes entry needed)
    assert skip_guard.chunk_sizes["metrica"] == 2
    assert skip_guard.chunk_sizes["skillcorner"] == 2


def test_guard_excludes_tracking_without_spadl() -> None:
    """Skip guard excludes tracking matches that have no paired SPADL actions.

    SkillCorner has tracking data but no SPADL converter, so those match IDs
    must not appear in the guard's output.
    """
    from contextlib import nullcontext
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    idsse_source_rows = [
        {"match_id": "J03WMX", "period": 1},
        {"match_id": "J03WMX", "period": 2},
        {"match_id": "J03WN1", "period": 1},
        {"match_id": "J03WN1", "period": 2},
    ]

    # find_new_ids for metrica/skillcorner
    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        if "metrica" in source_table:
            return ["Sample_Game_1"]
        if "skillcorner" in source_table:
            return ["sc_match_1", "sc_match_2"]
        return []

    # SPADL actions exist only for IDSSE and Metrica — not SkillCorner
    mock_spadl_ids = {
        "idsse": {"J03WMX", "J03WN1"},
        "metrica": {"Sample_Game_1"},
    }

    def table_side_effect(table_name):
        mock_t = MagicMock()
        mock_t.filter.return_value = mock_t
        mock_t.select.return_value = mock_t
        mock_t.distinct.return_value = mock_t
        if "idsse_tracking" in table_name:
            mock_t.collect.return_value = idsse_source_rows
        else:
            # Results table — nothing done yet
            mock_t.collect.return_value = []
        return mock_t

    mock_spark.table.side_effect = table_side_effect

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value=mock_spadl_ids),
        patch("ingestion.guards.ensure_table"),
        patch("ingestion.utils.tolerate_missing_table", return_value=nullcontext()),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    # IDSSE: 4 halves + Metrica: 1 match = 5, SkillCorner excluded (no SPADL)
    assert result.count == 5
    assert "idsse_halves" in result.metadata
    assert result.metadata["metrica_ids"] == ["Sample_Game_1"]
    assert result.metadata["skillcorner_ids"] == []


def test_guard_returns_zero_when_no_spadl_matches() -> None:
    """Guard returns count=0 when tracking exists but no provider has SPADL."""
    from contextlib import nullcontext
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    def table_side_effect(table_name):
        mock_t = MagicMock()
        mock_t.filter.return_value = mock_t
        mock_t.select.return_value = mock_t
        mock_t.distinct.return_value = mock_t
        mock_t.collect.return_value = []
        return mock_t

    mock_spark.table.side_effect = table_side_effect

    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        if "skillcorner" in source_table:
            return ["sc_match_1"]
        return []

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value={}),
        patch("ingestion.guards.ensure_table"),
        patch("ingestion.utils.tolerate_missing_table", return_value=nullcontext()),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    assert result.count == 0


def test_guard_partial_spadl_filters_correctly() -> None:
    """Guard filters individual match IDs — keeps only those with SPADL."""
    from contextlib import nullcontext
    from unittest.mock import MagicMock, patch

    from ingestion.tracking_context import _TrackingContextGuard

    guard = _TrackingContextGuard()
    mock_spark = MagicMock()

    # IDSSE has 3 tracking matches (6 halves), but only 2 have SPADL actions
    idsse_source_rows = [
        {"match_id": "J03WMX", "period": 1},
        {"match_id": "J03WMX", "period": 2},
        {"match_id": "J03WN1", "period": 1},
        {"match_id": "J03WN1", "period": 2},
        {"match_id": "J03WN2", "period": 1},
        {"match_id": "J03WN2", "period": 2},
    ]

    mock_spadl_ids = {"idsse": {"J03WMX", "J03WN2"}}  # J03WN1 missing

    def table_side_effect(table_name):
        mock_t = MagicMock()
        mock_t.filter.return_value = mock_t
        mock_t.select.return_value = mock_t
        mock_t.distinct.return_value = mock_t
        if "idsse_tracking" in table_name:
            mock_t.collect.return_value = idsse_source_rows
        else:
            mock_t.collect.return_value = []
        return mock_t

    mock_spark.table.side_effect = table_side_effect

    def mock_find_new_ids(_spark, source_table, _results_table, **_kw):
        return []

    with (
        patch("ingestion.guards.find_new_ids", side_effect=mock_find_new_ids),
        patch("ingestion.tracking_context._spadl_match_ids_by_provider", return_value=mock_spadl_ids),
        patch("ingestion.guards.ensure_table"),
        patch("ingestion.utils.tolerate_missing_table", return_value=nullcontext()),
    ):
        result = guard.check(mock_spark, "soccer_analytics", "bronze")

    # J03WMX (2 halves) + J03WN2 (2 halves) = 4
    assert result.count == 4
    assert len(result.metadata["idsse_halves"]) == 4
