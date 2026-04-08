"""Tests for FilterResult and SkipGuard protocol."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from ingestion.guards import FilterResult


class TestFilterResult:
    """FilterResult is a frozen dataclass with JSON round-trip support."""

    def test_skip_when_count_zero(self) -> None:
        """count=0 signals the freshness gate to skip this workflow."""
        result = FilterResult(workflow_id="wf-spadl", count=0)
        assert result.count == 0
        assert result.chunks is None
        assert result.metadata == {}

    def test_single_task_when_no_chunks(self) -> None:
        """count>0 with chunks=None means single-task execution."""
        result = FilterResult(workflow_id="wf-xg", count=5)
        assert result.count == 5
        assert result.chunks is None

    def test_fan_out_with_chunks(self) -> None:
        """chunks list triggers for_each_task fan-out."""
        chunks = [["m1", "m2"], ["m3", "m4"], ["m5"]]
        result = FilterResult(workflow_id="wf-pausa", count=5, chunks=chunks)
        assert result.chunks is not None
        assert len(result.chunks) == 3
        assert result.chunks[0] == ["m1", "m2"]

    def test_metadata_passthrough(self) -> None:
        """Arbitrary metadata dict is preserved for downstream pipelines."""
        meta = {"need_global": True, "competitions": [43, 11]}
        result = FilterResult(workflow_id="wf-defcon", count=12, metadata=meta)
        assert result.metadata["need_global"] is True
        assert result.metadata["competitions"] == [43, 11]

    def test_frozen_immutability(self) -> None:
        """Frozen dataclass prevents accidental mutation."""
        result = FilterResult(workflow_id="wf-spadl", count=3)
        with pytest.raises(FrozenInstanceError):
            result.count = 10  # type: ignore[misc]

    def test_json_round_trip(self) -> None:
        """to_json/from_json preserves all fields."""
        original = FilterResult(
            workflow_id="wf-pausa",
            count=7,
            chunks=[["m1", "m2"], ["m3"]],
            metadata={"need_global": False},
        )
        restored = FilterResult.from_json(original.to_json())
        assert restored == original

    def test_json_round_trip_skip(self) -> None:
        """Round-trip works for skip results (count=0, no chunks)."""
        original = FilterResult(workflow_id="wf-xg", count=0)
        restored = FilterResult.from_json(original.to_json())
        assert restored == original

    def test_manual_json_interop(self) -> None:
        """Consumers that don't use FilterResult can parse the JSON directly."""
        result = FilterResult(
            workflow_id="wf-defcon",
            count=3,
            chunks=[["m1"], ["m2"], ["m3"]],
            metadata={"phase": "silver"},
        )
        raw = result.to_json()
        data = json.loads(raw)

        assert data["workflow_id"] == "wf-defcon"
        assert data["count"] == 3
        assert data["chunks"] == [["m1"], ["m2"], ["m3"]]
        assert data["metadata"]["phase"] == "silver"

    def test_manual_json_construction(self) -> None:
        """FilterResult can be built from hand-crafted JSON (task value consumers)."""
        payload = json.dumps(
            {
                "workflow_id": "wf-xt",
                "count": 2,
                "chunks": None,
                "metadata": {"source": "statsbomb"},
            }
        )
        result = FilterResult.from_json(payload)
        assert result.workflow_id == "wf-xt"
        assert result.count == 2
        assert result.chunks is None
        assert result.metadata["source"] == "statsbomb"


def _row(match_id: str) -> dict[str, str]:
    """Simulate a Spark Row with dict-style access (``row["match_id"]``)."""
    return {"match_id": match_id}


class TestPitchControlGuard:
    """Pitch control skip guard adapter."""

    def test_returns_skip_when_all_processed(self) -> None:
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [_row("m1"), _row("m2")]
        results_rows = [_row("m1"), _row("m2")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                mock_df.select.return_value.distinct.return_value.collect.return_value = results_rows
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 0
        assert result.workflow_id == "wf-pitch-control"

    def test_returns_new_matches_no_fanout(self) -> None:
        """Two new matches — below chunk threshold, no fan-out."""
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [_row("m1"), _row("m2"), _row("m3")]
        results_rows = [_row("m1")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                mock_df.select.return_value.distinct.return_value.collect.return_value = results_rows
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
        assert result.chunks is None  # Only 1 chunk of 2 — no fan-out
        assert set(result.metadata["new_match_ids"]) == {"m2", "m3"}

    def test_returns_chunks_for_fanout(self) -> None:
        """Five new matches at 2/chunk = 3 chunks — fan-out."""
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [_row(f"m{i}") for i in range(1, 7)]
        results_rows = [_row("m1")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                mock_df.select.return_value.distinct.return_value.collect.return_value = results_rows
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 5
        assert result.chunks is not None
        assert len(result.chunks) == 3  # [m2,m3], [m4,m5], [m6]
        assert len(result.chunks[0]) == 2
        assert len(result.chunks[2]) == 1  # Last chunk may be smaller

    def test_returns_all_when_no_results_table(self) -> None:
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [_row("m1"), _row("m2")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                raise Exception("Table not found")
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
        assert result.chunks is None  # Only 1 chunk of 2 — no fan-out
