"""Unit tests for the AC-1 for-each iteration observability helpers.

``_iteration_fingerprint`` + ``_iteration_summary`` emit structured single-line
JSON records (`AC1_FINGERPRINT` / `AC1_SUMMARY`) at the start + end of every
``compute_action_context_iteration`` task so ops can grep/aggregate them
without opening the Databricks UI per-iteration.

Critical invariants tested:
1. Same fingerprint_hash for both start + end records (joinable in log aggregation).
2. fingerprint_hash is deterministic over (provider, ids, period) — reruns with
   identical inputs share the same hash for diffing.
3. zero-row matches surface in the summary so silent-drop cases are visible
   without per-line log inspection.
"""

from __future__ import annotations

from typing import cast

import pytest

pytest.importorskip("databricks.sdk")  # action_context module-level imports it

from ingestion.action_context import _iteration_fingerprint, _iteration_summary


def test_fingerprint_has_all_required_fields() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    required = {
        "event",
        "fingerprint_hash",
        "provider",
        "n_match_ids",
        "match_ids_sample",
        "period_filter",
        "catalog",
        "schema",
        "silly_kicks_version",
        "wheel_version",
        "databricks_run_id",
        "databricks_task_run_id",
    }
    assert required <= set(fp.keys()), f"missing fields: {required - set(fp.keys())}"
    assert fp["event"] == "ac1_iteration_start"
    assert fp["provider"] == "idsse"
    assert fp["n_match_ids"] == 2
    assert fp["period_filter"] == 1


def test_fingerprint_is_deterministic_over_inputs() -> None:
    """Same inputs → same hash (joinable on rerun for diff)."""
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    b = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    assert a["fingerprint_hash"] == b["fingerprint_hash"]


def test_fingerprint_hash_is_input_order_independent() -> None:
    """Match id list order should not affect the hash — same logical input."""
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["A", "B"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    b = _iteration_fingerprint(
        provider="idsse",
        ids=["B", "A"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    assert a["fingerprint_hash"] == b["fingerprint_hash"]


def test_fingerprint_hash_changes_when_provider_changes() -> None:
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["X"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    b = _iteration_fingerprint(
        provider="metrica",
        ids=["X"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert a["fingerprint_hash"] != b["fingerprint_hash"]


def test_fingerprint_sample_caps_at_3_ids() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=[f"M{i}" for i in range(10)],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert len(cast(list[str], fp["match_ids_sample"])) == 3
    assert fp["n_match_ids"] == 10


def test_fingerprint_sample_returns_all_when_few_ids() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["A", "B"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert fp["match_ids_sample"] == ["A", "B"]


def test_summary_pairs_with_fingerprint_via_hash() -> None:
    """The summary record carries the same fingerprint_hash so log aggregation can JOIN."""
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["X"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash=str(fp["fingerprint_hash"]),
        per_match_written={"X": 100},
        elapsed_seconds=5.0,
    )
    assert summary["fingerprint_hash"] == fp["fingerprint_hash"]
    assert summary["event"] == "ac1_iteration_end"


def test_summary_aggregates_rows_correctly() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 50, "C": 0},
        elapsed_seconds=10.5,
    )
    assert summary["n_matches_processed"] == 3
    assert summary["total_rows_written"] == 150
    assert summary["elapsed_seconds"] == 10.5


def test_summary_surfaces_zero_row_matches() -> None:
    """Silent-drop cases must surface in the summary, not vanish."""
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 0, "C": 0, "D": 50},
        elapsed_seconds=1.0,
    )
    assert summary["n_matches_zero_rows"] == 2
    assert summary["zero_row_match_sample"] == ["B", "C"]


def test_summary_zero_row_sample_caps_at_3() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={f"M{i}": 0 for i in range(10)},
        elapsed_seconds=1.0,
    )
    assert len(cast(list[str], summary["zero_row_match_sample"])) == 3
    assert summary["n_matches_zero_rows"] == 10


def test_summary_empty_zero_row_sample_when_all_succeed() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 50},
        elapsed_seconds=1.0,
    )
    assert summary["n_matches_zero_rows"] == 0
    assert summary["zero_row_match_sample"] == []


# The _BatchHeartbeat class (PR #320) was removed in favour of executor-side
# per-batch logging from inside the UDF closure. See ADR-031 for the rationale:
# Databricks serverless / Spark Connect forbids spark.sparkContext access, so
# the driver-aggregated LongAccumulator design was structurally incompatible
# with the only environment AC-1 actually runs in. The executor-side log
# (AC1_BATCH provider=... match_id=... batch_id=... elapsed_s=...) provides
# equivalent per-batch operator visibility without the Connect violation.
