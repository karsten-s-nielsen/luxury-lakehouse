"""Pure-function tests for the ADR-063 H4 staleness monitor."""

from __future__ import annotations

from ingestion.staleness_monitor import StaleArtifact, find_stale_artifacts


def test_flags_workflow_behind_upstream() -> None:
    stored = [("wf-action-context", "cat.bronze.expected_threat_grids", 5)]
    current = {"cat.bronze.expected_threat_grids": 9}
    stale = find_stale_artifacts(stored, current)
    assert stale == [
        StaleArtifact(
            workflow_id="wf-action-context",
            upstream_table="cat.bronze.expected_threat_grids",
            recorded_version=5,
            current_version=9,
        )
    ]
    assert stale[0].lag == 4


def test_up_to_date_is_not_flagged() -> None:
    stored = [("wf-x", "cat.t", 9)]
    assert find_stale_artifacts(stored, {"cat.t": 9}) == []


def test_recorded_ahead_is_not_flagged() -> None:
    # Defensive: a recorded version >= current is never stale.
    stored = [("wf-x", "cat.t", 11)]
    assert find_stale_artifacts(stored, {"cat.t": 9}) == []


def test_unknown_current_version_skipped() -> None:
    # An upstream with no readable data version (None) must not be flagged.
    stored = [("wf-x", "cat.t", 5)]
    assert find_stale_artifacts(stored, {"cat.t": None}) == []


def test_multiple_mixed() -> None:
    stored = [
        ("wf-a", "cat.t1", 3),  # stale (current 4)
        ("wf-b", "cat.t2", 7),  # fresh
        ("wf-c", "cat.t3", 1),  # upstream unknown → skip
    ]
    current = {"cat.t1": 4, "cat.t2": 7, "cat.t3": None}
    stale = find_stale_artifacts(stored, current)
    assert [s.workflow_id for s in stale] == ["wf-a"]
