"""Unit tests for the synced-table health-state predicate (T4 CI-failure fix, 2026-09-20).

The live health check (`tests/data_quality/test_synced_tables_online.py`) and the refresh-completion
wait (`refresh_synced_tables.wait_until_online`) both decide "is this synced table healthy?" from a
single pure predicate, so the semantics are testable without a warehouse. These tests pin that the
idle steady-state `SYNCED_TABLE_ONLINE` is accepted (the false-positive that turned the scheduled
Data Quality CI + Lakebase Maintenance jobs red) WHILE genuinely-degraded states still fail — i.e.
the guard is broadened, not defeated.
"""

from __future__ import annotations

from ingestion.refresh_synced_tables import (
    _SYNCED_TABLE_TERMINAL_FAILURE_STATES,
    HEALTHY_ONLINE_STATES,
    SYNCED_TABLE_ONLINE_STATE,
    is_synced_table_healthy,
)

# States a healthy, fully-synced TRIGGERED table reports.
_HEALTHY = ("SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE", "SYNCED_TABLE_ONLINE")

# States that MUST still fail — the guard is non-vacuous. Includes offline/terminal-failure,
# provisioning, and actively-updating (not fully synced), plus the null/unknown sentinels.
_UNHEALTHY = (
    "SYNCED_TABLE_OFFLINE",
    "SYNCED_TABLE_OFFLINE_FAILED",
    "SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
    "SYNCED_TABLE_PROVISIONING",
    "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
    "SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE",
    "UNKNOWN",
    None,
)


def test_healthy_idle_and_post_creation_states_accepted() -> None:
    for state in _HEALTHY:
        assert is_synced_table_healthy(state), f"{state} should be healthy"
    # The idle-decay state is the one the too-strict check rejected — it MUST now pass.
    assert is_synced_table_healthy("SYNCED_TABLE_ONLINE")


def test_degraded_states_still_fail() -> None:
    for state in _UNHEALTHY:
        assert not is_synced_table_healthy(state), f"{state} must NOT be treated as healthy"


def test_guard_is_not_vacuous() -> None:
    # Exactly the two documented healthy states — no accidental widening.
    assert HEALTHY_ONLINE_STATES == frozenset(_HEALTHY)
    assert SYNCED_TABLE_ONLINE_STATE in HEALTHY_ONLINE_STATES
    # Healthy and terminal-failure sets are disjoint — a failure can never read as healthy.
    assert HEALTHY_ONLINE_STATES.isdisjoint(_SYNCED_TABLE_TERMINAL_FAILURE_STATES)
