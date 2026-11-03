"""launchd for real (marker `macos`; spec §18, §23.4; plan Task 24): a LaunchAgent install, its status and its
uninstall on this host, under a test-only label."""

from __future__ import annotations

import uuid

import pytest

from lakehouse.schedule.launchd import LaunchdBackend

pytestmark = pytest.mark.macos


def test_install_status_uninstall() -> None:
    backend = LaunchdBackend(name=f"com.lakehouse.v2-test-{uuid.uuid4().hex[:8]}")
    try:
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})  # idempotent
        status = backend.status()
        assert (status.installed, status.enabled, status.cron) == (True, True, "0 3 * * *")
    finally:
        backend.uninstall()
    assert not backend.status().installed
