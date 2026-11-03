"""Task Scheduler for real (marker `windows`; spec §18, §23.4; plan Task 24): a user-level install, its status and
its uninstall on this host, under a test-only task name."""

from __future__ import annotations

import uuid

import pytest

from lakehouse.schedule.windows import WindowsBackend

pytestmark = pytest.mark.windows


def test_install_status_uninstall() -> None:
    backend = WindowsBackend(name=f"lakehouse-v2-test-{uuid.uuid4().hex[:8]}")
    try:
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})  # idempotent
        status = backend.status()
        assert (status.installed, status.enabled, status.cron) == (True, True, "0 3 * * *")
        assert status.next_run is not None and (status.next_run.hour, status.next_run.minute) == (3, 0)
    finally:
        backend.uninstall()
    assert not backend.status().installed
