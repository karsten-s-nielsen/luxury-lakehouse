"""A systemd user timer, or cron without a user systemd, for real (marker `linux`; spec §18, §23.4; plan Task 24):
an install, its status and its uninstall on this host, under a test-only name."""

from __future__ import annotations

import subprocess
import uuid

import pytest

from lakehouse.schedule.base import ScheduleBackend, resolve_tool
from lakehouse.schedule.cron import CronBackend
from lakehouse.schedule.systemd import SystemdBackend

pytestmark = pytest.mark.linux


def _backend(name: str) -> ScheduleBackend:
    try:
        systemctl = resolve_tool("systemctl")
    except FileNotFoundError:
        return CronBackend(name=name)
    user_systemd = subprocess.run(  # noqa: S603 - an absolute system tool, no shell
        [systemctl, "--user", "show-environment"], shell=False, check=False, capture_output=True, text=True
    )
    return SystemdBackend(name=name) if user_systemd.returncode == 0 else CronBackend(name=name)


def test_install_status_uninstall() -> None:
    backend = _backend(f"lakehouse-v2-test-{uuid.uuid4().hex[:8]}")
    try:
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})
        backend.install("0 3 * * *", system=False, env={"LAKEHOUSE_TEST": "1"})  # idempotent
        status = backend.status()
        assert (status.installed, status.cron) == (True, "0 3 * * *")
    finally:
        backend.uninstall()
    assert not backend.status().installed
