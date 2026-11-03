"""A systemd timer on Linux (spec §17.5, §18; plan Task 24).

User mode writes `~/.config/systemd/user/<name>.{service,timer}` and runs `systemctl --user`; system mode writes
`/etc/systemd/system/` with `User=` and `Group=` of the operator, never root (§20). `Persistent=true` runs one late
run after a boot or wake that missed the time, never a backlog (§17.5). A cron expression with both day fields
restricted becomes two `OnCalendar=` lines, because cron runs when either day field matches.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Final

from lakehouse.limits import OPEN_FILES_TARGET
from lakehouse.schedule.base import (
    STOP_GRACE_SECONDS,
    TASK_NAME,
    CronSpec,
    ScheduleStatus,
    check_env,
    check_run_as,
    command_line,
    default_command,
    next_run,
    parse_cron,
    resolve_tool,
)

__all__ = ["SystemdBackend", "render_units"]

_WEEKDAYS: Final = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
_HARD_OPEN_FILES: Final = 524_288  # systemd's default hard RLIMIT_NOFILE since version 240
_CRON: Final = re.compile(r"\(cron: (.+)\)$", re.MULTILINE)
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _list(values: tuple[int, ...], every: tuple[int, ...]) -> str:
    return "*" if values == every else ",".join(f"{value:02d}" for value in values)


def _calendars(spec: CronSpec) -> list[str]:
    months = _list(spec.months, tuple(range(1, 13)))
    clock = f"{_list(spec.hours, tuple(range(24)))}:{_list(spec.minutes, tuple(range(60)))}:00"
    days = _list(spec.days, tuple(range(1, 32)))
    weekdays = ",".join(_WEEKDAYS[d] for d in sorted(spec.weekdays, key=lambda d: (d == 0, d)))
    if spec.dom_any and spec.dow_any:
        return [f"*-{months}-* {clock}"]
    if spec.dom_any:
        return [f"{weekdays} *-{months}-* {clock}"]
    if spec.dow_any:
        return [f"*-{months}-{days} {clock}"]
    return [f"*-{months}-{days} {clock}", f"{weekdays} *-{months}-* {clock}"]


def _environment(key: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'Environment="{key}={escaped}"'


def render_units(
    cron: str,
    *,
    name: str,
    command: Sequence[str],
    env: Mapping[str, str],
    system: bool,
    run_as: str | None,
) -> tuple[str, str]:
    """The service and the timer unit."""
    spec = parse_cron(cron)
    checked = check_env(env)
    lines = [
        "[Unit]",
        "Description=lakehouse v2 ingestion",
        "",
        "[Service]",
        "Type=oneshot",
        f"ExecStart={command_line(command)}",
        f"TimeoutStopSec={STOP_GRACE_SECONDS}",  # SIGTERM to SIGKILL: a stopped run publishes its pending work (T3 D1)
        # soft open-file limit for DuckLake scans; hard = systemd's own default, so nothing is lowered (T12 spec D2)
        f"LimitNOFILE={OPEN_FILES_TARGET}:{_HARD_OPEN_FILES}",
        *(_environment(key, value) for key, value in checked.items()),
    ]
    if system:
        user, _, group = (check_run_as(run_as, system=True) or "").partition(":")
        lines += [f"User={user}", f"Group={group or user}"]
    timer = [
        "[Unit]",
        f"Description=lakehouse v2 ingestion schedule (cron: {spec.text})",
        "",
        "[Timer]",
        *(f"OnCalendar={calendar}" for calendar in _calendars(spec)),
        "Persistent=true",
        f"Unit={name}.service",
        "",
        "[Install]",
        "WantedBy=timers.target",
    ]
    return "\n".join(lines) + "\n", "\n".join(timer) + "\n"


class SystemdBackend:
    """A systemd service and timer through `systemctl` (never a shell)."""

    def __init__(
        self,
        *,
        name: str = TASK_NAME,
        command: Sequence[str] | None = None,
        run: Runner = subprocess.run,
        tool: Callable[[str], str] = resolve_tool,
        home: Path | None = None,
        system_dir: Path = Path("/etc/systemd/system"),
    ) -> None:
        self._name = name
        self._command = list(command) if command is not None else default_command()
        self._run = run
        self._tool = tool
        self._home = home if home is not None else Path.home()
        self._system_dir = system_dir

    def _place(self, system: bool) -> tuple[Path, list[str]]:
        if system:
            return self._system_dir, [self._tool("systemctl")]
        return self._home / ".config" / "systemd" / "user", [self._tool("systemctl"), "--user"]

    def _systemctl(self, base: list[str], *args: str, check: bool) -> subprocess.CompletedProcess[str]:
        return self._run([*base, *args], shell=False, check=check, capture_output=True, text=True)

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        service, timer = render_units(
            cron, name=self._name, command=self._command, env=env, system=system, run_as=run_as
        )
        folder, base = self._place(system)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{self._name}.service").write_text(service, encoding="utf-8")
        (folder / f"{self._name}.timer").write_text(timer, encoding="utf-8")
        self._systemctl(base, "daemon-reload", check=True)
        self._systemctl(base, "enable", "--now", f"{self._name}.timer", check=True)

    def uninstall(self) -> None:
        for system in (False, True):
            folder, base = self._place(system)
            timer = folder / f"{self._name}.timer"
            if timer.exists():
                self._systemctl(base, "disable", "--now", f"{self._name}.timer", check=False)
                timer.unlink()
                (folder / f"{self._name}.service").unlink(missing_ok=True)
                self._systemctl(base, "daemon-reload", check=False)

    def status(self) -> ScheduleStatus:
        for system in (False, True):
            folder, base = self._place(system)
            timer = folder / f"{self._name}.timer"
            if not timer.exists():
                continue
            found = _CRON.search(timer.read_text(encoding="utf-8"))
            cron = found.group(1) if found else None
            enabled = (
                self._systemctl(base, "is-enabled", f"{self._name}.timer", check=False).stdout.strip() == "enabled"
            )
            result = self._systemctl(base, "show", "-p", "Result", "--value", f"{self._name}.service", check=False)
            upcoming = next_run(parse_cron(cron), datetime.now().astimezone()) if cron and enabled else None
            return ScheduleStatus("systemd", True, enabled, cron, upcoming, result.stdout.strip() or None)
        return ScheduleStatus("systemd", False, False, None, None, None)
