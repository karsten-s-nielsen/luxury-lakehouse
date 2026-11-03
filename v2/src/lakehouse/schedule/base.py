"""The schedule of an install (spec §17.5, §18; plan Task 24): one schedule for all active modules, off by default.

Locally, v2 registers a native OS task itself — Task Scheduler on Windows, launchd on macOS, a systemd timer on Linux,
or cron where systemd is missing. On HF, the owner-created HF scheduled job is changed, paused, resumed and triggered
(`hf_jobs.py`). Every backend renders its task definition from one parsed cron expression; a missed run is not
replayed, and at most one run starts on wake (§17.5).

System mode (an always-on machine, §18) runs the task as the operator user, never as root (§20), and installing it
never opens the lake. A task definition carries only non-secret variables: a key that looks like a secret raises
before anything is written.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal, Protocol

__all__ = [
    "STOP_GRACE_SECONDS",
    "TASK_NAME",
    "Backend",
    "CronSpec",
    "ScheduleBackend",
    "ScheduleStatus",
    "backend_for_host",
    "check_env",
    "check_run_as",
    "default_command",
    "next_run",
    "parse_cron",
    "resolve_tool",
]

TASK_NAME: Final = "lakehouse-v2-ingestion"
# The time between SIGTERM and SIGKILL that a rendered launchd or systemd task gives a stopping run, which then
# publishes its pending work at the run end (T3 spec D1; owner decision, 2026-10-31). launchd's own default is
# "system-defined" (launchd.plist(5)), so the grace is set explicitly. HF Jobs kills about 2 s after SIGTERM and has no
# setting, so a cancelled HF run is a crash: it loses at most one checkpoint interval, which the next run redoes.
STOP_GRACE_SECONDS: Final = 60
Backend = Literal["windows", "launchd", "systemd", "cron", "hf_jobs"]
_SECRET_KEY: Final = re.compile(r"(^|_)(TOKEN|SECRET|PASSWORD|PASSWD|KEY)(_|$)")
_ENV_KEY: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MACROS: Final = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
_BOUNDS: Final = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
_ITEM: Final = re.compile(r"^(\*|\d+(?:-\d+)?)(?:/(\d+))?$")


@dataclass(frozen=True)
class ScheduleStatus:
    backend: Backend
    installed: bool
    enabled: bool
    cron: str | None
    next_run: datetime | None
    last_result: str | None


class ScheduleBackend(Protocol):
    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None: ...

    def uninstall(self) -> None: ...

    def status(self) -> ScheduleStatus: ...


def check_env(env: Mapping[str, str]) -> dict[str, str]:
    """The variables a task definition may carry: never a secret (§18, §20). A secret-like key raises."""
    checked: dict[str, str] = {}
    for key, value in sorted(env.items()):
        if not _ENV_KEY.match(key):
            raise ValueError(f"{key!r} is not an environment variable name")
        if _SECRET_KEY.search(key.upper()):
            raise ValueError(f"{key} looks like a secret: a task definition never holds one")
        if "\n" in value or "\r" in value or "\x00" in value:
            raise ValueError(f"the value of {key} holds a line break or a NUL")
        checked[key] = value
    return checked


_ROOT_ACCOUNTS: Final = frozenset({"root", "system", "nt authority\\system", "localsystem", "administrator"})


def check_run_as(run_as: str | None, *, system: bool) -> str | None:
    """A system-level task names the operator account it runs as, and never root (§18, §20)."""
    if not system:
        return run_as
    if not run_as:
        raise ValueError("a system-level task needs run_as: the operator account it runs as")
    user = run_as.split(":", 1)[0]
    if user.lower() in _ROOT_ACCOUNTS or user.lower().endswith("\\system"):
        raise ValueError(f"a system-level task never runs as root or SYSTEM, not as {run_as!r}")
    return run_as


_TOOLS: Final = {
    "schtasks": (r"%SystemRoot%\System32\schtasks.exe",),
    "powershell": (r"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe",),
    "launchctl": ("/bin/launchctl",),
    "systemctl": ("/usr/bin/systemctl", "/bin/systemctl"),
    "crontab": ("/usr/bin/crontab", "/bin/crontab"),
}


def resolve_tool(name: str) -> str:
    """The absolute path of a system tool, never a PATH lookup (a planted `schtasks` on PATH is never run)."""
    import os
    from pathlib import Path

    for candidate in _TOOLS[name]:
        path = os.path.expandvars(candidate)
        if Path(path).is_file():
            return path
    raise FileNotFoundError(f"{name} is not at {', '.join(_TOOLS[name])}")


def default_command() -> list[str]:
    """The scheduled command: this interpreter runs the CLI's `run` with the trigger `schedule` (§17.4)."""
    return [sys.executable, "-m", "lakehouse.cli", "run", "--trigger", "schedule"]


@dataclass(frozen=True)
class CronSpec:
    """A parsed five-field cron expression. `dom_any`/`dow_any`: the field was `*`; when both day fields are
    restricted, a day matches when either matches (cron's rule)."""

    text: str
    minutes: tuple[int, ...]
    hours: tuple[int, ...]
    days: tuple[int, ...]
    months: tuple[int, ...]
    weekdays: tuple[int, ...]  # 0 = Sunday ... 6 = Saturday
    dom_any: bool
    dow_any: bool

    def day_matches(self, day: datetime) -> bool:
        if day.month not in self.months:
            return False
        dom = day.day in self.days
        dow = (day.isoweekday() % 7) in self.weekdays
        if self.dom_any and self.dow_any:
            return True
        if self.dom_any:
            return dow
        if self.dow_any:
            return dom
        return dom or dow


def _field(text: str, low: int, high: int) -> tuple[tuple[int, ...], bool]:
    values: set[int] = set()
    for item in text.split(","):
        found = _ITEM.match(item)
        if found is None:
            raise ValueError(f"invalid cron field {text!r}")
        base, step_text = found.group(1), found.group(2)
        step = int(step_text) if step_text else 1
        if step < 1:
            raise ValueError(f"invalid cron step in {text!r}")
        if base == "*":
            start, end = low, high
        elif "-" in base:
            start, end = (int(part) for part in base.split("-"))
        else:
            start = int(base)
            end = high if step_text else start
        if not low <= start <= end <= high:
            raise ValueError(f"cron field {text!r} is outside {low}-{high}")
        values.update(range(start, end + 1, step))
    return tuple(sorted(values)), text == "*"


def parse_cron(text: str) -> CronSpec:
    """Five fields (minute hour day-of-month month day-of-week) or a macro such as `@daily`."""
    expression = _MACROS.get(text.strip(), text.strip())
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(f"invalid cron expression {text!r}: five fields or a macro such as @daily")
    parsed = [_field(field, low, high) for field, (low, high) in zip(fields, _BOUNDS, strict=True)]
    weekdays = tuple(sorted({day % 7 for day in parsed[4][0]}))  # 7 is Sunday too
    return CronSpec(
        text=text.strip(),
        minutes=parsed[0][0],
        hours=parsed[1][0],
        days=parsed[2][0],
        months=parsed[3][0],
        weekdays=weekdays,
        dom_any=parsed[2][1],
        dow_any=parsed[4][1],
    )


def next_run(spec: CronSpec, after: datetime) -> datetime | None:
    """The first matching minute after `after` (same time zone), within the next 5 years; `None` if none."""
    start = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    day = start.replace(hour=0, minute=0)
    for _ in range(366 * 5):
        if spec.day_matches(day):
            for hour in spec.hours:
                for minute in spec.minutes:
                    candidate = day.replace(hour=hour, minute=minute)
                    if candidate >= start:
                        return candidate
        day += timedelta(days=1)
    return None


def backend_for_host(*, platform: str | None = None, has_systemd: bool | None = None) -> ScheduleBackend:
    """The native backend of this host: Task Scheduler, launchd, a systemd timer, or cron without systemd."""
    from pathlib import Path

    chosen = platform if platform is not None else sys.platform
    if chosen == "win32":
        from lakehouse.schedule.windows import WindowsBackend

        return WindowsBackend()
    if chosen == "darwin":
        from lakehouse.schedule.launchd import LaunchdBackend

        return LaunchdBackend()
    systemd = has_systemd if has_systemd is not None else Path("/run/systemd/system").is_dir()
    if systemd:
        from lakehouse.schedule.systemd import SystemdBackend

        return SystemdBackend()
    from lakehouse.schedule.cron import CronBackend

    return CronBackend()


def command_line(command: Sequence[str]) -> str:
    """A POSIX shell rendering of a command (systemd and cron read it)."""
    import shlex

    return " ".join(shlex.quote(part) for part in command)
