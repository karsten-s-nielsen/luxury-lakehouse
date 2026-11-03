"""Task Scheduler on Windows (spec §17.5, §18; plan Task 24).

The task definition is Task Scheduler XML, registered with `schtasks /Create /XML ... /F` (idempotent). Task
Scheduler has no environment setting, so the action runs this module as an entry (`python -m
lakehouse.schedule.windows --env K=V ... -- <CLI arguments>`), which sets the non-secret variables and calls the CLI.
Missed runs: `StartWhenAvailable` runs one late run on wake, and `MultipleInstancesPolicy=IgnoreNew` never starts a
second one (§17.5). System mode registers the task for the operator account with the `S4U` logon type ("run whether
user is logged on or not", no stored password) and the least privilege (§18, §20).
"""

from __future__ import annotations

import getpass
import importlib
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Final
from xml.sax.saxutils import escape

from defusedxml import ElementTree

from lakehouse.limits import raise_open_file_limit
from lakehouse.schedule.base import (
    TASK_NAME,
    CronSpec,
    ScheduleStatus,
    check_env,
    check_run_as,
    default_command,
    next_run,
    parse_cron,
    resolve_tool,
)

__all__ = ["WindowsBackend", "main", "render_task_xml"]

_NS: Final = "http://schemas.microsoft.com/windows/2004/02/mit/task"
_MAX_TRIGGERS: Final = 48  # Task Scheduler's limit per task
_DAYS: Final = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
_MONTHS: Final = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_NAME: Final = re.compile(r"^[A-Za-z0-9._-]+$")
_CLI: Final = ("-m", "lakehouse.cli")
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _quote(argument: str) -> str:
    """One argument for the Windows command line (CommandLineToArgvW rules), always in double quotes."""
    out: list[str] = []
    backslashes = 0
    for char in argument:
        if char == "\\":
            backslashes += 1
            continue
        if char == '"':
            out.append("\\" * (backslashes * 2 + 1) + '"')
        else:
            out.append("\\" * backslashes + char)
        backslashes = 0
    out.append("\\" * (backslashes * 2))
    return '"' + "".join(out) + '"'


def _entry_arguments(command: Sequence[str], env: Mapping[str, str]) -> tuple[str, str]:
    if len(command) < 3 or tuple(command[1:3]) != _CLI:
        raise ValueError("the scheduled command must be '<python> -m lakehouse.cli <arguments>'")
    arguments = ["-m", "lakehouse.schedule.windows"]
    for key, value in env.items():
        arguments += ["--env", f"{key}={value}"]
    arguments += ["--", *command[3:]]
    return command[0], " ".join(_quote(argument) for argument in arguments)


def _schedules(spec: CronSpec) -> list[str]:
    all_months = spec.months == tuple(range(1, 13))
    months = "".join(f"<{_MONTHS[m - 1]}/>" for m in spec.months)
    by_month = (
        "<ScheduleByMonth><DaysOfMonth>"
        + "".join(f"<Day>{d}</Day>" for d in (range(1, 32) if spec.dom_any else spec.days))
        + f"</DaysOfMonth><Months>{months}</Months></ScheduleByMonth>"
    )
    weekdays = "".join(f"<{_DAYS[d]}/>" for d in sorted(spec.weekdays, key=lambda d: (d == 0, d)))
    by_week = (
        f"<ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek>{weekdays}</DaysOfWeek></ScheduleByWeek>"
        if all_months
        else "<ScheduleByMonthDayOfWeek><Weeks><Week>1</Week><Week>2</Week><Week>3</Week><Week>4</Week>"
        f"<Week>Last</Week></Weeks><DaysOfWeek>{weekdays}</DaysOfWeek><Months>{months}</Months>"
        "</ScheduleByMonthDayOfWeek>"
    )
    if spec.dom_any and spec.dow_any:
        return ["<ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>" if all_months else by_month]
    if spec.dom_any:
        return [by_week]
    if spec.dow_any:
        return [by_month]
    return [by_month, by_week]  # cron: either day field matches


def _times(spec: CronSpec) -> list[tuple[int, int, int | None]]:
    """(hour, minute, repetition minutes): one repeating trigger for an every-N-minutes schedule, else one per time."""
    if len(spec.minutes) > 1 and spec.hours == tuple(range(24)):
        first, step = spec.minutes[0], spec.minutes[1] - spec.minutes[0]
        if spec.minutes == tuple(range(first, 60, step)) and first < step and 60 % step == 0:
            return [(0, first, step)]
    return [(hour, minute, None) for hour in spec.hours for minute in spec.minutes]


def render_task_xml(
    cron: str,
    *,
    command: Sequence[str],
    env: Mapping[str, str],
    system: bool,
    run_as: str | None,
    user: str,
) -> str:
    spec = parse_cron(cron)
    checked = check_env(env)
    account = check_run_as(run_as, system=system) if system else user
    schedules, times = _schedules(spec), _times(spec)
    if len(schedules) * len(times) > _MAX_TRIGGERS:
        raise ValueError(f"the cron expression {cron!r} needs more than {_MAX_TRIGGERS} Task Scheduler triggers")
    triggers: list[str] = []
    for hour, minute, repeat in times:
        repetition = (
            f"<Repetition><Interval>PT{repeat}M</Interval><Duration>P1D</Duration></Repetition>" if repeat else ""
        )
        for schedule in schedules:
            triggers.append(
                f"<CalendarTrigger>{repetition}<StartBoundary>2026-01-01T{hour:02d}:{minute:02d}:00</StartBoundary>"
                f"<Enabled>true</Enabled>{schedule}</CalendarTrigger>"
            )
    program, arguments = _entry_arguments(command, checked)
    logon = "S4U" if system else "InteractiveToken"
    return (
        '<?xml version="1.0" encoding="UTF-16"?>\n'
        f'<Task version="1.4" xmlns="{_NS}">'
        f"<RegistrationInfo><Description>lakehouse v2 ingestion; cron: {escape(spec.text)}</Description>"
        "</RegistrationInfo>"
        f"<Triggers>{''.join(triggers)}</Triggers>"
        f'<Principals><Principal id="Author"><UserId>{escape(account or "")}</UserId><LogonType>{logon}</LogonType>'
        "<RunLevel>LeastPrivilege</RunLevel></Principal></Principals>"
        "<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"
        "<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>"
        "<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><StartWhenAvailable>true</StartWhenAvailable>"
        "<RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable><Enabled>true</Enabled>"
        "<ExecutionTimeLimit>PT12H</ExecutionTimeLimit></Settings>"
        f'<Actions Context="Author"><Exec><Command>{escape(program)}</Command>'
        f"<Arguments>{escape(arguments)}</Arguments></Exec></Actions>"
        "</Task>\n"
    )


def _current_user() -> str:
    domain = os.environ.get("USERDOMAIN")
    name = getpass.getuser()
    return f"{domain}\\{name}" if domain else name


class WindowsBackend:
    """Task Scheduler through `schtasks` (never a shell)."""

    def __init__(
        self,
        *,
        name: str = TASK_NAME,
        command: Sequence[str] | None = None,
        run: Runner = subprocess.run,
        tool: Callable[[str], str] = resolve_tool,
        user: str | None = None,
    ) -> None:
        if not _NAME.match(name):
            raise ValueError(f"invalid task name {name!r}")
        self._name = name
        self._command = list(command) if command is not None else default_command()
        self._run = run
        self._tool = tool
        self._user = user

    def install(self, cron: str, *, system: bool, env: Mapping[str, str], run_as: str | None = None) -> None:
        xml = render_task_xml(
            cron,
            command=self._command,
            env=env,
            system=system,
            run_as=run_as,
            user=self._user or _current_user(),
        )
        with tempfile.TemporaryDirectory(prefix="lakehouse-task-") as folder:
            path = Path(folder) / "task.xml"
            path.write_text(xml, encoding="utf-16")
            self._run(
                [self._tool("schtasks"), "/Create", "/TN", self._name, "/XML", str(path), "/F"],
                shell=False,
                check=True,
                capture_output=True,
                text=True,
            )

    def uninstall(self) -> None:
        self._run(
            [self._tool("schtasks"), "/Delete", "/TN", self._name, "/F"],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
        )

    def status(self) -> ScheduleStatus:
        found = self._run(
            [self._tool("schtasks"), "/Query", "/TN", self._name, "/XML"],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
        )
        if found.returncode != 0 or not found.stdout.strip():
            return ScheduleStatus("windows", False, False, None, None, None)
        task: Any = ElementTree.fromstring(found.stdout.strip().encode("utf-16"))
        ns = {"t": _NS}
        description = str(task.findtext("t:RegistrationInfo/t:Description", default="", namespaces=ns))
        cron = description.split("cron: ", 1)[1] if "cron: " in description else None
        enabled = str(task.findtext("t:Settings/t:Enabled", default="true", namespaces=ns)).lower() == "true"
        info = self._run(
            [
                self._tool("powershell"),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                f"(Get-ScheduledTaskInfo -TaskName '{self._name}').LastTaskResult",
            ],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
        )
        last = info.stdout.strip() if info.returncode == 0 and info.stdout.strip() else None
        upcoming = next_run(parse_cron(cron), datetime.now().astimezone()) if cron and enabled else None
        return ScheduleStatus("windows", True, enabled, cron, upcoming, last)


def main(argv: Sequence[str] | None = None) -> int:
    """The task's entry: `--env K=V` pairs (non-secret, checked again), then `--` and the CLI arguments."""
    raise_open_file_limit()  # a no-op on Windows; every entry point calls it first (T12 spec D1)
    args = list(sys.argv[1:] if argv is None else argv)
    if "--" not in args:
        raise SystemExit("usage: python -m lakehouse.schedule.windows [--env K=V ...] -- <CLI arguments>")
    split = args.index("--")
    head, tail = args[:split], args[split + 1 :]
    env: dict[str, str] = {}
    for flag, pair in zip(head[0::2], head[1::2], strict=True):
        if flag != "--env" or "=" not in pair:
            raise SystemExit(f"unexpected argument {flag!r}")
        key, value = pair.split("=", 1)
        env[key] = value
    os.environ.update(check_env(env))
    cli: Any = importlib.import_module("lakehouse.cli")
    return int(cli.main(tail))


if __name__ == "__main__":
    sys.exit(main())
